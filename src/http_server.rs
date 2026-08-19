use crate::crdt::*;
use crate::prelude::*;

use crate::http_client::HttpError;
use bytes::Bytes;
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1::Builder;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use log::{debug, error, info};
use std::net::SocketAddr;
use std::sync::Arc;
use std::{future::Future, pin::Pin};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::sync::RwLock;

use std::fmt::Debug;

pub type ClientMutationHandler<T> = Arc<
    dyn Fn(
            <T as CRDT>::Mutation,
        ) -> Pin<Box<dyn Future<Output = <T as CRDT>::ClientResponse> + Send>>
        + Send
        + Sync,
>;

pub async fn launch_http_server<T: CRDT + Send + Sync + 'static + Clone + Debug>(
    address: &ServerAddr,
    crdt: Arc<RwLock<T>>,
    mutation_handler: ClientMutationHandler<T>,
) -> oneshot::Sender<()> {
    let http_listener = TcpListener::bind(address.http())
        .await
        .expect("Failed to bind to http address");

    // Create a shutdown channel
    let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();

    tokio::spawn(http_server_loop::<T>(
        http_listener,
        crdt,
        mutation_handler,
        shutdown_receiver,
    ));

    shutdown_sender
}

/// Runs an HTTP service listening for incoming client requests. Queries read
/// shared state directly; mutations are handled in the HTTP data plane by the
/// supplied durable mutation handler. Runs until a shutdown signal is received.
async fn http_server_loop<T: CRDT + Send + Sync + 'static + Clone + Debug>(
    http_listener: TcpListener,
    crdt: Arc<RwLock<T>>,
    mutation_handler: ClientMutationHandler<T>,
    mut shutdown_receiver: oneshot::Receiver<()>,
) {
    info!("running CRDT HTTP server");
    info!(
        "listening for HTTP on {}",
        http_listener.local_addr().unwrap()
    );

    // Enter HTTP Listener event loop with shutdown capability:
    loop {
        let crdt_clone = crdt.clone();
        let mutation_handler_clone = mutation_handler.clone();

        tokio::select! {
            accept_result = http_listener.accept() => {
                match accept_result {
                    Ok((stream, addr)) => {
                        // Spawn a handler for the connection, using immutable reference to db
                        handle_http_connection(stream, addr, crdt_clone, mutation_handler_clone);
                    }
                    Err(e) => {
                        error!("http server accept error: {}", e);
                    }
                }
            }
            _ = &mut shutdown_receiver => {
                info!("http server received shutdown signal");
                break;
            }
        }
    }
    info!("http server shutdown complete");
}

/// Spawns a new handler for each new connection
/// The handler will handle all requests on the connection
fn handle_http_connection<T: CRDT + Send + Sync + 'static + Clone + Debug>(
    stream: TcpStream,
    addr: SocketAddr,
    crdt: Arc<RwLock<T>>,
    mutation_handler: ClientMutationHandler<T>,
) {
    let io = TokioIo::new(stream);
    tokio::spawn(async move {
        let service = service_fn(move |req: Request<Incoming>| {
            handle_request(req, crdt.clone(), mutation_handler.clone())
        });
        if Builder::new().serve_connection(io, service).await.is_err() {
            error!("http server connection error for {}", addr);
        }
    });
}

/// This is our service handler. It receives a Request, routes on its
/// path, and returns a Future of a Response.
async fn handle_request<T: CRDT + Send + Sync + 'static + Clone + Debug>(
    req: Request<Incoming>,
    crdt: Arc<RwLock<T>>,
    mutation_handler: ClientMutationHandler<T>,
) -> Result<Response<Full<Bytes>>, HttpError> {
    // let request_received = now();
    let request = client_req_from_incoming::<T>(req).await?;
    let response = match request {
        // Queries are handled directly by the HTTP Server
        CRDTClientRequest::<T>::Query(arg) => {
            let crdt = crdt.read().await;
            debug!("handling client query request");
            crdt.query(arg)
        }
        CRDTClientRequest::<T>::Mutation(mutation) => {
            debug!("handling client mutation in HTTP data plane");
            mutation_handler(mutation).await
        }
    };
    let response = serde_json::to_string(&response).unwrap();
    let response = Response::new(response.into());
    Ok(response)
}

async fn client_req_from_incoming<T: CRDT + Send + Sync + 'static + Clone + Debug>(
    incoming: Request<Incoming>,
) -> Result<CRDTClientRequest<T>, HttpError> {
    let bytes = incoming.into_body().collect().await?.to_bytes();
    Ok(serde_json::from_slice(&bytes)?)
}
