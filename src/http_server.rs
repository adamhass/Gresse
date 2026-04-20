use crate::crdt::*;
use crate::prelude::*;

use crate::http_client::HttpError;
use crate::replica_helpers::*;
use bytes::Bytes;
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1::Builder;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::sync::RwLock;

use std::fmt::Debug;

pub async fn launch_http_server<T: CRDT + Send + Sync + 'static + Clone + Debug>(
    address: &ServerAddr,
    crdt: Arc<RwLock<T>>,
) -> (
    Receiver<(T::Mutation, ClientResponder<T>)>,
    oneshot::Sender<()>,
) {
    let http_listener = TcpListener::bind(address.http())
        .await
        .expect("Failed to bind to http address");

    let (client_request_sender, client_request_receiver) =
        channel::<(T::Mutation, ClientResponder<T>)>(100);

    // Create a shutdown channel
    let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();

    tokio::spawn(http_server_loop::<T>(
        http_listener,
        client_request_sender,
        crdt,
        shutdown_receiver,
    ));

    (client_request_receiver, shutdown_sender)
}

/// Runs a HTTP Service listening for incoming ClientRequests
/// Handles queries and forwards mutations over the ´client_request_sender´
/// Runs until a shutdown signal is received.
async fn http_server_loop<T: CRDT + Send + Sync + 'static + Clone + Debug>(
    http_listener: TcpListener,
    client_request_sender: Sender<(T::Mutation, ClientResponder<T>)>,
    crdt: Arc<RwLock<T>>,
    mut shutdown_receiver: oneshot::Receiver<()>,
) {
    println!("Running CRDT HTTP Server");
    println!(
        "Listening for HTTP on: {}",
        http_listener.local_addr().unwrap()
    );

    // Enter HTTP Listener event loop with shutdown capability:
    loop {
        let crdt_clone = crdt.clone();
        let client_request_sender_clone = client_request_sender.clone();

        tokio::select! {
            accept_result = http_listener.accept() => {
                match accept_result {
                    Ok((stream, addr)) => {
                        // Spawn a handler for the connection, using immutable reference to db
                        handle_http_connection(stream, addr, crdt_clone, client_request_sender_clone);
                    }
                    Err(e) => {
                        eprintln!("server accept error: {}", e);
                    }
                }
            }
            _ = &mut shutdown_receiver => {
                println!("HTTP server received shutdown signal, terminating...");
                break;
            }
        }
    }
    println!("HTTP server shutdown complete");
}

/// Spawns a new handler for each new connection
/// The handler will handle all requests on the connection
fn handle_http_connection<T: CRDT + Send + Sync + 'static + Clone + Debug>(
    stream: TcpStream,
    addr: SocketAddr,
    crdt: Arc<RwLock<T>>,
    client_channel: Sender<(T::Mutation, ClientResponder<T>)>,
) {
    let io = TokioIo::new(stream);
    tokio::spawn(async move {
        let service = service_fn(move |req: Request<Incoming>| {
            handle_request(req, crdt.clone(), client_channel.clone())
        });
        if Builder::new().serve_connection(io, service).await.is_err() {
            eprintln!("Server error: {}", addr);
        }
    });
}

/// This is our service handler. It receives a Request, routes on its
/// path, and returns a Future of a Response.
async fn handle_request<T: CRDT + Send + Sync + 'static + Clone + Debug>(
    req: Request<Incoming>,
    crdt: Arc<RwLock<T>>,
    server_channel: Sender<(T::Mutation, ClientResponder<T>)>,
) -> Result<Response<Full<Bytes>>, HttpError> {
    // let request_received = now();
    let request = client_req_from_incoming::<T>(req).await?;
    let response = match request {
        // Queries are handled directly by the HTTP Server
        CRDTClientRequest::<T>::Query(arg) => {
            let crdt = crdt.read().await;
            // println!("handling query");
            crdt.query(arg)
        }
        CRDTClientRequest::<T>::Mutation(mutation) => {
            // println!("handling mutation");
            forward_request::<T>(mutation.clone(), server_channel).await
        }
    };
    // eprintln!("response: {:?}", response);
    let response = serde_json::to_string(&response).unwrap();
    let response = Response::new(response.into());
    Ok(response)
}

async fn forward_request<T: CRDT + Send + Sync + 'static>(
    client_request: T::Mutation,
    server_channel: Sender<(T::Mutation, ClientResponder<T>)>,
) -> T::ClientResponse {
    // Send the request to Paxos for coordination
    let (tx, rx) = oneshot::channel();
    server_channel.send((client_request, tx)).await.unwrap();
    // Wait for the request to be handled...
    let result = rx.await.expect("Server failed to handle the message");
    if let Ok(response) = result {
        // eprintln!("got response: {:?}", &response);
        response
    } else {
        todo!("Handle error for forwarded request");
    }
}

async fn client_req_from_incoming<T: CRDT + Send + Sync + 'static + Clone + Debug>(
    incoming: Request<Incoming>,
) -> Result<CRDTClientRequest<T>, HttpError> {
    let bytes = incoming.into_body().collect().await?.to_bytes();
    Ok(serde_json::from_slice(&bytes)?)
}
