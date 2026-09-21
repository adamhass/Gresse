use http_body_util::BodyExt;
use hyper::{client::conn::http1::SendRequest, Request, Version};
use hyper_util::rt::TokioIo;
use log::{debug, warn};
use serde::{de::DeserializeOwned, Serialize};
use std::net::SocketAddr;
use thiserror::Error;
use tokio::net::TcpStream;

const MAX_RETRIES: usize = 10;

#[derive(Debug, Error)]
pub enum HttpError {
    #[error("serialization failed: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("HTTP error: {0}")]
    Hyper(#[from] hyper::Error),
    #[error("HTTP error: {0}")]
    HyperHttp(#[from] hyper::http::Error),
}

pub struct HttpClient<Req, Res> {
    sender: SendRequest<String>,
    host: String,
    uri: String,
    _req: std::marker::PhantomData<Req>,
    _res: std::marker::PhantomData<Res>,
}

impl<Req, Res> HttpClient<Req, Res>
where
    Req: Serialize + Send + Sync + 'static,
    Res: DeserializeOwned + Send + 'static,
{
    pub async fn new(host: &str, uri: &str, addr: SocketAddr) -> Self {
        let tcpstream = TcpStream::connect(addr)
            .await
            .unwrap_or_else(|_| panic!("Failed to connect to server {}", addr));
        // let stream = connector.connect(dnsname, tcpstream).await.unwrap();
        let io = TokioIo::new(tcpstream);
        let (sender, conn) = hyper::client::conn::http1::Builder::new()
            .handshake(io)
            .await
            .unwrap();
        tokio::task::spawn(async move {
            if let Err(err) = conn.await {
                warn!("http client connection failed: {:?}", err);
            }
        });
        HttpClient::<Req, Res> {
            sender,
            host: host.into(),
            uri: uri.into(),
            _req: Default::default(),
            _res: Default::default(),
        }
    }

    pub async fn send(&mut self, request: &Req) -> Result<Res, HttpError> {
        let req = self.build_http_request(request)?;
        for i in 0..MAX_RETRIES {
            match self.sender.send_request(req.clone()).await {
                Ok(res) => {
                    let collected = res.into_body().collect().await;
                    let req_bytes = collected?.to_bytes();
                    let res: Res = serde_json::from_slice(&req_bytes).expect("deserialization");
                    return Ok(res);
                }
                Err(e) => {
                    debug!("http client send attempt {} failed: {}", i + 1, e);
                    if i == MAX_RETRIES - 1 {
                        return Err(HttpError::Hyper(e));
                    }
                }
            }
        }
        unreachable!("Unreachable code");
    }

    fn build_http_request(&self, request: &Req) -> Result<Request<String>, HttpError> {
        let body = serde_json::to_string(request)?;
        let content_length = body.len().to_string();
        Ok(Request::post(self.uri.clone())
            .version(Version::HTTP_11)
            .header("host", self.host.clone())
            .header("user-agent", "curl/8.6.0")
            .header("content-length", content_length)
            .header("content-type", "application/json")
            .header("accept", "*/*")
            .body(body)?)
    }
}
