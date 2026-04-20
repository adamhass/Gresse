// use http_body_util::BodyExt;
// use http_body_util::Collected;
// use hyper::body::Incoming;
// use hyper::{Request, Response, Version};
// use omnipaxos::macros::Entry;
// use rand::Rng;
// use rand_chacha::ChaCha8Rng;
// use serde::{de::DeserializeOwned, Deserialize, Serialize};
// use std::fs;
// use std::io::Write;
// use thiserror::Error;
// use tokio::io::AsyncReadExt;
// use tokio::sync::oneshot;

// use crate::prelude::*;
// use super::*;

// pub type QueryResponse = Vec<Key>;

// pub(crate) type ClientRequest = (DbRequest, oneshot::Sender<Result<ClientResponse, DbError>>);
// pub type ClientResponse = (); // Placeholder for now

// impl DbRequest {
//     pub async fn from_incoming(incoming: Request<Incoming>) -> Result<DbRequest, DbError> {
//         let req_bytes = incoming
//             .into_body()
//             .collect()
//             .await
//             .map_err(|_| DbError::ParseError)?
//             .to_bytes();
//         // Deserialize req_bytes into DbRequest
//         let request = serde_json::from_slice(&req_bytes).map_err(|_| DbError::ParseError)?;
//         Ok(request)
//     }

//     pub fn to_json(self) -> Result<String, DbError> {
//         match self {
//             DbRequest::Query(id, k) => {
//                 // Construct JSON for a query request: { "id": <id>, "k": <k> }
//                 let request = serde_json::json!({
//                     "id": id,
//                     "k": k
//                 });
//                 serde_json::to_string(&request).map_err(|_| DbError::ParseError)
//             }
//             DbRequest::SVector(id, vector) => {
//                 // Construct JSON for a vector insertion request:
//                 // { "vectors": [<vector>], "ids": [<id>] }
//                 let request = serde_json::json!({
//                     "vectors": [vector],
//                     "ids": [id]
//                 });
//                 serde_json::to_string(&request).map_err(|_| DbError::ParseError)
//             }
//             DbRequest::UVector(id, vector) => {
//                 // Construct JSON for a vector insertion request:
//                 // { "vectors": [<vector>], "ids": [<id>] }
//                 let request = serde_json::json!({
//                     "vectors": [vector],
//                     "ids": [id]
//                 });
//                 serde_json::to_string(&request).map_err(|_| DbError::ParseError)
//             }
//             DbRequest::Neighbors(_, _) => todo!(),
//         }
//     }

//     pub fn to_http_request(self, uri: &str, host: &str) -> Result<Request<String>, DbError> {
//         let body = self.to_json()?;
//         let content_length = body.len().to_string();
//         Ok(Request::post(uri)
//             .version(Version::HTTP_11)
//             .header("host", host)
//             .header("user-agent", "curl/8.6.0")
//             .header("content-length", content_length)
//             .header("content-type", "application/json")
//             .header("accept", "*/*")
//             .body(body)
//             .map_err(|_| DbError::ParseError)?)
//     }
// }

// #[derive(Debug, Error)]
// pub enum DbError {
//     #[error("Failed to parse")]
//     ParseError,

//     #[error("Invalid request")]
//     NotFound,

//     #[error("Hyper error: {0}")]
//     HyperError(#[from] hyper::Error),

//     #[error("Serde JSON error: {0}")]
//     SerdeError(#[from] serde_json::Error),

//     // You can add more variants if needed
//     #[error("Other error: {0}")]
//     Other(String),
// }

// #[derive(Serialize, Deserialize, Debug)]
// pub enum DbResponse {
//     Added {
//         status: String,
//         num_vectors: usize,
//         request_received: u128,
//         request_handled: u128,
//     },
//     SearchResult {
//         distances: Vec<f32>,
//         // indices: Vec<Key>,
//         request_received: u128,
//     },
//     Saved {
//         status: String,
//         file: String,
//     },
//     Loaded {
//         status: String,
//         file: String,
//     },
// }

// impl DbResponse {
//     pub async fn from_incoming(incoming: Response<Incoming>) -> Result<DbResponse, DbError> {
//         let body_bytes = incoming
//             .into_body()
//             .collect() // Collect all chunks
//             .await?
//             .to_bytes();
//         let response = serde_json::from_slice(&body_bytes)?;
//         Ok(response)
//     }

//     pub fn to_json(self) -> Result<String, DbError> {
//         serde_json::to_string(&self).map_err(|_| DbError::ParseError)
//     }

//     pub fn write_csv_record(self, writer: &mut csv::Writer<std::fs::File>) {
//         match self {
//             DbResponse::Added {
//                 status,
//                 num_vectors,
//                 request_received,
//                 request_handled,
//             } => {
//                 let record = vec![
//                     num_vectors.to_string(),
//                     request_received.to_string(),
//                     request_handled.to_string(),
//                 ];
//                 writer.write_record(&record).unwrap();
//             }
//             DbResponse::SearchResult {
//                 distances,
//                 // indices,
//                 request_received,
//             } => {
//                 let record = vec![
//                     distances
//                         .iter()
//                         .map(|d| d.to_string())
//                         .collect::<Vec<String>>()
//                         .join(","),
//                     // indices.iter().map(|i| i.to_string()).collect::<Vec<String>>().join(","),
//                     request_received.to_string(),
//                 ];
//                 writer.write_record(&record).unwrap();
//             }
//             DbResponse::Saved { status, file } => {}
//             DbResponse::Loaded { status, file } => {}
//         }
//     }
// }
