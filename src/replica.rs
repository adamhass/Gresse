use crate::http_server::launch_http_server;
// use crate::vectors::{api::*, vector_db::*, Float, Key, Vector};
// use crate::prelude::*;
use crate::network::NetworkManager;
use crate::prelude::now_micros;
use crate::{
    crdt::*,
    prelude::{Pid, ServerAddr},
};
use csv::WriterBuilder;
use rand::Rng;
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::sync::RwLock;

pub type ClientResponder<T> =
    oneshot::Sender<Result<<T as CRDT>::ClientResponse, <T as CRDT>::Error>>;

pub struct Replica<T: CRDT + Debug + Clone> {
    crdt: Arc<RwLock<T>>,
    pid: Pid,
    sync_interval: Duration,
    // Network Stuff:
    writers: HashMap<Pid, Sender<ReplicaMessage<T>>>,
    client_request_receiver: Receiver<(T::Mutation, ClientResponder<T>)>,
    replication_receiver: Receiver<ReplicaMessage<T>>,
    // Allows new connections to be established:
    replication_writer_receiver: Receiver<(Pid, Sender<ReplicaMessage<T>>)>,
    // Records results/metrics
    metric_writer: csv::Writer<std::fs::File>,
    // Shutdown channel
    shutdown_receiver: Option<oneshot::Receiver<()>>,
    http_shutdown_sender: Option<oneshot::Sender<()>>,
    network_shutdown_sender: Option<oneshot::Sender<()>>,
}

/// CRDT Server implementation
/// This server is responsible for handling all CRDT mutations
/// Queries are handled immediately by the HTTP server
/// Supports two replication schemes:
///     - Push Based: Immediately transmitting each mutation to other replicas
///     - Pull Based: Periodically pulls delta groups.
/// The replication scheme is determined by the "use_deltas" flag.
impl<T: CRDT + 'static + Send + Sync + Debug + Clone> Replica<T> {
    /// Create a new CRDT Server, returns when it is fully ready to run with established connections to all neighbors
    pub async fn new(
        pid: Pid,
        crdt: Arc<RwLock<T>>,
        address: ServerAddr,
        server_list_file_path: PathBuf,
        sync_interval: Duration,
        result_dir_path: PathBuf,
    ) -> (Self, oneshot::Sender<()>) {
        // Launch HTTP server with shutdown capability
        let (client_request_receiver, http_shutdown_sender) =
            launch_http_server::<T>(&address, crdt.clone()).await;

        // Launch NetworkManager with shutdown capability
        let (replication_receiver, replication_writer_receiver, network_shutdown_sender) =
            NetworkManager::<ReplicaMessage<T>>::launch_network_manager(
                address,
                pid,
                server_list_file_path,
            )
            .await;

        // Initialize metric writer
        let mut result_path = result_dir_path.clone();
        result_path.push(format!("server_{}.csv", pid));
        if let Some(parent) = result_path.parent() {
            println!("Path (debug): {:?}", parent);
            std::fs::create_dir_all(parent).expect("Failed to create directories");
        }
        let result_file = std::fs::File::create(result_path.clone())
            .unwrap_or_else(|_| panic!("Failed to create result file {:?}", result_path));
        let metric_writer = WriterBuilder::new().flexible(true).from_writer(result_file);

        // Create shutdown channel for the CRDT server itself
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();

        (
            Replica {
                pid,
                crdt,
                client_request_receiver,
                replication_writer_receiver,
                writers: HashMap::new(),
                replication_receiver,
                shutdown_receiver: Some(shutdown_receiver),
                http_shutdown_sender: Some(http_shutdown_sender),
                network_shutdown_sender: Some(network_shutdown_sender),
                sync_interval,
                metric_writer,
            },
            shutdown_sender,
        )
    }

    /// Gracefully shuts down the server, stopping all network connections and the HTTP server
    pub async fn shutdown(&mut self) {
        println!("Shutting down CRDT Server {}", self.pid);

        // Shutdown HTTP Server
        if let Some(http_sender) = self.http_shutdown_sender.take() {
            let _ = http_sender.send(()); // Ignore error if receiver is already dropped
        }

        // Shutdown NetworkManager
        if let Some(network_sender) = self.network_shutdown_sender.take() {
            let _ = network_sender.send(()); // Ignore error if receiver is already dropped
        }

        // Close all writer channels
        self.writers.clear();

        // Flush metric writer
        self.metric_writer
            .flush()
            .expect("Failed to flush metric writer");

        println!("CRDT Server {} shutdown complete", self.pid);
    }

    /// Get a shutdown handle that can be used to trigger server shutdown
    pub fn get_shutdown_handle(&mut self) -> oneshot::Sender<()> {
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
        self.shutdown_receiver = Some(shutdown_receiver);
        shutdown_sender
    }

    pub async fn run(&mut self) {
        // Take ownership of shutdown_receiver from self
        let mut shutdown_receiver = self
            .shutdown_receiver
            .take()
            .expect("Failed to take shutdown receiver");

        let mut interval = tokio::time::interval(self.sync_interval);
        loop {
            tokio::select! {
                Some((client_request, responder)) = self.client_request_receiver.recv() => {
                    self.handle_client_request(client_request, responder).await;
                }
                Some(remote_event) = self.replication_receiver.recv() => {
                    self.handle_remote_event(remote_event).await;
                }
                _ = interval.tick() => {
                    self.pull_delta().await;
                }
                Some((pid, writer)) = self.replication_writer_receiver.recv() => {
                    println!("{} Received replication writer for Pid: {}", self.pid, pid);
                    self.writers.insert(pid, writer);
                }
                _ = &mut shutdown_receiver => {
                    println!("Shutdown signal received for server {}", self.pid);
                    self.shutdown().await;
                    break;
                }
            }
        }
    }

    async fn handle_client_request(
        &mut self,
        mutation: T::Mutation,
        responder: ClientResponder<T>,
    ) {
        let received = now_micros();
        let (start, response) = {
            // Scoped to release the lock ASAP
            let mut writable = self.crdt.write().await;
            let start = now_micros();
            (start, writable.mutate(mutation))
        };
        let end = now_micros();
        self.metric_writer
            .write_record(&[
                "mutate".to_string(),
                "".to_string(),
                received.to_string(),
                start.to_string(),
                end.to_string(),
                "".to_string(),
                "".to_string(),
            ])
            .expect("Failed to write metric");
        responder
            .send(Ok(response))
            .expect("failed to send confirmation");
    }

    async fn handle_remote_event(&mut self, event: ReplicaMessage<T>) {
        let received = now_micros();
        match event {
            ReplicaMessage::<T>::DeltaGroup(delta, sent) => {
                let (start, (insert_count, delete_count)) = {
                    let mut writable = self.crdt.write().await;
                    let start = now_micros();
                    (start, writable.merge_delta_group(delta))
                };
                let end = now_micros();
                self.metric_writer
                    .write_record(&[
                        "merge_delta".to_string(),
                        sent.to_string(),
                        received.to_string(),
                        start.to_string(),
                        end.to_string(),
                        insert_count.to_string(),
                        delete_count.to_string(),
                    ])
                    .expect("Failed to write metric");
            }
            ReplicaMessage::<T>::VersionVector(pid, vv, sent) => {
                let (start, (delta, insert_count, delete_count)) = {
                    let readable = self.crdt.read().await;
                    let start = now_micros();
                    (start, readable.get_delta(&vv))
                };
                let end = now_micros();
                self.writers
                    .get(&pid)
                    .expect("Failed to get writer")
                    .send(ReplicaMessage::<T>::DeltaGroup(delta, end))
                    .await
                    .expect("Failed to send delta");
                self.metric_writer
                    .write_record(&[
                        "get_delta".to_string(),
                        sent.to_string(),
                        received.to_string(),
                        start.to_string(),
                        end.to_string(),
                        insert_count.to_string(),
                        delete_count.to_string(),
                    ])
                    .expect("Failed to write metric");
            }
        }
    }

    async fn pull_delta(&mut self) {
        if self.writers.is_empty() {
            return;
        }
        // Select a random writer from self.writers
        let n = rand::rng().random_range(0..self.writers.len());
        if let Some(writer) = self.writers.values().nth(n) {
            let vv = { self.crdt.read().await.get_version_vector().clone() };
            writer
                .send(ReplicaMessage::<T>::VersionVector(
                    self.pid,
                    vv,
                    now_micros(),
                ))
                .await
                .expect("Failed to send pull request");
        }
    }
    // async fn broadcast(&mut self, message: ReplicaMessage<T>) {
    //     for writer in self.writers.values_mut() {
    //         writer
    //             .send(message.clone())
    //             .await
    //             .expect("Failed to send message");
    //     }
    // }
}
