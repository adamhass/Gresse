use crate::http_server::launch_http_server;
// use crate::vectors::{api::*, vector_db::*, Float, Key, Vector};
// use crate::prelude::*;
use crate::network::{NetworkManager, NetworkMember};
use crate::object_storage::{ObjectStorageClient, ObjectStorageConfig, ObjectStorageError};
use crate::{
    crdt::*,
    prelude::{new_pid, now_micros, Pid, ServerAddr},
};
use csv::WriterBuilder;
use rand::Rng;
use std::collections::HashMap;
use std::env;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::sync::RwLock;

pub type ClientResponder<T> =
    oneshot::Sender<Result<<T as CRDT>::ClientResponse, <T as CRDT>::Error>>;

pub struct ReplicaHandle {
    shutdown_sender: oneshot::Sender<()>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl ReplicaHandle {
    pub fn shutdown_sender(self) -> oneshot::Sender<()> {
        self.shutdown_sender
    }

    pub async fn shutdown(self) {
        let _ = self.shutdown_sender.send(());
        let _ = self.join_handle.await;
    }
}

#[derive(Debug, Clone)]
pub struct ReplicaConfig {
    pub address: ServerAddr,
    pub sync_interval: Duration,
    pub result_dir_path: PathBuf,
    pub object_storage_config: ObjectStorageConfig,
}

impl ReplicaConfig {
    pub fn from_env() -> Self {
        Self {
            address: ServerAddr::from_env(),
            result_dir_path: env_path("GRESSE_RESULT_DIR_PATH"),
            sync_interval: env::var("GRESSE_SYNC_INTERVAL_MS")
                .ok()
                .map(|value| {
                    Duration::from_millis(
                        value
                            .parse()
                            .expect("GRESSE_SYNC_INTERVAL_MS must be an integer"),
                    )
                })
                .unwrap_or(Duration::from_secs(1)),
            object_storage_config: ObjectStorageConfig {
                url: env_string("GRESSE_OBJECT_STORAGE_URL"),
                region: env_string("GRESSE_OBJECT_STORAGE_REGION"),
                bucket: env_string("GRESSE_OBJECT_STORAGE_BUCKET"),
                access_key: env_string("GRESSE_OBJECT_STORAGE_ACCESS_KEY"),
                secret_key: env_string("GRESSE_OBJECT_STORAGE_SECRET_KEY"),
                persistent_replica_path: env_string("GRESSE_PERSISTENT_REPLICA_PATH"),
                membership_directory_path: env_string("GRESSE_MEMBERSHIP_DIRECTORY_PATH"),
                discovery_interval: env::var("GRESSE_OBJECT_STORAGE_DISCOVERY_INTERVAL_MS")
                    .ok()
                    .map(|value| {
                        Duration::from_millis(value.parse().expect(
                            "GRESSE_OBJECT_STORAGE_DISCOVERY_INTERVAL_MS must be an integer",
                        ))
                    })
                    .unwrap_or(Duration::from_secs(1)),
            },
        }
    }
}

fn env_string(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} environment variable is required"))
}

fn env_path(name: &str) -> PathBuf {
    env::var(name)
        .map(PathBuf::from)
        .unwrap_or_else(|_| panic!("{name} environment variable is required"))
}

pub struct Replica<T: CRDT + Debug + Clone> {
    crdt: Arc<RwLock<T>>,
    pid: Pid,
    address: ServerAddr,
    sync_interval: Duration,
    // Network Stuff:
    writers: HashMap<Pid, Sender<ReplicaMessage<T>>>,
    client_request_receiver: Receiver<(T::Mutation, ClientResponder<T>)>,
    replication_receiver: Receiver<ReplicaMessage<T>>,
    // Allows new connections to be established:
    replication_writer_receiver: Receiver<(Pid, Sender<ReplicaMessage<T>>)>,
    network_member_sender: Sender<NetworkMember>,
    // Records results/metrics
    metric_writer: csv::Writer<std::fs::File>,
    object_storage_client: ObjectStorageClient,
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
    /// Create a new CRDT server from environment variables.
    ///
    /// Required:
    /// - `GRESSE_ADDR`
    /// - `GRESSE_HTTP_PORT`
    /// - `GRESSE_INTERNAL_PORT`
    /// - `GRESSE_RESULT_DIR_PATH`
    /// - `GRESSE_OBJECT_STORAGE_URL`
    /// - `GRESSE_OBJECT_STORAGE_REGION`
    /// - `GRESSE_OBJECT_STORAGE_BUCKET`
    /// - `GRESSE_OBJECT_STORAGE_ACCESS_KEY`
    /// - `GRESSE_OBJECT_STORAGE_SECRET_KEY`
    /// - `GRESSE_PERSISTENT_REPLICA_PATH`
    /// - `GRESSE_MEMBERSHIP_DIRECTORY_PATH`
    ///
    /// Optional:
    /// - `GRESSE_SYNC_INTERVAL_MS`, defaults to 1000.
    /// - `GRESSE_OBJECT_STORAGE_DISCOVERY_INTERVAL_MS`, defaults to 1000.
    pub async fn new(crdt: T) -> ReplicaHandle {
        let (mut replica, shutdown_sender) =
            Self::with_config(new_pid(), crdt, ReplicaConfig::from_env()).await;
        let join_handle = tokio::spawn(async move {
            replica.run().await;
        });
        ReplicaHandle {
            shutdown_sender,
            join_handle,
        }
    }

    /// Create a new CRDT server from explicit configuration.
    pub async fn with_config(
        pid: Pid,
        mut crdt: T,
        config: ReplicaConfig,
    ) -> (Self, oneshot::Sender<()>) {
        crdt.set_pid(pid);
        let crdt = Arc::new(RwLock::new(crdt));
        Self::with_shared_config(pid, crdt, config).await
    }

    /// Create a new CRDT server from explicit configuration and shared CRDT state.
    pub async fn with_shared_config(
        pid: Pid,
        crdt: Arc<RwLock<T>>,
        config: ReplicaConfig,
    ) -> (Self, oneshot::Sender<()>) {
        crdt.write().await.set_pid(pid);

        // Launch HTTP server with shutdown capability
        let (client_request_receiver, http_shutdown_sender) =
            launch_http_server::<T>(&config.address, crdt.clone()).await;

        // Launch NetworkManager with shutdown capability
        let (
            replication_receiver,
            replication_writer_receiver,
            network_member_sender,
            network_shutdown_sender,
        ) = NetworkManager::<ReplicaMessage<T>>::launch_network_manager(config.address, pid).await;

        // Initialize metric writer
        let mut result_path = config.result_dir_path.clone();
        result_path.push(format!("server_{}.csv", pid));
        if let Some(parent) = result_path.parent() {
            println!("Path (debug): {:?}", parent);
            std::fs::create_dir_all(parent).expect("Failed to create directories");
        }
        let result_file = std::fs::File::create(result_path.clone())
            .unwrap_or_else(|_| panic!("Failed to create result file {:?}", result_path));
        let metric_writer = WriterBuilder::new().flexible(true).from_writer(result_file);
        let object_storage_client = ObjectStorageClient::new(config.object_storage_config)
            .unwrap_or_else(|error| panic!("Failed to initialize object storage client: {error}"));

        // Create shutdown channel for the CRDT server itself
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();

        (
            Replica {
                pid,
                address: config.address,
                crdt,
                client_request_receiver,
                replication_writer_receiver,
                network_member_sender,
                writers: HashMap::new(),
                replication_receiver,
                shutdown_receiver: Some(shutdown_receiver),
                http_shutdown_sender: Some(http_shutdown_sender),
                network_shutdown_sender: Some(network_shutdown_sender),
                sync_interval: config.sync_interval,
                metric_writer,
                object_storage_client,
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
        self.init().await;

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

    async fn init(&mut self) {
        let (members, persistent_crdt) = tokio::join!(
            async {
                self.push_replica_descriptor().await;
                self.list_members().await
            },
            self.read_persistent_replica()
        );
        println!("Discovered {} replica descriptors", members.len());

        self.enqueue_network_members(&members).await;

        if let Some(mut persistent_crdt) = persistent_crdt {
            let max_membership_epoch = Self::max_membership_epoch(&members);
            if max_membership_epoch <= persistent_crdt.epoch() {
                persistent_crdt.set_pid(self.pid);
                *self.crdt.write().await = persistent_crdt;
            } else {
                todo!(
                    "Persistent replica epoch {} is older than membership epoch {}, handler not implemented yet!",
                    persistent_crdt.epoch(),
                    max_membership_epoch
                );
            }
        }
    }

    fn max_membership_epoch(members: &[String]) -> Epoch {
        members
            .iter()
            .filter_map(|member| Self::parse_membership_descriptor(member))
            .map(|(_, epoch)| epoch)
            .max()
            .unwrap_or(0)
    }

    fn parse_membership_descriptor(member: &str) -> Option<(NetworkMember, Epoch)> {
        let descriptor = member.rsplit('/').next().unwrap_or(member);
        let mut parts = descriptor.split(',');
        let pid = parts.next()?.parse().ok()?;
        let address = parts.next()?.parse().ok()?;
        let epoch = parts.next()?.parse().ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some((NetworkMember { pid, address }, epoch))
    }

    async fn enqueue_network_members(&self, members: &[String]) {
        for member in members {
            if let Some((network_member, _)) = Self::parse_membership_descriptor(member) {
                self.network_member_sender
                    .send(network_member)
                    .await
                    .expect("Failed to send network member");
            }
        }
    }

    async fn read_persistent_replica(&self) -> Option<T> {
        match self
            .object_storage_client
            .download_data::<T>(self.object_storage_client.persistent_replica_path())
            .await
        {
            Ok((persistent_crdt, _)) => Some(persistent_crdt),
            Err(ObjectStorageError::FileNotFound) => None,
            Err(error) => {
                panic!("Failed to read persistent replica state: {error}");
            }
        }
    }

    async fn push_replica_descriptor(&self) {
        let descriptor_path = self.replica_descriptor_path().await;
        self.object_storage_client
            .upload_empty_atomic_create(&descriptor_path)
            .await
            .unwrap_or_else(|error| panic!("Failed to create replica descriptor: {error}"));
    }

    async fn list_members(&self) -> Vec<String> {
        self.object_storage_client
            .list_objects(self.object_storage_client.membership_directory_path())
            .await
            .unwrap_or_else(|error| panic!("Failed to list replica membership: {error}"))
    }

    async fn replica_descriptor_path(&self) -> String {
        let membership_directory = self
            .object_storage_client
            .membership_directory_path()
            .trim_end_matches('/');
        let epoch = self.crdt.read().await.epoch();
        format!(
            "{}/{},{},{}",
            membership_directory,
            self.pid,
            self.address.internal(),
            epoch
        )
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
