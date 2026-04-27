use crate::http_server::launch_http_server;
// use crate::vectors::{api::*, vector_db::*, Float, Key, Vector};
// use crate::prelude::*;
use crate::dots::{Counter, Dot, DotSet, VersionMatrix};
use crate::network::{NetworkManager, NetworkMember};
use crate::object_storage::ObjectStorageClient;
use crate::replica_helpers::*;
use crate::{
    crdt::*,
    prelude::{new_pid, now_micros, Pid, ServerAddr},
};
use csv::WriterBuilder;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use tokio::time::{Instant, Interval};

const STABLE_REPLICA_PID: Pid = 0;
const INITIAL_GC_COUNTER: Counter = 0;
const DEFAULT_GC_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct CRDTWrapper {
    version_matrix: VersionMatrix,
    gc_markers: Vec<(Dot, DotSet)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistentReplica<T> {
    local_state: T,
    crdt_wrapper: CRDTWrapper,
}

impl CRDTWrapper {
    fn new() -> Self {
        Self::default()
    }

    fn current_gc_marker(&self) -> Dot {
        self.gc_markers
            .last()
            .map(|(marker, _)| *marker)
            .unwrap_or(Dot {
                pid: STABLE_REPLICA_PID,
                counter: INITIAL_GC_COUNTER,
            })
    }

    fn current_stable(&self) -> Option<&DotSet> {
        self.gc_markers.last().map(|(_, stable)| stable)
    }

    fn needs_gc(&self, stable: &DotSet) -> bool {
        self.current_stable() != Some(stable)
    }

    fn push_gc_marker(&mut self, marker: Dot, stable: DotSet) {
        self.gc_markers.push((marker, stable));
    }

    fn gc_metadata(&self) -> Option<GcMetadata> {
        self.gc_markers.last().map(|(marker, stable)| GcMetadata {
            marker: *marker,
            stable: stable.clone(),
        })
    }
}

pub struct Replica<T: CRDT + Debug + Clone> {
    crdt: Arc<RwLock<T>>,
    crdt_wrapper: CRDTWrapper,
    // Identifiers
    pid: Pid,
    address: ServerAddr,
    // Config:
    sync_interval: Duration,
    gc_base_interval: Duration,
    gc_interval: Interval,
    membership_poll_interval: Interval,
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
        let membership_poll_period = config.object_storage_config.discovery_interval;
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
                crdt_wrapper: CRDTWrapper::new(),
                writers: HashMap::new(),
                replication_receiver,
                shutdown_receiver: Some(shutdown_receiver),
                http_shutdown_sender: Some(http_shutdown_sender),
                network_shutdown_sender: Some(network_shutdown_sender),
                sync_interval: config.sync_interval,
                gc_base_interval: DEFAULT_GC_INTERVAL,
                gc_interval: Self::gc_interval(DEFAULT_GC_INTERVAL),
                membership_poll_interval: Self::membership_poll_interval(membership_poll_period),
                metric_writer,
                object_storage_client,
            },
            shutdown_sender,
        )
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
                _ = self.gc_interval.tick() => {
                    let stable = self.crdt_wrapper.version_matrix.get_stable();
                    self.init_gc(stable).await;
                }
                _ = self.membership_poll_interval.tick() => {
                    self.poll_membership_directory().await;
                }
                Some((pid, writer)) = self.replication_writer_receiver.recv() => {
                    println!("{} Received replication writer for Pid: {}", self.pid, pid);
                    self.writers.insert(pid, writer);
                    // Update the gc_interval to minimize risk of simultanous attempts.
                    self.change_gc_interval();
                }
                _ = &mut shutdown_receiver => {
                    println!("Shutdown signal received for server {}", self.pid);
                    self.shutdown().await;
                    break;
                }
            }
        }
    }

    pub fn set_gc_interval(&mut self, interval: Duration) {
        self.gc_base_interval = interval;
        self.change_gc_interval();
    }

    fn gc_interval(interval: Duration) -> Interval {
        tokio::time::interval_at(Instant::now() + interval, interval)
    }

    fn membership_poll_interval(interval: Duration) -> Interval {
        tokio::time::interval_at(Instant::now() + interval, interval)
    }

    async fn init(&mut self) {
        let (members, persistent_replica) = tokio::join!(
            async {
                self.push_replica_descriptor().await;
                self.list_members().await
            },
            self.read_persistent_replica()
        );
        println!("Discovered {} replica descriptors", members.len());

        self.enqueue_network_members(&members).await;

        match persistent_replica {
            Some(mut persistent_replica) => {
                persistent_replica.local_state.set_pid(self.pid);
                *self.crdt.write().await = persistent_replica.local_state;
                self.crdt_wrapper = persistent_replica.crdt_wrapper;
            }
            None => self.write_initial_persistent_replica().await,
        }
    }

    async fn enqueue_network_members(&self, members: &[ReplicaDescriptor]) {
        let shutdown_pids = members
            .iter()
            .filter(|descriptor| descriptor.is_shutdown())
            .map(|descriptor| descriptor.pid)
            .collect::<HashSet<_>>();

        for descriptor in members {
            if descriptor.is_shutdown() || shutdown_pids.contains(&descriptor.pid) {
                continue;
            }

            self.network_member_sender
                .send(NetworkMember {
                    pid: descriptor.pid,
                    address: descriptor.address,
                    final_counter: descriptor.final_counter,
                })
                .await
                .expect("Failed to send network member");
        }
    }

    async fn read_persistent_replica(&self) -> Option<PersistentReplica<T>> {
        self.object_storage_client
            .read_persistent_replica()
            .await
            .unwrap_or_else(|error| panic!("Failed to read persistent replica state: {error}"))
    }

    async fn write_initial_persistent_replica(&self) {
        let persistent_replica = PersistentReplica {
            local_state: self.crdt.read().await.clone(),
            crdt_wrapper: self.crdt_wrapper.clone(),
        };
        self.object_storage_client
            .write_persistent_replica(&persistent_replica)
            .await
            .unwrap_or_else(|error| {
                panic!("Failed to write initial persistent replica state: {error}")
            });
    }

    async fn push_replica_descriptor(&self) {
        let descriptor = self.replica_descriptor().await;
        self.object_storage_client
            .write_membership_descriptor(descriptor)
            .await
            .unwrap_or_else(|error| panic!("Failed to create replica descriptor: {error}"));
    }

    async fn replica_descriptor(&self) -> ReplicaDescriptor {
        ReplicaDescriptor {
            pid: self.pid,
            address: self.address.internal(),
            gc_counter: self.crdt_wrapper.current_gc_marker().counter,
            final_counter: None,
        }
    }

    async fn list_members(&self) -> Vec<ReplicaDescriptor> {
        self.object_storage_client
            .list_membership_descriptors()
            .await
            .unwrap_or_else(|error| panic!("Failed to list replica membership: {error}"))
    }

    async fn poll_membership_directory(&mut self) {
        let members = self.list_members().await;
        let previous_writer_count = self.writers.len();
        // Find shutdown descriptors
        let shutdown_descriptors = members
            .iter()
            .filter(|descriptor| descriptor.is_shutdown())
            .copied()
            .collect::<Vec<_>>();
        for descriptor in shutdown_descriptors {
            self.handle_shutdown_descriptor(descriptor).await;
        }
        
        // Update the gc interval
        if self.writers.len() != previous_writer_count {
            self.change_gc_interval();
        }
    }

    async fn handle_shutdown_descriptor(&mut self, descriptor: ReplicaDescriptor) {
        // Keep the replica row until its final dot is stable everywhere.
        self.crdt_wrapper.version_matrix.insert_final_dot(Dot {
            pid: descriptor.pid,
            counter: descriptor
                .final_counter
                .expect("shutdown descriptor missing final counter"),
        });

        // Remove the writers
        let _ = self.writers.remove(&descriptor.pid);
        // Notify the network manager
        self.network_member_sender
            .send(NetworkMember {
                pid: descriptor.pid,
                address: descriptor.address,
                final_counter: descriptor.final_counter,
            })
            .await
            .expect("Failed to notify network manager about shutdown replica");
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
            ReplicaMessage::<T>::DeltaGroup(delta, gc_metadata, sent) => {
                if let Some(gc_metadata) = gc_metadata {
                    self.observe_gc(gc_metadata).await;
                }
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
                self.crdt_wrapper.version_matrix.update(pid, vv.clone());
                let (start, (delta, insert_count, delete_count)) = {
                    let readable = self.crdt.read().await;
                    let start = now_micros();
                    (start, readable.get_delta(&vv))
                };
                let end = now_micros();
                let gc_metadata = self.gc_metadata();
                self.writers
                    .get(&pid)
                    .expect("Failed to get writer")
                    .send(ReplicaMessage::<T>::DeltaGroup(delta, gc_metadata, end))
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
        if let Some(writer) = self.writers.values().nth(n).cloned() {
            let vv = { self.crdt.read().await.get_version_vector().clone() };
            self.crdt_wrapper.version_matrix.update(self.pid, vv.clone());
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

    async fn init_gc(&mut self, mut stable: DotSet) {
        if !self.crdt_wrapper.needs_gc(&stable) {
            // No need to gc
            return;
        }

        let previous_members = self.object_storage_client.membership_descriptors().await;
        let previous_gc_counters = Self::membership_gc_counters_by_pid(&previous_members);
        let previous_descriptor = self.replica_descriptor().await;

        // 1. Increment our GC marker in the Membership directory before GC.
        let new_marker = Dot {
            pid: STABLE_REPLICA_PID,
            counter: self
                .crdt_wrapper
                .current_gc_marker()
                .counter
                .saturating_add(1),
        };
        let new_descriptor = self.change_own_gc_counter(new_marker.counter).await;

        // 2. Read the Membership directory and check that: no new replicas have appeared, no other replicas have incremented their GC marker.
        let current_members = self.list_members().await;
        if !Self::membership_is_still_stable(
            &previous_gc_counters,
            &current_members,
            self.pid,
        ) {
            // Rollback the new membership desriptor
            self.object_storage_client
                .delete_membership_descriptor(new_descriptor)
                .await
                .unwrap_or_else(|error| {
                    panic!("Failed to roll back replica membership GC marker: {error}")
                });
            let _ = self.list_members().await;
            return;
        }

        // 3. Perform GC
        let departed_pids = self.crdt_wrapper.version_matrix.garbage_collect(&stable);
        if departed_pids.is_some()  {
            // Update the stable DotSet
            stable = self.crdt_wrapper.version_matrix.get_stable(); 
        }
        let local_state = {
            let mut writable = self.crdt.write().await;
            writable.gc(stable.clone(), departed_pids.clone());
            writable.clone()
        };

        // DONE
        self.crdt_wrapper.push_gc_marker(new_marker, stable.clone());
        let persistent_replica = PersistentReplica {
            local_state,
            crdt_wrapper: self.crdt_wrapper.clone(),
        };

        // 4. Overwrite persistent replica with the new local state.
        self.object_storage_client
            .write_persistent_replica(&persistent_replica)
            .await
            .unwrap_or_else(|error| {
                panic!("Failed to write persistent replica after garbage collection: {error}")
            });
        
        if let Some(departed_pids) = departed_pids {
            self.gc_departed_pids(departed_pids).await;
        }

        // 5. Remove our old Membership Descriptor ? 
        self.object_storage_client
            .delete_membership_descriptor(previous_descriptor)
            .await
            .unwrap_or_else(|error| {
                panic!("Failed to delete previous replica membership descriptor: {error}")
            });
    }

    async fn gc_departed_pids(&mut self, departed_pids: Vec<Pid>) {
        futures::future::try_join_all(
            departed_pids
                .into_iter()
                .map(|pid| self.object_storage_client.delete_membership_descriptors_for_pid(pid)),
        )
        .await
        .unwrap_or_else(|error| {
            panic!("Failed to garbage-collect departed replica membership descriptors: {error}")
        });
    }

    async fn change_own_gc_counter(&mut self, gc_counter: Counter) -> ReplicaDescriptor {
        let descriptor = ReplicaDescriptor {
            pid: self.pid,
            address: self.address.internal(),
            gc_counter,
            final_counter: None,
        };
        let descriptor_created = self
            .object_storage_client
            .write_membership_descriptor(descriptor)
            .await
            .unwrap_or_else(|error| {
                panic!("Failed to write replica membership GC marker: {error}")
            });
        if !descriptor_created {
            panic!("Replica membership GC marker already exists: {descriptor}");
        }
        descriptor
    }

    async fn observe_gc(&mut self, metadata: GcMetadata) {
        if metadata.marker.pid != STABLE_REPLICA_PID
            || metadata.marker.counter <= self.crdt_wrapper.current_gc_marker().counter
        {
            return;
        }

        self.crdt.write().await.gc(metadata.stable.clone(), None);
        self.crdt_wrapper
            .push_gc_marker(metadata.marker, metadata.stable.clone());
        self.change_own_gc_counter(metadata.marker.counter).await;
        self.gc_interval.reset();
    }

    fn gc_metadata(&self) -> Option<GcMetadata> {
        self.crdt_wrapper.gc_metadata()
    }

    fn membership_gc_counters_by_pid(members: &[ReplicaDescriptor]) -> HashMap<Pid, Counter> {
        let mut gc_counters = HashMap::new();
        for descriptor in members {
            gc_counters
                .entry(descriptor.pid)
                .and_modify(|gc_counter: &mut Counter| {
                    *gc_counter = (*gc_counter).max(descriptor.gc_counter)
                })
                .or_insert(descriptor.gc_counter);
        }
        gc_counters
    }

    fn membership_is_still_stable(
        previous_gc_counters: &HashMap<Pid, Counter>,
        current_members: &[ReplicaDescriptor],
        local_pid: Pid,
    ) -> bool {
        let current_gc_counters = Self::membership_gc_counters_by_pid(current_members);
        for (pid, current_gc_counter) in current_gc_counters {
            if pid == local_pid {
                continue;
            }

            let Some(previous_gc_counter) = previous_gc_counters.get(&pid) else {
                return false;
            };

            if current_gc_counter > *previous_gc_counter {
                return false;
            }
        }

        true
    }

    fn change_gc_interval(&mut self) {
        let replica_count = self.writers.len().saturating_add(1).max(1);
        let base_nanos = self.gc_base_interval.as_nanos();
        if base_nanos == 0 {
            self.gc_interval = Self::gc_interval(Duration::from_nanos(1));
            return;
        }

        let jitter_nanos = base_nanos / replica_count as u128;
        let lower_bound = base_nanos.saturating_sub(jitter_nanos).max(1);
        let upper_bound = base_nanos.saturating_add(jitter_nanos).max(lower_bound + 1);
        let interval_nanos = rand::rng().random_range(lower_bound..=upper_bound);
        let interval_nanos = interval_nanos.min(u64::MAX as u128) as u64;

        self.gc_interval = Self::gc_interval(Duration::from_nanos(interval_nanos));
    }

    async fn write_shutdown_descriptor(&self) {
        let local_state = self.crdt.read().await.clone();
        let final_counter = local_state
            .get_version_vector()
            .counter(&self.pid)
            .unwrap_or(-1);
        let descriptor = ReplicaDescriptor {
            pid: self.pid,
            address: self.address.internal(),
            gc_counter: self.crdt_wrapper.current_gc_marker().counter,
            final_counter: Some(final_counter),
        };

        let descriptor_created = self
            .object_storage_client
            .write_membership_descriptor_payload(descriptor, &local_state)
            .await
            .unwrap_or_else(|error| {
                panic!("Failed to write shutdown membership descriptor: {error}")
            });
        if !descriptor_created {
            panic!("Shutdown membership descriptor already exists: {descriptor}");
        }
    }

    /// Gracefully shuts down the server, stopping all network connections and the HTTP server
    pub async fn shutdown(&mut self) {
        println!("Shutting down CRDT Server {}", self.pid);

        self.write_shutdown_descriptor().await;

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
}
