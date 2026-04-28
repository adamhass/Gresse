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
use log::{debug, info, trace};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
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
    gc_markers: Vec<GcMarker>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistentReplica<T> {
    local_state: T,
    crdt_wrapper: CRDTWrapper,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RawDurabilityRecord {
    record_type: String,
    payload: serde_json::Value,
}

#[derive(Debug, Clone)]
struct DurabilityJournal {
    path: PathBuf,
}

impl DurabilityJournal {
    fn new(path: PathBuf) -> Result<Self, DurabilityError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        Ok(Self { path })
    }

    fn recover<T: CRDT + Debug + Clone>(&self) -> Result<Option<PersistentReplica<T>>, DurabilityError> {
        if !self.path.exists() {
            return Ok(None);
        }

        let file = std::fs::File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut recovered: Option<PersistentReplica<T>> = None;

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            let record: RawDurabilityRecord = serde_json::from_str(&line)?;
            match record.record_type.as_str() {
                "snapshot" => {
                    recovered = Some(serde_json::from_value(record.payload)?);
                }
                "delta_group" => {
                    let Some(snapshot) = recovered.as_mut() else {
                        return Err(DurabilityError::MissingSnapshot);
                    };
                    let delta_group = serde_json::from_value(record.payload)?;
                    snapshot.local_state.merge_delta_group(delta_group);
                }
                other => return Err(DurabilityError::UnknownRecordType(other.to_string())),
            }
        }

        Ok(recovered)
    }

    fn append_snapshot<T: CRDT + Debug + Clone>(
        &self,
        snapshot: &PersistentReplica<T>,
    ) -> Result<(), DurabilityError> {
        self.append_record(&RawDurabilityRecord {
            record_type: "snapshot".to_string(),
            payload: serde_json::to_value(snapshot)?,
        })
    }

    fn append_delta_group<T: CRDT + Debug + Clone>(
        &self,
        delta_group: &DeltaGroup<T::Delta>,
    ) -> Result<(), DurabilityError> {
        self.append_record(&RawDurabilityRecord {
            record_type: "delta_group".to_string(),
            payload: serde_json::to_value(delta_group)?,
        })
    }

    fn append_record(&self, record: &RawDurabilityRecord) -> Result<(), DurabilityError> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        serde_json::to_writer(&mut file, record)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
enum DurabilityError {
    #[error("durability journal requires a snapshot before delta records")]
    MissingSnapshot,
    #[error("unknown durability record type: {0}")]
    UnknownRecordType(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

impl CRDTWrapper {
    fn new() -> Self {
        Self::default()
    }

    fn current_gc_marker(&self) -> Dot {
        self.gc_markers
            .last()
            .map(|gc_marker| gc_marker.marker)
            .unwrap_or(Dot {
                pid: STABLE_REPLICA_PID,
                counter: INITIAL_GC_COUNTER,
            })
    }

    fn current_stable(&self) -> Option<&DotSet> {
        self.gc_markers.last().map(|gc_marker| &gc_marker.stable)
    }

    fn needs_gc(&self, stable: &DotSet) -> bool {
        self.current_stable() != Some(stable)
    }

    fn push_gc_marker(&mut self, marker: Dot, stable: DotSet) {
        self.gc_markers.push(GcMarker { marker, stable });
    }

    fn gc_metadata(&self) -> Option<GcMarker> {
        self.gc_markers.last().cloned()
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
    durability_journal: Option<DurabilityJournal>,
    recovered_from_durability: bool,
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
    /// - `GRESSE_OBJECT_STORAGE_REGION`
    /// - `GRESSE_OBJECT_STORAGE_BUCKET`
    /// - `GRESSE_PERSISTENT_REPLICA_PATH`
    /// - `GRESSE_MEMBERSHIP_DIRECTORY_PATH`
    ///
    /// Optional:
    /// - `GRESSE_OBJECT_STORAGE_URL`, for S3-compatible custom endpoints such as MinIO.
    /// - `GRESSE_OBJECT_STORAGE_ACCESS_KEY`
    /// - `GRESSE_OBJECT_STORAGE_SECRET_KEY`
    /// - `GRESSE_OBJECT_STORAGE_SESSION_TOKEN`
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
        crdt: T,
        config: ReplicaConfig,
    ) -> (Self, oneshot::Sender<()>) {
        let durability_journal = config
            .durability_path
            .clone()
            .map(DurabilityJournal::new)
            .transpose()
            .unwrap_or_else(|error| panic!("Failed to initialize durability journal: {error}"));
        let recovered_replica = durability_journal
            .as_ref()
            .map(|journal| journal.recover::<T>())
            .transpose()
            .unwrap_or_else(|error| panic!("Failed to recover durable replica state: {error}"))
            .flatten();

        let mut effective_crdt = recovered_replica
            .as_ref()
            .map(|replica| replica.local_state.clone())
            .unwrap_or(crdt);
        effective_crdt.set_pid(pid);
        let crdt = Arc::new(RwLock::new(effective_crdt));
        Self::with_shared_config_internal(pid, crdt, config, durability_journal, recovered_replica)
            .await
    }

    /// Create a new CRDT server from explicit configuration and shared CRDT state.
    pub async fn with_shared_config(
        pid: Pid,
        crdt: Arc<RwLock<T>>,
        config: ReplicaConfig,
    ) -> (Self, oneshot::Sender<()>) {
        Self::with_shared_config_internal(pid, crdt, config, None, None).await
    }

    async fn with_shared_config_internal(
        pid: Pid,
        crdt: Arc<RwLock<T>>,
        config: ReplicaConfig,
        durability_journal: Option<DurabilityJournal>,
        recovered_replica: Option<PersistentReplica<T>>,
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
            debug!("ensuring metrics directory exists at {:?}", parent);
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
                crdt_wrapper: recovered_replica
                    .as_ref()
                    .map(|replica| replica.crdt_wrapper.clone())
                    .unwrap_or_else(CRDTWrapper::new),
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
                durability_journal,
                recovered_from_durability: recovered_replica.is_some(),
            },
            shutdown_sender,
        )
    }

    pub async fn run(&mut self) {
        info!(
            "replica {} initiating runtime at http={} internal={}",
            self.pid,
            self.address.http(),
            self.address.internal()
        );
        self.init().await;
        info!("replica {} startup completed", self.pid);

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
                    info!(
                        "replica {} established peer replication connection with replica {}",
                        self.pid,
                        pid
                    );
                    self.writers.insert(pid, writer);
                    // Update the gc_interval to minimize risk of simultanous attempts.
                    self.change_gc_interval();
                }
                _ = &mut shutdown_receiver => {
                    info!("shutdown signal received for replica {}", self.pid);
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
        debug!("replica {} initiating bootstrap", self.pid);
        self.push_replica_descriptor().await;
        let members = self.list_members().await;
        info!(
            "replica {} discovered {} membership descriptors during init",
            self.pid,
            members.len()
        );

        self.enqueue_network_members(&members).await;

        if self.recovered_from_durability {
            info!(
                "replica {} restored local state from durability journal before startup",
                self.pid
            );
        } else {
            match self.read_persistent_replica().await {
                Some(mut persistent_replica) => {
                    debug!("replica {} restoring persistent replica state", self.pid);
                    persistent_replica.local_state.set_pid(self.pid);
                    *self.crdt.write().await = persistent_replica.local_state;
                    self.crdt_wrapper = persistent_replica.crdt_wrapper;
                }
                None => {
                    debug!(
                        "replica {} found no persistent replica state; writing initial snapshot",
                        self.pid
                    );
                    self.write_initial_persistent_replica().await
                }
            }
        }

        self.persist_durable_snapshot().await;
    }

    async fn enqueue_network_members(&self, members: &[ReplicaDescriptor]) {
        let shutdown_pids = members
            .iter()
            .filter(|descriptor| descriptor.is_shutdown())
            .map(|descriptor| descriptor.pid)
            .collect::<HashSet<_>>();
        let connected_pids = self.writers.keys().copied().collect::<HashSet<_>>();

        for descriptor in members {
            if descriptor.pid == self.pid
                || descriptor.is_shutdown()
                || shutdown_pids.contains(&descriptor.pid)
                || connected_pids.contains(&descriptor.pid)
            {
                continue;
            }

            debug!(
                "replica {} discovering member pid={} addr={} gc_counter={}",
                self.pid,
                descriptor.pid,
                descriptor.address,
                descriptor.gc_counter
            );

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
        trace!(
            "replica {} polling membership directory saw {} descriptors",
            self.pid,
            members.len()
        );
        let previous_writer_count = self.writers.len();
        debug!(
            "replica {} scanning membership directory for new members and shutdown descriptors",
            self.pid
        );
        self.enqueue_network_members(&members).await;
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
        info!(
            "replica {} observed shutdown descriptor for replica {} with final counter {:?}",
            self.pid,
            descriptor.pid,
            descriptor.final_counter
        );
        self.merge_shutdown_payload(descriptor).await;
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

    async fn merge_shutdown_payload(&mut self, descriptor: ReplicaDescriptor) {
        let Some(shutdown_state) = self
            .object_storage_client
            .read_membership_descriptor_payload::<T>(descriptor)
            .await
            .unwrap_or_else(|error| {
                panic!("Failed to read shutdown membership descriptor payload: {error}")
            })
        else {
            return;
        };

        let local_version_vector = { self.crdt.read().await.get_version_vector().clone() };
        let (delta, _, _) = shutdown_state.get_delta(&local_version_vector);
        if delta.list.is_empty() {
            let current_version_vector = { self.crdt.read().await.get_version_vector().clone() };
            self.crdt_wrapper
                .version_matrix
                .update(self.pid, current_version_vector);
            debug!(
                "replica {} found no missing shutdown deltas for replica {}",
                self.pid,
                descriptor.pid
            );
            return;
        }

        debug!(
            "replica {} merging {} shutdown deltas from replica {}",
            self.pid,
            delta.list.len(),
            descriptor.pid
        );
        self.persist_delta_before_apply(&delta);
        self.crdt.write().await.merge_delta_group(delta);
        let current_version_vector = { self.crdt.read().await.get_version_vector().clone() };
        self.crdt_wrapper
            .version_matrix
            .update(self.pid, current_version_vector);
    }

    async fn handle_client_request(
        &mut self,
        mutation: T::Mutation,
        responder: ClientResponder<T>,
    ) {
        debug!("replica {} handling client mutation", self.pid);
        let received = now_micros();
        let (start, response) = {
            let mut writable = self.crdt.write().await;
            let version_vector_before = writable.get_version_vector().clone();
            let mut candidate = writable.clone();
            let response = candidate.mutate(mutation);
            let (delta_group, _, _) = candidate.get_delta(&version_vector_before);
            self.persist_delta_before_apply(&delta_group);
            let start = now_micros();
            writable.merge_delta_group(delta_group);
            (start, response)
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
                let delta_len = delta.list.len();
                self.persist_delta_before_apply(&delta);
                let (start, (insert_count, delete_count)) = {
                    let mut writable = self.crdt.write().await;
                    let start = now_micros();
                    (start, writable.merge_delta_group(delta))
                };
                debug!(
                    "replica {} merging remote delta group with {} entries",
                    self.pid,
                    delta_len
                );
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
                debug!(
                    "replica {} get_delta for replica {} produced {} deltas",
                    self.pid,
                    pid,
                    delta.list.len()
                );
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
            debug!(
                "replica {} initiating pull_delta with {} connected peers",
                self.pid,
                self.writers.len()
            );
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
        debug!(
            "replica {} initiating gc with stable frontier {:?}",
            self.pid,
            stable
        );

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
            debug!(
                "replica {} aborted gc round because membership changed",
                self.pid
            );
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
        info!(
            "replica {} gc departed_pids={:?} updated_stable={:?}",
            self.pid,
            departed_pids,
            stable
        );
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
        self.persist_durable_snapshot_with(persistent_replica.clone());
        info!("replica {} completed gc round", self.pid);
        
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

    async fn observe_gc(&mut self, metadata: GcMarker) {
        if metadata.marker.pid != STABLE_REPLICA_PID
            || metadata.marker.counter <= self.crdt_wrapper.current_gc_marker().counter
        {
            return;
        }

        debug!(
            "replica {} observing gc marker {} with stable frontier {:?}",
            self.pid,
            metadata.marker.counter,
            metadata.stable
        );
        self.crdt.write().await.gc(metadata.stable.clone(), None);
        self.crdt_wrapper
            .push_gc_marker(metadata.marker, metadata.stable.clone());
        self.persist_durable_snapshot().await;
        self.change_own_gc_counter(metadata.marker.counter).await;
        self.gc_interval.reset();
    }

    fn gc_metadata(&self) -> Option<GcMarker> {
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
        info!(
            "replica {} shutting down with {} connected writers",
            self.pid,
            self.writers.len()
        );

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

        debug!("replica {} shutdown complete", self.pid);
    }

    /// Get a shutdown handle that can be used to trigger server shutdown
    pub fn get_shutdown_handle(&mut self) -> oneshot::Sender<()> {
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
        self.shutdown_receiver = Some(shutdown_receiver);
        shutdown_sender
    }

    async fn persist_durable_snapshot(&self) {
        let snapshot = PersistentReplica {
            local_state: self.crdt.read().await.clone(),
            crdt_wrapper: self.crdt_wrapper.clone(),
        };
        self.persist_durable_snapshot_with(snapshot);
    }

    fn persist_durable_snapshot_with(&self, snapshot: PersistentReplica<T>) {
        let Some(journal) = &self.durability_journal else {
            return;
        };
        journal
            .append_snapshot(&snapshot)
            .unwrap_or_else(|error| panic!("Failed to append durable snapshot: {error}"));
    }

    fn persist_delta_before_apply(&self, delta_group: &DeltaGroup<T::Delta>) {
        let Some(journal) = &self.durability_journal else {
            return;
        };
        journal
            .append_delta_group::<T>(delta_group)
            .unwrap_or_else(|error| panic!("Failed to append durable delta group: {error}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orset::{ORSet, OrSetMutation, OrSetQuery, OrSetResponse};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_journal_path(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock drifted before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("gresse-{name}-{unique}.jsonl"))
    }

    #[test]
    fn durability_journal_recovers_snapshot_and_delta_groups() {
        let journal_path = temp_journal_path("durability");
        let journal = DurabilityJournal::new(journal_path.clone())
            .expect("failed to create durability journal");

        let mut base = ORSet::<String>::new();
        base.set_pid(1);
        base.mutate(OrSetMutation::Insert("apple".into())).unwrap();

        journal
            .append_snapshot(&PersistentReplica {
                local_state: base.clone(),
                crdt_wrapper: CRDTWrapper::new(),
            })
            .expect("failed to append durability snapshot");

        let version_vector_before = base.get_version_vector().clone();
        let mut candidate = base.clone();
        candidate
            .mutate(OrSetMutation::Insert("banana".into()))
            .unwrap();
        let (delta_group, _, _) = candidate.get_delta(&version_vector_before);

        journal
            .append_delta_group::<ORSet<String>>(&delta_group)
            .expect("failed to append durability delta group");

        let recovered = journal
            .recover::<ORSet<String>>()
            .expect("failed to recover durability journal")
            .expect("expected recovered replica");

        assert_eq!(
            recovered
                .local_state
                .query(OrSetQuery::Elements)
                .expect("failed to query recovered ORSet"),
            OrSetResponse::Elements(vec!["apple".into(), "banana".into()])
        );

        std::fs::remove_file(journal_path).expect("failed to clean up durability journal");
    }
}
