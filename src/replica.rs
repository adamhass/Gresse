use crate::http_server::{launch_http_server, ClientMutationHandler};
use crate::journal::Journal;
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
use log::{debug, info, trace, warn};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Instant, Interval};

const STABLE_REPLICA_PID: Pid = 0;
const INITIAL_GC_COUNTER: Counter = 0;
const DEFAULT_GC_INTERVAL: Duration = Duration::from_secs(60);

/// Return one current, connectable descriptor per peer PID.
///
/// Membership descriptors are immutable and a replica publishes a new one
/// whenever its GC marker changes.  Sending every historical descriptor to
/// the network manager creates redundant connection work and can overflow its
/// bounded discovery channel during churn.  A final descriptor retires the
/// entire PID, including any older non-final descriptors that remain listed.
fn network_connection_candidates<'a>(
    members: &'a [ReplicaDescriptor],
    local_pid: Pid,
    connected_pids: &HashSet<Pid>,
) -> Vec<&'a ReplicaDescriptor> {
    let shutdown_pids = members
        .iter()
        .filter(|descriptor| descriptor.is_shutdown())
        .map(|descriptor| descriptor.pid)
        .collect::<HashSet<_>>();
    let mut newest_by_pid = HashMap::<Pid, &ReplicaDescriptor>::new();

    for descriptor in members {
        if descriptor.pid == local_pid
            || descriptor.is_shutdown()
            || shutdown_pids.contains(&descriptor.pid)
            || connected_pids.contains(&descriptor.pid)
        {
            continue;
        }
        newest_by_pid
            .entry(descriptor.pid)
            .and_modify(|current| {
                if descriptor.gc_counter > current.gc_counter {
                    *current = descriptor;
                }
            })
            .or_insert(descriptor);
    }

    let mut candidates = newest_by_pid.into_values().collect::<Vec<_>>();
    candidates.sort_by_key(|descriptor| descriptor.pid);
    candidates
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct CRDTWrapper {
    version_matrix: VersionMatrix,
    gc_markers: Vec<GcMarker>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PersistentReplica<T> {
    pub(crate) local_state: T,
    pub(crate) crdt_wrapper: CRDTWrapper,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartupMetrics {
    pub crdt_pid_set_us: u128,
    pub http_server_ready_us: u128,
    pub network_manager_ready_us: u128,
    pub metric_writer_ready_us: u128,
    pub object_storage_client_start_us: u128,
    pub object_storage_client_ready_us: u128,
    pub replica_run_start_us: u128,
    pub replica_init_start_us: u128,
    pub persistent_state_fetch_completed_us: u128,
    pub membership_descriptor_write_completed_us: u128,
    pub membership_directory_read_completed_us: u128,
    pub replica_init_completed_us: u128,
}

#[derive(Debug, Clone, Copy)]
struct StartupConstructionMetrics {
    crdt_pid_set_us: u128,
    http_server_ready_us: u128,
    network_manager_ready_us: u128,
    metric_writer_ready_us: u128,
    object_storage_client_start_us: u128,
    object_storage_client_ready_us: u128,
}

struct TimedResult<T> {
    value: T,
    start_us: u128,
    end_us: u128,
    detail: String,
}

struct MembershipInitResult {
    descriptor_write: TimedResult<()>,
    membership_list: TimedResult<()>,
    members: Vec<ReplicaDescriptor>,
}

#[derive(Debug, Serialize)]
struct MetricRecord {
    source: String,
    event: String,
    phase: String,
    timestamp_us: u128,
    replica_pid: Pid,
    peer_pid: Option<Pid>,
    gc_marker: Option<Counter>,
    sent_us: Option<u128>,
    received_us: Option<u128>,
    start_us: Option<u128>,
    end_us: Option<u128>,
    insert_count: Option<u16>,
    delete_count: Option<u16>,
    detail: Option<String>,
    client_id: Option<String>,
    operation: Option<String>,
    value: Option<i32>,
    status_code: Option<u16>,
    latency_us: Option<u128>,
}

#[derive(Debug, Clone, Copy)]
struct ClientMutationMetric {
    received_us: u128,
    start_us: u128,
    end_us: u128,
}

struct MembershipPollResult {
    start_us: u128,
    end_us: u128,
    members: Result<Vec<ReplicaDescriptor>, String>,
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
    /// Local listener address.
    address: ServerAddr,
    /// Routable listener address written to membership descriptors.
    advertised_address: ServerAddr,
    // Config:
    sync_interval: Duration,
    gc_base_interval: Duration,
    gc_interval: Interval,
    membership_poll_interval: Interval,
    // Network Stuff:
    writers: HashMap<Pid, Sender<ReplicaMessage<T>>>,
    /// Serializes durable CRDT commits initiated by the HTTP data plane and
    /// replica-side replication handling.
    commit_gate: Arc<Mutex<()>>,
    client_mutation_metric_receiver: UnboundedReceiver<ClientMutationMetric>,
    replication_receiver: Receiver<ReplicaMessage<T>>,
    // Allows new connections to be established:
    replication_writer_receiver: Receiver<(Pid, Sender<ReplicaMessage<T>>)>,
    network_member_sender: Sender<NetworkMember>,
    // Records results/metrics
    metric_writer: csv::Writer<std::fs::File>,
    object_storage_client: Arc<ObjectStorageClient>,
    membership_poll_in_flight: bool,
    membership_poll_sender: UnboundedSender<MembershipPollResult>,
    membership_poll_receiver: UnboundedReceiver<MembershipPollResult>,
    membership_poll_task: Option<tokio::task::JoinHandle<()>>,
    durability_journal: Option<Arc<Journal>>,
    recovered_from_durability: bool,
    recovered_predecessor_pid: Option<Pid>,
    startup_construction_metrics: StartupConstructionMetrics,
    replica_run_start_us: Option<u128>,
    startup_metrics_sender: Option<oneshot::Sender<StartupMetrics>>,
    // Shutdown channel
    shutdown_receiver: Option<oneshot::Receiver<()>>,
    http_shutdown_sender: Option<oneshot::Sender<()>>,
    network_start_sender: Option<oneshot::Sender<()>>,
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
    /// - `GRESSE_PERSISTENT_REPLICA_PATH`
    /// - `GRESSE_MEMBERSHIP_DIRECTORY_PATH`
    ///
    /// Optional:
    /// - `GRESSE_OBJECT_STORAGE_LOCAL_DIR`, to use a shared local directory instead of S3/MinIO.
    /// - `GRESSE_OBJECT_STORAGE_REGION`, required for S3/MinIO and defaults to `us-east-1`.
    /// - `GRESSE_OBJECT_STORAGE_BUCKET`, required for S3/MinIO and defaults to `gresse`.
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
            .map(Journal::new)
            .transpose()
            .map(|journal| journal.map(Arc::new))
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
        durability_journal: Option<Arc<Journal>>,
        recovered_replica: Option<PersistentReplica<T>>,
    ) -> (Self, oneshot::Sender<()>) {
        crdt.write().await.set_pid(pid);
        let crdt_pid_set_us = now_micros();

        // The HTTP data plane applies local mutations directly.  It must not
        // wait behind membership, GC, or anti-entropy work in Replica::run.
        // The shared gate preserves durable-before-apply ordering with remote
        // delta merges.
        let commit_gate = Arc::new(Mutex::new(()));
        let mutation_crdt = crdt.clone();
        let mutation_journal = durability_journal.clone();
        let mutation_commit_gate = commit_gate.clone();
        let (client_mutation_metric_sender, client_mutation_metric_receiver) = unbounded_channel();
        let mutation_handler: ClientMutationHandler<T> = Arc::new(move |mutation| {
            let crdt = mutation_crdt.clone();
            let journal = mutation_journal.clone();
            let commit_gate = mutation_commit_gate.clone();
            let metric_sender = client_mutation_metric_sender.clone();
            Box::pin(async move {
                let received_us = now_micros();
                let _commit = commit_gate.lock().await;
                let start_us = now_micros();
                if let Some(journal) = &journal {
                    journal
                        .append_mutation::<T>(&mutation)
                        .unwrap_or_else(|error| {
                            panic!("Failed to append durable client mutation: {error}")
                        });
                }
                let response = crdt.write().await.mutate(mutation);
                let end_us = now_micros();
                let _ = metric_sender.send(ClientMutationMetric {
                    received_us,
                    start_us,
                    end_us,
                });
                response
            })
        });
        let http_shutdown_sender =
            launch_http_server::<T>(&config.address, crdt.clone(), mutation_handler).await;
        let http_server_ready_us = now_micros();

        // Launch NetworkManager with shutdown capability
        let (
            replication_receiver,
            replication_writer_receiver,
            network_member_sender,
            network_start_sender,
            network_shutdown_sender,
        ) = NetworkManager::<ReplicaMessage<T>>::launch_network_manager(config.address, pid).await;
        let network_manager_ready_us = now_micros();

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
        let metric_writer_ready_us = now_micros();
        let membership_poll_period = config.object_storage_config.discovery_interval;
        let object_storage_client_start_us = now_micros();
        let object_storage_client = Arc::new(
            ObjectStorageClient::new(config.object_storage_config).unwrap_or_else(|error| {
                panic!("Failed to initialize object storage client: {error}")
            }),
        );
        let object_storage_client_ready_us = now_micros();
        let (membership_poll_sender, membership_poll_receiver) = unbounded_channel();

        // Create shutdown channel for the CRDT server itself
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();

        (
            Replica {
                pid,
                address: config.address,
                advertised_address: config.advertised_address,
                crdt,
                commit_gate,
                client_mutation_metric_receiver,
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
                network_start_sender: Some(network_start_sender),
                network_shutdown_sender: Some(network_shutdown_sender),
                sync_interval: config.sync_interval,
                gc_base_interval: DEFAULT_GC_INTERVAL,
                gc_interval: Self::gc_interval(DEFAULT_GC_INTERVAL),
                membership_poll_interval: Self::membership_poll_interval(membership_poll_period),
                metric_writer,
                object_storage_client,
                membership_poll_in_flight: false,
                membership_poll_sender,
                membership_poll_receiver,
                membership_poll_task: None,
                durability_journal,
                recovered_from_durability: recovered_replica.is_some(),
                recovered_predecessor_pid: recovered_replica
                    .as_ref()
                    .and(config.recovered_predecessor_pid)
                    .filter(|predecessor_pid| *predecessor_pid != pid),
                startup_construction_metrics: StartupConstructionMetrics {
                    crdt_pid_set_us,
                    http_server_ready_us,
                    network_manager_ready_us,
                    metric_writer_ready_us,
                    object_storage_client_start_us,
                    object_storage_client_ready_us,
                },
                replica_run_start_us: None,
                startup_metrics_sender: None,
            },
            shutdown_sender,
        )
    }

    pub async fn run(&mut self) {
        let replica_run_start_us = now_micros();
        self.replica_run_start_us = Some(replica_run_start_us);
        info!(
            "replica {} initiating runtime at bind_http={} bind_internal={} advertised_internal={}",
            self.pid,
            self.address.http(),
            self.address.internal(),
            self.advertised_address.internal(),
        );
        let initial_members = self.init().await;
        info!("replica {} startup completed", self.pid);
        // Membership registration and the initial directory read are part of
        // bootstrap.  Establishing peer sockets is deliberately deferred
        // until that bootstrap is complete: connection attempts can trigger
        // inbound replication and must not delay readiness.
        if let Some(network_start_sender) = self.network_start_sender.take() {
            let _ = network_start_sender.send(());
        }
        self.enqueue_network_members(&initial_members);

        // Take ownership of shutdown_receiver from self
        let mut shutdown_receiver = self
            .shutdown_receiver
            .take()
            .expect("Failed to take shutdown receiver");

        let mut interval = tokio::time::interval(self.sync_interval);
        loop {
            tokio::select! {
                Some(metric) = self.client_mutation_metric_receiver.recv() => {
                    self.metric_span(
                        "client_mutation",
                        "completed",
                        None,
                        None,
                        metric.start_us,
                        metric.end_us,
                        None,
                        None,
                        Some(metric.received_us),
                        Some("http_data_plane".to_string()),
                    );
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
                    self.start_membership_poll();
                }
                Some(result) = self.membership_poll_receiver.recv() => {
                    self.apply_membership_poll(result).await;
                }
                Some((pid, writer)) = self.replication_writer_receiver.recv() => {
                    info!(
                        "replica {} established peer replication connection with replica {}",
                        self.pid,
                        pid
                    );
                    self.writers.insert(pid, writer);
                    self.metric_instant(
                        "peer_replication_connection",
                        "established",
                        Some(pid),
                        None,
                        None,
                    );
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

    async fn init(&mut self) -> Vec<ReplicaDescriptor> {
        debug!("replica {} initiating bootstrap", self.pid);
        let replica_init_start_us = self.metric_instant("replica_init", "start", None, None, None);
        let descriptor = self.replica_descriptor().await;
        let membership_init;
        let persistent_state_fetch_completed_us;
        let mut restored_from_storage = false;

        if self.recovered_from_durability {
            membership_init = Self::register_and_list_members_during_init(
                &self.object_storage_client,
                descriptor,
            )
            .await;
            if let Some(predecessor_pid) = self.recovered_predecessor_pid {
                // The lifecycle controller has confirmed the predecessor is
                // gone.  Once this replacement is registered, its old
                // descriptors are no longer needed and would otherwise
                // accumulate forever after crash recovery.
                match self
                    .object_storage_client
                    .delete_membership_descriptors_for_pid(predecessor_pid)
                    .await
                {
                    Ok(()) => {
                        self.metric_instant(
                            "recovered_membership_cleanup",
                            "completed",
                            Some(predecessor_pid),
                            None,
                            None,
                        );
                    }
                    Err(error) => {
                        warn!(
                            "replica {} could not remove recovered predecessor {} descriptors: {}",
                            self.pid, predecessor_pid, error
                        );
                        self.metric_instant(
                            "recovered_membership_cleanup",
                            "failed",
                            Some(predecessor_pid),
                            None,
                            Some(error.to_string()),
                        );
                    }
                }
            }
            self.record_membership_init_metrics(&membership_init);
            info!(
                "replica {} restored local state from durability journal before startup",
                self.pid
            );
            persistent_state_fetch_completed_us = self.metric_instant(
                "persistent_state_fetch",
                "completed",
                None,
                None,
                Some("recovered_from_durability_journal".to_string()),
            );
            info!(
                "replica {} discovered {} membership descriptors during init",
                self.pid,
                membership_init.members.len()
            );
        } else {
            let (persistent_replica_result, concurrent_membership_init) = tokio::join!(
                Self::timed_storage_operation(
                    Self::read_persistent_replica_from_storage(&self.object_storage_client),
                    |result| {
                        if result.is_some() {
                            "restored_existing_snapshot".to_string()
                        } else {
                            "persistent_snapshot_missing".to_string()
                        }
                    }
                ),
                Self::register_and_list_members_during_init(
                    &self.object_storage_client,
                    descriptor
                )
            );

            self.metric_span(
                "persistent_state_fetch",
                "completed",
                None,
                None,
                persistent_replica_result.start_us,
                persistent_replica_result.end_us,
                None,
                None,
                None,
                Some(persistent_replica_result.detail),
            );
            persistent_state_fetch_completed_us = persistent_replica_result.end_us;
            membership_init = concurrent_membership_init;
            self.record_membership_init_metrics(&membership_init);
            restored_from_storage = persistent_replica_result.value.is_some();

            match persistent_replica_result.value {
                Some((mut persistent_replica, serialized_snapshot)) => {
                    debug!("replica {} restoring persistent replica state", self.pid);
                    persistent_replica.local_state.set_pid(self.pid);
                    *self.crdt.write().await = persistent_replica.local_state;
                    self.crdt_wrapper = persistent_replica.crdt_wrapper;
                    self.persist_downloaded_durable_snapshot(&serialized_snapshot);
                }
                None => {
                    debug!(
                        "replica {} found no persistent replica state; writing initial snapshot",
                        self.pid
                    );
                    if self.write_initial_persistent_replica().await {
                        self.metric_instant(
                            "persistent_state_fetch",
                            "completed",
                            None,
                            None,
                            Some("initialized_new_persistent_snapshot".to_string()),
                        );
                    }
                }
            }
            info!(
                "replica {} discovered {} membership descriptors during init",
                self.pid,
                membership_init.members.len()
            );
        }

        // A journal recovery has already reconstructed the latest durable
        // state. Re-appending that same full snapshot during bootstrap only
        // delays readiness and grows the journal; subsequent mutations and
        // GC operations continue to append their normal durable records.
        if !self.recovered_from_durability && !restored_from_storage {
            self.persist_durable_snapshot().await;
        }
        let replica_init_completed_us =
            self.metric_instant("replica_init", "completed", None, None, None);
        if let Some(startup_metrics_sender) = self.startup_metrics_sender.take() {
            let _ = startup_metrics_sender.send(StartupMetrics {
                crdt_pid_set_us: self.startup_construction_metrics.crdt_pid_set_us,
                http_server_ready_us: self.startup_construction_metrics.http_server_ready_us,
                network_manager_ready_us: self
                    .startup_construction_metrics
                    .network_manager_ready_us,
                metric_writer_ready_us: self.startup_construction_metrics.metric_writer_ready_us,
                object_storage_client_start_us: self
                    .startup_construction_metrics
                    .object_storage_client_start_us,
                object_storage_client_ready_us: self
                    .startup_construction_metrics
                    .object_storage_client_ready_us,
                replica_run_start_us: self
                    .replica_run_start_us
                    .expect("replica_run_start_us should be set before init"),
                replica_init_start_us,
                persistent_state_fetch_completed_us,
                membership_descriptor_write_completed_us: membership_init.descriptor_write.end_us,
                membership_directory_read_completed_us: membership_init.membership_list.end_us,
                replica_init_completed_us,
            });
        }

        membership_init.members
    }

    async fn timed_storage_operation<F, R, D>(future: F, detail_fn: D) -> TimedResult<R>
    where
        F: Future<Output = R>,
        D: FnOnce(&R) -> String,
    {
        let start_us = now_micros();
        let value = future.await;
        let end_us = now_micros();
        let detail = detail_fn(&value);
        TimedResult {
            value,
            start_us,
            end_us,
            detail,
        }
    }

    async fn read_persistent_replica_from_storage(
        object_storage_client: &ObjectStorageClient,
    ) -> Option<(PersistentReplica<T>, Vec<u8>)> {
        object_storage_client
            .read_persistent_replica_with_serialized_data()
            .await
            .unwrap_or_else(|error| panic!("Failed to read persistent replica state: {error}"))
    }

    async fn register_and_list_members_during_init(
        object_storage_client: &ObjectStorageClient,
        descriptor: ReplicaDescriptor,
    ) -> MembershipInitResult {
        let descriptor_write = Self::timed_storage_operation(
            object_storage_client.write_membership_descriptor(descriptor),
            |_| descriptor.to_string(),
        )
        .await;
        descriptor_write
            .value
            .unwrap_or_else(|error| panic!("Failed to create replica descriptor: {error}"));
        let descriptor_write = TimedResult {
            value: (),
            start_us: descriptor_write.start_us,
            end_us: descriptor_write.end_us,
            detail: descriptor_write.detail,
        };

        let members = Self::timed_storage_operation(
            object_storage_client.list_membership_descriptors(),
            |members| {
                format!(
                    "init:{} descriptors",
                    members.as_ref().map(Vec::len).unwrap_or(0)
                )
            },
        )
        .await;
        let descriptors = members
            .value
            .unwrap_or_else(|error| panic!("Failed to list replica membership: {error}"));

        MembershipInitResult {
            descriptor_write,
            membership_list: TimedResult {
                value: (),
                start_us: members.start_us,
                end_us: members.end_us,
                detail: members.detail,
            },
            members: descriptors,
        }
    }

    fn record_membership_init_metrics(&mut self, membership_init: &MembershipInitResult) {
        self.metric_span(
            "membership_descriptor_write",
            "completed",
            None,
            None,
            membership_init.descriptor_write.start_us,
            membership_init.descriptor_write.end_us,
            None,
            None,
            None,
            Some(membership_init.descriptor_write.detail.clone()),
        );
        self.metric_span(
            "membership_directory_read",
            "completed",
            None,
            None,
            membership_init.membership_list.start_us,
            membership_init.membership_list.end_us,
            None,
            None,
            None,
            Some(membership_init.membership_list.detail.clone()),
        );
    }

    fn enqueue_network_members(&self, members: &[ReplicaDescriptor]) {
        let connected_pids = self.writers.keys().copied().collect::<HashSet<_>>();

        for descriptor in network_connection_candidates(members, self.pid, &connected_pids) {
            debug!(
                "replica {} discovering member pid={} addr={} gc_counter={}",
                self.pid, descriptor.pid, descriptor.address, descriptor.gc_counter
            );

            if let Err(error) = self.network_member_sender.try_send(NetworkMember {
                pid: descriptor.pid,
                address: descriptor.address,
                final_counter: descriptor.final_counter,
            }) {
                // Discovery is periodic.  A full network-manager queue means
                // there is already sufficient pending work; retry this peer
                // on the next membership poll without stalling replication,
                // GC, or client-visible state transitions.
                warn!(
                    "replica {} deferred discovery of peer {}: {}",
                    self.pid, descriptor.pid, error
                );
            }
        }
    }

    async fn write_initial_persistent_replica(&mut self) -> bool {
        let persistent_replica = PersistentReplica {
            local_state: self.crdt.read().await.clone(),
            crdt_wrapper: self.crdt_wrapper.clone(),
        };
        let start = now_micros();
        if let Err(error) = self
            .object_storage_client
            .write_persistent_replica(&persistent_replica)
            .await
        {
            warn!(
                "replica {} could not write initial persistent replica state: {}",
                self.pid, error
            );
            self.metric_span(
                "persistent_state_write",
                "failed",
                None,
                None,
                start,
                now_micros(),
                None,
                None,
                None,
                Some(error.to_string()),
            );
            return false;
        }
        let end = now_micros();
        self.metric_span(
            "persistent_state_write",
            "completed",
            None,
            None,
            start,
            end,
            None,
            None,
            None,
            Some("initial_snapshot".to_string()),
        );
        true
    }

    async fn replica_descriptor(&self) -> ReplicaDescriptor {
        ReplicaDescriptor {
            pid: self.pid,
            address: self.advertised_address.internal(),
            gc_counter: self.crdt_wrapper.current_gc_marker().counter,
            final_counter: None,
        }
    }

    async fn list_members(
        &mut self,
        detail: &'static str,
        gc_marker: Option<Counter>,
    ) -> Option<Vec<ReplicaDescriptor>> {
        let start = now_micros();
        let members = match self
            .object_storage_client
            .list_membership_descriptors()
            .await
        {
            Ok(members) => members,
            Err(error) => {
                self.metric_span(
                    "membership_directory_read",
                    "failed",
                    None,
                    gc_marker,
                    start,
                    now_micros(),
                    None,
                    None,
                    None,
                    Some(format!("{detail}: {error}")),
                );
                warn!("replica {} could not list membership: {}", self.pid, error);
                return None;
            }
        };
        let end = now_micros();
        self.metric_span(
            "membership_directory_read",
            "completed",
            None,
            gc_marker,
            start,
            end,
            None,
            None,
            None,
            Some(format!("{detail}:{} descriptors", members.len())),
        );
        Some(members)
    }

    fn start_membership_poll(&mut self) {
        if self.membership_poll_in_flight {
            debug!(
                "replica {} skipped membership poll; previous poll is still in flight",
                self.pid
            );
            return;
        }
        self.membership_poll_in_flight = true;
        let object_storage_client = self.object_storage_client.clone();
        let result_sender = self.membership_poll_sender.clone();
        self.membership_poll_task = Some(tokio::spawn(async move {
            let start_us = now_micros();
            let members = object_storage_client
                .fetch_membership_descriptors()
                .await
                .map_err(|error| error.to_string());
            let end_us = now_micros();
            let _ = result_sender.send(MembershipPollResult {
                start_us,
                end_us,
                members,
            });
        }));
    }

    async fn apply_membership_poll(&mut self, result: MembershipPollResult) {
        self.membership_poll_in_flight = false;
        self.membership_poll_task.take();
        let members = match result.members {
            Ok(members) => members,
            Err(error) => {
                self.metric_span(
                    "membership_directory_read",
                    "failed",
                    None,
                    None,
                    result.start_us,
                    result.end_us,
                    None,
                    None,
                    None,
                    Some(error.clone()),
                );
                warn!("replica {} membership poll failed: {}", self.pid, error);
                return;
            }
        };
        self.object_storage_client
            .update_membership_cache(members.clone())
            .await;
        self.metric_span(
            "membership_directory_read",
            "completed",
            None,
            None,
            result.start_us,
            result.end_us,
            None,
            None,
            None,
            Some(format!("poll:{} descriptors", members.len())),
        );
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
        self.enqueue_network_members(&members);
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
        let final_dot = Dot {
            pid: descriptor.pid,
            counter: descriptor
                .final_counter
                .expect("shutdown descriptor missing final counter"),
        };
        if self
            .crdt_wrapper
            .version_matrix
            .contains_final_dot(final_dot)
        {
            trace!(
                "replica {} already processed shutdown descriptor for replica {} with final counter {}",
                self.pid,
                descriptor.pid,
                final_dot.counter,
            );
            return;
        }
        info!(
            "replica {} observed shutdown descriptor for replica {} with final counter {:?}",
            self.pid, descriptor.pid, descriptor.final_counter
        );
        self.merge_shutdown_payload(descriptor).await;
        // Keep the replica row until its final dot is stable everywhere.
        self.crdt_wrapper.version_matrix.insert_final_dot(final_dot);

        // Remove the writers
        let _ = self.writers.remove(&descriptor.pid);
        // Notify the network manager
        if let Err(error) = self.network_member_sender.try_send(NetworkMember {
            pid: descriptor.pid,
            address: descriptor.address,
            final_counter: descriptor.final_counter,
        }) {
            // Membership polling will repeat this notification.  Never hold
            // the replica's event loop behind a congested network manager.
            warn!(
                "replica {} deferred shutdown notification for {}: {}",
                self.pid, descriptor.pid, error
            );
        }
    }

    async fn merge_shutdown_payload(&mut self, descriptor: ReplicaDescriptor) {
        let shutdown_state = match self
            .object_storage_client
            .read_membership_descriptor_payload::<T>(descriptor)
            .await
        {
            Ok(state) => state,
            Err(error) => {
                warn!(
                    "replica {} could not read shutdown payload for {}: {}",
                    self.pid, descriptor.pid, error
                );
                return;
            }
        };
        let Some(shutdown_state) = shutdown_state else {
            return;
        };

        let local_version_vector = { self.crdt.read().await.get_version_vector().clone() };
        let delta = shutdown_state.get_delta(&local_version_vector);
        if delta.list.is_empty() {
            let current_version_vector = { self.crdt.read().await.get_version_vector().clone() };
            self.crdt_wrapper
                .version_matrix
                .update(self.pid, current_version_vector);
            debug!(
                "replica {} found no missing shutdown deltas for replica {}",
                self.pid, descriptor.pid
            );
            return;
        }

        debug!(
            "replica {} merging {} shutdown deltas from replica {}",
            self.pid,
            delta.list.len(),
            descriptor.pid
        );
        let _commit = self.commit_gate.lock().await;
        self.persist_delta_before_apply(&delta);
        self.crdt.write().await.merge_delta_group(delta);
        let current_version_vector = { self.crdt.read().await.get_version_vector().clone() };
        self.crdt_wrapper
            .version_matrix
            .update(self.pid, current_version_vector);
    }

    async fn handle_remote_event(&mut self, event: ReplicaMessage<T>) {
        let received = now_micros();
        match event {
            ReplicaMessage::<T>::DeltaGroup(delta, gc_metadata, sent) => {
                let gc_marker = gc_metadata.as_ref().map(|marker| marker.marker.counter);
                if let Some(gc_metadata) = gc_metadata {
                    self.observe_gc(gc_metadata).await;
                }
                let delta_len = delta.list.len();
                let (start, end) = {
                    let commit_gate = self.commit_gate.clone();
                    let _commit = commit_gate.lock().await;
                    self.persist_delta_before_apply(&delta);
                    let start = now_micros();
                    let mut writable = self.crdt.write().await;
                    writable.merge_delta_group(delta);
                    let end = now_micros();
                    (start, end)
                };
                self.metric_span(
                    "peer_replication_merge_delta",
                    "completed",
                    None,
                    gc_marker,
                    start,
                    end,
                    None,
                    None,
                    Some(received),
                    Some(format!("delta_len={delta_len},sent_us={sent}")),
                );
                debug!(
                    "replica {} merging remote delta group with {} entries",
                    self.pid, delta_len
                );
            }
            ReplicaMessage::<T>::VersionVector(pid, vv, sent) => {
                self.crdt_wrapper.version_matrix.update(pid, vv.clone());
                let (start, delta) = {
                    let readable = self.crdt.read().await;
                    let start = now_micros();
                    (start, readable.get_delta(&vv))
                };
                let delta_len = delta.list.len();
                debug!(
                    "replica {} get_delta for replica {} produced {} deltas",
                    self.pid, pid, delta_len
                );
                let end = now_micros();
                let gc_metadata = self.gc_metadata();
                let gc_marker = gc_metadata.as_ref().map(|marker| marker.marker.counter);
                let send_result = self.writers.get(&pid).map(|writer| {
                    writer.try_send(ReplicaMessage::<T>::DeltaGroup(delta, gc_metadata, end))
                });
                match send_result {
                    Some(Ok(())) => {}
                    Some(Err(error)) => {
                        if matches!(error, tokio::sync::mpsc::error::TrySendError::Closed(_)) {
                            self.writers.remove(&pid);
                        }
                        // Anti-entropy is periodic: dropping one response when
                        // a peer's bounded queue is full is preferable to
                        // blocking every client request behind that peer.
                        warn!(
                            "replica {} deferred delta response to peer {}: {}",
                            self.pid, pid, error
                        );
                        return;
                    }
                    None => {
                        debug!("replica {} has no writer for peer {}; response will be retried by anti-entropy", self.pid, pid);
                        return;
                    }
                }
                self.metric_span(
                    "peer_replication_get_delta",
                    "completed",
                    Some(pid),
                    gc_marker,
                    start,
                    end,
                    None,
                    None,
                    Some(received),
                    Some(format!("delta_len={delta_len},sent_us={sent}")),
                );
            }
        }
    }

    async fn pull_delta(&mut self) {
        if self.writers.is_empty() {
            return;
        }
        // Select a random writer from self.writers
        let n = rand::rng().random_range(0..self.writers.len());
        if let Some((pid, writer)) = self
            .writers
            .iter()
            .nth(n)
            .map(|(pid, writer)| (*pid, writer.clone()))
        {
            let vv = { self.crdt.read().await.get_version_vector().clone() };
            self.crdt_wrapper
                .version_matrix
                .update(self.pid, vv.clone());
            debug!(
                "replica {} initiating pull_delta with {} connected peers",
                self.pid,
                self.writers.len()
            );
            if let Err(error) = writer.try_send(ReplicaMessage::<T>::VersionVector(
                self.pid,
                vv,
                now_micros(),
            )) {
                if matches!(error, tokio::sync::mpsc::error::TrySendError::Closed(_)) {
                    self.writers.remove(&pid);
                }
                warn!(
                    "replica {} deferred pull request to peer {}: {}",
                    self.pid, pid, error
                );
                return;
            }
            self.metric_instant(
                "peer_replication_pull_request",
                "sent",
                None,
                None,
                Some(format!("connected_peers={}", self.writers.len())),
            );
        }
    }

    async fn init_gc(&mut self, mut stable: DotSet) {
        if !self.crdt_wrapper.needs_gc(&stable) {
            // No need to gc
            return;
        }
        debug!(
            "replica {} initiating gc with stable frontier {:?}",
            self.pid, stable
        );

        let previous_marker = self.crdt_wrapper.current_gc_marker().counter;
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
        self.metric_instant(
            "gc_init",
            "start",
            None,
            Some(new_marker.counter),
            Some(format!("previous_gc_marker={previous_marker}")),
        );
        let Some(new_descriptor) = self.change_own_gc_counter(new_marker.counter).await else {
            self.metric_instant(
                "gc_init",
                "failed",
                None,
                Some(new_marker.counter),
                Some("failed to write GC membership marker".to_string()),
            );
            return;
        };

        // 2. Read the Membership directory and check that: no new replicas have appeared, no other replicas have incremented their GC marker.
        let Some(current_members) = self
            .list_members("gc_validation", Some(new_marker.counter))
            .await
        else {
            if let Err(error) = self
                .object_storage_client
                .delete_membership_descriptor(new_descriptor)
                .await
            {
                warn!(
                    "replica {} could not roll back GC marker after membership-read failure: {}",
                    self.pid, error
                );
            }
            self.metric_instant(
                "gc_init",
                "failed",
                None,
                Some(new_marker.counter),
                Some("failed to validate membership".to_string()),
            );
            return;
        };
        if !Self::membership_is_still_stable(&previous_gc_counters, &current_members, self.pid) {
            debug!(
                "replica {} aborted gc round because membership changed",
                self.pid
            );
            // Rollback the new membership desriptor
            if let Err(error) = self
                .object_storage_client
                .delete_membership_descriptor(new_descriptor)
                .await
            {
                warn!(
                    "replica {} could not roll back GC marker after membership change: {}",
                    self.pid, error
                );
            }
            self.metric_instant(
                "gc_init",
                "aborted",
                None,
                Some(new_marker.counter),
                Some("membership_changed".to_string()),
            );
            let _ = self
                .list_members("gc_abort_refresh", Some(new_marker.counter))
                .await;
            return;
        }

        // 3. Perform GC
        let gc_start = now_micros();
        let departed_pids = self.crdt_wrapper.version_matrix.garbage_collect(&stable);
        if departed_pids.is_some() {
            // Update the stable DotSet
            stable = self.crdt_wrapper.version_matrix.get_stable();
        }
        info!(
            "replica {} gc departed_pids={:?} updated_stable={:?}",
            self.pid, departed_pids, stable
        );
        let local_state = {
            let mut writable = self.crdt.write().await;
            writable.gc(stable.clone(), departed_pids.clone());
            writable.clone()
        };
        let gc_end = now_micros();
        self.metric_span(
            "gc_local_collect",
            "completed",
            None,
            Some(new_marker.counter),
            gc_start,
            gc_end,
            None,
            None,
            None,
            Some(format!("departed_pids={:?}", departed_pids)),
        );

        // DONE
        self.crdt_wrapper.push_gc_marker(new_marker, stable.clone());
        let persistent_replica = PersistentReplica {
            local_state,
            crdt_wrapper: self.crdt_wrapper.clone(),
        };

        // 4. Overwrite persistent replica with the new local state.
        let persist_start = now_micros();
        if let Err(error) = self
            .object_storage_client
            .write_persistent_replica(&persistent_replica)
            .await
        {
            warn!(
                "replica {} could not persist GC state; aborting GC cleanup: {}",
                self.pid, error
            );
            self.metric_instant(
                "gc_persistent_state_write",
                "failed",
                None,
                Some(new_marker.counter),
                Some(error.to_string()),
            );
            return;
        }
        let persist_end = now_micros();
        self.metric_span(
            "gc_persistent_state_write",
            "completed",
            None,
            Some(new_marker.counter),
            persist_start,
            persist_end,
            None,
            None,
            None,
            None,
        );
        self.persist_durable_snapshot_with(persistent_replica.clone());
        info!("replica {} completed gc round", self.pid);

        if let Some(departed_pids) = departed_pids {
            self.gc_departed_pids(departed_pids).await;
        }

        // 5. Remove our old Membership Descriptor ?
        if let Err(error) = self
            .object_storage_client
            .delete_membership_descriptor(previous_descriptor)
            .await
        {
            warn!(
                "replica {} could not delete previous membership descriptor: {}",
                self.pid, error
            );
            return;
        }
        self.metric_instant(
            "gc_finalize",
            "completed",
            None,
            Some(new_marker.counter),
            Some(previous_descriptor.to_string()),
        );
    }

    async fn gc_departed_pids(&mut self, departed_pids: Vec<Pid>) {
        if let Err(error) = futures::future::try_join_all(departed_pids.into_iter().map(|pid| {
            self.object_storage_client
                .delete_membership_descriptors_for_pid(pid)
        }))
        .await
        {
            warn!(
                "replica {} could not garbage-collect departed membership descriptors: {}",
                self.pid, error
            );
            return;
        }
        self.metric_instant(
            "gc_membership_cleanup",
            "completed",
            None,
            self.gc_metadata()
                .as_ref()
                .map(|metadata| metadata.marker.counter),
            None,
        );
    }

    async fn change_own_gc_counter(&mut self, gc_counter: Counter) -> Option<ReplicaDescriptor> {
        let descriptor = ReplicaDescriptor {
            pid: self.pid,
            address: self.advertised_address.internal(),
            gc_counter,
            final_counter: None,
        };
        let descriptor_created = match self
            .object_storage_client
            .write_membership_descriptor(descriptor)
            .await
        {
            Ok(created) => created,
            Err(error) => {
                warn!(
                    "replica {} could not write GC membership marker {}: {}",
                    self.pid, descriptor, error
                );
                return None;
            }
        };
        if !descriptor_created {
            warn!(
                "replica {} found existing GC membership marker {}; aborting round",
                self.pid, descriptor
            );
            return None;
        }
        self.metric_instant(
            "gc_membership_descriptor_write",
            "completed",
            None,
            Some(gc_counter),
            Some(descriptor.to_string()),
        );
        Some(descriptor)
    }

    async fn observe_gc(&mut self, metadata: GcMarker) {
        if metadata.marker.pid != STABLE_REPLICA_PID
            || metadata.marker.counter <= self.crdt_wrapper.current_gc_marker().counter
        {
            return;
        }

        debug!(
            "replica {} observing gc marker {} with stable frontier {:?}",
            self.pid, metadata.marker.counter, metadata.stable
        );
        self.metric_instant(
            "gc_observed",
            "start",
            None,
            Some(metadata.marker.counter),
            None,
        );
        self.crdt.write().await.gc(metadata.stable.clone(), None);
        self.crdt_wrapper
            .push_gc_marker(metadata.marker, metadata.stable.clone());
        self.persist_durable_snapshot().await;
        if self
            .change_own_gc_counter(metadata.marker.counter)
            .await
            .is_none()
        {
            self.metric_instant(
                "gc_observed",
                "failed",
                None,
                Some(metadata.marker.counter),
                Some("failed to publish observed GC marker".to_string()),
            );
            return;
        }
        self.gc_interval.reset();
        self.metric_instant(
            "gc_observed",
            "completed",
            None,
            Some(metadata.marker.counter),
            None,
        );
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
            address: self.advertised_address.internal(),
            gc_counter: self.crdt_wrapper.current_gc_marker().counter,
            final_counter: Some(final_counter),
        };

        let descriptor_created = match self
            .object_storage_client
            .write_membership_descriptor_payload(descriptor, &local_state)
            .await
        {
            Ok(created) => created,
            Err(error) => {
                warn!(
                    "replica {} could not write shutdown membership descriptor: {}",
                    self.pid, error
                );
                return;
            }
        };
        if !descriptor_created {
            warn!(
                "replica {} found existing shutdown membership descriptor {}; continuing shutdown",
                self.pid, descriptor
            );
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

        if let Some(task) = self.membership_poll_task.take() {
            task.abort();
        }
        self.membership_poll_in_flight = false;

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

    pub fn take_startup_metrics_receiver(&mut self) -> oneshot::Receiver<StartupMetrics> {
        let (startup_metrics_sender, startup_metrics_receiver) =
            oneshot::channel::<StartupMetrics>();
        self.startup_metrics_sender = Some(startup_metrics_sender);
        startup_metrics_receiver
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

    fn persist_downloaded_durable_snapshot(&self, serialized_snapshot: &[u8]) {
        let Some(journal) = &self.durability_journal else {
            return;
        };
        journal
            .replace_with_serialized_snapshot(serialized_snapshot)
            .unwrap_or_else(|error| {
                panic!("Failed to append downloaded durable snapshot: {error}")
            });
    }

    fn persist_delta_before_apply(&self, delta_group: &DeltaGroup<T::Delta, T::SideEffects>) {
        let Some(journal) = &self.durability_journal else {
            return;
        };
        journal
            .append_delta_group::<T>(delta_group)
            .unwrap_or_else(|error| panic!("Failed to append durable delta group: {error}"));
    }

    fn metric_instant(
        &mut self,
        event: &str,
        phase: &str,
        peer_pid: Option<Pid>,
        gc_marker: Option<Counter>,
        detail: Option<String>,
    ) -> u128 {
        let timestamp_us = now_micros();
        self.write_metric(MetricRecord {
            source: "server".to_string(),
            event: event.to_string(),
            phase: phase.to_string(),
            timestamp_us,
            replica_pid: self.pid,
            peer_pid,
            gc_marker,
            sent_us: None,
            received_us: None,
            start_us: None,
            end_us: None,
            insert_count: None,
            delete_count: None,
            detail,
            client_id: None,
            operation: None,
            value: None,
            status_code: None,
            latency_us: None,
        });
        timestamp_us
    }

    fn metric_span(
        &mut self,
        event: &str,
        phase: &str,
        peer_pid: Option<Pid>,
        gc_marker: Option<Counter>,
        start_us: u128,
        end_us: u128,
        insert_count: Option<u16>,
        delete_count: Option<u16>,
        received_us: Option<u128>,
        detail: Option<String>,
    ) {
        self.write_metric(MetricRecord {
            source: "server".to_string(),
            event: event.to_string(),
            phase: phase.to_string(),
            timestamp_us: end_us,
            replica_pid: self.pid,
            peer_pid,
            gc_marker,
            sent_us: None,
            received_us,
            start_us: Some(start_us),
            end_us: Some(end_us),
            insert_count,
            delete_count,
            detail,
            client_id: None,
            operation: None,
            value: None,
            status_code: None,
            latency_us: Some(end_us.saturating_sub(start_us)),
        });
    }

    fn write_metric(&mut self, record: MetricRecord) {
        self.metric_writer
            .serialize(record)
            .expect("Failed to write metric");
        // Crash experiments use SIGKILL. Flush each record so the trace up to
        // the kill is retained instead of being lost in the CSV writer buffer.
        self.metric_writer.flush().expect("Failed to flush metric");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{Journal, JournalError, RawDurabilityRecord};
    use crate::orset::{ORSet, OrSetMutation, OrSetQuery, OrSetResponse};
    use std::collections::HashSet;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::path::PathBuf;
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_journal_path(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock drifted before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("gresse-{name}-{unique}.jsonl"))
    }

    fn membership_descriptor(
        pid: Pid,
        gc_counter: Counter,
        final_counter: Option<Counter>,
    ) -> ReplicaDescriptor {
        ReplicaDescriptor {
            pid,
            address: "127.0.0.1:19080".parse().unwrap(),
            gc_counter,
            final_counter,
        }
    }

    #[test]
    fn network_connection_candidates_deduplicates_and_skips_departed_members() {
        let members = vec![
            membership_descriptor(1, 1, None),
            membership_descriptor(1, 2, None),
            membership_descriptor(2, 1, None),
            membership_descriptor(2, 2, Some(9)),
            membership_descriptor(3, 1, None),
            membership_descriptor(4, 1, None),
        ];
        let connected_pids = HashSet::from([4]);

        let candidates = network_connection_candidates(&members, 3, &connected_pids);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].pid, 1);
        assert_eq!(candidates[0].gc_counter, 2);
    }

    #[test]
    fn durability_journal_recovers_snapshot_and_delta_groups() {
        let journal_path = temp_journal_path("durability");
        let journal =
            Journal::new(journal_path.clone()).expect("failed to create durability journal");

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
        let delta_group = candidate.get_delta(&version_vector_before);

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

    #[test]
    fn durability_journal_accepts_downloaded_serialized_snapshot() {
        let journal_path = temp_journal_path("downloaded-snapshot");
        let journal =
            Journal::new(journal_path.clone()).expect("failed to create durability journal");
        let mut state = ORSet::<String>::new();
        state.set_pid(1);
        state.mutate(OrSetMutation::Insert("apple".into())).unwrap();
        let serialized_snapshot = serde_json::to_vec(&PersistentReplica {
            local_state: state,
            crdt_wrapper: CRDTWrapper::new(),
        })
        .expect("failed to serialize snapshot");

        journal
            .replace_with_serialized_snapshot(&serialized_snapshot)
            .expect("failed to persist downloaded snapshot");

        let recovered = journal
            .recover::<ORSet<String>>()
            .expect("failed to recover downloaded snapshot")
            .expect("snapshot missing from journal");
        assert_eq!(
            recovered
                .local_state
                .query(OrSetQuery::Contains("apple".into()))
                .expect("failed to query recovered ORSet"),
            OrSetResponse::Contains(true)
        );

        std::fs::remove_file(journal_path).expect("failed to clean up durability journal");
    }

    #[test]
    fn durability_snapshot_compacts_prior_history() {
        let journal_path = temp_journal_path("snapshot-compaction");
        let journal =
            Journal::new(journal_path.clone()).expect("failed to create durability journal");

        let mut state = ORSet::<String>::new();
        state.set_pid(1);
        state.mutate(OrSetMutation::Insert("apple".into())).unwrap();
        journal
            .append_snapshot(&PersistentReplica {
                local_state: state.clone(),
                crdt_wrapper: CRDTWrapper::new(),
            })
            .expect("failed to append initial snapshot");

        journal
            .append_mutation::<ORSet<String>>(&OrSetMutation::Insert("banana".into()))
            .expect("failed to append mutation");
        state
            .mutate(OrSetMutation::Insert("banana".into()))
            .unwrap();
        journal
            .append_snapshot(&PersistentReplica {
                local_state: state,
                crdt_wrapper: CRDTWrapper::new(),
            })
            .expect("failed to compact journal to snapshot");

        let contents =
            std::fs::read_to_string(&journal_path).expect("failed to read compacted journal");
        assert_eq!(contents.lines().count(), 1);
        let recovered = journal
            .recover::<ORSet<String>>()
            .expect("failed to recover compacted journal")
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

    #[test]
    fn durability_journal_discards_an_incomplete_trailing_record() {
        let journal_path = temp_journal_path("torn-tail");
        let journal =
            Journal::new(journal_path.clone()).expect("failed to create durability journal");

        let mut base = ORSet::<String>::new();
        base.set_pid(1);
        base.mutate(OrSetMutation::Insert("apple".into())).unwrap();
        journal
            .append_snapshot(&PersistentReplica {
                local_state: base,
                crdt_wrapper: CRDTWrapper::new(),
            })
            .expect("failed to append durability snapshot");

        let mut file = OpenOptions::new()
            .append(true)
            .open(&journal_path)
            .expect("failed to reopen durability journal");
        file.write_all(br#"{"record_type":"mutation","payload":"#)
            .expect("failed to write incomplete record");
        file.sync_data().expect("failed to sync incomplete record");

        let recovered = journal
            .recover::<ORSet<String>>()
            .expect("incomplete trailing record should be ignored")
            .expect("expected recovered replica");
        assert_eq!(
            recovered
                .local_state
                .query(OrSetQuery::Elements)
                .expect("failed to query recovered ORSet"),
            OrSetResponse::Elements(vec!["apple".into()])
        );
        let contents = std::fs::read_to_string(&journal_path)
            .expect("failed to read repaired durability journal");
        assert_eq!(contents.lines().count(), 1);
        serde_json::from_str::<RawDurabilityRecord>(contents.trim_end())
            .expect("repaired durability journal should contain valid JSON");

        std::fs::remove_file(journal_path).expect("failed to clean up durability journal");
    }

    #[test]
    fn durability_journal_repairs_a_corrupt_non_tail_suffix() {
        let journal_path = temp_journal_path("corrupt-suffix");
        let journal =
            Journal::new(journal_path.clone()).expect("failed to create durability journal");

        let mut base = ORSet::<String>::new();
        base.set_pid(1);
        base.mutate(OrSetMutation::Insert("apple".into())).unwrap();
        journal
            .append_snapshot(&PersistentReplica {
                local_state: base,
                crdt_wrapper: CRDTWrapper::new(),
            })
            .expect("failed to append durability snapshot");

        let mut file = OpenOptions::new()
            .append(true)
            .open(&journal_path)
            .expect("failed to reopen durability journal");
        // This models a torn write followed by a later writer appending its
        // own complete record.  The two fragments form one malformed physical
        // line, so merely discarding an unterminated final line is insufficient.
        file.write_all(br#"{"record_type":"mutation","payload":{"Insert":"#)
            .expect("failed to write torn record prefix");
        file.write_all(br#"{"record_type":"mutation","payload":{"Insert":"banana"}}\n"#)
            .expect("failed to write later record");
        file.sync_data().expect("failed to sync corrupt suffix");

        let recovered = journal
            .recover::<ORSet<String>>()
            .expect("corrupt suffix should be repaired")
            .expect("expected recovered replica");
        assert_eq!(
            recovered
                .local_state
                .query(OrSetQuery::Elements)
                .expect("failed to query recovered ORSet"),
            OrSetResponse::Elements(vec!["apple".into()])
        );

        let contents = std::fs::read_to_string(&journal_path)
            .expect("failed to read repaired durability journal");
        assert_eq!(contents.lines().count(), 1);
        serde_json::from_str::<RawDurabilityRecord>(contents.trim_end())
            .expect("repaired durability journal should contain valid JSON");

        journal
            .append_mutation::<ORSet<String>>(&OrSetMutation::Insert("cherry".into()))
            .expect("failed to append after repair");
        let recovered_after_append = journal
            .recover::<ORSet<String>>()
            .expect("repaired journal should remain recoverable")
            .expect("expected recovered replica");
        assert_eq!(
            recovered_after_append
                .local_state
                .query(OrSetQuery::Elements)
                .expect("failed to query recovered ORSet"),
            OrSetResponse::Elements(vec!["apple".into(), "cherry".into()])
        );

        std::fs::remove_file(journal_path).expect("failed to clean up durability journal");
    }

    #[test]
    fn durability_journal_excludes_another_process_owner() {
        let journal_path = temp_journal_path("exclusive-lock");
        let first =
            Journal::new(journal_path.clone()).expect("failed to create first durability journal");

        assert!(matches!(
            Journal::new(journal_path.clone()),
            Err(JournalError::AlreadyLocked(path)) if path == journal_path
        ));

        drop(first);
        Journal::new(journal_path.clone())
            .expect("journal should be available after its owner exits");
        std::fs::remove_file(Journal::lock_path(&journal_path))
            .expect("failed to clean up durability lock file");
    }

    #[test]
    fn durability_journal_serializes_concurrent_appends() {
        let journal_path = temp_journal_path("concurrent-appends");
        let journal = Arc::new(
            Journal::new(journal_path.clone()).expect("failed to create durability journal"),
        );
        let mut base = ORSet::<String>::new();
        base.set_pid(1);
        journal
            .append_snapshot(&PersistentReplica {
                local_state: base,
                crdt_wrapper: CRDTWrapper::new(),
            })
            .expect("failed to append durability snapshot");

        let writers = (0..16)
            .map(|index| {
                let journal = journal.clone();
                thread::spawn(move || {
                    journal
                        .append_mutation::<ORSet<String>>(&OrSetMutation::Insert(format!(
                            "value-{index}"
                        )))
                        .expect("failed to append concurrent mutation");
                })
            })
            .collect::<Vec<_>>();
        for writer in writers {
            writer.join().expect("concurrent writer panicked");
        }

        let contents =
            std::fs::read_to_string(&journal_path).expect("failed to read durability journal");
        assert_eq!(contents.lines().count(), 17);
        for line in contents.lines() {
            serde_json::from_str::<RawDurabilityRecord>(line)
                .expect("concurrent append produced malformed JSON");
        }
        journal
            .recover::<ORSet<String>>()
            .expect("concurrent append journal should recover");

        std::fs::remove_file(journal_path).expect("failed to clean up durability journal");
    }
}
