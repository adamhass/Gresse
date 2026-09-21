use crate::http_server::{launch_http_server, ClientMutationHandler};
use crate::journal::{DiskJournal, Journal};
pub use crate::metrics::StartupMetrics;
use crate::metrics::{ClientMutationMetric, MembershipInitResult, Metrics, TimedResult};
// use crate::vectors::{api::*, vector_db::*, Float, Key, Vector};
// use crate::prelude::*;
use crate::dots::{Counter, Dot, DotSet};
use crate::network::{network_connection_candidates, NetworkManager, NetworkMember};
use crate::object_storage::{MembershipPollResult, ObjectStorageClient};
use crate::replica_helpers::*;
use crate::{
    crdt::*,
    prelude::{new_pid, now_micros, Pid, ServerAddr},
};
use log::{debug, info, trace, warn};
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, Receiver, Sender, UnboundedReceiver};
use tokio::sync::oneshot;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Instant, Interval};

const DEFAULT_GC_INTERVAL: Duration = Duration::from_secs(60);

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
    metrics: Metrics,
    object_storage_client: Arc<ObjectStorageClient>,
    durability_journal: Arc<DiskJournal>,
    recovered_from_durability: bool,
    recovered_predecessor_pid: Option<Pid>,
    startup_metrics: StartupMetrics,
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
    fn effective_pid(requested_pid: Pid, recovered_replica: Option<&PersistentReplica<T>>) -> Pid {
        recovered_replica
            .map(|replica| replica.crdt_wrapper.own_pid())
            .unwrap_or(requested_pid)
    }

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
        let durability_journal = Arc::new(
            config
                .durability_path
                .clone()
                .map(DiskJournal::new)
                .transpose()
                .unwrap_or_else(|error| panic!("Failed to initialize durability journal: {error}"))
                .unwrap_or_else(DiskJournal::disabled),
        );
        let recovered_replica = durability_journal
            .recover::<T>()
            .unwrap_or_else(|error| panic!("Failed to recover durable replica state: {error}"));

        // A durability journal belongs to one logical replica. Reuse that
        // replica's persisted identity instead of assigning a fresh PID after
        // process recovery.
        let pid = Self::effective_pid(pid, recovered_replica.as_ref());

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
        Self::with_shared_config_internal(
            pid,
            crdt,
            config,
            Arc::new(DiskJournal::disabled()),
            None,
        )
        .await
    }

    async fn with_shared_config_internal(
        pid: Pid,
        crdt: Arc<RwLock<T>>,
        config: ReplicaConfig,
        durability_journal: Arc<DiskJournal>,
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
                let start_us = now_micros();
                let _commit = commit_gate.lock().await;
                journal
                    .append_mutation::<T>(&mutation)
                    .unwrap_or_else(|error| {
                        panic!("Failed to append durable client mutation: {error}")
                    });
                let response: <T as CRDT>::ClientResponse = crdt.write().await.mutate(mutation);
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

        let metrics = Metrics::create(&config.result_dir_path, pid);
        let metric_writer_ready_us = now_micros();
        let membership_poll_period = config.object_storage_config.discovery_interval;
        let object_storage_client_start_us = now_micros();
        let object_storage_client = Arc::new(
            ObjectStorageClient::new(config.object_storage_config).unwrap_or_else(|error| {
                panic!("Failed to initialize object storage client: {error}")
            }),
        );
        let object_storage_client_ready_us = now_micros();

        let mut crdt_wrapper = recovered_replica
            .as_ref()
            .map(|replica| replica.crdt_wrapper.clone())
            .unwrap_or(CRDTWrapper::new(pid));
        let local_version_vector = { crdt.read().await.get_version_vector().clone() };
        crdt_wrapper.rebind_own_pid(pid, local_version_vector);

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
                crdt_wrapper,
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
                metrics,
                object_storage_client,
                durability_journal,
                recovered_from_durability: recovered_replica.is_some(),
                recovered_predecessor_pid: recovered_replica
                    .as_ref()
                    .and(config.recovered_predecessor_pid)
                    .filter(|predecessor_pid| *predecessor_pid != pid),
                startup_metrics: StartupMetrics::construction_complete(
                    crdt_pid_set_us,
                    http_server_ready_us,
                    network_manager_ready_us,
                    metric_writer_ready_us,
                    object_storage_client_start_us,
                    object_storage_client_ready_us,
                ),
                startup_metrics_sender: None,
            },
            shutdown_sender,
        )
    }

    pub async fn run(&mut self) {
        let replica_run_start_us = now_micros();
        self.startup_metrics
            .record_run_started(replica_run_start_us);
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
        let membership_poll_client = self.object_storage_client.clone();
        loop {
            tokio::select! {
                Some(metric) = self.client_mutation_metric_receiver.recv() => {
                    self.refresh_own_version_vector().await;
                    self.metrics.client_mutation_completed(metric.received_us, metric.start_us, metric.end_us);
                }
                Some(remote_event) = self.replication_receiver.recv() => {
                    self.handle_remote_event(remote_event).await;
                }
                _ = interval.tick() => {
                    self.pull_delta().await;
                }
                _ = self.gc_interval.tick() => {
                    self.init_gc().await;
                }
                _ = self.membership_poll_interval.tick() => {
                    self.object_storage_client.start_membership_poll();
                }
                Some(result) = membership_poll_client.recv_membership_poll() => {
                    self.apply_membership_poll(result).await;
                }
                Some((pid, writer)) = self.replication_writer_receiver.recv() => {
                    info!(
                        "replica {} established peer replication connection with replica {}",
                        self.pid,
                        pid
                    );
                    self.writers.insert(pid, writer);
                    self.metrics.peer_replication_connection_established(pid);
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
        let replica_init_start_us = self.metrics.replica_init_started();
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
                    Ok(()) => self
                        .metrics
                        .recovered_membership_cleanup(predecessor_pid, Ok(())),
                    Err(error) => {
                        warn!(
                            "replica {} could not remove recovered predecessor {} descriptors: {}",
                            self.pid, predecessor_pid, error
                        );
                        self.metrics
                            .recovered_membership_cleanup(predecessor_pid, Err(error.to_string()));
                    }
                }
            }
            self.record_membership_init_metrics(&membership_init);
            info!(
                "replica {} restored local state from durability journal before startup",
                self.pid
            );
            persistent_state_fetch_completed_us = self
                .metrics
                .persistent_state_fetch_instant("recovered_from_durability_journal");
            info!(
                "replica {} discovered {} membership descriptors during init",
                self.pid,
                membership_init.members.len()
            );
        } else {
            let (persistent_replica_result, concurrent_membership_init) = tokio::join!(
                TimedResult::measure(
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

            self.metrics.persistent_state_fetch_completed(
                persistent_replica_result.start_us,
                persistent_replica_result.end_us,
                persistent_replica_result.detail,
            );
            persistent_state_fetch_completed_us = persistent_replica_result.end_us;
            membership_init = concurrent_membership_init;
            self.record_membership_init_metrics(&membership_init);
            restored_from_storage = persistent_replica_result.value.is_some();

            match persistent_replica_result.value {
                Some(mut persistent_replica) => {
                    debug!("replica {} restoring persistent replica state", self.pid);
                    let commit_gate = self.commit_gate.clone();
                    let _commit = commit_gate.lock().await;
                    persistent_replica.rebind_own_pid(self.pid);
                    self.durability_journal
                        .append_snapshot(&persistent_replica)
                        .unwrap_or_else(|error| {
                            panic!("Failed to persist rebound bootstrap snapshot: {error}")
                        });
                    *self.crdt.write().await = persistent_replica.local_state;
                    self.crdt_wrapper = persistent_replica.crdt_wrapper;
                    // The object-storage snapshot is state for bootstrapping a
                    // new replica, not an identity-bearing recovery record.
                    // Persisting the decoded, rebound value above ensures a
                    // later local journal recovery retains this replica's PID.
                }
                None => {
                    debug!(
                        "replica {} found no persistent replica state; writing initial snapshot",
                        self.pid
                    );
                    if self.write_initial_persistent_replica().await {
                        self.metrics
                            .persistent_state_fetch_instant("initialized_new_persistent_snapshot");
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
        let replica_init_completed_us = self.metrics.replica_init_completed();
        self.startup_metrics.record_bootstrap_completed(
            replica_init_start_us,
            persistent_state_fetch_completed_us,
            membership_init.descriptor_write.end_us,
            membership_init.membership_list.end_us,
            replica_init_completed_us,
        );
        if let Some(startup_metrics_sender) = self.startup_metrics_sender.take() {
            let _ = startup_metrics_sender.send(self.startup_metrics.clone());
        }

        membership_init.members
    }

    async fn read_persistent_replica_from_storage(
        object_storage_client: &ObjectStorageClient,
    ) -> Option<PersistentReplica<T>> {
        object_storage_client
            .read_persistent_replica()
            .await
            .unwrap_or_else(|error| panic!("Failed to read persistent replica state: {error}"))
    }

    async fn register_and_list_members_during_init(
        object_storage_client: &ObjectStorageClient,
        descriptor: ReplicaDescriptor,
    ) -> MembershipInitResult {
        let descriptor_write = TimedResult::measure(
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

        let members = TimedResult::measure(
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
        self.metrics.membership_descriptor_write_completed(
            membership_init.descriptor_write.start_us,
            membership_init.descriptor_write.end_us,
            membership_init.descriptor_write.detail.clone(),
        );
        self.metrics.membership_directory_read_completed(
            None,
            membership_init.membership_list.start_us,
            membership_init.membership_list.end_us,
            membership_init.membership_list.detail.clone(),
        );
    }

    fn enqueue_network_members(&mut self, members: &[ReplicaDescriptor]) {
        let departed_pids = members
            .iter()
            .filter(|descriptor| descriptor.is_shutdown())
            .map(|descriptor| descriptor.pid)
            .collect::<HashSet<_>>();
        let mut discovered_gc_counters = HashMap::<Pid, Counter>::new();
        for descriptor in members {
            if descriptor.pid == self.pid
                || descriptor.is_shutdown()
                || departed_pids.contains(&descriptor.pid)
            {
                continue;
            }
            discovered_gc_counters
                .entry(descriptor.pid)
                .and_modify(|counter| *counter = (*counter).max(descriptor.gc_counter))
                .or_insert(descriptor.gc_counter);
        }
        for (pid, gc_counter) in discovered_gc_counters {
            self.crdt_wrapper.observe_active_replica(pid, gc_counter);
        }

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
            self.metrics
                .persistent_state_write_failed(start, now_micros(), error.to_string());
            return false;
        }
        let end = now_micros();
        self.metrics.persistent_state_write_completed(
            start,
            end,
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
                self.metrics.membership_directory_read_failed(
                    gc_marker,
                    start,
                    now_micros(),
                    detail,
                    &error,
                );
                warn!("replica {} could not list membership: {}", self.pid, error);
                return None;
            }
        };
        let end = now_micros();
        self.metrics.membership_directory_read_with_count(
            gc_marker,
            start,
            end,
            detail,
            members.len(),
        );
        Some(members)
    }

    async fn apply_membership_poll(&mut self, result: MembershipPollResult) {
        self.object_storage_client.finish_membership_poll().await;
        let members = match result.members {
            Ok(members) => members,
            Err(error) => {
                self.metrics.membership_directory_read_failed(
                    None,
                    result.start_us,
                    result.end_us,
                    "poll",
                    &error,
                );
                warn!("replica {} membership poll failed: {}", self.pid, error);
                return;
            }
        };
        self.object_storage_client
            .update_membership_cache(members.clone())
            .await;
        self.metrics.membership_directory_read_with_count(
            None,
            result.start_us,
            result.end_us,
            "poll",
            members.len(),
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
        if !self.crdt_wrapper.should_fetch_departed_replica(final_dot) {
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
        if !self.merge_shutdown_payload(descriptor).await {
            return;
        }
        // Keep the replica row until its final dot is stable everywhere.
        self.crdt_wrapper.observe_departed_replica(final_dot);

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

    async fn merge_shutdown_payload(&mut self, descriptor: ReplicaDescriptor) -> bool {
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
                return false;
            }
        };
        let Some(shutdown_state) = shutdown_state else {
            return false;
        };

        let local_version_vector = { self.crdt.read().await.get_version_vector().clone() };
        let delta = shutdown_state.get_delta(&local_version_vector);
        if delta.list.is_empty() {
            self.refresh_own_version_vector().await;
            debug!(
                "replica {} found no missing shutdown deltas for replica {}",
                self.pid, descriptor.pid
            );
            return true;
        }

        debug!(
            "replica {} merging {} shutdown deltas from replica {}",
            self.pid,
            delta.list.len(),
            descriptor.pid
        );
        {
            let _commit = self.commit_gate.lock().await;
            self.persist_delta_before_apply(&delta);
            self.crdt.write().await.merge_delta_group(delta);
        }
        self.refresh_own_version_vector().await;
        true
    }

    async fn handle_remote_event(&mut self, event: ReplicaMessage<T>) {
        let received = now_micros();
        match event {
            ReplicaMessage::<T>::DeltaGroup(delta, gc_metadata, sent) => {
                let remote_gc_counter = *delta.version_vector.gc_counter();
                let current_gc_counter = self.crdt_wrapper.current_gc_marker().counter;
                if remote_gc_counter < current_gc_counter {
                    self.metrics.peer_replication_merge_delta_refused_stale_gc(
                        remote_gc_counter,
                        current_gc_counter,
                        received,
                        delta.list.len(),
                        sent,
                    );
                    // Do not accept the Delta
                    debug!(
                        "replica {} refusing remote delta group with GC counter={} below current GC counter={}",
                        self.pid, remote_gc_counter, current_gc_counter
                    );
                    return;
                }
                let gc_marker = Some(gc_metadata.marker.counter);
                self.observe_gc(gc_metadata).await;
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
                self.refresh_own_version_vector().await;
                self.metrics.peer_replication_merge_delta_completed(
                    gc_marker, received, start, end, delta_len, sent,
                );
                debug!(
                    "replica {} merging remote delta group with {} entries",
                    self.pid, delta_len
                );
            }
            ReplicaMessage::<T>::VersionVector(pid, vv, sent) => {
                self.crdt_wrapper.version_matrix.update(pid, vv.clone());
                let (start, mut delta) = {
                    let readable = self.crdt.read().await;
                    let start = now_micros();
                    (start, readable.get_delta(&vv))
                };
                delta.version_vector = self
                    .crdt_wrapper
                    .update_own_version_vector(delta.version_vector);
                let delta_len = delta.list.len();
                debug!(
                    "replica {} get_delta for replica {} produced {} deltas",
                    self.pid, pid, delta_len
                );
                let end = now_micros();
                let gc_metadata = self.gc_metadata();
                let gc_marker = Some(gc_metadata.marker.counter);
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
                self.metrics.peer_replication_get_delta_completed(
                    pid, gc_marker, received, start, end, delta_len, sent,
                );
            }
        }
    }

    async fn refresh_own_version_vector(&mut self) -> DotSet {
        let version_vector = { self.crdt.read().await.get_version_vector().clone() };
        self.crdt_wrapper.update_own_version_vector(version_vector)
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
            let vv = self.refresh_own_version_vector().await;
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
            self.metrics
                .peer_replication_pull_request_sent(self.writers.len());
        }
    }

    async fn init_gc(&mut self) {
        self.refresh_own_version_vector().await;
        if !self.crdt_wrapper.needs_gc() {
            // No need to gc
            return;
        }
        let (Some(stable), departed_pids) = self.crdt_wrapper.version_matrix.get_stable() else {
            return;
        };

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
        self.metrics
            .gc_init_started(new_marker.counter, previous_marker);
        let Some(new_descriptor) = self.change_own_gc_counter(new_marker.counter).await else {
            self.metrics
                .gc_init_failed(new_marker.counter, "failed to write GC membership marker");
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
            self.metrics
                .gc_init_failed(new_marker.counter, "failed to validate membership");
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
            self.metrics.gc_init_aborted(new_marker.counter);
            let _ = self
                .list_members("gc_abort_refresh", Some(new_marker.counter))
                .await;
            return;
        }

        // 3. Perform GC
        let gc_start = now_micros();
        info!(
            "replica {} gc departed_pids={:?} updated_stable={:?}",
            self.pid, departed_pids, stable
        );
        let local_state = {
            let mut writable = self.crdt.write().await;
            writable.gc(&stable, &departed_pids);
            writable.clone()
        };
        let gc_end = now_micros();
        self.metrics.gc_local_collect_completed(
            new_marker.counter,
            gc_start,
            gc_end,
            &departed_pids,
        );

        // DONE
        self.crdt_wrapper.push_gc_marker(GcMarker {
            marker: new_marker,
            stable: stable.clone(),
            departed_pids: departed_pids.clone(),
        });
        let persistent_replica = PersistentReplica {
            local_state,
            crdt_wrapper: self.crdt_wrapper.clone(),
        };

        // 4. Overwrite persistent replica with the new local state.
        let persist_start = now_micros();
        let state_bytes = match self
            .object_storage_client
            .write_persistent_replica_with_size(&persistent_replica)
            .await
        {
            Ok(state_bytes) => state_bytes,
            Err(error) => {
                warn!(
                    "replica {} could not persist GC state; aborting GC cleanup: {}",
                    self.pid, error
                );
                self.metrics
                    .gc_persistent_state_write_failed(new_marker.counter, error.to_string());
                return;
            }
        };
        let persist_end = now_micros();
        self.metrics.gc_persistent_state_write_completed(
            new_marker.counter,
            persist_start,
            persist_end,
            state_bytes,
        );
        self.persist_durable_snapshot().await;
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
        self.metrics
            .gc_finalize_completed(new_marker.counter, previous_descriptor.to_string());
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
        self.metrics
            .gc_membership_cleanup_completed(Some(self.gc_metadata().marker.counter));
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
        self.metrics
            .gc_membership_descriptor_write_completed(gc_counter, descriptor.to_string());
        Some(descriptor)
    }

    async fn observe_gc(&mut self, metadata: GcMarker) {
        if metadata.marker.pid != STABLE_REPLICA_PID
            || metadata.marker.counter <= self.crdt_wrapper.current_gc_marker().counter
        {
            return;
        }
        let counter = metadata.marker.counter;
        debug!(
            "replica {} observing gc marker {} with stable frontier {:?}",
            self.pid, metadata.marker.counter, metadata.stable
        );
        self.metrics.gc_observed_started(counter);
        self.crdt
            .write()
            .await
            .gc(&metadata.stable, &metadata.departed_pids);
        self.crdt_wrapper.push_gc_marker(metadata);
        self.persist_durable_snapshot().await;
        self.gc_interval.reset();
        self.metrics.gc_observed_completed(counter);
    }

    fn gc_metadata(&self) -> GcMarker {
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

        self.object_storage_client.abort_membership_poll().await;

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
        // A compacted snapshot must include every mutation whose log entry it
        // replaces. The same gate protects durable-before-apply mutation and
        // delta commits, so take it before reading the CRDT.
        let _commit = self.commit_gate.lock().await;
        let snapshot = PersistentReplica {
            local_state: self.crdt.read().await.clone(),
            crdt_wrapper: self.crdt_wrapper.clone(),
        };
        self.durability_journal
            .append_snapshot(&snapshot)
            .unwrap_or_else(|error| panic!("Failed to append durable snapshot: {error}"));
    }

    fn persist_delta_before_apply(&self, delta_group: &DeltaGroup<T::Delta, T::SideEffects>) {
        self.durability_journal
            .append_delta_group::<T>(delta_group)
            .unwrap_or_else(|error| panic!("Failed to append durable delta group: {error}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{DiskJournal, Journal, JournalError, RawDurabilityRecord};
    use crate::orset::{ORSet, OrSetMutation, OrSetQuery, OrSetResponse};
    use crate::prelude::ObjectStorageConfig;
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

    fn cleanup_journal(path: &PathBuf) {
        for path in [
            DiskJournal::snapshot_path(path),
            DiskJournal::mutation_log_path(path),
            DiskJournal::lock_path(path),
        ] {
            let _ = std::fs::remove_file(path);
        }
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
    fn recovered_replica_uses_persisted_pid() {
        let recovered = PersistentReplica {
            local_state: ORSet::<String>::new(),
            crdt_wrapper: CRDTWrapper::new(41),
        };

        assert_eq!(
            Replica::<ORSet<String>>::effective_pid(99, Some(&recovered)),
            41
        );
        assert_eq!(Replica::<ORSet<String>>::effective_pid(99, None), 99);
    }

    #[test]
    fn rebound_object_storage_state_persists_the_launch_pid() {
        let journal_path = temp_journal_path("rebound-bootstrap");
        let journal =
            DiskJournal::new(journal_path.clone()).expect("failed to create durability journal");
        let mut state = ORSet::<String>::new();
        state.set_pid(1);
        state
            .mutate(OrSetMutation::Insert("source".into()))
            .unwrap();
        let mut downloaded = PersistentReplica {
            local_state: state,
            crdt_wrapper: CRDTWrapper::new(1),
        };

        downloaded.rebind_own_pid(2);
        journal
            .append_snapshot(&downloaded)
            .expect("failed to persist rebound bootstrap state");

        let mut recovered = journal
            .recover::<ORSet<String>>()
            .expect("failed to recover rebound bootstrap state")
            .expect("rebound bootstrap state missing from journal");
        assert_eq!(recovered.crdt_wrapper.own_pid(), 2);
        recovered
            .local_state
            .mutate(OrSetMutation::Insert("local".into()))
            .unwrap();
        assert_eq!(
            recovered.local_state.get_version_vector().counter(&2),
            Some(0)
        );

        cleanup_journal(&journal_path);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn object_storage_bootstrap_journals_the_new_replica_identity() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock drifted before unix epoch")
            .as_nanos();
        let test_root = std::env::temp_dir().join(format!("gresse-bootstrap-pid-{unique}"));
        let object_store_root = test_root.join("object-store");
        let result_dir = test_root.join("results");
        let journal_path = test_root.join("replica.journal");
        std::fs::create_dir_all(&object_store_root)
            .expect("failed to create object-store directory");
        std::fs::create_dir_all(&result_dir).expect("failed to create result directory");

        let mut source_state = ORSet::<String>::new();
        source_state.set_pid(1);
        source_state
            .mutate(OrSetMutation::Insert("source".into()))
            .unwrap();
        let source_snapshot = PersistentReplica {
            local_state: source_state,
            crdt_wrapper: CRDTWrapper::new(1),
        };
        std::fs::write(
            object_store_root.join("persistent.json"),
            serde_json::to_vec(&source_snapshot).expect("failed to serialize source snapshot"),
        )
        .expect("failed to seed object storage");

        let address = ServerAddr {
            ip: "127.0.0.1".parse().expect("failed to parse loopback"),
            http_port: 19190,
            internal_port: 18180,
        };
        let config = ReplicaConfig {
            address,
            advertised_address: address,
            sync_interval: Duration::from_secs(60),
            result_dir_path: result_dir,
            durability_path: Some(journal_path.clone()),
            recovered_predecessor_pid: None,
            object_storage_config: ObjectStorageConfig {
                local_dir: Some(object_store_root),
                url: None,
                region: "local".to_string(),
                bucket: "local".to_string(),
                access_key: None,
                secret_key: None,
                session_token: None,
                persistent_replica_path: "persistent.json".to_string(),
                membership_directory_path: "membership".to_string(),
                discovery_interval: Duration::from_secs(60),
            },
        };
        let (mut replica, shutdown_sender) =
            Replica::with_config(2, ORSet::<String>::new(), config).await;
        let startup_receiver = replica.take_startup_metrics_receiver();
        let replica_task = tokio::spawn(async move { replica.run().await });
        tokio::time::timeout(Duration::from_secs(5), startup_receiver)
            .await
            .expect("replica bootstrap timed out")
            .expect("replica bootstrap metric sender dropped");
        let _ = shutdown_sender.send(());
        tokio::time::timeout(Duration::from_secs(5), replica_task)
            .await
            .expect("replica shutdown timed out")
            .expect("replica task failed");

        let journal =
            DiskJournal::new(journal_path.clone()).expect("failed to reopen durability journal");
        let recovered = journal
            .recover::<ORSet<String>>()
            .expect("failed to recover bootstrapped journal")
            .expect("bootstrapped journal was empty");
        assert_eq!(recovered.crdt_wrapper.own_pid(), 2);

        drop(journal);
        std::fs::remove_dir_all(test_root).expect("failed to clean bootstrap test directory");
    }

    #[test]
    fn durability_journal_recovers_snapshot_and_delta_groups() {
        let journal_path = temp_journal_path("durability");
        let journal =
            DiskJournal::new(journal_path.clone()).expect("failed to create durability journal");

        let mut base = ORSet::<String>::new();
        base.set_pid(1);
        base.mutate(OrSetMutation::Insert("apple".into())).unwrap();

        journal
            .append_snapshot(&PersistentReplica {
                local_state: base.clone(),
                crdt_wrapper: CRDTWrapper::new(1),
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

        cleanup_journal(&journal_path);
    }

    #[test]
    fn durability_snapshot_compacts_prior_history() {
        let journal_path = temp_journal_path("snapshot-compaction");
        let journal =
            DiskJournal::new(journal_path.clone()).expect("failed to create durability journal");

        let mut state = ORSet::<String>::new();
        state.set_pid(1);
        state.mutate(OrSetMutation::Insert("apple".into())).unwrap();
        journal
            .append_snapshot(&PersistentReplica {
                local_state: state.clone(),
                crdt_wrapper: CRDTWrapper::new(1),
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
                crdt_wrapper: CRDTWrapper::new(1),
            })
            .expect("failed to compact journal to snapshot");

        let contents = std::fs::read_to_string(DiskJournal::snapshot_path(&journal_path))
            .expect("failed to read compacted snapshot");
        assert_eq!(contents.lines().count(), 1);
        assert!(
            std::fs::read_to_string(DiskJournal::mutation_log_path(&journal_path))
                .expect("failed to read compacted mutation log")
                .is_empty()
        );
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

        cleanup_journal(&journal_path);
    }

    #[test]
    fn durability_recovery_ignores_a_log_left_by_an_interrupted_compaction() {
        let journal_path = temp_journal_path("compaction-generation");
        let journal =
            DiskJournal::new(journal_path.clone()).expect("failed to create durability journal");
        let mut state = ORSet::<String>::new();
        state.set_pid(1);
        state.mutate(OrSetMutation::Insert("apple".into())).unwrap();
        journal
            .append_snapshot(&PersistentReplica {
                local_state: state.clone(),
                crdt_wrapper: CRDTWrapper::new(1),
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
                crdt_wrapper: CRDTWrapper::new(1),
            })
            .expect("failed to compact journal");

        // A crash after the atomic snapshot replacement but before truncating
        // the former log leaves generation 1 records behind. Recovery must
        // ignore them rather than replay them over generation 2.
        std::fs::write(
            DiskJournal::mutation_log_path(&journal_path),
            b"{\"generation\":1,\"record_type\":\"mutation\",\"payload\":{\"Insert\":\"obsolete\"}}\n",
        )
        .expect("failed to restore stale mutation log");

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
        cleanup_journal(&journal_path);
    }

    #[test]
    fn durability_journal_discards_an_incomplete_trailing_record() {
        let journal_path = temp_journal_path("torn-tail");
        let journal =
            DiskJournal::new(journal_path.clone()).expect("failed to create durability journal");

        let mut base = ORSet::<String>::new();
        base.set_pid(1);
        base.mutate(OrSetMutation::Insert("apple".into())).unwrap();
        journal
            .append_snapshot(&PersistentReplica {
                local_state: base,
                crdt_wrapper: CRDTWrapper::new(1),
            })
            .expect("failed to append durability snapshot");

        let mut file = OpenOptions::new()
            .append(true)
            .open(DiskJournal::mutation_log_path(&journal_path))
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
        let contents = std::fs::read_to_string(DiskJournal::mutation_log_path(&journal_path))
            .expect("failed to read repaired mutation log");
        assert!(contents.is_empty());

        cleanup_journal(&journal_path);
    }

    #[test]
    fn durability_journal_repairs_a_corrupt_non_tail_suffix() {
        let journal_path = temp_journal_path("corrupt-suffix");
        let journal =
            DiskJournal::new(journal_path.clone()).expect("failed to create durability journal");

        let mut base = ORSet::<String>::new();
        base.set_pid(1);
        base.mutate(OrSetMutation::Insert("apple".into())).unwrap();
        journal
            .append_snapshot(&PersistentReplica {
                local_state: base,
                crdt_wrapper: CRDTWrapper::new(1),
            })
            .expect("failed to append durability snapshot");

        let mut file = OpenOptions::new()
            .append(true)
            .open(DiskJournal::mutation_log_path(&journal_path))
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

        let contents = std::fs::read_to_string(DiskJournal::mutation_log_path(&journal_path))
            .expect("failed to read repaired mutation log");
        assert!(contents.is_empty());

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

        cleanup_journal(&journal_path);
    }

    #[test]
    fn durability_journal_excludes_another_process_owner() {
        let journal_path = temp_journal_path("exclusive-lock");
        let first = DiskJournal::new(journal_path.clone())
            .expect("failed to create first durability journal");

        assert!(matches!(
            DiskJournal::new(journal_path.clone()),
            Err(JournalError::AlreadyLocked(path)) if path == journal_path
        ));

        drop(first);
        DiskJournal::new(journal_path.clone())
            .expect("journal should be available after its owner exits");
        cleanup_journal(&journal_path);
    }

    #[test]
    fn durability_journal_serializes_concurrent_appends() {
        let journal_path = temp_journal_path("concurrent-appends");
        let journal = Arc::new(
            DiskJournal::new(journal_path.clone()).expect("failed to create durability journal"),
        );
        let mut base = ORSet::<String>::new();
        base.set_pid(1);
        journal
            .append_snapshot(&PersistentReplica {
                local_state: base,
                crdt_wrapper: CRDTWrapper::new(1),
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

        let contents = std::fs::read_to_string(DiskJournal::mutation_log_path(&journal_path))
            .expect("failed to read mutation log");
        assert_eq!(contents.lines().count(), 16);
        for line in contents.lines() {
            serde_json::from_str::<RawDurabilityRecord>(line)
                .expect("concurrent append produced malformed JSON");
        }
        journal
            .recover::<ORSet<String>>()
            .expect("concurrent append journal should recover");

        cleanup_journal(&journal_path);
    }
}
