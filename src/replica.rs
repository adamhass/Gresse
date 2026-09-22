use crate::dots::{Counter, Dot, DotSet};
use crate::http_server::{launch_http_server, ClientMutationHandler};
use crate::journal::{DiskJournal, Journal};
use crate::network::{NetworkManager, ReplicaNetwork};
use crate::object_storage::{MembershipPollResult, ObjectStorageClient};
use crate::replica_helpers::*;
use crate::{crdt::*, prelude::Pid};
use log::{debug, info, warn};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::sync::oneshot;
use tokio::sync::{Mutex, RwLock};

pub struct Replica<T: CRDT + Debug + Clone> {
    crdt: Arc<RwLock<T>>,
    crdt_wrapper: CRDTWrapper,
    pid: Pid,
    config: ReplicaRuntimeConfig,
    network: ReplicaNetwork<ReplicaMessage<T>>,
    /// Serializes durable CRDT commits initiated by the HTTP data plane and
    /// replica-side replication handling.
    commit_gate: Arc<Mutex<()>>,
    local_mutation_receiver: UnboundedReceiver<()>,
    object_storage_client: Arc<ObjectStorageClient>,
    durability_journal: Arc<DiskJournal>,
    lifecycle: ReplicaLifecycle,
}

/// Replicated CRDT server. Queries are served from local state while mutations
/// are persisted locally and propagated through periodic delta synchronization.
impl<T: CRDT + 'static + Send + Sync + Debug + Clone> Replica<T> {
    fn effective_pid(requested_pid: Pid, recovered_replica: Option<&PersistentReplica<T>>) -> Pid {
        recovered_replica
            .map(|replica| replica.crdt_wrapper.own_pid())
            .unwrap_or(requested_pid)
    }

    /// Create and bootstrap a new CRDT server from explicit configuration.
    pub async fn with_config(
        pid: Pid,
        crdt: T,
        config: ReplicaConfig,
    ) -> (Self, oneshot::Sender<()>) {
        let durability_journal = Arc::new(DiskJournal::open_or_disabled(
            config.durability_path.clone(),
        ));
        let recovered_replica = durability_journal
            .recover::<T>()
            .unwrap_or_else(|error| panic!("Failed to recover durable replica state: {error}"));
        let recovered_from_durability = recovered_replica.is_some();

        // A durability journal belongs to one logical replica. Reuse that
        // replica's persisted identity instead of assigning a fresh PID after
        // process recovery.
        let pid = Self::effective_pid(pid, recovered_replica.as_ref());
        let (mut effective_crdt, mut crdt_wrapper) = match recovered_replica {
            Some(recovered) => (recovered.local_state, recovered.crdt_wrapper),
            None => (crdt, CRDTWrapper::new(pid)),
        };
        effective_crdt.set_pid(pid);
        crdt_wrapper.rebind_own_pid(pid, effective_crdt.get_version_vector().clone());
        let crdt = Arc::new(RwLock::new(effective_crdt));

        // The HTTP data plane applies local mutations directly.  It must not
        // wait behind membership, GC, or anti-entropy work in Replica::run.
        // The shared gate preserves durable-before-apply ordering with remote
        // delta merges.
        let commit_gate = Arc::new(Mutex::new(()));
        let (local_mutation_sender, local_mutation_receiver) = unbounded_channel();
        let mutation_handler: ClientMutationHandler<T> = {
            let crdt = crdt.clone();
            let journal = durability_journal.clone();
            let commit_gate = commit_gate.clone();
            Arc::new(move |mutation| {
                let crdt = crdt.clone();
                let journal = journal.clone();
                let commit_gate = commit_gate.clone();
                let local_mutation_sender = local_mutation_sender.clone();
                Box::pin(async move {
                    let _commit = commit_gate.lock().await;
                    journal
                        .append_mutation::<T>(&mutation)
                        .unwrap_or_else(|error| {
                            panic!("Failed to append durable client mutation: {error}")
                        });
                    let response = crdt.write().await.mutate(mutation);
                    let _ = local_mutation_sender.send(());
                    response
                })
            })
        };
        let http_shutdown_sender =
            launch_http_server::<T>(&config.address, crdt.clone(), mutation_handler).await;

        let network =
            NetworkManager::<ReplicaMessage<T>>::launch_network_manager(config.address, pid).await;

        let runtime_config = ReplicaRuntimeConfig::new(&config);
        let object_storage_client =
            Arc::new(ObjectStorageClient::new(config.object_storage_config));
        let (lifecycle, shutdown_sender) = ReplicaLifecycle::new(http_shutdown_sender);

        let mut replica = Replica {
            pid,
            config: runtime_config,
            crdt,
            commit_gate,
            network,
            local_mutation_receiver,
            crdt_wrapper,
            lifecycle,
            object_storage_client,
            durability_journal,
        };
        debug!("replica {} initiating bootstrap", replica.pid);
        if recovered_from_durability {
            replica.bootstrap_from_durability().await;
        } else {
            replica.bootstrap_from_object_storage().await;
        }
        info!("replica {} startup completed", replica.pid);

        (replica, shutdown_sender)
    }

    pub async fn run(&mut self) {
        info!(
            "replica {} initiating runtime at bind_http={} bind_internal={} advertised_internal={}",
            self.pid,
            self.config.address().http(),
            self.config.address().internal(),
            self.config.advertised_address().internal(),
        );
        let initial_members = self.object_storage_client.membership_descriptors().await;
        self.network.start();
        self.network
            .enqueue_members(&initial_members, &mut self.crdt_wrapper);

        let mut shutdown_receiver = self.lifecycle.take_shutdown_receiver();

        let mut interval = tokio::time::interval(self.config.sync_interval());
        let membership_poll_client = self.object_storage_client.clone();
        loop {
            let (gc_interval, membership_poll_interval) = self.config.timers_mut();
            tokio::select! {
                Some(()) = self.local_mutation_receiver.recv() => {
                    self.refresh_own_version_vector().await;
                }
                Some(remote_event) = self.network.poll() => {
                    match remote_event {
                        ReplicaMessage::DeltaGroup(delta, gc_metadata) => {
                            self.handle_delta(delta, gc_metadata).await;
                        }
                        ReplicaMessage::VersionVector(pid, version_vector) => {
                            self.handle_version_vector(pid, version_vector).await;
                        }
                    }
                }
                _ = interval.tick() => {
                    self.pull_delta().await;
                }
                _ = gc_interval.tick() => {
                    self.init_gc().await;
                }
                _ = membership_poll_interval.tick() => {
                    self.object_storage_client.start_membership_poll();
                }
                Some(result) = membership_poll_client.recv_membership_poll() => {
                    self.apply_membership_poll(result).await;
                }
                _ = &mut shutdown_receiver => {
                    info!("shutdown signal received for replica {}", self.pid);
                    self.shutdown().await;
                    break;
                }
            }
        }
    }

    async fn bootstrap_from_durability(&mut self) {
        let descriptor = self.replica_descriptor();
        let members = self
            .object_storage_client
            .register_and_list_members(descriptor)
            .await
            .expect("Failed to register and list replica membership");
        info!(
            "replica {} restored local state from durability journal before startup",
            self.pid
        );
        info!(
            "replica {} discovered {} membership descriptors during bootstrap",
            self.pid,
            members.len()
        );
    }

    async fn bootstrap_from_object_storage(&mut self) {
        let descriptor = self.replica_descriptor();
        let (persistent_replica_result, membership_result) = tokio::join!(
            self.object_storage_client
                .read_persistent_replica::<PersistentReplica<T>>(),
            self.object_storage_client
                .register_and_list_members(descriptor)
        );

        let persistent_replica = persistent_replica_result
            .unwrap_or_else(|error| panic!("Failed to read persistent replica state: {error}"));
        let members = membership_result.expect("Failed to register and list replica membership");

        match persistent_replica {
            Some(mut persistent_replica) => {
                debug!("replica {} restoring persistent replica state", self.pid);
                let commit_gate = self.commit_gate.clone();
                let _commit = commit_gate.lock().await;
                persistent_replica.rebind_own_pid(self.pid);
                self.durability_journal.append_snapshot(&persistent_replica);
                *self.crdt.write().await = persistent_replica.local_state;
                self.crdt_wrapper = persistent_replica.crdt_wrapper;
                // Object-storage state is used to bootstrap a new identity.
                // The rebound snapshot ensures later journal recovery retains
                // this replica's PID.
            }
            None => {
                debug!(
                    "replica {} found no persistent replica state; writing initial snapshot",
                    self.pid
                );
                self.write_initial_persistent_replica().await;
                self.persist_durable_snapshot().await;
            }
        }
        info!(
            "replica {} discovered {} membership descriptors during bootstrap",
            self.pid,
            members.len()
        );
    }

    // Membership directory interactions

    async fn apply_membership_poll(&mut self, result: MembershipPollResult) {
        self.object_storage_client.finish_membership_poll().await;
        let members = match result.members {
            Ok(members) => members,
            Err(error) => {
                warn!("replica {} membership poll failed: {}", self.pid, error);
                return;
            }
        };
        self.object_storage_client
            .update_membership_cache(members.clone())
            .await;
        let previous_peer_count = self.network.peer_count();
        debug!(
            "replica {} scanning membership directory for new members and shutdown descriptors",
            self.pid
        );
        self.network
            .enqueue_members(&members, &mut self.crdt_wrapper);
        for descriptor in members
            .iter()
            .copied()
            .filter(ReplicaDescriptor::is_shutdown)
        {
            self.handle_shutdown_descriptor(descriptor).await;
        }

        let peer_count = self.network.peer_count();
        if peer_count != previous_peer_count {
            self.config.reschedule_gc(peer_count);
        }
    }

    async fn handle_shutdown_descriptor(&mut self, descriptor: ReplicaDescriptor) {
        let final_counter = descriptor
            .final_counter
            .expect("shutdown descriptor missing final counter");
        let final_dot = Dot {
            pid: descriptor.pid,
            counter: final_counter,
        };
        if !self.crdt_wrapper.should_fetch_departed_replica(final_dot) {
            return;
        }
        info!(
            "replica {} observed shutdown descriptor for replica {} with final counter {:?}",
            self.pid, descriptor.pid, final_counter
        );
        if !self.merge_shutdown_payload(descriptor).await {
            return;
        }
        // Keep the replica row until its final dot is stable everywhere.
        self.crdt_wrapper.observe_departed_replica(final_dot);

        self.network.notify_departure(descriptor);
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

        let local_version_vector = self.crdt.read().await.get_version_vector().clone();
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
            self.durability_journal.append_delta_group::<T>(&delta);
            self.crdt.write().await.merge_delta_group(delta);
        }
        self.refresh_own_version_vector().await;
        true
    }

    // * * * Replication * * *

    async fn handle_delta(
        &mut self,
        delta: DeltaGroup<T::Delta, T::SideEffects>,
        gc_metadata: GcMarker,
    ) {
        let remote_gc_counter = *delta.version_vector.gc_counter();
        let current_gc_counter = self.crdt_wrapper.current_gc_marker().counter;
        if remote_gc_counter < current_gc_counter {
            debug!(
                "replica {} refusing remote delta group with GC counter={} below current GC counter={}",
                self.pid, remote_gc_counter, current_gc_counter
            );
            return;
        }
        self.observe_gc(gc_metadata).await;
        let delta_len = delta.list.len();
        {
            let _commit = self.commit_gate.lock().await;
            self.durability_journal.append_delta_group::<T>(&delta);
            self.crdt.write().await.merge_delta_group(delta);
        }
        self.refresh_own_version_vector().await;
        debug!(
            "replica {} merging remote delta group with {} entries",
            self.pid, delta_len
        );
    }

    async fn handle_version_vector(&mut self, pid: Pid, version_vector: DotSet) {
        self.crdt_wrapper
            .version_matrix
            .update(pid, version_vector.clone());
        let mut delta = {
            let readable = self.crdt.read().await;
            readable.get_delta(&version_vector)
        };
        delta.version_vector = self
            .crdt_wrapper
            .update_own_version_vector(delta.version_vector);
        let delta_len = delta.list.len();
        debug!(
            "replica {} get_delta for replica {} produced {} deltas",
            self.pid, pid, delta_len
        );
        let Some(writer) = self.network.writer(pid) else {
            debug!(
                "replica {} has no writer for peer {}; response will be retried by anti-entropy",
                self.pid, pid
            );
            return;
        };
        let message = ReplicaMessage::DeltaGroup(delta, self.crdt_wrapper.gc_metadata());
        if let Err(error) = writer.try_send(message) {
            if matches!(error, tokio::sync::mpsc::error::TrySendError::Closed(_)) {
                self.network.remove_writer(pid);
            }
            // Anti-entropy is periodic: dropping one response when a peer
            // queue is full avoids blocking unrelated work.
            warn!(
                "replica {} deferred delta response to peer {}: {}",
                self.pid, pid, error
            );
        }
    }

    async fn pull_delta(&mut self) {
        let Some((pid, writer)) = self.network.random_writer() else {
            return;
        };
        let version_vector = self.refresh_own_version_vector().await;
        debug!(
            "replica {} initiating pull_delta with {} connected peers",
            self.pid,
            self.network.peer_count()
        );
        if let Err(error) = writer.try_send(ReplicaMessage::VersionVector(self.pid, version_vector))
        {
            if matches!(error, tokio::sync::mpsc::error::TrySendError::Closed(_)) {
                self.network.remove_writer(pid);
            }
            warn!(
                "replica {} deferred pull request to peer {}: {}",
                self.pid, pid, error
            );
        }
    }

    // * * * Garbage collection * * *

    async fn init_gc(&mut self) {
        self.refresh_own_version_vector().await;
        if !self.crdt_wrapper.needs_gc() {
            return;
        }
        let (Some(stable), departed_pids) = self.crdt_wrapper.version_matrix.get_stable() else {
            return;
        };

        let previous_members = self.object_storage_client.membership_descriptors().await;
        let previous_gc_counters = Self::membership_gc_counters_by_pid(&previous_members);
        let previous_descriptor = self.replica_descriptor();

        // 1. Increment our GC marker in the Membership directory before GC.
        let new_marker = self.crdt_wrapper.next_gc_marker();
        let Some(new_descriptor) = self.change_own_gc_counter(new_marker.counter).await else {
            return;
        };

        if !self
            .validate_gc_membership(&previous_gc_counters, new_descriptor)
            .await
        {
            return;
        }

        let gc_marker = GcMarker {
            marker: new_marker,
            stable,
            departed_pids,
        };
        if !self.complete_local_gc(gc_marker).await {
            return;
        }

        // Remove our old Membership Descriptor
        self.object_storage_client
            .delete_membership_descriptor(previous_descriptor)
            .await;
    }

    async fn complete_local_gc(&mut self, marker: GcMarker) -> bool {
        info!(
            "replica {} gc departed_pids={:?} updated_stable={:?}",
            self.pid, marker.departed_pids, marker.stable
        );
        let departed_pids = marker.departed_pids.clone();
        let persistent_replica = self.apply_gc_marker(marker).await;
        if !self
            .object_storage_client
            .write_persistent_replica(&persistent_replica)
            .await
        {
            return false;
        }

        self.persist_durable_snapshot().await;
        if let Some(departed_pids) = departed_pids {
            self.gc_departed_pids(departed_pids).await;
        }
        info!("replica {} completed gc round", self.pid);
        true
    }

    async fn gc_departed_pids(&self, departed_pids: Vec<Pid>) {
        futures::future::join_all(departed_pids.into_iter().map(|pid| {
            self.object_storage_client
                .delete_membership_descriptors_for_pid(pid)
        }))
        .await;
    }

    async fn change_own_gc_counter(&mut self, gc_counter: Counter) -> Option<ReplicaDescriptor> {
        let descriptor = ReplicaDescriptor {
            pid: self.pid,
            address: self.config.advertised_address().internal(),
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
        {
            let commit_gate = self.commit_gate.clone();
            let _commit = commit_gate.lock().await;
            let snapshot = self.apply_gc_marker(metadata).await;
            self.durability_journal.append_snapshot(&snapshot);
        }
        self.config.reset_gc_interval();
    }

    async fn apply_gc_marker(&mut self, marker: GcMarker) -> PersistentReplica<T> {
        let local_state = {
            let mut writable = self.crdt.write().await;
            writable.gc(&marker.stable, &marker.departed_pids);
            writable.clone()
        };
        self.crdt_wrapper.push_gc_marker(marker);
        PersistentReplica {
            local_state,
            crdt_wrapper: self.crdt_wrapper.clone(),
        }
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

    async fn validate_gc_membership(
        &self,
        previous: &HashMap<Pid, Counter>,
        new_descriptor: ReplicaDescriptor,
    ) -> bool {
        let Some(current_members) = self
            .object_storage_client
            .list_membership_descriptors()
            .await
        else {
            self.object_storage_client
                .delete_membership_descriptor(new_descriptor)
                .await;
            return false;
        };
        let current = Self::membership_gc_counters_by_pid(&current_members);
        let is_stable = current.into_iter().all(|(pid, counter)| {
            pid == self.pid || previous.get(&pid).is_some_and(|old| counter <= *old)
        });
        if is_stable {
            return true;
        }

        debug!(
            "replica {} aborted gc round because membership changed",
            self.pid
        );
        self.object_storage_client
            .delete_membership_descriptor(new_descriptor)
            .await;
        let _ = self
            .object_storage_client
            .list_membership_descriptors()
            .await;
        false
    }

    // State and lifecycle helpers
    fn replica_descriptor(&self) -> ReplicaDescriptor {
        ReplicaDescriptor {
            pid: self.pid,
            address: self.config.advertised_address().internal(),
            gc_counter: self.crdt_wrapper.current_gc_marker().counter,
            final_counter: None,
        }
    }

    async fn refresh_own_version_vector(&mut self) -> DotSet {
        let version_vector = self.crdt.read().await.get_version_vector().clone();
        self.crdt_wrapper.update_own_version_vector(version_vector)
    }

    async fn write_initial_persistent_replica(&self) {
        let persistent_replica = PersistentReplica {
            local_state: self.crdt.read().await.clone(),
            crdt_wrapper: self.crdt_wrapper.clone(),
        };
        self.object_storage_client
            .write_persistent_replica(&persistent_replica)
            .await;
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
        self.durability_journal.append_snapshot(&snapshot);
    }

    async fn write_shutdown_descriptor(&self) {
        let local_state = self.crdt.read().await.clone();
        let final_counter = local_state
            .get_version_vector()
            .counter(&self.pid)
            .unwrap_or(-1);
        let descriptor = ReplicaDescriptor {
            pid: self.pid,
            address: self.config.advertised_address().internal(),
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
            self.network.peer_count()
        );

        self.write_shutdown_descriptor().await;
        self.object_storage_client.abort_membership_poll().await;
        self.lifecycle.stop_http_server();
        self.network.shutdown();

        debug!("replica {} shutdown complete", self.pid);
    }
}

#[cfg(test)]
mod tests;
