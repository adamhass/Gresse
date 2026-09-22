use crate::crdt::CRDTWrapper;
use crate::dots::Counter;
use crate::prelude::{Pid, ServerAddr};
use crate::replica_helpers::ReplicaDescriptor;
use log::{debug, info, warn};
use rand::Rng;
// use crate::prelude::*;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::{HashMap, HashSet};
use std::env;
use std::fmt::Debug;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{timeout, Instant};

use std::net::SocketAddr;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::{
    io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader, Interest, Lines, ReadHalf, WriteHalf},
    sync::mpsc::{
        channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
    },
};

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CONNECTION_RETRY_BACKOFF: Duration = Duration::from_secs(30);

/// Return one current, connectable descriptor per peer PID.
///
/// Membership descriptors are immutable and a replica publishes a new one
/// whenever its GC marker changes. A final descriptor retires the entire PID.
pub(crate) fn network_connection_candidates<'a>(
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

struct EstablishedConnection<T> {
    peer_pid: Pid,
    writer: Sender<T>,
    reader_task: JoinHandle<()>,
    writer_task: JoinHandle<()>,
}

enum ConnectionResult<T> {
    Established {
        expected_pid: Option<Pid>,
        connection: EstablishedConnection<T>,
    },
    Failed {
        expected_pid: Option<Pid>,
        detail: String,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct NetworkMember {
    pub pid: Pid,
    pub address: SocketAddr,
    pub final_counter: Option<Counter>,
}

/// Replica-side networking state.
///
/// This owns the channels connecting a [`crate::replica::Replica`] to the
/// network manager, along with the currently available peer writers. Keeping
/// these together prevents transport bookkeeping from leaking into the
/// replica's CRDT and durability logic.
pub(crate) struct ReplicaNetwork<M> {
    local_pid: Pid,
    writers: HashMap<Pid, Sender<M>>,
    replication_receiver: Receiver<M>,
    replication_writer_receiver: Receiver<(Pid, Sender<M>)>,
    member_sender: Sender<NetworkMember>,
    start_sender: Option<oneshot::Sender<()>>,
    shutdown_sender: Option<oneshot::Sender<()>>,
}

impl<M> ReplicaNetwork<M> {
    fn new(
        local_pid: Pid,
        replication_receiver: Receiver<M>,
        replication_writer_receiver: Receiver<(Pid, Sender<M>)>,
        member_sender: Sender<NetworkMember>,
        start_sender: oneshot::Sender<()>,
        shutdown_sender: oneshot::Sender<()>,
    ) -> Self {
        Self {
            local_pid,
            writers: HashMap::new(),
            replication_receiver,
            replication_writer_receiver,
            member_sender,
            start_sender: Some(start_sender),
            shutdown_sender: Some(shutdown_sender),
        }
    }

    pub(crate) fn start(&mut self) {
        if let Some(sender) = self.start_sender.take() {
            let _ = sender.send(());
        }
    }

    pub(crate) fn shutdown(&mut self) {
        if let Some(sender) = self.shutdown_sender.take() {
            let _ = sender.send(());
        }
        self.clear_writers();
    }

    /// Wait for the next message from a peer while absorbing newly established
    /// peer writers into the network state.
    pub(crate) async fn poll(&mut self) -> Option<M> {
        loop {
            tokio::select! {
                remote_event = self.replication_receiver.recv() => {
                    return remote_event;
                }
                new_writer = self.replication_writer_receiver.recv() => {
                    let Some((pid, writer)) = new_writer else {
                        return self.replication_receiver.recv().await;
                    };
                    info!(
                        "replica {} established peer replication connection with replica {}",
                        self.local_pid,
                        pid
                    );
                    self.add_writer(pid, writer);
                }
            }
        }
    }

    pub(crate) fn peer_count(&self) -> usize {
        self.writers.len()
    }

    pub(crate) fn add_writer(&mut self, pid: Pid, writer: Sender<M>) {
        self.writers.insert(pid, writer);
    }

    pub(crate) fn remove_writer(&mut self, pid: Pid) {
        self.writers.remove(&pid);
    }

    pub(crate) fn writer(&self, pid: Pid) -> Option<Sender<M>> {
        self.writers.get(&pid).cloned()
    }

    pub(crate) fn random_writer(&self) -> Option<(Pid, Sender<M>)> {
        if self.writers.is_empty() {
            return None;
        }
        let index = rand::rng().random_range(0..self.writers.len());
        self.writers
            .iter()
            .nth(index)
            .map(|(pid, writer)| (*pid, writer.clone()))
    }

    fn clear_writers(&mut self) {
        self.writers.clear();
    }

    pub(crate) fn enqueue_members(
        &mut self,
        members: &[ReplicaDescriptor],
        crdt_wrapper: &mut CRDTWrapper,
    ) {
        let departed_pids = members
            .iter()
            .filter(|descriptor| descriptor.is_shutdown())
            .map(|descriptor| descriptor.pid)
            .collect::<HashSet<_>>();
        let mut discovered_gc_counters = HashMap::<Pid, Counter>::new();
        for descriptor in members {
            if descriptor.pid == self.local_pid
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
            crdt_wrapper.observe_active_replica(pid, gc_counter);
        }

        let connected_pids = self.writers.keys().copied().collect::<HashSet<_>>();
        for descriptor in network_connection_candidates(members, self.local_pid, &connected_pids) {
            debug!(
                "replica {} discovering member pid={} addr={} gc_counter={}",
                self.local_pid, descriptor.pid, descriptor.address, descriptor.gc_counter
            );
            if let Err(error) = self.member_sender.try_send(NetworkMember {
                pid: descriptor.pid,
                address: descriptor.address,
                final_counter: descriptor.final_counter,
            }) {
                warn!(
                    "replica {} deferred discovery of peer {}: {}",
                    self.local_pid, descriptor.pid, error
                );
            }
        }
    }

    pub(crate) fn notify_departure(&mut self, descriptor: ReplicaDescriptor) {
        self.remove_writer(descriptor.pid);
        if let Err(error) = self.member_sender.try_send(NetworkMember {
            pid: descriptor.pid,
            address: descriptor.address,
            final_counter: descriptor.final_counter,
        }) {
            warn!(
                "replica {} deferred shutdown notification for {}: {}",
                self.local_pid, descriptor.pid, error
            );
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct NetworkLatencyProfile {
    base_latency: Duration,
    jitter: Duration,
}

impl NetworkLatencyProfile {
    fn from_env() -> Self {
        Self {
            base_latency: env_duration_ms("GRESSE_REPLICA_NETWORK_LATENCY_MS"),
            jitter: env_duration_ms("GRESSE_REPLICA_NETWORK_LATENCY_JITTER_MS"),
        }
    }

    fn is_enabled(&self) -> bool {
        !self.base_latency.is_zero() || !self.jitter.is_zero()
    }

    fn sample_delay(&self) -> Duration {
        if self.jitter.is_zero() {
            return self.base_latency;
        }

        let base_ms = self.base_latency.as_millis() as u64;
        let jitter_ms = self.jitter.as_millis() as u64;
        let lower = base_ms.saturating_sub(jitter_ms);
        let upper = base_ms.saturating_add(jitter_ms);
        Duration::from_millis(rand::rng().random_range(lower..=upper))
    }
}

impl NetworkMember {
    pub fn is_shutdown(&self) -> bool {
        self.final_counter.is_some()
    }
}

/// Network manager for a server, handles the systems internal network connections
/// It listens for incoming connections and connects to members received from the replica.
pub struct NetworkManager<T> {
    pub local_event_sender: Sender<T>,
    pub connection_sender: Sender<(Pid, Sender<T>)>,
    pub member_receiver: Receiver<NetworkMember>,
    pub listener: TcpListener,
    pub pid: Pid,
    pub address: ServerAddr,
    dead_members: HashSet<Pid>,
    connected_members: HashSet<Pid>,
    connecting_members: HashSet<Pid>,
    connection_retry_at: HashMap<Pid, Instant>,
    connection_failures: HashMap<Pid, u32>,
    connection_result_sender: UnboundedSender<ConnectionResult<T>>,
    connection_result_receiver: UnboundedReceiver<ConnectionResult<T>>,
    latency_profile: NetworkLatencyProfile,
    start_receiver: Option<oneshot::Receiver<()>>,
    // For graceful shutdown
    shutdown_receiver: Option<oneshot::Receiver<()>>,
    task_handles: Vec<JoinHandle<()>>,
}

impl<T: Send + 'static + Serialize + DeserializeOwned + Debug + Sync> NetworkManager<T> {
    pub async fn launch_network_manager(address: ServerAddr, pid: Pid) -> ReplicaNetwork<T> {
        let (local_event_sender, local_event_receiver) = channel::<T>(100);
        let (connection_sender, connection_receiver) = channel::<(Pid, Sender<T>)>(100);
        let (member_sender, member_receiver) = channel::<NetworkMember>(100);
        let (connection_result_sender, connection_result_receiver) = unbounded_channel();
        let listener = TcpListener::bind(address.internal())
            .await
            .expect("Failed to bind to internal address");
        let latency_profile = NetworkLatencyProfile::from_env();
        if latency_profile.is_enabled() {
            info!(
                "replica {} enabling network latency emulation: base={}ms jitter={}ms",
                pid,
                latency_profile.base_latency.as_millis(),
                latency_profile.jitter.as_millis(),
            );
        }
        // Create shutdown channel
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
        let (start_sender, start_receiver) = oneshot::channel::<()>();

        let mut this = NetworkManager::<T> {
            local_event_sender,
            connection_sender,
            member_receiver,
            listener,
            pid,
            address,
            dead_members: HashSet::new(),
            connected_members: HashSet::new(),
            connecting_members: HashSet::new(),
            connection_retry_at: HashMap::new(),
            connection_failures: HashMap::new(),
            connection_result_sender,
            connection_result_receiver,
            latency_profile,
            start_receiver: Some(start_receiver),
            shutdown_receiver: Some(shutdown_receiver),
            task_handles: Vec::new(),
        };
        tokio::spawn(async move {
            this.run().await;
        });
        ReplicaNetwork::new(
            pid,
            local_event_receiver,
            connection_receiver,
            member_sender,
            start_sender,
            shutdown_sender,
        )
    }

    pub async fn run(&mut self) {
        // Take ownership of shutdown_receiver from self
        let mut shutdown_receiver = self
            .shutdown_receiver
            .take()
            .expect("Failed to take shutdown receiver");
        let mut start_receiver = self
            .start_receiver
            .take()
            .expect("Failed to take network start receiver");

        // Bind the internal socket during construction, but do not accept or
        // initiate peer connections until replica bootstrap has completed.
        // This keeps inbound anti-entropy from contending with recovery.
        tokio::select! {
            _ = &mut start_receiver => {}
            _ = &mut shutdown_receiver => {
                info!("network manager for replica {} shutdown before startup", self.pid);
                return;
            }
        }

        loop {
            tokio::select! {
                accept_result = self.listener.accept() => {
                    match accept_result {
                        Ok((stream, _)) => {
                            self.spawn_connection_attempt(None, stream);
                        }
                        Err(error) => warn!("replica {} failed to accept peer connection: {}", self.pid, error),
                    }
                }
                Some(member) = self.member_receiver.recv() => {
                    self.handle_new_member(member);
                }
                Some(result) = self.connection_result_receiver.recv() => {
                    self.handle_connection_result(result);
                }
                Ok(()) = &mut shutdown_receiver => {
                    break;
                }
            }
        }
        for handle in self.task_handles.iter() {
            handle.abort();
        }
        info!("network manager for replica {} shutdown complete", self.pid);
    }

    pub fn handle_new_member(&mut self, member: NetworkMember) {
        if member.is_shutdown() {
            self.dead_members.insert(member.pid);
            self.connecting_members.remove(&member.pid);
            self.connected_members.remove(&member.pid);
            self.connection_retry_at.remove(&member.pid);
            self.connection_failures.remove(&member.pid);
            return;
        }

        // Ensure there's no self connection, or mutual connection attempts
        if member.pid == self.pid
            || member.address == self.address.internal()
            || member.address < self.address.internal()
            || self.dead_members.contains(&member.pid)
            || self.connected_members.contains(&member.pid)
            || self.connecting_members.contains(&member.pid)
            || self
                .connection_retry_at
                .get(&member.pid)
                .is_some_and(|retry_at| *retry_at > Instant::now())
        {
            return;
        }
        info!(
            "replica {} discovered network member {:?}",
            self.pid, member
        );
        self.connecting_members.insert(member.pid);
        let result_sender = self.connection_result_sender.clone();
        let local_event_sender = self.local_event_sender.clone();
        let latency_profile = self.latency_profile;
        let local_pid = self.pid;
        let handle = tokio::spawn(async move {
            let stream = match timeout(CONNECTION_TIMEOUT, TcpStream::connect(member.address)).await
            {
                Ok(Ok(stream)) => stream,
                Ok(Err(error)) => {
                    let _ = result_sender.send(ConnectionResult::Failed {
                        expected_pid: Some(member.pid),
                        detail: format!("could not connect to {}: {error}", member.address),
                    });
                    return;
                }
                Err(_) => {
                    let _ = result_sender.send(ConnectionResult::Failed {
                        expected_pid: Some(member.pid),
                        detail: format!("connection to {} timed out", member.address),
                    });
                    return;
                }
            };
            let result = Self::establish_connection(
                stream,
                local_pid,
                Some(member.pid),
                local_event_sender,
                latency_profile,
            )
            .await;
            let _ = result_sender.send(result);
        });
        self.task_handles.push(handle);
    }

    fn spawn_connection_attempt(&mut self, expected_pid: Option<Pid>, stream: TcpStream) {
        let result_sender = self.connection_result_sender.clone();
        let local_event_sender = self.local_event_sender.clone();
        let latency_profile = self.latency_profile;
        let local_pid = self.pid;
        let handle = tokio::spawn(async move {
            let result = Self::establish_connection(
                stream,
                local_pid,
                expected_pid,
                local_event_sender,
                latency_profile,
            )
            .await;
            let _ = result_sender.send(result);
        });
        self.task_handles.push(handle);
    }

    fn handle_connection_result(&mut self, result: ConnectionResult<T>) {
        match result {
            ConnectionResult::Failed {
                expected_pid,
                detail,
            } => {
                if let Some(pid) = expected_pid {
                    self.connecting_members.remove(&pid);
                    let failures = self.connection_failures.entry(pid).or_default();
                    *failures = failures.saturating_add(1);
                    let multiplier = 1u32 << (*failures).min(5);
                    let delay =
                        Duration::from_secs(multiplier as u64).min(MAX_CONNECTION_RETRY_BACKOFF);
                    self.connection_retry_at.insert(pid, Instant::now() + delay);
                }
                warn!(
                    "replica {} peer connection attempt failed: {}",
                    self.pid, detail
                );
            }
            ConnectionResult::Established {
                expected_pid,
                connection,
            } => {
                if let Some(expected_pid) = expected_pid {
                    self.connecting_members.remove(&expected_pid);
                    self.connection_retry_at.remove(&expected_pid);
                    self.connection_failures.remove(&expected_pid);
                    if connection.peer_pid != expected_pid {
                        warn!(
                            "replica {} rejected peer handshake: expected {}, received {}",
                            self.pid, expected_pid, connection.peer_pid
                        );
                        connection.reader_task.abort();
                        connection.writer_task.abort();
                        return;
                    }
                }
                if self.dead_members.contains(&connection.peer_pid)
                    || !self.connected_members.insert(connection.peer_pid)
                {
                    connection.reader_task.abort();
                    connection.writer_task.abort();
                    return;
                }
                match self
                    .connection_sender
                    .try_send((connection.peer_pid, connection.writer))
                {
                    Ok(()) => {
                        self.task_handles.push(connection.reader_task);
                        self.task_handles.push(connection.writer_task);
                        info!(
                            "replica {} registered bidirectional network channel for replica {}",
                            self.pid, connection.peer_pid
                        );
                    }
                    Err(error) => {
                        self.connected_members.remove(&connection.peer_pid);
                        connection.reader_task.abort();
                        connection.writer_task.abort();
                        warn!(
                            "replica {} deferred peer {} registration: {}",
                            self.pid, connection.peer_pid, error
                        );
                    }
                }
            }
        }
        self.task_handles.retain(|handle| !handle.is_finished());
    }

    /// Establish a peer stream without blocking the network-manager loop.
    async fn establish_connection(
        mut stream: TcpStream,
        local_pid: Pid,
        expected_pid: Option<Pid>,
        local_event_sender: Sender<T>,
        latency_profile: NetworkLatencyProfile,
    ) -> ConnectionResult<T> {
        // Tell the stream who we are
        let handshake = async {
            stream.write_u128(local_pid).await?;
            stream.read_u128().await
        };
        let pid = match timeout(CONNECTION_TIMEOUT, handshake).await {
            Ok(Ok(pid)) => pid,
            Ok(Err(error)) => {
                return ConnectionResult::Failed {
                    expected_pid,
                    detail: format!("handshake failed: {error}"),
                };
            }
            Err(_) => {
                return ConnectionResult::Failed {
                    expected_pid,
                    detail: "handshake timed out".to_string(),
                };
            }
        };
        // Find out who it is on the opposite end
        if let Err(error) = stream.ready(Interest::READABLE | Interest::WRITABLE).await {
            return ConnectionResult::Failed {
                expected_pid,
                detail: format!("peer {pid} was not ready: {error}"),
            };
        }
        let (read_half, stream_writer) = io::split(stream);
        let stream_reader = BufReader::new(read_half).lines();
        let (from_local_sender, from_local_receiver) = channel::<T>(100);
        let reader_task = tokio::spawn(async move {
            Self::read_loop(stream_reader, local_event_sender).await;
        });
        let writer_task = tokio::spawn(async move {
            Self::write_loop(stream_writer, from_local_receiver, latency_profile).await;
        });
        ConnectionResult::Established {
            expected_pid,
            connection: EstablishedConnection {
                peer_pid: pid,
                writer: from_local_sender,
                reader_task,
                writer_task,
            },
        }
    }

    async fn read_loop(mut reader: Lines<BufReader<ReadHalf<TcpStream>>>, sender: Sender<T>) {
        loop {
            let line = match reader.next_line().await {
                Ok(Some(line)) => line,
                Ok(None) => return,
                Err(error) => {
                    warn!("peer connection read failed: {}", error);
                    return;
                }
            };
            let message: T = match serde_json::from_str(&line) {
                Ok(message) => message,
                Err(error) => {
                    warn!("discarding malformed peer message: {}", error);
                    continue;
                }
            };
            debug!("network read loop received message: {:?}", message);
            if sender.send(message).await.is_err() {
                debug!("peer connection reader stopped because local receiver is closed");
                return;
            }
        }
    }

    pub async fn send_message(
        writer: &mut WriteHalf<TcpStream>,
        message: &T,
    ) -> tokio::io::Result<()> {
        let json = serde_json::to_string(message).unwrap();
        writer.write_all(json.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        Ok(())
    }

    async fn write_loop(
        mut writer: WriteHalf<TcpStream>,
        mut receiver: Receiver<T>,
        latency_profile: NetworkLatencyProfile,
    ) {
        while let Some(request) = receiver.recv().await {
            debug!("network write loop sending message: {:?}", request);
            if latency_profile.is_enabled() {
                tokio::time::sleep(latency_profile.sample_delay()).await;
            }
            if let Err(error) = Self::send_message(&mut writer, &request).await {
                warn!("peer connection write failed: {}", error);
                return;
            }
        }
    }
}

fn env_duration_ms(name: &str) -> Duration {
    env::var(name)
        .ok()
        .map(|value| {
            Duration::from_millis(
                value
                    .parse()
                    .unwrap_or_else(|_| panic!("{name} must be an integer number of milliseconds")),
            )
        })
        .unwrap_or_default()
}
