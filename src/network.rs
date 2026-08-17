use crate::dots::Counter;
use crate::prelude::{Pid, ServerAddr};
use log::{debug, info, warn};
use rand::Rng;
// use crate::prelude::*;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashSet;
use std::env;
use std::fmt::Debug;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use std::net::SocketAddr;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::{
    io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader, Interest, Lines, ReadHalf, WriteHalf},
    sync::mpsc::{channel, Receiver, Sender},
};

#[derive(Debug, Clone, Copy)]
pub struct NetworkMember {
    pub pid: Pid,
    pub address: SocketAddr,
    pub final_counter: Option<Counter>,
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
    latency_profile: NetworkLatencyProfile,
    // For graceful shutdown
    shutdown_receiver: Option<oneshot::Receiver<()>>,
    task_handles: Vec<JoinHandle<()>>,
}

impl<T: Send + 'static + Serialize + DeserializeOwned + Debug + Sync> NetworkManager<T> {
    pub async fn launch_network_manager(
        address: ServerAddr,
        pid: Pid,
    ) -> (
        Receiver<T>,
        Receiver<(Pid, Sender<T>)>,
        Sender<NetworkMember>,
        oneshot::Sender<()>,
    ) {
        let (local_event_sender, local_event_receiver) = channel::<T>(100);
        let (connection_sender, connection_receiver) = channel::<(Pid, Sender<T>)>(100);
        let (member_sender, member_receiver) = channel::<NetworkMember>(100);
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

        let mut this = NetworkManager::<T> {
            local_event_sender,
            connection_sender,
            member_receiver,
            listener,
            pid,
            address,
            dead_members: HashSet::new(),
            latency_profile,
            shutdown_receiver: Some(shutdown_receiver),
            task_handles: Vec::new(),
        };
        tokio::spawn(async move {
            this.run().await;
        });
        (
            local_event_receiver,
            connection_receiver,
            member_sender,
            shutdown_sender,
        )
    }

    pub async fn run(&mut self) {
        // Take ownership of shutdown_receiver from self
        let mut shutdown_receiver = self
            .shutdown_receiver
            .take()
            .expect("Failed to take shutdown receiver");

        loop {
            tokio::select! {
                accept_result = self.listener.accept() => {
                    match accept_result {
                        Ok((stream, _)) => {
                            if let Err(error) = self.handle_new_stream(stream).await {
                                warn!("replica {} ignored failed incoming peer handshake: {}", self.pid, error);
                            }
                        }
                        Err(error) => warn!("replica {} failed to accept peer connection: {}", self.pid, error),
                    }
                }
                Some(member) = self.member_receiver.recv() => {
                    self.handle_new_member(member).await;
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

    pub async fn handle_new_member(&mut self, member: NetworkMember) {
        if member.is_shutdown() {
            self.dead_members.insert(member.pid);
            return;
        }

        // Ensure there's no self connection, or mutual connection attempts
        if member.pid == self.pid
            || member.address == self.address.internal()
            || member.address < self.address.internal()
            || self.dead_members.contains(&member.pid)
        {
            return;
        }
        info!(
            "replica {} discovered network member {:?}",
            self.pid, member
        );
        let stream = match TcpStream::connect(member.address).await {
            Ok(stream) => stream,
            Err(error) => {
                warn!(
                    "replica {} could not connect to discovered peer {} at {}: {}",
                    self.pid, member.pid, member.address, error
                );
                return;
            }
        };
        if let Err(error) = self.handle_new_stream(stream).await {
            warn!(
                "replica {} ignored failed handshake with peer {} at {}: {}",
                self.pid, member.pid, member.address, error
            );
        }
    }

    /// This method can be used both for incoming and outgoing streams
    async fn handle_new_stream(&mut self, mut stream: TcpStream) -> io::Result<()> {
        // Tell the stream who we are
        stream.write_u128(self.pid).await?;
        // Find out who it is on the opposite end
        let pid = stream.read_u128().await?;
        if self.dead_members.contains(&pid) {
            return Ok(());
        }
        info!(
            "replica {} completed network handshake with replica {}",
            self.pid, pid
        );
        stream
            .ready(Interest::READABLE | Interest::WRITABLE)
            .await?;
        let (read_half, stream_writer) = io::split(stream);
        let stream_reader = BufReader::new(read_half).lines();
        let (from_local_sender, from_local_receiver) = channel::<T>(100);
        let sender_clone = self.local_event_sender.clone();
        let handle = tokio::spawn(async move {
            Self::read_loop(stream_reader, sender_clone).await;
        });
        self.task_handles.push(handle);
        let latency_profile = self.latency_profile;
        let handle = tokio::spawn(async move {
            Self::write_loop(stream_writer, from_local_receiver, latency_profile).await;
        });
        self.task_handles.push(handle);
        if self
            .connection_sender
            .send((pid as Pid, from_local_sender))
            .await
            .is_err()
        {
            warn!(
                "replica {} dropped peer {} because its local connection receiver is closed",
                self.pid, pid
            );
            return Ok(());
        }
        info!(
            "replica {} registered bidirectional network channel for replica {}",
            self.pid, pid
        );
        Ok(())
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
