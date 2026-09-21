use crate::crdt::CRDT;
use crate::dots::Counter;
use crate::prelude::{ObjectStorageConfig, Pid, ServerAddr};
use rand::Rng;
use std::env;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::{Instant, Interval};

const DEFAULT_GC_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub struct ReplicaConfig {
    /// Address on which the local HTTP and replication listeners bind.
    pub address: ServerAddr,
    /// Address published in membership descriptors. This defaults to `address`,
    /// but may differ when a replica binds `0.0.0.0` behind NAT or a tunnel.
    pub advertised_address: ServerAddr,
    pub sync_interval: Duration,
    pub gc_interval: Duration,
    pub durability_path: Option<PathBuf>,
    pub object_storage_config: ObjectStorageConfig,
}

impl ReplicaConfig {
    /// Load replica, storage, durability, synchronization, and GC settings
    /// from the `GRESSE_*` environment variables documented in the README.
    pub fn from_env() -> Self {
        let durable = env_bool("GRESSE_DURABLE").unwrap_or(false);
        let address = ServerAddr::from_env();
        let advertised_address = env_optional_string("GRESSE_ADVERTISE_ADDR")
            .map(|ip| {
                address.with_ip(
                    ip.parse()
                        .expect("GRESSE_ADVERTISE_ADDR must be an IP address"),
                )
            })
            .unwrap_or(address);
        Self {
            address,
            advertised_address,
            durability_path: if durable {
                Some(env_path("GRESSE_DURABILITY_PATH"))
            } else {
                None
            },
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
            gc_interval: env::var("GRESSE_GC_INTERVAL_MS")
                .ok()
                .map(|value| {
                    Duration::from_millis(
                        value
                            .parse()
                            .expect("GRESSE_GC_INTERVAL_MS must be an integer"),
                    )
                })
                .unwrap_or(DEFAULT_GC_INTERVAL),
            object_storage_config: object_storage_config_from_env(),
        }
    }
}

pub(crate) struct ReplicaRuntimeConfig {
    /// Local listener address.
    address: ServerAddr,
    /// Routable listener address written to membership descriptors.
    advertised_address: ServerAddr,
    sync_interval: Duration,
    gc_base_interval: Duration,
    gc_interval: Interval,
    membership_poll_interval: Interval,
}

impl ReplicaRuntimeConfig {
    pub(crate) fn new(config: &ReplicaConfig) -> Self {
        let gc_base_interval = config.gc_interval;
        Self {
            address: config.address,
            advertised_address: config.advertised_address,
            sync_interval: config.sync_interval,
            gc_base_interval,
            gc_interval: Self::interval(gc_base_interval),
            membership_poll_interval: Self::interval(
                config.object_storage_config.discovery_interval,
            ),
        }
    }

    fn interval(period: Duration) -> Interval {
        tokio::time::interval_at(Instant::now() + period, period)
    }

    pub(crate) fn address(&self) -> ServerAddr {
        self.address
    }

    pub(crate) fn advertised_address(&self) -> ServerAddr {
        self.advertised_address
    }

    pub(crate) fn sync_interval(&self) -> Duration {
        self.sync_interval
    }

    pub(crate) fn timers_mut(&mut self) -> (&mut Interval, &mut Interval) {
        (&mut self.gc_interval, &mut self.membership_poll_interval)
    }

    pub(crate) fn reset_gc_interval(&mut self) {
        self.gc_interval.reset();
    }

    pub(crate) fn reschedule_gc(&mut self, peer_count: usize) {
        let replica_count = peer_count.saturating_add(1);
        let base_nanos = self.gc_base_interval.as_nanos();
        if base_nanos == 0 {
            self.gc_interval = Self::interval(Duration::from_nanos(1));
            return;
        }

        let jitter_nanos = base_nanos / replica_count as u128;
        let lower_bound = base_nanos.saturating_sub(jitter_nanos).max(1);
        let upper_bound = base_nanos.saturating_add(jitter_nanos).max(lower_bound + 1);
        let interval_nanos = rand::rng().random_range(lower_bound..=upper_bound);
        let interval_nanos = interval_nanos.min(u64::MAX as u128) as u64;

        self.gc_interval = Self::interval(Duration::from_nanos(interval_nanos));
    }
}

/// Coordinates the lifecycle signals for the replica and its child services.
pub(crate) struct ReplicaLifecycle {
    shutdown_receiver: Option<oneshot::Receiver<()>>,
    http_shutdown_sender: Option<oneshot::Sender<()>>,
    network_start_sender: Option<oneshot::Sender<()>>,
    network_shutdown_sender: Option<oneshot::Sender<()>>,
}

impl ReplicaLifecycle {
    pub(crate) fn new(
        shutdown_receiver: oneshot::Receiver<()>,
        http_shutdown_sender: oneshot::Sender<()>,
        network_start_sender: oneshot::Sender<()>,
        network_shutdown_sender: oneshot::Sender<()>,
    ) -> Self {
        Self {
            shutdown_receiver: Some(shutdown_receiver),
            http_shutdown_sender: Some(http_shutdown_sender),
            network_start_sender: Some(network_start_sender),
            network_shutdown_sender: Some(network_shutdown_sender),
        }
    }

    pub(crate) fn start_network(&mut self) {
        if let Some(sender) = self.network_start_sender.take() {
            let _ = sender.send(());
        }
    }

    pub(crate) fn take_shutdown_receiver(&mut self) -> oneshot::Receiver<()> {
        self.shutdown_receiver
            .take()
            .expect("Failed to take shutdown receiver")
    }

    pub(crate) fn stop_services(&mut self) {
        if let Some(sender) = self.http_shutdown_sender.take() {
            let _ = sender.send(());
        }
        if let Some(sender) = self.network_shutdown_sender.take() {
            let _ = sender.send(());
        }
    }
}

fn object_storage_config_from_env() -> ObjectStorageConfig {
    let local_dir = env_optional_path("GRESSE_OBJECT_STORAGE_LOCAL_DIR");

    ObjectStorageConfig {
        local_dir,
        url: env_optional_string("GRESSE_OBJECT_STORAGE_URL"),
        region: env_string_or_default("GRESSE_OBJECT_STORAGE_REGION", "us-east-1"),
        bucket: env_string_or_default("GRESSE_OBJECT_STORAGE_BUCKET", "gresse"),
        access_key: env_optional_string("GRESSE_OBJECT_STORAGE_ACCESS_KEY"),
        secret_key: env_optional_string("GRESSE_OBJECT_STORAGE_SECRET_KEY"),
        session_token: env_optional_string("GRESSE_OBJECT_STORAGE_SESSION_TOKEN"),
        persistent_replica_path: env_string("GRESSE_PERSISTENT_REPLICA_PATH"),
        membership_directory_path: env_string("GRESSE_MEMBERSHIP_DIRECTORY_PATH"),
        discovery_interval: env::var("GRESSE_OBJECT_STORAGE_DISCOVERY_INTERVAL_MS")
            .ok()
            .map(|value| {
                Duration::from_millis(
                    value
                        .parse()
                        .expect("GRESSE_OBJECT_STORAGE_DISCOVERY_INTERVAL_MS must be an integer"),
                )
            })
            .unwrap_or(Duration::from_secs(1)),
    }
}

fn env_string(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} environment variable is required"))
}

fn env_optional_string(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn env_string_or_default(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_path(name: &str) -> PathBuf {
    env::var(name)
        .map(PathBuf::from)
        .unwrap_or_else(|_| panic!("{name} environment variable is required"))
}

fn env_optional_path(name: &str) -> Option<PathBuf> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn env_bool(name: &str) -> Option<bool> {
    env::var(name)
        .ok()
        .map(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => panic!("{name} must be a boolean value"),
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaDescriptor {
    pub pid: Pid,
    pub address: SocketAddr,
    pub gc_counter: Counter,
    pub final_counter: Option<Counter>,
}

impl ReplicaDescriptor {
    pub fn object_path(&self, membership_directory_path: &str) -> String {
        let membership_directory = membership_directory_path.trim_end_matches('/');
        format!("{}/{}", membership_directory, self)
    }

    pub fn is_shutdown(&self) -> bool {
        self.final_counter.is_some()
    }
}

impl fmt::Display for ReplicaDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{},{},{}",
            self.pid, self.address, self.gc_counter
        )?;
        if let Some(final_counter) = self.final_counter {
            write!(formatter, ",{}", final_counter)?;
        }
        Ok(())
    }
}

impl FromStr for ReplicaDescriptor {
    type Err = ReplicaDescriptorParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let descriptor = value.rsplit('/').next().unwrap_or(value);
        let mut parts = descriptor.split(',');
        let pid = parts
            .next()
            .ok_or(ReplicaDescriptorParseError)?
            .parse()
            .map_err(|_| ReplicaDescriptorParseError)?;
        let address = parts
            .next()
            .ok_or(ReplicaDescriptorParseError)?
            .parse()
            .map_err(|_| ReplicaDescriptorParseError)?;
        let gc_counter = parts
            .next()
            .ok_or(ReplicaDescriptorParseError)?
            .parse()
            .map_err(|_| ReplicaDescriptorParseError)?;
        let final_counter = parts
            .next()
            .map(|value| value.parse().map_err(|_| ReplicaDescriptorParseError))
            .transpose()?;
        if parts.next().is_some() {
            return Err(ReplicaDescriptorParseError);
        }
        Ok(Self {
            pid,
            address,
            gc_counter,
            final_counter,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaDescriptorParseError;

pub type ClientResponder<T> =
    tokio::sync::oneshot::Sender<Result<<T as CRDT>::ClientResponse, <T as CRDT>::Error>>;

pub struct ReplicaHandle {
    pub shutdown_sender: tokio::sync::oneshot::Sender<()>,
    pub join_handle: tokio::task::JoinHandle<()>,
}

impl ReplicaHandle {
    pub fn shutdown_sender(self) -> tokio::sync::oneshot::Sender<()> {
        self.shutdown_sender
    }

    pub async fn shutdown(self) {
        let _ = self.shutdown_sender.send(());
        let _ = self.join_handle.await;
    }
}
