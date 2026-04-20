use crate::crdt::{Epoch, CRDT};
use crate::object_storage::ObjectStorageConfig;
use crate::prelude::{Pid, ServerAddr};
use std::env;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaDescriptor {
    pub pid: Pid,
    pub address: SocketAddr,
    pub epoch: Epoch,
}

impl ReplicaDescriptor {
    pub fn object_path(&self, membership_directory_path: &str) -> String {
        let membership_directory = membership_directory_path.trim_end_matches('/');
        format!("{}/{}", membership_directory, self)
    }
}

impl fmt::Display for ReplicaDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{},{},{}", self.pid, self.address, self.epoch)
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
        let epoch = parts
            .next()
            .ok_or(ReplicaDescriptorParseError)?
            .parse()
            .map_err(|_| ReplicaDescriptorParseError)?;
        if parts.next().is_some() {
            return Err(ReplicaDescriptorParseError);
        }
        Ok(Self {
            pid,
            address,
            epoch,
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
