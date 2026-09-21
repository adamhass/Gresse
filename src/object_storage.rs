use futures::future::try_join_all;
use futures::StreamExt;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    ClientOptions, Error, ObjectStore, ObjectStoreExt, PutMode, PutPayload, RetryConfig,
    UpdateVersion,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::RwLock;

use crate::prelude::ObjectStorageConfig;
use crate::replica_helpers::ReplicaDescriptor;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;

const SLOW_DOWNLOAD_THRESHOLD: Duration = Duration::from_secs(1);
const OBJECT_STORE_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const OBJECT_STORE_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const OBJECT_STORE_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
const OBJECT_STORE_OPERATION_TIMEOUT: Duration = Duration::from_secs(45);
const OBJECT_STORE_MAX_RETRIES: usize = 2;

pub(crate) struct MembershipPollResult {
    pub(crate) members: Result<Vec<ReplicaDescriptor>, String>,
}

pub struct ObjectStorageClient {
    store: Arc<dyn ObjectStore>,
    backend: StorageBackend,
    persistent_replica_path: String,
    membership_directory_path: String,
    membership_descriptors: RwLock<Vec<ReplicaDescriptor>>,
    membership_poll_in_flight: AtomicBool,
    membership_poll_sender: UnboundedSender<MembershipPollResult>,
    membership_poll_receiver: Mutex<UnboundedReceiver<MembershipPollResult>>,
    membership_poll_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    connection_logged: AtomicBool,
}

impl ObjectStorageClient {
    pub fn new(config: ObjectStorageConfig) -> Result<Self, ObjectStorageError> {
        let backend = StorageBackend::from_config(&config)?;
        let (membership_poll_sender, membership_poll_receiver) = unbounded_channel();

        Ok(ObjectStorageClient {
            store: backend.store(),
            backend,
            persistent_replica_path: config.persistent_replica_path,
            membership_directory_path: config.membership_directory_path,
            membership_descriptors: RwLock::new(Vec::new()),
            membership_poll_in_flight: AtomicBool::new(false),
            membership_poll_sender,
            membership_poll_receiver: Mutex::new(membership_poll_receiver),
            membership_poll_task: Mutex::new(None),
            connection_logged: AtomicBool::new(false),
        })
    }

    /// Reads state used to bootstrap a replica from object storage.
    pub async fn read_persistent_replica<T: for<'de> Deserialize<'de>>(
        &self,
    ) -> Result<Option<T>, ObjectStorageError> {
        match self.download_data(&self.persistent_replica_path).await {
            Ok((persistent_crdt, _)) => Ok(Some(persistent_crdt)),
            Err(ObjectStorageError::FileNotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub async fn write_persistent_replica<T: Serialize>(
        &self,
        crdt: &T,
    ) -> Result<(), ObjectStorageError> {
        self.upload_data(&self.persistent_replica_path, crdt).await
    }

    pub async fn write_membership_descriptor(
        &self,
        descriptor: ReplicaDescriptor,
    ) -> Result<bool, ObjectStorageError> {
        let descriptor_path = descriptor.object_path(&self.membership_directory_path);
        self.upload_empty_atomic_create(&descriptor_path).await
    }

    /// Registers a replica and returns the resulting membership view.
    pub(crate) async fn register_and_list_members(
        &self,
        descriptor: ReplicaDescriptor,
    ) -> Option<Vec<ReplicaDescriptor>> {
        if let Err(error) = self.write_membership_descriptor(descriptor).await {
            log::warn!("could not register replica membership: {error}");
            return None;
        }
        self.list_membership_descriptors().await
    }

    pub async fn write_membership_descriptor_payload<T: Serialize>(
        &self,
        descriptor: ReplicaDescriptor,
        payload: &T,
    ) -> Result<bool, ObjectStorageError> {
        let descriptor_path = descriptor.object_path(&self.membership_directory_path);
        self.upload_data_atomic_create(&descriptor_path, payload)
            .await
    }

    pub async fn read_membership_descriptor_payload<T: for<'de> Deserialize<'de>>(
        &self,
        descriptor: ReplicaDescriptor,
    ) -> Result<Option<T>, ObjectStorageError> {
        let descriptor_path = descriptor.object_path(&self.membership_directory_path);
        match self.download_data(&descriptor_path).await {
            Ok((payload, _)) => Ok(Some(payload)),
            Err(ObjectStorageError::FileNotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub async fn delete_membership_descriptor(
        &self,
        descriptor: ReplicaDescriptor,
    ) -> Result<(), ObjectStorageError> {
        let descriptor_path = descriptor.object_path(&self.membership_directory_path);
        self.delete_data(&descriptor_path).await
    }

    pub async fn delete_membership_descriptors_for_pid(
        &self,
        pid: crate::prelude::Pid,
    ) -> Result<(), ObjectStorageError> {
        let descriptors_to_delete = self
            .membership_descriptors()
            .await
            .into_iter()
            .filter(|descriptor| descriptor.pid == pid)
            .collect::<Vec<_>>();

        try_join_all(
            descriptors_to_delete
                .iter()
                .copied()
                .map(|descriptor| self.delete_membership_descriptor(descriptor)),
        )
        .await?;

        self.membership_descriptors
            .write()
            .await
            .retain(|descriptor| descriptor.pid != pid);

        Ok(())
    }

    pub async fn list_membership_descriptors(&self) -> Option<Vec<ReplicaDescriptor>> {
        let membership_descriptors = match self.fetch_membership_descriptors().await {
            Ok(descriptors) => descriptors,
            Err(error) => {
                log::warn!("could not list replica membership: {error}");
                return None;
            }
        };
        self.update_membership_cache(membership_descriptors.clone())
            .await;
        Some(membership_descriptors)
    }

    /// Fetch membership descriptors without changing the shared cache. This is
    /// used by background discovery polling so a slow/stale read cannot race
    /// with a synchronous GC membership-validation round.
    pub async fn fetch_membership_descriptors(
        &self,
    ) -> Result<Vec<ReplicaDescriptor>, ObjectStorageError> {
        let members = self.list_objects(&self.membership_directory_path).await?;
        Ok(members
            .into_iter()
            .filter_map(|member| member.parse::<ReplicaDescriptor>().ok())
            .collect::<Vec<_>>())
    }

    pub async fn update_membership_cache(&self, membership_descriptors: Vec<ReplicaDescriptor>) {
        *self.membership_descriptors.write().await = membership_descriptors.clone();
    }

    /// Begins a background membership listing unless the prior listing has not
    /// yet been consumed. Keeping this lifecycle beside the storage client
    /// prevents overlapping object-store reads from staleing its cache.
    pub(crate) fn start_membership_poll(self: &Arc<Self>) {
        if self.membership_poll_in_flight.swap(true, Ordering::AcqRel) {
            log::debug!("skipped membership poll; previous poll is still in flight");
            return;
        }
        let client = self.clone();
        let result_sender = self.membership_poll_sender.clone();
        let task = tokio::spawn(async move {
            let members = client
                .fetch_membership_descriptors()
                .await
                .map_err(|error| error.to_string());
            let _ = result_sender.send(MembershipPollResult { members });
        });
        *self
            .membership_poll_task
            .try_lock()
            .expect("membership poll task lock unexpectedly held") = Some(task);
    }

    pub(crate) async fn recv_membership_poll(&self) -> Option<MembershipPollResult> {
        self.membership_poll_receiver.lock().await.recv().await
    }

    pub(crate) async fn finish_membership_poll(&self) {
        self.membership_poll_in_flight
            .store(false, Ordering::Release);
        self.membership_poll_task.lock().await.take();
    }

    pub(crate) async fn abort_membership_poll(&self) {
        if let Some(task) = self.membership_poll_task.lock().await.take() {
            task.abort();
        }
        self.membership_poll_in_flight
            .store(false, Ordering::Release);
    }

    #[allow(unused)]
    pub async fn membership_descriptors(&self) -> Vec<ReplicaDescriptor> {
        self.membership_descriptors.read().await.clone()
    }

    pub async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, ObjectStorageError> {
        let prefix_path = Path::from(prefix);
        let mut list_stream = self.store.list(Some(&prefix_path));
        let file_names = self
            .with_operation_timeout("list", prefix, async {
                let mut file_names = Vec::new();
                while let Some(meta) = list_stream.next().await.transpose()? {
                    file_names.push(meta.location.to_string());
                }
                Ok(file_names)
            })
            .await?;
        self.log_connection_established_once("list", prefix);

        Ok(file_names)
    }

    pub async fn upload_data<T: Serialize>(
        &self,
        file_path: &str,
        data: &T,
    ) -> Result<(), ObjectStorageError> {
        self.upload_data_with_size(file_path, data)
            .await
            .map(|_| ())
    }

    async fn upload_data_with_size<T: Serialize>(
        &self,
        file_path: &str,
        data: &T,
    ) -> Result<usize, ObjectStorageError> {
        let serialized_data = serde_json::to_string(data)?;
        let serialized_bytes = serialized_data.len();
        let path = Path::from(file_path);
        let payload = PutPayload::from(serialized_data);
        self.with_operation_timeout("put", file_path, self.store.put(&path, payload))
            .await?;
        self.log_connection_established_once("put", file_path);
        Ok(serialized_bytes)
    }

    pub async fn download_data<T: for<'de> Deserialize<'de>>(
        &self,
        file_path: &str,
    ) -> Result<(T, UpdateVersion), ObjectStorageError> {
        let (data, _, version) = self.download_data_with_serialized_data(file_path).await?;
        Ok((data, version))
    }

    async fn download_data_with_serialized_data<T: for<'de> Deserialize<'de>>(
        &self,
        file_path: &str,
    ) -> Result<(T, Vec<u8>, UpdateVersion), ObjectStorageError> {
        let path = Path::from(file_path);
        let download_started = Instant::now();
        let get_started = Instant::now();
        let response = match self
            .with_operation_timeout("get", file_path, self.store.get(&path))
            .await
        {
            Ok(response) => response,
            Err(error) => match error {
                ObjectStorageError::StoreError(Error::NotFound { .. }) => {
                    self.log_connection_established_once("get", file_path);
                    return Err(ObjectStorageError::FileNotFound);
                }
                _ => {
                    log::warn!(
                        "object storage get failed: path={}, get_elapsed_ms={}, error={}",
                        file_path,
                        get_started.elapsed().as_millis(),
                        error,
                    );
                    return Err(error);
                }
            },
        };
        self.log_connection_established_once("get", file_path);
        let version = UpdateVersion {
            e_tag: response.meta.e_tag.clone(),
            version: response.meta.version.clone(),
        };

        let get_elapsed = get_started.elapsed();
        let body_started = Instant::now();
        let raw_data = match self
            .with_operation_timeout("get-body", file_path, response.bytes())
            .await
        {
            Ok(raw_data) => raw_data,
            Err(error) => {
                log::warn!(
                    "object storage response body read failed: path={}, get_elapsed_ms={}, body_elapsed_ms={}, error={}",
                    file_path,
                    get_elapsed.as_millis(),
                    body_started.elapsed().as_millis(),
                    error,
                );
                return Err(error);
            }
        };
        let body_elapsed = body_started.elapsed();
        let decode_started = Instant::now();
        let data: T = serde_json::from_slice(&raw_data)?;
        let decode_elapsed = decode_started.elapsed();
        let total_elapsed = download_started.elapsed();

        if total_elapsed >= SLOW_DOWNLOAD_THRESHOLD {
            log::warn!(
                "slow object storage download: path={}, bytes={}, etag={:?}, version={:?}, get_elapsed_ms={}, body_elapsed_ms={}, decode_elapsed_ms={}, total_elapsed_ms={}",
                file_path,
                raw_data.len(),
                version.e_tag,
                version.version,
                get_elapsed.as_millis(),
                body_elapsed.as_millis(),
                decode_elapsed.as_millis(),
                total_elapsed.as_millis(),
            );
        }

        Ok((data, raw_data.to_vec(), version))
    }

    #[allow(unused)]
    pub async fn delete_data(&self, file_path: &str) -> Result<(), ObjectStorageError> {
        let path = Path::from(file_path);
        self.with_operation_timeout("delete", file_path, self.store.delete(&path))
            .await?;
        self.log_connection_established_once("delete", file_path);
        Ok(())
    }

    pub async fn upload_empty_atomic_create(
        &self,
        file_path: &str,
    ) -> Result<bool, ObjectStorageError> {
        let path = Path::from(file_path);
        let payload = PutPayload::from(Vec::new());
        let result = self
            .with_operation_timeout(
                "put-create",
                file_path,
                self.store.put_opts(&path, payload, PutMode::Create.into()),
            )
            .await;
        match result {
            Ok(_) => {
                self.log_connection_established_once("put-create", file_path);
                Ok(true)
            }
            Err(ObjectStorageError::StoreError(Error::AlreadyExists { .. }))
            | Err(ObjectStorageError::StoreError(Error::Precondition { .. })) => {
                self.log_connection_established_once("put-create", file_path);
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    async fn upload_data_atomic_create<T: Serialize>(
        &self,
        file_path: &str,
        data: &T,
    ) -> Result<bool, ObjectStorageError> {
        let path = Path::from(file_path);
        let serialized_data = serde_json::to_string(data)?;
        let payload = PutPayload::from(serialized_data);
        let result = self
            .with_operation_timeout(
                "put-create",
                file_path,
                self.store.put_opts(&path, payload, PutMode::Create.into()),
            )
            .await;
        match result {
            Ok(_) => {
                self.log_connection_established_once("put-create", file_path);
                Ok(true)
            }
            Err(ObjectStorageError::StoreError(Error::AlreadyExists { .. }))
            | Err(ObjectStorageError::StoreError(Error::Precondition { .. })) => {
                self.log_connection_established_once("put-create", file_path);
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    async fn with_operation_timeout<T, F>(
        &self,
        operation: &'static str,
        file_path: &str,
        future: F,
    ) -> Result<T, ObjectStorageError>
    where
        F: Future<Output = Result<T, Error>>,
    {
        match tokio::time::timeout(OBJECT_STORE_OPERATION_TIMEOUT, future).await {
            Ok(result) => result.map_err(ObjectStorageError::StoreError),
            Err(_) => {
                log::warn!(
                    "object storage operation timed out: operation={}, path={}, timeout_ms={}",
                    operation,
                    file_path,
                    OBJECT_STORE_OPERATION_TIMEOUT.as_millis(),
                );
                Err(ObjectStorageError::OperationTimeout {
                    operation,
                    path: file_path.to_string(),
                    timeout: OBJECT_STORE_OPERATION_TIMEOUT,
                })
            }
        }
    }

    fn log_connection_established_once(&self, operation: &str, object_path: &str) {
        if self
            .connection_logged
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            match &self.backend {
                StorageBackend::S3 {
                    bucket_name,
                    region,
                    endpoint_url,
                    ..
                } => {
                    let endpoint = endpoint_url
                        .as_deref()
                        .unwrap_or("AWS default endpoint resolution");
                    log::info!(
                        "object storage connection established: backend=s3, bucket={}, region={}, endpoint={}, first_operation={}, object_path={}",
                        bucket_name,
                        region,
                        endpoint,
                        operation,
                        object_path,
                    );
                }
                StorageBackend::Local { root_dir, .. } => {
                    log::info!(
                        "object storage connection established: backend=local_fs, root_dir={}, first_operation={}, object_path={}",
                        root_dir.display(),
                        operation,
                        object_path,
                    );
                }
            }
        }
    }

    // Optimization: Implement download_metadata with head()
    // Optimization: Cache implementation? (https://docs.rs/object_store/latest/object_store/#conditional-fetch)
}

enum StorageBackend {
    S3 {
        store: Arc<AmazonS3>,
        bucket_name: String,
        region: String,
        endpoint_url: Option<String>,
    },
    Local {
        store: Arc<LocalFileSystem>,
        root_dir: PathBuf,
    },
}

impl StorageBackend {
    fn from_config(config: &ObjectStorageConfig) -> Result<Self, ObjectStorageError> {
        if let Some(root_dir) = config.local_dir.clone() {
            fs::create_dir_all(&root_dir)?;
            let store = LocalFileSystem::new_with_prefix(&root_dir)
                .map_err(|error| ObjectStorageError::ConfigurationError(error.to_string()))?;
            return Ok(Self::Local {
                store: Arc::new(store),
                root_dir,
            });
        }

        let store = Arc::new(build_bucket(config)?);
        Ok(Self::S3 {
            store,
            bucket_name: config.bucket.clone(),
            region: config.region.clone(),
            endpoint_url: config.url.clone(),
        })
    }

    fn store(&self) -> Arc<dyn ObjectStore> {
        match self {
            Self::S3 { store, .. } => store.clone(),
            Self::Local { store, .. } => store.clone(),
        }
    }
}

fn build_bucket(config: &ObjectStorageConfig) -> Result<AmazonS3, ObjectStorageError> {
    let use_explicit_static_credentials =
        config.access_key.is_some() && config.secret_key.is_some();

    // Fast path for MinIO/S3-compatible benchmarks: when the caller already provides
    // endpoint + static credentials explicitly, avoid scanning the full AWS env/provider
    // chain during client construction. Keep the generic AWS path intact for later S3 runs.
    let mut builder = if use_explicit_static_credentials {
        AmazonS3Builder::new()
    } else {
        AmazonS3Builder::from_env()
    }
    .with_region(config.region.clone())
    .with_bucket_name(config.bucket.clone());

    if let Some(endpoint) = config
        .url
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        builder = builder
            .with_endpoint(endpoint)
            .with_allow_http(endpoint.starts_with("http://"));
    }

    if !use_explicit_static_credentials {
        if let Some(profile_credentials) = resolve_profile_credentials(config)? {
            builder = builder
                .with_access_key_id(profile_credentials.access_key_id)
                .with_secret_access_key(profile_credentials.secret_access_key);
            if let Some(session_token) = profile_credentials.session_token {
                builder = builder.with_token(session_token);
            }
        }
    }

    if let Some(access_key) = config.access_key.clone() {
        builder = builder.with_access_key_id(access_key);
    }
    if let Some(secret_key) = config.secret_key.clone() {
        builder = builder.with_secret_access_key(secret_key);
    }
    if let Some(session_token) = config.session_token.clone() {
        builder = builder.with_token(session_token);
    }

    let client_options = ClientOptions::default()
        .with_connect_timeout(OBJECT_STORE_CONNECT_TIMEOUT)
        .with_timeout(OBJECT_STORE_REQUEST_TIMEOUT);
    let retry_config = RetryConfig {
        max_retries: OBJECT_STORE_MAX_RETRIES,
        retry_timeout: OBJECT_STORE_RETRY_TIMEOUT,
        ..Default::default()
    };

    builder
        .with_client_options(client_options)
        .with_retry(retry_config)
        .build()
        .map_err(|error| ObjectStorageError::ConfigurationError(error.to_string()))
}

fn resolve_profile_credentials(
    config: &ObjectStorageConfig,
) -> Result<Option<SharedProfileCredentials>, ObjectStorageError> {
    if config.access_key.is_some() || config.secret_key.is_some() || config.session_token.is_some()
    {
        return Ok(None);
    }

    if env::var_os("AWS_ACCESS_KEY_ID").is_some() || env::var_os("AWS_SECRET_ACCESS_KEY").is_some()
    {
        return Ok(None);
    }

    let profile_name = env::var("AWS_PROFILE").unwrap_or_else(|_| "default".to_string());
    let credentials_path = env::var_os("AWS_SHARED_CREDENTIALS_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(default_credentials_path);
    let config_path = env::var_os("AWS_CONFIG_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(default_config_path);

    let credentials = parse_profile_file(&credentials_path)?;
    let config_profiles = parse_profile_file(&config_path)?;

    let credentials_section = credentials
        .get(&profile_name)
        .or_else(|| config_profiles.get(&profile_name))
        .or_else(|| config_profiles.get(&format!("profile {profile_name}")));

    let Some(section) = credentials_section else {
        return Ok(None);
    };

    let Some(access_key_id) = section.get("aws_access_key_id").cloned() else {
        return Ok(None);
    };
    let Some(secret_access_key) = section.get("aws_secret_access_key").cloned() else {
        return Ok(None);
    };

    Ok(Some(SharedProfileCredentials {
        access_key_id,
        secret_access_key,
        session_token: section.get("aws_session_token").cloned(),
    }))
}

fn parse_profile_file(
    path: &PathBuf,
) -> Result<HashMap<String, HashMap<String, String>>, ObjectStorageError> {
    let Ok(raw_contents) = fs::read_to_string(path) else {
        return Ok(HashMap::new());
    };

    let mut profiles = HashMap::new();
    let mut current_section: Option<String> = None;

    for line in raw_contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with(';') {
            continue;
        }

        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            let section = trimmed[1..trimmed.len() - 1].trim().to_string();
            profiles.entry(section.clone()).or_insert_with(HashMap::new);
            current_section = Some(section);
            continue;
        }

        let Some(section_name) = current_section.clone() else {
            continue;
        };
        let Some((key, value)) = trimmed.split_once('=') else {
            continue;
        };
        profiles
            .entry(section_name)
            .or_insert_with(HashMap::new)
            .insert(key.trim().to_string(), value.trim().to_string());
    }

    Ok(profiles)
}

fn default_credentials_path() -> PathBuf {
    aws_home_dir().join(".aws").join("credentials")
}

fn default_config_path() -> PathBuf {
    aws_home_dir().join(".aws").join("config")
}

fn aws_home_dir() -> PathBuf {
    env::var_os("HOME")
        .map(PathBuf::from)
        .expect("HOME environment variable is required")
}

#[derive(Debug)]
struct SharedProfileCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

#[derive(Error, Debug)]
pub enum ObjectStorageError {
    #[error("File not found")]
    FileNotFound,
    #[error("Object storage configuration error: {0}")]
    ConfigurationError(String),
    #[error("Object store error: {0}")]
    StoreError(#[from] Error),
    #[error("Object storage {operation} timed out after {timeout:?}: {path}")]
    OperationTimeout {
        operation: &'static str,
        path: String,
        timeout: Duration,
    },
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::{
        parse_profile_file, resolve_profile_credentials, SharedProfileCredentials, StorageBackend,
    };
    use crate::prelude::ObjectStorageConfig;
    use std::fs;
    use std::path::Path;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn temp_file_path(name: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock drifted before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("gresse-{name}-{unique}.ini"))
    }

    fn test_config() -> ObjectStorageConfig {
        ObjectStorageConfig {
            local_dir: None,
            url: None,
            region: "eu-north-1".to_string(),
            bucket: "gresse".to_string(),
            access_key: None,
            secret_key: None,
            session_token: None,
            persistent_replica_path: "experiment1/persistent.json".to_string(),
            membership_directory_path: "experiment1/membership".to_string(),
            discovery_interval: Duration::from_secs(1),
        }
    }

    #[test]
    fn parses_shared_credentials_file() {
        let path = temp_file_path("credentials");
        fs::write(
            &path,
            "[default]\naws_access_key_id = abc\naws_secret_access_key = def\naws_session_token = ghi\n",
        )
        .expect("failed to write credentials file");

        let parsed = parse_profile_file(&path).expect("failed to parse credentials file");
        let section = parsed.get("default").expect("missing default profile");
        assert_eq!(section.get("aws_access_key_id"), Some(&"abc".to_string()));
        assert_eq!(
            section.get("aws_secret_access_key"),
            Some(&"def".to_string())
        );
        assert_eq!(section.get("aws_session_token"), Some(&"ghi".to_string()));

        fs::remove_file(path).expect("failed to clean up credentials file");
    }

    #[test]
    fn resolves_default_profile_credentials() {
        let credentials_path = temp_file_path("shared-credentials");
        let config_path = temp_file_path("shared-config");
        fs::write(
            &credentials_path,
            "[default]\naws_access_key_id = abc\naws_secret_access_key = def\naws_session_token = ghi\n",
        )
        .expect("failed to write shared credentials");
        fs::write(&config_path, "[default]\nregion = eu-north-1\n")
            .expect("failed to write shared config");

        unsafe {
            std::env::remove_var("AWS_ACCESS_KEY_ID");
            std::env::remove_var("AWS_SECRET_ACCESS_KEY");
            std::env::remove_var("AWS_PROFILE");
            std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", &credentials_path);
            std::env::set_var("AWS_CONFIG_FILE", &config_path);
        }

        let resolved = resolve_profile_credentials(&test_config())
            .expect("failed to resolve shared profile credentials");

        assert!(matches!(
            resolved,
            Some(SharedProfileCredentials {
                access_key_id,
                secret_access_key,
                session_token
            }) if access_key_id == "abc"
                && secret_access_key == "def"
                && session_token == Some("ghi".to_string())
        ));

        unsafe {
            std::env::remove_var("AWS_SHARED_CREDENTIALS_FILE");
            std::env::remove_var("AWS_CONFIG_FILE");
        }
        fs::remove_file(credentials_path).expect("failed to clean up shared credentials");
        fs::remove_file(config_path).expect("failed to clean up shared config");
    }

    #[test]
    fn creates_local_backend_from_config() {
        let root = std::env::temp_dir().join(format!(
            "gresse-local-store-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock drifted before unix epoch")
                .as_nanos()
        ));
        let config = ObjectStorageConfig {
            local_dir: Some(root.clone()),
            ..test_config()
        };

        let backend = StorageBackend::from_config(&config).expect("failed to create local backend");
        assert!(matches!(
            backend,
            StorageBackend::Local { root_dir, .. } if root_dir == root
        ));

        if Path::new(&root).exists() {
            fs::remove_dir_all(root).expect("failed to clean up local backend root");
        }
    }
}
