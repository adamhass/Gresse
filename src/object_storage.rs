use futures::StreamExt;
use futures::future::try_join_all;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path;
use object_store::{Error, ObjectStore, ObjectStoreExt, PutMode, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use thiserror::Error;
use tokio::sync::RwLock;

use crate::prelude::ObjectStorageConfig;
use crate::replica_helpers::ReplicaDescriptor;

pub struct ObjectStorageClient {
    bucket: AmazonS3,
    bucket_name: String,
    region: String,
    endpoint_url: Option<String>,
    persistent_replica_path: String,
    membership_directory_path: String,
    membership_descriptors: RwLock<Vec<ReplicaDescriptor>>,
    connection_logged: AtomicBool,
}

impl ObjectStorageClient {
    pub fn new(config: ObjectStorageConfig) -> Result<Self, ObjectStorageError> {
        let bucket = build_bucket(&config)?;

        Ok(ObjectStorageClient {
            bucket,
            bucket_name: config.bucket,
            region: config.region,
            endpoint_url: config.url,
            persistent_replica_path: config.persistent_replica_path,
            membership_directory_path: config.membership_directory_path,
            membership_descriptors: RwLock::new(Vec::new()),
            connection_logged: AtomicBool::new(false),
        })
    }

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

    pub async fn write_membership_descriptor_payload<T: Serialize>(
        &self,
        descriptor: ReplicaDescriptor,
        payload: &T,
    ) -> Result<bool, ObjectStorageError> {
        let descriptor_path = descriptor.object_path(&self.membership_directory_path);
        self.upload_data_atomic_create(&descriptor_path, payload).await
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

    pub async fn list_membership_descriptors(
        &self,
    ) -> Result<Vec<ReplicaDescriptor>, ObjectStorageError> {
        let members = self.list_objects(&self.membership_directory_path).await?;
        let membership_descriptors = members
            .into_iter()
            .filter_map(|member| member.parse::<ReplicaDescriptor>().ok())
            .collect::<Vec<_>>();

        *self.membership_descriptors.write().await = membership_descriptors.clone();

        Ok(membership_descriptors)
    }

    #[allow(unused)]
    pub async fn membership_descriptors(&self) -> Vec<ReplicaDescriptor> {
        self.membership_descriptors.read().await.clone()
    }

    pub async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, ObjectStorageError> {
        let prefix_path = Path::from(prefix);
        let mut list_stream = self.bucket.list(Some(&prefix_path));
        let mut file_names = Vec::new();
        while let Some(meta) = list_stream.next().await.transpose()? {
            file_names.push(meta.location.to_string())
        }
        self.log_connection_established_once("list", prefix);

        Ok(file_names)
    }

    pub async fn upload_data<T: Serialize>(
        &self,
        file_path: &str,
        data: &T,
    ) -> Result<(), ObjectStorageError> {
        let serialized_data = serde_json::to_string(data)?;
        let path = Path::from(file_path);
        let payload = PutPayload::from(serialized_data);
        let result = self.bucket.put(&path, payload).await;
        if let Err(content) = result {
            return Err(ObjectStorageError::S3Error(content));
        }
        self.log_connection_established_once("put", file_path);
        Ok(())
    }

    pub async fn download_data<T: for<'de> Deserialize<'de>>(
        &self,
        file_path: &str,
    ) -> Result<(T, UpdateVersion), ObjectStorageError> {
        let path = Path::from(file_path);
        let result = self.bucket.get(&path).await;
        if let Err(error) = result {
            return match error {
                Error::NotFound { path: _, source: _ } => {
                    self.log_connection_established_once("get", file_path);
                    Err(ObjectStorageError::FileNotFound)
                }
                _ => Err(ObjectStorageError::S3Error(error)),
            };
        }
        let response = result?;
        self.log_connection_established_once("get", file_path);
        let version = UpdateVersion {
            e_tag: response.meta.e_tag.clone(),
            version: response.meta.version.clone(),
        };

        let raw_data = response.bytes().await?;
        let data: T = serde_json::from_slice(&raw_data)?;

        Ok((data, version))
    }

    #[allow(unused)]
    pub async fn delete_data(&self, file_path: &str) -> Result<(), ObjectStorageError> {
        let path = Path::from(file_path);
        let result = self.bucket.delete(&path).await;
        if let Err(content) = result {
            return Err(ObjectStorageError::S3Error(content));
        }
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
            .bucket
            .put_opts(&path, payload, PutMode::Create.into())
            .await;
        match result {
            Ok(_) => {
                self.log_connection_established_once("put-create", file_path);
                Ok(true)
            }
            Err(Error::AlreadyExists { .. }) => {
                self.log_connection_established_once("put-create", file_path);
                Ok(false)
            }
            Err(Error::Precondition { .. }) => {
                self.log_connection_established_once("put-create", file_path);
                Ok(false)
            }
            Err(error) => Err(ObjectStorageError::S3Error(error)),
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
            .bucket
            .put_opts(&path, payload, PutMode::Create.into())
            .await;
        match result {
            Ok(_) => {
                self.log_connection_established_once("put-create", file_path);
                Ok(true)
            }
            Err(Error::AlreadyExists { .. }) => {
                self.log_connection_established_once("put-create", file_path);
                Ok(false)
            }
            Err(Error::Precondition { .. }) => {
                self.log_connection_established_once("put-create", file_path);
                Ok(false)
            }
            Err(error) => Err(ObjectStorageError::S3Error(error)),
        }
    }

    fn log_connection_established_once(&self, operation: &str, object_path: &str) {
        if self
            .connection_logged
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let endpoint = self
                .endpoint_url
                .as_deref()
                .unwrap_or("AWS default endpoint resolution");
            log::info!(
                "object storage connection established: bucket={}, region={}, endpoint={}, first_operation={}, object_path={}",
                self.bucket_name,
                self.region,
                endpoint,
                operation,
                object_path,
            );
        }
    }

    // Optimization: Implement download_metadata with bucket.head()
    // Optimization: Cache implementation? (https://docs.rs/object_store/latest/object_store/#conditional-fetch)
}

fn build_bucket(config: &ObjectStorageConfig) -> Result<AmazonS3, ObjectStorageError> {
    let mut builder = AmazonS3Builder::from_env()
        .with_region(config.region.clone())
        .with_bucket_name(config.bucket.clone());

    if let Some(endpoint) = config.url.as_deref().filter(|value| !value.trim().is_empty()) {
        builder = builder
            .with_endpoint(endpoint)
            .with_allow_http(endpoint.starts_with("http://"));
    }

    if let Some(profile_credentials) = resolve_profile_credentials(config)? {
        builder = builder
            .with_access_key_id(profile_credentials.access_key_id)
            .with_secret_access_key(profile_credentials.secret_access_key);
        if let Some(session_token) = profile_credentials.session_token {
            builder = builder.with_token(session_token);
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

    builder
        .build()
        .map_err(|error| ObjectStorageError::ConfigurationError(error.to_string()))
}

fn resolve_profile_credentials(
    config: &ObjectStorageConfig,
) -> Result<Option<SharedProfileCredentials>, ObjectStorageError> {
    if config.access_key.is_some() || config.secret_key.is_some() || config.session_token.is_some() {
        return Ok(None);
    }

    if env::var_os("AWS_ACCESS_KEY_ID").is_some() || env::var_os("AWS_SECRET_ACCESS_KEY").is_some() {
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
    #[error("S3 error: {0}")]
    S3Error(#[from] Error),
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::{parse_profile_file, resolve_profile_credentials, SharedProfileCredentials};
    use crate::prelude::ObjectStorageConfig;
    use std::fs;
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
        assert_eq!(section.get("aws_secret_access_key"), Some(&"def".to_string()));
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
}
