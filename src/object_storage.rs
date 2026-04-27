use futures::StreamExt;
use futures::future::try_join_all;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path;
use object_store::{Error, ObjectStore, ObjectStoreExt, PutMode, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;

use crate::prelude::ObjectStorageConfig;
use crate::replica_helpers::ReplicaDescriptor;

pub struct ObjectStorageClient {
    bucket: AmazonS3,
    persistent_replica_path: String,
    membership_directory_path: String,
    membership_descriptors: RwLock<Vec<ReplicaDescriptor>>,
}

impl ObjectStorageClient {
    pub fn new(config: ObjectStorageConfig) -> Result<Self, ObjectStorageError> {
        let bucket = AmazonS3Builder::new()
            .with_region(config.clone().region)
            .with_bucket_name(config.clone().bucket)
            .with_access_key_id(config.clone().access_key)
            .with_secret_access_key(config.clone().secret_key)
            .with_endpoint(config.clone().url)
            .with_allow_http(true)
            .build();

        let bucket = match bucket {
            Ok(bucket) => bucket,
            Err(_) => return Err(ObjectStorageError::BucketNotFound),
        };

        Ok(ObjectStorageClient {
            bucket,
            persistent_replica_path: config.persistent_replica_path,
            membership_directory_path: config.membership_directory_path,
            membership_descriptors: RwLock::new(Vec::new()),
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
                Error::NotFound { path: _, source: _ } => Err(ObjectStorageError::FileNotFound),
                _ => Err(ObjectStorageError::S3Error(error)),
            };
        }
        let response = result?;
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
            Ok(_) => Ok(true),
            Err(Error::AlreadyExists { .. }) => Ok(false),
            Err(Error::Precondition { .. }) => Ok(false),
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
            Ok(_) => Ok(true),
            Err(Error::AlreadyExists { .. }) => Ok(false),
            Err(Error::Precondition { .. }) => Ok(false),
            Err(error) => Err(ObjectStorageError::S3Error(error)),
        }
    }

    // Optimization: Implement download_metadata with bucket.head()
    // Optimization: Cache implementation? (https://docs.rs/object_store/latest/object_store/#conditional-fetch)
}

#[derive(Error, Debug)]
pub enum ObjectStorageError {
    #[error("Bucket not found")]
    BucketNotFound,
    #[error("File not found")]
    FileNotFound,
    #[error("S3 error: {0}")]
    S3Error(#[from] Error),
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}
