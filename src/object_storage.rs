use futures::StreamExt;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::{Error, ObjectStore, PutMode, PutPayload, UpdateVersion, ObjectStoreExt};
use object_store::path::Path;
use thiserror::Error;
use serde::{Serialize, Deserialize};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct ObjectStorageConfig {
    pub url: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub prefix_delimiter: Option<String>,
    pub prefixes: ObjectStoragePrefixes,
    pub discovery_interval: Duration
}

#[derive(Clone, Debug)]
pub struct ObjectStoragePrefixes {
    pub discovery: String,
    pub bottom: String,
    pub dormant: String,
    pub lock: String,
}

pub struct ObjectStorageClient {
    bucket: AmazonS3,
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
        })
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


    pub async fn upload_data<T: Serialize>(&self, file_path: &str, data: &T) -> Result<(), ObjectStorageError> {
        let serialized_data = serde_json::to_string(data)?;
        let path = Path::from(file_path);
        let payload = PutPayload::from(serialized_data);
        let result = self.bucket.put(&path, payload).await;
        if let Err(content) = result {
            return Err(ObjectStorageError::S3Error(content));
        }
        Ok(())
    }

    pub async fn download_data<T: for<'de> Deserialize<'de>>(&self, file_path: &str) -> Result<(T, UpdateVersion), ObjectStorageError> {
        let path = Path::from(file_path);
        let result = self.bucket.get(&path).await;
        if let Err(error) = result {
            return match error {
                Error::NotFound { path: _, source: _} => Err(ObjectStorageError::FileNotFound),
                _ => Err(ObjectStorageError::S3Error(error)),
            }
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

    pub async fn delete_data(&self, file_path: &str) -> Result<(), ObjectStorageError> {
        let path = Path::from(file_path);
        let result = self.bucket.delete(&path).await;
        if let Err(content) = result {
            return Err(ObjectStorageError::S3Error(content));
        }
        Ok(())
    }

    pub async fn upload_data_atomic_update<T: Serialize>(&self, file_path: &str, data: &T, version: UpdateVersion) -> Result<bool, ObjectStorageError> {
        let path = Path::from(file_path);
        let serialized_data = serde_json::to_string(data)?;
        let payload = PutPayload::from(serialized_data);
        let result = self.bucket.put_opts(&path, payload, PutMode::Update(version).into()).await;
        match result {
            Ok(_) => {
                Ok(true)
            }
            Err(Error::Precondition { .. }) => {
                Ok(false)
            }
            Err(error) => {
                Err(ObjectStorageError::S3Error(error))
            }
        }
    }
    
    pub async fn upload_data_atomic_create<T: Serialize>(&self, file_path: &str, data: &T) -> Result<bool, ObjectStorageError> {
        let path = Path::from(file_path);
        let serialized_data = serde_json::to_string(data)?;
        let payload = PutPayload::from(serialized_data);
        let result = self.bucket.put_opts(&path, payload, PutMode::Create.into()).await;
        match result {
            Ok(_) => {
                Ok(true)
            }
            Err(Error::AlreadyExists { .. }) => {
                Ok(false)
            }
            Err(Error::Precondition { .. }) => {
                Ok(false)
            }
            Err(error) => {
                Err(ObjectStorageError::S3Error(error))
            }
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