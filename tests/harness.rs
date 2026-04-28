use futures::StreamExt;
use gresse::crdt::{CRDTClientRequest, CRDT};
use gresse::http_client::HttpClient;
use gresse::logging;
use gresse::prelude::{ObjectStorageConfig, ServerAddr};
use gresse::replica::Replica;
use gresse::replica_helpers::ReplicaDescriptor;
use gresse::replica_helpers::ReplicaConfig;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use std::fmt::Debug;
use std::fs;
use std::path::{Path as FsPath, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};

const DEFAULT_MINIO_URL: &str = "http://127.0.0.1:9000";
const DEFAULT_MINIO_REGION: &str = "us-east-1";
const DEFAULT_MINIO_BUCKET: &str = "gresse-integration";
const DEFAULT_MINIO_ACCESS_KEY: &str = "minioadmin";
const DEFAULT_MINIO_SECRET_KEY: &str = "minioadmin";

pub struct MinioHarness {
    bucket: String,
    run_prefix: String,
    persistent_replica_path: String,
    membership_directory_path: String,
    result_dir: PathBuf,
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicaTimingConfig {
    pub sync_interval: Duration,
    pub discovery_interval: Duration,
    pub gc_interval: Duration,
}

impl Default for ReplicaTimingConfig {
    fn default() -> Self {
        Self {
            sync_interval: Duration::from_millis(250),
            discovery_interval: Duration::from_millis(250),
            gc_interval: Duration::from_secs(60),
        }
    }
}

impl MinioHarness {
    pub async fn new() -> Self {
        logging::init_for_tests();
        ensure_minio_available().await;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock drifted before unix epoch")
            .as_nanos();
        let result_dir = std::env::temp_dir().join(format!("gresse-minio-test-{unique}"));
        fs::create_dir_all(&result_dir).expect("failed to create test result directory");

        Self {
            bucket: std::env::var("GRESSE_TEST_MINIO_BUCKET")
                .unwrap_or_else(|_| DEFAULT_MINIO_BUCKET.to_string()),
            run_prefix: format!("test-runs/{unique}"),
            persistent_replica_path: format!("test-runs/{unique}/persistent.json"),
            membership_directory_path: format!("test-runs/{unique}/membership"),
            result_dir,
        }
    }

    pub async fn spawn_replica<T>(
        &self,
        pid: u128,
        address: ServerAddr,
        crdt: T,
    ) -> TestReplicaHandle<T>
    where
        T: CRDT + Send + Sync + Debug + Clone + 'static,
    {
        self.spawn_replica_with_timing(pid, address, crdt, ReplicaTimingConfig::default())
            .await
    }

    pub async fn spawn_replica_with_timing<T>(
        &self,
        pid: u128,
        address: ServerAddr,
        crdt: T,
        timing: ReplicaTimingConfig,
    ) -> TestReplicaHandle<T>
    where
        T: CRDT + Send + Sync + Debug + Clone + 'static,
    {
        let config = ReplicaConfig {
            address,
            sync_interval: timing.sync_interval,
            result_dir_path: self.result_dir.clone(),
            durability_path: None,
            object_storage_config: self.object_storage_config(timing.discovery_interval),
        };

        let (mut replica, shutdown_sender) = Replica::with_config(pid, crdt, config).await;
        replica.set_gc_interval(timing.gc_interval);
        let join_handle = tokio::spawn(async move {
            replica.run().await;
        });

        TestReplicaHandle {
            address,
            shutdown_sender,
            join_handle,
            _crdt: std::marker::PhantomData,
        }
    }

    pub async fn wait_for_bootstrap(&self, max_wait: Duration) {
        let bucket = self.verification_bucket();

        timeout(max_wait, async {
            loop {
                let persistent_exists = bucket
                    .get(&Path::from(self.persistent_replica_path.as_str()))
                    .await
                    .is_ok();
                let membership_count = self.membership_count(&bucket).await;
                if persistent_exists && membership_count > 0 {
                    return;
                }
                sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .expect("timed out waiting for replica bootstrap artifacts");
    }

    pub async fn wait_for_membership_count(&self, expected: usize, max_wait: Duration) {
        let bucket = self.verification_bucket();

        timeout(max_wait, async {
            loop {
                if self.membership_count(&bucket).await >= expected {
                    return;
                }
                sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .expect("timed out waiting for replica membership descriptors");
    }

    pub async fn wait_for_pid_removal(&self, pid: u128, max_wait: Duration) {
        let bucket = self.verification_bucket();

        timeout(max_wait, async {
            loop {
                let descriptors = self.membership_descriptors(&bucket).await;
                if descriptors.iter().all(|descriptor| descriptor.pid != pid) {
                    return;
                }
                sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .expect("timed out waiting for replica membership descriptor removal");
    }

    pub async fn cleanup(&self) {
        let bucket = self.verification_bucket();
        let mut entries = bucket.list(Some(&Path::from(self.run_prefix.as_str())));
        let mut objects_to_delete = Vec::new();
        while let Some(meta) = entries
            .next()
            .await
            .transpose()
            .expect("failed to list test prefix objects for cleanup")
        {
            objects_to_delete.push(meta.location);
        }

        for location in objects_to_delete {
            bucket
                .delete(&location)
                .await
                .expect("failed to delete test object from MinIO");
        }

        if FsPath::new(&self.result_dir).exists() {
            fs::remove_dir_all(&self.result_dir).expect("failed to remove test result directory");
        }
    }

    fn object_storage_config(&self, discovery_interval: Duration) -> ObjectStorageConfig {
        ObjectStorageConfig {
            url: Some(env_or_default("GRESSE_TEST_MINIO_URL", DEFAULT_MINIO_URL)),
            region: env_or_default("GRESSE_TEST_MINIO_REGION", DEFAULT_MINIO_REGION),
            bucket: self.bucket.clone(),
            access_key: Some(env_or_default(
                "GRESSE_TEST_MINIO_ACCESS_KEY",
                DEFAULT_MINIO_ACCESS_KEY,
            )),
            secret_key: Some(env_or_default(
                "GRESSE_TEST_MINIO_SECRET_KEY",
                DEFAULT_MINIO_SECRET_KEY,
            )),
            session_token: None,
            persistent_replica_path: self.persistent_replica_path.clone(),
            membership_directory_path: self.membership_directory_path.clone(),
            discovery_interval,
        }
    }

    fn verification_bucket(&self) -> object_store::aws::AmazonS3 {
        AmazonS3Builder::new()
            .with_region(env_or_default("GRESSE_TEST_MINIO_REGION", DEFAULT_MINIO_REGION))
            .with_bucket_name(&self.bucket)
            .with_access_key_id(env_or_default(
                "GRESSE_TEST_MINIO_ACCESS_KEY",
                DEFAULT_MINIO_ACCESS_KEY,
            ))
            .with_secret_access_key(env_or_default(
                "GRESSE_TEST_MINIO_SECRET_KEY",
                DEFAULT_MINIO_SECRET_KEY,
            ))
            .with_endpoint(env_or_default("GRESSE_TEST_MINIO_URL", DEFAULT_MINIO_URL))
            .with_allow_http(true)
            .build()
            .expect("failed to create verification object-store client")
    }

    async fn membership_count(&self, bucket: &object_store::aws::AmazonS3) -> usize {
        self.membership_descriptors(bucket).await.len()
    }

    async fn membership_descriptors(
        &self,
        bucket: &object_store::aws::AmazonS3,
    ) -> Vec<ReplicaDescriptor> {
        let mut membership_entries =
            bucket.list(Some(&Path::from(self.membership_directory_path.as_str())));
        let mut descriptors = Vec::new();
        while let Some(meta) = membership_entries
            .next()
            .await
            .transpose()
            .expect("failed to list membership objects")
        {
            if let Ok(descriptor) = meta.location.to_string().parse::<ReplicaDescriptor>() {
                descriptors.push(descriptor);
            }
        }
        descriptors
    }
}

pub struct TestReplicaHandle<T> {
    address: ServerAddr,
    shutdown_sender: oneshot::Sender<()>,
    join_handle: tokio::task::JoinHandle<()>,
    _crdt: std::marker::PhantomData<T>,
}

impl<T> TestReplicaHandle<T>
where
    T: CRDT + Send + Sync + Debug + Clone + 'static,
{
    pub async fn mutate(&self, mutation: T::Mutation) -> T::ClientResponse {
        self.send(CRDTClientRequest::<T>::Mutation(mutation)).await
    }

    pub async fn query(&self, query: T::Query) -> T::ClientResponse {
        self.send(CRDTClientRequest::<T>::Query(query)).await
    }

    pub async fn shutdown(self) {
        let _ = self.shutdown_sender.send(());
        let _ = self.join_handle.await;
    }

    async fn send(&self, request: CRDTClientRequest<T>) -> T::ClientResponse {
        let host = self.address.http().to_string();
        let mut client =
            HttpClient::<CRDTClientRequest<T>, T::ClientResponse>::new(&host, "/", self.address.http()).await;
        client
            .send(&request)
            .await
            .expect("failed to send request to replica")
    }
}

fn env_or_default(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

async fn ensure_minio_available() {
    let bucket = AmazonS3Builder::new()
        .with_region(env_or_default("GRESSE_TEST_MINIO_REGION", DEFAULT_MINIO_REGION))
        .with_bucket_name(env_or_default("GRESSE_TEST_MINIO_BUCKET", DEFAULT_MINIO_BUCKET))
        .with_access_key_id(env_or_default(
            "GRESSE_TEST_MINIO_ACCESS_KEY",
            DEFAULT_MINIO_ACCESS_KEY,
        ))
        .with_secret_access_key(env_or_default(
            "GRESSE_TEST_MINIO_SECRET_KEY",
            DEFAULT_MINIO_SECRET_KEY,
        ))
        .with_endpoint(env_or_default("GRESSE_TEST_MINIO_URL", DEFAULT_MINIO_URL))
        .with_allow_http(true)
        .build()
        .expect("failed to create verification object-store client");

    if let Err(error) = bucket.list(Some(&Path::from("test-runs"))).next().await.transpose() {
        panic!(
            "MinIO integration tests require a reachable local MinIO bucket. Start it with `docker compose -f docker-compose.minio.yml up -d` and rerun `cargo test --test basic_integration -- --nocapture`. Underlying error: {error}"
        );
    }
}
