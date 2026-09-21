mod harness;

use gresse::orset::{ORSet, OrSetMeta, OrSetMutation, OrSetQuery, OrSetResponse};
use gresse::prelude::ServerAddr;
use harness::{FilesystemHarness, MinioHarness, ReplicaTimingConfig};
use serial_test::serial;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn replica_bootstraps_against_local_minio() {
    let Some(harness) = MinioHarness::new().await else {
        return;
    };
    let replica = harness
        .spawn_replica(
            1,
            ServerAddr {
                ip: "127.0.0.1"
                    .parse()
                    .expect("failed to parse loopback address"),
                http_port: 19090,
                internal_port: 18080,
            },
            ORSet::<String>::new(),
        )
        .await;

    harness.wait_for_bootstrap(Duration::from_secs(10)).await;
    replica.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn replica_bootstraps_against_local_filesystem_directory() {
    let harness = FilesystemHarness::new().await;
    let replica = harness
        .spawn_replica(
            1,
            ServerAddr {
                ip: "127.0.0.1"
                    .parse()
                    .expect("failed to parse loopback address"),
                http_port: 19190,
                internal_port: 18180,
            },
            ORSet::<String>::new(),
        )
        .await;

    harness.wait_for_bootstrap(Duration::from_secs(10)).await;
    replica.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn single_replica_gc_progresses_without_replication_writers() {
    let harness = FilesystemHarness::new().await;
    let timing = ReplicaTimingConfig {
        sync_interval: Duration::from_millis(100),
        discovery_interval: Duration::from_millis(100),
        gc_interval: Duration::from_millis(200),
    };
    let replica = harness
        .spawn_replica_with_timing(
            1,
            ServerAddr {
                ip: "127.0.0.1"
                    .parse()
                    .expect("failed to parse loopback address"),
                http_port: 19190,
                internal_port: 18180,
            },
            ORSet::<String>::new(),
            timing,
        )
        .await;

    harness.wait_for_bootstrap(Duration::from_secs(10)).await;
    assert_eq!(
        replica
            .mutate(OrSetMutation::Insert("apple".to_string()))
            .await,
        Ok(OrSetResponse::Acknowledged)
    );

    let expected = OrSetResponse::Meta(OrSetMeta {
        entry_count: 1,
        delta_log_count: 0,
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if replica.query(OrSetQuery::Meta).await == Ok(expected.clone()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("timed out waiting for single-replica GC progress");

    replica.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn mutation_replicates_between_two_replicas_via_minio() {
    let Some(harness) = MinioHarness::new().await else {
        return;
    };
    let replica_a = harness
        .spawn_replica(
            1,
            ServerAddr {
                ip: "127.0.0.1"
                    .parse()
                    .expect("failed to parse loopback address"),
                http_port: 19090,
                internal_port: 18080,
            },
            ORSet::<String>::new(),
        )
        .await;
    let replica_b = harness
        .spawn_replica(
            2,
            ServerAddr {
                ip: "127.0.0.1"
                    .parse()
                    .expect("failed to parse loopback address"),
                http_port: 19091,
                internal_port: 18081,
            },
            ORSet::<String>::new(),
        )
        .await;

    harness
        .wait_for_membership_count(2, Duration::from_secs(10))
        .await;

    let mutation_response: Result<OrSetResponse<String>, _> = replica_a
        .mutate(OrSetMutation::Insert("apple".to_string()))
        .await;
    assert_eq!(mutation_response, Ok(OrSetResponse::Acknowledged));

    tokio::time::sleep(Duration::from_millis(1500)).await;

    let query_response: Result<OrSetResponse<String>, _> = replica_b
        .query(OrSetQuery::Contains("apple".to_string()))
        .await;
    assert_eq!(query_response, Ok(OrSetResponse::Contains(true)));

    replica_a.shutdown().await;
    replica_b.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn mutation_replicates_between_two_replicas_via_local_filesystem_directory() {
    let harness = FilesystemHarness::new().await;
    let replica_a = harness
        .spawn_replica(
            1,
            ServerAddr {
                ip: "127.0.0.1"
                    .parse()
                    .expect("failed to parse loopback address"),
                http_port: 19190,
                internal_port: 18180,
            },
            ORSet::<String>::new(),
        )
        .await;
    let replica_b = harness
        .spawn_replica(
            2,
            ServerAddr {
                ip: "127.0.0.1"
                    .parse()
                    .expect("failed to parse loopback address"),
                http_port: 19191,
                internal_port: 18181,
            },
            ORSet::<String>::new(),
        )
        .await;

    harness
        .wait_for_membership_count(2, Duration::from_secs(10))
        .await;

    let mutation_response: Result<OrSetResponse<String>, _> = replica_a
        .mutate(OrSetMutation::Insert("apple".to_string()))
        .await;
    assert_eq!(mutation_response, Ok(OrSetResponse::Acknowledged));

    tokio::time::sleep(Duration::from_millis(1500)).await;

    let query_response: Result<OrSetResponse<String>, _> = replica_b
        .query(OrSetQuery::Contains("apple".to_string()))
        .await;
    assert_eq!(query_response, Ok(OrSetResponse::Contains(true)));

    replica_a.shutdown().await;
    replica_b.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn gc_converges_meta_lengths_across_replicas_via_minio() {
    let Some(harness) = MinioHarness::new().await else {
        return;
    };
    let timing = ReplicaTimingConfig {
        sync_interval: Duration::from_millis(100),
        discovery_interval: Duration::from_millis(100),
        gc_interval: Duration::from_millis(400),
    };

    let replica_a = harness
        .spawn_replica_with_timing(
            1,
            ServerAddr {
                ip: "127.0.0.1"
                    .parse()
                    .expect("failed to parse loopback address"),
                http_port: 19090,
                internal_port: 18080,
            },
            ORSet::<String>::new(),
            timing,
        )
        .await;
    let replica_b = harness
        .spawn_replica_with_timing(
            2,
            ServerAddr {
                ip: "127.0.0.1"
                    .parse()
                    .expect("failed to parse loopback address"),
                http_port: 19091,
                internal_port: 18081,
            },
            ORSet::<String>::new(),
            timing,
        )
        .await;

    harness
        .wait_for_membership_count(2, Duration::from_secs(10))
        .await;

    let operations = [
        OrSetMutation::Insert("apple".to_string()),
        OrSetMutation::Insert("banana".to_string()),
        OrSetMutation::Insert("citrus".to_string()),
        OrSetMutation::Remove("banana".to_string()),
        OrSetMutation::Insert("date".to_string()),
        OrSetMutation::Remove("apple".to_string()),
    ];

    for operation in operations {
        let response: Result<OrSetResponse<String>, _> = replica_a.mutate(operation).await;
        assert_eq!(response, Ok(OrSetResponse::Acknowledged));
    }

    let expected_meta = OrSetResponse::Meta(OrSetMeta {
        entry_count: 2,
        delta_log_count: 0,
    });

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let meta_a: Result<OrSetResponse<String>, _> = replica_a.query(OrSetQuery::Meta).await;
            let meta_b: Result<OrSetResponse<String>, _> = replica_b.query(OrSetQuery::Meta).await;

            if meta_a == Ok(expected_meta.clone()) && meta_b == Ok(expected_meta.clone()) {
                break;
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("timed out waiting for post-GC OR-Set meta convergence");

    let final_meta_a: Result<OrSetResponse<String>, _> = replica_a.query(OrSetQuery::Meta).await;
    let final_meta_b: Result<OrSetResponse<String>, _> = replica_b.query(OrSetQuery::Meta).await;
    assert_eq!(final_meta_a, Ok(expected_meta.clone()));
    assert_eq!(final_meta_b, Ok(expected_meta));

    replica_a.shutdown().await;
    replica_b.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn gc_converges_meta_lengths_across_replicas_via_local_filesystem_directory() {
    let harness = FilesystemHarness::new().await;
    let timing = ReplicaTimingConfig {
        sync_interval: Duration::from_millis(100),
        discovery_interval: Duration::from_millis(100),
        gc_interval: Duration::from_millis(400),
    };

    let replica_a = harness
        .spawn_replica_with_timing(
            1,
            ServerAddr {
                ip: "127.0.0.1"
                    .parse()
                    .expect("failed to parse loopback address"),
                http_port: 19190,
                internal_port: 18180,
            },
            ORSet::<String>::new(),
            timing,
        )
        .await;
    let replica_b = harness
        .spawn_replica_with_timing(
            2,
            ServerAddr {
                ip: "127.0.0.1"
                    .parse()
                    .expect("failed to parse loopback address"),
                http_port: 19191,
                internal_port: 18181,
            },
            ORSet::<String>::new(),
            timing,
        )
        .await;

    harness
        .wait_for_membership_count(2, Duration::from_secs(10))
        .await;

    let operations = [
        OrSetMutation::Insert("apple".to_string()),
        OrSetMutation::Insert("banana".to_string()),
        OrSetMutation::Insert("citrus".to_string()),
        OrSetMutation::Remove("banana".to_string()),
        OrSetMutation::Insert("date".to_string()),
        OrSetMutation::Remove("apple".to_string()),
    ];

    for operation in operations {
        let response: Result<OrSetResponse<String>, _> = replica_a.mutate(operation).await;
        assert_eq!(response, Ok(OrSetResponse::Acknowledged));
    }

    let expected_meta = OrSetResponse::Meta(OrSetMeta {
        entry_count: 2,
        delta_log_count: 0,
    });

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let meta_a: Result<OrSetResponse<String>, _> = replica_a.query(OrSetQuery::Meta).await;
            let meta_b: Result<OrSetResponse<String>, _> = replica_b.query(OrSetQuery::Meta).await;

            if meta_a == Ok(expected_meta.clone()) && meta_b == Ok(expected_meta.clone()) {
                break;
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("timed out waiting for post-GC OR-Set meta convergence");

    let final_meta_a: Result<OrSetResponse<String>, _> = replica_a.query(OrSetQuery::Meta).await;
    let final_meta_b: Result<OrSetResponse<String>, _> = replica_b.query(OrSetQuery::Meta).await;
    assert_eq!(final_meta_a, Ok(expected_meta.clone()));
    assert_eq!(final_meta_b, Ok(expected_meta));

    replica_a.shutdown().await;
    replica_b.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn shutdown_preserves_final_mutation_and_removes_departed_membership_descriptor_via_minio() {
    let Some(harness) = MinioHarness::new().await else {
        return;
    };
    let timing = ReplicaTimingConfig {
        sync_interval: Duration::from_secs(2),
        discovery_interval: Duration::from_millis(100),
        gc_interval: Duration::from_millis(400),
    };

    let replica_a = harness
        .spawn_replica_with_timing(
            1,
            ServerAddr {
                ip: "127.0.0.1"
                    .parse()
                    .expect("failed to parse loopback address"),
                http_port: 19090,
                internal_port: 18080,
            },
            ORSet::<String>::new(),
            timing,
        )
        .await;
    let replica_b = harness
        .spawn_replica_with_timing(
            2,
            ServerAddr {
                ip: "127.0.0.1"
                    .parse()
                    .expect("failed to parse loopback address"),
                http_port: 19091,
                internal_port: 18081,
            },
            ORSet::<String>::new(),
            timing,
        )
        .await;

    harness
        .wait_for_membership_count(2, Duration::from_secs(10))
        .await;

    let operations = [
        OrSetMutation::Insert("apple".to_string()),
        OrSetMutation::Insert("banana".to_string()),
        OrSetMutation::Insert("citrus".to_string()),
        OrSetMutation::Remove("banana".to_string()),
        OrSetMutation::Insert("date".to_string()),
        OrSetMutation::Remove("apple".to_string()),
    ];

    for operation in operations {
        let response: Result<OrSetResponse<String>, _> = replica_a.mutate(operation).await;
        assert_eq!(response, Ok(OrSetResponse::Acknowledged));
    }

    let final_response: Result<OrSetResponse<String>, _> = replica_b
        .mutate(OrSetMutation::Insert("elderberry".to_string()))
        .await;
    assert_eq!(final_response, Ok(OrSetResponse::Acknowledged));

    replica_b.shutdown().await;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let contains_elderberry: Result<OrSetResponse<String>, _> = replica_a
                .query(OrSetQuery::Contains("elderberry".to_string()))
                .await;
            if contains_elderberry == Ok(OrSetResponse::Contains(true)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("timed out waiting for replica a to absorb replica b's final shutdown mutation");

    harness
        .wait_for_pid_removal(2, Duration::from_secs(15))
        .await;

    let expected_meta = OrSetResponse::Meta(OrSetMeta {
        entry_count: 3,
        delta_log_count: 0,
    });

    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let meta_a: Result<OrSetResponse<String>, _> = replica_a.query(OrSetQuery::Meta).await;
            if meta_a == Ok(expected_meta.clone()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("timed out waiting for replica a to absorb replica b shutdown state and gc");

    let final_meta_a: Result<OrSetResponse<String>, _> = replica_a.query(OrSetQuery::Meta).await;
    assert_eq!(final_meta_a, Ok(expected_meta));

    let contains_elderberry: Result<OrSetResponse<String>, _> = replica_a
        .query(OrSetQuery::Contains("elderberry".to_string()))
        .await;
    assert_eq!(contains_elderberry, Ok(OrSetResponse::Contains(true)));

    replica_a.shutdown().await;
    harness.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn shutdown_preserves_final_mutation_and_removes_departed_membership_descriptor_via_local_filesystem_directory(
) {
    let harness = FilesystemHarness::new().await;
    let timing = ReplicaTimingConfig {
        sync_interval: Duration::from_secs(2),
        discovery_interval: Duration::from_millis(100),
        gc_interval: Duration::from_millis(400),
    };

    let replica_a = harness
        .spawn_replica_with_timing(
            1,
            ServerAddr {
                ip: "127.0.0.1"
                    .parse()
                    .expect("failed to parse loopback address"),
                http_port: 19190,
                internal_port: 18180,
            },
            ORSet::<String>::new(),
            timing,
        )
        .await;
    let replica_b = harness
        .spawn_replica_with_timing(
            2,
            ServerAddr {
                ip: "127.0.0.1"
                    .parse()
                    .expect("failed to parse loopback address"),
                http_port: 19191,
                internal_port: 18181,
            },
            ORSet::<String>::new(),
            timing,
        )
        .await;

    harness
        .wait_for_membership_count(2, Duration::from_secs(10))
        .await;

    let operations = [
        OrSetMutation::Insert("apple".to_string()),
        OrSetMutation::Insert("banana".to_string()),
        OrSetMutation::Insert("citrus".to_string()),
        OrSetMutation::Remove("banana".to_string()),
        OrSetMutation::Insert("date".to_string()),
        OrSetMutation::Remove("apple".to_string()),
    ];

    for operation in operations {
        let response: Result<OrSetResponse<String>, _> = replica_a.mutate(operation).await;
        assert_eq!(response, Ok(OrSetResponse::Acknowledged));
    }

    let final_response: Result<OrSetResponse<String>, _> = replica_b
        .mutate(OrSetMutation::Insert("elderberry".to_string()))
        .await;
    assert_eq!(final_response, Ok(OrSetResponse::Acknowledged));

    replica_b.shutdown().await;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let contains_elderberry: Result<OrSetResponse<String>, _> = replica_a
                .query(OrSetQuery::Contains("elderberry".to_string()))
                .await;
            if contains_elderberry == Ok(OrSetResponse::Contains(true)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("timed out waiting for replica a to absorb replica b's final shutdown mutation");

    harness
        .wait_for_pid_removal(2, Duration::from_secs(15))
        .await;

    let expected_meta = OrSetResponse::Meta(OrSetMeta {
        entry_count: 3,
        delta_log_count: 0,
    });

    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let meta_a: Result<OrSetResponse<String>, _> = replica_a.query(OrSetQuery::Meta).await;
            if meta_a == Ok(expected_meta.clone()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("timed out waiting for replica a to absorb replica b shutdown state and gc");

    let final_meta_a: Result<OrSetResponse<String>, _> = replica_a.query(OrSetQuery::Meta).await;
    assert_eq!(final_meta_a, Ok(expected_meta));

    let contains_elderberry: Result<OrSetResponse<String>, _> = replica_a
        .query(OrSetQuery::Contains("elderberry".to_string()))
        .await;
    assert_eq!(contains_elderberry, Ok(OrSetResponse::Contains(true)));

    replica_a.shutdown().await;
    harness.cleanup().await;
}
