mod harness;

use gresse::dots::{DotSet, VersionMatrix};
use gresse::orset::{ORSet, OrSetMutation, OrSetQuery, OrSetResponse};
use gresse::prelude::ServerAddr;
use harness::{minio_skip_message, minio_tests_enabled, MinioHarness};
use serial_test::serial;
use std::time::Duration;

#[test]
fn integration_test_harness_runs() {
    let stable = VersionMatrix::new().get_stable();
    assert_eq!(stable, DotSet::new());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn replica_bootstraps_against_local_minio() {
    if !minio_tests_enabled() {
        eprintln!("{}", minio_skip_message());
        return;
    }

    let harness = MinioHarness::new();
    let replica = harness
        .spawn_replica(
            1,
            ServerAddr {
                ip: "127.0.0.1".parse().expect("failed to parse loopback address"),
                http_port: 19090,
                internal_port: 18080,
            },
            ORSet::<String>::new(),
        )
        .await;

    harness.wait_for_bootstrap(Duration::from_secs(10)).await;
    replica.shutdown().await;
    harness.cleanup();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn mutation_replicates_between_two_replicas() {
    if !minio_tests_enabled() {
        eprintln!("{}", minio_skip_message());
        return;
    }

    let harness = MinioHarness::new();
    let replica_a = harness
        .spawn_replica(
            1,
            ServerAddr {
                ip: "127.0.0.1".parse().expect("failed to parse loopback address"),
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
                ip: "127.0.0.1".parse().expect("failed to parse loopback address"),
                http_port: 19091,
                internal_port: 18081,
            },
            ORSet::<String>::new(),
        )
        .await;

    harness.wait_for_membership_count(2, Duration::from_secs(10)).await;

    let mutation_response: Result<OrSetResponse<String>, _> =
        replica_a.mutate(OrSetMutation::Insert("apple".to_string())).await;
    assert_eq!(mutation_response, Ok(OrSetResponse::Acknowledged));

    tokio::time::sleep(Duration::from_millis(1500)).await;

    let query_response: Result<OrSetResponse<String>, _> =
        replica_b.query(OrSetQuery::Contains("apple".to_string())).await;
    assert_eq!(query_response, Ok(OrSetResponse::Contains(true)));

    replica_a.shutdown().await;
    replica_b.shutdown().await;
    harness.cleanup();
}
