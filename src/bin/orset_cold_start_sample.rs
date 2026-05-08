use gresse::logging;
use gresse::orset::ORSet;
use gresse::prelude::now_micros;
use gresse::replica::Replica;
use gresse::replica_helpers::ReplicaConfig;
use serde::Serialize;
use std::env;
use std::time::Duration;

#[derive(Debug, Serialize)]
struct ColdStartSample {
    process_start_us: u128,
    logging_initialized_us: u128,
    config_loaded_us: u128,
    replica_with_config_completed_us: u128,
    replica_task_spawned_us: u128,
    crdt_pid_set_us: u128,
    http_server_ready_us: u128,
    network_manager_ready_us: u128,
    metric_writer_ready_us: u128,
    object_storage_client_start_us: u128,
    object_storage_client_ready_us: u128,
    replica_run_start_us: u128,
    replica_init_start_us: u128,
    persistent_state_fetch_completed_us: u128,
    membership_descriptor_write_completed_us: u128,
    membership_directory_read_completed_us: u128,
    replica_init_completed_us: u128,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let process_start_us = now_micros();
    logging::init();
    let logging_initialized_us = now_micros();

    let pid = env_u128("GRESSE_BENCH_PID");
    let startup_timeout = env_duration_secs("GRESSE_COLD_START_TIMEOUT_SECS")
        .unwrap_or_else(|| Duration::from_secs(30));
    let config = ReplicaConfig::from_env();
    let config_loaded_us = now_micros();

    let (mut replica, shutdown_sender) =
        Replica::with_config(pid, ORSet::<i32>::new(), config).await;
    let replica_with_config_completed_us = now_micros();
    let startup_metrics_receiver = replica.take_startup_metrics_receiver();

    let join_handle = tokio::spawn(async move {
        replica.run().await;
    });
    let replica_task_spawned_us = now_micros();

    let startup_metrics = tokio::time::timeout(startup_timeout, startup_metrics_receiver).await??;
    let _ = shutdown_sender.send(());
    join_handle.await?;

    let sample = ColdStartSample {
        process_start_us,
        logging_initialized_us,
        config_loaded_us,
        replica_with_config_completed_us,
        replica_task_spawned_us,
        crdt_pid_set_us: startup_metrics.crdt_pid_set_us,
        http_server_ready_us: startup_metrics.http_server_ready_us,
        network_manager_ready_us: startup_metrics.network_manager_ready_us,
        metric_writer_ready_us: startup_metrics.metric_writer_ready_us,
        object_storage_client_start_us: startup_metrics.object_storage_client_start_us,
        object_storage_client_ready_us: startup_metrics.object_storage_client_ready_us,
        replica_run_start_us: startup_metrics.replica_run_start_us,
        replica_init_start_us: startup_metrics.replica_init_start_us,
        persistent_state_fetch_completed_us: startup_metrics.persistent_state_fetch_completed_us,
        membership_descriptor_write_completed_us: startup_metrics
            .membership_descriptor_write_completed_us,
        membership_directory_read_completed_us: startup_metrics
            .membership_directory_read_completed_us,
        replica_init_completed_us: startup_metrics.replica_init_completed_us,
    };
    println!("{}", serde_json::to_string(&sample)?);
    Ok(())
}

fn env_u128(name: &str) -> u128 {
    env::var(name)
        .unwrap_or_else(|_| panic!("{name} environment variable is required"))
        .parse()
        .unwrap_or_else(|_| panic!("{name} must be an unsigned integer"))
}

fn env_duration_secs(name: &str) -> Option<Duration> {
    env::var(name).ok().map(|value| {
        Duration::from_secs(
            value
                .parse()
                .unwrap_or_else(|_| panic!("{name} must be an integer number of seconds")),
        )
    })
}
