pub mod faiss_proxy;
pub mod helpers;
pub mod vector_client;
pub mod vector_db;
pub mod vector_dbc;
pub mod vector_dbw;
pub mod vector_experiment;

use gresse::{
    object_storage::ObjectStorageConfig, prelude::*, replica::Replica,
    replica_helpers::ReplicaConfig,
};
// use dialoguer::{theme::ColorfulTheme, MultiSelect};
use faiss_proxy::FaissProxy;
use helpers::to_absolute;
use std::{sync::Arc, time::Duration};
use tokio::sync::RwLock;
use vector_dbc::VectorDBC;
use vector_dbw::VectorDBW;
use vector_experiment::ExperimentConfig;

const RUNTIME: u32 = 100; // runtime in seconds
const DIMENSIONS: u32 = 128; // vector dimensionality
const SYNC_INTERVAL: Duration = Duration::from_secs(1); // synchronization interval
const CLIENTS: u64 = 10; // number of clients
const K: usize = 10; // query argument (number of nearest vectors)
const PERCENT_READS: f64 = 0.5; // query/mutate ratio for client events
const PERCENT_INSERTS: f64 = 0.5; // insert/remove ratio for client mutation events
const PERCENT_S: f64 = 0.5; // first/second table event ratio

/// Generates a list of experiment configurations with different parameters
fn generate_experiment_configs() -> Vec<(String, String, ExperimentConfig)> {
    // (variant, elements, eps, selectivity, n_servers)
    let mut parameter_set: Vec<(&str, u64, u32, f64, u32)> = vec![];

    // Parameter set generation:
    for eps in [200, 500, 1000, 1500, 2000] {
        for servers in [3, 6] {
            for var in ["dbc", "dbw"] {
                parameter_set.push((var, 10000, eps, 0.1, servers));
            }
        }
        // we set selectivity to 0.1 to access a prebuilt database
        parameter_set.push(("faiss", 10000, eps, 0.1, 1));
    }

    // Generate full configs with path names etc.
    let mut configs = Vec::new();
    for (var, elems, eps, selectivity, servers) in parameter_set {
        let experiment_name = format!(
            "{}-{}S-{}U-{}eps-{}selectivity-{}servers",
            var, elems, elems, eps, selectivity, servers
        );
        let config = ExperimentConfig {
            db_dir_path: to_absolute(format!(
                "results/dbs/{}S-{}U-{}sel/",
                elems, elems, selectivity
            )),
            result_dir_path: to_absolute(format!("results/{}/", experiment_name)),
            server_list_file_path: to_absolute(format!("results/{}/servers.json", experiment_name)),
            config_path: to_absolute(format!("results/{}/config.json", experiment_name)),
            runtime: RUNTIME,
            clients: CLIENTS,
            sync_interval: SYNC_INTERVAL,
            dimensions: DIMENSIONS,
            percent_reads: PERCENT_READS,
            percent_inserts: PERCENT_INSERTS,
            percent_s: PERCENT_S,
            k: K,
            init_s: elems,
            init_u: elems,
            eps,
            servers,
            max_distance: vector_experiment::get_max_distance(selectivity, DIMENSIONS).unwrap(),
        };
        configs.push((var.into(), experiment_name, config));
    }
    configs
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let experiment_configs = generate_experiment_configs();
    let mut port_offset = 0;
    let num_experiments = experiment_configs.len();

    // Run all experiments
    for (i, (variant, config_name, base_config)) in experiment_configs.into_iter().enumerate() {
        println!(
            "Running experiment configuration {} of {}: {}",
            i + 1,
            num_experiments,
            config_name
        );
        run_experiment(base_config, &variant, port_offset).await?;
        port_offset += 100;
    }

    println!("All experiments completed successfully");
    Ok(())
}

/// Runs a single experiment with a specific DB type and replication mode
async fn run_experiment(
    config: ExperimentConfig,
    db_type: &str,
    port_offset: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create experiment directory
    std::fs::create_dir_all(&config.result_dir_path)?;

    // Remove old server list
    config.truncate_server_list().await;

    // Save configuration
    let config_json = serde_json::to_string_pretty(&config)?;
    std::fs::write(&config.config_path, config_json)?;

    // Start servers based on DB type
    let (addrs, handles, shutdown_handles) = match db_type {
        "dbc" => {
            let (_, addrs, handles, shutdown_handles) =
                run_dbc_servers(config.clone(), port_offset).await;
            (addrs, handles, shutdown_handles)
        }
        "dbw" => {
            let (_, addrs, handles, shutdown_handles) =
                run_dbw_servers(config.clone(), port_offset).await;
            (addrs, handles, shutdown_handles)
        }
        "faiss" => {
            let (addr, handle, shutdown_handle) =
                run_faiss_server(config.clone(), port_offset).await;
            (vec![addr], vec![handle], vec![shutdown_handle])
        }
        _ => return Err("Invalid DB type".into()),
    };

    // Let servers initialize
    tokio::time::sleep(std::time::Duration::from_secs(30)).await;

    // Run client
    println!("Starting client for {}, addrs {:?}", db_type, addrs);
    let mut bench_client = vector_client::BenchClient::new(addrs, config.clone()).await;
    bench_client.run().await?;

    // Let servers synchronize
    println!("Client complete, letting servers synchronize...");
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Gracefully shut down all servers
    println!("Gracefully shutting down servers...");
    for shutdown_handle in shutdown_handles {
        let _ = shutdown_handle.send(());
    }

    // Give servers time to shut down gracefully
    tokio::time::sleep(std::time::Duration::from_secs(30)).await;

    // Now abort any tasks that didn't shut down gracefully
    for handle in handles {
        eprintln!("Aborting task that was not already shut down");
        handle.abort();
    }

    println!("Experiment complete: {:?} ", config.result_dir_path);

    // Wait for resources to be fully released
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    Ok(())
}

async fn run_dbc_servers(
    config: ExperimentConfig,
    port_offset: u16,
) -> (
    Vec<Arc<RwLock<VectorDBC>>>,
    Vec<ServerAddr>,
    Vec<tokio::task::JoinHandle<()>>,
    Vec<tokio::sync::oneshot::Sender<()>>,
) {
    let pids = config.get_pids();
    let mut dbs = Vec::new();
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    let mut shutdown_handles = Vec::new();
    let base_db = config.prebuild_db();
    for pid in pids {
        let db = Arc::new(RwLock::new(VectorDBC::new(pid, base_db.clone())));
        let addr = ServerAddr::default().increment_ports((10 * pid as u16) + port_offset);
        dbs.push(db.clone());
        let (mut server, shutdown_sender) = Replica::with_shared_config(
            pid,
            db,
            ReplicaConfig {
                address: addr,
                sync_interval: config.sync_interval,
                result_dir_path: config.result_dir_path.clone(),
                object_storage_config: local_object_storage_config(),
            },
        )
        .await;
        addrs.push(addr);

        // Get shutdown handle before spawning the server task
        shutdown_handles.push(shutdown_sender);

        let handle = tokio::spawn(async move {
            server.run().await;
        });
        handles.push(handle);
    }
    (dbs, addrs, handles, shutdown_handles)
}

async fn run_dbw_servers(
    config: ExperimentConfig,
    port_offset: u16,
) -> (
    Vec<Arc<RwLock<VectorDBW>>>,
    Vec<ServerAddr>,
    Vec<tokio::task::JoinHandle<()>>,
    Vec<tokio::sync::oneshot::Sender<()>>,
) {
    let pids = config.get_pids();
    let mut dbs = Vec::new();
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    let mut shutdown_handles = Vec::new();
    let base_db = config.prebuild_db();
    for pid in pids {
        let db = Arc::new(RwLock::new(VectorDBW::new(pid, base_db.clone())));
        let addr = ServerAddr::default().increment_ports((10 * pid as u16) + port_offset);
        dbs.push(db.clone());
        let (mut server, shutdown_sender) = Replica::with_shared_config(
            pid,
            db,
            ReplicaConfig {
                address: addr,
                sync_interval: config.sync_interval,
                result_dir_path: config.result_dir_path.clone(),
                object_storage_config: local_object_storage_config(),
            },
        )
        .await;
        addrs.push(addr);

        // Get shutdown handle before spawning the server task
        shutdown_handles.push(shutdown_sender);

        let handle = tokio::spawn(async move {
            server.run().await;
        });
        handles.push(handle);
    }
    (dbs, addrs, handles, shutdown_handles)
}

async fn run_faiss_server(
    config: ExperimentConfig,
    port_offset: u16,
) -> (
    ServerAddr,
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let pid = config.get_pids()[0];
    let addr = ServerAddr::default().increment_ports((10 * pid as u16) + port_offset);
    _ = config.prebuild_db(); // save db to files so that it can be read by faiss server
    let (mut faiss_proxy, shutdown_sender) = FaissProxy::new(addr, config.clone().config_path);
    let handle = tokio::spawn(async move {
        faiss_proxy.run().await;
    });
    (addr, handle, shutdown_sender)
}

fn local_object_storage_config() -> ObjectStorageConfig {
    ObjectStorageConfig {
        url: "http://localhost:9000".to_string(),
        region: "local".to_string(),
        bucket: "gresse".to_string(),
        access_key: "gresse".to_string(),
        secret_key: "gresse".to_string(),
        persistent_replica_path: "replicas".to_string(),
        membership_directory_path: "membership".to_string(),
        discovery_interval: Duration::from_secs(1),
    }
}

#[cfg(test)]
mod tests {
    use gresse::db::composite_view::CompositeView;
    use gresse::prelude::ServerAddr;
    use helpers::Float;
    use std::time::Duration;
    use vector_client::BenchClient;
    use vector_experiment::*;

    use super::*;

    fn testing_cfg() -> ExperimentConfig {
        let selectivity = 0.2;
        let size = 500;
        let dimensions = 32;
        ExperimentConfig {
            db_dir_path: to_absolute(format!("tests/dbs/{size}_{selectivity}/")),
            server_list_file_path: to_absolute("tests/"),
            result_dir_path: to_absolute("tests/results/testing_results/"),
            config_path: to_absolute("tests/"),
            runtime: 100,
            clients: 10,
            servers: 10,
            sync_interval: Duration::from_secs(1),
            dimensions,
            max_distance: vector_experiment::get_max_distance(selectivity, dimensions).unwrap(),
            init_s: size,
            init_u: size,
            k: 10,
            eps: 100,
            percent_reads: 0.5,
            percent_inserts: 0.5,
            percent_s: 0.5,
        }
    }

    async fn run_client(addrs: Vec<ServerAddr>, config: &ExperimentConfig) {
        let mut bench_client = BenchClient::new(addrs, config.clone()).await;
        bench_client.run().await.expect("client_failed");
    }

    /// Test runner for VectorDBC tests
    async fn run_dbc_test(port_offset: u16, test_name: &str) {
        let mut config = testing_cfg();
        config
            .server_list_file_path
            .push(format!("{}_server_list.json", test_name));
        config
            .config_path
            .push(format!("{}_config.json", test_name));
        config.truncate_server_list().await;

        let config_json = serde_json::to_string_pretty(&config).unwrap();
        std::fs::write(&config.config_path, config_json).unwrap();

        // Run DBC servers
        let (dbs, addrs, _, shutdown_handles) = run_dbc_servers(config.clone(), port_offset).await;

        // Let the servers synchronize properly
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Collect initial views
        let mut initial_views = Vec::new();
        for dbc in dbs.clone() {
            let readable = dbc.read().await;
            initial_views.push(readable.db.v.clone())
        }

        // Run client
        run_client(addrs, &config).await;

        // Let the servers synchronize properly
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Collect final state
        let mut views = Vec::new();
        let mut dot_maps = Vec::new();
        for dbc in dbs {
            let readable = dbc.read().await;
            views.push(readable.db.v.clone());
            dot_maps.push(readable.dot_map.clone());
        }

        // Shutdown servers
        println!("Shutting down servers");
        for handle in shutdown_handles {
            handle.send(()).unwrap();
        }
        // Run standard assertions
        do_asserts(initial_views, views, config);
    }

    /// Test runner for VectorDBW tests
    async fn run_dbw_test(port_offset: u16, test_name: &str) {
        let mut config = testing_cfg();
        config
            .server_list_file_path
            .push(format!("{}_server_list.json", test_name));
        config
            .config_path
            .push(format!("{}_config.json", test_name));
        config.truncate_server_list().await;

        let config_json = serde_json::to_string_pretty(&config).unwrap();
        std::fs::write(&config.config_path, config_json).unwrap();

        // Run DBW servers
        let (dbs, addrs, _, shutdown_handles) = run_dbw_servers(config.clone(), port_offset).await;

        // Let the servers synchronize properly
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Collect initial views
        let mut initial_views = Vec::new();
        for dbw in dbs.clone() {
            let readable = dbw.read().await;
            initial_views.push(readable.db.v.clone())
        }

        // Run client
        run_client(addrs, &config).await;

        // Let the servers synchronize properly
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Collect final state
        let mut views = Vec::new();
        let mut dot_maps = Vec::new();
        for dbw in dbs {
            let readable = dbw.read().await;
            views.push(readable.db.v.clone());
            dot_maps.push(readable.dot_map.clone());
        }

        // Shutdown servers
        println!("Shutting down servers");
        for handle in shutdown_handles {
            handle.send(()).unwrap();
        }
        // Run standard assertions
        do_asserts(initial_views, views, config);
    }

    #[tokio::test]
    async fn test_dbc() {
        run_dbc_test(100, "test_dbc").await;
    }

    #[tokio::test]
    async fn test_dbw() {
        run_dbw_test(300, "test_dbw").await;
    }

    fn do_asserts(
        initial_views: Vec<CompositeView<u64, Float>>,
        views: Vec<CompositeView<u64, Float>>,
        _config: ExperimentConfig,
    ) {
        //views[0].export_sorted_view("final_view_0.json").unwrap();
        for i in 1..views.len() {
            // views[i]
            //     .export_sorted_view(&format!("final_view_{}.json", i))
            //     .unwrap();
            assert_eq!(
                views[i], views[0],
                "Views at index {} and 0 are not identical",
                i
            );
            assert_ne!(
                views[i], initial_views[i],
                "Views at index {} did not change",
                i
            );
            assert!(views[i].len() > 10, "View is empty");
        }
    }
}
