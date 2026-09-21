use gresse::logging;
use gresse::orset::ORSet;
use gresse::replica::Replica;
use gresse::replica_helpers::ReplicaConfig;
use std::env;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    logging::init();

    let pid = env_u128("GRESSE_BENCH_PID");
    let gc_interval = env_duration_ms("GRESSE_GC_INTERVAL_MS");
    let config = ReplicaConfig::from_env();

    let (mut replica, shutdown_sender) =
        Replica::with_config(pid, ORSet::<i32>::new(), config).await;
    if let Some(gc_interval) = gc_interval {
        replica.set_gc_interval(gc_interval);
    }

    let join_handle = tokio::spawn(async move {
        replica.run().await;
    });

    wait_for_shutdown_signal().await?;
    let _ = shutdown_sender.send(());
    join_handle.await?;
    Ok(())
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> Result<(), Box<dyn std::error::Error>> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    tokio::select! {
        _ = sigint.recv() => {}
        _ = sigterm.recv() => {}
    }
    Ok(())
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> Result<(), Box<dyn std::error::Error>> {
    tokio::signal::ctrl_c().await?;
    Ok(())
}

fn env_u128(name: &str) -> u128 {
    env::var(name)
        .unwrap_or_else(|_| panic!("{name} environment variable is required"))
        .parse()
        .unwrap_or_else(|_| panic!("{name} must be an unsigned integer"))
}

fn env_duration_ms(name: &str) -> Option<Duration> {
    env::var(name).ok().map(|value| {
        Duration::from_millis(
            value
                .parse()
                .unwrap_or_else(|_| panic!("{name} must be an integer number of milliseconds")),
        )
    })
}
