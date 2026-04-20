use std::net::SocketAddr;

use super::vector_experiment::*;
use crate::helpers::*;
use gresse::http_client;
use gresse::http_client::*;
use gresse::prelude::*;
use csv::WriterBuilder;
use futures::future::join_all;
use tokio::task::JoinHandle;

pub struct BenchClient {
    clients: Vec<DbClient>,
}

impl BenchClient {
    pub async fn new(servers: Vec<ServerAddr>, config: ExperimentConfig) -> Self {
        let mut clients = Vec::new();
        let mut generators = WorkloadGenerator::from_config(&config);
        for i in 0..config.clients {
            let client = DbClient::new(
                servers[i as usize % servers.len()].http(),
                &config,
                generators.pop().unwrap(),
            )
            .await;
            clients.push(client);
        }
        // Return the bench client as it is now ready to run
        BenchClient { clients }
    }

    pub async fn run(&mut self) -> Result<(), http_client::HttpError> {
        let mut handles = Vec::new();

        while let Some(mut client) = self.clients.pop() {
            let handle: JoinHandle<Result<(), http_client::HttpError>> =
                tokio::spawn(async move { client.run().await });
            handles.push(handle);
        }

        // Wait for all futures to complete
        let results = join_all(handles).await;

        for result in results {
            match result {
                Ok(res) => {
                    println!("Client completed with {:?}", res);
                }
                Err(e) => {
                    eprintln!("Client failed: {:?}", e);
                }
            }
        }
        Ok(())
    }
}

pub struct DbClient {
    sender: HttpClient<DbRequest, DbResponse>,
    writer: csv::Writer<std::fs::File>,
    generator: WorkloadGenerator,
    runtime_micros: u128,
}

impl DbClient {
    pub async fn new(
        addr: SocketAddr,
        config: &ExperimentConfig,
        generator: WorkloadGenerator,
    ) -> Self {
        let sender = HttpClient::new(HOST, URI, addr).await;
        let mut result_path = config.result_dir_path.clone();
        result_path.push(format!("client_{}.csv", generator.id));
        if let Some(parent) = result_path.parent() {
            std::fs::create_dir_all(parent).expect("Failed to create directories");
        }
        let result_file = std::fs::File::create(result_path.clone())
            .unwrap_or_else(|_| panic!("Failed to create result file {:?}", result_path));
        let writer = WriterBuilder::new().flexible(true).from_writer(result_file);

        DbClient {
            sender,
            writer,
            generator,
            runtime_micros: (config.runtime as u128) * 1_000_000,
        }
    }

    pub async fn run(&mut self) -> Result<(), http_client::HttpError> {
        self.generator.set_start();
        let start_time = now_micros();
        while now_micros() < (start_time + self.runtime_micros) {
            let sent = now_micros();
            let req = self.generator.get_next().await;
            let _ = self.sender.send(&req).await?;
            let received = now_micros();
            self.writer
                .write_record(&[req.record_str(), sent.to_string(), received.to_string()])
                .expect("Failed to write record");
        }
        self.writer.flush().expect("Failed to flush writer");
        Ok(())
    }
}
