use gresse::crdt::{DeltaGroup, CRDT};
use gresse::dots::{Dot, DotMap, DotSet};
use gresse::prelude::Pid;
use gresse::replica::Replica;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crdt = DummyCRDT::new();
    let handle = Replica::new(crdt).await;
    handle.join_handle.await?;
    Ok(())
}

/// DummyCRDT is a deliberately simple benchmark CRDT for exercising the
/// Gresse runtime with a serverless-function-shaped workload.
///
/// Clients send either:
/// - `BenchFunctionMutation::Run(BenchFunctionRequest)` to execute one
///   benchmark invocation.
/// - `BenchFunctionQuery::LastMeasurement` to read the most recent benchmark
///   result observed by this replica.
/// - `BenchFunctionQuery::StateSize { state_key }` to read the byte size of a
///   stored state blob at a key.
///
/// A `Run` mutation mirrors the Python `template.py` handler:
/// - `state_size_kb` controls how many KiB of synthetic state are generated.
/// - `state_key` selects where that state blob is stored in the CRDT map.
/// - `ops` controls the size of a bounded lightweight compute loop.
///
/// On each `Run`, the replica:
/// - generates deterministic synthetic state bytes,
/// - writes them into `state`,
/// - reads the same key back,
/// - runs the accumulator loop,
/// - records write/read/compute timings in microseconds,
/// - marks the replica warm after the first request,
/// - appends a delta for the invocation.
///
/// The mutation response is `BenchFunctionUserResponse::Mutation`, containing
/// a `BenchFunctionResponse` with a request id, cold-start flag, begin/end
/// timestamps, and the benchmark measurement.
///
/// Replicas synchronize `BenchFunctionDelta` values. Each delta contains:
/// - the causal `Dot`,
/// - the original `BenchFunctionMutation`,
/// - the measured `BenchFunctionResponse`.
///
/// When a remote delta is merged, the receiving replica materializes the same
/// synthetic state blob for the mutation's `state_key`, records the remote
/// measurement as its latest observed measurement, advances its version vector,
/// and stores the delta in `delta_log` so it can be forwarded to other replicas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DummyCRDT {
    pid: Pid,
    is_cold: bool,
    state: HashMap<String, Vec<u8>>,
    last_measurement: Option<BenchFunctionResponse>,
    delta_log: DotMap<BenchFunctionDelta>,
    version_vector: DotSet,
}

impl DummyCRDT {
    pub fn new() -> Self {
        Self {
            pid: 0,
            is_cold: true,
            state: HashMap::new(),
            last_measurement: None,
            delta_log: DotMap::new(),
            version_vector: DotSet::new(),
        }
    }

    fn handle_run(&mut self, request: BenchFunctionRequest) -> BenchFunctionResponse {
        let request_id = format!("{:032x}", random_u128());
        let begin = now_micros();
        let state_size_kb = request.state_size_kb();
        let state_key = request.state_key();
        let ops = request.ops();

        let state_blob = generated_state_blob(state_size_kb);

        let write_begin = now_micros();
        self.state.insert(state_key.clone(), state_blob);
        let state_write_lat_us = now_micros().saturating_sub(write_begin);

        let read_begin = now_micros();
        let _ = self.state.get(&state_key);
        let state_read_lat_us = now_micros().saturating_sub(read_begin);

        let compute_begin = now_micros();
        let mut accumulator = 0;
        for idx in 0..(ops * 64).min(20_000) {
            accumulator = (accumulator + idx + state_size_kb) % 1_000_003;
        }
        let compute_time_us = now_micros().saturating_sub(compute_begin);

        let response = BenchFunctionResponse {
            request_id,
            is_cold: self.is_cold,
            begin,
            end: now_micros(),
            measurement: BenchFunctionMeasurement {
                compute_time_us,
                state_read_lat_us,
                state_write_lat_us,
                state_size_kb,
                state_ops: ops,
                accumulator,
            },
        };

        self.is_cold = false;
        self.last_measurement = Some(response.clone());
        response
    }

    fn apply_delta(&mut self, delta: BenchFunctionDelta) -> (u16, u16) {
        if self.version_vector.contains(&delta.dot) {
            return (0, 0);
        }

        let BenchFunctionMutation::Run(request) = &delta.mutation;
        let state_key = request.state_key();
        self.state
            .insert(state_key, generated_state_blob(request.state_size_kb()));
        self.last_measurement = Some(delta.response.clone());
        self.is_cold = false;
        self.version_vector.insert(&delta.dot);
        self.delta_log.insert(delta.dot, delta);
        (1, 0)
    }
}

impl Default for DummyCRDT {
    fn default() -> Self {
        Self::new()
    }
}

impl CRDT for DummyCRDT {
    type Delta = BenchFunctionDelta;
    type Query = BenchFunctionQuery;
    type Mutation = BenchFunctionMutation;
    type ClientResponse = BenchFunctionClientResponse;
    type Error = BenchFunctionError;

    fn query(&self, query: Self::Query) -> Self::ClientResponse {
        let response = match query {
            BenchFunctionQuery::LastMeasurement => {
                BenchFunctionQueryResponse::LastMeasurement(self.last_measurement.clone())
            }
            BenchFunctionQuery::StateSize { state_key } => {
                BenchFunctionQueryResponse::StateSize(self.state.get(&state_key).map(Vec::len))
            }
        };
        Ok(BenchFunctionUserResponse::Query(response))
    }

    fn set_pid(&mut self, pid: Pid) {
        self.pid = pid;
    }

    fn mutate(&mut self, mutation: Self::Mutation) -> Self::ClientResponse {
        let response = match mutation.clone() {
            BenchFunctionMutation::Run(request) => self.handle_run(request),
        };
        let dot = self.version_vector.increment_and_get(self.pid);
        self.delta_log.insert(
            dot,
            BenchFunctionDelta {
                dot,
                mutation,
                response: response.clone(),
            },
        );
        Ok(BenchFunctionUserResponse::Mutation(response))
    }

    fn get_version_vector(&self) -> &DotSet {
        &self.version_vector
    }

    fn get_delta(&self, version_vector: &DotSet) -> (DeltaGroup<Self::Delta>, u16, u16) {
        let list = self
            .delta_log
            .get_all_greater_iter(version_vector)
            .map(|(_, delta)| delta.clone())
            .collect::<Vec<_>>();
        let count = list.len() as u16;
        (
            DeltaGroup {
                list,
                version_vector: self.version_vector.clone(),
            },
            count,
            0,
        )
    }

    fn merge_delta_group(&mut self, delta_group: DeltaGroup<Self::Delta>) -> (u16, u16) {
        let mut counts = (0, 0);
        for delta in delta_group.list {
            let delta_counts = self.apply_delta(delta);
            counts.0 += delta_counts.0;
            counts.1 += delta_counts.1;
        }
        counts
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BenchFunctionRequest {
    pub state_size_kb: Option<usize>,
    pub state_key: Option<String>,
    pub ops: Option<usize>,
}

impl BenchFunctionRequest {
    fn state_size_kb(&self) -> usize {
        self.state_size_kb.unwrap_or(1).max(1)
    }

    fn state_key(&self) -> String {
        self.state_key
            .clone()
            .unwrap_or_else(|| "bench:state".to_string())
    }

    fn ops(&self) -> usize {
        self.ops.unwrap_or(1).max(1)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BenchFunctionQuery {
    LastMeasurement,
    StateSize { state_key: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BenchFunctionMutation {
    Run(BenchFunctionRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BenchFunctionMeasurement {
    pub compute_time_us: u128,
    pub state_read_lat_us: u128,
    pub state_write_lat_us: u128,
    pub state_size_kb: usize,
    pub state_ops: usize,
    pub accumulator: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BenchFunctionResponse {
    pub request_id: String,
    pub is_cold: bool,
    pub begin: u128,
    pub end: u128,
    pub measurement: BenchFunctionMeasurement,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BenchFunctionQueryResponse {
    LastMeasurement(Option<BenchFunctionResponse>),
    StateSize(Option<usize>),
}

#[derive(Debug, Error, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BenchFunctionError {
    #[error("benchmark request failed")]
    RequestFailed,
}

pub type BenchFunctionClientResponse = Result<BenchFunctionUserResponse, BenchFunctionError>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BenchFunctionUserResponse {
    Mutation(BenchFunctionResponse),
    Query(BenchFunctionQueryResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BenchFunctionDelta {
    dot: Dot,
    mutation: BenchFunctionMutation,
    response: BenchFunctionResponse,
}

fn generated_state_blob(state_size_kb: usize) -> Vec<u8> {
    (0..state_size_kb * 1024)
        .map(|idx| ((idx * 31 + state_size_kb) % 256) as u8)
        .collect()
}

fn random_u128() -> u128 {
    rand::random()
}

fn now_micros() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is before UNIX_EPOCH")
        .as_micros()
}
