# Gresse

Geo-replicated Stateful Serverless for the Edge.

Gresse lets an application define its state as a custom CRDT, then run that CRDT as a replicated server. The application owns the data type and merge semantics. Gresse owns the replica runtime: HTTP client requests, peer-to-peer delta synchronization, membership discovery, and object-storage-backed bootstrap state.

# SEC_2026 Branch
Checkout the SEC_2026 branch to view all the results, experiments and code that were used for the SEC 2026 Paper.

## Usage

Define a custom CRDT by implementing the `CRDT` trait:

```rust
use gresse::crdt::{CRDT, DeltaGroup};
use gresse::dots::DotSet;
use gresse::prelude::Pid;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MyCrdt {
    pid: Pid,
    version_vector: DotSet,
}

impl CRDT for MyCrdt {
    type Delta = MyDelta;
    type Query = MyQuery;
    type Mutation = MyMutation;
    type ClientResponse = MyResponse;
    type Error = MyError;

    fn query(&self, query: Self::Query) -> Self::ClientResponse {
        // Read local state.
    }

    fn set_pid(&mut self, pid: Pid) {
        self.pid = pid;
    }

    fn mutate(&mut self, mutation: Self::Mutation) -> Self::ClientResponse {
        // Apply a local mutation and record the corresponding delta.
    }

    fn get_version_vector(&self) -> &DotSet {
        &self.version_vector
    }

    fn get_delta(&self, version_vector: &DotSet) -> (DeltaGroup<Self::Delta>, u16, u16) {
        // Return deltas newer than the supplied version vector.
    }

    fn merge_delta_group(&mut self, delta: DeltaGroup<Self::Delta>) -> (u16, u16) {
        // Merge remote deltas into local state.
    }
}
```

Deploy it by running a `Replica<Crdt>`:

```rust
use gresse::replica::Replica;

#[tokio::main]
async fn main() {
    let crdt = MyCrdt::new();
    let handle = Replica::new(crdt).await;

    handle.join_handle.await.expect("replica task failed");
}
```

`Replica::new` generates a fresh replica PID, reads configuration from environment variables, starts the HTTP server, starts the internal replication server, joins membership through object storage, loads persistent state when available, and begins synchronizing with other replicas.

## Logging

Gresse now uses the standard `log` facade with `env_logger`.

Initialize logging once near process startup:

```rust
gresse::logging::init();
```

The crate default is compile-time `info` logging. You can select a different compile-time max level with Cargo features:

```sh
cargo test --no-default-features --features log-level-debug
cargo run --no-default-features --features log-level-trace
```

At runtime, `GRESSE_LOG` can further filter output within that compile-time ceiling:

```sh
GRESSE_LOG=debug cargo test --test basic_integration -- --nocapture
```

## Required Config

Gresse reads runtime configuration from environment variables.

| Variable | Meaning |
| --- | --- |
| `GRESSE_ADDR` | IP address this replica binds to. |
| `GRESSE_ADVERTISE_ADDR` | Optional routable IP published to peers. Defaults to `GRESSE_ADDR`; set it when binding a wildcard/local address behind NAT or a tunnel. |
| `GRESSE_HTTP_PORT` | HTTP port for client query and mutation requests. |
| `GRESSE_INTERNAL_PORT` | Internal replication port for peer-to-peer replica traffic. |
| `GRESSE_RESULT_DIR_PATH` | Local directory where replica metrics are written. |
| `GRESSE_PERSISTENT_REPLICA_PATH` | Object path for the serialized persistent CRDT state. |
| `GRESSE_MEMBERSHIP_DIRECTORY_PATH` | Object-storage directory prefix used for replica membership descriptors. |

Optional configuration:

| Variable | Default | Meaning |
| --- | --- | --- |
| `GRESSE_OBJECT_STORAGE_LOCAL_DIR` | unset | Shared local directory used as the storage backend. When set, Gresse uses the local filesystem instead of S3/MinIO. |
| `GRESSE_OBJECT_STORAGE_REGION` | `us-east-1` | Object storage region for S3-compatible backends. |
| `GRESSE_OBJECT_STORAGE_BUCKET` | `gresse` | Object storage bucket name for S3-compatible backends. |
| `GRESSE_OBJECT_STORAGE_URL` | unset | Custom object-storage endpoint URL. Set this for S3-compatible stores such as MinIO. Leave it unset for AWS S3. |
| `GRESSE_OBJECT_STORAGE_ACCESS_KEY` | unset | Optional explicit access key. |
| `GRESSE_OBJECT_STORAGE_SECRET_KEY` | unset | Optional explicit secret key. |
| `GRESSE_OBJECT_STORAGE_SESSION_TOKEN` | unset | Optional explicit session token for temporary credentials. |
| `GRESSE_SYNC_INTERVAL_MS` | `1000` | Interval between replica delta synchronization ticks. |
| `GRESSE_OBJECT_STORAGE_DISCOVERY_INTERVAL_MS` | `1000` | Object storage discovery interval. |
| `GRESSE_DURABLE` | `false` | Enables local durable replay logging when set to `true`/`1`. |
| `GRESSE_DURABILITY_PATH` | unset | Append-only local durability journal path. Required when `GRESSE_DURABLE` is enabled. |
| `GRESSE_GC_INTERVAL_MS` | runtime-specific | GC interval override for the benchmark replica binary. |
| `GRESSE_REPLICA_NETWORK_LATENCY_MS` | `0` | Fixed one-way latency added before each replica-to-replica send. |
| `GRESSE_REPLICA_NETWORK_LATENCY_JITTER_MS` | `0` | Uniform jitter applied around `GRESSE_REPLICA_NETWORK_LATENCY_MS`. |
| `GRESSE_BENCH_PID` | unset | Required replica identifier for the standalone OR-Set benchmark binary. |

Authentication options:
- Explicit Gresse credentials via `GRESSE_OBJECT_STORAGE_ACCESS_KEY`, `GRESSE_OBJECT_STORAGE_SECRET_KEY`, and optionally `GRESSE_OBJECT_STORAGE_SESSION_TOKEN`.
- Standard AWS environment variables such as `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN`.
- Shared AWS profiles via `AWS_PROFILE` together with `~/.aws/credentials` and `~/.aws/config`.
- AWS metadata-based providers already supported by `object_store`, such as EC2 instance roles, ECS task roles, and web-identity credentials.

Example for local MinIO:

```sh
export GRESSE_ADDR=0.0.0.0
export GRESSE_HTTP_PORT=9090
export GRESSE_INTERNAL_PORT=8080
export GRESSE_RESULT_DIR_PATH=./results

export GRESSE_OBJECT_STORAGE_URL=http://localhost:9000
export GRESSE_OBJECT_STORAGE_REGION=us-east-1
export GRESSE_OBJECT_STORAGE_BUCKET=gresse
export GRESSE_OBJECT_STORAGE_ACCESS_KEY=minioadmin
export GRESSE_OBJECT_STORAGE_SECRET_KEY=minioadmin
export GRESSE_PERSISTENT_REPLICA_PATH=replicas/app-state.json
export GRESSE_MEMBERSHIP_DIRECTORY_PATH=membership
```

Example for a shared local filesystem directory:

```sh
export GRESSE_ADDR=0.0.0.0
export GRESSE_HTTP_PORT=9090
export GRESSE_INTERNAL_PORT=8080
export GRESSE_RESULT_DIR_PATH=./results

export GRESSE_OBJECT_STORAGE_LOCAL_DIR=/tmp/gresse-shared-store
export GRESSE_PERSISTENT_REPLICA_PATH=replicas/app-state.json
export GRESSE_MEMBERSHIP_DIRECTORY_PATH=membership
```

Start multiple replicas with the same `GRESSE_OBJECT_STORAGE_LOCAL_DIR`, `GRESSE_PERSISTENT_REPLICA_PATH`, and `GRESSE_MEMBERSHIP_DIRECTORY_PATH` values and they will discover each other through that shared directory just as they would through MinIO.

Example for AWS S3 with a shared AWS profile:

```sh
export AWS_PROFILE=gresse-experiment

export GRESSE_ADDR=0.0.0.0
export GRESSE_HTTP_PORT=9090
export GRESSE_INTERNAL_PORT=8080
export GRESSE_RESULT_DIR_PATH=./results

export GRESSE_OBJECT_STORAGE_REGION=eu-north-1
export GRESSE_OBJECT_STORAGE_BUCKET=gresse
export GRESSE_PERSISTENT_REPLICA_PATH=experiment1/persistent.json
export GRESSE_MEMBERSHIP_DIRECTORY_PATH=experiment1/membership
```

With that setup, Gresse will read credentials from your normal AWS profile files instead of storing secrets in the repository.

## Distributed OR-Set Benchmark

The repository now includes:
- `src/bin/orset_bench_replica.rs`: a standalone `ORSet<i32>` replica process for benchmarks.
- `scripts/orset_benchmark_lib.py`: reusable orchestration and analysis helpers for OR-Set benchmark experiments.
- `scripts/orset_distributed_benchmark.py`: a single-run process-based benchmark wrapper built on that library.
- `scripts/orset_throughput_vs_replicas.py`: an experiment driver that runs the throughput-vs-replica-count benchmark for increasing replica counts.

Server metrics are written as structured CSV rows and include:
- replica bootstrap timestamps
- membership descriptor and membership directory timings
- persistent-state fetch and write timings
- GC initiation, validation, persistence, cleanup, and GC observation timestamps
- peer replication pull/get-delta/merge timings

GC-related rows include the integer `gc_marker` so separate replicas can be correlated.

Example:

```sh
export AWS_PROFILE='gresse'
export AWS_REGION='eu-north-1'
export AWS_DEFAULT_REGION='eu-north-1'

python3 scripts/orset_distributed_benchmark.py \
  --replicas 3 \
  --duration-seconds 60 \
  --result-dir ./results/experiment1 \
  --max-store-size-mb 64 \
  --bucket gresse \
  --region eu-north-1 \
  --persistent-path experiment1/persistent.json \
  --membership-path experiment1/membership \
  --network-latency-ms 25 \
  --network-latency-jitter-ms 5 \
  --lifecycle-event start:1:0 \
  --lifecycle-event start:2:0 \
  --lifecycle-event start:3:15 \
  --lifecycle-event stop:2:40
```

The single-run wrapper now supports multiple clients per replica and a stable-window throughput analysis:

```sh
python3 scripts/orset_distributed_benchmark.py \
  --replicas 4 \
  --duration-seconds 90 \
  --result-dir ./results/single-run \
  --max-store-size-mb 64 \
  --clients-per-replica 8 \
  --ops-per-second-per-client 0 \
  --warmup-seconds 20 \
  --cooldown-seconds 10
```

`--ops-per-second-per-client 0` means unthrottled clients, which is the intended mode when you want to saturate each replica.

If you want to target a custom S3-compatible object store such as MinIO, pass:
- `--object-storage-url`
- `--object-storage-access-key`
- `--object-storage-secret-key`
- optionally `--object-storage-session-token`

The throughput-vs-replicas experiment uses:
- GC interval fixed at `60000ms`
- peer-to-peer replication tick fixed at `1000ms`
- replica startup stagger fixed at `1s` by default
- replica counts increasing from `1` to `8` by default
- stable throughput computed only over the configured warmup/cooldown-trimmed window

## Six-region crash and churn experiment

`scripts/geo_churn_experiment.py` is the laptop-side coordinator for the ACM
SEC geo-distributed evaluation. It stages one `geo_churn_remote_agent.py` on
each VM, assigns that VM its filtered lifecycle schedule before the run is
armed, and gives all agents one absolute UTC start time. Each agent owns its
five local replica processes, local workload, bootstrap checks, and shutdown;
the laptop only preflights, periodically reads agent status, and collects
artifacts.

`workload_rate_per_server` is the aggregate request rate for each VM. The
default topology sets it to 10 operations/sec, distributed round-robin across
that VM's live replicas. If the laptop running `run.sh` is interrupted after
the remote agents have been armed, run `./scripts/geo-experiment/run.sh
--resume` later to reopen the saved run, monitor its agents, collect artifacts,
and regenerate the summary. Resume deliberately skips build, preflight, and
all remote lifecycle actions. Use `--resume --result-dir PATH` when the local
active-run marker is unavailable.

Copy and edit `scripts/geo_churn_topology.example.json`: each VM must have a
routable `advertise_ip` for replica-to-replica traffic, an SSH target, and an
already installed release `orset_bench_replica` binary. The VM's AWS identity
must have access to the configured shared bucket. `client_host` is retained
for legacy laptop-controller mode; remote agents send their workload to their
own loopback HTTP ports.
Use an SSH host alias in `ssh_host` (configured with its user, key, and any
`ProxyJump` in `~/.ssh/config`); the controller runs SSH/SCP in noninteractive
`BatchMode` and never accepts passwords. Prefer an IAM instance role for S3
access on each VM, rather than exporting long-lived credentials.
Set `expected_binary_sha256` to the SHA-256 of the release binary copied to
the VMs; preflight rejects a mixed or unexpected binary. Preflight also checks
the midpoint-estimated VM clock offset against `max_clock_skew_ms`. The
experiment uses a per-VM-slot durable journal by default, retaining local
state across a hard-crashed process and its replacement. An agent verifies
bootstrap from its local generation-specific metrics file and local child PID,
not through SSH.

```sh
python3 scripts/geo_churn_experiment.py \
  --topology scripts/geo_churn_topology.json \
  --result-dir ./results/acm-sec-geo-churn

python3 scripts/analyze_geo_churn.py ./results/acm-sec-geo-churn
```

Run `--dry-run` first to validate the topology and lifecycle schedule without
connecting to hosts. The result directory contains the controller event trace,
state-digest snapshots, topology/manifest, and a `remote_artifacts/` directory
with metrics and logs collected from all five VMs. `crash` sends `SIGKILL`;
`graceful_stop` sends `SIGTERM`, which intentionally exercises GRESSE's normal
shutdown path. A subsequent `spawn` always receives a fresh GRESSE PID.
The analyzer writes `summary.json` with availability split by expected process
state, first-success recovery time, lifecycle counts, and state-digest
convergence; correlate collected replica CSVs by `gc_marker` for GC progress.

Every invocation creates a timestamp-and-random-suffixed run ID from
`run_label`; this isolates the S3 prefix and remote log directory from all
previous attempts. Result directories are refused when they already exist.
At preflight the controller terminates processes recorded by any previous
controller run on the five VMs, without deleting prior artifacts. Every started
agent has its own deadline-based lifecycle loop, so a laptop interruption does
not stop a running experiment. SSH/SCP operations have bounded timeouts and
artifact collection continues when one VM is unavailable. Set
`execution_mode` to `"laptop"` only to use the legacy laptop lifecycle
controller. Recover artifacts after a laptop failure with a fresh local result
directory:

```sh
python3 scripts/geo_churn_experiment.py \
  --topology scripts/geo_churn_topology.json \
  --result-dir ./results/acm-sec-geo-churn-recovered \
  --collect-run-id '<run-id-from-the-original-manifest>'
```

Example:

```sh
python3 scripts/orset_throughput_vs_replicas.py \
  --result-root ./results/throughput-vs-replicas \
  --max-store-size-mb 64 \
  --clients-per-replica 8 \
  --duration-seconds 90 \
  --warmup-seconds 20 \
  --cooldown-seconds 10
```

This writes:
- one run directory per replica count
- `throughput_vs_replicas_summary.csv`
- `throughput_vs_replicas.svg`

If you want to run the same experiment on a Kubernetes-backed server using the bundled MinIO latency proxy, use:

```sh
bash scripts/run_minio_latency_benchmark_server.sh
```

That script will:
- deploy `integrations/minio-latency/minio-latency.yaml` unless `DEPLOY_MINIO=0`
- start a local `kubectl port-forward` to the MinIO proxy
- run `scripts/orset_throughput_vs_replicas.py` against the proxied MinIO endpoint

Common overrides are passed as environment variables:

```sh
RESULT_ROOT=./results/server-run \
CLIENTS_PER_REPLICA=12 \
MAX_STORE_SIZE_MB=128 \
MIN_REPLICAS=1 \
MAX_REPLICAS=8 \
DURATION_SECONDS=120 \
WARMUP_SECONDS=30 \
COOLDOWN_SECONDS=15 \
bash scripts/run_minio_latency_benchmark_server.sh
```

The summary reports both:
- stable combined throughput for the full replica set
- stable average per-replica throughput

The runner derives the random `i32` domain from `max_store_size_mb` using:

```text
floor(max_store_size_mb * 1024 * 1024 / 4)
```

This is an intentionally simple raw-value approximation for workload generation, not an exact in-memory OR-Set capacity bound.

## Local Durability

Replicas are non-durable by default. If you enable durability, Gresse writes an append-only local journal containing:
- snapshot checkpoints of the replica state
- delta groups for local mutations and remote merges, written before they are applied

On restart, the replica replays that journal before joining the cluster.

Example:

```sh
export GRESSE_DURABLE=true
export GRESSE_DURABILITY_PATH=./results/replica-9090.journal
```

## Local MinIO For Integration Tests

The repository includes [docker-compose.minio.yml](/Users/adam/Dev/Gresse/docker-compose.minio.yml) for a local MinIO deployment that automatically creates the `gresse-integration` bucket used by the integration test.

Start it with:

```sh
docker compose -f docker-compose.minio.yml up -d
```

Run the MinIO-backed integration test with:

```sh
cargo test --test basic_integration -- --nocapture
```

Optional overrides for non-default MinIO settings:

```sh
export GRESSE_TEST_MINIO_URL=http://127.0.0.1:9000
export GRESSE_TEST_MINIO_REGION=us-east-1
export GRESSE_TEST_MINIO_BUCKET=gresse-integration
export GRESSE_TEST_MINIO_ACCESS_KEY=minioadmin
export GRESSE_TEST_MINIO_SECRET_KEY=minioadmin
```

## Membership And Bootstrap

On startup, each replica writes an empty membership descriptor into `GRESSE_MEMBERSHIP_DIRECTORY_PATH`. The descriptor file name encodes:

```text
UUID,ip:port,epoch
```

After writing its descriptor, the replica lists the membership directory and begins connecting to discovered peers asynchronously.

The replica also reads `GRESSE_PERSISTENT_REPLICA_PATH` from object storage. If a persistent CRDT exists and its epoch is at least as new as the maximum membership epoch, the replica installs that state before running. If no persistent CRDT exists, the replica writes its current initial state to that path and continues.

## Client Requests

The HTTP server accepts JSON-serialized `CRDTClientRequest<T>` values:

```rust
use gresse::crdt::CRDTClientRequest;

CRDTClientRequest::<MyCrdt>::Query(my_query);
CRDTClientRequest::<MyCrdt>::Mutation(my_mutation);
```

Queries are served directly from local state. Mutations are applied by the replica and then propagated to peers through delta synchronization.

## Examples

The examples directory contains two examples: 
1. vector_db: A Vector DB implemented as a CRDT, which is a work-in-progress thing you are recommended to ignore for now.
2. bench_function: A simple, static CRDT used for benchmarking.

The `dots.rs` file contains some additional structures we provide for implementing *causal CRDTs*.
