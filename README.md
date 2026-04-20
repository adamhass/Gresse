# Gresse

Geo-replicated Stateful Serverless for the Edge.

Gresse lets an application define its state as a custom CRDT, then run that CRDT as a replicated server. The application owns the data type and merge semantics. Gresse owns the replica runtime: HTTP client requests, peer-to-peer delta synchronization, membership discovery, and object-storage-backed bootstrap state.

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

## Required Config

Gresse reads runtime configuration from environment variables.

| Variable | Meaning |
| --- | --- |
| `GRESSE_ADDR` | IP address this replica binds to. |
| `GRESSE_HTTP_PORT` | HTTP port for client query and mutation requests. |
| `GRESSE_INTERNAL_PORT` | Internal replication port for peer-to-peer replica traffic. |
| `GRESSE_RESULT_DIR_PATH` | Local directory where replica metrics are written. |
| `GRESSE_OBJECT_STORAGE_URL` | Object storage endpoint URL. |
| `GRESSE_OBJECT_STORAGE_REGION` | Object storage region. |
| `GRESSE_OBJECT_STORAGE_BUCKET` | Object storage bucket name. |
| `GRESSE_OBJECT_STORAGE_ACCESS_KEY` | Object storage access key. |
| `GRESSE_OBJECT_STORAGE_SECRET_KEY` | Object storage secret key. |
| `GRESSE_PERSISTENT_REPLICA_PATH` | Object path for the serialized persistent CRDT state. |
| `GRESSE_MEMBERSHIP_DIRECTORY_PATH` | Object-storage directory prefix used for replica membership descriptors. |

Optional configuration:

| Variable | Default | Meaning |
| --- | --- | --- |
| `GRESSE_SYNC_INTERVAL_MS` | `1000` | Interval between replica delta synchronization ticks. |
| `GRESSE_OBJECT_STORAGE_DISCOVERY_INTERVAL_MS` | `1000` | Object storage discovery interval. |

Example:

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
