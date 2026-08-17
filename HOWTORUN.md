# Running the six-region GRESSE crash and churn experiment

This runbook describes the distributed experiment used to evaluate GRESSE
under geographically distributed replica crashes, graceful departures,
replacement, bounded churn, and GC progress.

The experiment has one controller laptop, six cloud VMs in distinct regions,
and five GRESSE replica processes on every VM (30 initial replicas total).
The controller starts and stops replica processes over SSH; client requests
come directly from the laptop to replica HTTP endpoints. Replica state and
membership use one shared S3 bucket.

## Quick workflow

After provisioning the six VMs and preparing
`scripts/geo_churn_topology.json`, the normal workflow is four commands:

```sh
./scripts/geo-experiment/build.sh
./scripts/geo-experiment/aws_setup.sh
./scripts/geo-experiment/preflight.sh
./scripts/geo-experiment/run.sh
```

`build.sh` verifies local tooling and builds the release artifact; it never
copies anything to cloud VMs. `aws_setup.sh` checks the configured AWS identity
and bucket (add `--create-bucket` only for an intended new bucket).
`preflight.sh` copies the built binary to all six VMs, clears tracked stale
processes, and performs the remote checks. `run.sh` builds again, runs the
experiment, collects artifacts, and writes the summary.

Common options:

```sh
./scripts/geo-experiment/preflight.sh --topology scripts/geo_churn_topology.json
./scripts/geo-experiment/run.sh --result-dir ./results/acm-sec-run-01
```

On a fresh checkout, the first `aws_setup.sh` invocation creates
`scripts/geo_churn_topology.json` from the example and exits. Edit the created
file before running it again.

To create the six inexpensive public-IPv4 EC2 VMs and generate the topology
automatically, use an existing EC2 key-pair name and the controller laptop's
public CIDR:

```sh
./scripts/geo-experiment/aws_setup.sh --provision --create-bucket \
  --bucket '<globally-unique-bucket-name>' \
  --key-name '<existing-ec2-key-pair>' \
  --controller-cidr '<your-public-ip>/32'
```

For the one-shot default setup, simply run:

```sh
AWS_PROFILE=gresse-experiment ./scripts/geo-experiment/aws_setup.sh
```

It generates a unique S3 bucket and regional EC2 key pair, saves the private
key under `~/.ssh/`, discovers the laptop's public IP, creates missing default
VPCs, and writes the topology with the generated key configured. It refuses to
overwrite an existing topology.

The provisioning identity needs `ec2:CreateKeyPair` and `ec2:ImportKeyPair`
in addition to the EC2/IAM/S3 permissions listed for the provisioner; the
automatic path creates the private key in the first region and imports its
public half into the other five regions.

This mode requires default VPCs and default public subnets in all six regions.
It creates one `t3.small` Amazon Linux 2023 instance per region, an S3 instance
profile, and security groups limited to the laptop for SSH/client traffic and
the six VMs for replication. It does not create NAT Gateways, Transit Gateway,
VPN infrastructure, or Elastic IPs.
It refuses to overwrite an existing topology; use `--force-topology` only when
regenerating it intentionally (for example, after recreating instances).

## Teardown

Terminate the provisioned VMs and delete the experiment bucket and all its
contents:

```sh
AWS_PROFILE=gresse-experiment \
  ./scripts/geo-experiment/aws_setup.sh --destroy --project gresse-geo-churn \
  --bucket '<experiment-bucket>'
```

Teardown requires `ec2:TerminateInstances`, `s3:DeleteBucket`, and, for the
forced bucket removal, permission to delete its contained objects. It leaves
security groups, IAM configuration, key pairs, and default VPCs untouched.

## 1. Prerequisites

On the controller laptop, install:

- Rust and Cargo;
- Python 3.10 or later;
- OpenSSH client tools (`ssh`, `scp`);
- an SSH agent or `~/.ssh/config` entries for all six VMs.

On every VM, install:

- a Linux environment with `setsid`, `nohup`, `sha256sum`, `grep`, and `date`;
- the same release build of `orset_bench_replica` at the configured path;
- a synchronized clock (chrony or the cloud provider's NTP service);
- an AWS instance role with access to the experiment S3 bucket.

Use one VM in each of six cloud regions. Record the provider, regions/AZs,
machine type, operating system, and image version in your experiment notes.

## 2. Network configuration

Each VM runs five processes. By default their ports are:

| Traffic | Ports per VM | Who may connect |
| --- | --- | --- |
| Client HTTP | 18080–18084 | Controller laptop only |
| Replica replication | 19080–19084 | The other five experiment VMs only |
| SSH | 22 | Controller laptop only |

Use a private routed VPC, VPN, or overlay network for replica replication.
Set `advertise_ip` to the address reachable by the other VMs, and set
`bind_ip` to the local address on which the process listens (usually
`0.0.0.0`). Never expose replication ports to the public Internet.

`client_host` must be directly reachable from the controller laptop. It may be
a public IP/DNS name protected by firewall rules, or a VPN/overlay address. The
controller does not create SSH tunnels itself.

Before running the full experiment, verify from the controller laptop that it
can reach each intended client endpoint and that every VM can route to every
other VM's advertised replication address.

## 3. S3 access

Create or select one bucket for the experiment. All replicas use the same
bucket, but every invocation receives a unique object prefix, so runs do not
share state or membership descriptors.

Attach a least-privilege IAM instance role to every VM. It must permit the
following operations for the experiment bucket and its experiment prefixes:

- `s3:ListBucket`;
- `s3:GetObject`;
- `s3:PutObject`;
- `s3:DeleteObject`.

An instance role is preferred: the remote process then obtains credentials
without putting AWS keys in the topology file, local environment, or remote
shell profile. If an instance role is not possible, arrange an AWS credential
provider on every VM that is available to non-interactive SSH commands.

## 4. SSH authentication

The controller uses noninteractive SSH (`BatchMode=yes`), so it never accepts
password prompts. Configure a distinct alias for each VM in
`~/.ssh/config`, for example:

```sshconfig
Host gresse-stockholm
    HostName 203.0.113.10
    User ubuntu
    IdentityFile ~/.ssh/gresse-experiment
    IdentitiesOnly yes
    StrictHostKeyChecking yes

Host gresse-tokyo
    HostName 203.0.113.20
    User ubuntu
    IdentityFile ~/.ssh/gresse-experiment
    IdentitiesOnly yes
    StrictHostKeyChecking yes
    # Add ProxyJump here if the VM is reachable only through a bastion.
```

Load the private key into the agent if necessary:

```sh
ssh-add ~/.ssh/gresse-experiment
ssh gresse-stockholm 'hostname && date -u'
```

Set the topology's `ssh_host` values to these aliases. Extra SSH arguments,
such as a project-specific config file, can be placed in the top-level
`ssh_options` list.

## 5. Build and deploy one identical binary

Build the benchmark replica from the exact paper revision:

```sh
git rev-parse HEAD
cargo build --release --bin orset_bench_replica
shasum -a 256 target/release/orset_bench_replica
```

Copy that binary to the same path on all VMs. For example:

```sh
for host in gresse-stockholm gresse-frankfurt gresse-virginia \
            gresse-oregon gresse-singapore gresse-tokyo; do
  ssh "$host" 'sudo mkdir -p /opt/gresse && sudo chown "$USER" /opt/gresse'
  scp target/release/orset_bench_replica "$host":/opt/gresse/orset_bench_replica
  ssh "$host" 'chmod 0755 /opt/gresse/orset_bench_replica && sha256sum /opt/gresse/orset_bench_replica'
done
```

Use the printed SHA-256 in `expected_binary_sha256`. Controller preflight
rejects a run unless every VM reports that same hash.

## 6. Create the topology

Copy the template and replace every placeholder:

```sh
cp scripts/geo_churn_topology.example.json scripts/geo_churn_topology.json
```

Required settings include:

- `run_label`: a readable experiment label. The controller appends a timestamp
  and random suffix, preventing S3 and remote-artifact collisions.
- `bucket` and `s3_region`: shared S3 location.
- `expected_binary_sha256`: the release binary hash from the previous step.
- six `vms` entries, each with a unique `name`, `region`, `ssh_host`,
  `advertise_ip`, `client_host`, and `binary_path`.
- `events`: the scheduled `crash`, `graceful_stop`, and `spawn` actions.

Recommended controls:

```json
{
  "replicas_per_vm": 5,
  "sync_interval_ms": 1000,
  "discovery_interval_ms": 5000,
  "gc_interval_ms": 60000,
  "workload_rate_per_replica": 0.02,
  "membership_settle_seconds": 30,
  "startup_timeout_seconds": 60,
  "process_stop_timeout_seconds": 20,
  "max_clock_skew_ms": 5000,
  "durable_recovery": true,
  "remote_shutdown_grace_seconds": 20,
  "remote_deadline_slack_seconds": 600,
  "ssh_timeout_seconds": 30,
  "collection_timeout_seconds": 300
}
```

The full schedule uses four churn waves. At 300, 720, 1140, and 1560 seconds,
each wave has ten concurrent operations: four starts, three graceful stops,
and three hard crashes. Each is preceded by four concurrent crashes and
followed by six concurrent recoveries. The final recovery occurs at 1680
seconds, leaving a two-minute quiet tail for convergence and GC.

## 7. Dry-run and preflight

Dry-run validates the topology and lifecycle schedule without connecting to
VMs or issuing HTTP requests:

```sh
python3 scripts/geo_churn_experiment.py \
  --topology scripts/geo_churn_topology.json \
  --result-dir ./results/geo-churn-dry-run \
  --dry-run
```

Use a new result directory for every invocation. The controller intentionally
refuses to overwrite an existing directory.

For the first non-dry run, preflight will:

1. terminate replica processes and watchdogs registered by earlier controller
   runs on each VM, while retaining their old artifact directories;
2. verify the configured binary exists and has the expected identical SHA-256;
3. estimate each VM's clock offset from controller time and reject offsets over
   `max_clock_skew_ms` (5 seconds by default). This uses a fresh SSH connection
   and is therefore a coarse sanity bound, not a precision clock measurement;
   the estimate and its round-trip time are retained in `controller_events.csv`;
4. launch and wait for each replica's HTTP readiness and completed bootstrap;
5. wait for the configured membership stabilization interval before generating
   client mutations.

Before the first use of this controller version, manually inspect each VM for
older unregistered GRESSE processes and stop them. Later runs clean up all
processes recorded in the controller registry automatically.

## 8. Run the experiment

Start the experiment from the controller laptop:

```sh
python3 scripts/geo_churn_experiment.py \
  --topology scripts/geo_churn_topology.json \
  --result-dir ./results/acm-sec-geo-churn-run-01
```

The controller creates a manifest immediately. Copy its `run_id` somewhere
safe; it is needed for recovery collection if the laptop dies.

During the run it sends low-rate asynchronous OR-Set mutations directly from
the laptop to all known replica HTTP endpoints. Requests to deliberately
crashed endpoints are retained as expected availability failures; requests to
unaffected endpoints continue concurrently. State queries run periodically to
record canonical OR-Set digests.

`crash` sends `SIGKILL`. `graceful_stop` sends `SIGTERM`, waits for process
exit, and records a GRESSE shutdown descriptor. Every `spawn` receives a fresh
GRESSE PID and reuses the slot's durable journal.

Every process also has a detached remote watchdog. At the global deadline it
sends `SIGTERM`, then `SIGKILL` after the configured grace period. This bounds
remote resource use even if the controller laptop stalls or loses power.

## 9. Results and recovery

On completion, artifacts are fetched independently from all VMs into:

```text
results/acm-sec-geo-churn-run-01/
  manifest.json
  topology.json
  controller_events.csv
  remote_artifacts/<vm>/<run-id>/...
```

Each process directory contains `server_<pid>.csv`, `stdout.log`, `stderr.log`,
the OS/watchdog PID records, and watchdog output. Server metric records are
flushed after every write, including immediately before a crash.

If the controller disappears, wait until remote watchdogs have reached their
deadline, then collect artifacts to a fresh local directory:

```sh
python3 scripts/geo_churn_experiment.py \
  --topology scripts/geo_churn_topology.json \
  --result-dir ./results/acm-sec-geo-churn-run-01-recovered \
  --collect-run-id '<run-id-from-manifest>'
```

One unreachable VM is recorded as `collection_failed`; the controller
continues collecting the other VMs. Retry recovery collection later with the
same remote run ID and another fresh local result directory.

To produce an initial request-latency scatter plot, install the two Python
plotting dependencies once and run:

```sh
python3 -m pip install pandas matplotlib
python3 scripts/plot_geo_churn.py
```

The x-axis is normalized so that the earliest trace timestamp is zero. The
plot includes controller-side client observations and any replica metrics that
record both request receipt and completion; its legend distinguishes them.
With no arguments it selects the most recently modified experiment trace below
`./results/geo_churn/` and writes
`./results/plots/geo_churn/<run-id>/request_latency.png`. Pass a result
directory and/or `--output PATH` to select a specific run or output file.

## 10. Building the EC2 binary on macOS

The EC2 instances are 64-bit Amazon Linux hosts, so the experiment must deploy
an `x86_64-unknown-linux-gnu` executable, not the controller laptop's macOS
executable. `build.sh` checks this format before reporting success. On macOS,
install the cross-compilation prerequisites once:

```sh
brew install zig
cargo install cargo-zigbuild
rustup target add x86_64-unknown-linux-gnu
```

Then use the normal workflow. `preflight.sh` and `run.sh` automatically select
`target/x86_64-unknown-linux-gnu/release/orset_bench_replica` for deployment.

## 11. Short concurrent-churn smoke validation

After a replica-code change, deploy it with preflight and run this inexpensive
validation before the full experiment:

```sh
./scripts/geo-experiment/preflight.sh
./scripts/geo-experiment/run.sh --smoke
```

The smoke run uses the configured six-VM topology but has a five-minute
workload, one-minute convergence period, and a low rate of 0.02 requests per
second per replica. Requests are issued one at a time in rotating endpoint
order rather than as a synchronized 30-way burst. It deliberately performs two concurrent hard crashes at
45 seconds, two concurrent replacements at 90 seconds, two concurrent
graceful shutdowns at 135 seconds, and two concurrent replacements at 180
seconds. The normal zero-argument `run.sh` remains the full 30-minute schedule.

## 10. Analyze and retain evidence

Run the controller-side summary:

```sh
python3 scripts/analyze_geo_churn.py ./results/acm-sec-geo-churn-run-01
```

This writes `summary.json` with expected-live versus intentionally-down client
availability, lifecycle counts, first-success recovery times, and final state
digest convergence. Use the collected per-replica CSV files to construct the
GC timeline, correlated by `gc_marker`, including GC initiations, membership
change aborts, successful GC completion, persistent-state writes, and departed
membership cleanup.

Keep the topology, manifest, controller CSV, all remote artifacts, binary
SHA-256, source commit, VM metadata, IAM policy revision, and measured regional
RTT matrix with the paper's artifact package.
