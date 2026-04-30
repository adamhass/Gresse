from __future__ import annotations

import csv
import http.client
import json
import os
import random
import shutil
import signal
import socket
import subprocess
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional


METRIC_HEADERS = [
    "source",
    "event",
    "phase",
    "timestamp_us",
    "replica_pid",
    "peer_pid",
    "gc_marker",
    "sent_us",
    "received_us",
    "start_us",
    "end_us",
    "insert_count",
    "delete_count",
    "detail",
    "client_id",
    "operation",
    "value",
    "status_code",
    "latency_us",
]


@dataclass(frozen=True)
class LifecycleEvent:
    at_seconds: float
    action: str
    replica_id: int


@dataclass(frozen=True)
class ReplicaSpec:
    replica_id: int
    pid: int
    http_port: int
    internal_port: int


@dataclass(frozen=True)
class BenchmarkConfig:
    replicas: int
    duration_seconds: float
    result_dir: Path
    max_store_size_mb: float
    host: str = "127.0.0.1"
    base_http_port: int = 18080
    base_internal_port: int = 19080
    clients_per_replica: int = 1
    ops_per_second_per_client: float = 0.0
    remove_probability: float = 0.5
    sync_interval_ms: int = 1000
    discovery_interval_ms: int = 1000
    gc_interval_ms: int = 60000
    network_latency_ms: int = 0
    network_latency_jitter_ms: int = 0
    startup_stagger_seconds: float = 0.0
    warmup_seconds: float = 10.0
    cooldown_seconds: float = 5.0
    aws_profile: str = "gresse"
    region: str = "eu-north-1"
    bucket: str = "gresse"
    object_storage_url: Optional[str] = None
    object_storage_access_key: Optional[str] = None
    object_storage_secret_key: Optional[str] = None
    object_storage_session_token: Optional[str] = None
    persistent_path: str = "experiment1/persistent.json"
    membership_path: str = "experiment1/membership"
    cargo_profile: str = "release"
    lifecycle_events: tuple[LifecycleEvent, ...] = ()


@dataclass(frozen=True)
class ThroughputSummary:
    replica_pid: int
    successful_requests: int
    stable_successful_requests: int
    stable_throughput_ops_per_sec: float


@dataclass(frozen=True)
class BenchmarkRunResult:
    result_dir: Path
    manifest_path: Path
    combined_metrics_path: Path
    throughput_summary_path: Path
    per_second_throughput_path: Path
    stable_window_start_us: int
    stable_window_end_us: int
    stable_window_seconds: float
    per_replica: List[ThroughputSummary]
    combined_stable_throughput_ops_per_sec: float


class ReplicaClientWorker(threading.Thread):
    def __init__(
        self,
        client_id: str,
        replica: ReplicaSpec,
        host: str,
        domain_limit: int,
        ops_per_second: float,
        remove_probability: float,
        csv_path: Path,
    ) -> None:
        super().__init__(daemon=True)
        self.client_id = client_id
        self.replica = replica
        self.host = host
        self.domain_limit = domain_limit
        self.ops_per_second = ops_per_second
        self.remove_probability = remove_probability
        self.csv_path = csv_path
        self.stop_event = threading.Event()
        self.random = random.Random(hash((client_id, replica.replica_id, replica.pid)) & 0xFFFFFFFF)

    def stop(self) -> None:
        self.stop_event.set()

    def run(self) -> None:
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        period = 0.0 if self.ops_per_second <= 0 else 1.0 / self.ops_per_second

        with self.csv_path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=METRIC_HEADERS)
            writer.writeheader()

            while not self.stop_event.is_set():
                operation = "Remove" if self.random.random() < self.remove_probability else "Insert"
                value = self.random.randint(-self.domain_limit, self.domain_limit)
                payload = {"type": "Mutation", "params": {operation: value}}
                sent_us = now_micros()
                status_code = None
                detail = None
                conn: Optional[http.client.HTTPConnection] = None

                try:
                    conn = http.client.HTTPConnection(self.host, self.replica.http_port, timeout=5)
                    conn.request(
                        "POST",
                        "/",
                        body=json.dumps(payload),
                        headers={"Content-Type": "application/json"},
                    )
                    response = conn.getresponse()
                    response.read()
                    status_code = response.status
                except Exception as exc:  # pragma: no cover
                    detail = f"{type(exc).__name__}: {exc}"
                finally:
                    try:
                        if conn is not None:
                            conn.close()
                    except Exception:
                        pass

                received_us = now_micros()
                writer.writerow(
                    {
                        "source": "client",
                        "event": "client_request",
                        "phase": "completed",
                        "timestamp_us": received_us,
                        "replica_pid": self.replica.pid,
                        "peer_pid": "",
                        "gc_marker": "",
                        "sent_us": sent_us,
                        "received_us": received_us,
                        "start_us": sent_us,
                        "end_us": received_us,
                        "insert_count": "",
                        "delete_count": "",
                        "detail": detail or "",
                        "client_id": self.client_id,
                        "operation": operation,
                        "value": value,
                        "status_code": status_code or "",
                        "latency_us": max(0, received_us - sent_us),
                    }
                )
                handle.flush()

                if period > 0:
                    self.stop_event.wait(period)


class ManagedReplica:
    def __init__(
        self,
        replica: ReplicaSpec,
        config: BenchmarkConfig,
        binary_path: Path,
        result_dir: Path,
        domain_limit: int,
    ) -> None:
        self.replica = replica
        self.config = config
        self.binary_path = binary_path
        self.result_dir = result_dir
        self.domain_limit = domain_limit
        self.process: Optional[subprocess.Popen[str]] = None
        self.client_workers: List[ReplicaClientWorker] = []
        self.stdout_handle = None
        self.stderr_handle = None

    def start(self) -> None:
        if self.process is not None and self.process.poll() is None:
            return

        env = os.environ.copy()
        env.update(
            {
                "AWS_PROFILE": self.config.aws_profile,
                "AWS_REGION": self.config.region,
                "AWS_DEFAULT_REGION": self.config.region,
                "GRESSE_BENCH_PID": str(self.replica.pid),
                "GRESSE_ADDR": self.config.host,
                "GRESSE_HTTP_PORT": str(self.replica.http_port),
                "GRESSE_INTERNAL_PORT": str(self.replica.internal_port),
                "GRESSE_RESULT_DIR_PATH": str(self.result_dir),
                "GRESSE_SYNC_INTERVAL_MS": str(self.config.sync_interval_ms),
                "GRESSE_OBJECT_STORAGE_DISCOVERY_INTERVAL_MS": str(
                    self.config.discovery_interval_ms
                ),
                "GRESSE_GC_INTERVAL_MS": str(self.config.gc_interval_ms),
                "GRESSE_REPLICA_NETWORK_LATENCY_MS": str(self.config.network_latency_ms),
                "GRESSE_REPLICA_NETWORK_LATENCY_JITTER_MS": str(
                    self.config.network_latency_jitter_ms
                ),
                "GRESSE_OBJECT_STORAGE_REGION": self.config.region,
                "GRESSE_OBJECT_STORAGE_BUCKET": self.config.bucket,
                "GRESSE_PERSISTENT_REPLICA_PATH": self.config.persistent_path,
                "GRESSE_MEMBERSHIP_DIRECTORY_PATH": self.config.membership_path,
            }
        )
        if self.config.object_storage_url:
            env["GRESSE_OBJECT_STORAGE_URL"] = self.config.object_storage_url
        if self.config.object_storage_access_key:
            env["GRESSE_OBJECT_STORAGE_ACCESS_KEY"] = self.config.object_storage_access_key
        if self.config.object_storage_secret_key:
            env["GRESSE_OBJECT_STORAGE_SECRET_KEY"] = self.config.object_storage_secret_key
        if self.config.object_storage_session_token:
            env["GRESSE_OBJECT_STORAGE_SESSION_TOKEN"] = self.config.object_storage_session_token

        stdout_path = self.result_dir / f"replica_{self.replica.replica_id}.stdout.log"
        stderr_path = self.result_dir / f"replica_{self.replica.replica_id}.stderr.log"
        self.stdout_handle = stdout_path.open("w")
        self.stderr_handle = stderr_path.open("w")
        self.process = subprocess.Popen(
            [str(self.binary_path)],
            env=env,
            stdout=self.stdout_handle,
            stderr=self.stderr_handle,
            text=True,
        )

        wait_for_port(self.config.host, self.replica.http_port, timeout_seconds=20.0)
        self.client_workers = []
        for client_index in range(self.config.clients_per_replica):
            worker = ReplicaClientWorker(
                client_id=f"client-{self.replica.replica_id}-{client_index}",
                replica=self.replica,
                host=self.config.host,
                domain_limit=self.domain_limit,
                ops_per_second=self.config.ops_per_second_per_client,
                remove_probability=self.config.remove_probability,
                csv_path=self.result_dir / f"client_{self.replica.replica_id}_{client_index}.csv",
            )
            worker.start()
            self.client_workers.append(worker)

    def stop(self) -> None:
        for worker in self.client_workers:
            worker.stop()
        for worker in self.client_workers:
            worker.join(timeout=10)
        self.client_workers = []

        if self.process is not None and self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        self.process = None

        if self.stdout_handle is not None:
            self.stdout_handle.close()
            self.stdout_handle = None
        if self.stderr_handle is not None:
            self.stderr_handle.close()
            self.stderr_handle = None


class BenchmarkRun:
    def __init__(self, config: BenchmarkConfig) -> None:
        self.config = config
        self.result_dir = config.result_dir.resolve()
        self.binary_path = build_binary(config.cargo_profile)
        self.domain_limit = max(1, int((config.max_store_size_mb * 1024 * 1024) / 4))
        self.replicas = [
            ReplicaSpec(
                replica_id=replica_id,
                pid=replica_id,
                http_port=config.base_http_port + replica_id,
                internal_port=config.base_internal_port + replica_id,
            )
            for replica_id in range(1, config.replicas + 1)
        ]
        self.managed = {
            replica.replica_id: ManagedReplica(
                replica, config, self.binary_path, self.result_dir, self.domain_limit
            )
            for replica in self.replicas
        }

    def run(self) -> BenchmarkRunResult:
        prepare_result_dir(self.result_dir)
        reset_s3_prefixes(self.config)

        has_explicit_start_events = any(
            event.action == "start" for event in self.config.lifecycle_events
        )
        started_at_zero = {
            event.replica_id
            for event in self.config.lifecycle_events
            if event.action == "start" and event.at_seconds == 0
        }

        run_started_us = now_micros()
        if not self.config.lifecycle_events or not has_explicit_start_events:
            self.start_replica_ids(sorted(self.managed))
        else:
            self.start_replica_ids(sorted(started_at_zero))

        start_time = time.monotonic()
        event_index = 0
        try:
            while True:
                elapsed = time.monotonic() - start_time
                if elapsed >= self.config.duration_seconds:
                    break

                due_events: List[LifecycleEvent] = []
                while (
                    event_index < len(self.config.lifecycle_events)
                    and self.config.lifecycle_events[event_index].at_seconds <= elapsed
                ):
                    due_events.append(self.config.lifecycle_events[event_index])
                    event_index += 1

                if due_events:
                    self.handle_due_events(due_events)

                time.sleep(0.1)
        finally:
            for replica_id in sorted(self.managed):
                self.managed[replica_id].stop()

        run_finished_us = now_micros()
        combined_metrics_path = merge_metrics(self.result_dir)
        stable_window_start_us = run_started_us + int(self.config.warmup_seconds * 1_000_000)
        stable_window_end_us = run_finished_us - int(self.config.cooldown_seconds * 1_000_000)
        if stable_window_end_us <= stable_window_start_us:
            stable_window_start_us = run_started_us
            stable_window_end_us = run_finished_us
        throughput_summary_path, per_second_path, per_replica, combined_stable = analyze_throughput(
            combined_metrics_path=combined_metrics_path,
            result_dir=self.result_dir,
            stable_window_start_us=stable_window_start_us,
            stable_window_end_us=stable_window_end_us,
            replica_pids=[replica.pid for replica in self.replicas],
        )
        manifest_path = write_manifest(
            result_dir=self.result_dir,
            config=self.config,
            replicas=self.replicas,
            domain_limit=self.domain_limit,
            run_started_us=run_started_us,
            run_finished_us=run_finished_us,
            stable_window_start_us=stable_window_start_us,
            stable_window_end_us=stable_window_end_us,
        )
        return BenchmarkRunResult(
            result_dir=self.result_dir,
            manifest_path=manifest_path,
            combined_metrics_path=combined_metrics_path,
            throughput_summary_path=throughput_summary_path,
            per_second_throughput_path=per_second_path,
            stable_window_start_us=stable_window_start_us,
            stable_window_end_us=stable_window_end_us,
            stable_window_seconds=max(
                0.0, (stable_window_end_us - stable_window_start_us) / 1_000_000.0
            ),
            per_replica=per_replica,
            combined_stable_throughput_ops_per_sec=combined_stable,
        )

    def start_replica_ids(self, replica_ids: List[int]) -> None:
        for index, replica_id in enumerate(replica_ids):
            self.managed[replica_id].start()
            if (
                self.config.startup_stagger_seconds > 0
                and index + 1 < len(replica_ids)
            ):
                time.sleep(self.config.startup_stagger_seconds)

    def handle_due_events(self, due_events: List[LifecycleEvent]) -> None:
        stop_ids = sorted(
            event.replica_id for event in due_events if event.action == "stop"
        )
        start_ids = sorted(
            event.replica_id for event in due_events if event.action == "start"
        )

        for replica_id in stop_ids:
            self.managed[replica_id].stop()
        if start_ids:
            self.start_replica_ids(start_ids)


def build_binary(cargo_profile: str) -> Path:
    command = ["cargo", "build", "--bin", "orset_bench_replica"]
    if cargo_profile == "release":
        command.append("--release")
    subprocess.run(command, check=True)
    return (Path("target") / cargo_profile / "orset_bench_replica").resolve()


def prepare_result_dir(result_dir: Path) -> None:
    if result_dir.exists():
        shutil.rmtree(result_dir)
    result_dir.mkdir(parents=True, exist_ok=True)


def reset_s3_prefixes(config: BenchmarkConfig) -> None:
    persistent_uri = f"s3://{config.bucket}/{config.persistent_path}"
    membership_uri = f"s3://{config.bucket}/{config.membership_path}"
    aws_base_command = ["aws"]
    if config.object_storage_url:
        aws_base_command.extend(["--endpoint-url", config.object_storage_url])

    env = os.environ.copy()
    if config.object_storage_access_key:
        env["AWS_ACCESS_KEY_ID"] = config.object_storage_access_key
    if config.object_storage_secret_key:
        env["AWS_SECRET_ACCESS_KEY"] = config.object_storage_secret_key
    if config.object_storage_session_token:
        env["AWS_SESSION_TOKEN"] = config.object_storage_session_token

    persistent_command = aws_base_command + ["s3", "rm", persistent_uri, "--region", config.region]
    membership_command = aws_base_command + [
        "s3",
        "rm",
        membership_uri,
        "--recursive",
        "--region",
        config.region,
    ]
    if not config.object_storage_url:
        persistent_command.extend(["--profile", config.aws_profile])
        membership_command.extend(["--profile", config.aws_profile])

    subprocess.run(
        persistent_command,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
    )
    subprocess.run(
        membership_command,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
    )


def parse_lifecycle_events(raw_events: List[str], replica_count: int) -> tuple[LifecycleEvent, ...]:
    events: List[LifecycleEvent] = []
    for raw_event in raw_events:
        parts = raw_event.split(":")
        if len(parts) != 3:
            raise ValueError(f"Invalid lifecycle event {raw_event!r}")
        action, replica_id_text, seconds_text = parts
        if action not in {"start", "stop"}:
            raise ValueError(f"Invalid lifecycle action {action!r}")
        replica_id = int(replica_id_text)
        if replica_id < 1 or replica_id > replica_count:
            raise ValueError(f"Replica id {replica_id} is out of range 1..{replica_count}")
        events.append(
            LifecycleEvent(
                at_seconds=float(seconds_text),
                action=action,
                replica_id=replica_id,
            )
        )
    return tuple(sorted(events, key=lambda event: (event.at_seconds, event.replica_id, event.action)))


def wait_for_port(host: str, port: int, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Optional[Exception] = None
    while time.monotonic() < deadline:
        sock = socket.socket()
        try:
            sock.settimeout(1)
            sock.connect((host, port))
            return
        except OSError as exc:
            last_error = exc
            time.sleep(0.2)
        finally:
            sock.close()
    raise TimeoutError(f"Timed out waiting for {host}:{port}: {last_error}")


def merge_metrics(result_dir: Path) -> Path:
    rows: List[Dict[str, str]] = []
    for csv_path in sorted(result_dir.glob("*.csv")):
        if csv_path.name in {"combined_metrics.csv", "throughput_summary.csv", "per_second_throughput.csv"}:
            continue
        with csv_path.open() as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                rows.append({header: row.get(header, "") for header in METRIC_HEADERS})

    rows.sort(key=lambda row: int(row["timestamp_us"]) if row["timestamp_us"] else 0)
    combined_metrics_path = result_dir / "combined_metrics.csv"
    with combined_metrics_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=METRIC_HEADERS)
        writer.writeheader()
        writer.writerows(rows)
    return combined_metrics_path


def analyze_throughput(
    combined_metrics_path: Path,
    result_dir: Path,
    stable_window_start_us: int,
    stable_window_end_us: int,
    replica_pids: List[int],
) -> tuple[Path, Path, List[ThroughputSummary], float]:
    successful_rows: List[Dict[str, str]] = []
    with combined_metrics_path.open() as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("source") != "client" or row.get("event") != "client_request":
                continue
            if str(row.get("status_code", "")).strip() != "200":
                continue
            successful_rows.append(row)

    stable_window_seconds = max(
        1e-9, (stable_window_end_us - stable_window_start_us) / 1_000_000.0
    )
    per_replica_success = {pid: 0 for pid in replica_pids}
    per_replica_stable = {pid: 0 for pid in replica_pids}
    per_second_counts: Dict[int, Dict[int, int]] = {}

    for row in successful_rows:
        timestamp_us = int(row["timestamp_us"])
        replica_pid = int(row["replica_pid"])
        if replica_pid in per_replica_success:
            per_replica_success[replica_pid] += 1
        second_bucket = timestamp_us // 1_000_000
        per_second_counts.setdefault(second_bucket, {})
        per_second_counts[second_bucket][replica_pid] = (
            per_second_counts[second_bucket].get(replica_pid, 0) + 1
        )
        if stable_window_start_us <= timestamp_us <= stable_window_end_us:
            if replica_pid in per_replica_stable:
                per_replica_stable[replica_pid] += 1

    per_replica = [
        ThroughputSummary(
            replica_pid=pid,
            successful_requests=per_replica_success[pid],
            stable_successful_requests=per_replica_stable[pid],
            stable_throughput_ops_per_sec=per_replica_stable[pid] / stable_window_seconds,
        )
        for pid in replica_pids
    ]
    combined_stable = sum(summary.stable_successful_requests for summary in per_replica) / stable_window_seconds

    throughput_summary_path = result_dir / "throughput_summary.csv"
    with throughput_summary_path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "replica_pid",
                "successful_requests",
                "stable_successful_requests",
                "stable_throughput_ops_per_sec",
            ],
        )
        writer.writeheader()
        for summary in per_replica:
            writer.writerow(asdict(summary))

    per_second_path = result_dir / "per_second_throughput.csv"
    with per_second_path.open("w", newline="") as handle:
        fieldnames = ["second_bucket", "combined_successful_requests"] + [
            f"replica_{pid}_successful_requests" for pid in replica_pids
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for second_bucket in sorted(per_second_counts):
            row = {
                "second_bucket": second_bucket,
                "combined_successful_requests": sum(per_second_counts[second_bucket].values()),
            }
            for pid in replica_pids:
                row[f"replica_{pid}_successful_requests"] = per_second_counts[second_bucket].get(pid, 0)
            writer.writerow(row)

    return throughput_summary_path, per_second_path, per_replica, combined_stable


def write_manifest(
    result_dir: Path,
    config: BenchmarkConfig,
    replicas: List[ReplicaSpec],
    domain_limit: int,
    run_started_us: int,
    run_finished_us: int,
    stable_window_start_us: int,
    stable_window_end_us: int,
) -> Path:
    manifest = {
        "config": {
            **asdict(config),
            "result_dir": str(config.result_dir),
            "lifecycle_events": [asdict(event) for event in config.lifecycle_events],
        },
        "replicas": [asdict(replica) for replica in replicas],
        "domain_limit": domain_limit,
        "raw_i32_domain_formula": "floor(max_store_size_mb * 1024 * 1024 / 4)",
        "run_started_us": run_started_us,
        "run_finished_us": run_finished_us,
        "stable_window_start_us": stable_window_start_us,
        "stable_window_end_us": stable_window_end_us,
    }
    manifest_path = result_dir / "manifest.json"
    with manifest_path.open("w") as handle:
        json.dump(manifest, handle, indent=2)
    return manifest_path


def now_micros() -> int:
    return time.time_ns() // 1_000
