#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import http.client
import json
import os
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

try:
    from orset_benchmark_lib import (
        BenchmarkConfig,
        build_binary,
        reset_membership_prefix,
        reset_s3_prefixes,
        wait_for_port,
    )
except ModuleNotFoundError:  # pragma: no cover
    from scripts.orset_benchmark_lib import (
        BenchmarkConfig,
        build_binary,
        reset_membership_prefix,
        reset_s3_prefixes,
        wait_for_port,
    )


DEFAULT_REGION = "us-east-1"
DEFAULT_BUCKET = "gresse"
DEFAULT_ACCESS_KEY = "minioadmin"
DEFAULT_SECRET_KEY = "minioadmin"


@dataclass(frozen=True)
class MinioEndpoint:
    url: str
    port_forward_pid: Optional[int]
    reused_port_forward: bool


@dataclass(frozen=True)
class ColdStartSample:
    process_start_us: int
    logging_initialized_us: int
    config_loaded_us: int
    replica_with_config_completed_us: int
    replica_task_spawned_us: int
    crdt_pid_set_us: int
    http_server_ready_us: int
    network_manager_ready_us: int
    metric_writer_ready_us: int
    object_storage_client_start_us: int
    object_storage_client_ready_us: int
    replica_run_start_us: int
    replica_init_start_us: int
    persistent_state_fetch_completed_us: int
    membership_descriptor_write_completed_us: int
    membership_directory_read_completed_us: int
    replica_init_completed_us: int


class ReplicaProcess:
    def __init__(
        self,
        binary_path: Path,
        env: dict[str, str],
        stdout_path: Path,
        stderr_path: Path,
    ) -> None:
        self.binary_path = binary_path
        self.env = env
        self.stdout_path = stdout_path
        self.stderr_path = stderr_path
        self.stdout_handle = None
        self.stderr_handle = None
        self.process: Optional[subprocess.Popen[str]] = None

    def start(self) -> None:
        self.stdout_path.parent.mkdir(parents=True, exist_ok=True)
        self.stdout_handle = self.stdout_path.open("w")
        self.stderr_handle = self.stderr_path.open("w")
        self.process = subprocess.Popen(
            [str(self.binary_path)],
            env=self.env,
            stdout=self.stdout_handle,
            stderr=self.stderr_handle,
            text=True,
        )

    def stop(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        if self.stdout_handle is not None:
            self.stdout_handle.close()
            self.stdout_handle = None
        if self.stderr_handle is not None:
            self.stderr_handle.close()
            self.stderr_handle = None


def main() -> int:
    args = parse_args()
    result_root = args.result_root.resolve()
    result_root.mkdir(parents=True, exist_ok=True)

    aws_profile_mode = use_aws_profile_mode(args)
    minio_endpoint = None
    object_storage_url = None
    if aws_profile_mode:
        object_storage_url = None
    else:
        minio_endpoint = ensure_local_minio(args, result_root)
        object_storage_url = minio_endpoint.url
    sample_binary = build_binary(args.cargo_profile, "orset_cold_start_sample")
    peer_binary = build_binary(args.cargo_profile, "orset_bench_replica")
    seed_storage_state(args, result_root, object_storage_url, sample_binary)

    samples_csv_path = result_root / "cold_start_samples.csv"
    samples_json_path = result_root / "cold_start_samples.json"

    samples: list[dict[str, object]] = []

    try:
        for sample_index in range(1, args.samples + 1):
            sample_dir = result_root / f"sample_{sample_index:03d}"
            sample_dir.mkdir(parents=True, exist_ok=True)

            sample_record = run_sample(
                args=args,
                sample_index=sample_index,
                sample_dir=sample_dir,
                object_storage_url=object_storage_url,
                sample_binary=sample_binary,
                peer_binary=peer_binary,
            )
            samples.append(sample_record)
            print(
                f"sample {sample_index}/{args.samples}: "
                f"init_complete={sample_record['replica_init_completed_delta_us']}us"
            )
    finally:
        if minio_endpoint is not None:
            cleanup_minio_port_forward(minio_endpoint)

    write_samples_csv(samples_csv_path, samples)
    samples_json_path.write_text(json.dumps(samples, indent=2))

    summary = {
        "result_root": str(result_root),
        "samples_csv_path": str(samples_csv_path),
        "samples_json_path": str(samples_json_path),
        "samples": len(samples),
        "peer_replicas": args.peer_replicas,
        "network_latency_ms": args.network_latency_ms,
        "network_latency_jitter_ms": args.network_latency_jitter_ms,
        "object_storage_url": object_storage_url,
    }
    print(json.dumps(summary, indent=2))
    return 0


def run_sample(
    args: argparse.Namespace,
    sample_index: int,
    sample_dir: Path,
    object_storage_url: Optional[str],
    sample_binary: Path,
    peer_binary: Path,
) -> dict[str, object]:
    persistent_path = f"{args.experiment_prefix}/persistent.json"
    membership_path = f"{args.experiment_prefix}/membership"

    reset_membership_prefix(
        storage_config(args, sample_dir, object_storage_url, persistent_path, membership_path)
    )

    peer_processes: list[ReplicaProcess] = []
    try:
        for peer_index in range(args.peer_replicas):
            replica_id = peer_index + 1
            http_port = args.base_http_port + replica_id
            internal_port = args.base_internal_port + replica_id
            peer_dir = sample_dir / f"peer_{replica_id}"
            peer_dir.mkdir(parents=True, exist_ok=True)
            env = replica_env(
                args=args,
                pid=replica_id,
                http_port=http_port,
                internal_port=internal_port,
                result_dir=peer_dir,
                object_storage_url=object_storage_url,
                persistent_path=persistent_path,
                membership_path=membership_path,
            )
            peer = ReplicaProcess(
                binary_path=peer_binary,
                env=env,
                stdout_path=peer_dir / "stdout.log",
                stderr_path=peer_dir / "stderr.log",
            )
            peer.start()
            wait_for_port(args.host, http_port, timeout_seconds=args.startup_timeout_seconds)
            peer_processes.append(peer)

        if peer_processes and args.peer_settle_seconds > 0:
            time.sleep(args.peer_settle_seconds)

        measured_replica_id = args.peer_replicas + 1
        measured_http_port = args.base_http_port + measured_replica_id
        measured_internal_port = args.base_internal_port + measured_replica_id
        measured_dir = sample_dir / "measured_replica"
        measured_dir.mkdir(parents=True, exist_ok=True)

        env = replica_env(
            args=args,
            pid=measured_replica_id,
            http_port=measured_http_port,
            internal_port=measured_internal_port,
            result_dir=measured_dir,
            object_storage_url=object_storage_url,
            persistent_path=persistent_path,
            membership_path=membership_path,
        )
        env["GRESSE_COLD_START_TIMEOUT_SECS"] = str(int(args.startup_timeout_seconds))

        completed = subprocess.run(
            [str(sample_binary)],
            env=env,
            capture_output=True,
            text=True,
            timeout=args.startup_timeout_seconds + 20,
            check=True,
        )
        sample = ColdStartSample(**json.loads(completed.stdout.strip()))
        sample_record = {
            "sample_index": sample_index,
            "peer_replicas": args.peer_replicas,
            "network_latency_ms": args.network_latency_ms,
            "network_latency_jitter_ms": args.network_latency_jitter_ms,
            "process_start_us": sample.process_start_us,
            "logging_initialized_us": sample.logging_initialized_us,
            "config_loaded_us": sample.config_loaded_us,
            "replica_with_config_completed_us": sample.replica_with_config_completed_us,
            "replica_task_spawned_us": sample.replica_task_spawned_us,
            "crdt_pid_set_us": sample.crdt_pid_set_us,
            "http_server_ready_us": sample.http_server_ready_us,
            "network_manager_ready_us": sample.network_manager_ready_us,
            "metric_writer_ready_us": sample.metric_writer_ready_us,
            "object_storage_client_start_us": sample.object_storage_client_start_us,
            "object_storage_client_ready_us": sample.object_storage_client_ready_us,
            "replica_run_start_us": sample.replica_run_start_us,
            "replica_init_start_us": sample.replica_init_start_us,
            "persistent_state_fetch_completed_us": sample.persistent_state_fetch_completed_us,
            "membership_descriptor_write_completed_us": sample.membership_descriptor_write_completed_us,
            "membership_directory_read_completed_us": sample.membership_directory_read_completed_us,
            "replica_init_completed_us": sample.replica_init_completed_us,
            "logging_initialized_delta_us": (
                sample.logging_initialized_us - sample.process_start_us
            ),
            "config_loaded_delta_us": (
                sample.config_loaded_us - sample.process_start_us
            ),
            "replica_with_config_completed_delta_us": (
                sample.replica_with_config_completed_us - sample.process_start_us
            ),
            "replica_task_spawned_delta_us": (
                sample.replica_task_spawned_us - sample.process_start_us
            ),
            "crdt_pid_set_delta_us": (
                sample.crdt_pid_set_us - sample.process_start_us
            ),
            "http_server_ready_delta_us": (
                sample.http_server_ready_us - sample.process_start_us
            ),
            "network_manager_ready_delta_us": (
                sample.network_manager_ready_us - sample.process_start_us
            ),
            "metric_writer_ready_delta_us": (
                sample.metric_writer_ready_us - sample.process_start_us
            ),
            "object_storage_client_start_delta_us": (
                sample.object_storage_client_start_us - sample.process_start_us
            ),
            "object_storage_client_ready_delta_us": (
                sample.object_storage_client_ready_us - sample.process_start_us
            ),
            "replica_run_start_delta_us": (
                sample.replica_run_start_us - sample.process_start_us
            ),
            "replica_init_start_delta_us": (
                sample.replica_init_start_us - sample.process_start_us
            ),
            "persistent_state_fetch_completed_delta_us": (
                sample.persistent_state_fetch_completed_us - sample.process_start_us
            ),
            "membership_descriptor_write_completed_delta_us": (
                sample.membership_descriptor_write_completed_us - sample.process_start_us
            ),
            "membership_directory_read_completed_delta_us": (
                sample.membership_directory_read_completed_us - sample.process_start_us
            ),
            "replica_init_completed_delta_us": (
                sample.replica_init_completed_us - sample.process_start_us
            ),
            "sample_dir": str(sample_dir),
            "persistent_path": persistent_path,
            "membership_path": membership_path,
        }
        (sample_dir / "sample.json").write_text(json.dumps(sample_record, indent=2))
        (measured_dir / "stdout.log").write_text(completed.stdout)
        (measured_dir / "stderr.log").write_text(completed.stderr)
        return sample_record
    finally:
        for peer in reversed(peer_processes):
            peer.stop()


def seed_storage_state(
    args: argparse.Namespace,
    result_root: Path,
    object_storage_url: Optional[str],
    sample_binary: Path,
) -> None:
    seed_dir = result_root / "seed"
    seed_dir.mkdir(parents=True, exist_ok=True)
    persistent_path = f"{args.experiment_prefix}/persistent.json"
    membership_path = f"{args.experiment_prefix}/membership"

    config = storage_config(args, seed_dir, object_storage_url, persistent_path, membership_path)
    reset_s3_prefixes(config)

    env = replica_env(
        args=args,
        pid=1,
        http_port=args.base_http_port + 1,
        internal_port=args.base_internal_port + 1,
        result_dir=seed_dir,
        object_storage_url=object_storage_url,
        persistent_path=persistent_path,
        membership_path=membership_path,
    )
    env["GRESSE_COLD_START_TIMEOUT_SECS"] = str(int(args.startup_timeout_seconds))
    completed = subprocess.run(
        [str(sample_binary)],
        env=env,
        capture_output=True,
        text=True,
        timeout=args.startup_timeout_seconds + 20,
        check=True,
    )
    (seed_dir / "stdout.log").write_text(completed.stdout)
    (seed_dir / "stderr.log").write_text(completed.stderr)

    reset_membership_prefix(config)


def replica_env(
    args: argparse.Namespace,
    pid: int,
    http_port: int,
    internal_port: int,
    result_dir: Path,
    object_storage_url: Optional[str],
    persistent_path: str,
    membership_path: str,
) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "AWS_REGION": args.region,
            "AWS_DEFAULT_REGION": args.region,
            "GRESSE_BENCH_PID": str(pid),
            "GRESSE_ADDR": args.host,
            "GRESSE_HTTP_PORT": str(http_port),
            "GRESSE_INTERNAL_PORT": str(internal_port),
            "GRESSE_RESULT_DIR_PATH": str(result_dir),
            "GRESSE_SYNC_INTERVAL_MS": str(args.sync_interval_ms),
            "GRESSE_OBJECT_STORAGE_DISCOVERY_INTERVAL_MS": str(args.discovery_interval_ms),
            "GRESSE_GC_INTERVAL_MS": str(args.gc_interval_ms),
            "GRESSE_REPLICA_NETWORK_LATENCY_MS": str(args.network_latency_ms),
            "GRESSE_REPLICA_NETWORK_LATENCY_JITTER_MS": str(args.network_latency_jitter_ms),
            "GRESSE_OBJECT_STORAGE_REGION": args.region,
            "GRESSE_OBJECT_STORAGE_BUCKET": args.bucket,
            "GRESSE_PERSISTENT_REPLICA_PATH": persistent_path,
            "GRESSE_MEMBERSHIP_DIRECTORY_PATH": membership_path,
        }
    )
    if object_storage_url:
        env["GRESSE_OBJECT_STORAGE_URL"] = object_storage_url
    if args.object_storage_access_key:
        env["GRESSE_OBJECT_STORAGE_ACCESS_KEY"] = args.object_storage_access_key
        env["AWS_ACCESS_KEY_ID"] = args.object_storage_access_key
    if args.object_storage_secret_key:
        env["GRESSE_OBJECT_STORAGE_SECRET_KEY"] = args.object_storage_secret_key
        env["AWS_SECRET_ACCESS_KEY"] = args.object_storage_secret_key
    if args.object_storage_session_token:
        env["GRESSE_OBJECT_STORAGE_SESSION_TOKEN"] = args.object_storage_session_token
        env["AWS_SESSION_TOKEN"] = args.object_storage_session_token
    return env


def storage_config(
    args: argparse.Namespace,
    result_dir: Path,
    object_storage_url: Optional[str],
    persistent_path: str,
    membership_path: str,
) -> BenchmarkConfig:
    return BenchmarkConfig(
        replicas=max(1, args.peer_replicas + 1),
        duration_seconds=0.0,
        result_dir=result_dir,
        max_store_size_mb=1.0,
        region=args.region,
        bucket=args.bucket,
        object_storage_url=object_storage_url,
        object_storage_access_key=args.object_storage_access_key,
        object_storage_secret_key=args.object_storage_secret_key,
        object_storage_session_token=args.object_storage_session_token,
        persistent_path=persistent_path,
        membership_path=membership_path,
        cargo_profile=args.cargo_profile,
    )


def use_aws_profile_mode(args: argparse.Namespace) -> bool:
    return (
        not args.object_storage_access_key
        and not args.object_storage_secret_key
        and not args.object_storage_session_token
        and bool(os.environ.get("AWS_PROFILE"))
    )


def ensure_local_minio(args: argparse.Namespace, result_root: Path) -> MinioEndpoint:
    if args.deploy_minio:
        subprocess.run(
            ["bash", "integrations/minio-latency/deploy_minio_latency.sh"],
            check=True,
        )

    port_forward_log = result_root / "minio_port_forward.log"
    port = args.host_port
    existing_port = find_reusable_minio_port_forward()
    if existing_port is not None:
        return MinioEndpoint(
            url=f"http://127.0.0.1:{existing_port}",
            port_forward_pid=None,
            reused_port_forward=True,
        )

    if port_is_listening(port):
        port = find_free_port(port + 1)
        if port is None:
            raise RuntimeError("Could not find a free local port for the MinIO port-forward")

    with port_forward_log.open("w") as log_handle:
        process = subprocess.Popen(
            [
                "kubectl",
                "-n",
                "gresse-minio",
                "port-forward",
                "svc/minio-proxy",
                f"{port}:9000",
            ],
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )

    if not wait_for_minio_health(port, timeout_seconds=30.0):
        process.kill()
        process.wait(timeout=10)
        raise RuntimeError(f"MinIO proxy port-forward did not become ready. See {port_forward_log}")

    return MinioEndpoint(
        url=f"http://127.0.0.1:{port}",
        port_forward_pid=process.pid,
        reused_port_forward=False,
    )


def cleanup_minio_port_forward(endpoint: MinioEndpoint) -> None:
    if endpoint.reused_port_forward or endpoint.port_forward_pid is None:
        return
    try:
        os.kill(endpoint.port_forward_pid, signal.SIGTERM)
    except ProcessLookupError:
        return


def port_is_listening(port: int) -> bool:
    with socket.socket() as sock:
        sock.settimeout(1)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def minio_proxy_healthy(port: int) -> bool:
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
        conn.request("GET", "/minio/health/live")
        response = conn.getresponse()
        response.read()
        conn.close()
        return response.status == 200
    except OSError:
        return False


def wait_for_minio_health(port: int, timeout_seconds: float) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if port_is_listening(port) and minio_proxy_healthy(port):
            return True
        time.sleep(1)
    return False


def find_reusable_minio_port_forward() -> Optional[int]:
    completed = subprocess.run(
        ["ps", "-ax", "-o", "pid=,command="],
        capture_output=True,
        text=True,
        check=True,
    )
    for line in completed.stdout.splitlines():
        command = line.strip()
        if "kubectl" not in command or "port-forward" not in command or "svc/minio-proxy" not in command:
            continue
        for token in command.split():
            if token.endswith(":9000"):
                parts = token.split(":")
                if len(parts) < 2:
                    continue
                try:
                    port = int(parts[-2])
                except ValueError:
                    continue
                if port_is_listening(port) and minio_proxy_healthy(port):
                    return port
    return None


def find_free_port(start_port: int) -> Optional[int]:
    for port in range(start_port, start_port + 100):
        if not port_is_listening(port):
            return port
    return None


def write_samples_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run repeated cold-start replica samples.")
    parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--result-root", type=Path, required=True)
    parser.add_argument("--experiment-prefix", default="cold-start")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--base-http-port", type=int, default=18080)
    parser.add_argument("--base-internal-port", type=int, default=19080)
    parser.add_argument("--peer-replicas", type=int, default=1)
    parser.add_argument("--peer-settle-seconds", type=float, default=1.0)
    parser.add_argument("--sync-interval-ms", type=int, default=1000)
    parser.add_argument("--discovery-interval-ms", type=int, default=1000)
    parser.add_argument("--gc-interval-ms", type=int, default=60000)
    parser.add_argument("--network-latency-ms", type=int, default=0)
    parser.add_argument("--network-latency-jitter-ms", type=int, default=0)
    parser.add_argument("--startup-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--cargo-profile", choices=["debug", "release"], default="release")
    parser.add_argument("--deploy-minio", action="store_true")
    parser.add_argument("--host-port", type=int, default=9000)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--object-storage-access-key", default=DEFAULT_ACCESS_KEY)
    parser.add_argument("--object-storage-secret-key", default=DEFAULT_SECRET_KEY)
    parser.add_argument("--object-storage-session-token")
    return parser.parse_args()


if __name__ == "__main__":
    sys.exit(main())
