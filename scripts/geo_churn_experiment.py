#!/usr/bin/env python3
"""Coordinate the multi-region GRESSE crash, recovery, and churn experiment.

By default the laptop stages one local lifecycle agent per VM.  Those agents
own replica processes and workload traffic; the laptop only preflights, polls
agent status, and collects artifacts.  ``execution_mode: laptop`` retains the
older SSH-driven controller for comparison.

See ``geo_churn_topology.example.json`` for the input format.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import http.client
import json
import random
import re
import shlex
import shutil
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


CSV_FIELDS = [
    "timestamp_us", "event", "replica", "region", "slot", "pid", "detail",
    "status_code", "latency_us", "operation", "value", "expected_live", "state_digest",
    "clock_offset_us", "rtt_us", "binary_sha256",
]


def durability_journal_files(base_path: str) -> tuple[str, ...]:
    """Return every remote file owned by the split durability journal."""
    path = Path(base_path)
    return (
        str(path),
        str(path.with_name(f"{path.name}.snapshot.json")),
        str(path.with_name(f"{path.name}.mutations.jsonl")),
        str(path.with_name(f".{path.name}.lock")),
    )


def replica_pid_from_probe(output: str) -> int | None:
    """Extract the effective replica PID printed by a bootstrap probe."""
    for line in reversed(output.splitlines()):
        candidate = line.strip()
        if candidate.isdigit() and int(candidate) > 0:
            return int(candidate)
    return None


@dataclass(frozen=True)
class Vm:
    name: str
    region: str
    ssh_host: str
    bind_ip: str
    advertise_ip: str
    client_host: str
    binary_path: str
    remote_root: str
    http_port_base: int
    internal_port_base: int


@dataclass(frozen=True)
class ScheduledEvent:
    at_seconds: float
    action: str
    vm: str
    slot: int


@dataclass
class Replica:
    vm: Vm
    slot: int
    pid: int
    generation: int = 0
    live: bool = False
    bootstrapping: bool = False
    remote_dir: str = ""

    @property
    def name(self) -> str:
        return f"{self.vm.name}-slot{self.slot}-g{self.generation}"

    @property
    def http_port(self) -> int:
        return self.vm.http_port_base + self.slot

    @property
    def internal_port(self) -> int:
        return self.vm.internal_port_base + self.slot


class CsvLog:
    def __init__(self, path: Path, append: bool = False) -> None:
        write_header = not append or not path.exists() or path.stat().st_size == 0
        self.handle = path.open("a" if append else "w", newline="")
        self.writer = csv.DictWriter(self.handle, fieldnames=CSV_FIELDS)
        if write_header:
            self.writer.writeheader()
        self.lock = threading.Lock()

    def write(self, event: str, replica: Optional[Replica] = None, **values: Any) -> None:
        row = {key: "" for key in CSV_FIELDS}
        row.update(timestamp_us=time.time_ns() // 1_000, event=event)
        if replica:
            row.update(
                replica=replica.name,
                region=replica.vm.region,
                slot=replica.slot,
                pid=replica.pid,
            )
        row.update({key: str(value) for key, value in values.items() if value is not None})
        with self.lock:
            self.writer.writerow(row)
            self.handle.flush()

    def close(self) -> None:
        self.handle.close()


class Controller:
    process_tag_env = "GRESSE_EXPERIMENT_TAG"
    process_tag_value = "geo-churn"

    def __init__(self, config: dict[str, Any], result_dir: Path, dry_run: bool) -> None:
        self.config = config
        self.result_dir = result_dir
        self.dry_run = dry_run
        self.execution_mode = str(config.get("execution_mode", "remote_agents"))
        if self.execution_mode not in {"remote_agents", "laptop"}:
            raise ValueError("execution_mode must be 'remote_agents' or 'laptop'")
        run_label = str(config.get("run_label", config.get("run_id", "geo-churn")))
        if not run_label.replace("-", "").replace("_", "").isalnum():
            raise ValueError("run_label may contain only letters, digits, hyphens, and underscores")
        self.run_label = run_label
        self.run_id = f"{run_label}-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{uuid.uuid4().hex[:8]}"
        self.remote_run_root = config.get("remote_run_root", "/tmp/gresse-geo-churn")
        self.registry_dir_name = ".gresse-geo-churn-active"
        self.bucket = required(config, "bucket")
        self.region = config.get("s3_region", "eu-north-1")
        self.sync_interval_ms = int(config.get("sync_interval_ms", 1000))
        self.discovery_interval_ms = int(config.get("discovery_interval_ms", 5000))
        self.gc_interval_ms = int(config.get("gc_interval_ms", 60000))
        self.workload_rate = float(config.get("workload_rate_per_server", 1.0))
        self.workload_value_domain_size = int(config.get("workload_value_domain_size", 1_000))
        if self.workload_value_domain_size < 1:
            raise ValueError("workload_value_domain_size must be positive")
        self.workload_max_in_flight = int(config.get("workload_max_in_flight", 512))
        self.workload_max_in_flight_per_replica = int(config.get("workload_max_in_flight_per_replica", 8))
        if self.workload_max_in_flight < 1:
            raise ValueError("workload_max_in_flight must be positive")
        if self.workload_max_in_flight_per_replica < 1:
            raise ValueError("workload_max_in_flight_per_replica must be positive")
        self.snapshot_interval = float(config.get("snapshot_interval_seconds", 30.0))
        self.final_convergence_seconds = float(config.get("final_convergence_seconds", 120.0))
        self.startup_timeout_seconds = float(config.get("startup_timeout_seconds", 180.0))
        self.startup_max_attempts = int(config.get("startup_max_attempts", 3))
        self.startup_retry_delay_seconds = float(config.get("startup_retry_delay_seconds", 1.0))
        if self.startup_max_attempts < 1:
            raise ValueError("startup_max_attempts must be positive")
        if self.startup_retry_delay_seconds < 0:
            raise ValueError("startup_retry_delay_seconds must be non-negative")
        self.ssh_timeout_seconds = float(config.get("ssh_timeout_seconds", 30.0))
        self.launch_timeout_seconds = float(config.get("launch_timeout_seconds", self.startup_timeout_seconds))
        if self.launch_timeout_seconds <= 0:
            raise ValueError("launch_timeout_seconds must be positive")
        self.collection_timeout_seconds = float(config.get("collection_timeout_seconds", 300.0))
        self.remote_shutdown_grace_seconds = int(config.get("remote_shutdown_grace_seconds", 20))
        self.remote_deadline_slack_seconds = int(config.get("remote_deadline_slack_seconds", 600))
        self.process_stop_timeout_seconds = float(config.get("process_stop_timeout_seconds", 20.0))
        self.membership_settle_seconds = float(config.get("membership_settle_seconds", max(10.0, 3 * self.discovery_interval_ms / 1000)))
        # A fresh SSH connection is used for this estimate.  Its setup delay is
        # asymmetric, particularly to far-away regions, so this is a coarse
        # sanity bound rather than an NTP-grade clock-synchronization test.
        self.max_clock_skew_ms = float(config.get("max_clock_skew_ms", 5000.0))
        self.expected_binary_sha256 = config.get("expected_binary_sha256")
        self.durable_recovery = bool(config.get("durable_recovery", True))
        self.ssh_options = config.get("ssh_options", [])
        if not isinstance(self.ssh_options, list) or not all(isinstance(value, str) for value in self.ssh_options):
            raise ValueError("ssh_options must be a list of SSH command-line arguments")
        self.duration = float(required(config, "duration_seconds"))
        self.remote_deadline_epoch = int(time.time() + self.duration + self.final_convergence_seconds + self.remote_deadline_slack_seconds)
        self.agent_start_delay_seconds = float(config.get("agent_start_delay_seconds", 45.0))
        self.agent_monitor_interval_seconds = float(config.get("agent_monitor_interval_seconds", 15.0))
        self.agent_completion_grace_seconds = float(config.get("agent_completion_grace_seconds", 120.0))
        self.agent_arm_timeout_seconds = float(config.get("agent_arm_timeout_seconds", 180.0))
        if self.agent_start_delay_seconds < 5:
            raise ValueError("agent_start_delay_seconds must be at least 5 seconds")
        if self.agent_monitor_interval_seconds <= 0:
            raise ValueError("agent_monitor_interval_seconds must be positive")
        if self.agent_arm_timeout_seconds <= 0:
            raise ValueError("agent_arm_timeout_seconds must be positive")
        self.log = CsvLog(result_dir / "controller_events.csv", append=(result_dir / "controller_events.csv").exists())
        self.pid_counter = int(time.time_ns() // 1_000) * 100
        self.pid_lock = threading.Lock()
        self.random = random.Random(int(config.get("seed", 0)))
        self.vms = self.parse_vms(config)
        self.replicas = {
            (vm.name, slot): Replica(vm=vm, slot=slot, pid=self.next_pid())
            for vm in self.vms.values()
            for slot in range(int(config.get("replicas_per_vm", 5)))
        }
        self.events = self.parse_events(config.get("events", []))
        self.workload_stop = threading.Event()

    @staticmethod
    def progress(message: str) -> None:
        """Emit a concise, uncoloured status line for an interactive runner."""
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        print(f"[{timestamp}] {message}", flush=True)

    def parse_vms(self, config: dict[str, Any]) -> dict[str, Vm]:
        result: dict[str, Vm] = {}
        for raw in required(config, "vms"):
            vm = Vm(
                name=required(raw, "name"), region=required(raw, "region"),
                ssh_host=required(raw, "ssh_host"), bind_ip=raw.get("bind_ip", raw["advertise_ip"]), advertise_ip=required(raw, "advertise_ip"),
                client_host=raw.get("client_host", raw["advertise_ip"]),
                binary_path=required(raw, "binary_path"),
                remote_root=raw.get("remote_root", self.remote_run_root),
                http_port_base=int(raw.get("http_port_base", 18080)),
                internal_port_base=int(raw.get("internal_port_base", 19080)),
            )
            if vm.name in result:
                raise ValueError(f"duplicate VM name: {vm.name}")
            result[vm.name] = vm
        if len(result) != 5:
            raise ValueError("this experiment requires exactly five VMs (one per cloud region)")
        if len({vm.region for vm in result.values()}) != 5:
            raise ValueError("each VM must name a distinct region")
        return result

    def parse_events(self, raw_events: list[dict[str, Any]]) -> list[ScheduledEvent]:
        events = []
        for raw in raw_events:
            event = ScheduledEvent(float(required(raw, "at_seconds")), required(raw, "action"), required(raw, "vm"), int(required(raw, "slot")))
            if event.action not in {"crash", "graceful_stop", "spawn"}:
                raise ValueError(f"invalid event action {event.action!r}")
            if event.vm not in self.vms or not 0 <= event.slot < int(self.config.get("replicas_per_vm", 5)):
                raise ValueError(f"event target is invalid: {raw}")
            if not 0 <= event.at_seconds <= self.duration:
                raise ValueError(
                    f"event time must fall within the experiment duration (0..{self.duration}): {raw}"
                )
            events.append(event)
        return sorted(events, key=lambda item: item.at_seconds)

    def next_pid(self) -> int:
        with self.pid_lock:
            self.pid_counter += 1
            return self.pid_counter

    def ssh(
        self, vm: Vm, command: str, check: bool = True, timeout_seconds: Optional[float] = None,
    ) -> subprocess.CompletedProcess[str]:
        argv = self.ssh_argv("ssh", vm.ssh_host) + [command]
        self.log.write("ssh_command", detail=f"{vm.name}: {command}")
        if self.dry_run:
            return subprocess.CompletedProcess(argv, 0, "dry-run", "")
        try:
            result = subprocess.run(
                argv, text=True, capture_output=True, check=False,
                timeout=timeout_seconds if timeout_seconds is not None else self.ssh_timeout_seconds,
            )
            if result.returncode != 0 and check:
                detail = f"{vm.name}: exit={result.returncode}; stderr={result.stderr.strip()}"
                self.log.write("ssh_failed", detail=detail)
                raise subprocess.CalledProcessError(
                    result.returncode, argv, output=result.stdout, stderr=result.stderr,
                )
            return result
        except subprocess.TimeoutExpired as error:
            self.log.write("ssh_timeout", detail=f"{vm.name}: {error}")
            if check:
                raise
            return subprocess.CompletedProcess(argv, 124, "", "SSH timeout")

    def ssh_argv(self, program: str, target: str) -> list[str]:
        return [program, "-n", *self.connection_options(), target]

    def connection_options(self) -> list[str]:
        return [
            "-o", "BatchMode=yes", "-o", f"ConnectTimeout={int(self.ssh_timeout_seconds)}",
            "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
            *self.ssh_options,
        ]

    def registry_path(self, replica: Replica) -> str:
        return f"{replica.vm.remote_root}/{self.registry_dir_name}/{self.run_id}-{replica.name}.pid"

    def durability_path(self, replica: Replica) -> str:
        return f"{replica.vm.remote_root}/{self.run_id}/durability/{replica.vm.name}-slot{replica.slot}.journal"

    @staticmethod
    def launch_cancel_path(replica: Replica) -> str:
        return f"{replica.remote_dir}/launch.cancel"

    def cleanup_stale_processes(self, vm: Vm) -> None:
        registry = f"{vm.remote_root}/{self.registry_dir_name}"
        registry_q = shlex.quote(registry)
        process_pattern = shlex.quote(
            f"^({self.process_tag_env}={self.process_tag_value}$|"
            f"GRESSE_RESULT_DIR_PATH=.*/{self.run_label}-)"
        )
        command = (
            f"registry={registry_q}; if [ -d \"$registry\" ]; then "
            "found=0; "
            "for record in \"$registry\"/*.pid; do [ -f \"$record\" ] || continue; "
            "found=1; read replica_pid watchdog_pid < \"$record\"; for pid in \"$replica_pid\" \"$watchdog_pid\"; do case \"$pid\" in ''|*[!0-9]*) ;; *) kill -TERM \"$pid\" 2>/dev/null || true ;; esac; done; done; "
            f"[ \"$found\" -eq 1 ] && sleep {self.remote_shutdown_grace_seconds}; "
            "for record in \"$registry\"/*.pid; do [ -f \"$record\" ] || continue; "
            "read replica_pid watchdog_pid < \"$record\"; for pid in \"$replica_pid\" \"$watchdog_pid\"; do case \"$pid\" in ''|*[!0-9]*) ;; *) kill -KILL \"$pid\" 2>/dev/null || true ;; esac; done; rm -f \"$record\"; done; fi"
            "; tagged=0; for proc in /proc/[0-9]*; do env_file=\"$proc/environ\"; [ -r \"$env_file\" ] || continue; "
            f"if tr '\\000' '\\n' < \"$env_file\" 2>/dev/null | grep -Eq {process_pattern}; then tagged=1; pid=${{proc##*/}}; kill -TERM \"$pid\" 2>/dev/null || true; fi; done; "
            f"[ \"$tagged\" -eq 1 ] && sleep {self.remote_shutdown_grace_seconds}; "
            "remaining=0; for proc in /proc/[0-9]*; do env_file=\"$proc/environ\"; [ -r \"$env_file\" ] || continue; "
            f"if tr '\\000' '\\n' < \"$env_file\" 2>/dev/null | grep -Eq {process_pattern}; then remaining=1; pid=${{proc##*/}}; kill -KILL \"$pid\" 2>/dev/null || true; fi; done; "
            "sleep 1; for proc in /proc/[0-9]*; do env_file=\"$proc/environ\"; [ -r \"$env_file\" ] || continue; "
            f"if tr '\\000' '\\n' < \"$env_file\" 2>/dev/null | grep -Eq {process_pattern}; then echo \"tagged process survived cleanup: ${{proc##*/}}\" >&2; exit 1; fi; done"
        )
        result = self.ssh(vm, command, check=False)
        if result.returncode != 0:
            raise RuntimeError(f"failed to clear tagged processes on {vm.name}: {result.stderr.strip()}")
        self.log.write("stale_process_cleanup_requested", detail=vm.name)

    def deploy_binary(self, local_binary: Path) -> str:
        if not local_binary.is_file() or not local_binary.stat().st_mode & 0o111:
            raise RuntimeError(f"release binary is missing or not executable: {local_binary}")
        digest = hashlib.sha256(local_binary.read_bytes()).hexdigest()
        for vm in self.vms.values():
            parent = str(Path(vm.binary_path).parent)
            self.ssh(vm, f"mkdir -p {shlex.quote(parent)}")
            if self.dry_run:
                self.log.write("binary_deploy_requested", detail=f"{local_binary} -> {vm.name}:{vm.binary_path}")
                continue
            try:
                subprocess.run(
                    ["scp", *self.connection_options(), str(local_binary), f"{vm.ssh_host}:{vm.binary_path}"],
                    text=True, capture_output=True, check=True, timeout=self.collection_timeout_seconds,
                )
                self.ssh(vm, f"chmod 0755 {shlex.quote(vm.binary_path)}")
                self.log.write("binary_deployed", detail=f"{vm.name}:{vm.binary_path}", binary_sha256=digest)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
                self.log.write("binary_deploy_failed", detail=f"{vm.name}: {error}")
                raise RuntimeError(f"failed to deploy binary to {vm.name}") from error
        return digest

    def preflight(self, deploy_binary: Optional[Path] = None) -> None:
        deployed_digest = None
        for vm in self.vms.values():
            self.progress(f"preflight cleaning tracked processes on {vm.name}")
            self.cleanup_stale_processes(vm)
        if deploy_binary is not None:
            deployed_digest = self.deploy_binary(deploy_binary)
            if self.expected_binary_sha256 and deployed_digest != self.expected_binary_sha256.lower():
                raise RuntimeError(f"local binary SHA-256 {deployed_digest} does not match expected {self.expected_binary_sha256}")
        binary_hashes: dict[str, str] = {}
        for vm in self.vms.values():
            self.progress(f"preflight verifying {vm.name}")
            if self.dry_run:
                self.log.write("preflight_ok", detail=f"{vm.name}: dry-run")
                continue
            command = (
                "command -v setsid >/dev/null && command -v nohup >/dev/null && "
                "command -v sha256sum >/dev/null && command -v grep >/dev/null && command -v date >/dev/null && command -v python3 >/dev/null && "
                f"test -x {shlex.quote(vm.binary_path)} && date -u +%s%N && sha256sum {shlex.quote(vm.binary_path)}"
            )
            before_ns = time.time_ns()
            result = self.ssh(vm, command)
            after_ns = time.time_ns()
            lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
            if len(lines) < 2 or not re.fullmatch(r"[0-9]+", lines[0]):
                raise RuntimeError(f"invalid preflight response from {vm.name}: {result.stdout!r}")
            digest = lines[-1].split(maxsplit=1)[0]
            if not re.fullmatch(r"[0-9a-fA-F]{64}", digest):
                raise RuntimeError(f"invalid SHA-256 response from {vm.name}: {lines[-1]!r}")
            remote_ns = int(lines[0])
            offset_us = (remote_ns - ((before_ns + after_ns) // 2)) // 1_000
            rtt_us = (after_ns - before_ns) // 1_000
            binary_hashes[vm.name] = digest.lower()
            self.log.write("preflight_ok", detail=result.stdout.strip(), clock_offset_us=offset_us, rtt_us=rtt_us, binary_sha256=digest.lower())
            if abs(offset_us) > self.max_clock_skew_ms * 1_000:
                raise RuntimeError(f"clock skew for {vm.name} is {offset_us / 1_000:.1f}ms, above {self.max_clock_skew_ms:.1f}ms")
            self.progress(f"preflight OK: {vm.name} (clock estimate {offset_us / 1_000:.1f}ms, RTT {rtt_us / 1_000:.1f}ms)")
        if self.dry_run:
            self.log.write("preflight_binary_verified", detail="dry-run")
            return
        hashes = set(binary_hashes.values())
        if len(hashes) != 1:
            raise RuntimeError(f"VMs have different benchmark binaries: {binary_hashes}")
        binary_hash = hashes.pop()
        if self.expected_binary_sha256 and binary_hash != self.expected_binary_sha256.lower():
            raise RuntimeError(f"binary SHA-256 {binary_hash} does not match expected {self.expected_binary_sha256}")
        if deployed_digest and binary_hash != deployed_digest:
            raise RuntimeError(f"deployed binary SHA-256 {deployed_digest} differs from VM hash {binary_hash}")
        self.log.write("preflight_binary_verified", detail=binary_hash)

    def start(self, replica: Replica) -> None:
        if replica.live:
            raise RuntimeError(f"cannot spawn already-live replica {replica.name}")
        # The HTTP listener comes up before Replica::init completes.  Keep
        # workload traffic away from this process until its bootstrap metric
        # confirms it can safely serve mutations.
        replica.bootstrapping = True
        try:
            failures: list[str] = []
            for attempt in range(1, self.startup_max_attempts + 1):
                try:
                    self.start_attempt(replica, attempt)
                    return
                except (TimeoutError, subprocess.TimeoutExpired, subprocess.CalledProcessError) as error:
                    detail = f"attempt {attempt}/{self.startup_max_attempts}: {type(error).__name__}: {error}"
                    failures.append(detail)
                    self.log.write("bootstrap_attempt_failed", replica, detail=detail)
                    self.cleanup_failed_start(replica, attempt)
                    if attempt < self.startup_max_attempts:
                        self.progress(f"retrying {replica.name} after failed bootstrap attempt {attempt}")
                        time.sleep(self.startup_retry_delay_seconds)
            raise TimeoutError(
                f"replica failed to initialize after {self.startup_max_attempts} attempts: "
                + "; ".join(failures)
            )
        finally:
            replica.bootstrapping = False

    def start_attempt(self, replica: Replica, attempt: int) -> None:
        predecessor_pid = replica.pid
        replica.generation += 1
        replica.pid = self.next_pid()
        replica.remote_dir = f"{replica.vm.remote_root}/{self.run_id}/{replica.name}"
        registry_path = self.registry_path(replica)
        durability_path = self.durability_path(replica)
        launch_cancel_path = self.launch_cancel_path(replica)
        prefix = f"{self.run_id}"
        env = {
            "GRESSE_BENCH_PID": str(replica.pid), "GRESSE_ADDR": replica.vm.bind_ip,
            "GRESSE_ADVERTISE_ADDR": replica.vm.advertise_ip,
            "GRESSE_HTTP_PORT": str(replica.http_port), "GRESSE_INTERNAL_PORT": str(replica.internal_port),
            "GRESSE_RESULT_DIR_PATH": replica.remote_dir,
            "GRESSE_SYNC_INTERVAL_MS": str(self.sync_interval_ms),
            "GRESSE_OBJECT_STORAGE_DISCOVERY_INTERVAL_MS": str(self.discovery_interval_ms),
            "GRESSE_GC_INTERVAL_MS": str(self.gc_interval_ms),
            "GRESSE_OBJECT_STORAGE_REGION": self.region, "GRESSE_OBJECT_STORAGE_BUCKET": self.bucket,
                "GRESSE_PERSISTENT_REPLICA_PATH": f"{prefix}/persistent.json",
                "GRESSE_MEMBERSHIP_DIRECTORY_PATH": f"{prefix}/membership",
                "GRESSE_RECOVERED_PREDECESSOR_PID": str(predecessor_pid),
                self.process_tag_env: self.process_tag_value,
        }
        if self.durable_recovery:
            env["GRESSE_DURABLE"] = "true"
            env["GRESSE_DURABILITY_PATH"] = durability_path
        exported = " ".join(f"{key}={shlex.quote(value)}" for key, value in env.items())
        runtime_command = f"exec env {exported} {shlex.quote(replica.vm.binary_path)}"
        watchdog_command = (
            f"remaining=$(({self.remote_deadline_epoch} - $(date +%s))); [ \"$remaining\" -gt 0 ] || remaining=1; "
            f"sleep \"$remaining\"; kill -TERM \"$1\" 2>/dev/null || true; sleep {self.remote_shutdown_grace_seconds}; "
            f"kill -KILL \"$1\" 2>/dev/null || true; rm -f {shlex.quote(registry_path)}"
        )
        command = (
            f"mkdir -p {shlex.quote(replica.remote_dir)} {shlex.quote(replica.vm.remote_root + '/' + self.registry_dir_name)} {shlex.quote(replica.vm.remote_root + '/' + self.run_id + '/durability')} && "
            f"test ! -e {shlex.quote(launch_cancel_path)} && "
            f"(setsid sh -c {shlex.quote(runtime_command)} "
            f"> {shlex.quote(replica.remote_dir + '/stdout.log')} "
            f"2> {shlex.quote(replica.remote_dir + '/stderr.log')} < /dev/null & "
            f"replica_pid=$!; echo \"$replica_pid\" > {shlex.quote(replica.remote_dir + '/os.pid')}; "
            f"if test -e {shlex.quote(launch_cancel_path)}; then kill -KILL \"$replica_pid\" 2>/dev/null || true; exit 1; fi; "
            f"nohup env {self.process_tag_env}={shlex.quote(self.process_tag_value)} sh -c {shlex.quote(watchdog_command)} sh \"$replica_pid\" "
            f"> {shlex.quote(replica.remote_dir + '/watchdog.log')} 2>&1 < /dev/null & watchdog_pid=$!; echo \"$watchdog_pid\" > {shlex.quote(replica.remote_dir + '/watchdog.pid')}; "
            f"echo \"$replica_pid $watchdog_pid\" > {shlex.quote(registry_path)})"
        )
        self.log.write("bootstrap_attempt_started", replica, detail=f"attempt {attempt}/{self.startup_max_attempts}")
        self.ssh(replica.vm, command, timeout_seconds=self.launch_timeout_seconds)
        replica.live = True
        self.log.write("spawn_requested", replica, detail=replica.remote_dir)
        self.progress(f"starting {replica.name} ({replica.vm.region}), attempt {attempt}")
        deadline = time.monotonic() + self.startup_timeout_seconds
        self.wait_until_ready(replica, deadline)
        self.wait_until_bootstrapped(replica, deadline)
        self.progress(f"ready {replica.name}")

    def cleanup_failed_start(self, replica: Replica, attempt: int) -> None:
        cancel_path = self.launch_cancel_path(replica)
        command = (
            f"mkdir -p {shlex.quote(replica.remote_dir)}; touch {shlex.quote(cancel_path)}; "
            f"test -f {shlex.quote(replica.remote_dir + '/watchdog.pid')} && kill -KILL $(cat {shlex.quote(replica.remote_dir + '/watchdog.pid')}) 2>/dev/null || true; "
            f"test -f {shlex.quote(replica.remote_dir + '/os.pid')} && kill -KILL $(cat {shlex.quote(replica.remote_dir + '/os.pid')}) 2>/dev/null || true; "
            f"rm -f {shlex.quote(self.registry_path(replica))}"
        )
        self.ssh(replica.vm, command, check=False, timeout_seconds=self.ssh_timeout_seconds)
        replica.live = False
        self.log.write("bootstrap_attempt_cleaned_up", replica, detail=f"attempt {attempt}/{self.startup_max_attempts}")

    def wait_until_ready(self, replica: Replica, deadline: Optional[float] = None) -> None:
        if self.dry_run:
            self.log.write("http_ready", replica, detail="dry-run")
            return
        deadline = deadline if deadline is not None else time.monotonic() + self.startup_timeout_seconds
        last_error = ""
        while time.monotonic() < deadline:
            try:
                request_timeout = min(3.0, max(0.1, deadline - time.monotonic()))
                connection = http.client.HTTPConnection(replica.vm.client_host, replica.http_port, timeout=request_timeout)
                connection.request("POST", "/", body=json.dumps({"type": "Query", "params": "Meta"}), headers={"Content-Type": "application/json"})
                response = connection.getresponse()
                response.read()
                connection.close()
                if response.status == 200:
                    self.log.write("http_ready", replica)
                    return
                last_error = f"HTTP {response.status}"
            except Exception as error:
                last_error = f"{type(error).__name__}: {error}"
            time.sleep(min(0.5, max(0.0, deadline - time.monotonic())))
        self.log.write("http_ready_timeout", replica, detail=last_error)
        raise TimeoutError(f"replica did not become HTTP-ready: {replica.name}: {last_error}")

    def wait_until_bootstrapped(self, replica: Replica, deadline: Optional[float] = None) -> None:
        if self.dry_run:
            self.log.write("bootstrap_ready", replica, detail="dry-run")
            return
        metrics_glob = f"{shlex.quote(replica.remote_dir)}/server_*.csv"
        command = (
            f"for metrics_file in {metrics_glob}; do "
            "[ -f \"$metrics_file\" ] || continue; "
            "if grep -q '^server,replica_init,completed,' \"$metrics_file\"; then "
            "metrics_name=${metrics_file##*/}; metrics_pid=${metrics_name#server_}; "
            "printf '%s\\n' \"${metrics_pid%.csv}\"; exit 0; fi; "
            "done; exit 1"
        )
        deadline = deadline if deadline is not None else time.monotonic() + self.startup_timeout_seconds
        while time.monotonic() < deadline:
            # This is a lightweight status probe.  A stalled SSH session must
            # not consume the full replica-bootstrap allowance.
            probe_timeout = min(5.0, self.ssh_timeout_seconds, max(0.1, deadline - time.monotonic()))
            result = self.ssh(
                replica.vm,
                command,
                check=False,
                timeout_seconds=probe_timeout,
            )
            effective_pid = replica_pid_from_probe(result.stdout) if result.returncode == 0 else None
            if effective_pid is not None:
                if effective_pid != replica.pid:
                    requested_pid = replica.pid
                    self.log.write(
                        "replica_pid_recovered",
                        replica,
                        detail=f"requested_pid={requested_pid},effective_pid={effective_pid}",
                    )
                    replica.pid = effective_pid
                self.log.write("bootstrap_ready", replica)
                return
            time.sleep(0.5)
        self.log.write("bootstrap_timeout", replica)
        raise TimeoutError(f"replica did not complete bootstrap: {replica.name}")

    def wait_for_membership_stabilization(self) -> None:
        if self.dry_run:
            self.log.write("membership_stabilized", detail="dry-run")
            return
        self.log.write("membership_settle_started", detail=str(self.membership_settle_seconds))
        self.progress(f"waiting {self.membership_settle_seconds:.0f}s for membership stabilization")
        deadline = time.monotonic() + self.membership_settle_seconds
        while time.monotonic() < deadline:
            time.sleep(min(1.0, deadline - time.monotonic()))
        self.log.write("membership_stabilized", detail=str(self.membership_settle_seconds))
        self.progress("membership stabilized")

    def stop(self, replica: Replica, graceful: bool, retire_durability: bool = False) -> None:
        if not replica.live:
            self.log.write("lifecycle_ignored_not_live", replica, detail="graceful_stop" if graceful else "crash")
            return
        replica.bootstrapping = False
        signal = "TERM" if graceful else "KILL"
        self.progress(f"{'gracefully stopping' if graceful else 'crashing'} {replica.name}")
        command = (
            f"test -f {shlex.quote(replica.remote_dir + '/watchdog.pid')} && kill -KILL $(cat {shlex.quote(replica.remote_dir + '/watchdog.pid')}) 2>/dev/null || true; "
            f"test -f {shlex.quote(replica.remote_dir + '/os.pid')} && kill -{signal} $(cat {shlex.quote(replica.remote_dir + '/os.pid')}) 2>/dev/null || true"
        )
        self.ssh(replica.vm, command, check=False)
        self.log.write("graceful_stop_requested" if graceful else "crash_requested", replica)
        if not self.wait_until_stopped(replica, self.process_stop_timeout_seconds):
            self.log.write("process_stop_escalated", replica)
            kill_command = f"test -f {shlex.quote(replica.remote_dir + '/os.pid')} && kill -KILL $(cat {shlex.quote(replica.remote_dir + '/os.pid')}) 2>/dev/null || true"
            self.ssh(replica.vm, kill_command, check=False)
            if not self.wait_until_stopped(replica, self.process_stop_timeout_seconds):
                self.log.write("process_stop_timeout", replica)
                raise TimeoutError(f"replica did not exit after forced stop: {replica.name}")
        self.ssh(replica.vm, f"rm -f {shlex.quote(self.registry_path(replica))}", check=False)
        replica.live = False
        self.log.write("process_stopped", replica)
        if retire_durability and self.durable_recovery:
            journal_files = " ".join(
                shlex.quote(path)
                for path in durability_journal_files(self.durability_path(replica))
            )
            self.ssh(replica.vm, f"rm -f {journal_files}", check=False)
            self.log.write("durability_journal_retired", replica)
        self.progress(f"stopped {replica.name}")

    def wait_until_stopped(self, replica: Replica, timeout_seconds: float) -> bool:
        if self.dry_run:
            return True
        pid_path = shlex.quote(replica.remote_dir + "/os.pid")
        command = f"test -f {pid_path} && kill -0 $(cat {pid_path}) 2>/dev/null"
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            result = self.ssh(replica.vm, command, check=False)
            if result.returncode == 1:
                return True
            if result.returncode not in (0, 1):
                self.log.write("process_stop_check_failed", replica, detail=str(result.returncode))
            time.sleep(0.5)
        return False

    def shutdown_all(self) -> None:
        for replica in self.replicas.values():
            try:
                self.stop(replica, graceful=True)
            except Exception as error:
                self.log.write("process_shutdown_failed", replica, detail=f"{type(error).__name__}: {error}")

    def execute_lifecycle_event(self, event: ScheduledEvent) -> None:
        """Run one scheduled lifecycle operation in a controller worker thread."""
        replica = self.replicas[(event.vm, event.slot)]
        self.progress(f"scheduled event at +{event.at_seconds:.0f}s: {event.action} {replica.name}")
        self.log.write("scheduled_event_started", replica, detail=f"t={event.at_seconds:.3f}s {event.action}")
        if event.action == "crash":
            self.stop(replica, graceful=False)
        elif event.action == "graceful_stop":
            # A planned replacement is a new replica initialization.  Its
            # local journal is retired after clean shutdown; crash recovery
            # deliberately retains its journal for the next spawn.
            self.stop(replica, graceful=True, retire_durability=True)
        else:
            self.start(replica)
        self.log.write("scheduled_event_completed", replica, detail=f"t={event.at_seconds:.3f}s {event.action}")

    def dispatch_due_events(self, events: list[ScheduledEvent]) -> None:
        """Start a same-timestamp event group concurrently and wait for its completion.

        Waiting preserves the schedule's ordering between distinct timestamps,
        while parallel SSH workers ensure operations in one group are issued
        independently instead of being serialized behind remote round trips.
        """
        if not events:
            return
        scheduled_time = events[0].at_seconds
        if any(event.at_seconds != scheduled_time for event in events):
            raise ValueError("dispatch_due_events requires events with the same timestamp")
        self.progress(f"dispatching {len(events)} concurrent lifecycle events scheduled for +{scheduled_time:.0f}s")
        for event in events:
            replica = self.replicas[(event.vm, event.slot)]
            self.log.write("scheduled_event_dispatched", replica, detail=f"t={event.at_seconds:.3f}s {event.action}")
        with ThreadPoolExecutor(max_workers=len(events), thread_name_prefix="lifecycle-event") as pool:
            futures = [pool.submit(self.execute_lifecycle_event, event) for event in events]
            for future in futures:
                future.result()

    def request(self, replica: Replica, payload: dict[str, Any]) -> None:
        started = time.time_ns() // 1_000
        status: Optional[int] = None
        detail = ""
        try:
            if self.dry_run:
                raise RuntimeError("dry-run: HTTP request suppressed")
            connection = http.client.HTTPConnection(replica.vm.client_host, replica.http_port, timeout=5)
            connection.request("POST", "/", body=json.dumps(payload), headers={"Content-Type": "application/json"})
            response = connection.getresponse()
            response.read()
            status = response.status
            connection.close()
        except Exception as error:  # expected for deliberately unavailable processes
            detail = f"{type(error).__name__}: {error}"
        latency = (time.time_ns() // 1_000) - started
        operation, value = next(iter(payload["params"].items()))
        self.log.write("client_request", replica, status_code=status, latency_us=latency, operation=operation, value=value, expected_live=replica.live, detail=detail)

    def workload(self) -> None:
        """Dispatch the configured aggregate rate once per VM, round-robin over live replicas."""
        replicas_by_vm: dict[str, list[Replica]] = {}
        for replica in self.replicas.values():
            replicas_by_vm.setdefault(replica.vm.name, []).append(replica)
        if self.workload_rate <= 0:
            return

        period = 1.0 / self.workload_rate
        next_dispatch = time.monotonic()
        next_replica_index = {vm_name: 0 for vm_name in replicas_by_vm}
        # Bound slow/unavailable replicas independently.  Without this, a
        # 5-second HTTP timeout turns a 10/s schedule into 50 queued requests
        # for one replica, which can starve its startup work and the laptop's
        # request pool.
        in_flight: dict[Any, tuple[str, int]] = {}
        in_flight_per_replica: dict[tuple[str, int], int] = {}
        with ThreadPoolExecutor(max_workers=self.workload_max_in_flight, thread_name_prefix="workload-request") as pool:
            while not self.workload_stop.is_set():
                for future, replica_key in list(in_flight.items()):
                    if future.done():
                        del in_flight[future]
                        in_flight_per_replica[replica_key] -= 1
                for vm_name, replicas in replicas_by_vm.items():
                    if len(in_flight) >= self.workload_max_in_flight:
                        break
                    eligible = [
                        replica
                        for replica in replicas
                        if replica.live
                        and not replica.bootstrapping
                        and in_flight_per_replica.get((replica.vm.name, replica.slot), 0)
                        < self.workload_max_in_flight_per_replica
                    ]
                    if not eligible:
                        continue
                    replica = eligible[next_replica_index[vm_name] % len(eligible)]
                    next_replica_index[vm_name] += 1
                    replica_key = (replica.vm.name, replica.slot)
                    operation = "Remove" if self.random.random() < 0.5 else "Insert"
                    payload = {"type": "Mutation", "params": {operation: self.random.randrange(self.workload_value_domain_size)}}
                    future = pool.submit(self.request, replica, payload)
                    in_flight[future] = replica_key
                    in_flight_per_replica[replica_key] = in_flight_per_replica.get(replica_key, 0) + 1

                next_dispatch += period
                self.workload_stop.wait(max(0.0, next_dispatch - time.monotonic()))
                if next_dispatch < time.monotonic() - period:
                    next_dispatch = time.monotonic()

    def snapshot_states(self) -> None:
        digests: dict[str, list[str]] = {}
        if self.dry_run:
            self.log.write("state_snapshot_summary", detail="dry-run: HTTP state snapshots suppressed")
            return
        for replica in self.replicas.values():
            if not replica.live:
                continue
            try:
                connection = http.client.HTTPConnection(replica.vm.client_host, replica.http_port, timeout=5)
                connection.request("POST", "/", body=json.dumps({"type": "Query", "params": "Elements"}), headers={"Content-Type": "application/json"})
                response = connection.getresponse()
                body = response.read()
                connection.close()
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status}")
                canonical = json.dumps(json.loads(body), sort_keys=True, separators=(",", ":"))
                digest = hashlib.sha256(canonical.encode()).hexdigest()
                digests.setdefault(digest, []).append(replica.name)
                self.log.write("state_snapshot", replica, status_code=200, state_digest=digest)
            except Exception as error:
                self.log.write("state_snapshot_failed", replica, detail=f"{type(error).__name__}: {error}")
        self.log.write("state_snapshot_summary", detail=json.dumps(digests, sort_keys=True))

    @staticmethod
    def remote_agent_source() -> Path:
        return Path(__file__).with_name("geo_churn_remote_agent.py")

    def agent_remote_paths(self, vm: Vm) -> tuple[str, str, str, str]:
        root = f"{vm.remote_root}/{self.run_id}"
        return (
            root,
            f"{root}/geo_churn_remote_agent.py",
            f"{root}/agent_spec.json",
            f"{root}/agent_status.json",
        )

    def agent_start_signal_path(self, vm: Vm) -> str:
        return f"{vm.remote_root}/{self.run_id}/agent_start_epoch"

    def agent_abort_path(self, vm: Vm) -> str:
        return f"{vm.remote_root}/{self.run_id}/agent_abort"

    @property
    def run_state_path(self) -> Path:
        return self.result_dir / "run_state.json"

    def write_run_state(self, state: str, start_epoch: int) -> None:
        payload = {"run_id": self.run_id, "start_epoch": start_epoch, "state": state}
        temporary_path = self.run_state_path.with_suffix(".tmp")
        temporary_path.write_text(json.dumps(payload, indent=2) + "\n")
        temporary_path.replace(self.run_state_path)

    def agent_spec(self, vm: Vm, vm_index: int) -> dict[str, Any]:
        settings = {
            key: self.config[key]
            for key in (
                "duration_seconds", "bucket", "s3_region", "replicas_per_vm",
                "sync_interval_ms", "discovery_interval_ms", "gc_interval_ms",
                "workload_rate_per_server", "workload_value_domain_size", "workload_max_in_flight",
                "workload_max_in_flight_per_replica", "snapshot_interval_seconds",
                "final_convergence_seconds", "startup_timeout_seconds",
                "startup_max_attempts", "startup_retry_delay_seconds",
                "process_stop_timeout_seconds", "membership_settle_seconds",
                "durable_recovery", "seed",
            )
            if key in self.config
        }
        vm_spec = {
            "name": vm.name, "region": vm.region, "bind_ip": vm.bind_ip,
            "advertise_ip": vm.advertise_ip, "binary_path": vm.binary_path,
            "remote_root": vm.remote_root, "http_port_base": vm.http_port_base,
            "internal_port_base": vm.internal_port_base,
        }
        return {
            "version": 1,
            "run_id": self.run_id,
            "run_label": self.run_label,
            # Leave plenty of headroom between VMs while retaining an easily
            # recognisable, globally unique GRESSE PID range.
            "pid_base": (int(time.time_ns() // 1_000) * 100) + vm_index * 1_000_000,
            "vm": vm_spec,
            "settings": settings,
            "events": [
                {"at_seconds": event.at_seconds, "action": event.action, "slot": event.slot}
                for event in self.events if event.vm == vm.name
            ],
        }

    def scp_to_vm(self, vm: Vm, source: Path, destination: str) -> None:
        if self.dry_run:
            self.log.write("agent_stage_requested", detail=f"{source} -> {vm.name}:{destination}")
            return
        try:
            subprocess.run(
                ["scp", *self.connection_options(), str(source), f"{vm.ssh_host}:{destination}"],
                text=True, capture_output=True, check=True, timeout=self.collection_timeout_seconds,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
            detail = getattr(error, "stderr", "") or str(error)
            self.log.write("agent_stage_failed", detail=f"{vm.name}: {detail}")
            raise RuntimeError(f"failed to stage remote agent on {vm.name}: {detail}") from error

    def prepare_remote_agents(self) -> int:
        """Copy the agent program, then assign all per-VM schedules before arming."""
        source = self.remote_agent_source()
        if not source.is_file():
            raise RuntimeError(f"remote agent program is missing: {source}")
        self.progress("staging per-VM experiment agents")
        for vm in self.vms.values():
            root, remote_agent, _, _ = self.agent_remote_paths(vm)
            self.ssh(vm, f"mkdir -p {shlex.quote(root)}")
            self.scp_to_vm(vm, source, remote_agent)
            self.ssh(vm, f"chmod 0755 {shlex.quote(remote_agent)}")

        specs_dir = self.result_dir / "agent_specs"
        specs_dir.mkdir(exist_ok=True)
        for index, vm in enumerate(self.vms.values()):
            _, _, remote_spec, _ = self.agent_remote_paths(vm)
            local_spec = specs_dir / f"{vm.name}.json"
            local_spec.write_text(json.dumps(self.agent_spec(vm, index), indent=2) + "\n")
            self.scp_to_vm(vm, local_spec, remote_spec)
            self.log.write("agent_schedule_assigned", detail=f"{vm.name}: {remote_spec}")

        for vm in self.vms.values():
            root, remote_agent, remote_spec, _ = self.agent_remote_paths(vm)
            command = (
                f"mkdir -p {shlex.quote(root)} && "
                "("
                f"nohup env {self.process_tag_env}={shlex.quote(self.process_tag_value)} "
                f"{shlex.quote(remote_agent)} --spec {shlex.quote(remote_spec)} "
                f"> {shlex.quote(root + '/agent.stdout.log')} "
                f"2> {shlex.quote(root + '/agent.stderr.log')} < /dev/null & "
                f"agent_pid=$!; echo \"$agent_pid\" > {shlex.quote(root + '/agent.pid')}"
                ")"
            )
            self.ssh(vm, command, timeout_seconds=self.launch_timeout_seconds)
            self.log.write("agent_launch_requested", detail=vm.name)

        if self.dry_run:
            return int(time.time() + self.agent_start_delay_seconds)

        # Each agent has its schedule on disk but cannot execute it until the
        # shared start signal below is published.  This removes sequential SSH
        # launch latency from the experiment's timing.
        arm_deadline = time.monotonic() + self.agent_arm_timeout_seconds
        armed: set[str] = set()
        while time.monotonic() < arm_deadline:
            for vm in self.vms.values():
                if vm.name in armed:
                    continue
                status = self.read_agent_status(vm)
                if status is not None and status.get("state") == "armed":
                    armed.add(vm.name)
                    self.log.write("agent_armed", detail=vm.name)
            if len(armed) == len(self.vms):
                break
            time.sleep(min(1.0, max(0.0, arm_deadline - time.monotonic())))
        if len(armed) != len(self.vms):
            missing = sorted(set(self.vms) - armed)
            self.cancel_remote_agent_start()
            raise RuntimeError(f"remote agents did not arm before timeout: {', '.join(missing)}")

        start_epoch = int(time.time() + self.agent_start_delay_seconds)
        failures: list[str] = []
        def publish_start(vm: Vm) -> None:
            signal_path = self.agent_start_signal_path(vm)
            command = (
                f"printf '%s\\n' {shlex.quote(str(start_epoch))} > {shlex.quote(signal_path + '.tmp')} && "
                f"mv {shlex.quote(signal_path + '.tmp')} {shlex.quote(signal_path)}"
            )
            self.ssh(vm, command, timeout_seconds=self.launch_timeout_seconds)
            self.log.write("agent_start_assigned", detail=f"{vm.name}: {start_epoch}")

        with ThreadPoolExecutor(max_workers=len(self.vms), thread_name_prefix="agent-start") as pool:
            futures = {pool.submit(publish_start, vm): vm for vm in self.vms.values()}
            for future, vm in futures.items():
                try:
                    future.result()
                except Exception as error:
                    failures.append(f"{vm.name}: {type(error).__name__}: {error}")
        if failures:
            self.cancel_remote_agent_start()
            raise RuntimeError("failed to publish remote-agent start signal: " + "; ".join(failures))
        return start_epoch

    def abort_remote_agents(self) -> None:
        """Tell every remote agent to stop, both before and after its start time."""
        def cancel(vm: Vm) -> None:
            self.ssh(vm, f"touch {shlex.quote(self.agent_abort_path(vm))}", check=False, timeout_seconds=min(5.0, self.ssh_timeout_seconds))
        with ThreadPoolExecutor(max_workers=len(self.vms), thread_name_prefix="agent-cancel") as pool:
            futures = [pool.submit(cancel, vm) for vm in self.vms.values()]
            for future in futures:
                future.result()

    def cancel_remote_agent_start(self) -> None:
        """Best-effort cancellation prevents a late launch from starting alone."""
        self.abort_remote_agents()

    def read_agent_status(self, vm: Vm) -> Optional[dict[str, Any]]:
        _, _, _, status_path = self.agent_remote_paths(vm)
        result = self.ssh(vm, f"test -f {shlex.quote(status_path)} && cat {shlex.quote(status_path)}", check=False, timeout_seconds=min(5.0, self.ssh_timeout_seconds))
        if result.returncode != 0:
            self.log.write("agent_status_unavailable", detail=f"{vm.name}: SSH/status return code {result.returncode}")
            return None
        try:
            status = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            self.log.write("agent_status_invalid", detail=f"{vm.name}: {error}")
            return None
        self.log.write("agent_status", detail=f"{vm.name}: {json.dumps(status, sort_keys=True)}")
        return status

    def monitor_remote_agents(self, start_epoch: int, resumed: bool = False) -> list[str]:
        deadline = start_epoch + self.duration + self.final_convergence_seconds + self.agent_completion_grace_seconds
        if resumed:
            deadline = max(deadline, time.time() + self.agent_completion_grace_seconds)
        final_states: dict[str, str] = {}
        abort_sent = False
        self.progress(f"remote agents armed; experiment starts at {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(start_epoch))}")
        while time.time() < deadline:
            for vm in self.vms.values():
                status = self.read_agent_status(vm)
                if status is not None:
                    final_states[vm.name] = str(status.get("state", "unknown"))
            failed_agents = sorted(name for name, state in final_states.items() if state == "failed")
            if failed_agents and not abort_sent:
                self.progress(f"remote agent failure detected ({', '.join(failed_agents)}); aborting remaining agents")
                self.log.write("remote_agent_abort_requested", detail=", ".join(failed_agents))
                self.abort_remote_agents()
                abort_sent = True
            if len(final_states) == len(self.vms) and all(
                state in {"completed", "failed", "aborted", "cancelled"}
                for state in final_states.values()
            ):
                break
            time.sleep(self.agent_monitor_interval_seconds)
        failed = [name for name, state in final_states.items() if state != "completed"]
        for vm in self.vms.values():
            if vm.name not in final_states:
                failed.append(vm.name)
        return failed

    def run_remote_agents(self) -> None:
        started = False
        try:
            self.progress("preflight: cleaning prior tracked processes and validating hosts")
            self.preflight()
            start_epoch = self.prepare_remote_agents()
            started = True
            self.write_run_state("running", start_epoch)
            if self.dry_run:
                self.log.write("remote_agent_plan_validated", detail=f"start_epoch={start_epoch}")
                self.log.close()
                self.progress("remote-agent experiment plan validated")
                return
            failed = self.monitor_remote_agents(start_epoch)
            if failed:
                raise RuntimeError(f"remote agent did not complete successfully: {', '.join(sorted(set(failed)))}")
            self.write_run_state("completed", start_epoch)
            self.progress("all remote agents completed")
        except BaseException:
            # Do not use SSH to stop agents here: each agent owns its local
            # process tree and has a deadline.  The laptop only observes.
            self.collect()
            self.log.close()
            self.progress("remote-agent experiment failed; artifacts collected")
            raise
        self.collect()
        self.log.close()
        self.progress("remote-agent experiment completed")

    def resume_remote_agents(self) -> None:
        if not self.run_state_path.exists():
            raise RuntimeError(f"cannot resume without {self.run_state_path}")
        state = json.loads(self.run_state_path.read_text())
        self.run_id = required(state, "run_id")
        start_epoch = int(required(state, "start_epoch"))
        self.progress(f"resuming remote-agent monitoring for run {self.run_id}")
        try:
            failed = self.monitor_remote_agents(start_epoch, resumed=True)
            if failed:
                raise RuntimeError(f"remote agent did not complete successfully: {', '.join(sorted(set(failed)))}")
            self.write_run_state("completed", start_epoch)
            self.progress("all remote agents completed")
        finally:
            self.collect()
            self.log.close()

    def collect(self) -> None:
        artifacts = self.result_dir / "remote_artifacts"
        artifacts.mkdir(exist_ok=True)
        self.progress("collecting remote logs and metrics")
        for vm in self.vms.values():
            destination = artifacts / vm.name
            staging = artifacts / f".{vm.name}.{self.run_id}.collecting"
            if self.dry_run:
                self.log.write("collect_requested", detail=f"{vm.name} -> {destination}")
                continue
            try:
                # SCP copies a source directory *inside* an existing
                # destination.  A resumed collection used to leave the first
                # partial copy in ``destination`` and create
                # ``destination/<run_id>`` for the fresh copy.  Fetch into a
                # sibling first, then replace the VM directory only after a
                # complete successful transfer.
                if staging.exists():
                    shutil.rmtree(staging)
                subprocess.run(
                    ["scp", *self.connection_options(), "-r", f"{vm.ssh_host}:{vm.remote_root}/{self.run_id}", str(staging)],
                    text=True, capture_output=True, check=True, timeout=self.collection_timeout_seconds,
                )
                if destination.exists():
                    shutil.rmtree(destination)
                staging.replace(destination)
                self.log.write("collected", detail=f"{vm.name} -> {destination}")
                self.progress(f"collected artifacts from {vm.name}")
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
                self.log.write("collection_failed", detail=f"{vm.name}: {error}")
                self.progress(f"artifact collection failed for {vm.name}; continuing")

    def run(self) -> None:
        if self.execution_mode == "remote_agents":
            self.run_remote_agents()
            return
        try:
            self.progress("preflight: cleaning prior tracked processes and validating hosts")
            self.preflight()
            self.progress(f"launching {len(self.replicas)} replicas across {len(self.vms)} regions")
            for replica in self.replicas.values():
                self.start(replica)
            self.wait_for_membership_stabilization()
        except BaseException:
            self.shutdown_all()
            self.collect()
            self.log.close()
            raise
        if self.dry_run:
            event_index = 0
            while event_index < len(self.events):
                event_time = self.events[event_index].at_seconds
                due_events = []
                while event_index < len(self.events) and self.events[event_index].at_seconds == event_time:
                    event = self.events[event_index]
                    replica = self.replicas[(event.vm, event.slot)]
                    self.log.write("scheduled_event_validated", replica, detail=f"t={event.at_seconds}s {event.action}")
                    due_events.append(event)
                    event_index += 1
                self.dispatch_due_events(due_events)
            self.log.close()
            return
        worker = threading.Thread(target=self.workload, name="laptop-workload", daemon=True)
        worker.start()
        started = time.monotonic()
        event_index = 0
        next_snapshot = 0.0
        next_progress = 60.0
        completed = False
        self.progress(f"workload started; scheduled duration is {self.duration:.0f}s")
        try:
            while (elapsed := time.monotonic() - started) < self.duration:
                while event_index < len(self.events) and self.events[event_index].at_seconds <= elapsed:
                    event_time = self.events[event_index].at_seconds
                    due_events = []
                    while event_index < len(self.events) and self.events[event_index].at_seconds == event_time:
                        due_events.append(self.events[event_index])
                        event_index += 1
                    self.dispatch_due_events(due_events)
                if elapsed >= next_snapshot:
                    self.snapshot_states()
                    next_snapshot += self.snapshot_interval
                if elapsed >= next_progress:
                    self.progress(f"workload progress: {elapsed:.0f}s / {self.duration:.0f}s; {sum(replica.live for replica in self.replicas.values())} replicas live")
                    next_progress += 60.0
                time.sleep(0.2)
            completed = True
        finally:
            self.workload_stop.set()
            worker.join(timeout=15)
            if self.final_convergence_seconds > 0:
                self.log.write("final_convergence_wait_started", detail=str(self.final_convergence_seconds))
                self.progress(f"workload complete; waiting {self.final_convergence_seconds:.0f}s for final convergence")
                time.sleep(self.final_convergence_seconds)
            self.snapshot_states()
            self.progress("final snapshot complete; shutting down replicas")
            self.shutdown_all()
            self.collect()
            self.log.close()
            self.progress(
                "experiment controller completed"
                if completed
                else "experiment controller failed; cleanup completed"
            )


def required(mapping: dict[str, Any], name: str) -> Any:
    if name not in mapping or mapping[name] in (None, ""):
        raise ValueError(f"missing required configuration field: {name}")
    return mapping[name]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run GRESSE's six-region replica churn experiment.")
    parser.add_argument("--topology", type=Path, required=True, help="JSON topology and event schedule")
    parser.add_argument("--result-dir", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true", help="validate and print remote control actions without SSH or HTTP")
    parser.add_argument("--collect-run-id", help="recover artifacts for an existing remote run without starting processes")
    parser.add_argument("--resume", action="store_true", help="resume monitoring and artifact collection for an interrupted remote-agent run")
    parser.add_argument("--preflight-only", action="store_true", help="clean tracked replicas and validate remote hosts without starting the experiment")
    parser.add_argument("--deploy-binary", type=Path, help="release binary to copy to every VM before preflight")
    parser.add_argument("--expected-binary-sha256", help="override expected_binary_sha256 from the topology")
    args = parser.parse_args()
    with args.topology.open() as handle:
        config = json.load(handle)
    if args.expected_binary_sha256:
        config["expected_binary_sha256"] = args.expected_binary_sha256
    if args.collect_run_id and (args.preflight_only or args.deploy_binary or args.resume):
        raise SystemExit("--collect-run-id cannot be combined with preflight, binary deployment, or --resume")
    if args.resume and (args.preflight_only or args.deploy_binary or args.dry_run or args.expected_binary_sha256):
        raise SystemExit("--resume cannot be combined with preflight, binary deployment, dry-run, or binary-hash override")
    if args.deploy_binary and not args.preflight_only:
        raise SystemExit("--deploy-binary requires --preflight-only")
    if args.result_dir.exists() and not args.resume:
        raise SystemExit(f"result directory already exists: {args.result_dir}")
    if not args.resume:
        args.result_dir.mkdir(parents=True)
        shutil.copy2(args.topology, args.result_dir / "topology.json")
    controller = Controller(config, args.result_dir, args.dry_run)
    if args.collect_run_id:
        controller.run_id = args.collect_run_id
    if not args.resume:
        (args.result_dir / "manifest.json").write_text(json.dumps({"run_id": controller.run_id, "config": config, "collect_only": bool(args.collect_run_id)}, indent=2))
    if args.resume:
        controller.resume_remote_agents()
        return 0
    if args.collect_run_id:
        controller.collect()
        controller.log.close()
        return 0
    if args.preflight_only:
        try:
            controller.preflight(args.deploy_binary)
        finally:
            controller.log.close()
        return 0
    controller.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
