#!/usr/bin/env python3
"""Per-VM lifecycle agent for the geo-churn experiment.

The laptop prepares one JSON specification per VM and then starts this program
there.  All lifecycle decisions, bootstrap checks, and workload requests are
made locally on the VM; SSH is deliberately not part of the experiment's
critical path.
"""

from __future__ import annotations

import argparse
import csv
import http.client
import json
import os
import random
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any


CSV_FIELDS = [
    "timestamp_us", "event", "replica", "region", "slot", "pid", "detail",
    "status_code", "latency_us", "operation", "value", "expected_live", "state_digest",
    "clock_offset_us", "rtt_us", "binary_sha256",
]


@dataclass
class Replica:
    slot: int
    pid: int
    generation: int = 0
    live: bool = False
    bootstrapping: bool = False
    process: subprocess.Popen[bytes] | None = None
    remote_dir: Path | None = None

    @property
    def name(self) -> str:
        return f"{self.vm_name}-slot{self.slot}-g{self.generation}"

    @property
    def http_port(self) -> int:
        return self.http_port_base + self.slot

    @property
    def internal_port(self) -> int:
        return self.internal_port_base + self.slot

    # Values filled in by Agent after construction to keep the state compact.
    vm_name: str = ""
    http_port_base: int = 0
    internal_port_base: int = 0


class EventLog:
    def __init__(self, path: Path, region: str) -> None:
        self.region = region
        self.handle = path.open("w", newline="")
        self.writer = csv.DictWriter(self.handle, fieldnames=CSV_FIELDS)
        self.writer.writeheader()
        self.lock = threading.Lock()

    def write(self, event: str, replica: Replica | None = None, **values: Any) -> None:
        row = {field: "" for field in CSV_FIELDS}
        row.update(timestamp_us=time.time_ns() // 1_000, event=event)
        if replica is not None:
            row.update(replica=replica.name, region=self.region, slot=replica.slot, pid=replica.pid)
        row.update({key: str(value) for key, value in values.items() if value is not None})
        with self.lock:
            self.writer.writerow(row)
            self.handle.flush()

    def close(self) -> None:
        self.handle.close()


class Agent:
    process_tag_env = "GRESSE_EXPERIMENT_TAG"
    process_tag_value = "geo-churn"

    def __init__(self, spec: dict[str, Any]) -> None:
        self.spec = spec
        self.settings = required(spec, "settings")
        self.vm = required(spec, "vm")
        self.vm_name = required(self.vm, "name")
        self.run_id = required(spec, "run_id")
        self.start_epoch: float | None = None
        self.duration = float(required(self.settings, "duration_seconds"))
        self.final_convergence_seconds = float(self.settings.get("final_convergence_seconds", 120))
        self.deadline_epoch: float | None = None
        self.root = Path(required(self.vm, "remote_root")) / self.run_id
        self.root.mkdir(parents=True, exist_ok=True)
        self.status_path = self.root / "agent_status.json"
        self.start_signal_path = self.root / "agent_start_epoch"
        self.abort_path = self.root / "agent_abort"
        self.log = EventLog(self.root / "agent_events.csv", required(self.vm, "region"))
        self.binary_path = required(self.vm, "binary_path")
        self.bind_ip = self.vm.get("bind_ip", required(self.vm, "advertise_ip"))
        self.advertise_ip = required(self.vm, "advertise_ip")
        self.http_port_base = int(self.vm.get("http_port_base", 18080))
        self.internal_port_base = int(self.vm.get("internal_port_base", 19080))
        self.replica_count = int(self.settings.get("replicas_per_vm", 5))
        self.startup_timeout_seconds = float(self.settings.get("startup_timeout_seconds", 30))
        self.startup_max_attempts = int(self.settings.get("startup_max_attempts", 3))
        self.startup_retry_delay_seconds = float(self.settings.get("startup_retry_delay_seconds", 1))
        self.stop_timeout_seconds = float(self.settings.get("process_stop_timeout_seconds", 20))
        self.workload_rate_per_server = float(self.settings.get("workload_rate_per_server", 0))
        # A small, fixed key domain reaches a steady expected cardinality
        # quickly.  With equal-probability inserts and removes, its expected
        # occupancy is half this size.
        self.workload_value_domain_size = int(self.settings.get("workload_value_domain_size", 1_000))
        if self.workload_value_domain_size < 1:
            raise ValueError("workload_value_domain_size must be positive")
        self.workload_max_in_flight = int(self.settings.get("workload_max_in_flight", 64))
        self.workload_max_in_flight_per_replica = int(self.settings.get("workload_max_in_flight_per_replica", 8))
        self.snapshot_interval = float(self.settings.get("snapshot_interval_seconds", 30))
        self.membership_settle_seconds = float(self.settings.get("membership_settle_seconds", 0))
        self.durable_recovery = bool(self.settings.get("durable_recovery", True))
        self.pid_base = int(required(spec, "pid_base"))
        self.events = sorted(required(spec, "events"), key=lambda event: float(event["at_seconds"]))
        self.random = random.Random(int(self.settings.get("seed", 0)) + self.pid_base)
        self.workload_stop = threading.Event()
        self.workload_next_slot = 0
        self.replicas = {
            slot: Replica(
                slot=slot,
                pid=self.pid_for(slot, 0),
                vm_name=self.vm_name,
                http_port_base=self.http_port_base,
                internal_port_base=self.internal_port_base,
            )
            for slot in range(self.replica_count)
        }

    def pid_for(self, slot: int, generation: int) -> int:
        return self.pid_base + slot * 1_000 + generation

    def write_status(self, state: str, error: str = "") -> None:
        payload = {
            "state": state,
            "error": error,
            "vm": self.vm_name,
            "run_id": self.run_id,
            "start_epoch": self.start_epoch,
            "updated_epoch": time.time(),
            "live_replicas": [replica.name for replica in self.replicas.values() if replica.live],
            "completed_events": 0 if self.start_epoch is None else sum(
                1 for event in self.events if float(event["at_seconds"]) <= time.time() - self.start_epoch
            ),
            "total_events": len(self.events),
        }
        temporary = self.status_path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(payload, sort_keys=True) + "\n")
        temporary.replace(self.status_path)

    def replica_dir(self, replica: Replica) -> Path:
        return self.root / replica.name

    def durability_path(self, replica: Replica) -> Path:
        return self.root / "durability" / f"{self.vm_name}-slot{replica.slot}.journal"

    def start(self, replica: Replica) -> None:
        if replica.live:
            raise RuntimeError(f"cannot start live replica {replica.name}")
        failures: list[str] = []
        replica.bootstrapping = True
        try:
            for attempt in range(1, self.startup_max_attempts + 1):
                try:
                    self.start_attempt(replica, attempt)
                    return
                except Exception as error:
                    detail = f"attempt {attempt}/{self.startup_max_attempts}: {type(error).__name__}: {error}"
                    failures.append(detail)
                    self.log.write("bootstrap_attempt_failed", replica, detail=detail)
                    self.stop_process(replica, force=True)
                    if attempt < self.startup_max_attempts:
                        time.sleep(self.startup_retry_delay_seconds)
            raise TimeoutError("replica failed to initialize after %d attempts: %s" % (self.startup_max_attempts, "; ".join(failures)))
        finally:
            replica.bootstrapping = False

    def start_attempt(self, replica: Replica, attempt: int) -> None:
        predecessor_pid = replica.pid
        replica.generation += 1
        replica.pid = self.pid_for(replica.slot, replica.generation)
        replica.remote_dir = self.replica_dir(replica)
        replica.remote_dir.mkdir(parents=True, exist_ok=True)
        self.durability_path(replica).parent.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        env.update({
            "GRESSE_BENCH_PID": str(replica.pid),
            "GRESSE_ADDR": self.bind_ip,
            "GRESSE_ADVERTISE_ADDR": self.advertise_ip,
            "GRESSE_HTTP_PORT": str(replica.http_port),
            "GRESSE_INTERNAL_PORT": str(replica.internal_port),
            "GRESSE_RESULT_DIR_PATH": str(replica.remote_dir),
            "GRESSE_SYNC_INTERVAL_MS": str(self.settings.get("sync_interval_ms", 1000)),
            "GRESSE_OBJECT_STORAGE_DISCOVERY_INTERVAL_MS": str(self.settings.get("discovery_interval_ms", 5000)),
            "GRESSE_GC_INTERVAL_MS": str(self.settings.get("gc_interval_ms", 60000)),
            "GRESSE_OBJECT_STORAGE_REGION": str(self.settings.get("s3_region", "eu-north-1")),
            "GRESSE_OBJECT_STORAGE_BUCKET": required(self.settings, "bucket"),
            "GRESSE_PERSISTENT_REPLICA_PATH": f"{self.run_id}/persistent.json",
            "GRESSE_MEMBERSHIP_DIRECTORY_PATH": f"{self.run_id}/membership",
            "GRESSE_RECOVERED_PREDECESSOR_PID": str(predecessor_pid),
            self.process_tag_env: self.process_tag_value,
        })
        if self.durable_recovery:
            env["GRESSE_DURABLE"] = "true"
            env["GRESSE_DURABILITY_PATH"] = str(self.durability_path(replica))
        self.log.write("bootstrap_attempt_started", replica, detail=f"attempt {attempt}/{self.startup_max_attempts}")
        with (replica.remote_dir / "stdout.log").open("wb") as stdout, (replica.remote_dir / "stderr.log").open("wb") as stderr:
            replica.process = subprocess.Popen(
                [self.binary_path], stdout=stdout, stderr=stderr, stdin=subprocess.DEVNULL,
                env=env, start_new_session=True,
            )
        (replica.remote_dir / "os.pid").write_text(f"{replica.process.pid}\n")
        replica.live = True
        self.log.write("spawn_requested", replica, detail=str(replica.remote_dir))
        self.wait_until_bootstrapped(replica)
        self.log.write("bootstrap_ready", replica)

    def wait_until_bootstrapped(self, replica: Replica) -> None:
        assert replica.process is not None and replica.remote_dir is not None
        metrics_path = replica.remote_dir / f"server_{replica.pid}.csv"
        deadline = time.monotonic() + self.startup_timeout_seconds
        while time.monotonic() < deadline:
            if replica.process.poll() is not None:
                raise RuntimeError(f"replica process exited with status {replica.process.returncode}")
            try:
                if metrics_path.is_file() and "server,replica_init,completed," in metrics_path.read_text(errors="replace"):
                    return
            except OSError:
                pass
            time.sleep(0.1)
        raise TimeoutError(f"replica did not complete local bootstrap: {replica.name}")

    def stop_process(self, replica: Replica, force: bool = False) -> None:
        process = replica.process
        if process is None:
            replica.live = False
            return
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGKILL if force else signal.SIGTERM)
            except ProcessLookupError:
                pass
            deadline = time.monotonic() + self.stop_timeout_seconds
            while process.poll() is None and time.monotonic() < deadline:
                time.sleep(0.1)
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
        replica.process = None
        replica.live = False

    def ensure_live_replicas_healthy(self) -> None:
        """Fail the agent as soon as a non-scheduled replica exit is observed."""
        for replica in self.replicas.values():
            if not replica.live:
                continue
            process = replica.process
            if process is None:
                raise RuntimeError(f"live replica has no process handle: {replica.name}")
            returncode = process.poll()
            if returncode is None:
                continue
            replica.live = False
            replica.process = None
            self.log.write("replica_process_exited", replica, detail=f"unexpected exit status {returncode}")
            raise RuntimeError(f"replica exited unexpectedly: {replica.name}: status {returncode}")

    def abort_active_run(self) -> None:
        """Stop local processes promptly after the coordinating controller aborts."""
        self.workload_stop.set()
        for replica in self.replicas.values():
            if replica.live:
                self.log.write("agent_abort_requested", replica)
                self.stop_process(replica, force=True)
        self.log.write("agent_aborted", detail="abort marker received during run")
        self.write_status("aborted")

    def stop(self, replica: Replica, graceful: bool, retire_durability: bool = False) -> None:
        if not replica.live:
            self.log.write("lifecycle_ignored_not_live", replica, detail="graceful_stop" if graceful else "crash")
            return
        self.log.write("graceful_stop_requested" if graceful else "crash_requested", replica)
        self.stop_process(replica, force=not graceful)
        self.log.write("process_stopped", replica)
        if retire_durability and self.durable_recovery:
            try:
                self.durability_path(replica).unlink()
            except FileNotFoundError:
                pass
            self.log.write("durability_journal_retired", replica)

    def execute_event(self, event: dict[str, Any]) -> None:
        replica = self.replicas[int(event["slot"])]
        action = required(event, "action")
        self.log.write("scheduled_event_started", replica, detail=f"t={float(event['at_seconds']):.3f}s {action}")
        if action == "crash":
            self.stop(replica, graceful=False)
        elif action == "graceful_stop":
            self.stop(replica, graceful=True, retire_durability=True)
        elif action == "spawn":
            self.start(replica)
        else:
            raise ValueError(f"unknown action: {action}")
        self.log.write("scheduled_event_completed", replica, detail=f"t={float(event['at_seconds']):.3f}s {action}")

    def request(self, replica: Replica) -> None:
        started = time.time_ns() // 1_000
        operation = "Remove" if self.random.random() < 0.5 else "Insert"
        value = self.random.randrange(self.workload_value_domain_size)
        status: int | None = None
        detail = ""
        try:
            connection = http.client.HTTPConnection("127.0.0.1", replica.http_port, timeout=5)
            connection.request("POST", "/", body=json.dumps({"type": "Mutation", "params": {operation: value}}), headers={"Content-Type": "application/json"})
            response = connection.getresponse()
            response.read()
            status = response.status
            connection.close()
        except Exception as error:
            detail = f"{type(error).__name__}: {error}"
        self.log.write("client_request", replica, status_code=status, latency_us=(time.time_ns() // 1_000) - started, operation=operation, value=value, expected_live=replica.live, detail=detail)

    def workload(self) -> None:
        if self.workload_rate_per_server <= 0:
            return
        period = 1.0 / self.workload_rate_per_server
        next_dispatch = time.monotonic()
        with ThreadPoolExecutor(max_workers=self.workload_max_in_flight) as pool:
            in_flight: dict[Any, int] = {}
            while not self.workload_stop.is_set():
                for future, slot in list(in_flight.items()):
                    if future.done():
                        del in_flight[future]
                eligible = [
                    replica
                    for replica in self.replicas.values()
                    if replica.live
                    and not replica.bootstrapping
                    and sum(slot == replica.slot for slot in in_flight.values())
                    < self.workload_max_in_flight_per_replica
                ]
                if eligible and len(in_flight) < self.workload_max_in_flight:
                    replica = eligible[self.workload_next_slot % len(eligible)]
                    self.workload_next_slot += 1
                    in_flight[pool.submit(self.request, replica)] = replica.slot
                next_dispatch += period
                self.workload_stop.wait(max(0, next_dispatch - time.monotonic()))
                if next_dispatch < time.monotonic() - period:
                    next_dispatch = time.monotonic()

    def snapshot_states(self) -> None:
        for replica in self.replicas.values():
            if not replica.live:
                continue
            try:
                connection = http.client.HTTPConnection("127.0.0.1", replica.http_port, timeout=5)
                connection.request("POST", "/", body=json.dumps({"type": "Query", "params": "Elements"}), headers={"Content-Type": "application/json"})
                response = connection.getresponse()
                response.read()
                connection.close()
                self.log.write("state_snapshot", replica, status_code=response.status)
            except Exception as error:
                self.log.write("state_snapshot_failed", replica, detail=f"{type(error).__name__}: {error}")

    def wait_for_start_signal(self) -> bool:
        """Wait until the laptop releases this already-staged agent."""
        while True:
            if self.abort_path.exists():
                self.log.write("agent_start_cancelled", detail="abort marker present before start")
                self.write_status("cancelled")
                return False
            try:
                start_epoch = float(self.start_signal_path.read_text().strip())
                if start_epoch <= time.time():
                    raise ValueError("start time is already in the past")
                self.start_epoch = start_epoch
                self.deadline_epoch = start_epoch + self.duration + self.final_convergence_seconds
                self.log.write("agent_start_received", detail=str(start_epoch))
                return True
            except FileNotFoundError:
                pass
            except ValueError as error:
                self.log.write("agent_start_invalid", detail=str(error))
                self.write_status("failed", f"invalid start signal: {error}")
                return False
            self.write_status("armed")
            time.sleep(0.25)

    def run(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.write_status("armed")
        if not self.wait_for_start_signal():
            self.log.close()
            return
        assert self.start_epoch is not None
        while time.time() < self.start_epoch:
            if self.abort_path.exists():
                self.log.write("agent_start_cancelled", detail="abort marker present before start")
                self.write_status("cancelled")
                self.log.close()
                return
            self.write_status("armed")
            time.sleep(min(1, self.start_epoch - time.time()))
        try:
            self.write_status("initializing")
            for replica in self.replicas.values():
                self.start(replica)
                self.ensure_live_replicas_healthy()
            if self.membership_settle_seconds:
                settle_deadline = time.monotonic() + self.membership_settle_seconds
                while time.monotonic() < settle_deadline:
                    if self.abort_path.exists():
                        self.abort_active_run()
                        return
                    self.ensure_live_replicas_healthy()
                    time.sleep(min(0.1, settle_deadline - time.monotonic()))
            self.write_status("running")
            worker = threading.Thread(target=self.workload, name="agent-workload", daemon=True)
            worker.start()
            event_index = 0
            next_snapshot = 0.0
            while time.time() - self.start_epoch < self.duration:
                if self.abort_path.exists():
                    self.abort_active_run()
                    return
                self.ensure_live_replicas_healthy()
                elapsed = time.time() - self.start_epoch
                while event_index < len(self.events) and float(self.events[event_index]["at_seconds"]) <= elapsed:
                    self.execute_event(self.events[event_index])
                    event_index += 1
                    self.write_status("running")
                if elapsed >= next_snapshot:
                    self.snapshot_states()
                    next_snapshot += self.snapshot_interval
                time.sleep(0.1)
            self.workload_stop.set()
            worker.join(timeout=15)
            if self.final_convergence_seconds:
                convergence_deadline = time.monotonic() + self.final_convergence_seconds
                while time.monotonic() < convergence_deadline:
                    if self.abort_path.exists():
                        self.abort_active_run()
                        return
                    self.ensure_live_replicas_healthy()
                    time.sleep(min(0.1, convergence_deadline - time.monotonic()))
            self.snapshot_states()
            for replica in self.replicas.values():
                self.stop(replica, graceful=True)
            self.write_status("completed")
        except BaseException as error:
            self.workload_stop.set()
            for replica in self.replicas.values():
                try:
                    self.stop(replica, graceful=True)
                except Exception:
                    pass
            self.log.write("agent_failed", detail=f"{type(error).__name__}: {error}")
            self.write_status("failed", f"{type(error).__name__}: {error}")
            raise
        finally:
            self.log.close()


def required(mapping: dict[str, Any], name: str) -> Any:
    value = mapping.get(name)
    if value in (None, ""):
        raise ValueError(f"missing required field: {name}")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one geo-churn VM agent")
    parser.add_argument("--spec", type=Path, required=True)
    args = parser.parse_args()
    with args.spec.open() as handle:
        agent = Agent(json.load(handle))
    agent.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
