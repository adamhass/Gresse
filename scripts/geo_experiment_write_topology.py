#!/usr/bin/env python3
"""Write a geo-churn topology from provisioned VM records on stdin."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topology", type=Path, required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--s3-region", required=True)
    parser.add_argument("--ssh-user", required=True)
    parser.add_argument("--binary-path")
    parser.add_argument("--identity-file")
    args = parser.parse_args()
    records = [line.rstrip("\n").split("\t") for line in sys.stdin if line.strip()]
    if len(records) != 5 or any(len(record) != 3 for record in records):
        raise SystemExit("expected five tab-separated name, region, public-IP records")
    binary_path = args.binary_path or f"/home/{args.ssh_user}/gresse/orset_bench_replica"
    remote_root = f"/home/{args.ssh_user}/gresse-runs"
    vms = [
        {
            "name": name,
            "region": region,
            "ssh_host": f"{args.ssh_user}@{public_ip}",
            "bind_ip": "0.0.0.0",
            "advertise_ip": public_ip,
            "client_host": public_ip,
            "binary_path": binary_path,
            "remote_root": remote_root,
        }
        for name, region, public_ip in records
    ]
    # At every odd minute, gracefully stop three replicas and hard-crash
    # three others. At the following even minute, start three fresh replicas
    # and recover the three crashed replicas. Rotate across all VM/slot pairs
    # so the five-region topology experiences balanced churn.
    duration_seconds = 3 * 60 * 60
    events = []
    slots = [(name, slot) for name, _, _ in records for slot in range(5)]
    for wave, stop_time in enumerate(range(60, duration_seconds, 120)):
        selected = [slots[(wave * 6 + offset) % len(slots)] for offset in range(6)]
        graceful_stops, hard_crashes = selected[:3], selected[3:]
        start_time = stop_time + 60
        events.extend({"at_seconds": stop_time, "action": "graceful_stop", "vm": vm, "slot": slot} for vm, slot in graceful_stops)
        events.extend({"at_seconds": stop_time, "action": "crash", "vm": vm, "slot": slot} for vm, slot in hard_crashes)
        events.extend({"at_seconds": start_time, "action": "spawn", "vm": vm, "slot": slot} for vm, slot in [*graceful_stops, *hard_crashes])
    config = {
        "run_label": "acm-sec-geo-churn",
        "execution_mode": "remote_agents",
        "agent_start_delay_seconds": 45,
        "agent_monitor_interval_seconds": 15,
        "agent_arm_timeout_seconds": 180,
        "agent_completion_grace_seconds": 120,
        "duration_seconds": duration_seconds,
        "bucket": args.bucket,
        "s3_region": args.s3_region,
        "replicas_per_vm": 5,
        "sync_interval_ms": 1000,
        "discovery_interval_ms": 5000,
        "gc_interval_ms": 60000,
        "workload_rate_per_server": 10.0,
        "workload_value_domain_size": 1_000,
        "snapshot_interval_seconds": 30,
        "final_convergence_seconds": 120,
        "startup_timeout_seconds": 60,
        "process_stop_timeout_seconds": 20,
        "membership_settle_seconds": 30,
        "max_clock_skew_ms": 5000,
        "durable_recovery": True,
        "remote_shutdown_grace_seconds": 20,
        "remote_deadline_slack_seconds": 600,
        "ssh_timeout_seconds": 30,
        "collection_timeout_seconds": 300,
        "ssh_options": (["-i", args.identity_file, "-o", "IdentitiesOnly=yes"] if args.identity_file else []),
        "vms": vms,
        "events": events,
    }
    args.topology.write_text(json.dumps(config, indent=2) + "\n")
    print(f"Wrote {args.topology}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
