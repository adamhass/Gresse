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
    if len(records) != 6 or any(len(record) != 3 for record in records):
        raise SystemExit("expected six tab-separated name, region, public-IP records")
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
    # Four waves across the experiment.  Each full batch has exactly ten
    # simultaneous operations: 3 graceful departures, 3 hard crashes, and 4
    # replacement starts.  A preceding crash quartet supplies the four slots
    # to be restarted; a recovery sextet restores all replicas afterward.
    waves = [
        ((("stockholm", 0), ("frankfurt", 1), ("virginia", 2), ("oregon", 3)),
         (("singapore", 4), ("tokyo", 0), ("stockholm", 1)),
         (("frankfurt", 2), ("virginia", 3), ("oregon", 4))),
        ((("singapore", 0), ("tokyo", 1), ("stockholm", 2), ("frankfurt", 3)),
         (("virginia", 4), ("oregon", 0), ("singapore", 1)),
         (("tokyo", 2), ("stockholm", 3), ("frankfurt", 4))),
        ((("virginia", 0), ("oregon", 1), ("singapore", 2), ("tokyo", 3)),
         (("stockholm", 4), ("frankfurt", 0), ("virginia", 1)),
         (("oregon", 2), ("singapore", 3), ("tokyo", 4))),
        ((("stockholm", 0), ("frankfurt", 1), ("virginia", 2), ("oregon", 3)),
         (("singapore", 0), ("tokyo", 1), ("stockholm", 2)),
         (("frankfurt", 3), ("virginia", 4), ("oregon", 0))),
    ]
    events = []
    for wave, (seed_crashes, graceful_stops, hard_crashes) in enumerate(waves):
        seed_time = 180 + wave * 420
        batch_time = seed_time + 120
        recovery_time = batch_time + 120
        events.extend({"at_seconds": seed_time, "action": "crash", "vm": vm, "slot": slot} for vm, slot in seed_crashes)
        events.extend({"at_seconds": batch_time, "action": "spawn", "vm": vm, "slot": slot} for vm, slot in seed_crashes)
        events.extend({"at_seconds": batch_time, "action": "graceful_stop", "vm": vm, "slot": slot} for vm, slot in graceful_stops)
        events.extend({"at_seconds": batch_time, "action": "crash", "vm": vm, "slot": slot} for vm, slot in hard_crashes)
        recovery_slots = [*graceful_stops, *hard_crashes]
        events.extend({"at_seconds": recovery_time, "action": "spawn", "vm": vm, "slot": slot} for vm, slot in recovery_slots)
    config = {
        "run_label": "acm-sec-geo-churn",
        "duration_seconds": 1800,
        "bucket": args.bucket,
        "s3_region": args.s3_region,
        "replicas_per_vm": 5,
        "sync_interval_ms": 1000,
        "discovery_interval_ms": 5000,
        "gc_interval_ms": 60000,
        "workload_rate_per_replica": 0.02,
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
