#!/usr/bin/env python3
"""Summarize the laptop-controller trace from geo_churn_experiment.py."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize a GRESSE geo churn result directory.")
    parser.add_argument("result_dir", type=Path)
    args = parser.parse_args()
    trace = args.result_dir / "controller_events.csv"
    with trace.open() as handle:
        rows = list(csv.DictReader(handle))

    requests = [row for row in rows if row["event"] == "client_request"]
    availability = {}
    for expected_live in ("True", "False"):
        sample = [row for row in requests if row["expected_live"] == expected_live]
        successes = sum(row["status_code"] == "200" for row in sample)
        availability["expected_live" if expected_live == "True" else "intentionally_down"] = {
            "requests": len(sample), "successful": successes,
            "success_rate": successes / len(sample) if sample else None,
        }

    starts = {}
    first_success = {}
    for row in rows:
        if row["event"] == "spawn_requested":
            starts[(row["replica"], row["pid"])] = int(row["timestamp_us"])
        elif row["event"] == "client_request" and row["status_code"] == "200":
            key = (row["replica"], row["pid"])
            first_success.setdefault(key, int(row["timestamp_us"]))
    recovery_us = sorted(first_success[key] - started for key, started in starts.items() if key in first_success)

    snapshots = []
    for row in rows:
        if row["event"] == "state_snapshot_summary":
            try:
                groups = json.loads(row["detail"])
            except json.JSONDecodeError:
                continue
            snapshots.append({"timestamp_us": int(row["timestamp_us"]), "digest_groups": len(groups), "replicas": sum(len(group) for group in groups.values())})

    lifecycle = defaultdict(int)
    for row in rows:
        if row["event"] in {"crash_requested", "graceful_stop_requested", "spawn_requested"}:
            lifecycle[row["event"]] += 1
    summary = {
        "availability": availability,
        "lifecycle_events": dict(lifecycle),
        "recovery_to_first_success_us": {
            "samples": len(recovery_us),
            "min": recovery_us[0] if recovery_us else None,
            "median": recovery_us[len(recovery_us) // 2] if recovery_us else None,
            "max": recovery_us[-1] if recovery_us else None,
        },
        "state_snapshots": snapshots,
        "final_snapshot_converged": bool(snapshots and snapshots[-1]["digest_groups"] == 1),
        "note": "GC completion and departed-membership cleanup are in the collected per-replica CSV traces, correlated by gc_marker.",
    }
    output = args.result_dir / "summary.json"
    output.write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
