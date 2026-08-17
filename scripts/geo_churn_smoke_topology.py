#!/usr/bin/env python3
"""Derive a short concurrent-churn validation topology from a full topology."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a short GRESSE concurrent-churn smoke topology.")
    parser.add_argument("--source", type=Path, required=True, help="full experiment topology")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    config = json.loads(args.source.read_text())
    names = {vm["name"] for vm in config["vms"]}
    required = {"stockholm", "frankfurt", "virginia", "oregon"}
    missing = sorted(required - names)
    if missing:
        raise SystemExit(f"smoke topology requires VM names {sorted(required)}; missing {missing}")

    config["run_label"] = f"{config.get('run_label', 'geo-churn')}-smoke"
    config["duration_seconds"] = 300
    config["final_convergence_seconds"] = 60
    config["workload_rate_per_replica"] = 0.02
    config["events"] = [
        # Two simultaneous hard crashes.
        {"at_seconds": 45, "action": "crash", "vm": "stockholm", "slot": 0},
        {"at_seconds": 45, "action": "crash", "vm": "virginia", "slot": 2},
        # Two simultaneous replacements, each with a fresh replica PID.
        {"at_seconds": 90, "action": "spawn", "vm": "stockholm", "slot": 0},
        {"at_seconds": 90, "action": "spawn", "vm": "virginia", "slot": 2},
        # Two simultaneous graceful shutdowns.
        {"at_seconds": 135, "action": "graceful_stop", "vm": "frankfurt", "slot": 1},
        {"at_seconds": 135, "action": "graceful_stop", "vm": "oregon", "slot": 3},
        # Two simultaneous replacements after graceful departure.
        {"at_seconds": 180, "action": "spawn", "vm": "frankfurt", "slot": 1},
        {"at_seconds": 180, "action": "spawn", "vm": "oregon", "slot": 3},
    ]
    args.output.write_text(json.dumps(config, indent=2) + "\n")
    print(f"Wrote smoke topology: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
