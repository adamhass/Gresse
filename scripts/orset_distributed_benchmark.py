#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    from orset_benchmark_lib import BenchmarkConfig, BenchmarkRun, parse_lifecycle_events
except ModuleNotFoundError:  # pragma: no cover
    from scripts.orset_benchmark_lib import (
        BenchmarkConfig,
        BenchmarkRun,
        parse_lifecycle_events,
    )


def main() -> int:
    args = parse_args()
    config = BenchmarkConfig(
        replicas=args.replicas,
        duration_seconds=args.duration_seconds,
        result_dir=args.result_dir.resolve(),
        max_store_size_mb=args.max_store_size_mb,
        host=args.host,
        base_http_port=args.base_http_port,
        base_internal_port=args.base_internal_port,
        clients_per_replica=args.clients_per_replica,
        ops_per_second_per_client=args.ops_per_second_per_client,
        remove_probability=args.remove_probability,
        sync_interval_ms=args.sync_interval_ms,
        discovery_interval_ms=args.discovery_interval_ms,
        gc_interval_ms=args.gc_interval_ms,
        network_latency_ms=args.network_latency_ms,
        network_latency_jitter_ms=args.network_latency_jitter_ms,
        startup_stagger_seconds=args.startup_stagger_seconds,
        warmup_seconds=args.warmup_seconds,
        cooldown_seconds=args.cooldown_seconds,
        aws_profile=args.aws_profile,
        region=args.region,
        bucket=args.bucket,
        object_storage_url=args.object_storage_url,
        object_storage_access_key=args.object_storage_access_key,
        object_storage_secret_key=args.object_storage_secret_key,
        object_storage_session_token=args.object_storage_session_token,
        persistent_path=args.persistent_path,
        membership_path=args.membership_path,
        cargo_profile=args.cargo_profile,
        lifecycle_events=parse_lifecycle_events(args.lifecycle_event, args.replicas),
    )
    result = BenchmarkRun(config).run()
    payload = {
        "result_dir": str(result.result_dir),
        "combined_metrics_path": str(result.combined_metrics_path),
        "throughput_summary_path": str(result.throughput_summary_path),
        "per_second_throughput_path": str(result.per_second_throughput_path),
        "stable_window_start_us": result.stable_window_start_us,
        "stable_window_end_us": result.stable_window_end_us,
        "stable_window_seconds": result.stable_window_seconds,
        "combined_stable_throughput_ops_per_sec": result.combined_stable_throughput_ops_per_sec,
        "per_replica": [
            {
                "replica_pid": summary.replica_pid,
                "successful_requests": summary.successful_requests,
                "stable_successful_requests": summary.stable_successful_requests,
                "stable_throughput_ops_per_sec": summary.stable_throughput_ops_per_sec,
            }
            for summary in result.per_replica
        ],
    }
    print(json.dumps(payload, indent=2))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a single distributed OR-Set benchmark.")
    parser.add_argument("--replicas", type=int, required=True)
    parser.add_argument("--duration-seconds", type=float, required=True)
    parser.add_argument("--result-dir", type=Path, required=True)
    parser.add_argument("--max-store-size-mb", type=float, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--base-http-port", type=int, default=18080)
    parser.add_argument("--base-internal-port", type=int, default=19080)
    parser.add_argument("--clients-per-replica", type=int, default=1)
    parser.add_argument("--ops-per-second-per-client", type=float, default=0.0)
    parser.add_argument("--remove-probability", type=float, default=0.5)
    parser.add_argument("--sync-interval-ms", type=int, default=1000)
    parser.add_argument("--discovery-interval-ms", type=int, default=1000)
    parser.add_argument("--gc-interval-ms", type=int, default=60000)
    parser.add_argument("--network-latency-ms", type=int, default=0)
    parser.add_argument("--network-latency-jitter-ms", type=int, default=0)
    parser.add_argument("--startup-stagger-seconds", type=float, default=1.0)
    parser.add_argument("--warmup-seconds", type=float, default=10.0)
    parser.add_argument("--cooldown-seconds", type=float, default=5.0)
    parser.add_argument("--aws-profile", default="gresse")
    parser.add_argument("--region", default="eu-north-1")
    parser.add_argument("--bucket", default="gresse")
    parser.add_argument("--object-storage-url")
    parser.add_argument("--object-storage-access-key")
    parser.add_argument("--object-storage-secret-key")
    parser.add_argument("--object-storage-session-token")
    parser.add_argument("--persistent-path", default="experiment1/persistent.json")
    parser.add_argument("--membership-path", default="experiment1/membership")
    parser.add_argument("--cargo-profile", choices=["debug", "release"], default="release")
    parser.add_argument(
        "--lifecycle-event",
        action="append",
        default=[],
        help="Replica lifecycle event as start:<replica_id>:<seconds> or stop:<replica_id>:<seconds>",
    )
    return parser.parse_args()


if __name__ == "__main__":
    sys.exit(main())
