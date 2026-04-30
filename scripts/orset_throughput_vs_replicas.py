#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

try:
    from orset_benchmark_lib import BenchmarkConfig, BenchmarkRun
except ModuleNotFoundError:  # pragma: no cover
    from scripts.orset_benchmark_lib import BenchmarkConfig, BenchmarkRun

try:
    from plot_orset_throughput_results import generate_plots
except ModuleNotFoundError:  # pragma: no cover
    from scripts.plot_orset_throughput_results import generate_plots


def main() -> int:
    args = parse_args()
    result_root = args.result_root.resolve()
    result_root.mkdir(parents=True, exist_ok=True)

    run_summaries = []
    for replica_count in range(args.min_replicas, args.max_replicas + 1):
        run_dir = result_root / f"replicas_{replica_count}"
        run_prefix = f"{args.experiment_prefix}/replicas_{replica_count}"
        config = BenchmarkConfig(
            replicas=replica_count,
            duration_seconds=args.duration_seconds,
            result_dir=run_dir,
            max_store_size_mb=args.max_store_size_mb,
            seed=args.seed,
            host=args.host,
            base_http_port=args.base_http_port + (replica_count * 100),
            base_internal_port=args.base_internal_port + (replica_count * 100),
            clients_per_replica=args.clients_per_replica,
            ops_per_second_per_client=args.ops_per_second_per_client,
            remove_probability=args.remove_probability,
            sync_interval_ms=1000,
            discovery_interval_ms=args.discovery_interval_ms,
            gc_interval_ms=60000,
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
            persistent_path=f"{run_prefix}/persistent.json",
            membership_path=f"{run_prefix}/membership",
            cargo_profile=args.cargo_profile,
        )
        result = BenchmarkRun(config).run()
        per_replica_values = [summary.stable_throughput_ops_per_sec for summary in result.per_replica]
        avg_per_replica = sum(per_replica_values) / len(per_replica_values)
        run_summaries.append(
            {
                "replicas": replica_count,
                "clients_per_replica": args.clients_per_replica,
                "stable_window_seconds": result.stable_window_seconds,
                "combined_stable_throughput_ops_per_sec": result.combined_stable_throughput_ops_per_sec,
                "average_replica_stable_throughput_ops_per_sec": avg_per_replica,
                "min_replica_stable_throughput_ops_per_sec": min(per_replica_values),
                "max_replica_stable_throughput_ops_per_sec": max(per_replica_values),
                "run_dir": str(run_dir),
            }
        )

    summary_csv = result_root / "throughput_vs_replicas_summary.csv"
    write_summary_csv(summary_csv, run_summaries)
    artifacts = generate_plots(
        result_root=result_root,
        output_dir=result_root / "plots",
    )
    print(summary_csv)
    print(artifacts.summary_plot_pdf)
    print(artifacts.error_bar_plot_pdf)
    for path in artifacts.time_plot_pdfs:
        print(path)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the throughput-vs-replica-count OR-Set benchmark for replica counts 1..8."
    )
    parser.add_argument("--result-root", type=Path, required=True)
    parser.add_argument("--experiment-prefix", default="throughput-vs-replicas")
    parser.add_argument("--min-replicas", type=int, default=1)
    parser.add_argument("--max-replicas", type=int, default=8)
    parser.add_argument("--duration-seconds", type=float, default=90.0)
    parser.add_argument("--warmup-seconds", type=float, default=20.0)
    parser.add_argument("--cooldown-seconds", type=float, default=10.0)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--max-store-size-mb", type=float, required=True)
    parser.add_argument("--clients-per-replica", type=int, default=8)
    parser.add_argument("--ops-per-second-per-client", type=float, default=0.0)
    parser.add_argument("--remove-probability", type=float, default=0.5)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--base-http-port", type=int, default=18080)
    parser.add_argument("--base-internal-port", type=int, default=19080)
    parser.add_argument("--discovery-interval-ms", type=int, default=1000)
    parser.add_argument("--network-latency-ms", type=int, default=0)
    parser.add_argument("--network-latency-jitter-ms", type=int, default=0)
    parser.add_argument("--startup-stagger-seconds", type=float, default=1.0)
    parser.add_argument("--aws-profile", default="gresse")
    parser.add_argument("--region", default="eu-north-1")
    parser.add_argument("--bucket", default="gresse")
    parser.add_argument("--object-storage-url")
    parser.add_argument("--object-storage-access-key")
    parser.add_argument("--object-storage-secret-key")
    parser.add_argument("--object-storage-session-token")
    parser.add_argument("--cargo-profile", choices=["debug", "release"], default="release")
    return parser.parse_args()


def write_summary_csv(path: Path, rows: list[dict[str, object]]) -> None:
    fieldnames = [
        "replicas",
        "clients_per_replica",
        "stable_window_seconds",
        "combined_stable_throughput_ops_per_sec",
        "average_replica_stable_throughput_ops_per_sec",
        "min_replica_stable_throughput_ops_per_sec",
        "max_replica_stable_throughput_ops_per_sec",
        "run_dir",
    ]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    sys.exit(main())
