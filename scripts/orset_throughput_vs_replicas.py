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
    plot_svg = result_root / "throughput_vs_replicas.svg"
    write_svg_plot(plot_svg, run_summaries)
    print(summary_csv)
    print(plot_svg)
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


def write_svg_plot(path: Path, rows: list[dict[str, object]]) -> None:
    width = 900
    height = 520
    margin_left = 80
    margin_right = 30
    margin_top = 40
    margin_bottom = 70
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom

    x_values = [int(row["replicas"]) for row in rows]
    combined_values = [float(row["combined_stable_throughput_ops_per_sec"]) for row in rows]
    average_values = [float(row["average_replica_stable_throughput_ops_per_sec"]) for row in rows]
    y_max = max(combined_values + average_values + [1.0]) * 1.1

    def x_pos(value: int) -> float:
        if len(x_values) == 1:
            return margin_left + plot_width / 2
        fraction = (value - min(x_values)) / (max(x_values) - min(x_values))
        return margin_left + fraction * plot_width

    def y_pos(value: float) -> float:
        return margin_top + plot_height - (value / y_max) * plot_height

    combined_points = " ".join(f"{x_pos(x):.2f},{y_pos(y):.2f}" for x, y in zip(x_values, combined_values))
    average_points = " ".join(f"{x_pos(x):.2f},{y_pos(y):.2f}" for x, y in zip(x_values, average_values))

    y_ticks = 5
    grid_lines = []
    for tick in range(y_ticks + 1):
        value = y_max * tick / y_ticks
        y = y_pos(value)
        grid_lines.append(
            f"<line x1='{margin_left}' y1='{y:.2f}' x2='{width - margin_right}' y2='{y:.2f}' "
            "stroke='#d9e0e6' stroke-width='1' />"
        )
        grid_lines.append(
            f"<text x='{margin_left - 10}' y='{y + 5:.2f}' text-anchor='end' "
            "font-family='Helvetica, Arial, sans-serif' font-size='12' fill='#334155'>"
            f"{value:.1f}</text>"
        )

    x_labels = []
    for x in x_values:
        xpos = x_pos(x)
        x_labels.append(
            f"<text x='{xpos:.2f}' y='{height - margin_bottom + 25}' text-anchor='middle' "
            "font-family='Helvetica, Arial, sans-serif' font-size='12' fill='#334155'>"
            f"{x}</text>"
        )

    svg = f"""<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}' viewBox='0 0 {width} {height}'>
<rect width='100%' height='100%' fill='#f8fafc' />
<text x='{width / 2}' y='26' text-anchor='middle' font-family='Helvetica, Arial, sans-serif' font-size='20' fill='#0f172a'>
Throughput vs Replica Count
</text>
<text x='{width / 2}' y='{height - 16}' text-anchor='middle' font-family='Helvetica, Arial, sans-serif' font-size='14' fill='#334155'>
Replica count
</text>
<text x='22' y='{height / 2}' transform='rotate(-90 22 {height / 2})' text-anchor='middle' font-family='Helvetica, Arial, sans-serif' font-size='14' fill='#334155'>
Stable throughput (ops/sec)
</text>
{''.join(grid_lines)}
<line x1='{margin_left}' y1='{margin_top}' x2='{margin_left}' y2='{height - margin_bottom}' stroke='#475569' stroke-width='2' />
<line x1='{margin_left}' y1='{height - margin_bottom}' x2='{width - margin_right}' y2='{height - margin_bottom}' stroke='#475569' stroke-width='2' />
<polyline fill='none' stroke='#0f766e' stroke-width='3' points='{combined_points}' />
<polyline fill='none' stroke='#b45309' stroke-width='3' points='{average_points}' />
{''.join(f"<circle cx='{x_pos(x):.2f}' cy='{y_pos(y):.2f}' r='4' fill='#0f766e' />" for x, y in zip(x_values, combined_values))}
{''.join(f"<circle cx='{x_pos(x):.2f}' cy='{y_pos(y):.2f}' r='4' fill='#b45309' />" for x, y in zip(x_values, average_values))}
{''.join(x_labels)}
<rect x='{width - 255}' y='50' width='220' height='58' rx='8' fill='white' stroke='#cbd5e1' />
<line x1='{width - 240}' y1='72' x2='{width - 205}' y2='72' stroke='#0f766e' stroke-width='3' />
<text x='{width - 195}' y='77' font-family='Helvetica, Arial, sans-serif' font-size='13' fill='#0f172a'>
Combined stable throughput
</text>
<line x1='{width - 240}' y1='94' x2='{width - 205}' y2='94' stroke='#b45309' stroke-width='3' />
<text x='{width - 195}' y='99' font-family='Helvetica, Arial, sans-serif' font-size='13' fill='#0f172a'>
Average per-replica throughput
</text>
</svg>
"""
    path.write_text(svg)


if __name__ == "__main__":
    sys.exit(main())
