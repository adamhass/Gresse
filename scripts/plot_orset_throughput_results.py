#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt


POINTS_PER_INCH = 72.0
FIGURE_WIDTH_IN = 252.0 / POINTS_PER_INCH
FIGURE_HEIGHT_IN = 180.0 / POINTS_PER_INCH
DEFAULT_RESULT_ROOT = Path("results/throughput-vs-replicas-minio-server")
DEFAULT_OUTPUT_DIR = Path("results/plots")


@dataclass(frozen=True)
class PlotArtifacts:
    summary_plot_pdf: Path
    error_bar_plot_pdf: Path
    time_plot_pdfs: list[Path]


def configure_matplotlib() -> None:
    plt.rcParams.update(
        {
            "font.family": "Arial",
            "font.size": 8,
            "axes.titlesize": 8,
            "axes.labelsize": 8,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "legend.fontsize": 8,
            "pdf.fonttype": 42,
        }
    )


def save_figure(fig: plt.Figure, pdf_path: Path) -> None:
    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(pdf_path, format="pdf")


def load_summary_rows(result_root: Path) -> list[dict[str, object]]:
    with (result_root / "throughput_vs_replicas_summary.csv").open() as handle:
        reader = csv.DictReader(handle)
        rows = []
        for row in reader:
            rows.append(
                {
                    "replicas": int(row["replicas"]),
                    "clients_per_replica": int(row["clients_per_replica"]),
                    "stable_window_seconds": float(row["stable_window_seconds"]),
                    "combined_stable_throughput_ops_per_sec": float(
                        row["combined_stable_throughput_ops_per_sec"]
                    ),
                    "average_replica_stable_throughput_ops_per_sec": float(
                        row["average_replica_stable_throughput_ops_per_sec"]
                    ),
                    "min_replica_stable_throughput_ops_per_sec": float(
                        row["min_replica_stable_throughput_ops_per_sec"]
                    ),
                    "max_replica_stable_throughput_ops_per_sec": float(
                        row["max_replica_stable_throughput_ops_per_sec"]
                    ),
                    "run_dir": row["run_dir"],
                }
            )
    return sorted(rows, key=lambda row: int(row["replicas"]))


def load_per_second_rows(result_root: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for run_dir in sorted(result_root.glob("replicas_*")):
        replica_count = int(run_dir.name.split("_")[1])
        manifest = json.loads((run_dir / "manifest.json").read_text())
        stable_start_sec = math.ceil(manifest["stable_window_start_us"] / 1_000_000)
        stable_end_sec = math.floor(manifest["stable_window_end_us"] / 1_000_000)

        with (run_dir / "per_second_throughput.csv").open() as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                second_bucket = int(row["second_bucket"])
                if second_bucket < stable_start_sec or second_bucket > stable_end_sec:
                    continue

                replica_counts = {
                    key: int(value)
                    for key, value in row.items()
                    if key.startswith("replica_") and key.endswith("_successful_requests")
                }
                rows.append(
                    {
                        "replica_count": replica_count,
                        "second_bucket": second_bucket,
                        "stable_start_sec": stable_start_sec,
                        "stable_end_sec": stable_end_sec,
                        "time_since_stable_start_s": second_bucket - stable_start_sec,
                        "combined_successful_requests": int(row["combined_successful_requests"]),
                        "per_replica_counts": replica_counts,
                    }
                )
    return rows


def build_window_samples(
    per_second_rows: list[dict[str, object]],
    window_seconds: int,
    step_seconds: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    rows_by_replica_count: dict[int, list[dict[str, object]]] = {}
    for row in per_second_rows:
        rows_by_replica_count.setdefault(int(row["replica_count"]), []).append(row)

    combined_rows: list[dict[str, object]] = []
    per_replica_rows: list[dict[str, object]] = []

    for replica_count, rows in sorted(rows_by_replica_count.items()):
        rows = sorted(rows, key=lambda row: int(row["second_bucket"]))
        rows_by_second = {int(row["second_bucket"]): row for row in rows}
        first_start = int(rows[0]["second_bucket"])
        last_start = int(rows[-1]["second_bucket"]) - window_seconds + 1
        if last_start < first_start:
            continue

        stable_start_sec = int(rows[0]["stable_start_sec"])
        window_index = 0
        for window_start in range(first_start, last_start + 1, step_seconds):
            window_seconds_rows = [
                rows_by_second.get(second)
                for second in range(window_start, window_start + window_seconds)
            ]
            if any(row is None for row in window_seconds_rows):
                continue

            concrete_rows = [row for row in window_seconds_rows if row is not None]
            combined_throughput = sum(
                int(row["combined_successful_requests"]) for row in concrete_rows
            ) / float(window_seconds)
            time_since_start = (window_start - stable_start_sec) + (window_seconds / 2.0)
            combined_rows.append(
                {
                    "replica_count": replica_count,
                    "window_index": window_index,
                    "window_start_sec": window_start,
                    "time_since_stable_start_s": time_since_start,
                    "throughput_ops_per_sec": combined_throughput,
                }
            )

            first_row_counts = concrete_rows[0]["per_replica_counts"]
            for replica_column in sorted(first_row_counts.keys()):
                replica_pid = int(
                    replica_column.removeprefix("replica_").removesuffix("_successful_requests")
                )
                throughput = sum(
                    int(row["per_replica_counts"].get(replica_column, 0))
                    for row in concrete_rows
                ) / float(window_seconds)
                per_replica_rows.append(
                    {
                        "replica_count": replica_count,
                        "replica_pid": replica_pid,
                        "window_index": window_index,
                        "window_start_sec": window_start,
                        "time_since_stable_start_s": time_since_start,
                        "throughput_ops_per_sec": throughput,
                    }
                )
            window_index += 1

    return combined_rows, per_replica_rows


def summarize_samples(
    sample_rows: list[dict[str, object]],
    series_name: str,
) -> list[dict[str, object]]:
    values_by_replica_count: dict[int, list[float]] = {}
    for row in sample_rows:
        values_by_replica_count.setdefault(int(row["replica_count"]), []).append(
            float(row["throughput_ops_per_sec"])
        )

    summary_rows: list[dict[str, object]] = []
    for replica_count in sorted(values_by_replica_count):
        values = values_by_replica_count[replica_count]
        count = len(values)
        mean = statistics.fmean(values)
        stddev = statistics.stdev(values) if count > 1 else 0.0
        ci95 = 1.96 * stddev / math.sqrt(count) if count > 0 else 0.0
        summary_rows.append(
            {
                "series": series_name,
                "replica_count": replica_count,
                "mean": mean,
                "min": min(values),
                "max": max(values),
                "count": count,
                "std": stddev,
                "ci95": ci95,
            }
        )
    return summary_rows


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_sample_csvs(
    output_dir: Path,
    dataset_name: str,
    combined_window_rows: list[dict[str, object]],
    per_replica_window_rows: list[dict[str, object]],
    aggregate_summary_rows: list[dict[str, object]],
) -> None:
    write_csv(
        output_dir / f"{dataset_name}_combined_window_samples.csv",
        [
            "replica_count",
            "window_index",
            "window_start_sec",
            "time_since_stable_start_s",
            "throughput_ops_per_sec",
        ],
        combined_window_rows,
    )
    write_csv(
        output_dir / f"{dataset_name}_per_replica_window_samples.csv",
        [
            "replica_count",
            "replica_pid",
            "window_index",
            "window_start_sec",
            "time_since_stable_start_s",
            "throughput_ops_per_sec",
        ],
        per_replica_window_rows,
    )
    write_csv(
        output_dir / f"{dataset_name}_throughput_with_error_bars.csv",
        ["series", "replica_count", "mean", "min", "max", "count", "std", "ci95"],
        aggregate_summary_rows,
    )


def plot_throughput_vs_replicas_summary(
    output_path: Path,
    summary_rows: list[dict[str, object]],
) -> None:
    configure_matplotlib()
    x_values = [int(row["replicas"]) for row in summary_rows]
    combined_values = [
        float(row["combined_stable_throughput_ops_per_sec"]) for row in summary_rows
    ]
    average_values = [
        float(row["average_replica_stable_throughput_ops_per_sec"]) for row in summary_rows
    ]

    fig, ax = plt.subplots(figsize=(FIGURE_WIDTH_IN, FIGURE_HEIGHT_IN))
    fig.subplots_adjust(left=0.22, right=0.98, bottom=0.23, top=0.97)
    ax.plot(
        x_values,
        combined_values,
        color="#0f766e",
        marker="o",
        linewidth=1.5,
        markersize=3.5,
        label="Combined",
    )
    ax.plot(
        x_values,
        average_values,
        color="#b45309",
        marker="o",
        linewidth=1.5,
        markersize=3.5,
        label="Per replica",
    )
    ax.set_xlabel("Replica count")
    ax.set_ylabel("Stable throughput (ops/sec)")
    ax.set_xticks(x_values)
    ax.grid(axis="y", color="#d9e0e6", linewidth=0.6)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(loc="upper left", frameon=False, ncol=2, handlelength=1.8, columnspacing=1.2)
    save_figure(fig, output_path)
    plt.close(fig)


def plot_throughput_over_time(
    output_path: Path,
    combined_window_rows: list[dict[str, object]],
    replica_count: int,
) -> None:
    configure_matplotlib()
    rows = [
        row
        for row in combined_window_rows
        if int(row["replica_count"]) == replica_count
    ]
    rows.sort(key=lambda row: float(row["time_since_stable_start_s"]))
    x_values = [float(row["time_since_stable_start_s"]) for row in rows]
    y_values = [float(row["throughput_ops_per_sec"]) for row in rows]

    fig, ax = plt.subplots(figsize=(FIGURE_WIDTH_IN, FIGURE_HEIGHT_IN))
    fig.subplots_adjust(left=0.22, right=0.98, bottom=0.23, top=0.97)
    ax.plot(
        x_values,
        y_values,
        color="#0f766e",
        marker="o",
        linewidth=1.5,
        markersize=3.5,
    )
    ax.set_xlabel("Time since stable window start (s)")
    ax.set_ylabel("Throughput (ops/sec)")
    ax.grid(axis="y", color="#d9e0e6", linewidth=0.6)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    save_figure(fig, output_path)
    plt.close(fig)


def plot_error_bars(
    output_path: Path,
    aggregate_summary_rows: list[dict[str, object]],
) -> None:
    configure_matplotlib()
    color_map = {"Combined throughput": "#0f766e", "Per-replica throughput": "#b45309"}
    rows_by_series: dict[str, list[dict[str, object]]] = {}
    for row in aggregate_summary_rows:
        rows_by_series.setdefault(str(row["series"]), []).append(row)

    fig, ax = plt.subplots(figsize=(FIGURE_WIDTH_IN, FIGURE_HEIGHT_IN))
    fig.subplots_adjust(left=0.22, right=0.98, bottom=0.23, top=0.97)
    for series_name, rows in rows_by_series.items():
        rows.sort(key=lambda row: int(row["replica_count"]))
        ax.errorbar(
            [int(row["replica_count"]) for row in rows],
            [float(row["mean"]) for row in rows],
            yerr=[float(row["ci95"]) for row in rows],
            color=color_map[series_name],
            marker="o",
            linewidth=1.5,
            markersize=3.5,
            capsize=2.5,
            label=series_name,
        )
    ax.set_xlabel("Replica count")
    ax.set_ylabel("Throughput (ops/sec)")
    ax.set_xticks(
        sorted({int(row["replica_count"]) for row in aggregate_summary_rows})
    )
    ax.grid(axis="y", color="#d9e0e6", linewidth=0.6)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(loc="upper left", frameon=False)
    save_figure(fig, output_path)
    plt.close(fig)


def generate_plots(
    result_root: Path,
    output_dir: Path,
    window_seconds: int = 10,
    step_seconds: int = 4,
) -> PlotArtifacts:
    result_root = result_root.resolve()
    output_dir = output_dir.resolve()
    dataset_name = result_root.name.replace(" ", "-")

    summary_rows = load_summary_rows(result_root)
    per_second_rows = load_per_second_rows(result_root)
    combined_window_rows, per_replica_window_rows = build_window_samples(
        per_second_rows=per_second_rows,
        window_seconds=window_seconds,
        step_seconds=step_seconds,
    )
    aggregate_summary_rows = (
        summarize_samples(combined_window_rows, "Combined throughput")
        + summarize_samples(per_replica_window_rows, "Per-replica throughput")
    )

    write_sample_csvs(
        output_dir=output_dir,
        dataset_name=dataset_name,
        combined_window_rows=combined_window_rows,
        per_replica_window_rows=per_replica_window_rows,
        aggregate_summary_rows=aggregate_summary_rows,
    )

    summary_plot_pdf = output_dir / f"{dataset_name}_throughput_vs_replicas.pdf"
    plot_throughput_vs_replicas_summary(summary_plot_pdf, summary_rows)

    error_bar_plot_pdf = output_dir / f"{dataset_name}_throughput_vs_replicas_error_bars.pdf"
    plot_error_bars(error_bar_plot_pdf, aggregate_summary_rows)

    time_plot_pdfs: list[Path] = []
    for summary_row in summary_rows:
        replica_count = int(summary_row["replicas"])
        time_plot_pdf = (
            output_dir
            / f"{dataset_name}_replicas_{replica_count}_throughput_over_time_{window_seconds}s.pdf"
        )
        plot_throughput_over_time(time_plot_pdf, combined_window_rows, replica_count)
        time_plot_pdfs.append(time_plot_pdf)

    return PlotArtifacts(
        summary_plot_pdf=summary_plot_pdf,
        error_bar_plot_pdf=error_bar_plot_pdf,
        time_plot_pdfs=time_plot_pdfs,
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    artifacts = generate_plots(
        result_root=args.result_root,
        output_dir=args.output_dir,
        window_seconds=args.window_seconds,
        step_seconds=args.step_seconds,
    )
    print(artifacts.summary_plot_pdf)
    print(artifacts.error_bar_plot_pdf)
    for path in artifacts.time_plot_pdfs:
        print(path)
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Regenerate benchmark plots from an existing throughput-vs-replicas result root."
    )
    parser.add_argument("--result-root", type=Path, default=DEFAULT_RESULT_ROOT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--window-seconds", type=int, default=10)
    parser.add_argument(
        "--step-seconds",
        type=int,
        default=4,
        help="Sample spacing for the rolling time plots. Default 4s gives about 15 samples for the 8-replica run.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(main())
