#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import math
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt


POINTS_PER_INCH = 72.0
FIGURE_WIDTH_IN = 252.0 / POINTS_PER_INCH
FIGURE_HEIGHT_IN = 110.0 / POINTS_PER_INCH


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


def load_average_segments(csv_path: Path) -> dict[str, float]:
    with csv_path.open() as handle:
        rows = list(csv.DictReader(handle))

    object_storage_client_start_ms = mean_delta_ms(rows, "object_storage_client_start_delta_us")
    object_storage_client_ready_ms = mean_delta_ms(rows, "object_storage_client_ready_delta_us")
    persistent_read_ms = mean_delta_ms(rows, "persistent_state_fetch_completed_delta_us")
    membership_write_ms = mean_delta_ms(rows, "membership_descriptor_write_completed_delta_us")
    membership_read_ms = mean_delta_ms(rows, "membership_directory_read_completed_delta_us")

    return {
        "pre_client_setup_ms": object_storage_client_start_ms,
        "object_storage_client_init_ms": object_storage_client_ready_ms
        - object_storage_client_start_ms,
        "persistent_read_ms": persistent_read_ms - object_storage_client_ready_ms,
        "membership_descriptor_write_ms": membership_write_ms - object_storage_client_ready_ms,
        "membership_descriptors_read_ms": membership_read_ms - membership_write_ms,
    }


def mean_delta_ms(rows: list[dict[str, str]], field: str) -> float:
    return statistics.fmean(int(row[field]) for row in rows) / 1000.0


def compute_confidence_intervals(csv_path: Path) -> dict[str, tuple[float, float]]:
    """Return {segment_name: (mean_ms, half_width_ms)} for 95% CIs."""
    with csv_path.open() as handle:
        rows = list(csv.DictReader(handle))

    segments_per_row: dict[str, list[float]] = {
        "object_storage_client_init_ms": [],
        "membership_descriptor_write_ms": [],
        "membership_descriptors_read_ms": [],
        "persistent_read_ms": [],
    }
    for row in rows:
        start = int(row["object_storage_client_start_delta_us"]) / 1000.0
        ready = int(row["object_storage_client_ready_delta_us"]) / 1000.0
        persistent = int(row["persistent_state_fetch_completed_delta_us"]) / 1000.0
        mem_write = int(row["membership_descriptor_write_completed_delta_us"]) / 1000.0
        mem_read = int(row["membership_directory_read_completed_delta_us"]) / 1000.0

        segments_per_row["object_storage_client_init_ms"].append(ready - start)
        segments_per_row["membership_descriptor_write_ms"].append(mem_write - ready)
        segments_per_row["membership_descriptors_read_ms"].append(mem_read - mem_write)
        segments_per_row["persistent_read_ms"].append(persistent - ready)

    n = len(rows)
    z_crit = statistics.NormalDist().inv_cdf(0.975)

    result: dict[str, tuple[float, float]] = {}
    for key, values in segments_per_row.items():
        mean = statistics.fmean(values)
        half_width = z_crit * statistics.stdev(values) / math.sqrt(n)
        result[key] = (mean, half_width)
    return result


def print_confidence_intervals(cis: dict[str, tuple[float, float]], n: int) -> None:
    labels = {
        "object_storage_client_init_ms": "Init S3 Client",
        "membership_descriptor_write_ms": "(N1) Register in M",
        "membership_descriptors_read_ms": "(N2) Read M",
        "persistent_read_ms": "(N3) Read r_P",
    }
    print(f"\n95% Confidence Intervals (n={n}):")
    print(f"{'Segment':<25} {'Mean':>8} {'± Half-width':>14} {'CI':>20}")
    print("-" * 70)
    for key, label in labels.items():
        mean, hw = cis[key]
        print(f"{label:<25} {mean:>8.2f} ms {hw:>8.2f} ms   [{mean - hw:.2f}, {mean + hw:.2f}]")


def save_figure(fig: plt.Figure, pdf_path: Path) -> None:
    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(pdf_path, format="pdf")


def main() -> int:
    args = parse_args()
    configure_matplotlib()
    segments = load_average_segments(args.samples_csv)
    cis = compute_confidence_intervals(args.samples_csv)
    print_confidence_intervals(cis, n=100)

    fig, ax = plt.subplots(figsize=(FIGURE_WIDTH_IN, FIGURE_HEIGHT_IN))
    shared_start = segments["pre_client_setup_ms"]
    split_start = shared_start + segments["object_storage_client_init_ms"]
    top_total = split_start + segments["persistent_read_ms"]
    bottom_total = (
        split_start
        + segments["membership_descriptor_write_ms"]
        + segments["membership_descriptors_read_ms"]
    )
    max_total_ms = max(top_total, bottom_total)

    shared_y = 0.0
    shared_height = 0.495
    branch_height = shared_height / 2.0
    top_y = branch_height / 2.0
    bottom_y = -branch_height / 2.0

    # Tiny pre-client setup stub before the main S3 init block.
    ax.barh(
        y=shared_y,
        width=segments["pre_client_setup_ms"],
        left=0.0,
        height=shared_height,
        color="#d9d9d9",
        edgecolor="white",
        linewidth=0.8,
    )

    # Shared thick S3 client initialization bar.
    ax.barh(
        y=shared_y,
        width=segments["object_storage_client_init_ms"],
        left=shared_start,
        height=shared_height,
        color="#377eb8",
        edgecolor="white",
        linewidth=0.8,
    )
    ax.text(
        shared_start + segments["object_storage_client_init_ms"] / 2.0,
        shared_y,
        f"{segments['object_storage_client_init_ms']:.0f} ms",
        ha="center",
        va="center",
        color="black",
    )

    # Top branch: persistent read.
    ax.barh(
        y=top_y,
        width=segments["persistent_read_ms"],
        left=split_start,
        height=branch_height,
        color="#ff7f00",
        edgecolor="white",
        linewidth=0.8,
    )
    ax.text(
        split_start + segments["persistent_read_ms"] / 2.0,
        top_y,
        f"{segments['persistent_read_ms']:.0f} ms",
        ha="center",
        va="center",
        color="black",
    )

    # Bottom branch: membership write + read sequence.
    ax.barh(
        y=bottom_y,
        width=segments["membership_descriptor_write_ms"],
        left=split_start,
        height=branch_height,
        color="#984ea3",
        edgecolor="white",
        linewidth=0.8,
    )
    ax.barh(
        y=bottom_y,
        width=segments["membership_descriptors_read_ms"],
        left=split_start + segments["membership_descriptor_write_ms"],
        height=branch_height,
        color="#e41a1c",
        edgecolor="white",
        linewidth=0.8,
    )
    ax.text(
        split_start + segments["membership_descriptor_write_ms"] / 2.0,
        bottom_y,
        f"{segments['membership_descriptor_write_ms']:.0f} ms",
        ha="center",
        va="center",
        color="black",
    )
    ax.text(
        split_start
        + segments["membership_descriptor_write_ms"]
        + segments["membership_descriptors_read_ms"] / 2.0,
        bottom_y - branch_height / 2.0 - 0.04,
        f"{segments['membership_descriptors_read_ms']:.0f} ms",
        ha="center",
        va="top",
        color="black",
    )

    # 95% CI whiskers at the right edge of each bar.
    whisker_style = dict(fmt="none", ecolor="black", capsize=2, elinewidth=0.8)
    ax.errorbar(
        x=split_start,
        y=shared_y,
        xerr=cis["object_storage_client_init_ms"][1],
        **whisker_style,
    )
    ax.errorbar(
        x=split_start + segments["persistent_read_ms"],
        y=top_y,
        xerr=cis["persistent_read_ms"][1],
        **whisker_style,
    )
    ax.errorbar(
        x=split_start + segments["membership_descriptor_write_ms"],
        y=bottom_y,
        xerr=cis["membership_descriptor_write_ms"][1],
        **whisker_style,
    )
    ax.errorbar(
        x=split_start
        + segments["membership_descriptor_write_ms"]
        + segments["membership_descriptors_read_ms"],
        y=bottom_y,
        xerr=cis["membership_descriptors_read_ms"][1],
        **whisker_style,
    )

    ax.set_xlim(0.0, max_total_ms * 1.04)
    ax.set_ylim(-0.55, 0.55)
    ax.set_xlabel("Time Since Process Start (ms)")
    ax.set_yticks([])
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)

    # Order accounts for matplotlib's column-first legend layout with ncol=2:
    # Col 1: S3, N1 | Col 2: N3, N2 → Row 1: S3 | N3, Row 2: N1 | N2.
    legend_patches = [
        mpatches.Patch(color="#377eb8", label="Init S3 Client"),
        mpatches.Patch(color="#984ea3", label=r"(N1) Register in $\mathcal{M}$"),
        mpatches.Patch(color="#ff7f00", label=r"(N3) Read $r_P$"),
        mpatches.Patch(color="#e41a1c", label=r"(N2) Read $\mathcal{M}$"),
    ]
    fig.legend(
        handles=legend_patches,
        ncol=2,
        loc="upper center",
        frameon=False,
        handlelength=1.0,
        handletextpad=0.4,
        columnspacing=1.0,
    )

    save_figure(fig, args.output_path)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot average cold-start timeline.")
    parser.add_argument("--samples-csv", type=Path, required=True)
    parser.add_argument(
        "--output-path",
        type=Path,
        default=Path("results/plots/cold_start_timeline_s3_100.pdf"),
    )
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
