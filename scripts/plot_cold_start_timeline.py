#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt


POINTS_PER_INCH = 72.0
FIGURE_WIDTH_IN = 252.0 / POINTS_PER_INCH
FIGURE_HEIGHT_IN = 74.0 / POINTS_PER_INCH


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


def save_figure(fig: plt.Figure, pdf_path: Path) -> None:
    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(pdf_path, format="pdf")


def main() -> int:
    args = parse_args()
    configure_matplotlib()
    segments = load_average_segments(args.samples_csv)

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
    top_y = 0.32
    bottom_y = -0.32
    shared_height = 0.34
    branch_height = 0.24

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
        f"Init S3 Client\n{segments['object_storage_client_init_ms']:.1f} ms",
        ha="center",
        va="center",
        color="white",
    )

    # Branch connectors.
    ax.plot([split_start, split_start], [shared_y, top_y], color="#9e9e9e", linewidth=1.0)
    ax.plot([split_start, split_start], [shared_y, bottom_y], color="#9e9e9e", linewidth=1.0)

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
        f"Persistent Read\n{segments['persistent_read_ms']:.1f} ms",
        ha="center",
        va="center",
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
        f"Descriptor Write\n{segments['membership_descriptor_write_ms']:.1f} ms",
        ha="center",
        va="center",
        color="white",
    )
    ax.text(
        split_start
        + segments["membership_descriptor_write_ms"]
        + segments["membership_descriptors_read_ms"] / 2.0,
        bottom_y,
        f"Descriptors Read\n{segments['membership_descriptors_read_ms']:.1f} ms",
        ha="center",
        va="center",
        color="white",
    )

    ax.text(
        shared_start + 4.0,
        shared_y + 0.29,
        f"Pre-client setup: {segments['pre_client_setup_ms']:.1f} ms",
        ha="left",
        va="bottom",
    )

    ax.set_xlim(0.0, max_total_ms * 1.04)
    ax.set_ylim(-0.62, 0.62)
    ax.set_xlabel("Time Since Process Start (ms)")
    ax.set_yticks([])
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.grid(axis="x", color="#e0e0e0", linewidth=0.6)

    save_figure(fig, args.output_path)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot average cold-start timeline.")
    parser.add_argument("--samples-csv", type=Path, required=True)
    parser.add_argument("--output-path", type=Path, required=True)
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
