#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path


POINTS_PER_INCH = 72.0
FIGURE_WIDTH_IN = 252.0 / POINTS_PER_INCH
FIGURE_HEIGHT_IN = 140.0 / POINTS_PER_INCH

STEP_ORDER = [
    "1. Write GC marker",
    "2. Read membership",
    "3. Collect garbage",
    "4. Persist replica",
]

STEP_LABELS = {
    "gc_membership_descriptor_write": "1. Write GC marker",
    "membership_directory_read": "2. Read membership",
    "gc_local_collect": "3. Collect garbage",
    "gc_persistent_state_write": "4. Persist replica",
}

MEAN_LABEL_OFFSETS = {
    "1. Write GC marker": 0.22,
    "2. Read membership": -0.18,
    "3. Collect garbage": 0.0,
    "4. Persist replica": 0.0,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot GC step durations from one or more combined_metrics.csv files."
    )
    parser.add_argument(
        "inputs",
        nargs="*",
        type=Path,
        help="Files or directories to scan. Directories are searched recursively for combined_metrics.csv.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("results/plots/gc_profiling.pdf"),
        help="Output PDF path.",
    )
    parser.add_argument(
        "--exclude-replica-count",
        type=int,
        action="append",
        default=[],
        help="Exclude datasets under directories named like replicas_<N>. Can be repeated.",
    )
    return parser.parse_args()


def configure_matplotlib() -> None:
    import matplotlib
    import matplotlib.pyplot as plt

    matplotlib.use("Agg")
    plt.rcParams.update(
        {
            "font.family": "Arial",
            "font.size": 8,
            "axes.titlesize": 8,
            "axes.labelsize": 8,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "pdf.fonttype": 42,
        }
    )


def discover_combined_metrics(paths: list[Path], exclude_replica_counts: list[int]) -> list[Path]:
    if not paths:
        paths = [Path("results")]

    excluded_dirnames = {f"replicas_{count}" for count in exclude_replica_counts}
    combined_metrics: set[Path] = set()
    for path in paths:
        if path.is_file():
            if path.name == "combined_metrics.csv" and not any(
                part in excluded_dirnames for part in path.parts
            ):
                combined_metrics.add(path)
            continue
        if path.is_dir():
            for csv_path in path.rglob("combined_metrics.csv"):
                if any(part in excluded_dirnames for part in csv_path.parts):
                    continue
                combined_metrics.add(csv_path)

    return sorted(combined_metrics)


def parse_int(field: str) -> int | None:
    if not field:
        return None
    return int(field)


def classify_experiment(csv_path: Path) -> str:
    parts = csv_path.parts
    if "results" in parts:
        idx = parts.index("results")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    return csv_path.parent.name


def extract_gc_step_rows(csv_paths: list[Path]) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []

    for csv_path in csv_paths:
        experiment = classify_experiment(csv_path)
        gc_start_by_key: dict[tuple[int, int], int] = {}
        aborted_gc_keys: set[tuple[int, int]] = set()

        with csv_path.open(newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if row.get("source") != "server":
                    continue

                event = row.get("event", "")
                phase = row.get("phase", "")
                replica_pid = parse_int(row.get("replica_pid", ""))
                gc_marker = parse_int(row.get("gc_marker", ""))
                timestamp_us = parse_int(row.get("timestamp_us", ""))
                start_us = parse_int(row.get("start_us", ""))
                end_us = parse_int(row.get("end_us", ""))
                latency_us = parse_int(row.get("latency_us", ""))
                detail = row.get("detail", "")

                if replica_pid is None:
                    continue

                if event == "gc_init" and phase == "start" and gc_marker is not None:
                    gc_start_by_key[(replica_pid, gc_marker)] = timestamp_us or 0
                    aborted_gc_keys.discard((replica_pid, gc_marker))
                    continue

                if event == "gc_init" and phase == "aborted" and gc_marker is not None:
                    key = (replica_pid, gc_marker)
                    aborted_gc_keys.add(key)
                    gc_start_by_key.pop(key, None)
                    continue

                if gc_marker is None:
                    continue

                key = (replica_pid, gc_marker)
                if key in aborted_gc_keys:
                    continue
                duration_ms: float | None = None

                if event == "gc_membership_descriptor_write" and phase == "completed":
                    gc_start_us = gc_start_by_key.get(key)
                    if gc_start_us is not None and timestamp_us is not None:
                        duration_ms = (timestamp_us - gc_start_us) / 1000.0
                        gc_start_by_key.pop(key, None)
                elif event == "membership_directory_read" and detail.startswith("gc_validation:"):
                    if start_us is not None and end_us is not None:
                        duration_ms = (end_us - start_us) / 1000.0
                    elif latency_us is not None:
                        duration_ms = latency_us / 1000.0
                elif event in {"gc_local_collect", "gc_persistent_state_write"}:
                    if start_us is not None and end_us is not None:
                        duration_ms = (end_us - start_us) / 1000.0
                    elif latency_us is not None:
                        duration_ms = latency_us / 1000.0

                if duration_ms is None or event not in STEP_LABELS:
                    continue

                records.append(
                    {
                        "step": event,
                        "step_label": STEP_LABELS[event],
                        "duration_ms": duration_ms,
                        "experiment": experiment,
                        "replica_pid": replica_pid,
                        "gc_marker": gc_marker,
                        "csv_path": str(csv_path),
                    }
                )

    return records


def durations_by_step(step_rows: list[dict[str, object]]) -> dict[str, list[float]]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in step_rows:
        grouped[str(row["step_label"])].append(float(row["duration_ms"]))
    return grouped


def print_summary(step_rows: list[dict[str, object]]) -> None:
    grouped = durations_by_step(step_rows)
    print(f"Loaded {len(step_rows)} GC step datapoints")
    print()
    print(f"{'Step':<24} {'n':>5} {'mean':>10} {'median':>10}")
    print("-" * 52)
    for step_label in STEP_ORDER:
        series = grouped.get(step_label, [])
        if not series:
            print(f"{step_label:<24} {0:>5} {'-':>10} {'-':>10}")
            continue
        print(
            f"{step_label:<24} {len(series):>5} "
            f"{statistics.fmean(series):>9.2f} ms {statistics.median(series):>9.2f} ms"
        )


def plot_step_violins(step_rows: list[dict[str, object]], output_path: Path) -> None:
    import matplotlib.pyplot as plt
    import pandas as pd
    import seaborn as sns

    fig, ax = plt.subplots(figsize=(FIGURE_WIDTH_IN, FIGURE_HEIGHT_IN))
    fig.subplots_adjust(left=0.20, right=0.98, bottom=0.28, top=0.98)

    step_df = pd.DataFrame.from_records(step_rows)
    order = [step for step in STEP_ORDER if step in set(step_df["step_label"])]
    sns.violinplot(
        data=step_df,
        x="step_label",
        y="duration_ms",
        order=order,
        ax=ax,
        inner="quartile",
        cut=0,
        linewidth=0.8,
    )

    means = step_df.groupby("step_label")["duration_ms"].mean().reindex(order)
    ax.scatter(range(len(order)), means.values, color="black", marker="o", s=16, zorder=5)
    for idx, (label, mean_ms) in enumerate(means.items()):
        ax.text(
            idx + MEAN_LABEL_OFFSETS.get(label, 0.0),
            mean_ms,
            f"{mean_ms:.1f} ms",
            ha="center",
            va="center",
            color="black",
        )

    ax.set_xlabel("")
    ax.set_ylabel("Duration (ms)")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, format="pdf")


def main() -> int:
    args = parse_args()

    csv_paths = discover_combined_metrics(args.inputs, args.exclude_replica_count)
    if not csv_paths:
        raise SystemExit("No combined_metrics.csv files found.")

    step_rows = extract_gc_step_rows(csv_paths)
    if not step_rows:
        raise SystemExit("No GC step rows found in the selected combined_metrics.csv files.")

    print_summary(step_rows)
    try:
        configure_matplotlib()
        plot_step_violins(step_rows, args.output)
    except ModuleNotFoundError as error:
        print()
        print(f"Skipping plot generation because a plotting dependency is missing: {error.name}")
    else:
        print()
        print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
