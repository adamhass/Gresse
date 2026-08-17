#!/usr/bin/env python3
"""Load and plot traces collected by the geo churn experiment.

The public functions intentionally accept pandas objects and matplotlib figures
so that paper-specific plots can be composed in a notebook or another script.
For example::

    from pathlib import Path
    import matplotlib.pyplot as plt
    from plot_geo_churn import load_experiment_data, request_latency_dataframe, scatter_latency

    traces = load_experiment_data(Path("results/geo_churn/run-01"))
    figure = plt.figure(figsize=(9, 4))
    scatter_latency(figure, request_latency_dataframe(traces.events))
    figure.savefig("results/run-01/request_latency.png", dpi=200)
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
VENV_PYTHON = PROJECT_ROOT / ".venv/bin/python"
if not VENV_PYTHON.is_file() and sys.platform == "win32":
    VENV_PYTHON = PROJECT_ROOT / ".venv/Scripts/python.exe"

try:
    import pandas as pd
except ModuleNotFoundError as error:
    in_project_venv = Path(sys.prefix).resolve() == (PROJECT_ROOT / ".venv").resolve()
    if not in_project_venv and VENV_PYTHON.is_file():
        os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), *sys.argv])
    raise SystemExit(
        "Missing plotting dependency: pandas. Install dependencies with "
        "python3 -m pip install pandas matplotlib."
    ) from error

# Keep Matplotlib's cache within the repository so a restricted home directory
# cannot prevent plotting on a controller laptop or in an automated runner.
matplotlib_cache = PROJECT_ROOT / ".cache/matplotlib"
matplotlib_cache.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(matplotlib_cache))

import matplotlib

# This is a file-producing command, never an interactive GUI.  Selecting Agg
# avoids a macOS GUI-backend abort when it is run from a non-interactive shell.
matplotlib.use("Agg")

import matplotlib.figure
import matplotlib.pyplot as plt


DEFAULT_RESULTS_ROOT = Path("results/geo_churn")
DEFAULT_PLOTS_ROOT = Path("results/plots/geo_churn")


@dataclass(frozen=True)
class ExperimentData:
    """Normalized CSV traces from one collected experiment result directory."""

    controller: pd.DataFrame
    replicas: pd.DataFrame
    events: pd.DataFrame
    origin_us: int


def _read_csvs(paths: Iterable[Path], trace_kind: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in sorted(paths):
        frame = pd.read_csv(path)
        frame["trace_kind"] = trace_kind
        frame["source_file"] = str(path)
        frames.append(frame)
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def _normalize(frame: pd.DataFrame, origin_us: int) -> pd.DataFrame:
    """Return a copy with timestamp columns numeric and experiment_time_s added."""
    result = frame.copy()
    for column in ("timestamp_us", "received_us", "start_us", "end_us", "latency_us"):
        if column in result:
            result[column] = pd.to_numeric(result[column], errors="coerce")
    if "timestamp_us" in result:
        result["experiment_time_s"] = (result["timestamp_us"] - origin_us) / 1_000_000
    else:
        result["experiment_time_s"] = pd.Series(dtype="float64")
    return result


def load_experiment_data(result_dir: Path | str) -> ExperimentData:
    """Load controller and replica CSV files with a common, zero-based time axis.

    ``origin_us`` is the smallest valid ``timestamp_us`` across every available
    trace, not just the controller trace.  This preserves the early replica
    startup events that can precede a controller record.
    """
    root = Path(result_dir)
    controller_path = root / "controller_events.csv"
    if not controller_path.is_file():
        raise FileNotFoundError(f"controller trace not found: {controller_path}")

    controller = _read_csvs([controller_path], "controller")
    replicas = _read_csvs(root.glob("remote_artifacts/**/server_*.csv"), "replica")
    timestamp_sets = []
    for frame in (controller, replicas):
        if "timestamp_us" in frame:
            values = pd.to_numeric(frame["timestamp_us"], errors="coerce").dropna()
            if not values.empty:
                timestamp_sets.append(values)
    if not timestamp_sets:
        raise ValueError(f"no valid timestamp_us values found below {root}")
    origin_us = int(min(values.min() for values in timestamp_sets))

    controller = _normalize(controller, origin_us)
    replicas = _normalize(replicas, origin_us)
    events = pd.concat([controller, replicas], ignore_index=True, sort=False)
    return ExperimentData(controller=controller, replicas=replicas, events=events, origin_us=origin_us)


def latest_result_dir(results_root: Path | str = DEFAULT_RESULTS_ROOT) -> Path:
    """Return the most recently modified completed-or-partial experiment directory.

    A valid candidate contains ``controller_events.csv``. This means a timed-out
    run remains plot-able as soon as the controller has created its trace.
    """
    root = Path(results_root)
    candidates = [
        directory for directory in root.iterdir()
        if directory.is_dir() and (directory / "controller_events.csv").is_file()
    ] if root.is_dir() else []
    if not candidates:
        raise FileNotFoundError(
            f"no geo churn result directories found under {root}; "
            "pass a result directory explicitly or run the experiment first"
        )
    return max(candidates, key=lambda directory: (directory / "controller_events.csv").stat().st_mtime_ns)


def request_latency_dataframe(events: pd.DataFrame) -> pd.DataFrame:
    """Extract latency observations, measured from request receipt to completion.

    Replica-side observations are restricted to ``client_mutation``; peer
    replication spans may also have receipt timestamps, but are not client
    requests. Controller ``client_request`` events are included as end-to-end
    observations: their completion timestamp is ``timestamp_us`` and their
    receive time is reconstructed as ``timestamp_us - latency_us``.
    """
    frame = events.copy()
    for column in ("timestamp_us", "received_us", "end_us", "latency_us"):
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    trace_kind = frame.get("trace_kind", pd.Series(index=frame.index, dtype="object"))
    event = frame.get("event", pd.Series(index=frame.index, dtype="object"))
    replica = frame.loc[trace_kind.eq("replica") & event.eq("client_mutation")].copy()
    if "received_us" not in replica or "end_us" not in replica:
        replica = replica.iloc[0:0].copy()
        replica["received_us"] = pd.Series(dtype="float64")
        replica["end_us"] = pd.Series(dtype="float64")
    replica = replica.loc[replica["received_us"].notna() & replica["end_us"].notna()].copy()
    replica["completed_us"] = replica["end_us"]
    replica["request_latency_us"] = replica["completed_us"] - replica["received_us"]
    replica["latency_type"] = "replica: client mutation"

    controller = frame.loc[
        trace_kind.eq("controller") & event.eq("client_request")
    ].copy()
    controller = controller.loc[controller["timestamp_us"].notna() & controller["latency_us"].notna()].copy()
    controller["completed_us"] = controller["timestamp_us"]
    controller["received_us"] = controller["completed_us"] - controller["latency_us"]
    controller["request_latency_us"] = controller["completed_us"] - controller["received_us"]
    controller["latency_type"] = "controller: failed client request"
    successful = pd.to_numeric(controller.get("status_code"), errors="coerce").eq(200)
    controller.loc[successful, "latency_type"] = "controller: successful client request"

    result = pd.concat([replica, controller], ignore_index=True, sort=False)
    if result.empty:
        return pd.DataFrame(columns=["experiment_time_s", "request_latency_us", "latency_ms", "latency_type"])
    origin_us = pd.to_numeric(frame["timestamp_us"], errors="coerce").min()
    result["experiment_time_s"] = (result["completed_us"] - origin_us) / 1_000_000
    result["latency_ms"] = result["request_latency_us"] / 1_000
    return result.loc[result["request_latency_us"].ge(0)].copy()


def scatter_latency(
    figure: matplotlib.figure.Figure,
    dataframe: pd.DataFrame,
    *,
    ax: Optional[plt.Axes] = None,
    title: str = "Request latency during geo-distributed churn",
) -> plt.Axes:
    """Scatter request latency over experiment time, grouped into a legend."""
    axis = ax if ax is not None else figure.add_subplot(1, 1, 1)
    if dataframe.empty:
        axis.set_title(title)
        axis.set_xlabel("Experiment time (s)")
        axis.set_ylabel("Request latency (ms)")
        return axis

    colors = plt.get_cmap("tab10")
    for index, (kind, group) in enumerate(dataframe.groupby("latency_type", sort=True)):
        axis.scatter(
            group["experiment_time_s"], group["latency_ms"],
            label=kind, color=colors(index % 10), alpha=0.75, s=18, linewidths=0,
        )
    axis.set_title(title)
    axis.set_xlabel("Experiment time (s)")
    axis.set_ylabel("Request latency (ms)")
    axis.grid(True, alpha=0.25)
    axis.legend(title="Trace", loc="best")
    return axis


def main() -> int:
    parser = argparse.ArgumentParser(description="Plot request latency from a geo churn result directory.")
    parser.add_argument(
        "result_dir", type=Path, nargs="?",
        help="result directory (defaults to the newest directory in results/geo_churn)",
    )
    parser.add_argument("--output", type=Path, help="output PNG/PDF/SVG path (defaults under results/plots/geo_churn)")
    args = parser.parse_args()
    result_dir = args.result_dir if args.result_dir is not None else latest_result_dir()
    output = args.output if args.output is not None else DEFAULT_PLOTS_ROOT / result_dir.name / "request_latency.png"
    data = load_experiment_data(result_dir)
    figure = plt.figure(figsize=(9, 4.5), constrained_layout=True)
    scatter_latency(figure, request_latency_dataframe(data.events))
    output.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output, dpi=200)
    print(f"plotted {result_dir}")
    print(f"wrote {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
