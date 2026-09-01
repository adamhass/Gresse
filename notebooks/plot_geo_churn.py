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
if __name__ == "__main__":
    # The CLI writes files; do not try to open a GUI backend.
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

    # Remote-agent runs keep per-VM lifecycle/workload observations alongside
    # the replica artifacts.  Treat them as controller-originated traces so
    # existing plots work for both execution modes.
    controller = _read_csvs([controller_path, *root.glob("remote_artifacts/**/agent_events.csv")], "controller")
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


def lifecycle_duration_dataframe(controller: pd.DataFrame) -> pd.DataFrame:
    """Pair controller lifecycle events into boot and stop duration samples."""
    frame = controller.copy()
    if frame.empty:
        return pd.DataFrame(columns=["experiment_time_s", "duration_ms", "duration_type"])
    frame["timestamp_us"] = pd.to_numeric(frame["timestamp_us"], errors="coerce")
    frame["pid"] = pd.to_numeric(frame["pid"], errors="coerce")
    frame = frame.dropna(subset=["timestamp_us", "pid"]).sort_values("timestamp_us")
    samples: list[dict[str, float | str]] = []
    pairs = (
        ("spawn_requested", "http_ready", "spawn to HTTP-ready"),
        ("spawn_requested", "bootstrap_ready", "spawn to bootstrap-ready"),
        ("graceful_stop_requested", "process_stopped", "graceful stop"),
        ("crash_requested", "process_stopped", "crash stop"),
    )
    for start_event, end_event, duration_type in pairs:
        starts = frame.loc[frame["event"].eq(start_event)]
        ends = frame.loc[frame["event"].eq(end_event)]
        for _, start in starts.iterrows():
            matching = ends.loc[(ends["pid"] == start["pid"]) & (ends["timestamp_us"] >= start["timestamp_us"])]
            if matching.empty:
                continue
            end = matching.iloc[0]
            samples.append({
                "experiment_time_s": end["experiment_time_s"],
                "duration_ms": (end["timestamp_us"] - start["timestamp_us"]) / 1_000,
                "duration_type": duration_type,
            })
    return pd.DataFrame(samples)


def scatter_lifecycle_durations(figure: matplotlib.figure.Figure, dataframe: pd.DataFrame) -> plt.Axes:
    """Plot controller-observed boot and stop durations over experiment time."""
    axis = figure.add_subplot(1, 1, 1)
    colors = plt.get_cmap("tab10")
    for index, (kind, group) in enumerate(dataframe.groupby("duration_type", sort=True)):
        axis.scatter(group["experiment_time_s"], group["duration_ms"], label=kind,
                     color=colors(index % 10), alpha=0.8, s=24, linewidths=0)
    axis.set_title("Replica lifecycle durations")
    axis.set_xlabel("Experiment time (s)")
    axis.set_ylabel("Duration (ms)")
    axis.grid(True, alpha=0.25)
    if not dataframe.empty:
        axis.legend(loc="best")
    return axis


def gc_activity_dataframe(replicas: pd.DataFrame) -> pd.DataFrame:
    """Extract GC protocol observations for a categorical timeline plot."""
    phases = {
        ("gc_init", "start"): "GC round started",
        ("gc_init", "aborted"): "GC round aborted",
        ("gc_local_collect", "completed"): "Local collection completed",
        ("gc_persistent_state_write", "completed"): "Persistent state written",
        ("gc_finalize", "completed"): "GC finalized",
    }
    frame = replicas.copy()
    selected = pd.Series(False, index=frame.index)
    labels = pd.Series(index=frame.index, dtype="object")
    for (event, phase), label in phases.items():
        mask = frame["event"].eq(event) & frame["phase"].eq(phase)
        selected |= mask
        labels.loc[mask] = label
    result = frame.loc[selected, ["experiment_time_s", "replica_pid", "gc_marker"]].copy()
    result["gc_activity"] = labels.loc[selected]
    return result


def scatter_gc_activity(figure: matplotlib.figure.Figure, dataframe: pd.DataFrame) -> plt.Axes:
    """Plot GC protocol state transitions over experiment time."""
    axis = figure.add_subplot(1, 1, 1)
    activities = list(dataframe["gc_activity"].dropna().unique())
    positions = {activity: index for index, activity in enumerate(activities)}
    colors = plt.get_cmap("tab10")
    for index, activity in enumerate(activities):
        group = dataframe.loc[dataframe["gc_activity"].eq(activity)]
        axis.scatter(group["experiment_time_s"], [positions[activity]] * len(group), label=activity,
                     color=colors(index % 10), alpha=0.75, s=22, linewidths=0)
    axis.set_title("GC protocol activity")
    axis.set_xlabel("Experiment time (s)")
    axis.set_yticks(list(positions.values()), list(positions.keys()))
    axis.grid(True, axis="x", alpha=0.25)
    return axis


def replication_rate_dataframe(replicas: pd.DataFrame, bin_seconds: int = 10) -> pd.DataFrame:
    """Aggregate anti-entropy operations into fixed-width experiment-time bins."""
    events = {
        "peer_replication_connection": "connections",
        "peer_replication_pull_request": "pull requests",
        "peer_replication_get_delta": "delta computations",
        "peer_replication_merge_delta": "delta merges",
    }
    frame = replicas.loc[replicas["event"].isin(events)].copy()
    if frame.empty:
        return pd.DataFrame(columns=["experiment_time_s", "operation", "count"])
    frame["operation"] = frame["event"].map(events)
    frame["experiment_time_s"] = pd.to_numeric(frame["experiment_time_s"], errors="coerce")
    frame = frame.dropna(subset=["experiment_time_s"])
    frame["bin"] = (frame["experiment_time_s"] // bin_seconds).astype(int) * bin_seconds
    return frame.groupby(["bin", "operation"], as_index=False).size().rename(
        columns={"bin": "experiment_time_s", "size": "count"}
    )


def plot_replication_rate(figure: matplotlib.figure.Figure, dataframe: pd.DataFrame) -> plt.Axes:
    """Plot anti-entropy operation counts per ten-second bin."""
    axis = figure.add_subplot(1, 1, 1)
    for operation, group in dataframe.groupby("operation", sort=True):
        axis.plot(group["experiment_time_s"], group["count"], marker="o", markersize=3, label=operation)
    axis.set_title("Anti-entropy activity (10-second bins)")
    axis.set_xlabel("Experiment time (s)")
    axis.set_ylabel("Operations / bin")
    axis.grid(True, alpha=0.25)
    if not dataframe.empty:
        axis.legend(loc="best")
    return axis


def system_latency_dataframe(data: ExperimentData) -> pd.DataFrame:
    """Build P2P, initialization/recovery, and completed-GC latency samples.

    A pull request does not currently carry a request identifier.  P2P samples
    therefore pair each pull with the next local delta merge for that replica.
    The pairing is intentionally conservative: a merge must arrive within ten
    seconds, and each merge is used at most once.
    """
    replica = data.replicas.copy()
    for column in ("timestamp_us", "start_us", "end_us", "replica_pid", "gc_marker"):
        replica[column] = pd.to_numeric(replica.get(column), errors="coerce")
    origin_us = data.origin_us
    samples: list[dict[str, float | str]] = []

    for pid, group in replica.groupby("replica_pid"):
        group = group.sort_values("timestamp_us")
        pulls = group.loc[(group["event"] == "peer_replication_pull_request") & (group["phase"] == "sent")]
        merges = group.loc[(group["event"] == "peer_replication_merge_delta") & (group["phase"] == "completed")].copy()
        merge_index = 0
        for _, pull in pulls.iterrows():
            while merge_index < len(merges) and merges.iloc[merge_index]["timestamp_us"] < pull["timestamp_us"]:
                merge_index += 1
            if merge_index >= len(merges):
                break
            merge = merges.iloc[merge_index]
            completed_us = merge["end_us"] if pd.notna(merge["end_us"]) else merge["timestamp_us"]
            latency_us = completed_us - pull["timestamp_us"]
            if 0 <= latency_us <= 10_000_000:
                samples.append({
                    "experiment_time_s": (pull["timestamp_us"] - origin_us) / 1_000_000,
                    "latency_ms": latency_us / 1_000,
                    "latency_type": "P2P pull to delta merge",
                })
                merge_index += 1

    for source, group in replica.groupby("source_file"):
        init_start = group.loc[(group["event"] == "replica_init") & (group["phase"] == "start"), "timestamp_us"]
        init_end = group.loc[(group["event"] == "replica_init") & (group["phase"] == "completed"), "timestamp_us"]
        if init_start.empty or init_end.empty:
            continue
        start_us, end_us = init_start.iloc[0], init_end.iloc[0]
        if end_us < start_us:
            continue
        recovered = (
            (group["event"] == "persistent_state_fetch")
            & group["detail"].eq("recovered_from_durability_journal")
        ).any()
        samples.append({
            "experiment_time_s": (start_us - origin_us) / 1_000_000,
            "latency_ms": (end_us - start_us) / 1_000,
            "latency_type": "Replica recovery" if recovered else "Replica initialization",
        })

    # A GC marker is scoped to one replica trace.  Require one start and one
    # final record for that key; duplicate or aborted rounds are ambiguous and
    # must not silently overwrite each other in a dictionary lookup.
    gc_key = ["source_file", "replica_pid", "gc_marker"]
    starts = replica.loc[
        (replica["event"] == "gc_init") & (replica["phase"] == "start"),
        [*gc_key, "timestamp_us"],
    ].dropna(subset=[*gc_key, "timestamp_us"])
    finals = replica.loc[
        (replica["event"] == "gc_finalize") & (replica["phase"] == "completed"),
        [*gc_key, "timestamp_us"],
    ].dropna(subset=[*gc_key, "timestamp_us"])
    aborted = replica.loc[
        (replica["event"] == "gc_init") & (replica["phase"] == "aborted"), gc_key
    ].dropna(subset=gc_key)
    duplicate_keys = pd.concat([
        starts.loc[starts.duplicated(gc_key, keep=False), gc_key],
        finals.loc[finals.duplicated(gc_key, keep=False), gc_key],
    ], ignore_index=True).drop_duplicates()
    excluded_keys = pd.concat([aborted, duplicate_keys], ignore_index=True).drop_duplicates()
    if not excluded_keys.empty:
        starts = starts.merge(excluded_keys.assign(_excluded=True), on=gc_key, how="left")
        finals = finals.merge(excluded_keys.assign(_excluded=True), on=gc_key, how="left")
        starts = starts.loc[starts["_excluded"].isna()].drop(columns="_excluded")
        finals = finals.loc[finals["_excluded"].isna()].drop(columns="_excluded")
    rounds = starts.merge(finals, on=gc_key, how="inner", validate="one_to_one", suffixes=("_start", "_final"))
    rounds = rounds.loc[rounds["timestamp_us_final"] >= rounds["timestamp_us_start"]]
    for _, round_ in rounds.iterrows():
        samples.append({
            "experiment_time_s": (round_["timestamp_us_start"] - origin_us) / 1_000_000,
            "latency_ms": (round_["timestamp_us_final"] - round_["timestamp_us_start"]) / 1_000,
            "latency_type": "GC round",
        })
    return pd.DataFrame(samples)


def topology_change_dataframe(controller: pd.DataFrame) -> pd.DataFrame:
    """Return one controller timestamp per concurrently initiated churn group."""
    frame = controller.loc[controller["event"].isin(["scheduled_event_dispatched", "scheduled_event_started"])].copy()
    if frame.empty:
        return pd.DataFrame(columns=["experiment_time_s", "action"])
    parsed = frame["detail"].str.extract(r"t=([^s]+)s\s+(.*)$", expand=True)
    frame["scheduled_seconds"] = pd.to_numeric(parsed[0], errors="coerce")
    frame["action"] = parsed[1].fillna("topology change")
    return frame.groupby(["scheduled_seconds", "action"], as_index=False)["experiment_time_s"].min()


def scatter_system_latencies(
    figure: matplotlib.figure.Figure,
    dataframe: pd.DataFrame,
    topology_changes: pd.DataFrame,
) -> plt.Axes:
    """Plot system-operation latencies and topology-change initiation markers."""
    axis = figure.add_subplot(1, 1, 1)
    colors = plt.get_cmap("tab10")
    for index, (kind, group) in enumerate(dataframe.groupby("latency_type", sort=True)):
        axis.scatter(group["experiment_time_s"], group["latency_ms"], label=kind,
                     color=colors(index % 10), alpha=0.8, s=22, linewidths=0)
        print(kind)
    change_colors = {"crash": "#d62728", "graceful_stop": "#9467bd", "spawn": "#2ca02c"}
    for _, change in topology_changes.iterrows():
        action = change["action"]
        axis.axvline(change["experiment_time_s"], color=change_colors.get(action, "#555555"),
                     linestyle="--", linewidth=1.1, alpha=0.75, label=f"topology: {action}")
    handles, labels = axis.get_legend_handles_labels()
    unique = dict(zip(labels, handles))
    axis.set_title("System-operation latency during geo-distributed churn")
    axis.set_xlabel("Experiment time (s)")
    axis.set_ylabel("Latency (ms)")
    # axis.set_yscale("log")
    axis.grid(True, which="both", alpha=0.25)
    if unique:
        axis.legend(unique.values(), unique.keys(), loc="best")
    return axis


def _save_plot(path: Path, plotter, dataframe: pd.DataFrame) -> None:
    figure = plt.figure(figsize=(9, 4.5), constrained_layout=True)
    plotter(figure, dataframe)
    figure.savefig(path, dpi=200)
    plt.close(figure)
    print(f"wrote {path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Plot geo-churn request, lifecycle, GC, and replication traces.")
    parser.add_argument(
        "result_dir", type=Path, nargs="?",
        help="result directory (defaults to the newest directory in results/geo_churn)",
    )
    parser.add_argument("--output", type=Path, help="request-latency output path (also disables companion plots)")
    args = parser.parse_args()
    result_dir = args.result_dir if args.result_dir is not None else latest_result_dir()
    output = args.output if args.output is not None else DEFAULT_PLOTS_ROOT / result_dir.name / "request_latency.png"
    data = load_experiment_data(result_dir)
    output.parent.mkdir(parents=True, exist_ok=True)
    print(f"plotted {result_dir}")
    _save_plot(output, scatter_latency, request_latency_dataframe(data.events))
    if args.output is None:
        _save_plot(output.parent / "lifecycle_durations.png", scatter_lifecycle_durations,
                   lifecycle_duration_dataframe(data.controller))
        _save_plot(output.parent / "gc_activity.png", scatter_gc_activity,
                   gc_activity_dataframe(data.replicas))
        _save_plot(output.parent / "replication_activity.png", plot_replication_rate,
                   replication_rate_dataframe(data.replicas))
        system_latency = system_latency_dataframe(data)
        _save_plot(
            output.parent / "system_operation_latency.png",
            lambda figure, frame: scatter_system_latencies(
                figure, frame, topology_change_dataframe(data.controller)
            ),
            system_latency,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
