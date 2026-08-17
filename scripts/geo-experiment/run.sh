#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
topology="$root_dir/scripts/geo_churn_topology.json"
result_dir=""
smoke=false
temporary_topology=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --topology) topology=$2; shift 2 ;;
    --result-dir) result_dir=$2; shift 2 ;;
    --smoke) smoke=true; shift ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

[[ -f "$topology" ]] || { echo "Topology not found: $topology" >&2; exit 1; }
if [[ "$smoke" == true ]]; then
  temporary_topology=$(mktemp "${TMPDIR:-/tmp}/gresse-geo-churn-smoke.XXXXXX")
  trap 'rm -f "$temporary_topology"' EXIT
  python3 "$root_dir/scripts/geo_churn_smoke_topology.py" --source "$topology" --output "$temporary_topology"
  topology="$temporary_topology"
fi
if [[ -z "$result_dir" ]]; then
  result_dir="$root_dir/results/geo_churn/geo-churn-$(date -u +%Y%m%dT%H%M%SZ)-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')"
fi
[[ ! -e "$result_dir" ]] || { echo "Result directory already exists: $result_dir" >&2; exit 1; }

echo "GRESSE geo-churn experiment"
[[ "$smoke" == true ]] && echo "Mode:       short concurrent-churn smoke validation"
echo "Started:    $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Topology:   $topology"
echo "Results:    $result_dir"
python3 -c '
import json, sys
config = json.load(open(sys.argv[1]))
duration = float(config["duration_seconds"])
convergence = float(config.get("final_convergence_seconds", 0))
regions = len(config["vms"])
replicas = int(config.get("replicas_per_vm", 5)) * regions
events = len(config.get("events", []))
print(f"Topology:   {replicas} replicas in {regions} regions; {events} churn events")
print(f"Run phase:  {duration:.0f}s workload + {convergence:.0f}s final convergence ({(duration + convergence) / 60:.1f} min)")
print("Note:       startup, shutdown, and artifact collection add variable time.")
' "$topology"

echo "Phase 1/3: building release binary"
"$root_dir/scripts/geo-experiment/build.sh"
binary="$root_dir/target/x86_64-unknown-linux-gnu/release/orset_bench_replica"
sha256=$(python3 -c 'import hashlib, pathlib, sys; print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())' "$binary")
echo "Phase 2/3: running controller (status is printed below)"

cd "$root_dir"
python3 scripts/geo_churn_experiment.py \
  --topology "$topology" \
  --result-dir "$result_dir" \
  --expected-binary-sha256 "$sha256"
echo "Phase 3/3: writing summary"
python3 scripts/analyze_geo_churn.py "$result_dir"

echo "Completed:  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Experiment and analysis completed: $result_dir"
