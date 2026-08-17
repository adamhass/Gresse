#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
topology="$root_dir/scripts/geo_churn_topology.json"
result_dir=""
skip_build=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --topology) topology=$2; shift 2 ;;
    --result-dir) result_dir=$2; shift 2 ;;
    --skip-build) skip_build=true; shift ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

[[ -f "$topology" ]] || { echo "Topology not found: $topology" >&2; exit 1; }
if [[ "$skip_build" != true ]]; then
  "$root_dir/scripts/geo-experiment/build.sh"
fi

binary="$root_dir/target/x86_64-unknown-linux-gnu/release/orset_bench_replica"
[[ -x "$binary" ]] || { echo "Release binary missing: $binary" >&2; exit 1; }
sha256=$(python3 -c 'import hashlib, pathlib, sys; print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())' "$binary")
if [[ -z "$result_dir" ]]; then
  result_dir="$root_dir/results/preflight-$(date -u +%Y%m%dT%H%M%SZ)-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')"
fi

cd "$root_dir"
python3 scripts/geo_churn_experiment.py \
  --topology "$topology" \
  --result-dir "$result_dir" \
  --preflight-only \
  --deploy-binary "$binary" \
  --expected-binary-sha256 "$sha256"

echo "Preflight succeeded. Remote binary deployment and validation evidence: $result_dir"
