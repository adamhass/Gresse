#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULT_ROOT="${RESULT_ROOT:-${ROOT_DIR}/results/throughput-vs-replicas-minio-server}"
EXPERIMENT_PREFIX="${EXPERIMENT_PREFIX:-experiment1}"
MAX_STORE_SIZE_MB="${MAX_STORE_SIZE_MB:-64}"
CLIENTS_PER_REPLICA="${CLIENTS_PER_REPLICA:-8}"
OPS_PER_SECOND_PER_CLIENT="${OPS_PER_SECOND_PER_CLIENT:-0}"
DURATION_SECONDS="${DURATION_SECONDS:-90}"
WARMUP_SECONDS="${WARMUP_SECONDS:-20}"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-10}"
MIN_REPLICAS="${MIN_REPLICAS:-1}"
MAX_REPLICAS="${MAX_REPLICAS:-8}"
STARTUP_STAGGER_SECONDS="${STARTUP_STAGGER_SECONDS:-1}"
DISCOVERY_INTERVAL_MS="${DISCOVERY_INTERVAL_MS:-1000}"
CARGO_PROFILE="${CARGO_PROFILE:-release}"
HOST_PORT="${HOST_PORT:-9000}"
LATENCY_REGION="${LATENCY_REGION:-us-east-1}"
LATENCY_BUCKET="${LATENCY_BUCKET:-gresse}"
LATENCY_ACCESS_KEY="${LATENCY_ACCESS_KEY:-minioadmin}"
LATENCY_SECRET_KEY="${LATENCY_SECRET_KEY:-minioadmin}"
PORT_FORWARD_LOG="${PORT_FORWARD_LOG:-${ROOT_DIR}/results/minio-port-forward.log}"
DEPLOY_MINIO="${DEPLOY_MINIO:-1}"
NUM_RUNS="${NUM_RUNS:-1}"
BASE_SEED="${BASE_SEED:-1000}"
RUN_INDEX_CSV="${RUN_INDEX_CSV:-${RESULT_ROOT}/run_index.csv}"

PORT_FORWARD_PID=""
REUSED_PORT_FORWARD="0"

cleanup() {
  if [[ "${REUSED_PORT_FORWARD}" == "0" ]] && [[ -n "${PORT_FORWARD_PID}" ]] && kill -0 "${PORT_FORWARD_PID}" >/dev/null 2>&1; then
    kill "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
    wait "${PORT_FORWARD_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

existing_port_forward_pid() {
  local pid command
  while read -r pid; do
    [[ -n "${pid}" ]] || continue
    command="$(ps -p "${pid}" -o command= 2>/dev/null || true)"
    if [[ "${command}" == *"kubectl"* ]] && [[ "${command}" == *"port-forward"* ]] && [[ "${command}" == *"svc/minio-proxy"* ]]; then
      echo "${pid}"
      return 0
    fi
  done < <(lsof -ti "tcp:${HOST_PORT}" 2>/dev/null || true)
  return 1
}

wait_for_port_forward() {
  for _ in $(seq 1 30); do
    if nc -z 127.0.0.1 "${HOST_PORT}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

if [[ "${DEPLOY_MINIO}" == "1" ]]; then
  bash "${ROOT_DIR}/integrations/minio-latency/deploy_minio_latency.sh"
fi

mkdir -p "$(dirname "${PORT_FORWARD_LOG}")"
if PORT_FORWARD_PID="$(existing_port_forward_pid)"; then
  REUSED_PORT_FORWARD="1"
  echo "Reusing existing MinIO proxy port-forward on 127.0.0.1:${HOST_PORT} (pid ${PORT_FORWARD_PID})."
else
  if lsof -ti "tcp:${HOST_PORT}" >/dev/null 2>&1; then
    echo "Port ${HOST_PORT} is already in use by a non-MinIO process. Free that port or set HOST_PORT." >&2
    exit 1
  fi

  kubectl -n gresse-minio port-forward svc/minio-proxy "${HOST_PORT}:9000" >"${PORT_FORWARD_LOG}" 2>&1 &
  PORT_FORWARD_PID="$!"

  if ! wait_for_port_forward; then
    echo "MinIO proxy port-forward did not become ready. See ${PORT_FORWARD_LOG}" >&2
    exit 1
  fi
fi

cd "${ROOT_DIR}"
mkdir -p "${RESULT_ROOT}"

printf "run_index,seed,experiment_prefix,result_root\n" > "${RUN_INDEX_CSV}"

for run_index in $(seq 1 "${NUM_RUNS}"); do
  run_label="$(printf 'run_%02d' "${run_index}")"
  run_result_root="${RESULT_ROOT}/${run_label}"
  run_experiment_prefix="${EXPERIMENT_PREFIX}/${run_label}"
  run_seed="$((BASE_SEED + run_index - 1))"

  echo
  echo "Starting ${run_label} with seed ${run_seed}"

  python3 scripts/orset_throughput_vs_replicas.py \
    --result-root "${run_result_root}" \
    --experiment-prefix "${run_experiment_prefix}" \
    --min-replicas "${MIN_REPLICAS}" \
    --max-replicas "${MAX_REPLICAS}" \
    --max-store-size-mb "${MAX_STORE_SIZE_MB}" \
    --clients-per-replica "${CLIENTS_PER_REPLICA}" \
    --ops-per-second-per-client "${OPS_PER_SECOND_PER_CLIENT}" \
    --duration-seconds "${DURATION_SECONDS}" \
    --warmup-seconds "${WARMUP_SECONDS}" \
    --cooldown-seconds "${COOLDOWN_SECONDS}" \
    --startup-stagger-seconds "${STARTUP_STAGGER_SECONDS}" \
    --discovery-interval-ms "${DISCOVERY_INTERVAL_MS}" \
    --object-storage-url "http://127.0.0.1:${HOST_PORT}" \
    --object-storage-access-key "${LATENCY_ACCESS_KEY}" \
    --object-storage-secret-key "${LATENCY_SECRET_KEY}" \
    --region "${LATENCY_REGION}" \
    --bucket "${LATENCY_BUCKET}" \
    --cargo-profile "${CARGO_PROFILE}" \
    --seed "${run_seed}"

  printf "%s,%s,%s,%s\n" \
    "${run_label}" \
    "${run_seed}" \
    "${run_experiment_prefix}" \
    "${run_result_root}" >> "${RUN_INDEX_CSV}"
done

echo
echo "Benchmark runs complete."
echo "Run index: ${RUN_INDEX_CSV}"
echo "Results root: ${RESULT_ROOT}"
