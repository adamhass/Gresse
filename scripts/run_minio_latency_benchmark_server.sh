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

PORT_FORWARD_PID=""

cleanup() {
  if [[ -n "${PORT_FORWARD_PID}" ]] && kill -0 "${PORT_FORWARD_PID}" >/dev/null 2>&1; then
    kill "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
    wait "${PORT_FORWARD_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

if [[ "${DEPLOY_MINIO}" == "1" ]]; then
  bash "${ROOT_DIR}/integrations/minio-latency/deploy_minio_latency.sh"
fi

mkdir -p "$(dirname "${PORT_FORWARD_LOG}")"
kubectl -n gresse-minio port-forward svc/minio-proxy "${HOST_PORT}:9000" >"${PORT_FORWARD_LOG}" 2>&1 &
PORT_FORWARD_PID="$!"

for _ in $(seq 1 30); do
  if grep -q "Forwarding from 127.0.0.1:${HOST_PORT}" "${PORT_FORWARD_LOG}" 2>/dev/null; then
    break
  fi
  sleep 1
done

if ! grep -q "Forwarding from 127.0.0.1:${HOST_PORT}" "${PORT_FORWARD_LOG}" 2>/dev/null; then
  echo "MinIO proxy port-forward did not become ready. See ${PORT_FORWARD_LOG}" >&2
  exit 1
fi

cd "${ROOT_DIR}"

python3 scripts/orset_throughput_vs_replicas.py \
  --result-root "${RESULT_ROOT}" \
  --experiment-prefix "${EXPERIMENT_PREFIX}" \
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
  --cargo-profile "${CARGO_PROFILE}"

echo
echo "Benchmark complete."
echo "Summary: ${RESULT_ROOT}/throughput_vs_replicas_summary.csv"
echo "Plot:    ${RESULT_ROOT}/throughput_vs_replicas.svg"
