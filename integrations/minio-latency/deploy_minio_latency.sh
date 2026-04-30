#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST="${ROOT_DIR}/minio-latency.yaml"
NAMESPACE="gresse-minio"

kubectl apply -f "${MANIFEST}"

kubectl -n "${NAMESPACE}" rollout status deployment/minio --timeout=180s
kubectl -n "${NAMESPACE}" rollout status deployment/minio-proxy --timeout=180s
kubectl -n "${NAMESPACE}" wait --for=condition=complete job/minio-bootstrap --timeout=180s

cat <<'EOF'
MinIO with latency proxy is ready.

To reach the latency-injected S3 endpoint from the host, run:
  kubectl -n gresse-minio port-forward svc/minio-proxy 9000:9000

Then configure Gresse with:
  export GRESSE_OBJECT_STORAGE_URL='http://127.0.0.1:9000'
  export GRESSE_OBJECT_STORAGE_REGION='us-east-1'
  export GRESSE_OBJECT_STORAGE_BUCKET='gresse'
  export GRESSE_OBJECT_STORAGE_ACCESS_KEY='minioadmin'
  export GRESSE_OBJECT_STORAGE_SECRET_KEY='minioadmin'

This setup adds about 50ms round-trip latency by applying 25ms upstream and 25ms downstream delay in Toxiproxy.
If you want a different latency model, edit integrations/minio-latency/minio-latency.yaml.
EOF
