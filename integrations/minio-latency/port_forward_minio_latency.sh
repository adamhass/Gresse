#!/usr/bin/env bash
set -euo pipefail

kubectl -n gresse-minio port-forward svc/minio-proxy 9000:9000
