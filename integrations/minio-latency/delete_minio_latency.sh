#!/usr/bin/env bash
set -euo pipefail

kubectl delete namespace gresse-minio --ignore-not-found=true
