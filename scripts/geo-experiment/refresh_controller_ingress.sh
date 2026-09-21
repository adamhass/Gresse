#!/usr/bin/env bash
set -euo pipefail
export AWS_PAGER=""

usage() {
  cat >&2 <<'EOF'
Usage: refresh_controller_ingress.sh [--controller-cidr CIDR] [--project NAME]

Adds the controller CIDR to the existing geo-churn security groups in all five
regions. With no --controller-cidr, discovers the current public IPv4 address.
It does not create, start, stop, or terminate EC2 instances.
EOF
  exit 2
}

progress() {
  printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"
}

project="gresse-geo-churn"
controller_cidr=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) project=$2; shift 2 ;;
    --controller-cidr) controller_cidr=$2; shift 2 ;;
    -h|--help) usage ;;
    *) usage ;;
  esac
done

for command in aws curl; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "Missing required command: $command" >&2
    exit 1
  }
done

progress "Checking AWS credentials"
aws sts get-caller-identity >/dev/null

if [[ -z "$controller_cidr" ]]; then
  controller_ip=$(curl --fail --silent --show-error https://checkip.amazonaws.com | tr -d '[:space:]')
  controller_cidr="${controller_ip}/32"
fi
[[ "$controller_cidr" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}/([0-9]|[12][0-9]|3[0-2])$ ]] || {
  echo "--controller-cidr must be an IPv4 CIDR, got: $controller_cidr" >&2
  exit 2
}

regions=(stockholm:eu-north-1 frankfurt:eu-central-1 ireland:eu-west-1 london:eu-west-2 paris:eu-west-3)
progress "Allowing controller $controller_cidr for SSH and replica HTTP control ports"
for entry in "${regions[@]}"; do
  name=${entry%%:*}
  region=${entry##*:}
  group_name="${project}-${name}"
  group_id=$(aws ec2 describe-security-groups \
    --region "$region" \
    --filters "Name=group-name,Values=$group_name" \
    --query 'SecurityGroups[0].GroupId' \
    --output text)
  [[ "$group_id" != "None" && -n "$group_id" ]] || {
    echo "[$name/$region] security group not found: $group_name" >&2
    exit 1
  }

  for permission in \
    "IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=$controller_cidr}]" \
    "IpProtocol=tcp,FromPort=18080,ToPort=18084,IpRanges=[{CidrIp=$controller_cidr}]"; do
    if error=$(aws ec2 authorize-security-group-ingress \
      --region "$region" \
      --group-id "$group_id" \
      --ip-permissions "$permission" 2>&1); then
      continue
    fi
    if [[ "$error" != *"InvalidPermission.Duplicate"* ]]; then
      echo "[$name/$region] failed to authorize $controller_cidr: $error" >&2
      exit 1
    fi
  done
  progress "[$name/$region] refreshed $group_id"
done

progress "Controller ingress refresh complete"
