#!/usr/bin/env bash
set -euo pipefail
export AWS_PAGER=""

progress() {
  printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"
}

usage() { cat >&2 <<'EOF'
Usage:
  aws_setup.sh
  aws_setup.sh [--create-bucket] [topology.json]
  aws_setup.sh --provision --key-name NAME --controller-cidr CIDR --bucket NAME [options]
  aws_setup.sh --destroy --project NAME --bucket NAME

Options: --s3-region REGION (eu-north-1), --instance-type TYPE (t3.large),
--ssh-user USER (ec2-user), --identity-file PATH, --topology PATH, --force-topology,
--project NAME (gresse-geo-churn).
Provisioning uses one public-IPv4 EC2 VM in each default VPC, with no NAT,
Transit Gateway, VPN, or Elastic IP.
With no arguments it generates a unique bucket and EC2 key pair, discovers the
controller's public IP, creates any missing default VPCs, provisions all five
VMs, and writes the topology.
EOF
exit 2; }

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
topology="$root_dir/scripts/geo_churn_topology.json"
create_bucket=false; provision=false; bucket=""; s3_region="eu-north-1"
key_name=""; controller_cidr=""; instance_type="t3.large"; ssh_user="ec2-user"; project="gresse-geo-churn"; identity_file=""
force_topology=false; destroy=false
original_arg_count=$#; auto_provision=false; key_path=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --create-bucket) create_bucket=true; shift;; --provision) provision=true; shift;;
    --destroy) destroy=true; shift;;
    --bucket) bucket=$2; shift 2;; --s3-region) s3_region=$2; shift 2;; --key-name) key_name=$2; shift 2;;
    --controller-cidr) controller_cidr=$2; shift 2;; --instance-type) instance_type=$2; shift 2;;
    --ssh-user) ssh_user=$2; shift 2;; --identity-file) identity_file=$2; shift 2;; --topology) topology=$2; shift 2;; --project) project=$2; shift 2;;
    --force-topology) force_topology=true; shift;;
    -h|--help) usage;; *) [[ "$1" == *.json && $# == 1 ]] && { topology=$1; shift; } || usage;;
  esac
done
progress "Checking local prerequisites"
for cmd in aws python3 ssh-keygen; do command -v "$cmd" >/dev/null 2>&1 || { echo "Missing required command: $cmd" >&2; exit 1; }; done
progress "Checking AWS credentials"
account_id=$(aws sts get-caller-identity --query Account --output text)
if [[ "$original_arg_count" -eq 0 ]]; then
  for cmd in curl ssh-keygen; do command -v "$cmd" >/dev/null 2>&1 || { echo "Automatic setup needs: $cmd" >&2; exit 1; }; done
  auto_provision=true; provision=true; create_bucket=true
  suffix="$(date -u +%Y%m%d%H%M%S)-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:6])')"
  bucket="gresse-geo-${account_id}-${suffix}"
  key_name="${project}-${suffix}"
  controller_ip=$(curl --fail --silent --show-error https://checkip.amazonaws.com | tr -d '[:space:]')
  controller_cidr="${controller_ip}/32"
  key_path="$HOME/.ssh/${key_name}.pem"
  identity_file="$key_path"
  progress "Automatic configuration: bucket=$bucket, controller=$controller_cidr, key=$key_path"
fi

ensure_bucket() {
  if ! aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
    [[ "$create_bucket" == true ]] || { echo "Bucket $bucket is unavailable; pass --create-bucket only for an intended new bucket." >&2; exit 1; }
    progress "Creating S3 bucket $bucket in $s3_region"
    if [[ "$s3_region" == us-east-1 ]]; then aws s3api create-bucket --bucket "$bucket" --region "$s3_region"; else aws s3api create-bucket --bucket "$bucket" --region "$s3_region" --create-bucket-configuration "LocationConstraint=$s3_region"; fi
  else
    progress "Using existing S3 bucket $bucket"
  fi
}

destroy_resources() {
  local regions=(eu-north-1 eu-central-1 eu-west-1 eu-west-2 eu-west-3)
  progress "Destroying EC2 resources tagged Project=$project"
  for region in "${regions[@]}"; do
    instance_ids=$(aws ec2 describe-instances --region "$region" --filters "Name=tag:Project,Values=$project" Name=instance-state-name,Values=pending,running,stopped,stopping --query 'Reservations[].Instances[].InstanceId' --output text)
    if [[ -n "$instance_ids" && "$instance_ids" != None ]]; then
      progress "Terminating instances in $region: $instance_ids"
      aws ec2 terminate-instances --region "$region" --instance-ids $instance_ids >/dev/null
      aws ec2 wait instance-terminated --region "$region" --instance-ids $instance_ids
    fi
  done
  [[ -n "$bucket" ]] || { echo "--destroy requires --bucket NAME so deletion is explicit." >&2; exit 2; }
  progress "Permanently deleting bucket and all of its objects: $bucket"
  aws s3 rb "s3://$bucket" --force
  progress "Teardown complete: EC2 instances terminated and bucket deleted"
}

if [[ "$destroy" == true ]]; then
  [[ "$provision" != true ]] || { echo "--destroy and --provision cannot be combined" >&2; exit 2; }
  destroy_resources
  exit 0
fi

if [[ "$provision" != true ]]; then
  if [[ ! -f "$topology" ]]; then cp "$root_dir/scripts/geo_churn_topology.example.json" "$topology"; echo "Created $topology. Edit it, then re-run aws_setup.sh."; exit 0; fi
  read -r bucket s3_region < <(python3 -c 'import json,sys; c=json.load(open(sys.argv[1])); print(c["bucket"],c.get("s3_region","us-east-1"))' "$topology")
  ensure_bucket; echo "Bucket is accessible: $bucket"; exit 0
fi

[[ -n "$bucket" && -n "$key_name" && -n "$controller_cidr" && "$controller_cidr" == */* ]] || { echo "--provision requires --bucket, --key-name, and --controller-cidr CIDR." >&2; usage; }
if [[ -z "$identity_file" && -f "$HOME/.ssh/${key_name}.pem" ]]; then identity_file="$HOME/.ssh/${key_name}.pem"; fi
[[ ! -f "$topology" || "$force_topology" == true ]] || { echo "Refusing to overwrite existing topology: $topology (pass --force-topology to regenerate it)." >&2; exit 1; }
if [[ "$auto_provision" == true ]]; then
  progress "Creating a new EC2 key pair in eu-north-1"
  mkdir -p "$HOME/.ssh"
  temporary_key=$(mktemp)
  if ! aws ec2 create-key-pair --region eu-north-1 --key-name "$key_name" --query KeyMaterial --output text > "$temporary_key"; then
    rm -f "$temporary_key"
    echo "Automatic setup needs ec2:CreateKeyPair and ec2:ImportKeyPair permissions." >&2
    exit 1
  fi
  chmod 600 "$temporary_key"
  mv "$temporary_key" "$key_path"
  ssh-keygen -y -f "$key_path" > "${key_path}.pub"
fi
ensure_bucket
progress "Creating or updating the EC2 S3 instance role"
role_name="${project}-ec2-role"; profile_name="${project}-ec2-profile"
trust='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
policy=$(python3 -c 'import json,sys; b=sys.argv[1]; print(json.dumps({"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":f"arn:aws:s3:::{b}"},{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:DeleteObject"],"Resource":f"arn:aws:s3:::{b}/*"}]}))' "$bucket")
aws iam get-role --role-name "$role_name" >/dev/null 2>&1 || aws iam create-role --role-name "$role_name" --assume-role-policy-document "$trust" >/dev/null
aws iam put-role-policy --role-name "$role_name" --policy-name "${project}-s3" --policy-document "$policy"
if ! aws iam get-instance-profile --instance-profile-name "$profile_name" >/dev/null 2>&1; then
  progress "Creating EC2 instance profile (waiting briefly for IAM propagation)"
  aws iam create-instance-profile --instance-profile-name "$profile_name" >/dev/null
  aws iam add-role-to-instance-profile --instance-profile-name "$profile_name" --role-name "$role_name"
  sleep 10
fi

regions=(stockholm:eu-north-1 frankfurt:eu-central-1 ireland:eu-west-1 london:eu-west-2 paris:eu-west-3)
records=$(mktemp); trap 'rm -f "$records"' EXIT
declare -a security_groups public_ips
for entry in "${regions[@]}"; do
  name=${entry%%:*}; region=${entry##*:}
  progress "[$name/$region] Discovering network"
  vpc=$(aws ec2 describe-vpcs --region "$region" --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
  if [[ "$vpc" == None || -z "$vpc" ]]; then
    progress "[$name/$region] Creating default VPC"
    vpc=$(aws ec2 create-default-vpc --region "$region" --query 'Vpc.VpcId' --output text)
    sleep 10
  fi
  subnet=$(aws ec2 describe-subnets --region "$region" --filters "Name=vpc-id,Values=$vpc" Name=default-for-az,Values=true --query 'Subnets[0].SubnetId' --output text)
  [[ "$subnet" != None && -n "$subnet" ]] || { echo "No default public subnet in $region." >&2; exit 1; }
  if ! aws ec2 describe-key-pairs --region "$region" --key-names "$key_name" >/dev/null 2>&1; then
    [[ -n "$identity_file" && -f "$identity_file" ]] || { echo "[$name/$region] missing key pair $key_name; pass --identity-file PATH so it can be imported." >&2; exit 1; }
    progress "[$name/$region] Importing EC2 key pair $key_name"
    ssh-keygen -y -f "$identity_file" > "${identity_file}.pub"
    aws ec2 import-key-pair --region "$region" --key-name "$key_name" --public-key-material "fileb://${identity_file}.pub" >/dev/null
  fi
  sg_name="${project}-${name}"
  sg=$(aws ec2 describe-security-groups --region "$region" --filters "Name=vpc-id,Values=$vpc" "Name=group-name,Values=$sg_name" --query 'SecurityGroups[0].GroupId' --output text)
  if [[ "$sg" == None || -z "$sg" ]]; then
    progress "[$name/$region] Creating security group"
    sg=$(aws ec2 create-security-group --region "$region" --group-name "$sg_name" --description "GRESSE geo churn $name" --vpc-id "$vpc" --query GroupId --output text)
  fi
  aws ec2 authorize-security-group-ingress --region "$region" --group-id "$sg" --ip-permissions "IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=$controller_cidr}]" 2>/dev/null || true
  aws ec2 authorize-security-group-ingress --region "$region" --group-id "$sg" --ip-permissions "IpProtocol=tcp,FromPort=18080,ToPort=18084,IpRanges=[{CidrIp=$controller_cidr}]" 2>/dev/null || true
  instance=$(aws ec2 describe-instances --region "$region" --filters "Name=tag:Project,Values=$project" "Name=tag:RegionName,Values=$name" Name=instance-state-name,Values=pending,running,stopped,stopping --query 'Reservations[0].Instances[0].InstanceId' --output text)
  if [[ "$instance" == None || -z "$instance" ]]; then
    progress "[$name/$region] Launching $instance_type instance"
    ami=$(aws ssm get-parameter --region "$region" --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 --query 'Parameter.Value' --output text)
    instance=$(aws ec2 run-instances --region "$region" --image-id "$ami" --instance-type "$instance_type" --key-name "$key_name" --iam-instance-profile "Name=$profile_name" --subnet-id "$subnet" --associate-public-ip-address --security-group-ids "$sg" --tag-specifications "ResourceType=instance,Tags=[{Key=Project,Value=$project},{Key=RegionName,Value=$name},{Key=Name,Value=$project-$name}]" --query 'Instances[0].InstanceId' --output text)
  else
    state=$(aws ec2 describe-instances --region "$region" --instance-ids "$instance" --query 'Reservations[0].Instances[0].State.Name' --output text)
    if [[ "$state" == stopped ]]; then progress "[$name/$region] Starting existing instance $instance"; aws ec2 start-instances --region "$region" --instance-ids "$instance" >/dev/null; else progress "[$name/$region] Reusing instance $instance"; fi
  fi
  progress "[$name/$region] Waiting for EC2 status checks"
  aws ec2 wait instance-running --region "$region" --instance-ids "$instance"
  aws ec2 wait instance-status-ok --region "$region" --instance-ids "$instance"
  ip=$(aws ec2 describe-instances --region "$region" --instance-ids "$instance" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
  [[ "$ip" != None && -n "$ip" ]] || { echo "No public IP for $name." >&2; exit 1; }
  progress "[$name/$region] Ready at $ip"
  printf '%s\t%s\t%s\n' "$name" "$region" "$ip" >> "$records"; security_groups+=("$region:$sg"); public_ips+=("$ip")
done
progress "Authorizing replica-to-replica traffic across all five VMs"
for entry in "${security_groups[@]}"; do region=${entry%%:*}; sg=${entry##*:}; for ip in "${public_ips[@]}"; do aws ec2 authorize-security-group-ingress --region "$region" --group-id "$sg" --ip-permissions "IpProtocol=tcp,FromPort=19080,ToPort=19084,IpRanges=[{CidrIp=$ip/32}]" 2>/dev/null || true; done; done
topology_args=(--topology "$topology" --bucket "$bucket" --s3-region "$s3_region" --ssh-user "$ssh_user")
[[ -n "$identity_file" ]] && topology_args+=(--identity-file "$identity_file")
progress "Writing generated topology to $topology"
python3 "$root_dir/scripts/geo_experiment_write_topology.py" "${topology_args[@]}" < "$records"
progress "Provisioning complete. Next: ./scripts/geo-experiment/preflight.sh"
