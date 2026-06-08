#!/usr/bin/env bash
# Provision 4 hosts: 2 same-core metal/VM pairs (arm64 64c, amd64 96c).
set -euo pipefail
PROFILE=AdministratorAccess-925936573451
REGION=us-east-1
KEY=hostmm-bench
PEM=/Users/dliyevsky/projects/clrk/.hostmm-bench/hostmm-bench.pem
SGNAME=hostmm-rcu-sg
AMI_ARM=ami-02fc05c04329f93ff
AMI_AMD=ami-0021ac0c2e69d9c55
A() { aws --profile "$PROFILE" --region "$REGION" "$@"; }

echo "== keypair =="
A ec2 delete-key-pair --key-name "$KEY" >/dev/null 2>&1 || true
A ec2 create-key-pair --key-name "$KEY" --query KeyMaterial --output text > "$PEM"
chmod 600 "$PEM"
echo "key $KEY -> $PEM"

echo "== security group =="
MYIP=$(curl -s https://checkip.amazonaws.com)
VPC=$(A ec2 describe-vpcs --filters Name=isDefault,Values=true --query "Vpcs[0].VpcId" --output text)
SG=$(A ec2 describe-security-groups --filters Name=group-name,Values=$SGNAME Name=vpc-id,Values=$VPC --query "SecurityGroups[0].GroupId" --output text 2>/dev/null || echo None)
if [ "$SG" = "None" ] || [ -z "$SG" ]; then
  SG=$(A ec2 create-security-group --group-name "$SGNAME" --description "hostmm rcu bench" --vpc-id "$VPC" --query GroupId --output text)
fi
A ec2 authorize-security-group-ingress --group-id "$SG" --protocol tcp --port 22 --cidr "${MYIP}/32" >/dev/null 2>&1 || true
echo "sg $SG (ssh from ${MYIP}/32), vpc $VPC"

BDM='[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":30,"VolumeType":"gp3","DeleteOnTermination":true}}]'

launch() {
  local name="$1" type="$2" ami="$3"
  local id
  id=$(A ec2 run-instances --image-id "$ami" --instance-type "$type" --key-name "$KEY" \
       --security-group-ids "$SG" --block-device-mappings "$BDM" \
       --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$name},{Key=Batch,Value=hostmm-rcu}]" \
       --query "Instances[0].InstanceId" --output text)
  echo "$id $name $type"
}

echo "== launch =="
: > /tmp/hostmm_rcu_ids.txt
launch hostmm-rcu-c7g-metal     c7g.metal       "$AMI_ARM" | tee -a /tmp/hostmm_rcu_ids.txt
launch hostmm-rcu-c7g-16xlarge  c7g.16xlarge    "$AMI_ARM" | tee -a /tmp/hostmm_rcu_ids.txt
launch hostmm-rcu-c7i-metal24xl c7i.metal-24xl  "$AMI_AMD" | tee -a /tmp/hostmm_rcu_ids.txt
launch hostmm-rcu-c7i-24xlarge  c7i.24xlarge    "$AMI_AMD" | tee -a /tmp/hostmm_rcu_ids.txt
echo "ids -> /tmp/hostmm_rcu_ids.txt"
