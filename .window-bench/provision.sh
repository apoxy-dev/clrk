#!/usr/bin/env bash
# Provision the window/ROB falsification matrix: Intel metal + Graviton3 metal + 2 xlarge VMs.
# 96 + 64 + 4 + 4 = 168 vCPU (under the 256 On-Demand Standard quota).
set -euo pipefail
PROFILE=AdministratorAccess-925936573451
REGION=us-east-1
KEY=window-bench
D=/Users/dliyevsky/projects/clrk/.window-bench
PEM="$D/window-bench.pem"
SGNAME=window-bench-sg
AMI_ARM=ami-02fc05c04329f93ff   # Ubuntu 24.04 arm64 (noble 20260604)
AMI_AMD=ami-0021ac0c2e69d9c55   # Ubuntu 24.04 amd64 (noble 20260604)
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
  SG=$(A ec2 create-security-group --group-name "$SGNAME" --description "window/ROB bench" --vpc-id "$VPC" --query GroupId --output text)
fi
A ec2 authorize-security-group-ingress --group-id "$SG" --protocol tcp --port 22 --cidr "${MYIP}/32" >/dev/null 2>&1 || true
echo "$SG" > "$D/sg.id"
echo "sg $SG (ssh from ${MYIP}/32), vpc $VPC"

# Metal needs a bigger root vol for the go toolchain + clrk-src; 40G gp3.
BDM='[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":40,"VolumeType":"gp3","DeleteOnTermination":true}}]'

launch() {
  local name="$1" type="$2" ami="$3" id
  id=$(A ec2 run-instances --image-id "$ami" --instance-type "$type" --key-name "$KEY" \
       --security-group-ids "$SG" --block-device-mappings "$BDM" \
       --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$name},{Key=Batch,Value=window-bench}]" \
       --query "Instances[0].InstanceId" --output text)
  echo "$id	$name	$type"
}

echo "== launch =="
: > "$D/instances.tsv"
launch window-c7i-metal24xl c7i.metal-24xl "$AMI_AMD" | tee -a "$D/instances.tsv"
launch window-c7g-metal     c7g.metal      "$AMI_ARM" | tee -a "$D/instances.tsv"
launch window-c7i-xlarge    c7i.xlarge     "$AMI_AMD" | tee -a "$D/instances.tsv"
launch window-c7g-xlarge    c7g.xlarge     "$AMI_ARM" | tee -a "$D/instances.tsv"
echo "ids -> $D/instances.tsv"
