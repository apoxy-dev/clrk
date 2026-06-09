#!/usr/bin/env bash
# Provision two SMALL VMs for the workerd exec-path bench: one arm64 (Graviton3)
# and one amd64 (AMD). 2 vCPU / 4 GiB each -- "not huge", representative of a
# small worker pod. AMIs are resolved live from Canonical's SSM public params
# so this does not rot.
set -euo pipefail
PROFILE=AdministratorAccess-925936573451
REGION=us-east-1
KEY=workerd-bench
D=/Users/dliyevsky/projects/clrk/.workerd-bench
PEM="$D/workerd-bench.pem"
SGNAME=workerd-bench-sg
A() { aws --profile "$PROFILE" --region "$REGION" "$@"; }

echo "== resolve Ubuntu 24.04 AMIs (SSM) =="
SSM=/aws/service/canonical/ubuntu/server/24.04/stable/current
AMI_ARM=$(A ssm get-parameters --names "$SSM/arm64/hvm/ebs-gp3/ami-id" --query "Parameters[0].Value" --output text)
AMI_AMD=$(A ssm get-parameters --names "$SSM/amd64/hvm/ebs-gp3/ami-id" --query "Parameters[0].Value" --output text)
echo "arm64=$AMI_ARM amd64=$AMI_AMD"
[ "$AMI_ARM" != None ] && [ "$AMI_AMD" != None ] || { echo "AMI lookup failed"; exit 1; }

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
  SG=$(A ec2 create-security-group --group-name "$SGNAME" --description "workerd exec-path bench" --vpc-id "$VPC" --query GroupId --output text)
fi
A ec2 authorize-security-group-ingress --group-id "$SG" --protocol tcp --port 22 --cidr "${MYIP}/32" >/dev/null 2>&1 || true
echo "$SG" > "$D/sg.id"
echo "sg $SG (ssh from ${MYIP}/32), vpc $VPC"

# 24G gp3 root: go toolchain + module cache + clrk src + workerd (~150M) + rootfs image.
BDM='[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":24,"VolumeType":"gp3","DeleteOnTermination":true}}]'

launch() {
  local name="$1" type="$2" ami="$3" id
  id=$(A ec2 run-instances --image-id "$ami" --instance-type "$type" --key-name "$KEY" \
       --security-group-ids "$SG" --block-device-mappings "$BDM" \
       --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$name},{Key=Batch,Value=workerd-bench}]" \
       --query "Instances[0].InstanceId" --output text)
  echo "$id	$name	$type"
}

echo "== launch =="
: > "$D/instances.tsv"
launch workerd-c7g-large c7g.large "$AMI_ARM" | tee -a "$D/instances.tsv"
launch workerd-c7a-large c7a.large "$AMI_AMD" | tee -a "$D/instances.tsv"
echo "ids -> $D/instances.tsv"

echo "== resolve public IPs (wait for assignment) =="
sleep 5
: > "$D/hosts.tsv"
while read -r id name type; do
  ip=None
  for _ in $(seq 1 20); do
    ip=$(A ec2 describe-instances --instance-ids "$id" --query "Reservations[0].Instances[0].PublicIpAddress" --output text)
    [ "$ip" != None ] && [ -n "$ip" ] && break
    sleep 3
  done
  echo "$id	$name	$type	$ip" | tee -a "$D/hosts.tsv"
done < "$D/instances.tsv"
echo "hosts -> $D/hosts.tsv"
