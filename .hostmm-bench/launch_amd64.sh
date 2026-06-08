#!/usr/bin/env bash
set -u
PROFILE=AdministratorAccess-925936573451; REGION=us-east-1
A(){ aws --profile "$PROFILE" --region "$REGION" "$@"; }
KEY=hostmm-bench; AMI_AMD=ami-0021ac0c2e69d9c55
SG=$(A ec2 describe-security-groups --filters Name=group-name,Values=hostmm-rcu-sg --query "SecurityGroups[0].GroupId" --output text)
BDM='[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":30,"VolumeType":"gp3","DeleteOnTermination":true}}]'

echo "waiting for arm64 vCPU quota to free..."
for i in $(seq 1 50); do
  inflight=$(A ec2 describe-instances --filters Name=tag:Batch,Values=hostmm-rcu Name=instance-state-name,Values=running,pending,shutting-down,stopping --query "length(Reservations[].Instances[])" --output text)
  echo "in-flight hostmm-rcu instances: $inflight"
  [ "$inflight" = "0" ] && break
  sleep 10
done

launch(){
  A ec2 run-instances --image-id "$AMI_AMD" --instance-type "$2" --key-name "$KEY" \
    --security-group-ids "$SG" --block-device-mappings "$BDM" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$1},{Key=Batch,Value=hostmm-rcu}]" \
    --query "Instances[0].InstanceId" --output text
}
echo "launching amd64 pair (96c each)"
id1=$(launch hostmm-rcu-c7i-metal24xl c7i.metal-24xl)
id2=$(launch hostmm-rcu-c7i-24xlarge  c7i.24xlarge)
: > /tmp/hostmm_amd64_ids.txt
echo "$id1 hostmm-rcu-c7i-metal24xl" | tee -a /tmp/hostmm_amd64_ids.txt
echo "$id2 hostmm-rcu-c7i-24xlarge"  | tee -a /tmp/hostmm_amd64_ids.txt
echo "AMD64 LAUNCHED"
