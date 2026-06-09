#!/usr/bin/env bash
# teardown.sh -- terminate the workerd-bench fleet, delete keypair + SG, and
# verify nothing is left billing. Idempotent.
set -uo pipefail
PROFILE=AdministratorAccess-925936573451
REGION=us-east-1
KEY=workerd-bench
SGNAME=workerd-bench-sg
D=/Users/dliyevsky/projects/clrk/.workerd-bench
A() { aws --profile "$PROFILE" --region "$REGION" "$@"; }

echo "== account =="; A sts get-caller-identity --query Account --output text

echo "== terminate instances tagged Batch=workerd-bench =="
IDS=$(A ec2 describe-instances \
  --filters Name=tag:Batch,Values=workerd-bench "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query "Reservations[].Instances[].InstanceId" --output text)
if [ -n "$IDS" ]; then
  echo "terminating: $IDS"
  A ec2 terminate-instances --instance-ids $IDS --query "TerminatingInstances[].[InstanceId,CurrentState.Name]" --output text
  echo "waiting for termination..."
  A ec2 wait instance-terminated --instance-ids $IDS && echo "all terminated"
else
  echo "no live instances"
fi

echo "== delete keypair =="
A ec2 delete-key-pair --key-name "$KEY" >/dev/null 2>&1 && echo "keypair deleted" || echo "no keypair"

echo "== delete security group =="
if [ -f "$D/sg.id" ]; then
  SG=$(cat "$D/sg.id")
  for _ in $(seq 1 10); do
    if A ec2 delete-security-group --group-id "$SG" >/dev/null 2>&1; then echo "sg $SG deleted"; break; fi
    sleep 5
  done
fi

echo "== verify clean =="
LEFT=$(A ec2 describe-instances \
  --filters Name=tag:Batch,Values=workerd-bench "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query "Reservations[].Instances[].InstanceId" --output text)
echo "remaining workerd-bench instances: ${LEFT:-none}"
