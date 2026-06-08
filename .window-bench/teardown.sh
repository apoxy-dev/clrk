#!/usr/bin/env bash
set -uo pipefail
PROFILE=AdministratorAccess-925936573451; REGION=us-east-1
A(){ aws --profile $PROFILE --region $REGION "$@"; }
A sts get-caller-identity --query Account --output text || { echo "RUN: aws sso login --profile $PROFILE"; exit 1; }
IDS=$(A ec2 describe-instances --filters Name=tag:Batch,Values=window-bench Name=instance-state-name,Values=running,pending,stopping,stopped --query 'Reservations[].Instances[].InstanceId' --output text)
echo "terminating: ${IDS:-<none>}"
[ -n "$IDS" ] && A ec2 terminate-instances --instance-ids $IDS --query 'TerminatingInstances[].[InstanceId,CurrentState.Name]' --output text
A ec2 delete-key-pair --key-name window-bench && echo "keypair deleted"
[ -n "$IDS" ] && A ec2 wait instance-terminated --instance-ids $IDS && echo "all terminated"
SG=$(cat /Users/dliyevsky/projects/clrk/.window-bench/sg.id 2>/dev/null)
[ -n "${SG:-}" ] && { A ec2 delete-security-group --group-id "$SG" && echo "sg $SG deleted" || echo "sg delete deferred (retry shortly)"; }
echo "REMAINING window-bench:"; A ec2 describe-instances --filters Name=tag:Batch,Values=window-bench Name=instance-state-name,Values=running,pending,stopping,stopped --query 'Reservations[].Instances[].InstanceId' --output text | sed 's/^$/<none>/'
