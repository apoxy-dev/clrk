#!/usr/bin/env bash
# run_one.sh <ip> <label> : wait for ssh, push harness, run experiment, pull results.
set -u
D=/Users/dliyevsky/projects/clrk/.hostmm-bench
ip="$1"; label="$2"
PEM="$D/hostmm-bench.pem"
SCP="scp -i $PEM -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10"

echo "[$label] waiting for ssh $ip"
ok=0
for i in $(seq 1 70); do
  if bash "$D/ssh.sh" "$ip" true 2>/dev/null; then ok=1; break; fi
  sleep 6
done
[ "$ok" = 1 ] || { echo "[$label] SSH TIMEOUT"; exit 1; }

echo "[$label] ssh up; pushing harness"
bash "$D/push.sh" "$ip" "$D/rcubench.c"       rcubench.c
bash "$D/push.sh" "$ip" "$D/census.sh"        census.sh
bash "$D/push.sh" "$ip" "$D/rcu_run_host.sh"  rcu_run_host.sh

echo "[$label] running experiment"
bash "$D/ssh.sh" "$ip" "bash rcu_run_host.sh" > "$D/rcu_results_${label}.log" 2>&1

echo "[$label] pulling results"
mkdir -p "$D/rcu_results/$label"
bash "$D/ssh.sh" "$ip" "cd ~ && tar czf results.tgz results" 2>/dev/null
$SCP ubuntu@"$ip":results.tgz "$D/rcu_results/$label/results.tgz" 2>/dev/null
( cd "$D/rcu_results/$label" && tar xzf results.tgz 2>/dev/null )
echo "[$label] DONE label=$label"
