#!/usr/bin/env bash
# run_one.sh <ip> <label> : wait for ssh, ship the bench bundle, run run_host.sh, pull results JSON.
set -u
D=/Users/dliyevsky/projects/clrk/.window-bench
HM=/Users/dliyevsky/projects/clrk/.hostmm-bench
ip="$1"; label="$2"

echo "[$label] waiting for ssh $ip (metal can take several minutes)"
ok=0
for i in $(seq 1 90); do
  if bash "$D/ssh.sh" "$ip" true 2>/dev/null; then ok=1; break; fi
  sleep 6
done
[ "$ok" = 1 ] || { echo "[$label] SSH TIMEOUT"; exit 1; }

echo "[$label] ssh up; staging bundle under ~/window"
bash "$D/ssh.sh" "$ip" "mkdir -p ~/window/scripts ~/window/results"
# clrk-src.tar.gz already staged by prewarm; re-push only if missing.
bash "$D/ssh.sh" "$ip" "test -f ~/window/clrk-src.tar.gz" \
  || bash "$D/push.sh" "$ip" "$HM/clrk-src.tar.gz" "window/clrk-src.tar.gz"
# microbench sources + all host scripts.
bash "$D/push.sh" "$ip" "$D/src/latency.c"     "window/latency.c"
bash "$D/push.sh" "$ip" "$D/src/mlp.c"         "window/mlp.c"
for f in "$D"/scripts/*; do
  [ -f "$f" ] && bash "$D/push.sh" "$ip" "$f" "window/scripts/$(basename "$f")"
done

echo "[$label] running run_host.sh (label=$label)"
bash "$D/ssh.sh" "$ip" "cd ~/window && sudo LABEL=$label bash scripts/run_host.sh $label" \
  > "$D/results/run_${label}.log" 2>&1
echo "[$label] run_host.sh exit=$?"

echo "[$label] pulling results"
mkdir -p "$D/results/$label"
bash "$D/ssh.sh" "$ip" "cd ~/window && tar czf results.tgz results" 2>/dev/null
scp -i "$D/window-bench.pem" -o IdentitiesOnly=yes -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR \
    ubuntu@"$ip":window/results.tgz "$D/results/$label/results.tgz" 2>/dev/null
( cd "$D/results/$label" && tar xzf results.tgz 2>/dev/null )
# Surface the per-host JSON at the top level for analyze.py.
cp "$D/results/$label/results/"results-*.json "$D/results/" 2>/dev/null
echo "[$label] DONE"
