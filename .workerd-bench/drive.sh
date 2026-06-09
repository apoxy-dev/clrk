#!/usr/bin/env bash
# drive.sh <ip> <label> -- wait for ssh, ship the bundle, run setup + bench, pull results.
set -u
D=/Users/dliyevsky/projects/clrk/.workerd-bench
ip="$1"; label="$2"
mkdir -p "$D/results"
WORKERD_TAG=${WORKERD_TAG:-v1.20260606.1}
IMG=${IMG:-docker.io/library/node:bookworm-slim}
WARM_ITERS=${WARM_ITERS:-100}
COLD_ITERS=${COLD_ITERS:-30}
REQUESTS=${REQUESTS:-1}

echo "[$label] waiting for ssh $ip"
ok=0
for i in $(seq 1 60); do
  if bash "$D/ssh.sh" "$ip" true 2>/dev/null; then ok=1; break; fi
  sleep 5
done
[ "$ok" = 1 ] || { echo "[$label] SSH TIMEOUT"; exit 1; }

echo "[$label] staging bundle under ~/workerd-bench"
bash "$D/ssh.sh" "$ip" "mkdir -p ~/workerd-bench/results ~/workerd-bench/scripts"
bash "$D/push.sh" "$ip" "$D/clrk-src.tar.gz"        "workerd-bench/clrk-src.tar.gz"
for f in "$D"/worker/config*.capnp "$D"/worker/worker*.js; do
  [ -f "$f" ] && bash "$D/push.sh" "$ip" "$f" "workerd-bench/$(basename "$f")"
done
for f in "$D"/scripts/*; do
  [ -f "$f" ] && bash "$D/push.sh" "$ip" "$f" "workerd-bench/scripts/$(basename "$f")"
done

echo "[$label] setup_host.sh"
bash "$D/ssh.sh" "$ip" "cd ~/workerd-bench && WORKERD_TAG=$WORKERD_TAG bash scripts/setup_host.sh" \
  > "$D/results/setup.$label.log" 2>&1
src=$?
echo "[$label] setup exit=$src (log: results/setup.$label.log)"
tail -8 "$D/results/setup.$label.log"
[ "$src" = 0 ] || { echo "[$label] SETUP FAILED"; exit 1; }

echo "[$label] run_host.sh (warm=$WARM_ITERS cold=$COLD_ITERS req=$REQUESTS img=$IMG)"
bash "$D/ssh.sh" "$ip" "cd ~/workerd-bench && IMG=$IMG WARM_ITERS=$WARM_ITERS COLD_ITERS=$COLD_ITERS REQUESTS=$REQUESTS bash scripts/run_host.sh $label" \
  > "$D/results/drive.$label.log" 2>&1
echo "[$label] run exit=$?"

echo "[$label] pulling results"
mkdir -p "$D/results/$label"
bash "$D/ssh.sh" "$ip" "cd ~/workerd-bench && tar czf results.tgz results" 2>/dev/null
scp -i "$D/workerd-bench.pem" -o IdentitiesOnly=yes -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR \
    ubuntu@"$ip":workerd-bench/results.tgz "$D/results/$label/results.tgz" 2>/dev/null
( cd "$D/results/$label" && tar xzf results.tgz 2>/dev/null )
echo "[$label] DONE -> results/$label/"
