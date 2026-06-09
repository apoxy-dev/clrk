#!/usr/bin/env bash
# run_host.sh <label> -- for each worker bundle (echo floor + heavy), smoke-test
# workerd-under-gVisor then run the warm-binary + warm-request sweep.
set -uo pipefail
LABEL=${1:-host}
IMG=${IMG:-docker.io/library/node:bookworm-slim}
WARM_ITERS=${WARM_ITERS:-15}
COLD_ITERS=${COLD_ITERS:-0}
REQUESTS=${REQUESTS:-500}
BIN="$HOME/bench-runsc"
OUT="$HOME/workerd-bench/results"
mkdir -p "$OUT"

run_one() {
  local cfg="$1" tag="$2"
  echo "### [$tag] SMOKE ($cfg)"
  sudo "$BIN" -mode=exec-workerd -workerd-image="$IMG" -config-name="$cfg" \
    -warm-warmup=0 -warm-iters=1 -cold-iters=0 \
    > "$OUT/smoke.$LABEL.$tag.txt" 2>&1
  tail -20 "$OUT/smoke.$LABEL.$tag.txt"
  if ! grep -q 'HTTP/1.1 200' "$OUT/smoke.$LABEL.$tag.txt"; then
    echo "!!! [$tag] SMOKE FAILED -- see smoke.$LABEL.$tag.txt"
    return 1
  fi
  echo "### [$tag] FULL (warm=$WARM_ITERS req=$REQUESTS)"
  sudo "$BIN" -mode=exec-workerd -workerd-image="$IMG" -config-name="$cfg" \
    -warm-warmup=3 -warm-iters="$WARM_ITERS" -cold-iters="$COLD_ITERS" -requests="$REQUESTS" \
    -workerd-json="$OUT/rows.$LABEL.$tag.jsonl" \
    > "$OUT/run.$LABEL.$tag.txt" 2>"$OUT/run.$LABEL.$tag.err"
  echo "=========== run.$LABEL.$tag.txt ==========="
  cat "$OUT/run.$LABEL.$tag.txt"
}

run_one config.capnp       echo  || true
run_one config-heavy.capnp heavy || true
cp "$HOME/prereqs.txt" "$OUT/prereqs.$LABEL.txt" 2>/dev/null || true
echo "### done"
