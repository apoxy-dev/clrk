#!/usr/bin/env bash
# numa_test.sh -- Test 3 (collinearity breaker): remote-NUMA injection.
#
# Holds the CORE fixed (same cores, same ROB/LSQ) and moves ONLY the memory:
# CPUs on node 0 with memory local (node 0) vs. memory remote (node 1). On a
# multi-socket box remote DRAM adds ~50-100 ns of load latency with an
# otherwise identical core. If CORE_BOOT rises when memory moves while the ROB
# is unchanged, latency -- not the window -- is the lever (supports H2).
#
# Prints exactly ONE JSON object to stdout (status ok|skipped + reason).
# Single-node hosts (the xlarge VMs, single-socket metal) => skipped + reason.
set -u

# run_host.sh exports BENCH_BIN; accept that, a plain BENCH, then the default.
BENCH="${BENCH:-${BENCH_BIN:-$HOME/bench-runsc}}"
PY="${PY:-python3}"
ITERS="${ITERS:-30}"
WARMUP="${WARMUP:-5}"
PLATFORM="${PLATFORM:-systrap}"
CPUS="${CPUS:-0-3}"          # taskset pin (Sentry GOMAXPROCS=4)

emit() { printf '%s\n' "$1"; }
log()  { printf '%s\n' "$*" >&2; }

json_str() {
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  printf '"%s"' "$s"
}

skip() {
  local reason="$1" extra="${2:-}"
  local out
  out="{\"test\":\"numa\",\"status\":\"skipped\",\"reason\":$(json_str "$reason")"
  [ -n "$extra" ] && out="$out,$extra"
  out="$out}"
  emit "$out"
  exit 0
}

# ---- preconditions --------------------------------------------------------
[ -x "$BENCH" ] || skip "bench binary not found or not executable at $BENCH"
command -v taskset  >/dev/null 2>&1 || skip "taskset not available"
command -v "$PY"    >/dev/null 2>&1 || skip "python3 ($PY) not available"
command -v numactl  >/dev/null 2>&1 || skip "numactl not installed"

# Count NUMA nodes. `numactl -H` lists "available: N nodes".
NUMA_H="$(numactl -H 2>/dev/null || true)"
[ -n "$NUMA_H" ] || skip "numactl -H produced no output"

NNODES="$(printf '%s\n' "$NUMA_H" | "$PY" -c '
import sys, re
m=re.search(r"available:\s*(\d+)\s*nodes", sys.stdin.read())
print(m.group(1) if m else "0")
' 2>/dev/null)"
[ -n "$NNODES" ] || NNODES=0

topo_extra="\"numa_nodes\":$NNODES"

if [ "$NNODES" -lt 2 ]; then
  skip "single NUMA node (need >=2 nodes for remote-memory injection)" "$topo_extra"
fi

# Verify node 1 actually has memory to bind to (a memoryless node would make
# --membind=1 fail / fall back). Parse "node 1 size: <MB>".
NODE1_MB="$(printf '%s\n' "$NUMA_H" | "$PY" -c '
import sys, re
m=re.search(r"node\s+1\s+size:\s*(\d+)\s*MB", sys.stdin.read())
print(m.group(1) if m else "0")
' 2>/dev/null)"
[ -n "$NODE1_MB" ] || NODE1_MB=0
if [ "$NODE1_MB" -le 0 ]; then
  skip "node 1 has no memory (cannot bind remote memory)" "$topo_extra"
fi

# ---- measurement helper ---------------------------------------------------
# measure_p50 <out.jsonl> <numactl-args...> -> echoes CORE_BOOT p50 ms or empty.
measure_p50() {
  local jf="$1"; shift
  : > "$jf"
  # numactl wraps taskset wraps the bench. taskset still pins to the 0-3 CPU
  # mask, which lives on node 0; numactl --cpunodebind constrains the node and
  # --membind selects local vs remote DRAM for the Sentry's allocations.
  numactl "$@" taskset -c "$CPUS" "$BENCH" -platform="$PLATFORM" \
    -iters "$ITERS" -warmup "$WARMUP" -quiet -json "$jf" >/dev/null 2>&1 || true
  [ -s "$jf" ] || { echo ""; return; }
  "$PY" -c '
import sys, json
vals=[]
with open(sys.argv[1]) as fh:
    for ln in fh:
        ln=ln.strip()
        if not ln: continue
        try: o=json.loads(ln)
        except Exception: continue
        if o.get("warmup"): continue
        cb=o.get("core_boot")
        if cb is None: continue
        vals.append(cb/1e6)
if not vals:
    print(""); raise SystemExit
vals.sort()
print("%.6f" % vals[len(vals)//2])
' "$jf" 2>/dev/null
}

TMP="$(mktemp -d 2>/dev/null || echo /tmp/numa_test.$$)"
mkdir -p "$TMP"

log "numa_test: measuring LOCAL (cpunodebind=0 membind=0)"
LOCAL_P50="$(measure_p50 "$TMP/local.jsonl" --cpunodebind=0 --membind=0)"

log "numa_test: measuring REMOTE (cpunodebind=0 membind=1)"
REMOTE_P50="$(measure_p50 "$TMP/remote.jsonl" --cpunodebind=0 --membind=1)"

rm -rf "$TMP" 2>/dev/null || true

if [ -z "$LOCAL_P50" ] || [ -z "$REMOTE_P50" ]; then
  skip "bench produced no measurements under numactl (local or remote leg failed)" \
       "$topo_extra,\"node1_mb\":$NODE1_MB"
fi

DELTA_JSON="$("$PY" -c "
l=$LOCAL_P50; r=$REMOTE_P50
d=r-l
pct=(d/l*100.0) if l>0 else None
import json
print(json.dumps({'delta_ms':round(d,4),
                  'delta_pct':(round(pct,3) if pct is not None else None)}))
" 2>/dev/null)"
[ -n "$DELTA_JSON" ] || DELTA_JSON='{"delta_ms":null,"delta_pct":null}'

emit "{\"test\":\"numa\",\"status\":\"ok\",\"platform\":$(json_str "$PLATFORM"),\"cpus\":$(json_str "$CPUS"),\"iters\":$ITERS,\"warmup\":$WARMUP,$topo_extra,\"node1_mb\":$NODE1_MB,\"local_core_boot_p50_ms\":$LOCAL_P50,\"remote_core_boot_p50_ms\":$REMOTE_P50,\"delta\":$DELTA_JSON}"
exit 0
