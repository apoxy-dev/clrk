#!/usr/bin/env bash
# prefetch_msr.sh -- Test 3 (collinearity breaker): hardware-prefetcher toggle.
#
# x86 Intel only. Prefetch is one of the two mechanisms (the other is true MLP
# via LFBs/MSHRs) that hide DRAM latency the ROB gets credit for under H1. We
# measure CORE_BOOT p50 with prefetchers ENABLED (default) and then with all
# four core prefetchers DISABLED via MSR 0x1A4 = 0xf on the Sentry's cores
# (0-3), then restore. A jump in CORE_BOOT when prefetch is off means prefetch
# was hiding the latency -- i.e. the boot is memory-latency-bound and the
# window is not the binding resource (supports H2). No change would be evidence
# the other way and is reported faithfully.
#
# MSR 0x1A4 (MSR_MISC_FEATURE_CONTROL) bit map (1 = DISABLE):
#   bit0 L2 hardware prefetcher
#   bit1 L2 adjacent-cache-line prefetcher
#   bit2 DCU (L1 next-line) prefetcher
#   bit3 DCU IP prefetcher
# 0xf disables all four.
#
# Prints exactly ONE JSON object to stdout (status ok|skipped + reason).
set -u

# run_host.sh exports BENCH_BIN; accept that, a plain BENCH, then the default.
BENCH="${BENCH:-${BENCH_BIN:-$HOME/bench-runsc}}"
PY="${PY:-python3}"
ITERS="${ITERS:-30}"
WARMUP="${WARMUP:-5}"
PLATFORM="${PLATFORM:-systrap}"
CPUS="${CPUS:-0-3}"
MSR_PREFETCH="0x1A4"
PF_CORES="0 1 2 3"     # write MSR on each of the Sentry's pinned cores

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
  out="{\"test\":\"prefetch_msr\",\"status\":\"skipped\",\"reason\":$(json_str "$reason")"
  [ -n "$extra" ] && out="$out,$extra"
  out="$out}"
  emit "$out"
  exit 0
}

# ---- preconditions --------------------------------------------------------
[ -x "$BENCH" ] || skip "bench binary not found or not executable at $BENCH"
command -v taskset >/dev/null 2>&1 || skip "taskset not available"
command -v "$PY"   >/dev/null 2>&1 || skip "python3 ($PY) not available"

# run_host.sh exports ARCH (also from uname -m); honor it, else derive locally.
ARCH="${ARCH:-$(uname -m 2>/dev/null || echo unknown)}"
case "$ARCH" in
  x86_64|amd64) : ;;
  *) skip "not x86 (arch=$ARCH); MSR 0x1A4 prefetcher control is Intel-only" "\"arch\":$(json_str "$ARCH")" ;;
esac

# Confirm Intel vendor (AMD uses a different prefetch MSR layout).
VENDOR="$(grep -m1 -i '^vendor_id' /proc/cpuinfo 2>/dev/null | awk -F: '{gsub(/ /,"",$2);print $2}')"
if [ "$VENDOR" != "GenuineIntel" ]; then
  skip "CPU vendor is '${VENDOR:-unknown}', not GenuineIntel; MSR 0x1A4 layout is Intel-specific" \
       "\"arch\":$(json_str "$ARCH"),\"vendor\":$(json_str "${VENDOR:-unknown}")"
fi

command -v rdmsr >/dev/null 2>&1 || skip "rdmsr not found (install msr-tools)"
command -v wrmsr >/dev/null 2>&1 || skip "wrmsr not found (install msr-tools)"

# Ensure the msr module / /dev/cpu/N/msr is available; try to load it.
if [ ! -e /dev/cpu/0/msr ]; then
  modprobe msr >/dev/null 2>&1 || true
fi
[ -e /dev/cpu/0/msr ] || skip "msr device /dev/cpu/0/msr absent (msr module not loaded / not permitted)"

# Read the current MSR on cpu0 to confirm access works and to restore later.
ORIG_MSR="$(rdmsr -p 0 "$MSR_PREFETCH" 2>/dev/null || true)"
[ -n "$ORIG_MSR" ] || skip "rdmsr -p 0 $MSR_PREFETCH failed (no MSR read access; need root and msr module)"
# Normalize to 0x-prefixed lowercase hex.
ORIG_MSR_HEX="0x${ORIG_MSR}"

# Snapshot the original value on every core we intend to modify so restore is
# exact (cores can differ if firmware set them up unevenly).
declare -a ORIG_PER_CORE=()
for c in $PF_CORES; do
  v="$(rdmsr -p "$c" "$MSR_PREFETCH" 2>/dev/null || true)"
  ORIG_PER_CORE+=("$c=${v:-NA}")
done

restored=0
restore() {
  [ "$restored" = 1 ] && return
  restored=1
  for kv in "${ORIG_PER_CORE[@]}"; do
    local core="${kv%%=*}" val="${kv#*=}"
    [ "$val" = "NA" ] && continue
    wrmsr -p "$core" "$MSR_PREFETCH" "0x${val}" >/dev/null 2>&1 || true
  done
}
trap restore EXIT INT TERM

# ---- measurement helper ---------------------------------------------------
measure_p50() {
  local jf="$1"
  : > "$jf"
  taskset -c "$CPUS" "$BENCH" -platform="$PLATFORM" -iters "$ITERS" -warmup "$WARMUP" \
    -quiet -json "$jf" >/dev/null 2>&1 || true
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

TMP="$(mktemp -d 2>/dev/null || echo /tmp/prefetch_msr.$$)"
mkdir -p "$TMP"

# ---- leg 1: prefetchers ENABLED (default firmware state) ------------------
log "prefetch_msr: measuring with prefetch ENABLED (MSR $MSR_PREFETCH=$ORIG_MSR_HEX on cpu0)"
EN_P50="$(measure_p50 "$TMP/enabled.jsonl")"

# ---- disable all four prefetchers on the Sentry cores ---------------------
disabled_ok=0
for c in $PF_CORES; do
  if wrmsr -p "$c" "$MSR_PREFETCH" 0xf >/dev/null 2>&1; then
    disabled_ok=$((disabled_ok+1))
  else
    log "prefetch_msr: wrmsr disable failed on cpu$c"
  fi
done
if [ "$disabled_ok" -eq 0 ]; then
  restore
  rm -rf "$TMP" 2>/dev/null || true
  skip "could not disable prefetchers via wrmsr on any Sentry core" \
       "\"arch\":$(json_str "$ARCH"),\"vendor\":$(json_str "$VENDOR"),\"msr_0x1a4_enabled\":$(json_str "$ORIG_MSR_HEX")"
fi

# Read back cpu0 to record what disabled looks like.
DIS_MSR="$(rdmsr -p 0 "$MSR_PREFETCH" 2>/dev/null || echo NA)"
DIS_MSR_HEX="0x${DIS_MSR}"

log "prefetch_msr: measuring with prefetch DISABLED (MSR $MSR_PREFETCH=$DIS_MSR_HEX on cpu0)"
DIS_P50="$(measure_p50 "$TMP/disabled.jsonl")"

# ---- restore --------------------------------------------------------------
restore
rm -rf "$TMP" 2>/dev/null || true

if [ -z "$EN_P50" ] || [ -z "$DIS_P50" ]; then
  skip "bench produced no measurement (enabled or disabled leg failed)" \
       "\"arch\":$(json_str "$ARCH"),\"vendor\":$(json_str "$VENDOR"),\"msr_0x1a4_enabled\":$(json_str "$ORIG_MSR_HEX"),\"msr_0x1a4_disabled\":$(json_str "$DIS_MSR_HEX")"
fi

DELTA_JSON="$("$PY" -c "
e=$EN_P50; d=$DIS_P50
diff=d-e
pct=(diff/e*100.0) if e>0 else None
import json
print(json.dumps({'delta_ms':round(diff,4),
                  'delta_pct':(round(pct,3) if pct is not None else None)}))
" 2>/dev/null)"
[ -n "$DELTA_JSON" ] || DELTA_JSON='{"delta_ms":null,"delta_pct":null}'

emit "{\"test\":\"prefetch_msr\",\"status\":\"ok\",\"platform\":$(json_str "$PLATFORM"),\"cpus\":$(json_str "$CPUS"),\"iters\":$ITERS,\"warmup\":$WARMUP,\"arch\":$(json_str "$ARCH"),\"vendor\":$(json_str "$VENDOR"),\"msr\":\"0x1A4\",\"prefetch_cores\":$(json_str "$PF_CORES"),\"msr_0x1a4_enabled\":$(json_str "$ORIG_MSR_HEX"),\"msr_0x1a4_disabled\":$(json_str "$DIS_MSR_HEX"),\"enabled_core_boot_p50_ms\":$EN_P50,\"disabled_core_boot_p50_ms\":$DIS_P50,\"delta\":$DELTA_JSON}"
exit 0
