#!/usr/bin/env bash
# hugepage_sweep.sh -- Test 3 (collinearity breaker): hugepage / TLB sweep.
#
# Holds the CORE (ROB/LSQ) fixed and changes ONLY the page size backing the
# Sentry's anonymous memory by sweeping transparent-hugepage policy:
#   never   -> 4K base pages   (most TLB entries consumed, most page walks)
#   madvise -> 4K unless madvised (CONTROL: expected == never unless a VMA
#              issues MADV_HUGEPAGE; the Sentry does not, so it should match
#              never -- it is a control, not an independent page-size point)
#   always  -> 2M THP          (fewer TLB entries, fewer/cheaper walks)
# CORE_BOOT vs. page size with the ROB fixed isolates the TLB / page-walk
# channel. If boot shrinks as pages grow, the page-walk cost (a memory-system
# property, not the window) was part of the boot floor -- supports H2.
#
# CRITICAL CAVEAT (and why this script also sets defrag + verifies):
#   Setting enabled=always ALONE does NOT back a short-lived process's heap with
#   2M pages within its lifetime. Each bench iteration fork+execs a fresh runsc
#   Sentry that boots /bin/true in ~80-160 ms and is deleted. Under the default
#   defrag=madvise, anonymous first-touch faults on a non-MADV_HUGEPAGE VMA are
#   served as 4K; promotion to 2M is done asynchronously by khugepaged (default
#   scan interval ~10 s) -- far longer than the Sentry lives. So we ALSO set
#   transparent_hugepage/defrag=always (saved/restored) for the 'always' leg so
#   first-touch faults synchronously allocate 2M, AND we VERIFY per-leg that the
#   boot processes actually used huge pages by snapshotting /proc/meminfo
#   AnonHugePages around each leg. If the 'always' leg shows no AnonHugePages
#   increase, the page-size channel was NOT isolated and that leg is reported
#   skipped -- never an 'ok' object measuring a 4K-backed Sentry mislabeled 2M.
#
# 1G hugetlbfs is best-effort: only attempted if nr_hugepages for the 1G size
# can actually be reserved; otherwise that leg is reported skipped (never
# fabricated).
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

THP="/sys/kernel/mm/transparent_hugepage/enabled"
THP_DEFRAG="/sys/kernel/mm/transparent_hugepage/defrag"
MEMINFO="/proc/meminfo"

emit() { printf '%s\n' "$1"; }
log()  { printf '%s\n' "$*" >&2; }

# AnonHugePages in kB from /proc/meminfo (0 if absent).
anon_hugepages_kb() {
  awk '/^AnonHugePages:/ {print $2; found=1} END{if(!found) print 0}' "$MEMINFO" 2>/dev/null || echo 0
}

json_str() {
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  printf '"%s"' "$s"
}

skip() {
  local reason="$1" extra="${2:-}"
  local out
  out="{\"test\":\"hugepage\",\"status\":\"skipped\",\"reason\":$(json_str "$reason")"
  [ -n "$extra" ] && out="$out,$extra"
  out="$out}"
  emit "$out"
  exit 0
}

# ---- preconditions --------------------------------------------------------
[ -x "$BENCH" ] || skip "bench binary not found or not executable at $BENCH"
command -v taskset >/dev/null 2>&1 || skip "taskset not available"
command -v "$PY"   >/dev/null 2>&1 || skip "python3 ($PY) not available"
[ -w "$THP" ] || skip "transparent_hugepage/enabled not writable (need root, or THP not built in)"

# The THP control files look like: "always [madvise] never" -- the bracketed
# token is the current setting. Parse it so we can restore exactly.
read_bracketed() {
  "$PY" -c '
import sys, re
m=re.search(r"\[(\w+)\]", open(sys.argv[1]).read())
print(m.group(1) if m else "")
' "$1" 2>/dev/null
}
read_thp_current() { read_bracketed "$THP"; }

ORIG_THP="$(read_thp_current)"
# CRITICAL: if we could not read the original policy, do NOT guess a default
# (guessing 'madvise' would silently restore a host whose real prior was
# never/always to the wrong state). Refuse to mutate host state instead.
if [ -z "$ORIG_THP" ]; then
  skip "could not read original transparent_hugepage/enabled setting; refusing to mutate host state"
fi

# defrag controls whether 'always' first-touch faults synchronously allocate 2M.
ORIG_DEFRAG=""
[ -r "$THP_DEFRAG" ] && ORIG_DEFRAG="$(read_bracketed "$THP_DEFRAG")"

restored=0
restore() {
  [ "$restored" = 1 ] && return
  restored=1
  printf '%s' "$ORIG_THP" > "$THP" 2>/dev/null || true
  [ -n "$ORIG_DEFRAG" ] && printf '%s' "$ORIG_DEFRAG" > "$THP_DEFRAG" 2>/dev/null || true
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

TMP="$(mktemp -d 2>/dev/null || echo /tmp/hugepage_sweep.$$)"
mkdir -p "$TMP"

# ---- THP sweep: never, madvise, always ------------------------------------
declare_results=""
exec_order=""
add_result() {
  # add_result <requested> <applied> <p50-or-empty> <hugepage_verified:yes|no|na> <note>
  local requested="$1" applied="$2" p50="$3" verified="$4" note="$5" obj fields
  # Record the APPLIED setting as the authoritative thp label (never the
  # requested one when they differ) plus the requested for audit.
  fields="\"thp\":$(json_str "$applied"),\"requested_thp\":$(json_str "$requested"),\"hugepage_verified\":$(json_str "$verified")"
  if [ -n "$p50" ]; then
    obj="{$fields,\"core_boot_p50_ms\":$p50"
  else
    obj="{$fields,\"core_boot_p50_ms\":null"
  fi
  [ -n "$note" ] && obj="$obj,\"note\":$(json_str "$note")"
  obj="$obj}"
  if [ -z "$declare_results" ]; then declare_results="$obj"; else declare_results="$declare_results,$obj"; fi
  exec_order="${exec_order:+$exec_order,}$(json_str "$applied")"
}

for setting in never madvise always; do
  # For the 'always' leg, also force defrag=always so first-touch faults
  # synchronously allocate 2M within the Sentry's short lifetime (khugepaged's
  # ~10s async promotion would otherwise never fire before the Sentry exits).
  if [ "$setting" = "always" ] && [ -w "$THP_DEFRAG" ]; then
    printf '%s' "always" > "$THP_DEFRAG" 2>/dev/null || true
  fi

  if ! printf '%s' "$setting" > "$THP" 2>/dev/null; then
    log "hugepage_sweep: could not set THP=$setting"
    add_result "$setting" "$setting" "" "na" "could not write THP setting"
    continue
  fi
  applied="$(read_thp_current)"
  [ -n "$applied" ] || applied="$setting"
  mismatch_note=""
  if [ "$applied" != "$setting" ]; then
    log "hugepage_sweep: THP requested=$setting but applied=$applied"
    mismatch_note="thp mismatch requested=$setting applied=$applied"
  fi

  # Snapshot AnonHugePages around the measurement to VERIFY huge pages were
  # actually used (the 'always' leg is only meaningful if 2M backing took).
  ahp_before="$(anon_hugepages_kb)"
  p50="$(measure_p50 "$TMP/thp_${setting}.jsonl")"
  ahp_after="$(anon_hugepages_kb)"
  ahp_delta=$(( ${ahp_after:-0} - ${ahp_before:-0} ))

  verified="na"
  if [ "$applied" = "always" ]; then
    # Require a real AnonHugePages increase during the leg; else the channel was
    # not isolated (4K-backed Sentry) -- report skipped, never a mislabeled 'ok'.
    if [ "$ahp_delta" -gt 0 ]; then
      verified="yes"
    else
      verified="no"
      log "hugepage_sweep: THP=always but AnonHugePages did not increase (delta=${ahp_delta}kB); 2M backing did not take"
      add_result "$setting" "$applied" "" "no" \
        "THP=always did not back the Sentry heap with huge pages (defrag/khugepaged timing); page-size channel not actually isolated; ${mismatch_note}"
      continue
    fi
  fi

  log "hugepage_sweep: requested=$setting applied=$applied verified=$verified anonhuge_delta=${ahp_delta}kB core_boot_p50=${p50:-NA}ms"
  add_result "$setting" "$applied" "$p50" "$verified" "$mismatch_note"

  # Brief quiesce between legs: flush dirty state and let khugepaged/compaction
  # settle so the next leg does not inherit this leg's transient huge-page free
  # lists / collapse activity. exec_order is recorded so the aggregator can still
  # test for order-confounding (legs run fixed never->madvise->always).
  sync 2>/dev/null || true
  sleep 0.5
done

# ---- 1G hugetlbfs (best-effort) ------------------------------------------
# We do NOT mount hugetlbfs or rewire the Sentry's allocator (out of scope and
# would not be honored without a target mapping); we only PROBE whether 1G
# pages can be reserved. If the host has no 1G hugepage size, or reservation
# fails, the 1G leg is reported skipped with a reason -- never fabricated.
hugetlb_json='{"attempted":false,"status":"skipped","reason":"1G hugetlbfs requires explicit Sentry allocator wiring; only reservation feasibility is probed here"}'
HP1G_DIR="/sys/kernel/mm/hugepages/hugepages-1048576kB"
if [ -d "$HP1G_DIR" ] && [ -w "$HP1G_DIR/nr_hugepages" ]; then
  before="$(cat "$HP1G_DIR/nr_hugepages" 2>/dev/null || echo 0)"
  # Try to reserve a single 1G page, then release it.
  if printf '1' > "$HP1G_DIR/nr_hugepages" 2>/dev/null; then
    got="$(cat "$HP1G_DIR/nr_hugepages" 2>/dev/null || echo 0)"
    free1g="$(cat "$HP1G_DIR/free_hugepages" 2>/dev/null || echo 0)"
    # restore prior reservation count.
    printf '%s' "$before" > "$HP1G_DIR/nr_hugepages" 2>/dev/null || true
    if [ "${got:-0}" -ge 1 ]; then
      hugetlb_json="{\"attempted\":true,\"status\":\"skipped\",\"reason\":\"1G pages reservable (nr=$got, free=$free1g) but Sentry allocator not wired to hugetlbfs in this bench; not measured to avoid fabrication\",\"reservable\":true,\"nr_reserved_probe\":$got}"
    else
      hugetlb_json='{"attempted":true,"status":"skipped","reason":"1G hugepages present but could not be reserved (insufficient contiguous memory)","reservable":false}'
    fi
  else
    hugetlb_json='{"attempted":true,"status":"skipped","reason":"writing nr_hugepages for 1G failed"}'
  fi
else
  hugetlb_json='{"attempted":false,"status":"skipped","reason":"no 1G hugepage size exposed (hugepages-1048576kB absent) or not writable"}'
fi

restore
rm -rf "$TMP" 2>/dev/null || true

# How many THP legs produced a measurement?
nmeas="$(printf '%s' "[$declare_results]" | "$PY" -c '
import sys, json
arr=json.loads(sys.stdin.read())
print(sum(1 for o in arr if o.get("core_boot_p50_ms") is not None))
' 2>/dev/null)"
[ -n "$nmeas" ] || nmeas=0

if [ "$nmeas" -lt 1 ]; then
  skip "no THP setting produced a measurement (bench failed across the sweep, or THP=always huge-page backing never took so the page-size channel was not isolated)" \
       "\"original_thp\":$(json_str "$ORIG_THP"),\"original_defrag\":$(json_str "${ORIG_DEFRAG:-unknown}"),\"thp_sweep\":[$declare_results],\"exec_order\":[$exec_order],\"hugetlb_1g\":$hugetlb_json"
fi

# madvise is a CONTROL leg expected to equal 'never' unless a VMA issues
# MADV_HUGEPAGE (the Sentry does not), recorded so it is not mistaken for an
# independent page-size point. exec_order lets the aggregator test for
# order-confounding (legs run fixed never->madvise->always).
emit "{\"test\":\"hugepage\",\"status\":\"ok\",\"platform\":$(json_str "$PLATFORM"),\"cpus\":$(json_str "$CPUS"),\"iters\":$ITERS,\"warmup\":$WARMUP,\"isolates\":\"TLB/page-walk channel (page size at fixed ROB)\",\"madvise_note\":\"madvise leg == never unless the Sentry issues MADV_HUGEPAGE; included as a control, not an independent page-size point\",\"exec_order\":[$exec_order],\"original_thp\":$(json_str "$ORIG_THP"),\"original_defrag\":$(json_str "${ORIG_DEFRAG:-unknown}"),\"thp_sweep\":[$declare_results],\"hugetlb_1g\":$hugetlb_json}"
exit 0
