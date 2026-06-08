#!/usr/bin/env bash
# pmu_arm.sh -- Test 1 (DECISIVE), aarch64 / Arm only.
#
# The aarch64 twin of pmu_intel.sh. Same question: are runsc CORE_BOOT cycles Backend-Bound ->
# MEMORY-Bound (parked on L2/L3/DRAM, waiting on outstanding misses) or Backend-Bound -> CORE-Bound
# (dispatch/ROB-full)? Memory-bound dominant + small core-bound => a bigger ROB cannot help =>
# H1 (window-bound) falsified on this machine.
#
# Method:
#   1) Arm topdown-tool (Arm Telemetry Solution) Stage-1 backend/frontend split via `-m Topdown_L1`.
#      NOTE: there is NO "Backend->Memory vs Backend->Core" split on Arm. Arm's Stage-2 groups are
#      Cycle_Accounting / Cache_Effectiveness / Branch_Effectiveness (resource-effectiveness), not a
#      memory/core backend split. So we do NOT try to read a memory/core split out of topdown-tool;
#      we derive memory-bound attribution from the RAW events below.
#   2) Raw `perf stat -a` for the memory/core discriminator (the decisive H1-vs-H2 signal on Arm):
#        stall_backend       (TOTAL backend stall: memory + core/ROB combined -- NOT a discriminator)
#        stall_backend_mem   (backend stall attributable to MEMORY, event 0x4005 -- the PRIMARY
#                             memory-bound signal; implemented on Neoverse V1)
#        l1d_tlb_refill      (L1 dTLB refills)
#        dtlb_walk           (cycles/events with a hardware page-table walk -- page-walk corroborator)
#      each divided by cpu_cycles -> % of cycles. The core-bound remainder is
#      (stall_backend - stall_backend_mem).
#
# Decisive discriminator on Arm = STALL_BACKEND_MEM (memory) vs (STALL_BACKEND - STALL_BACKEND_MEM)
# (core/ROB). STALL_BACKEND ALONE is the TOTAL backend stall and is equally consistent with H1
# (core/ROB-full) and H2 (memory) -- it is exactly the quantity that must be SPLIT, so it is NOT by
# itself a memory-bound signal. If stall_backend_mem is genuinely absent we DO NOT invent it; status
# becomes "partial" (not "ok") so the consumer never mistakes the non-discriminating total for the
# decisive split.
#
# Caveat (recorded, not hidden): system-wide counting on an idle pinned box attributes ~all cycles
# to the boot work on cores 0-3. Intended attribution, not per-process isolation.
#
# NEVER fabricates: unavailable events are null + note. A non-Arm / no-perf / no-topdown-tool
# environment yields status "skipped" with a reason.
#
# Emits a single JSON object on stdout.
set -uo pipefail

OUTDIR="${OUTDIR:-$PWD}"
SELF_DIR="$(cd "$(dirname "$0")" && pwd)"
BOOTLOOP="$SELF_DIR/bootloop.sh"
TELEMETRY_DIR="${TELEMETRY_DIR:-$HOME/telemetry-solution}"
TOPDOWN_TIMEOUT="${TOPDOWN_TIMEOUT:-90}"
RAW_TIMEOUT="${RAW_TIMEOUT:-60}"
HOSTN="$(hostname 2>/dev/null || echo unknown)"

TOPDOWN_LOG="$OUTDIR/topdown.log"
RAW_LOG="$OUTDIR/perf_raw_arm.log"

# ---- helpers ---------------------------------------------------------------------------------

json_str() {
  # Always called piped: `printf '%s' "$reason" | json_str`. The fallback (python
  # absent) must ALSO read stdin, not $1.
  python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))' 2>/dev/null \
    || printf '"%s"' "$(cat | tr -d '\n"\\')"
}

emit_skip() {
  local reason="$1"
  printf '{\n'
  printf '  "test": "pmu_arm",\n'
  printf '  "status": "skipped",\n'
  printf '  "reason": %s,\n' "$(printf '%s' "$reason" | json_str)"
  printf '  "host": %s,\n' "$(printf '%s' "$HOSTN" | json_str)"
  printf '  "arch": %s\n' "$(printf '%s' "$(uname -m)" | json_str)"
  printf '}\n'
  exit 0
}

# ---- preflight: must be aarch64, with perf, on an idle box ------------------------------------

ARCH="$(uname -m)"
if [ "$ARCH" != "aarch64" ]; then
  emit_skip "not aarch64 (uname -m = $ARCH); pmu_arm only runs on Arm"
fi

if ! command -v perf >/dev/null 2>&1; then
  emit_skip "perf not installed; cannot run topdown-tool or raw perf stat"
fi

if [ ! -x "$BOOTLOOP" ]; then
  emit_skip "bootloop workload not found/executable at $BOOTLOOP"
fi

PARANOID="$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo unknown)"
if [ "$(id -u)" != "0" ] && [ "$PARANOID" != "unknown" ] && [ "$PARANOID" -gt 1 ] 2>/dev/null; then
  emit_skip "perf_event_paranoid=$PARANOID and not root; system-wide (-a) PMU counting denied"
fi

# CPU part for the record (Neoverse V1 = 0xd40); informational only, never used to fabricate.
CPU_PART="$(grep -m1 'CPU part' /proc/cpuinfo 2>/dev/null | awk -F: '{gsub(/ /,"",$2); print $2}')"

NPROC="$(nproc 2>/dev/null || echo 1)"
LOAD1="$(awk '{print $1}' /proc/loadavg 2>/dev/null || echo 0)"
IDLE_OK="$(python3 - "$LOAD1" "$NPROC" <<'PY' 2>/dev/null || echo unknown
import sys
try:
    load = float(sys.argv[1]); n = float(sys.argv[2]) or 1.0
    print("true" if (load / n) < 0.30 else "false")
except Exception:
    print("unknown")
PY
)"

mkdir -p "$OUTDIR"

# ---- step 1: Arm topdown-tool -----------------------------------------------------------------

TOPDOWN_STATUS="ok"
TOPDOWN_NOTE=""
TOPDOWN_BIN=""

# Resolve topdown-tool: prefer a PATH install, else the telemetry-solution clone.
# NOTE: topdown-tool is NOT published on PyPI (Arm ships it only inside the
# telemetry-solution repo: `git clone ... && pip install -e tools/topdown_tool`),
# so we do NOT attempt `pip3 install topdown-tool` -- it always fails and wastes
# ~120s. We go straight to the checked-in console-script after a shallow clone.
if command -v topdown-tool >/dev/null 2>&1; then
  TOPDOWN_BIN="topdown-tool"
elif [ -x "$TELEMETRY_DIR/tools/topdown_tool/topdown-tool" ]; then
  TOPDOWN_BIN="$TELEMETRY_DIR/tools/topdown_tool/topdown-tool"
else
  # Shallow git clone of the Arm telemetry-solution, then use its bundled
  # topdown-tool executable directly (no PyPI install needed).
  if command -v git >/dev/null 2>&1 && [ ! -d "$TELEMETRY_DIR" ] \
       && timeout 120 git clone --depth 1 \
            https://gitlab.arm.com/telemetry-solution/telemetry-solution.git "$TELEMETRY_DIR" \
            >"$OUTDIR/topdown_clone.log" 2>&1 \
       && [ -x "$TELEMETRY_DIR/tools/topdown_tool/topdown-tool" ]; then
    TOPDOWN_BIN="$TELEMETRY_DIR/tools/topdown_tool/topdown-tool"
  fi
fi

if [ -z "$TOPDOWN_BIN" ]; then
  TOPDOWN_STATUS="unavailable"
  TOPDOWN_NOTE="topdown-tool not installable (no pip wheel for this core and/or git clone failed); falling back to raw perf stat ratios"
else
  # -m Topdown_L1: Arm Stage-1 metric group (Frontend/Backend/Bad-Spec/Retiring). The Arm metric
  # group names use underscores (Topdown_L1, Cycle_Accounting, ...); the Intel/toplev "TopdownL1"/
  # "TopdownL2" nomenclature is NOT valid Arm syntax and there is NO Topdown_L2 memory/core split.
  # We therefore take only the Stage-1 backend% here and get the memory/core attribution from the
  # raw STALL_BACKEND_MEM event below. The bootloop is the measured command.
  echo "pmu_arm: running topdown-tool -m Topdown_L1 (timeout ${TOPDOWN_TIMEOUT}s)" >&2
  if ! "$TOPDOWN_BIN" -m Topdown_L1 -- \
        timeout "$TOPDOWN_TIMEOUT" bash "$BOOTLOOP" >"$TOPDOWN_LOG" 2>&1; then
    if [ ! -s "$TOPDOWN_LOG" ]; then
      TOPDOWN_STATUS="error"
      TOPDOWN_NOTE="topdown-tool produced no output (see topdown.log)"
    else
      TOPDOWN_NOTE="topdown-tool exited nonzero but produced output (workload timeout is expected)"
    fi
  fi
fi

# ---- step 2: raw perf stat for corroborating stall ratios -------------------------------------
# stall_backend_mem may not exist on Neoverse V1; perf errors out hard if ANY -e event is unknown.
# So we probe each event for existence first and only request the ones perf recognizes; the rest
# are recorded null by the parser. This keeps stall_backend / dtlb_walk usable as the fallback.

RAW_STATUS="ok"
RAW_NOTE=""
RAW_CSV="$OUTDIR/perf_raw_arm.csv"

CANDIDATE_EVENTS="stall_backend stall_backend_mem l1d_tlb_refill dtlb_walk cpu_cycles instructions"
PRESENT_EVENTS=""
MISSING_EVENTS=""
for ev in $CANDIDATE_EVENTS; do
  # `perf stat -e <ev> true` succeeds only if perf can program the event on this PMU.
  if perf stat -e "$ev" -- true >/dev/null 2>&1; then
    PRESENT_EVENTS="${PRESENT_EVENTS:+$PRESENT_EVENTS,}$ev"
  else
    MISSING_EVENTS="${MISSING_EVENTS:+$MISSING_EVENTS,}$ev"
  fi
done

if [ -z "$PRESENT_EVENTS" ]; then
  RAW_STATUS="error"
  RAW_NOTE="none of the requested Arm PMU events are available (missing: $MISSING_EVENTS)"
else
  echo "pmu_arm: running raw perf stat -a (present: $PRESENT_EVENTS; timeout ${RAW_TIMEOUT}s)" >&2
  if ! perf stat -a -x , -o "$RAW_CSV" \
        -e "$PRESENT_EVENTS" \
        -- timeout "$RAW_TIMEOUT" bash "$BOOTLOOP" >"$RAW_LOG" 2>&1; then
    if [ ! -s "$RAW_CSV" ]; then
      RAW_STATUS="error"
      RAW_NOTE="perf stat produced no CSV (see perf_raw_arm.log)"
    else
      RAW_NOTE="perf stat exited nonzero but CSV present (workload timeout is expected)"
    fi
  fi
fi

# ---- parse + emit -----------------------------------------------------------------------------

OUTDIR="$OUTDIR" HOSTN="$HOSTN" ARCH="$ARCH" CPU_PART="$CPU_PART" \
TOPDOWN_LOG="$TOPDOWN_LOG" RAW_CSV="$RAW_CSV" \
TOPDOWN_STATUS="$TOPDOWN_STATUS" TOPDOWN_NOTE="$TOPDOWN_NOTE" TOPDOWN_BIN="$TOPDOWN_BIN" \
RAW_STATUS="$RAW_STATUS" RAW_NOTE="$RAW_NOTE" \
MISSING_EVENTS="$MISSING_EVENTS" PRESENT_EVENTS="$PRESENT_EVENTS" \
IDLE_OK="$IDLE_OK" LOAD1="$LOAD1" NPROC="$NPROC" PARANOID="$PARANOID" \
TOPDOWN_TIMEOUT="$TOPDOWN_TIMEOUT" RAW_TIMEOUT="$RAW_TIMEOUT" \
python3 <<'PY'
import csv, json, os, re

outdir = os.environ["OUTDIR"]
def env(k, d=""): return os.environ.get(k, d)
notes = []

# ---- parse topdown-tool text output: pull only the Stage-1 Backend Bound % ---------------------
def parse_topdown(path):
    # topdown-tool Stage-1 (Topdown_L1) prints human-readable lines like:
    #   Backend Bound........................... 71.3% slots
    # There is NO memory/core split in Arm topdown output (Stage-2 is Cycle Accounting / Cache
    # Effectiveness / Branch Effectiveness, not Memory-Bound/Core-Bound), so we read ONLY the
    # backend_bound total here and derive memory vs core from the raw STALL_BACKEND_MEM event.
    targets = {
        "backend_bound_pct":  [r"backend\s*bound"],
    }
    found = {k: None for k in targets}
    if not path or not os.path.exists(path) or os.path.getsize(path) == 0:
        return found, "topdown-tool output missing or empty"
    try:
        with open(path, errors="replace") as f:
            lines = f.read().splitlines()
    except Exception as e:
        return found, "topdown-tool output unreadable: %s" % e

    pct_re = re.compile(r"(-?\d+(?:\.\d+)?)\s*%")
    for k, pats in targets.items():
        for line in lines:
            low = line.lower()
            if any(re.search(p, low) for p in pats):
                m = pct_re.search(line)
                if m:
                    try:
                        found[k] = round(float(m.group(1)), 3)
                        break
                    except ValueError:
                        pass
    miss = [k for k, v in found.items() if v is None]
    note = ("topdown-tool: could not parse %s" % ",".join(miss)) if miss else ""
    return found, note

# ---- parse raw perf stat CSV ------------------------------------------------------------------
def parse_perf(path):
    counts, supported = {}, {}
    if not path or not os.path.exists(path) or os.path.getsize(path) == 0:
        return counts, supported, "perf raw CSV missing or empty"
    try:
        with open(path, newline="") as f:
            for row in csv.reader(f):
                if not row or row[0].startswith("#") or len(row) < 3:
                    continue
                val, ev = row[0].strip(), row[2].strip()
                if not ev:
                    continue
                if val.startswith("<"):
                    supported[ev] = False; counts[ev] = None
                else:
                    try:
                        counts[ev] = float(val); supported[ev] = True
                    except ValueError:
                        supported[ev] = False; counts[ev] = None
    except Exception as e:
        return counts, supported, "perf raw CSV unreadable: %s" % e
    return counts, supported, ""

td, td_note = parse_topdown(env("TOPDOWN_LOG"))
counts, supported, perf_note = parse_perf(env("RAW_CSV"))

cycles = counts.get("cpu_cycles")
def pct_of_cycles(ev):
    v = counts.get(ev)
    if v is None or cycles is None or cycles == 0:
        return None
    return round(100.0 * v / cycles, 3)

stall_backend_pct     = pct_of_cycles("stall_backend")
stall_backend_mem_pct = pct_of_cycles("stall_backend_mem")
l1d_tlb_refill_pct    = pct_of_cycles("l1d_tlb_refill")
dtlb_walk_pct         = pct_of_cycles("dtlb_walk")

# Events perf could not even program (probed out before the run).
missing = [e for e in env("MISSING_EVENTS").split(",") if e]
for e in missing:
    notes.append("event %s not available on this Arm PMU (recorded null)" % e)
if "stall_backend_mem" in missing:
    notes.append("stall_backend_mem (0x4005) absent; STALL_BACKEND alone is the TOTAL backend stall (memory+core combined) and is NOT a memory/core discriminator -- T1 reported partial, not ok")
# Events present in the request but reported <not counted> by perf.
for ev in ("stall_backend", "stall_backend_mem", "l1d_tlb_refill", "dtlb_walk", "cpu_cycles", "instructions"):
    if supported.get(ev) is False:
        notes.append("event %s reported <not counted> by perf (recorded null)" % ev)

if env("TOPDOWN_STATUS") != "ok":
    notes.append("topdown-tool: %s" % (env("TOPDOWN_NOTE") or env("TOPDOWN_STATUS")))
elif td_note:
    notes.append(td_note)
elif env("TOPDOWN_NOTE"):
    notes.append(env("TOPDOWN_NOTE"))

if env("RAW_STATUS") != "ok":
    notes.append("perf raw: %s" % (env("RAW_NOTE") or env("RAW_STATUS")))
elif perf_note:
    notes.append(perf_note)
elif env("RAW_NOTE"):
    notes.append(env("RAW_NOTE"))

notes.append(
    "system-wide counting on a pinned idle box attributes ~all cycles to the boot loop on cores 0-3; "
    "not per-process isolation"
)
idle_ok = env("IDLE_OK")
if idle_ok == "false":
    notes.append("box did NOT look idle (loadavg/nproc >= 0.30); attribution may be contaminated")

# Derive the memory/core split on Arm from the RAW events (there is no topdown
# memory/core split). memory_bound_pct <- STALL_BACKEND_MEM (event 0x4005, the
# decisive memory-bound signal). core_bound_pct <- STALL_BACKEND - STALL_BACKEND_MEM
# (the core/ROB-full remainder). backend_bound_pct comes from Stage-1 topdown if
# present, else the raw STALL_BACKEND total.
backend_bound_pct = td["backend_bound_pct"]
if backend_bound_pct is None:
    backend_bound_pct = stall_backend_pct

memory_bound_pct = stall_backend_mem_pct
core_bound_pct = None
if stall_backend_pct is not None and stall_backend_mem_pct is not None:
    core_bound_pct = round(max(0.0, stall_backend_pct - stall_backend_mem_pct), 3)

# DECISIVE gate: T1 can answer H1-vs-H2 ONLY with a real memory/core discriminator
# (stall_backend_mem present). Without it, STALL_BACKEND alone cannot discriminate,
# so status is "partial" -- never "ok" -- so the consumer does not treat the total
# stall as the decisive split. status="ok" requires the discriminator.
have_discriminator = memory_bound_pct is not None and core_bound_pct is not None
have_any_backend = stall_backend_pct is not None or backend_bound_pct is not None
if have_discriminator:
    status = "ok"
elif have_any_backend:
    status = "partial"
    notes.append("no memory/core discriminator (stall_backend_mem absent); only the non-discriminating total backend stall is available -- cannot answer H1 vs H2 on this host")
else:
    status = "error"
    notes.append("neither a memory/core discriminator nor any backend-stall ratio was obtained")

raw_paths = {}
for label, key in (("perf_raw_csv", "RAW_CSV"),):
    p = env(key)
    if p and os.path.exists(p):
        raw_paths[label] = p
for label, fname in (("topdown_log", "topdown.log"), ("perf_raw_log", "perf_raw_arm.log")):
    p = os.path.join(outdir, fname)
    if os.path.exists(p):
        raw_paths[label] = p

# analyze.py "t1_tma" contract: a `slots` dict whose memory_bound / core_bound are
# FRACTIONS in [0,1] (analyze.py multiplies by 100). Emit it ONLY when the real
# memory/core discriminator is present; otherwise null so the consumer treats T1
# as skipped/partial rather than reading the non-discriminating total as a split.
def frac(pct):
    return round(pct / 100.0, 5) if isinstance(pct, (int, float)) else None

slots = None
if have_discriminator:
    slots = {
        "backend_bound": frac(backend_bound_pct),
        "memory_bound": frac(memory_bound_pct),
        "core_bound": frac(core_bound_pct),
    }

result = {
    "test": "pmu_arm",
    "status": status,
    "host": env("HOSTN"),
    "arch": env("ARCH"),
    "cpu_part": env("CPU_PART"),
    "topdown_tool": env("TOPDOWN_BIN") or None,
    # analyze.py contract: tool + slots (fractions in [0,1]); null unless the
    # decisive memory/core discriminator (stall_backend_mem) was measured.
    "tool": "topdown-tool+stall_backend_mem",
    "slots": slots,
    "backend_bound_pct": backend_bound_pct,
    "memory_bound_pct": memory_bound_pct,
    "core_bound_pct": core_bound_pct,
    "stall_backend_pct_cycles": stall_backend_pct,
    "stall_backend_mem_pct_cycles": stall_backend_mem_pct,
    "l1d_tlb_refill_pct_cycles": l1d_tlb_refill_pct,
    "dtlb_walk_pct_cycles": dtlb_walk_pct,
    "cpu_cycles": cycles,
    "instructions": counts.get("instructions"),
    "missing_events": missing,
    "idle_ok": (True if idle_ok == "true" else False if idle_ok == "false" else None),
    "loadavg_1min": (float(env("LOAD1")) if env("LOAD1") not in ("", "unknown") else None),
    "nproc": int(env("NPROC")) if env("NPROC").isdigit() else None,
    "perf_event_paranoid": env("PARANOID"),
    "topdown_timeout_s": int(env("TOPDOWN_TIMEOUT")),
    "raw_timeout_s": int(env("RAW_TIMEOUT")),
    "raw_paths": raw_paths,
    "notes": notes,
}
print(json.dumps(result, indent=2))
PY
