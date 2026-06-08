#!/usr/bin/env bash
# pmu_intel.sh -- Test 1 (DECISIVE), x86-64 / Intel only.
#
# Top-down microarchitectural attribution of the runsc CORE_BOOT loop. The question this answers:
# are boot cycles Backend-Bound -> MEMORY-Bound (the core parked on L3/DRAM, waiting on outstanding
# misses) or Backend-Bound -> CORE-Bound (ROB/RS/dispatch ports full)? Under H1 (window-bound) a
# bigger ROB would help, which requires Core-Bound to dominate. Under H2 (memory-bound) the binding
# resource is memory latency imperfectly hidden by TRUE MLP (LFB/MSHR), not ROB depth, so a bigger
# ROB cannot help -- which shows up as Memory-Bound dominating with small Core-Bound.
#
# Method:
#   1) Andi Kleen pmu-tools `toplev.py -l3` system-wide under the bootloop -> Backend/Memory/Core %.
#   2) Raw `perf stat -a` for the corroborating stall ratios:
#        cycle_activity.stalls_l3_miss      (cycles stalled with an outstanding L3 miss)
#        cycle_activity.stalls_total        (total stall cycles -- any-memory stall proxy;
#                                            STALLS_MEM_ANY was dropped after Skylake/CSL and
#                                            does NOT exist on Sapphire Rapids/Golden Cove)
#        dtlb_load_misses.walk_active       (cycles a page-table walk is active)
#      each divided by cycles -> % of cycles. Every event is PROBED with
#      `perf stat -e <ev> -- true` first; only events perf can program are passed
#      to the real run, so one unresolvable name never aborts the whole batch.
#
# Caveat (recorded, not hidden): this counts system-wide on an idle pinned box, so it attributes
# ~all cycles to the boot work on cores 0-3. That is the intended attribution, but it is NOT
# per-process isolation; a noisy box would contaminate it. The script records whether the box
# looked idle so the JSON consumer can judge.
#
# NEVER fabricates: any event the PMU cannot deliver is recorded null with a note. An unrunnable
# environment (non-Intel, no perf, no pmu-tools) yields status "skipped" with a reason.
#
# Emits a single JSON object on stdout.
set -uo pipefail

OUTDIR="${OUTDIR:-$PWD}"
SELF_DIR="$(cd "$(dirname "$0")" && pwd)"
BOOTLOOP="$SELF_DIR/bootloop.sh"
PMUTOOLS_DIR="${PMUTOOLS_DIR:-$HOME/pmu-tools}"
TOPLEV_TIMEOUT="${TOPLEV_TIMEOUT:-90}"
RAW_TIMEOUT="${RAW_TIMEOUT:-60}"
HOSTN="$(hostname 2>/dev/null || echo unknown)"

TOPLEV_CSV="$OUTDIR/toplev.csv"
TOPLEV_LOG="$OUTDIR/toplev.log"
RAW_LOG="$OUTDIR/perf_raw_intel.log"

# ---- helpers ---------------------------------------------------------------------------------

# json_str: minimal JSON string escaper for free-text notes (ASCII only by contract).
json_str() {
  # Always called piped: `printf '%s' "$reason" | json_str`. The python branch
  # reads stdin; the fallback (python absent) must ALSO read stdin, not $1.
  python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))' 2>/dev/null \
    || printf '"%s"' "$(cat | tr -d '\n"\\')"
}

emit_skip() {
  # emit_skip <reason>
  local reason="$1"
  printf '{\n'
  printf '  "test": "pmu_intel",\n'
  printf '  "status": "skipped",\n'
  printf '  "reason": %s,\n' "$(printf '%s' "$reason" | json_str)"
  printf '  "host": %s,\n' "$(printf '%s' "$HOSTN" | json_str)"
  printf '  "arch": %s\n' "$(printf '%s' "$(uname -m)" | json_str)"
  printf '}\n'
  exit 0
}

# ---- preflight: must be x86-64 Intel, with perf, on an idle box -------------------------------

ARCH="$(uname -m)"
if [ "$ARCH" != "x86_64" ]; then
  emit_skip "not x86_64 (uname -m = $ARCH); pmu_intel only runs on Intel x86-64"
fi

VENDOR="$(grep -m1 '^vendor_id' /proc/cpuinfo 2>/dev/null | awk -F: '{gsub(/ /,"",$2); print $2}')"
if [ "$VENDOR" != "GenuineIntel" ]; then
  emit_skip "CPU vendor is '${VENDOR:-unknown}', not GenuineIntel; toplev TMA events are Intel-specific"
fi

if ! command -v perf >/dev/null 2>&1; then
  emit_skip "perf not installed; cannot run toplev or raw perf stat"
fi

if [ ! -x "$BOOTLOOP" ]; then
  emit_skip "bootloop workload not found/executable at $BOOTLOOP"
fi

# perf_event_paranoid must allow system-wide counting (-a). >1 blocks it for non-root.
PARANOID="$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo unknown)"
if [ "$(id -u)" != "0" ] && [ "$PARANOID" != "unknown" ] && [ "$PARANOID" -gt 1 ] 2>/dev/null; then
  emit_skip "perf_event_paranoid=$PARANOID and not root; system-wide (-a) PMU counting denied"
fi

# Record idleness so the consumer can weigh the system-wide attribution caveat. load-1min over the
# online CPU count; a near-idle box is required for the "~all cycles == boot work" attribution.
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

# ---- step 1: pmu-tools / toplev ---------------------------------------------------------------

TOPLEV_STATUS="ok"
TOPLEV_NOTE=""

if [ ! -d "$PMUTOOLS_DIR" ]; then
  if command -v git >/dev/null 2>&1; then
    echo "pmu_intel: cloning pmu-tools to $PMUTOOLS_DIR" >&2
    if ! timeout 120 git clone --depth 1 https://github.com/andikleen/pmu-tools "$PMUTOOLS_DIR" \
         >"$OUTDIR/pmu_clone.log" 2>&1; then
      TOPLEV_STATUS="unavailable"
      TOPLEV_NOTE="git clone of pmu-tools failed (see pmu_clone.log)"
    fi
  else
    TOPLEV_STATUS="unavailable"
    TOPLEV_NOTE="git not installed and pmu-tools absent at $PMUTOOLS_DIR"
  fi
fi

TOPLEV="$PMUTOOLS_DIR/toplev.py"
if [ "$TOPLEV_STATUS" = "ok" ] && [ ! -f "$TOPLEV" ]; then
  TOPLEV_STATUS="unavailable"
  TOPLEV_NOTE="toplev.py not found at $TOPLEV after clone"
fi

if [ "$TOPLEV_STATUS" = "ok" ]; then
  # -l3: drill to level 3 (Memory-Bound / Core-Bound live at L2; their children at L3). -a: all CPUs
  # (system-wide). --no-desc -x ,: terse CSV. -o: csv file. --no-mux: SUPPRESS the trailing
  # multiplexing-percentage column. At -l3 toplev needs more counters than SPR physically enables, so
  # it MUST multiplex and by default appends a MUX% cell to every row; without --no-mux the parser's
  # "last number in [0,100]" heuristic would latch onto that MUX% instead of the TMA slot percentage,
  # silently measuring the counter-multiplexing fraction. The bootloop is the measured command.
  echo "pmu_intel: running toplev -l3 -a --no-mux (timeout ${TOPLEV_TIMEOUT}s)" >&2
  if ! python3 "$TOPLEV" -l3 -a --no-mux --no-desc -x , -o "$TOPLEV_CSV" -- \
        timeout "$TOPLEV_TIMEOUT" bash "$BOOTLOOP" >"$TOPLEV_LOG" 2>&1; then
    # toplev exits nonzero if the workload's timeout fires (expected: timeout returns 124) or on a
    # real error. Keep going if the CSV exists; flag if it does not.
    if [ ! -s "$TOPLEV_CSV" ]; then
      TOPLEV_STATUS="error"
      TOPLEV_NOTE="toplev produced no CSV (see toplev.log); likely event-programming or perf error"
    else
      TOPLEV_NOTE="toplev exited nonzero but CSV present (workload timeout is expected)"
    fi
  fi
fi

# ---- step 2: raw perf stat for corroborating stall ratios -------------------------------------

RAW_STATUS="ok"
RAW_NOTE=""
RAW_CSV="$OUTDIR/perf_raw_intel.csv"

# Probe each candidate event individually before the real run. kernel-6.17 perf
# treats an UNKNOWN event name (e.g. cycle_activity.stalls_mem_any, which does
# NOT exist on Sapphire Rapids/Golden Cove) as a SYNTAX error and aborts the
# WHOLE comma-list batch with nonzero rc, counting nothing -- it never reaches
# the <not supported>/<not counted> runtime state the parser records as null. So
# we keep only events perf can actually program. cycle_activity.stalls_total is
# the SPR-correct any-memory stall proxy; we also try cycle_activity.cycles_mem_any
# (a cycles, not stalls, event) as a secondary if present.
CANDIDATE_EVENTS="cycle_activity.stalls_l3_miss cycle_activity.stalls_total cycle_activity.cycles_mem_any dtlb_load_misses.walk_active cycles instructions"
PRESENT_EVENTS=""
MISSING_EVENTS=""
for ev in $CANDIDATE_EVENTS; do
  if perf stat -e "$ev" -- true >/dev/null 2>&1; then
    PRESENT_EVENTS="${PRESENT_EVENTS:+$PRESENT_EVENTS,}$ev"
  else
    MISSING_EVENTS="${MISSING_EVENTS:+$MISSING_EVENTS,}$ev"
  fi
done

if [ -z "$PRESENT_EVENTS" ]; then
  RAW_STATUS="error"
  RAW_NOTE="none of the candidate Intel PMU events are programmable (missing: $MISSING_EVENTS)"
else
  echo "pmu_intel: running raw perf stat -a (present: $PRESENT_EVENTS; timeout ${RAW_TIMEOUT}s)" >&2
  # -x , gives machine-parseable CSV; -o writes it to a file. Any present event
  # reported <not supported>/<not counted> at runtime is recorded null by the parser.
  if ! perf stat -a -x , -o "$RAW_CSV" \
        -e "$PRESENT_EVENTS" \
        -- timeout "$RAW_TIMEOUT" bash "$BOOTLOOP" >"$RAW_LOG" 2>&1; then
    if [ ! -s "$RAW_CSV" ]; then
      RAW_STATUS="error"
      RAW_NOTE="perf stat produced no CSV (see perf_raw_intel.log)"
    else
      RAW_NOTE="perf stat exited nonzero but CSV present (workload timeout is expected)"
    fi
  fi
fi

# ---- parse + emit (python3: available on every host; pure stdlib) -----------------------------

OUTDIR="$OUTDIR" HOSTN="$HOSTN" ARCH="$ARCH" VENDOR="$VENDOR" \
TOPLEV_CSV="$TOPLEV_CSV" RAW_CSV="$RAW_CSV" \
TOPLEV_STATUS="$TOPLEV_STATUS" TOPLEV_NOTE="$TOPLEV_NOTE" \
RAW_STATUS="$RAW_STATUS" RAW_NOTE="$RAW_NOTE" \
MISSING_EVENTS="$MISSING_EVENTS" PRESENT_EVENTS="$PRESENT_EVENTS" \
IDLE_OK="$IDLE_OK" LOAD1="$LOAD1" NPROC="$NPROC" PARANOID="$PARANOID" \
TOPLEV_TIMEOUT="$TOPLEV_TIMEOUT" RAW_TIMEOUT="$RAW_TIMEOUT" \
python3 <<'PY'
import csv, json, os, re

outdir = os.environ["OUTDIR"]

def env(k, d=""):
    return os.environ.get(k, d)

notes = []

# ---- parse toplev CSV: find Backend_Bound, Memory_Bound, Core_Bound node percentages ----------
def parse_toplev(path):
    # toplev -x , rows look roughly like:
    #   CPU,timestamp(optional),node-name,value,unit,...   -- columns vary by version.
    # Node names are dotted hierarchical paths (Backend_Bound, Backend_Bound.Memory_Bound,
    # Backend_Bound.Core_Bound, ...). We match on the EXACT TERMINAL node name (last '.'-segment),
    # not a substring of the path -- otherwise "Backend_Bound" would also match the
    # "Backend_Bound.Memory_Bound" / "...Core_Bound" child rows and the result would be
    # order-dependent. We then take the value cell ADJACENT TO (immediately after) the matched
    # node-name cell, not "the last number in the row" -- the latter latches onto a trailing
    # multiplexing% column (suppressed by --no-mux, but we are robust either way).
    want = {
        # canonical-target -> set of acceptable terminal node tokens (normalized).
        "backend_bound_pct": ("backend_bound",),
        "memory_bound_pct":  ("memory_bound",),
        "core_bound_pct":    ("core_bound",),
    }
    found = {k: None for k in want}
    if not path or not os.path.exists(path) or os.path.getsize(path) == 0:
        return found, "toplev CSV missing or empty"
    try:
        with open(path, newline="") as f:
            rows = list(csv.reader(f))
    except Exception as e:
        return found, "toplev CSV unreadable: %s" % e

    def norm(tok):
        # Normalize a node token for comparison: take the terminal '.'-segment,
        # lower-case, and collapse spaces/hyphens to underscores.
        seg = tok.strip().split(".")[-1].strip()
        return seg.lower().replace(" ", "_").replace("-", "_")

    def value_after(row, idx):
        # First numeric cell in [0,100] strictly AFTER column idx (the node name).
        for cell in row[idx + 1:]:
            c = cell.strip().rstrip("%")
            try:
                v = float(c)
            except ValueError:
                continue
            if 0.0 <= v <= 100.0:
                return v
        return None

    for k, toks in want.items():
        for row in rows:
            # Find the cell whose terminal node name exactly equals a target token.
            for i, cell in enumerate(row):
                if norm(cell) in toks:
                    pct = value_after(row, i)
                    if pct is not None:
                        found[k] = round(pct, 3)
                    break
            if found[k] is not None:
                break
    miss = [k for k, v in found.items() if v is None]
    note = ("toplev: could not parse %s from CSV" % ",".join(miss)) if miss else ""
    return found, note

# ---- parse raw perf stat CSV: event -> count, then ratios over cycles --------------------------
def parse_perf(path):
    # perf stat -x , CSV columns: value,unit,event,run-time,pct,... ; value may be
    # "<not supported>" / "<not counted>" for an unavailable event -> record null.
    counts = {}
    supported = {}
    if not path or not os.path.exists(path) or os.path.getsize(path) == 0:
        return counts, supported, "perf raw CSV missing or empty"
    try:
        with open(path, newline="") as f:
            for row in csv.reader(f):
                if not row or row[0].startswith("#"):
                    continue
                if len(row) < 3:
                    continue
                val = row[0].strip()
                ev = row[2].strip()
                if not ev:
                    continue
                if val.startswith("<"):
                    supported[ev] = False
                    counts[ev] = None
                else:
                    try:
                        counts[ev] = float(val)
                        supported[ev] = True
                    except ValueError:
                        supported[ev] = False
                        counts[ev] = None
    except Exception as e:
        return counts, supported, "perf raw CSV unreadable: %s" % e
    return counts, supported, ""

tl, tl_note = parse_toplev(env("TOPLEV_CSV"))
counts, supported, perf_note = parse_perf(env("RAW_CSV"))

cycles = counts.get("cycles")

def pct_of_cycles(ev_name):
    v = counts.get(ev_name)
    if v is None or cycles is None or cycles == 0:
        return None
    return round(100.0 * v / cycles, 3)

stalls_l3   = pct_of_cycles("cycle_activity.stalls_l3_miss")
# SPR any-memory stall proxy: cycle_activity.stalls_total (STALLS_MEM_ANY does not
# exist on Golden Cove/Sapphire Rapids). Reported under stalls_total_pct_cycles.
stalls_total = pct_of_cycles("cycle_activity.stalls_total")
cycles_mem   = pct_of_cycles("cycle_activity.cycles_mem_any")
dtlb_walk   = pct_of_cycles("dtlb_load_misses.walk_active")

# Events perf could not even program (probed out before the run).
missing = [e for e in env("MISSING_EVENTS").split(",") if e]
for e in missing:
    notes.append("event %s not programmable on this Intel PMU (recorded null)" % e)
if "cycle_activity.stalls_total" in missing and "cycle_activity.cycles_mem_any" in missing:
    notes.append("no any-memory stall event programmable on this uarch; using stalls_l3_miss + dtlb_walk_active as the memory-bound corroborators")

# Per-event availability notes for events that WERE requested but reported
# <not counted> at runtime (never invent a value for an unsupported event).
for ev, label in (
    ("cycle_activity.stalls_l3_miss", "stalls_l3_miss"),
    ("cycle_activity.stalls_total", "stalls_total"),
    ("cycle_activity.cycles_mem_any", "cycles_mem_any"),
    ("dtlb_load_misses.walk_active", "dtlb_walk_active"),
    ("cycles", "cycles"),
    ("instructions", "instructions"),
):
    if supported.get(ev) is False:
        notes.append("event %s reported <not counted> by perf (recorded null)" % ev)

if env("TOPLEV_STATUS") != "ok":
    notes.append("toplev: %s" % (env("TOPLEV_NOTE") or env("TOPLEV_STATUS")))
elif tl_note:
    notes.append(tl_note)
elif env("TOPLEV_NOTE"):
    notes.append(env("TOPLEV_NOTE"))

if env("RAW_STATUS") != "ok":
    notes.append("perf raw: %s" % (env("RAW_NOTE") or env("RAW_STATUS")))
elif perf_note:
    notes.append(perf_note)
elif env("RAW_NOTE"):
    notes.append(env("RAW_NOTE"))

# System-wide attribution caveat -- always recorded.
notes.append(
    "system-wide counting on a pinned idle box attributes ~all cycles to the boot loop on cores 0-3; "
    "not per-process isolation"
)

idle_ok = env("IDLE_OK")
if idle_ok == "false":
    notes.append("box did NOT look idle (loadavg/nproc >= 0.30); attribution may be contaminated")

# Overall status: ok if we got at least the toplev memory/core split OR the raw stall ratios.
have_tma = tl["memory_bound_pct"] is not None and tl["core_bound_pct"] is not None
have_raw = stalls_total is not None or stalls_l3 is not None
status = "ok" if (have_tma or have_raw) else "error"
if not (have_tma or have_raw):
    notes.append("neither toplev TMA split nor raw stall ratios were obtained")

raw_paths = {}
for label, key in (("toplev_csv", "TOPLEV_CSV"), ("perf_raw_csv", "RAW_CSV")):
    p = env(key)
    if p and os.path.exists(p):
        raw_paths[label] = p
for label, fname in (("toplev_log", "toplev.log"), ("perf_raw_log", "perf_raw_intel.log")):
    p = os.path.join(outdir, fname)
    if os.path.exists(p):
        raw_paths[label] = p

# Build the analyze.py "t1_tma" contract: a `slots` dict whose memory_bound /
# core_bound are FRACTIONS in [0,1] (analyze.py multiplies by 100). We emit it
# only when the TMA split is actually present; otherwise null so the consumer
# treats T1 as skipped on this host rather than reading a fabricated split.
def frac(pct):
    return round(pct / 100.0, 5) if isinstance(pct, (int, float)) else None

slots = None
if have_tma:
    slots = {
        "backend_bound": frac(tl["backend_bound_pct"]),
        "memory_bound": frac(tl["memory_bound_pct"]),
        "core_bound": frac(tl["core_bound_pct"]),
    }

result = {
    "test": "pmu_intel",
    "status": status,
    "host": env("HOSTN"),
    "arch": env("ARCH"),
    "vendor": env("VENDOR"),
    # analyze.py contract: tool + slots (fractions in [0,1]). When slots is None
    # the analyzer falls back to skipped, never reading a partial split.
    "tool": "toplev",
    "slots": slots,
    "backend_bound_pct": tl["backend_bound_pct"],
    "memory_bound_pct": tl["memory_bound_pct"],
    "core_bound_pct": tl["core_bound_pct"],
    "stalls_l3_miss_pct_cycles": stalls_l3,
    "stalls_total_pct_cycles": stalls_total,
    "cycles_mem_any_pct_cycles": cycles_mem,
    "dtlb_walk_active_pct_cycles": dtlb_walk,
    "cycles": cycles,
    "instructions": counts.get("instructions"),
    "missing_events": missing,
    "idle_ok": (True if idle_ok == "true" else False if idle_ok == "false" else None),
    "loadavg_1min": (float(env("LOAD1")) if env("LOAD1") not in ("", "unknown") else None),
    "nproc": int(env("NPROC")) if env("NPROC").isdigit() else None,
    "perf_event_paranoid": env("PARANOID"),
    "toplev_timeout_s": int(env("TOPLEV_TIMEOUT")),
    "raw_timeout_s": int(env("RAW_TIMEOUT")),
    "raw_paths": raw_paths,
    "notes": notes,
}
print(json.dumps(result, indent=2))
PY
