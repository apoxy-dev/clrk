#!/usr/bin/env bash
# freq_sweep.sh -- Test 2: CPU frequency sweep for the window/ROB falsification.
#
# Splits CORE_BOOT into a clock-scaling compute fraction and a clock-invariant
# memory-latency floor by measuring CORE_BOOT p50 at several CPU frequencies
# (ROB/LSQ held constant -- only the clock moves) and fitting the model
#
#     CORE_BOOT(f) = a/f + b
#
# via ordinary least squares in 1/f. The slope `a` is the compute work that
# scales with clock period; the intercept `b` is the part of boot that does
# NOT shrink as the clock rises -- the memory-latency / page-walk floor. A
# large `b` supports H2 (memory-bound); a non-trivial `a` separately rebuts the
# blog's "not clock-bound" assertion.
#
# Prints exactly ONE JSON object to stdout (status ok|skipped + reason).
# Never fabricates: if frequencies are not settable, status=skipped and we
# still record the available frequency list. Diagnostics go to stderr.
set -u

# ---- knobs (overridable from the orchestrator) ----------------------------
# run_host.sh exports BENCH_BIN; accept that, a plain BENCH, then the default.
BENCH="${BENCH:-${BENCH_BIN:-$HOME/bench-runsc}}"
PY="${PY:-python3}"
ITERS="${ITERS:-30}"
WARMUP="${WARMUP:-5}"
PLATFORM="${PLATFORM:-systrap}"
CPUS="${CPUS:-0-3}"            # taskset pin (Sentry GOMAXPROCS=4)
GOVCPU="${GOVCPU:-0}"          # representative cpu for sysfs cpufreq reads

emit() { printf '%s\n' "$1"; }   # single-line JSON to stdout
log()  { printf '%s\n' "$*" >&2; }

# json_str -- emit a JSON string literal (ASCII, escape backslash and quote).
json_str() {
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  printf '"%s"' "$s"
}

skip() {
  local reason="$1" extra="${2:-}"
  local out
  out="{\"test\":\"freq_sweep\",\"status\":\"skipped\",\"reason\":$(json_str "$reason")"
  [ -n "$extra" ] && out="$out,$extra"
  out="$out}"
  emit "$out"
  exit 0
}

# ---- preconditions --------------------------------------------------------
[ -x "$BENCH" ] || skip "bench binary not found or not executable at $BENCH"
command -v taskset >/dev/null 2>&1 || skip "taskset not available"
command -v "$PY"   >/dev/null 2>&1 || skip "python3 ($PY) not available"

# Collect the available-frequencies list up front so we can report it even on
# skip (the task requires recording scaling_available_frequencies regardless).
SYSCPU="/sys/devices/system/cpu/cpu${GOVCPU}/cpufreq"
avail_raw=""
[ -r "$SYSCPU/scaling_available_frequencies" ] && \
  avail_raw="$(cat "$SYSCPU/scaling_available_frequencies" 2>/dev/null)"
# Build a JSON array of available freqs (kHz integers) for the report.
avail_json="[]"
if [ -n "$avail_raw" ]; then
  avail_json="$(printf '%s\n' "$avail_raw" | "$PY" -c '
import sys
toks=sys.stdin.read().split()
out=[]
for t in toks:
    try: out.append(str(int(t)))
    except ValueError: pass
print("["+",".join(out)+"]")
' 2>/dev/null)"
  [ -n "$avail_json" ] || avail_json="[]"
fi
avail_extra="\"scaling_available_frequencies_khz\":$avail_json"

command -v cpupower >/dev/null 2>&1 || skip "cpupower not installed" "$avail_extra"
[ -d /sys/devices/system/cpu/cpu0/cpufreq ] || skip "no cpufreq sysfs (frequency not settable)" "$avail_extra"

# Determine the set of frequencies we can actually set. Prefer the discrete
# available list (acpi-cpufreq); fall back to min/mid/max from the hardware
# limits (intel_pstate passive / cppc with a continuous range).
pick_freqs() {
  # echoes a space-separated list of kHz to try, low..high.
  local toks
  toks="$(printf '%s\n' "$avail_raw" | tr -s ' \t' '\n' | grep -E '^[0-9]+$' || true)"
  if [ -n "$toks" ]; then
    # Discrete list. Choose min, max, and a few interior points (<=5 total).
    printf '%s\n' "$toks" | "$PY" -c '
import sys
fs=sorted(set(int(x) for x in sys.stdin.read().split() if x.isdigit()))
if not fs:
    sys.exit(0)
if len(fs)<=5:
    sel=fs
else:
    # min, max, and 3 spread interior indices.
    idx=sorted(set([0, len(fs)//4, len(fs)//2, (3*len(fs))//4, len(fs)-1]))
    sel=[fs[i] for i in idx]
print(" ".join(str(x) for x in sorted(set(sel))))
'
    return
  fi
  # Continuous range: read hardware min/max and synthesize points.
  local fmin fmax
  fmin="$(cat "$SYSCPU/cpuinfo_min_freq" 2>/dev/null || true)"
  fmax="$(cat "$SYSCPU/cpuinfo_max_freq" 2>/dev/null || true)"
  [ -n "$fmin" ] && [ -n "$fmax" ] || return 0
  "$PY" -c "
fmin=$fmin; fmax=$fmax
if fmax<=fmin:
    print(fmax); raise SystemExit
pts=sorted(set(int(fmin+(fmax-fmin)*k/4) for k in range(5)))
print(' '.join(str(p) for p in pts))
"
}

FREQS="$(pick_freqs 2>/dev/null || true)"
[ -n "$FREQS" ] && [ "$FREQS" != " " ] || skip "no settable frequencies discoverable" "$avail_extra"

# Count distinct freqs; need >=2 to fit a line.
nfreq="$(printf '%s\n' $FREQS | grep -cE '^[0-9]+$' || echo 0)"
[ "$nfreq" -ge 2 ] || skip "fewer than 2 settable frequencies (cannot fit a/f+b)" "$avail_extra"

# ---- save + restore governor state ---------------------------------------
ORIG_GOV=""
[ -r "$SYSCPU/scaling_governor" ] && ORIG_GOV="$(cat "$SYSCPU/scaling_governor" 2>/dev/null)"
restored=0
restore() {
  [ "$restored" = 1 ] && return
  restored=1
  if [ -n "$ORIG_GOV" ]; then
    cpupower frequency-set -g "$ORIG_GOV" >/dev/null 2>&1 || true
  else
    # ORIG_GOV was unreadable, but we may have switched to userspace below; do not
    # leave the host pinned on userspace at the sweep's last frequency. Fall back
    # to a sane non-userspace governor.
    cpupower frequency-set -g performance >/dev/null 2>&1 \
      || cpupower frequency-set -g ondemand >/dev/null 2>&1 \
      || cpupower frequency-set -g schedutil >/dev/null 2>&1 \
      || true
  fi
}
trap restore EXIT INT TERM

# Switch to userspace governor so a fixed frequency can be pinned.
if ! cpupower frequency-set -g userspace >/dev/null 2>&1; then
  restore
  skip "userspace governor unavailable (cannot pin frequency)" "$avail_extra"
fi

# ---- measurement helper ---------------------------------------------------
# measure_p50 <out.jsonl> -> echoes CORE_BOOT p50 in ms, or empty on failure.
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
        if not ln:
            continue
        try:
            o=json.loads(ln)
        except Exception:
            continue
        if o.get("warmup"):
            continue
        cb=o.get("core_boot")
        if cb is None:
            continue
        vals.append(cb/1e6)   # core_boot is int nanoseconds -> ms
if not vals:
    print(""); raise SystemExit
vals.sort()
print("%.6f" % vals[len(vals)//2])
' "$jf" 2>/dev/null
}

# ---- sweep ----------------------------------------------------------------
TMP="$(mktemp -d 2>/dev/null || echo /tmp/freq_sweep.$$)"
mkdir -p "$TMP"
points_json=""
npoints=0
accepted_actuals=""   # space-separated actual_khz of points whose pin took.
for f in $FREQS; do
  case "$f" in (*[!0-9]*|"") continue;; esac
  if ! cpupower frequency-set -f "$f" >/dev/null 2>&1; then
    log "freq_sweep: could not set ${f}kHz, skipping that point"
    continue
  fi
  # Settle: P-state transitions are not instantaneous; let the request take
  # effect before reading back / measuring (bench warmup absorbs the remainder).
  sleep 0.2
  # Read the applied frequency before AND after the measurement window and average
  # them, so the x-coordinate represents the frequency boot was actually run at
  # (not a single instantaneous read that may snap back between iterations).
  actual_before="$(cat "$SYSCPU/scaling_cur_freq" 2>/dev/null || echo "$f")"
  p50="$(measure_p50 "$TMP/iter_${f}.jsonl")"
  actual_after="$(cat "$SYSCPU/scaling_cur_freq" 2>/dev/null || echo "$actual_before")"
  if [ -z "$p50" ]; then
    log "freq_sweep: no measurement at ${f}kHz (bench produced no rows)"
    continue
  fi
  # Mean of before/after as the representative applied frequency.
  actual="$("$PY" -c '
import sys
try:
    a=int(sys.argv[1]); b=int(sys.argv[2]); print((a+b)//2)
except Exception:
    print(sys.argv[1])
' "${actual_before:-$f}" "${actual_after:-$f}" 2>/dev/null)"
  [ -n "$actual" ] || actual="$f"

  # CRITICAL: require the APPLIED frequency to track the REQUEST before accepting
  # the point. On intel_pstate active mode / many EC2 guests the userspace target
  # is silently ignored and every core stays at one P-state; without this gate the
  # tiny telemetry jitter across set_khz values yields a nonzero OLS denominator
  # and a fabricated clock/floor split from pure noise. Skip points where the
  # applied freq deviates >5% from the request.
  drift_ok="$("$PY" -c '
import sys
try:
    req=float(sys.argv[1]); act=float(sys.argv[2])
    print("yes" if req>0 and abs(act-req)/req<=0.05 else "no")
except Exception:
    print("no")
' "$f" "$actual" 2>/dev/null)"
  if [ "$drift_ok" != "yes" ]; then
    log "freq_sweep: set=${f}kHz but applied=${actual}kHz (>5% drift); governor target not honored, skipping point"
    continue
  fi

  log "freq_sweep: set=${f}kHz actual=${actual}kHz core_boot_p50=${p50}ms"
  pt="{\"set_khz\":$f,\"actual_khz\":${actual:-$f},\"core_boot_p50_ms\":$p50}"
  if [ -z "$points_json" ]; then points_json="$pt"; else points_json="$points_json,$pt"; fi
  accepted_actuals="$accepted_actuals $actual"
  npoints=$((npoints+1))
done

restore

rm -rf "$TMP" 2>/dev/null || true

if [ "$npoints" -lt 2 ]; then
  skip "fewer than 2 usable measurement points (bench failed or frequency target not honored at most freqs)" \
       "$avail_extra,\"points\":[${points_json}]"
fi

# CRITICAL: before fitting, require the ACCEPTED frequencies to actually span a
# range. If every accepted point landed at ~the same applied frequency (governor
# ignored the targets, telemetry jitter only), an OLS fit of CORE_BOOT vs 1/f is
# garbage no matter how clean it looks. Demand >=10% spread; else honest skip.
freq_spread_ok="$(printf '%s\n' "$accepted_actuals" | "$PY" -c '
import sys
vals=[float(x) for x in sys.stdin.read().split() if x.strip()]
if len(vals)<2:
    print("no"); raise SystemExit
lo, hi = min(vals), max(vals)
print("yes" if lo>0 and (hi-lo)/lo>=0.10 else "no")
' 2>/dev/null)"
if [ "$freq_spread_ok" != "yes" ]; then
  skip "frequency did not vary (governor target not honored; actual freq spread < 10%)" \
       "$avail_extra,\"points\":[${points_json}]"
fi

# ---- fit CORE_BOOT(f) = a/f + b ------------------------------------------
# Least squares with x = 1/f_GHz, y = core_boot_ms. We use the ACTUAL applied
# frequency for x. a has units ms*GHz (compute work), b has units ms (floor).
fit_json="$(printf '%s' "[$points_json]" | "$PY" -c '
import sys, json
pts=json.loads(sys.stdin.read())
xs=[]; ys=[]
for p in pts:
    f_khz=p.get("actual_khz") or p.get("set_khz")
    y=p.get("core_boot_p50_ms")
    if not f_khz or y is None:
        continue
    f_ghz=f_khz/1.0e6
    if f_ghz<=0:
        continue
    xs.append(1.0/f_ghz)   # x = 1/f in 1/GHz
    ys.append(y)
n=len(xs)
if n<2:
    print(json.dumps({"ok":False,"reason":"insufficient points for fit"}))
    raise SystemExit
sx=sum(xs); sy=sum(ys)
sxx=sum(x*x for x in xs); sxy=sum(x*y for x,y in zip(xs,ys))
den=n*sxx-sx*sx
if den==0:
    print(json.dumps({"ok":False,"reason":"degenerate fit (all freqs equal)"}))
    raise SystemExit
a=(n*sxy-sx*sy)/den          # slope: ms*GHz (compute, clock-scaling)
b=(sy-a*sx)/n                # intercept: ms (clock-invariant memory floor)
# R^2 for the fit quality.
ybar=sy/n
ss_tot=sum((y-ybar)**2 for y in ys)
ss_res=sum((y-(a*x+b))**2 for x,y in zip(xs,ys))
r2=(1-ss_res/ss_tot) if ss_tot>0 else None
# Fraction of boot that is the clock-invariant floor at the highest measured freq.
fmax_ghz=max(1.0/x for x in xs)
y_at_fmax=a/fmax_ghz+b
floor_frac = (b/y_at_fmax) if y_at_fmax>0 else None
# PHYSICAL-PLAUSIBILITY GATE: the a/f+b model is only meaningful when the fit
# holds and the parameters are physical. a<0 means boot RISES with clock
# (unphysical); b<0 is a negative floor; a floor fraction outside [0,1] is
# nonsense; r2 below 0.5 means b is noise, not a clock-invariant floor. Any of
# these => the fit does not describe a real clock/floor split; mark ok:False so
# the shell emits status=skipped rather than presenting garbage as a clean result.
reasons=[]
if a<=0:
    reasons.append("a<=0 (boot does not fall with clock; a/f+b model invalid)")
if b<0:
    reasons.append("b<0 (negative memory floor)")
if floor_frac is None or not (0.0<=floor_frac<=1.0):
    reasons.append("floor_fraction out of [0,1]")
if r2 is None or r2<0.5:
    reasons.append("r2<0.5 (fit does not hold)")
out={"ok": (len(reasons)==0),
     "model":"core_boot_ms = a/f_GHz + b",
     "a_ms_ghz":round(a,4),
     "b_ms":round(b,4),
     "r2":(round(r2,4) if r2 is not None else None),
     "n_points":n}
if floor_frac is not None:
    out["floor_fraction_at_fmax"]=round(floor_frac,4)
if reasons:
    out["reason"]="implausible fit: "+"; ".join(reasons)
print(json.dumps(out))
' 2>/dev/null)"
[ -n "$fit_json" ] || fit_json='{"ok":false,"reason":"fit failed"}'

# If the fit is implausible/degenerate, do NOT present it as a clean result:
# emit status=skipped with the points + the failed fit recorded, so the floor
# split never corrupts the downstream H1-vs-H2 verdict.
fit_ok="$(printf '%s' "$fit_json" | "$PY" -c 'import sys,json; print("yes" if json.loads(sys.stdin.read()).get("ok") else "no")' 2>/dev/null)"
if [ "$fit_ok" != "yes" ]; then
  fit_reason="$(printf '%s' "$fit_json" | "$PY" -c 'import sys,json; print(json.loads(sys.stdin.read()).get("reason","fit not valid"))' 2>/dev/null)"
  skip "${fit_reason:-implausible fit}" \
       "$avail_extra,\"points\":[$points_json],\"fit\":$fit_json"
fi

emit "{\"test\":\"freq_sweep\",\"status\":\"ok\",\"platform\":$(json_str "$PLATFORM"),\"cpus\":$(json_str "$CPUS"),\"iters\":$ITERS,\"warmup\":$WARMUP,\"original_governor\":$(json_str "${ORIG_GOV:-unknown}"),$avail_extra,\"points\":[$points_json],\"fit\":$fit_json}"
exit 0
