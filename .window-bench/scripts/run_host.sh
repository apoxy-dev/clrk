#!/usr/bin/env bash
#
# run_host.sh -- single per-host orchestrator for the window/ROB falsification
# bundle. Detects the host, installs best-effort tooling, builds the bench-runsc
# CORE_BOOT spike from clrk-src.tar.gz, runs CORE_BOOT + the latency.c / mlp.c
# microbenches, takes a config/topology census, invokes the per-test scripts
# that apply to THIS host (each self-detects applicability and prints a JSON
# object), then assembles one valid per-host JSON via python3.
#
# Hard rules honored here:
#   - ASCII only.
#   - Every external command that could block is wrapped in `timeout`.
#   - NEVER fabricate or interpolate: a test that cannot run becomes
#     {"status":"skipped","reason":...} (or a null field) -- never a made-up
#     number.
#   - Idempotent / re-runnable: results dir is cleaned of the artifacts this run
#     produces, jsonl is truncated before the append-mode bench writes to it.
#   - python3 builds the final JSON (json.dump), no hand-rolled string concat.
#
# Layout (matches run_one.sh which stages the bundle):
#   ~/window/                 bundle root
#   ~/window/scripts/         this script + per-test scripts
#   ~/window/clrk-src.tar.gz  CORE_BOOT source tree (cmd/bench-runsc)
#   ~/window/latency.c        microbench source
#   ~/window/mlp.c            microbench source
#   ~/window/results/         all outputs land here; pulled by run_one.sh
#
# Usage:  sudo LABEL=<instance-type> bash scripts/run_host.sh [LABEL]
# The LABEL (instance type, e.g. c7i.metal-24xl) may come from $LABEL, argv[1],
# or EC2 IMDS; if none resolve it stays "unknown".
#
# This script is Ubuntu/Linux shell; it is bash -n syntax-checked and its
# embedded python is python3 -c compile-checked on the build Mac, but it is only
# executed on the Ubuntu hosts.

set -uo pipefail

# ---------------------------------------------------------------------------
# 0. Paths, host identity, results dir.
# ---------------------------------------------------------------------------

# Resolve the bundle root from this script's own location so the orchestrator
# works whether invoked as `bash scripts/run_host.sh` or by absolute path.
SCRIPT_SRC="${BASH_SOURCE[0]}"
SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_SRC")" >/dev/null 2>&1 && pwd)"
BUNDLE_DIR="$(cd "$SCRIPT_DIR/.." >/dev/null 2>&1 && pwd)"

# When invoked via sudo, $HOME may be /root; anchor on the bundle root instead
# so results are pulled from where run_one.sh expects them (~user/window).
RESULTS="$BUNDLE_DIR/results"
mkdir -p "$RESULTS"

HOSTNAME_S="$(hostname 2>/dev/null || echo unknown-host)"
ARCH="$(uname -m 2>/dev/null || echo unknown-arch)"
KREL="$(uname -r 2>/dev/null || echo unknown)"

# LABEL: prefer env, then argv[1], then EC2 IMDS instance-type, else "unknown".
LABEL="${LABEL:-}"
if [ -z "$LABEL" ] && [ "${1:-}" != "" ]; then LABEL="$1"; fi
if [ -z "$LABEL" ]; then
  # IMDSv2 token first (best effort, short timeouts), fall back to IMDSv1.
  IMDS_TOK="$(timeout 5 curl -fsS -X PUT \
      -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
      http://169.254.169.254/latest/api/token 2>/dev/null || true)"
  if [ -n "$IMDS_TOK" ]; then
    LABEL="$(timeout 5 curl -fsS -H "X-aws-ec2-metadata-token: $IMDS_TOK" \
      http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null || true)"
  else
    LABEL="$(timeout 5 curl -fsS \
      http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null || true)"
  fi
fi
[ -z "$LABEL" ] && LABEL="unknown"

# Slugify hostname/arch for the filename (ASCII, no spaces/slashes).
slug() { printf '%s' "$1" | tr -c 'A-Za-z0-9._-' '-'; }
HOST_SLUG="$(slug "$HOSTNAME_S")"
ARCH_SLUG="$(slug "$ARCH")"
OUT_JSON="$RESULTS/results-${HOST_SLUG}-${ARCH_SLUG}.json"

# Run as root? bench-runsc + governor/MSR writes need it. We don't hard-fail
# (the microbenches + census still run unprivileged), but we record it.
IS_ROOT=0
if [ "$(id -u 2>/dev/null || echo 1)" = "0" ]; then IS_ROOT=1; fi
SUDO=""
if [ "$IS_ROOT" != "1" ] && command -v sudo >/dev/null 2>&1; then SUDO="sudo"; fi

log() { printf '[run_host %s] %s\n' "$LABEL" "$*"; }

log "host=$HOSTNAME_S arch=$ARCH kernel=$KREL label=$LABEL root=$IS_ROOT"
log "bundle=$BUNDLE_DIR results=$RESULTS out=$OUT_JSON"

# Status ledger: record_status <key> <ok|skipped|partial|error> <reason...>
# Written one line per key to install/test status files for the python step.
INSTALL_LOG="$RESULTS/install_status.tsv"
: > "$INSTALL_LOG"
record_install() {
  # key<TAB>status<TAB>reason
  printf '%s\t%s\t%s\n' "$1" "$2" "${3:-}" >> "$INSTALL_LOG"
}

# ---------------------------------------------------------------------------
# 1/2. Best-effort apt installs. Record per-package success/failure.
# ---------------------------------------------------------------------------

DEBIAN_FRONTEND=noninteractive
export DEBIAN_FRONTEND

apt_update_done=0
apt_update_once() {
  if [ "$apt_update_done" = "0" ]; then
    timeout 180 $SUDO apt-get update -y >/dev/null 2>&1 || true
    apt_update_done=1
  fi
}

# Install a single package, record ok/failed. Best effort -- the bundle must
# stay runnable even when a package is unavailable on this host.
apt_install() {
  local pkg="$1"
  if ! command -v apt-get >/dev/null 2>&1; then
    record_install "apt:$pkg" skipped "no apt-get"
    return
  fi
  apt_update_once
  if timeout 300 $SUDO apt-get install -y "$pkg" >/dev/null 2>&1; then
    record_install "apt:$pkg" ok ""
  else
    record_install "apt:$pkg" failed "apt-get install failed"
  fi
}

log "installing toolchain (best effort)"
for pkg in build-essential linux-tools-common "linux-tools-$(uname -r)" \
           numactl msr-tools python3 golang-go; do
  apt_install "$pkg"
done

# Arm PMU: try the Arm Telemetry Solution topdown-tool from pip. Mark skipped if
# pip is missing or the install fails; pmu_arm.sh self-detects either way.
if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
  PIP=""
  if command -v pip3 >/dev/null 2>&1; then PIP="pip3"
  elif command -v pip >/dev/null 2>&1; then PIP="pip"; fi
  if [ -z "$PIP" ]; then
    apt_install python3-pip
    if command -v pip3 >/dev/null 2>&1; then PIP="pip3"; fi
  fi
  if [ -n "$PIP" ]; then
    # --break-system-packages: Ubuntu 24.04 PEP 668 marks the system env
    # externally managed; this is a throwaway bench box.
    if timeout 240 $SUDO "$PIP" install --break-system-packages topdown-tool \
         >/dev/null 2>&1 \
       || timeout 240 $SUDO "$PIP" install topdown-tool >/dev/null 2>&1; then
      record_install "pip:topdown-tool" ok ""
    else
      record_install "pip:topdown-tool" skipped "pip install topdown-tool failed"
    fi
  else
    record_install "pip:topdown-tool" skipped "no pip available"
  fi
else
  record_install "pip:topdown-tool" skipped "not an aarch64 host"
fi

# Resolve a C compiler: prefer cc (gcc on Ubuntu), then gcc, then clang.
CC_BIN=""
for c in cc gcc clang; do
  if command -v "$c" >/dev/null 2>&1; then CC_BIN="$c"; break; fi
done
log "C compiler: ${CC_BIN:-<none>}"

# Resolve go: prefer one on PATH, else the apt golang-go install, else common
# manual install paths.
GO_BIN=""
for g in go /usr/local/go/bin/go /usr/lib/go/bin/go; do
  if command -v "$g" >/dev/null 2>&1; then GO_BIN="$g"; break; fi
done
log "go: ${GO_BIN:-<none>}"

# ---------------------------------------------------------------------------
# 3. Build bench-runsc (CORE_BOOT source of truth) from clrk-src.tar.gz.
# ---------------------------------------------------------------------------

# Locate the source tarball next to the bundle (run_one.sh stages it at
# ~/window/clrk-src.tar.gz).
CLRK_TGZ=""
for cand in "$BUNDLE_DIR/clrk-src.tar.gz" "$HOME/window/clrk-src.tar.gz" \
            "$HOME/clrk-src.tar.gz"; do
  if [ -f "$cand" ]; then CLRK_TGZ="$cand"; break; fi
done

BENCH_BIN="$HOME/bench-runsc"
CB_BUILD_STATUS="skipped"
CB_BUILD_REASON=""
BUILD_ERR="$RESULTS/bench_build.err"
: > "$BUILD_ERR"

if [ -z "$GO_BIN" ]; then
  CB_BUILD_REASON="go toolchain not available"
elif [ -z "$CLRK_TGZ" ]; then
  CB_BUILD_REASON="clrk-src.tar.gz not found"
else
  SRC_DIR="$HOME/clrk-src"
  rm -rf "$SRC_DIR"
  mkdir -p "$SRC_DIR"
  if ! timeout 180 tar xzf "$CLRK_TGZ" -C "$SRC_DIR" 2>>"$BUILD_ERR"; then
    CB_BUILD_REASON="tar extract failed"
  else
    # The tarball root may contain cmd/ directly or a single top dir; find the
    # bench-runsc package robustly.
    BR_DIR=""
    if [ -d "$SRC_DIR/cmd/bench-runsc" ]; then
      BR_DIR="$SRC_DIR/cmd/bench-runsc"
    else
      BR_DIR="$(find "$SRC_DIR" -maxdepth 4 -type d -name bench-runsc \
                  -path '*/cmd/bench-runsc' 2>/dev/null | head -1)"
    fi
    if [ -z "$BR_DIR" ]; then
      CB_BUILD_REASON="cmd/bench-runsc not found in tarball"
    else
      # Build from the module root that owns this package: walk up to go.mod.
      MOD_DIR="$BR_DIR"
      while [ "$MOD_DIR" != "/" ] && [ ! -f "$MOD_DIR/go.mod" ]; do
        MOD_DIR="$(dirname "$MOD_DIR")"
      done
      log "building bench-runsc from $BR_DIR (module $MOD_DIR)"
      # CGO is required (libcontainer/nsenter). 600s cap: cold module download.
      if ( cd "$BR_DIR" \
           && timeout 600 env CGO_ENABLED=1 GOFLAGS=-mod=mod \
                GOCACHE="$HOME/.cache/go-build" \
                "$GO_BIN" build -o "$BENCH_BIN" . ) >>"$BUILD_ERR" 2>&1; then
        if [ -x "$BENCH_BIN" ]; then
          CB_BUILD_STATUS="ok"
        else
          CB_BUILD_REASON="go build reported success but binary missing"
        fi
      else
        CB_BUILD_REASON="go build failed (see bench_build.err)"
      fi
    fi
  fi
fi
log "bench-runsc build: $CB_BUILD_STATUS ${CB_BUILD_REASON:+($CB_BUILD_REASON)}"

# ---------------------------------------------------------------------------
# 4. Governor 'performance' where exposed; record turbo state.
# ---------------------------------------------------------------------------

GOV_SET="unknown"
TURBO_STATE="unknown"

GOV0=/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor
if [ -e "$GOV0" ]; then
  # Try cpupower first (cleaner), then per-cpu sysfs writes.
  if command -v cpupower >/dev/null 2>&1 && [ "$IS_ROOT" = 1 -o -n "$SUDO" ]; then
    timeout 30 $SUDO cpupower frequency-set -g performance >/dev/null 2>&1 || true
  fi
  for g in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    [ -w "$g" ] || [ -n "$SUDO" ] || continue
    if [ "$IS_ROOT" = 1 ]; then
      echo performance > "$g" 2>/dev/null || true
    else
      echo performance | timeout 10 $SUDO tee "$g" >/dev/null 2>&1 || true
    fi
  done
  GOV_SET="$(cat "$GOV0" 2>/dev/null || echo unknown)"
else
  GOV_SET="no-cpufreq"
fi

# Turbo / boost state: intel_pstate no_turbo (0 => turbo on), or cpufreq boost.
if [ -e /sys/devices/system/cpu/intel_pstate/no_turbo ]; then
  NT="$(cat /sys/devices/system/cpu/intel_pstate/no_turbo 2>/dev/null || echo '')"
  if [ "$NT" = "0" ]; then TURBO_STATE="on(intel_pstate no_turbo=0)";
  elif [ "$NT" = "1" ]; then TURBO_STATE="off(intel_pstate no_turbo=1)";
  else TURBO_STATE="unknown(intel_pstate)"; fi
elif [ -e /sys/devices/system/cpu/cpufreq/boost ]; then
  BO="$(cat /sys/devices/system/cpu/cpufreq/boost 2>/dev/null || echo '')"
  if [ "$BO" = "1" ]; then TURBO_STATE="on(cpufreq boost=1)";
  elif [ "$BO" = "0" ]; then TURBO_STATE="off(cpufreq boost=0)";
  else TURBO_STATE="unknown(cpufreq boost)"; fi
else
  TURBO_STATE="not-exposed"
fi
log "governor=$GOV_SET turbo=$TURBO_STATE"

# ---------------------------------------------------------------------------
# 5. CORE_BOOT run: bench-runsc create+start of /bin/true, GOMAXPROCS pinned 4.
# ---------------------------------------------------------------------------

CB_JSONL="$RESULTS/coreboot.jsonl"
CB_STDOUT="$RESULTS/coreboot.stdout"
CB_RUN_STATUS="skipped"
CB_RUN_REASON=""

# Helper: does cmd X exist?
have() { command -v "$1" >/dev/null 2>&1; }

if [ "$CB_BUILD_STATUS" != "ok" ]; then
  CB_RUN_STATUS="skipped"
  CB_RUN_REASON="bench-runsc not built: ${CB_BUILD_REASON:-unknown}"
elif [ "$IS_ROOT" != "1" ] && [ -z "$SUDO" ]; then
  CB_RUN_STATUS="skipped"
  CB_RUN_REASON="bench-runsc requires root and no sudo available"
else
  # Truncate the append-mode jsonl so a re-run does not concatenate old iters.
  : > "$CB_JSONL"
  : > "$CB_STDOUT"

  # Build the privileged command. taskset -c 0-3 pins the Sentry's CPU affinity
  # to 4 cores; the runsc boot subprocess inherits that affinity across
  # fork+exec, so its loader also runs on cores 0-3. We additionally export
  # GOMAXPROCS=4 so the parallelism is deterministic even on a host WITHOUT
  # taskset or with GOMAXPROCS already exported in the env (taskset cannot
  # override an explicit GOMAXPROCS). The '# Parent GOMAXPROCS=' line is printed
  # by the PARENT bench-runsc process; we record it as a PROXY for the loader's
  # GOMAXPROCS (the loader inherits the same affinity/env) and then assert it
  # equals 4 below -- if the pin/env did not take, we DOWNGRADE rather than
  # report a CORE_BOOT measured under the wrong (96/192-way) parallelism.
  PRE_TASKSET=""
  if have taskset; then
    PRE_TASKSET="taskset -c 0-3"
  fi
  PRE_NICE=""
  have nice && PRE_NICE="nice -n -20"
  PRE_CHRT=""
  have chrt && PRE_CHRT="chrt --fifo 50"

  # Belt-and-suspenders: force GOMAXPROCS=4 via env so the value is deterministic
  # regardless of host affinity, then assert it after the run. We wrap the bench
  # in `env GOMAXPROCS=4` for the parent AND the fork-exec'd loader inherits it.
  PRE_ENV="env GOMAXPROCS=4"

  # The bench appends per-iter JSON rows to $CB_JSONL.
  BENCH_ARGS="-platform=systrap -iters 50 -warmup 5 -json $CB_JSONL"
  CB_ITERS=50

  # 900s cap: 55 iters of create+start+delete on the slowest VM, generously.
  if [ "$IS_ROOT" = 1 ]; then
    timeout 900 $PRE_ENV $PRE_TASKSET $PRE_NICE $PRE_CHRT \
      "$BENCH_BIN" $BENCH_ARGS >"$CB_STDOUT" 2>&1
    CB_RC=$?
  else
    timeout 900 $SUDO $PRE_ENV $PRE_TASKSET $PRE_NICE $PRE_CHRT \
      "$BENCH_BIN" $BENCH_ARGS >"$CB_STDOUT" 2>&1
    CB_RC=$?
  fi

  if [ "$CB_RC" -ne 0 ] && [ ! -s "$CB_JSONL" ]; then
    CB_RUN_STATUS="error"
    CB_RUN_REASON="bench-runsc exited rc=$CB_RC with no jsonl rows"
  elif [ ! -s "$CB_JSONL" ]; then
    CB_RUN_STATUS="error"
    CB_RUN_REASON="bench-runsc produced no jsonl rows"
  else
    CB_RUN_STATUS="ok"
  fi
fi
log "CORE_BOOT run: $CB_RUN_STATUS ${CB_RUN_REASON:+($CB_RUN_REASON)}"

# Capture the effective GOMAXPROCS from the '# Parent GOMAXPROCS=' line (a proxy
# for the loader's GOMAXPROCS; the loader inherits the same affinity+env).
CB_GOMAXPROCS=""
if [ -s "$CB_STDOUT" ]; then
  CB_GOMAXPROCS="$(grep -m1 '^# Parent GOMAXPROCS=' "$CB_STDOUT" 2>/dev/null \
                    | sed -n 's/^# Parent GOMAXPROCS=\([0-9][0-9]*\).*/\1/p')"
fi

# CRITICAL GATE: a CORE_BOOT measured at the wrong parallelism is worse than no
# measurement -- the whole ROB/window comparison depends on the 4-core pin. If
# the run reported ok but GOMAXPROCS is not 4 (env+taskset both failed to take,
# or the bench did not print the line), mark it error so the consumer never
# trusts an unpinned number as authoritative.
if [ "$CB_RUN_STATUS" = "ok" ]; then
  if [ -z "$CB_GOMAXPROCS" ]; then
    CB_RUN_STATUS="error"
    CB_RUN_REASON="could not confirm GOMAXPROCS from stdout; refusing to report an unverified-parallelism CORE_BOOT"
  elif [ "$CB_GOMAXPROCS" != "4" ]; then
    CB_RUN_STATUS="error"
    CB_RUN_REASON="GOMAXPROCS=$CB_GOMAXPROCS, expected 4 (4-core pin failed); CORE_BOOT measured under wrong parallelism"
  fi
fi

# Partial-data gate: a run KILLED by the 900s timeout (rc=124) that nonetheless
# appended >=1 row would otherwise pass as a complete measurement over a
# truncated sample. Mark it partial and surface the timeout so the percentile is
# not presented as the full host CORE_BOOT.
if [ "$CB_RUN_STATUS" = "ok" ] && [ "${CB_RC:-0}" = "124" ]; then
  CB_RUN_STATUS="partial"
  CB_RUN_REASON="bench-runsc hit the 900s timeout; CORE_BOOT computed over a truncated (under-sampled) set of iters"
fi
log "CORE_BOOT gate: status=$CB_RUN_STATUS gomaxprocs=${CB_GOMAXPROCS:-unknown} rc=${CB_RC:-na}"

# ---------------------------------------------------------------------------
# 6. Compile + run the microbenches (latency.c, mlp.c), pinned to core 0.
# ---------------------------------------------------------------------------

# Locate the C sources next to the bundle root or scripts dir.
find_src() {
  local name="$1" c
  for c in "$BUNDLE_DIR/$name" "$SCRIPT_DIR/$name" "$HOME/window/$name" \
           "$HOME/$name"; do
    if [ -f "$c" ]; then printf '%s' "$c"; return 0; fi
  done
  return 1
}

LAT_SRC="$(find_src latency.c || true)"
MLP_SRC="$(find_src mlp.c || true)"

LAT_JSON="$RESULTS/latency.json"
MLP_JSON="$RESULTS/mlp.json"
LAT_STATUS="skipped"; LAT_REASON=""
MLP_STATUS="skipped"; MLP_REASON=""

# Pin to core 0 for the microbenches if taskset exists; the C also best-effort
# pins via sched_setaffinity as a backstop.
PIN0=""
have taskset && PIN0="taskset -c 0"

CFLAGS="-O2 -fno-tree-vectorize -pthread"

compile_one() {
  # compile_one <src> <out>  -> echoes "" on success, error text on failure.
  local src="$1" out="$2"
  if [ -z "$CC_BIN" ]; then echo "no C compiler"; return 1; fi
  if [ -z "$src" ]; then echo "source not found"; return 1; fi
  local err
  err="$($CC_BIN $CFLAGS -o "$out" "$src" 2>&1)"
  if [ $? -ne 0 ] || [ ! -x "$out" ]; then
    echo "compile failed: $err"; return 1
  fi
  return 0
}

# latency.c
LAT_BIN="$RESULTS/latency"
rm -f "$LAT_JSON"
if msg="$(compile_one "$LAT_SRC" "$LAT_BIN")"; then
  # latency [json_path] [sizelist] [reps]; reps=5. 300s cap (DRAM points slow).
  if timeout 300 $PIN0 "$LAT_BIN" "$LAT_JSON" "" 5 >"$RESULTS/latency.stdout" 2>&1 \
     && [ -s "$LAT_JSON" ]; then
    LAT_STATUS="ok"
  else
    LAT_STATUS="error"; LAT_REASON="latency run failed or empty json"
  fi
else
  LAT_STATUS="skipped"; LAT_REASON="$msg"
fi
log "latency.c: $LAT_STATUS ${LAT_REASON:+($LAT_REASON)}"

# mlp.c
MLP_BIN="$RESULTS/mlp"
rm -f "$MLP_JSON"
if msg="$(compile_one "$MLP_SRC" "$MLP_BIN")"; then
  # mlp [json_path] [Nlist] [reps]; reps=5. 420s cap (1.5GB buffer + low-N slow).
  if timeout 420 $PIN0 "$MLP_BIN" "$MLP_JSON" "" 5 >"$RESULTS/mlp.stdout" 2>&1 \
     && [ -s "$MLP_JSON" ]; then
    MLP_STATUS="ok"
  else
    MLP_STATUS="error"; MLP_REASON="mlp run failed or empty json"
  fi
else
  MLP_STATUS="skipped"; MLP_REASON="$msg"
fi
log "mlp.c: $MLP_STATUS ${MLP_REASON:+($MLP_REASON)}"

# ---------------------------------------------------------------------------
# 7. Census: uname, pagesize, lscpu, numactl -H, governor, turbo, cpuidle,
#    /proc/cpuinfo flags, perf list head.
# ---------------------------------------------------------------------------

CENSUS_TXT="$RESULTS/census.txt"
{
  echo "=== uname -r ==="; uname -r 2>/dev/null
  echo "=== uname -a ==="; uname -a 2>/dev/null
  echo "=== arch ==="; uname -m 2>/dev/null
  echo "=== getconf PAGESIZE ==="; getconf PAGESIZE 2>/dev/null
  echo "=== nproc / online ==="
  echo "nproc=$(nproc 2>/dev/null) online=$(getconf _NPROCESSORS_ONLN 2>/dev/null)"
  echo "=== lscpu ==="
  if have lscpu; then timeout 30 lscpu 2>/dev/null; else echo "(no lscpu)"; fi
  echo "=== numactl -H ==="
  if have numactl; then timeout 30 numactl -H 2>/dev/null; else echo "(no numactl)"; fi
  echo "=== governor (cpu0) ==="
  cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo "(none)"
  echo "=== governor set this run ==="; echo "$GOV_SET"
  echo "=== turbo state ==="; echo "$TURBO_STATE"
  echo "=== cpuidle driver ==="
  cat /sys/devices/system/cpu/cpuidle/current_driver 2>/dev/null || echo "(none)"
  echo "=== cpuidle states (cpu0) ==="
  for s in /sys/devices/system/cpu/cpu0/cpuidle/state*; do
    [ -e "$s" ] || continue
    printf '%s name=%s latency_us=%s residency_us=%s disable=%s\n' \
      "$(basename "$s")" "$(cat "$s/name" 2>/dev/null)" \
      "$(cat "$s/latency" 2>/dev/null)" "$(cat "$s/residency" 2>/dev/null)" \
      "$(cat "$s/disable" 2>/dev/null)"
  done
  echo "=== /proc/cpuinfo flags ==="
  grep -m1 -E '^flags|^Features' /proc/cpuinfo 2>/dev/null || echo "(no flags line)"
  echo "=== /proc/cpuinfo model ==="
  grep -m1 -E '^model name|^CPU implementer|^CPU part' /proc/cpuinfo 2>/dev/null \
    || echo "(no model line)"
  echo "=== perf list (head) ==="
  if have perf; then
    timeout 30 perf list 2>/dev/null | head -60 || echo "(perf list failed)"
  else
    echo "(no perf)"
  fi
} > "$CENSUS_TXT" 2>&1
log "census -> $CENSUS_TXT"

PAGESIZE_VAL="$(getconf PAGESIZE 2>/dev/null || echo '')"

# ---------------------------------------------------------------------------
# 8. Per-test scripts. Each self-detects applicability and prints ONE JSON
#    object (status ok|skipped + reason when skipped). We capture stdout to a
#    file and let python validate/parse it. Absent script => skipped(reason).
# ---------------------------------------------------------------------------

# Map test-key -> script filename. These live in scripts/ alongside this file;
# they are separate deliverables. A missing/non-executable script is recorded
# skipped so the orchestrator stays runnable on every host.
TEST_KEYS="pmu_intel pmu_arm freq_sweep numa_test hugepage_sweep prefetch_msr"

run_pertest() {
  local key="$1"
  local script="$SCRIPT_DIR/${key}.sh"
  local out="$RESULTS/test_${key}.json"
  rm -f "$out"
  if [ ! -f "$script" ]; then
    # Emit a minimal skipped object via python so it is always valid JSON.
    timeout 30 python3 - "$out" "$key" "script not present in bundle" <<'PY' 2>/dev/null
import json, sys
out, key, reason = sys.argv[1], sys.argv[2], sys.argv[3]
with open(out, "w") as f:
    json.dump({"status": "skipped", "test": key, "reason": reason}, f)
PY
    log "test $key: skipped (script not present)"
    return
  fi
  # The per-test script owns its own applicability detection and prints JSON on
  # stdout. We pass useful env so it need not re-derive paths. 600s cap each.
  if timeout 600 env \
        BENCH_BIN="$BENCH_BIN" \
        BOOTLOOP="$SCRIPT_DIR/bootloop.sh" \
        RESULTS="$RESULTS" \
        LABEL="$LABEL" \
        ARCH="$ARCH" \
        bash "$script" > "$out" 2>"$RESULTS/test_${key}.err"; then
    :
  else
    log "test $key: script exited nonzero (rc=$?); output kept for validation"
  fi
  # Validate the captured stdout is JSON; if not, replace with an error object
  # so the final assembly never ingests garbage. Never fabricate data.
  if ! timeout 30 python3 - "$out" "$key" <<'PY' 2>/dev/null
import json, sys
out, key = sys.argv[1], sys.argv[2]
try:
    with open(out) as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError("top-level JSON is not an object")
    obj.setdefault("test", key)
    obj.setdefault("status", "error")
    # rewrite normalized
    with open(out, "w") as f:
        json.dump(obj, f)
    sys.exit(0)
except Exception as e:
    with open(out, "w") as f:
        json.dump({"status": "error", "test": key,
                   "reason": "non-JSON or unreadable script output: %s" % e}, f)
    sys.exit(0)
PY
  then
    # python itself failed (e.g. missing) -- write a guaranteed-valid object.
    printf '{"status":"error","test":"%s","reason":"validator failed"}' "$key" > "$out"
  fi
  log "test $key: captured -> $out"
}

for k in $TEST_KEYS; do
  run_pertest "$k"
done

# ---------------------------------------------------------------------------
# 9. Assemble the per-host JSON with python3 (json.dump). Absent data => null +
#    a status; NEVER fabricate. Reads all the intermediate artifacts above.
# ---------------------------------------------------------------------------

log "assembling $OUT_JSON"

PY_OK=1
timeout 120 python3 - \
  "$OUT_JSON" \
  "$HOSTNAME_S" "$ARCH" "$KREL" "$LABEL" "$PAGESIZE_VAL" \
  "$GOV_SET" "$TURBO_STATE" "$IS_ROOT" \
  "$INSTALL_LOG" \
  "$CB_BUILD_STATUS" "$CB_BUILD_REASON" \
  "$CB_RUN_STATUS" "$CB_RUN_REASON" "$CB_JSONL" "$CB_GOMAXPROCS" \
  "${CB_RC:-na}" "${CB_ITERS:-0}" \
  "$LAT_STATUS" "$LAT_REASON" "$LAT_JSON" \
  "$MLP_STATUS" "$MLP_REASON" "$MLP_JSON" \
  "$CENSUS_TXT" \
  "$RESULTS" \
  "$TEST_KEYS" <<'PY' || PY_OK=0
import json, os, sys

(out_json, hostname, arch, krel, label, pagesize,
 gov, turbo, is_root,
 install_log,
 cb_build_status, cb_build_reason,
 cb_run_status, cb_run_reason, cb_jsonl, cb_gomaxprocs,
 cb_rc, cb_iters,
 lat_status, lat_reason, lat_json,
 mlp_status, mlp_reason, mlp_json,
 census_txt,
 results_dir,
 test_keys_str) = sys.argv[1:29]


def load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def percentile(sorted_vals, q):
    # Nearest-rank percentile on an already-sorted list (q in [0,100]).
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    import math
    rank = q / 100.0 * (len(sorted_vals) - 1)
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return sorted_vals[lo]
    frac = rank - lo
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * frac


# --- install ledger -------------------------------------------------------
installs = {}
try:
    with open(install_log) as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            key = parts[0]
            status = parts[1] if len(parts) > 1 else "unknown"
            reason = parts[2] if len(parts) > 2 else ""
            installs[key] = {"status": status, "reason": reason}
except Exception:
    installs = {}

# --- coreboot: parse the jsonl, compute p50/p95 of measured core_boot (ns->ms).
try:
    cb_iters_n = int(cb_iters)
except (TypeError, ValueError):
    cb_iters_n = 0
coreboot = {
    "status": cb_run_status,
    "reason": cb_run_reason or None,
    "build_status": cb_build_status,
    "build_reason": cb_build_reason or None,
    "gomaxprocs": int(cb_gomaxprocs) if cb_gomaxprocs.isdigit() else None,
    "expected_n": cb_iters_n or None,
    "timeout_rc": (124 if cb_rc == "124" else None),
    "n": 0,
    "p50_ms": None,
    "p95_ms": None,
    "core_boot_ms": [],
}
vals_ms = []
# Parse the measured rows for ok AND partial: a partial (timeout) run still has
# real measured iters; we surface them but keep status=partial so the consumer
# knows the percentile is over a truncated sample, never presenting it as a
# complete CORE_BOOT.
if cb_run_status in ("ok", "partial"):
    try:
        with open(cb_jsonl) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                # Skip warmup iters; the bench marks them "warmup": true.
                if row.get("warmup"):
                    continue
                cb_ns = row.get("core_boot")
                if cb_ns is None:
                    continue
                # core_boot is a Go time.Duration => integer nanoseconds.
                vals_ms.append(float(cb_ns) / 1e6)
    except Exception:
        pass
    vals_ms.sort()
    coreboot["n"] = len(vals_ms)
    coreboot["core_boot_ms"] = vals_ms
    coreboot["p50_ms"] = percentile(vals_ms, 50)
    coreboot["p95_ms"] = percentile(vals_ms, 95)
    if not vals_ms:
        # Ran but yielded no usable measured rows: do not fabricate.
        coreboot["status"] = "error"
        coreboot["reason"] = "no measured (non-warmup) core_boot rows in jsonl"
    elif cb_run_status == "ok" and cb_iters_n and coreboot["n"] < cb_iters_n:
        # Claimed ok but under-sampled vs the requested iters: downgrade to
        # partial so a short sample is never presented as a full measurement.
        coreboot["status"] = "partial"
        coreboot["reason"] = (
            "only %d of %d requested iters measured (under-sampled)"
            % (coreboot["n"], cb_iters_n))

# --- latency curve --------------------------------------------------------
lat_obj = load_json(lat_json) if lat_status == "ok" else None
latency_curve = {
    "status": lat_status if lat_obj is not None or lat_status != "ok" else "ok",
    "reason": lat_reason or None,
    "line_bytes": None,
    "curve": None,
}
if lat_obj is not None:
    latency_curve["line_bytes"] = lat_obj.get("line_bytes")
    latency_curve["target_loads"] = lat_obj.get("target_loads")
    latency_curve["reps"] = lat_obj.get("reps")
    latency_curve["curve"] = lat_obj.get("curve")
elif lat_status == "ok":
    # claimed ok but json unreadable: downgrade honestly.
    latency_curve["status"] = "error"
    latency_curve["reason"] = "latency json unreadable"

# --- mlp curve + knee -----------------------------------------------------
mlp_obj = load_json(mlp_json) if mlp_status == "ok" else None
mlp_curve = {
    "status": mlp_status if mlp_obj is not None or mlp_status != "ok" else "ok",
    "reason": mlp_reason or None,
    "line_bytes": None,
    "buf_bytes": None,
    "curve": None,
    "knee_n": None,
    "peak_loads_per_ns": None,
}
if mlp_obj is not None:
    mlp_curve["line_bytes"] = mlp_obj.get("line_bytes")
    mlp_curve["buf_bytes"] = mlp_obj.get("buf_bytes")
    mlp_curve["hop_cap"] = mlp_obj.get("hop_cap")
    mlp_curve["reps"] = mlp_obj.get("reps")
    mlp_curve["curve"] = mlp_obj.get("curve")
    mlp_curve["knee_n"] = mlp_obj.get("knee_n")
    mlp_curve["peak_loads_per_ns"] = mlp_obj.get("peak_loads_per_ns")
elif mlp_status == "ok":
    mlp_curve["status"] = "error"
    mlp_curve["reason"] = "mlp json unreadable"

# --- census (kept as raw text; not parsed into JSON to avoid lossy guesses) -
census_text = None
try:
    with open(census_txt) as f:
        census_text = f.read()
except Exception:
    census_text = None

# --- per-test results -----------------------------------------------------
tests = {}
for key in test_keys_str.split():
    path = os.path.join(results_dir, "test_%s.json" % key)
    obj = load_json(path)
    if obj is None:
        tests[key] = {"status": "skipped", "test": key,
                      "reason": "no test output file produced"}
    else:
        obj.setdefault("test", key)
        obj.setdefault("status", "error")
        tests[key] = obj

# Remap the producer test keys onto the analyze.py "t1_tma/t2_freq/t3_*/t4_*"
# contract so the consumer can actually read them (producer and consumer agreed
# on key, nesting, and units). pmu_intel/pmu_arm already emit a `slots` dict with
# memory_bound/core_bound as FRACTIONS in [0,1] under the t1_tma shape. Only one
# of pmu_intel / pmu_arm runs per host (the other is skipped on the wrong arch);
# we fold whichever produced a real status into t1_tma. The raw keys are kept too.
def _pick_t1(t):
    pi = t.get("pmu_intel", {})
    pa = t.get("pmu_arm", {})
    # Prefer the one with an actual slots split; else the one that is ok/partial;
    # else either present object so the reason survives.
    for cand in (pi, pa):
        if isinstance(cand, dict) and cand.get("slots") is not None:
            return cand
    for cand in (pi, pa):
        if isinstance(cand, dict) and cand.get("status") in ("ok", "partial"):
            return cand
    for cand in (pi, pa):
        if isinstance(cand, dict) and cand.get("status"):
            return cand
    return {"status": "skipped", "reason": "no pmu_intel/pmu_arm output"}

if "pmu_intel" in tests or "pmu_arm" in tests:
    tests["t1_tma"] = _pick_t1(tests)
# Straight aliases for the remaining tests (same object, analyze.py key).
for src, dst in (("freq_sweep", "t2_freq"),
                 ("numa_test", "t3_numa"),
                 ("hugepage_sweep", "t3_hugepage"),
                 ("prefetch_msr", "t3_prefetch")):
    if src in tests and dst not in tests:
        tests[dst] = tests[src]

# Microbench curves under the t4_latency / t4_mlp contract analyze.py reads.
tests.setdefault("t4_latency", {
    "status": latency_curve.get("status", "skipped"),
    "bench": "latency",
    "reason": latency_curve.get("reason"),
    "curve": latency_curve.get("curve"),
})
tests.setdefault("t4_mlp", {
    "status": mlp_curve.get("status", "skipped"),
    "bench": "mlp",
    "reason": mlp_curve.get("reason"),
    "curve": mlp_curve.get("curve"),
    "knee_n": mlp_curve.get("knee_n"),
    "peak_loads_per_ns": mlp_curve.get("peak_loads_per_ns"),
})

doc = {
    "schema": "window-bench/host-result/v1",
    "env": {
        "hostname": hostname,
        "arch": arch,
        "kernel": krel,
        "instance_label": label,
        "pagesize_bytes": int(pagesize) if pagesize.isdigit() else None,
        "governor": gov,
        "turbo": turbo,
        "ran_as_root": is_root == "1",
        "installs": installs,
    },
    "coreboot": coreboot,
    "latency_curve": latency_curve,
    "mlp_curve": mlp_curve,
    "census_raw": census_text,
    "tests": tests,
}

tmp = out_json + ".tmp"
with open(tmp, "w") as f:
    json.dump(doc, f, indent=2, sort_keys=True)
    f.write("\n")
os.replace(tmp, out_json)
print("wrote %s" % out_json)
PY

if [ "$PY_OK" != "1" ]; then
  log "ERROR: python3 assembly failed; final JSON may be missing"
  # Last-resort: leave a minimal valid marker so downstream tooling sees a file.
  if [ ! -s "$OUT_JSON" ]; then
    printf '{"schema":"window-bench/host-result/v1","env":{"hostname":"%s","arch":"%s"},"error":"python assembly failed"}\n' \
      "$HOST_SLUG" "$ARCH_SLUG" > "$OUT_JSON"
  fi
  exit 1
fi

log "DONE -> $OUT_JSON"
exit 0
