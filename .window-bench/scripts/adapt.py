#!/usr/bin/env python3
# adapt.py -- bridge run_host.sh's results-*.json (schema window-bench/host-result/v1)
# into the envelope analyze.py expects (top-level host/arch/sku/core/env.nominal_ghz,
# core_boot{status,p50}, tests{t1_tma.slots, t2_freq, t3_*, t4_latency.curve, t4_mlp}).
#
# Also synthesizes the Apple M1 Pro datapoint: its latency/MLP curves are MEASURED
# here (src/{latency,mlp}.json), but CORE_BOOT cannot be re-measured (gVisor is
# Linux-only), so we carry the blog's published 78 ms and FLAG it as not-re-measured.
# Nothing is interpolated; absent values stay null.
import json, os, glob, sys

SRC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # .window-bench
RES = os.path.join(SRC, "results")
OUT = os.path.join(SRC, "results_adapted")
os.makedirs(OUT, exist_ok=True)

# instance_label prefix -> (uarch key, single-thread boost GHz)
def core_of(label):
    l = (label or "").lower()
    if l.startswith("c7i"):
        return "intel-golden-cove", 3.8   # Xeon 8488C, ST boost ~3.8
    if l.startswith("c7g"):
        return "arm-neoverse-v1", 2.6      # Graviton 3 fixed 2.6
    if l.startswith("c8g"):
        return "arm-neoverse-v2", 2.7
    return None, None

def frac_pct(x):
    return (float(x) / 100.0) if isinstance(x, (int, float)) else None

def adapt_host(d):
    env = d.get("env", {})
    label = env.get("instance_label", "?")
    arch_raw = env.get("arch", "?")
    arch = {"x86_64": "amd64", "aarch64": "arm64"}.get(arch_raw, arch_raw)
    core, ghz = core_of(label)
    t = d.get("tests", {})

    # --- t1_tma: build a fractional slots{} from the flat percents. memory_bound,
    # if toplev failed to parse it, is reconstructed as Backend - Core (TMA identity
    # Backend = Memory + Core). We carry the raw L3-miss-stall % as extra evidence.
    t1 = t.get("t1_tma", {}) or {}
    slots = None
    if t1.get("status") == "ok":
        be = t1.get("backend_bound_pct")
        co = t1.get("core_bound_pct")
        me = t1.get("memory_bound_pct")
        if me is None and isinstance(be, (int, float)) and isinstance(co, (int, float)):
            me = be - co
        slots = {
            "backend_bound": frac_pct(be),
            "core_bound": frac_pct(co),
            "memory_bound": frac_pct(me),
            "frontend_bound": frac_pct(t1.get("frontend_bound_pct")),
        }
    t1_out = dict(t1)
    t1_out["slots"] = slots

    # --- t3_prefetch / t3_hugepage delta_ms: pull from the detail test files.
    def detail(name):
        # results/<label-dir>/results/test_<name>.json -- find by hostname slug dir.
        for p in glob.glob(os.path.join(RES, "*", "results", "test_%s.json" % name)):
            try:
                o = json.load(open(p))
            except Exception:
                continue
            # match by host slug in path against this host
            if env.get("hostname", "") and env["hostname"] in p:
                return o
        return None

    t3_pref = dict(t.get("t3_prefetch", {}) or {})
    pd = detail("prefetch_msr")
    if pd and pd.get("status") == "ok":
        dm = (pd.get("delta") or {}).get("delta_ms")
        t3_pref["delta_ms"] = dm
        t3_pref["enabled_p50_ms"] = pd.get("enabled_core_boot_p50_ms")
        t3_pref["disabled_p50_ms"] = pd.get("disabled_core_boot_p50_ms")

    t3_hp = dict(t.get("t3_hugepage", {}) or {})
    hd = detail("hugepage_sweep")
    if hd and hd.get("status") == "ok":
        legs = {x.get("thp"): x.get("core_boot_p50_ms") for x in (hd.get("thp_sweep") or [])}
        vals = [v for v in legs.values() if isinstance(v, (int, float))]
        if len(vals) >= 2:
            t3_hp["delta_ms"] = max(vals) - min(vals)
        t3_hp["legs"] = legs
        t3_hp["note"] = "THP=always did not back the Sentry heap (page-size channel not cleanly isolated)"

    cb = d.get("coreboot", {})
    out = {
        "host": env.get("hostname", label),
        "arch": arch,
        "sku": label,
        "core": core,
        "env": {"nominal_ghz": ghz, "kernel": env.get("kernel"),
                "online_cpus": env.get("online_cpus")},
        "core_boot": {"status": cb.get("status"), "p50": cb.get("p50_ms"),
                      "p95": cb.get("p95_ms"), "n": cb.get("n"),
                      "gomaxprocs": cb.get("gomaxprocs")},
        "tests": {
            "t1_tma": t1_out,
            "t2_freq": t.get("t2_freq", {}),
            "t3_numa": t.get("t3_numa", {}),
            "t3_hugepage": t3_hp,
            "t3_prefetch": t3_pref,
            "t4_latency": {"status": d.get("latency_curve", {}).get("status", "ok"),
                           "curve": d.get("latency_curve", {}).get("curve")},
            "t4_mlp": {"status": d.get("mlp_curve", {}).get("status", "ok"),
                       "knee_n": d.get("mlp_curve", {}).get("knee_n"),
                       "mlp_knee": d.get("mlp_curve", {}).get("knee_n"),
                       "peak_loads_per_ns": d.get("mlp_curve", {}).get("peak_loads_per_ns")},
        },
        "_rcu_caveat": ("metal CORE_BOOT is inflated by the hostmm/RCU membarrier "
                        "grace-period tax that scales with online CPU count "
                        "(~+26ms c7i.metal/96c, ~+54ms c7g.metal/64c vs the 4-online "
                        "xlarge); see hostmm finding. Use xlarge for the ROB correlation.")
                       if (label and "metal" in label) else None,
    }
    return out

def main():
    n = 0
    for p in sorted(glob.glob(os.path.join(RES, "results-*.json"))):
        d = json.load(open(p))
        if d.get("schema") != "window-bench/host-result/v1":
            continue
        o = adapt_host(d)
        fn = "results-%s-%s.json" % (o["sku"].replace(".", "-"), o["arch"])
        json.dump(o, open(os.path.join(OUT, fn), "w"), indent=2)
        n += 1
        print("adapted %-16s core=%-18s CORE_BOOT=%s ms" % (o["sku"], o["core"], o["core_boot"]["p50"]))

    # --- Apple M1 Pro datapoint (measured latency/MLP here; CORE_BOOT from blog) ---
    lat = json.load(open(os.path.join(SRC, "src", "latency.json")))
    mlp = json.load(open(os.path.join(SRC, "src", "mlp.json")))
    m1 = {
        "host": "m1pro-local", "arch": "arm64", "sku": "M1 Pro (local)",
        "core": "apple-firestorm",
        "env": {"nominal_ghz": 3.2, "kernel": "darwin", "online_cpus": 10},
        "core_boot": {"status": "ok", "p50": 78.0, "n": None, "gomaxprocs": 4,
                      "source": "BLOG (gVisor is Linux-only; CORE_BOOT not re-measured on macOS)"},
        "tests": {
            "t1_tma": {"status": "skipped", "reason": "Apple PMU unavailable to perf", "slots": None},
            "t2_freq": {"status": "skipped", "reason": "no cpufreq on macOS"},
            "t3_numa": {"status": "skipped", "reason": "single node"},
            "t3_hugepage": {"status": "skipped", "reason": "n/a (no gVisor on macOS)"},
            "t3_prefetch": {"status": "skipped", "reason": "n/a"},
            "t4_latency": {"status": "ok", "curve": lat.get("curve")},
            "t4_mlp": {"status": "ok", "knee_n": mlp.get("knee_n"),
                       "mlp_knee": mlp.get("knee_n"),
                       "peak_loads_per_ns": mlp.get("peak_loads_per_ns")},
        },
        "_coreboot_caveat": "M1 CORE_BOOT is the blog's measured 78ms, NOT re-measured here.",
    }
    json.dump(m1, open(os.path.join(OUT, "results-m1pro-arm64.json"), "w"), indent=2)
    print("adapted M1 Pro (latency/MLP measured; CORE_BOOT=78ms from blog, flagged)")
    print("wrote %d host envelopes + M1 -> %s" % (n, OUT))

if __name__ == "__main__":
    main()
