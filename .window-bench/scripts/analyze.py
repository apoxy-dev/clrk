#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# analyze.py -- aggregate the window/ROB falsification bundle.
#
# Reads every per-host JSON (results/results-*.json) plus the local M1 Pro
# microbench JSON, builds a hardware+results table, computes Spearman rank
# correlations of CORE_BOOT against datasheet ROB / datasheet LSQ / measured
# DRAM latency / measured MLP knee, pulls the Test-1/2/3 outputs from each host,
# applies the verdict rules mechanically, and writes REPORT.md.
#
# Stdlib only. No numpy / scipy. Spearman, least-squares, and a hand-built SVG
# plot are implemented below. If matplotlib happens to be importable we also
# drop PNGs, but the report never depends on it: ASCII tables + a hand-built SVG
# are always emitted.
#
# NEVER fabricate or interpolate a measurement. A test that did not run on a
# host arrives as {"status": "skipped", "reason": ...}; such a host is excluded
# from that correlation/aggregate and the exclusion is noted in REPORT.md.
#
# ----------------------------------------------------------------------------
# Per-host JSON envelope this consumes  (schema "window-bench/host/v1").
# run_host.sh on each EC2 box emits exactly one of these as
# results/results-<host>-<arch>.json. The local Mac microbench is the same
# shape with core_boot + PMU tests marked skipped.
#
# {
#   "schema": "window-bench/host/v1",
#   "host":   "c7i-metal24xl",                 # short label
#   "arch":   "amd64",                          # amd64 | arm64
#   "sku":    "c7i.metal-24xl",
#   "core":   "intel-golden-cove",              # core key (see CORES below)
#   "env": { "uname_r": "...", "pagesize": 4096, "governor": "performance",
#            "turbo": "off", "gomaxprocs": 4, "numa_nodes": 2,
#            "nominal_ghz": 3.2 },
#   "core_boot": {                              # CORE_BOOT = runsc create+start
#       "status": "ok", "unit": "ms",
#       "p50": 134.2, "p95": 151.0, "n": 50,
#       "rows_ms": [ ... per-iter core_boot in ms ... ]   # optional
#   },
#   "tests": {
#     "t1_tma":   { "status":"ok", "tool":"toplev|topdown-tool",
#                   "slots": { "frontend_bound":.., "bad_speculation":..,
#                              "retiring":.., "backend_bound":..,
#                              "memory_bound":.., "core_bound":.. } },
#                 # slots are FRACTIONS of pipeline slots in [0,1].
#                 # memory_bound + core_bound are the level-3 split of
#                 # backend_bound (Backend->Memory vs Backend->Core).
#     "t2_freq":  { "status":"ok",
#                   "points":[ {"f_ghz":2.0,"core_boot_ms_p50":..}, ... ],
#                   "fit": {"a":..,"b":..,"r2":..} },   # CORE_BOOT(f)=a/f+b
#                 # a in ms*GHz (clock-scaling compute), b in ms (clk-invariant)
#     "t3_numa":  { "status":"ok",
#                   "local_ms_p50":.., "remote_ms_p50":.., "delta_ms":.. },
#     "t3_hugepage": { "status":"ok",
#                   "points":[ {"page":"4k","core_boot_ms_p50":..},
#                              {"page":"2m","core_boot_ms_p50":..}, ... ],
#                   "delta_ms":.. },               # max-min across pages
#     "t3_prefetch": { "status":"ok",
#                   "on_ms_p50":.., "off_ms_p50":.., "delta_ms":.. },
#     "t4_latency": { "status":"ok", "bench":"latency",
#                   "curve":[ {"wss_bytes":..,"ns_per_load":..}, ... ],
#                   "dram_ns": 135.9 },          # optional; else derived here
#     "t4_mlp":   { "status":"ok", "bench":"mlp",
#                   "curve":[ {"n":..,"loads_per_ns":..}, ... ],
#                   "knee_n": 20, "peak_loads_per_ns": 0.118 }
#   }
# }
#
# Any of core_boot / tests.* may instead be {"status":"skipped","reason":...}.
# Missing keys are treated as skipped with reason "absent from host JSON".
#
# ----------------------------------------------------------------------------
# Datasheet hardware table (cited; counts NOT measured -- vendor/3rd-party):
#   Apple Firestorm (M1 Pro)           ROB ~630  LSQ ~150
#       -- Dougall Johnson, "Apple M1 Firestorm" microarchitecture overview.
#   Intel Golden Cove / Sapphire Rapids 8488C  ROB 512  LSQ 192 (LDQ+STQ)
#       -- Chips and Cheese, "Golden Cove: Intel's new big core."
#   ARM Neoverse V2 (Axion / Graviton4) ROB 320  LSQ ~128
#       -- Chips and Cheese, "Neoverse V2 / Graviton 4."
#   ARM Neoverse V1 (Graviton 3)        ROB 256  LSQ ~136
#       -- Chips and Cheese, "Neoverse V1: Arm's first server-class big core."
#   LFB / MSHR (true-MLP) counts: marked unknown unless a citable figure
#   exists; we do NOT invent them. Intel client/server lines historically
#   document ~10-16 line-fill buffers per core, but Sapphire Rapids' L2 MSHR /
#   superqueue depth is not cleanly published, so we leave it null rather than
#   guess. Measured MLP knee (mlp.c) is the empirical stand-in for true MLP.
# ----------------------------------------------------------------------------
#
# Usage:
#   analyze.py [--results-dir DIR] [--out REPORT.md] [--plots-dir DIR]
#   analyze.py --selftest        # feed 2 synthetic hosts through the pipeline
#
# ASCII only.

import argparse
import glob
import json
import math
import os
import sys
import tempfile


# --------------------------------------------------------------------------
# Datasheet hardware table. Keys are the per-host JSON "core" field.
# rob/lsq are datasheet integers; lfb_mshr is None when not citably known.
# l1_tlb / l2_tlb entries and l2/l3 sizes are datasheet/typical figures used
# only for context columns -- never for a correlation -- and may be None.
# --------------------------------------------------------------------------
CORES = {
    "apple-firestorm": {
        "label":   "Apple Firestorm (M1 Pro)",
        "arch":    "arm64",
        "rob":     630,    # Dougall Johnson firestorm overview (~630).
        "lsq":     150,    # ~150 (combined load/store), Dougall.
        "lfb_mshr": None,  # not citably published -> unknown, not invented.
        "l1_tlb":  None,
        "l2_tlb":  3072,   # ~3072-entry L2 TLB (Dougall).
        "l2_kib":  12288,  # 12 MiB shared performance-cluster L2.
        "l3_kib":  None,   # no conventional L3 (SLC ~24-32 MiB, not a core L3).
        "cite":    "Dougall Johnson, Apple M1 Firestorm microarch overview.",
    },
    "intel-golden-cove": {
        "label":   "Intel Golden Cove (Sapphire Rapids 8488C)",
        "arch":    "amd64",
        "rob":     512,    # Chips and Cheese Golden Cove.
        "lsq":     192,    # ~192 = 192-deep load + store queues, C&C.
        "lfb_mshr": None,  # SPR L2 MSHR/superqueue not cleanly published.
        "l1_tlb":  None,
        "l2_tlb":  2048,   # ~2048-entry STLB (C&C).
        "l2_kib":  2048,   # 2 MiB private L2 (server SKU).
        "l3_kib":  None,   # ~1.875 MiB/core shared LLC, SKU dependent.
        "cite":    "Chips and Cheese, Golden Cove: Intel's new big core.",
    },
    "arm-neoverse-v2": {
        "label":   "ARM Neoverse V2 (Axion / Graviton4)",
        "arch":    "arm64",
        "rob":     320,    # Chips and Cheese Neoverse V2 / Graviton 4.
        "lsq":     128,    # ~128, C&C.
        "lfb_mshr": None,
        "l1_tlb":  None,
        "l2_tlb":  None,
        "l2_kib":  2048,   # 2 MiB private L2.
        "l3_kib":  None,
        "cite":    "Chips and Cheese, Neoverse V2 / Graviton 4.",
    },
    "arm-neoverse-v1": {
        "label":   "ARM Neoverse V1 (Graviton 3)",
        "arch":    "arm64",
        "rob":     256,    # Chips and Cheese Neoverse V1.
        "lsq":     136,    # ~136, C&C.
        "lfb_mshr": None,
        "l1_tlb":  None,
        "l2_tlb":  None,
        "l2_kib":  1024,   # 1 MiB private L2.
        "l3_kib":  None,
        "cite":    "Chips and Cheese, Neoverse V1: Arm's first server big core.",
    },
}

# Verdict thresholds (from PLAN.md, applied mechanically).
T1_MEM_REJECT = 0.40    # Backend->Memory-Bound >= 40% of slots ...
T1_CORE_OK    = 0.10    # ... while core/ROB-full stalls <= 10% -> reject H1.
T2_B_LARGE_FRAC = 0.40  # clock-invariant b >= 40% of CORE_BOOT@nominal -> H2.
T2_FIT_R2     = 0.50    # CORE_BOOT(f)=a/f+b fit must clear this r2 (and a>0) to
                        # be trusted; below it b is noise, not a memory floor.
T3_MOVE_MS    = 3.0     # a CORE_BOOT shift this large (ms) counts as "moves".


# ==========================================================================
# Statistics (hand-rolled; stdlib only).
# ==========================================================================

def _rankdata(xs):
    """Fractional ranks (average rank for ties), 1-based. Like scipy default."""
    n = len(xs)
    order = sorted(range(n), key=lambda i: xs[i])
    ranks = [0.0] * n
    i = 0
    while i < n:
        j = i
        while j + 1 < n and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        # Indices order[i..j] are tied; assign their average 1-based rank.
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = avg
        i = j + 1
    return ranks


def spearman(xs, ys):
    """Spearman rho via Pearson on ranks (tie-safe). Returns (rho, n) or
    (None, n) when undefined (n < 2 or a constant input)."""
    assert len(xs) == len(ys)
    n = len(xs)
    if n < 2:
        return None, n
    rx = _rankdata(xs)
    ry = _rankdata(ys)
    return _pearson(rx, ry), n


def _pearson(xs, ys):
    n = len(xs)
    mx = sum(xs) / n
    my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    sxy = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    if sxx <= 0.0 or syy <= 0.0:
        return None
    return sxy / math.sqrt(sxx * syy)


def least_squares(xs, ys):
    """Ordinary least squares y = m*x + c. Returns (m, c, r2) or None."""
    n = len(xs)
    if n < 2:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx <= 0.0:
        return None
    sxy = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    m = sxy / sxx
    c = my - m * mx
    ss_tot = sum((y - my) ** 2 for y in ys)
    ss_res = sum((ys[i] - (m * xs[i] + c)) ** 2 for i in range(n))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0.0 else 1.0
    return m, c, r2


def fit_reciprocal_freq(points):
    """Fit CORE_BOOT(f) = a/f + b via OLS on the transform x=1/f, y=boot.
    points: list of (f_ghz, boot_ms). Returns dict(a,b,r2) or None."""
    pts = [(p[0], p[1]) for p in points if p[0] and p[0] > 0.0]
    if len(pts) < 2:
        return None
    xs = [1.0 / f for (f, _) in pts]
    ys = [b for (_, b) in pts]
    res = least_squares(xs, ys)
    if res is None:
        return None
    a, b, r2 = res   # y = a*(1/f) + b
    return {"a": a, "b": b, "r2": r2}


# ==========================================================================
# Microbench derivations (DRAM plateau, knee). NEVER interpolate -- these read
# only points that actually exist in the curve.
# ==========================================================================

def derive_dram_ns(latency_test):
    """Largest-WSS plateau ns/load from latency.c. Prefer an explicit
    'dram_ns' if the host JSON already carries one; else take the curve point
    at the maximum working set (the right-edge DRAM plateau). Returns float or
    None. No interpolation: this is an observed point."""
    if not isinstance(latency_test, dict):
        return None
    if latency_test.get("status") != "ok":
        return None
    if isinstance(latency_test.get("dram_ns"), (int, float)):
        return float(latency_test["dram_ns"])
    curve = latency_test.get("curve")
    if not curve:
        return None
    pt = max(curve, key=lambda c: c.get("wss_bytes", 0))
    v = pt.get("ns_per_load")
    return float(v) if isinstance(v, (int, float)) else None


def derive_mlp_knee(mlp_test):
    """MLP knee N from mlp.c. Prefer explicit 'knee_n'; else recompute the
    knee as the N at peak loads_per_ns (still an observed point). Returns int
    or None."""
    if not isinstance(mlp_test, dict):
        return None
    if mlp_test.get("status") != "ok":
        return None
    if isinstance(mlp_test.get("knee_n"), (int, float)):
        return int(mlp_test["knee_n"])
    curve = mlp_test.get("curve")
    if not curve:
        return None
    pk = max(curve, key=lambda c: c.get("loads_per_ns", -1.0))
    v = pk.get("n")
    return int(v) if isinstance(v, (int, float)) else None


# ==========================================================================
# Ingest.
# ==========================================================================

def _test(host, name):
    """Return the named test sub-object, defaulting to a skipped stub when the
    key is absent so downstream code never KeyErrors."""
    t = host.get("tests", {}).get(name)
    if t is None:
        return {"status": "skipped", "reason": "absent from host JSON"}
    return t


def load_hosts(results_dir):
    """Load every results-*.json under results_dir. Returns (hosts, warnings)."""
    hosts = []
    warnings = []
    paths = sorted(glob.glob(os.path.join(results_dir, "results-*.json")))
    for p in paths:
        try:
            with open(p, "r") as f:
                obj = json.load(f)
        except (OSError, ValueError) as e:
            warnings.append("could not parse %s: %s" % (os.path.basename(p), e))
            continue
        if not isinstance(obj, dict) or "host" not in obj:
            warnings.append("ignoring %s: not a host envelope" % os.path.basename(p))
            continue
        obj.setdefault("_path", p)
        hosts.append(obj)
    return hosts, warnings


def core_boot_p50(host):
    """CORE_BOOT p50 in ms, or None if skipped/absent."""
    cb = host.get("core_boot")
    if not isinstance(cb, dict) or cb.get("status") != "ok":
        return None
    if isinstance(cb.get("p50"), (int, float)):
        return float(cb["p50"])
    rows = cb.get("rows_ms")
    if rows:
        s = sorted(rows)
        return float(s[len(s) // 2])
    return None


# ==========================================================================
# Per-host derived record + the master table.
# ==========================================================================

def build_record(host):
    core_key = host.get("core")
    ds = CORES.get(core_key, {})
    lat = _test(host, "t4_latency")
    mlp = _test(host, "t4_mlp")
    rec = {
        "host":   host.get("host", "?"),
        "arch":   host.get("arch", "?"),
        "sku":    host.get("sku", "?"),
        "core":   core_key,
        "label":  ds.get("label", core_key or "?"),
        "ghz":    host.get("env", {}).get("nominal_ghz"),
        "rob":    ds.get("rob"),
        "lsq":    ds.get("lsq"),
        "lfb_mshr": ds.get("lfb_mshr"),
        "l2_tlb": ds.get("l2_tlb"),
        "l2_kib": ds.get("l2_kib"),
        "l3_kib": ds.get("l3_kib"),
        "dram_ns":  derive_dram_ns(lat),
        "mlp_knee": derive_mlp_knee(mlp),
        "core_boot_p50": core_boot_p50(host),
        "cite":   ds.get("cite", ""),
    }
    return rec


# ==========================================================================
# Correlations. Each pairs CORE_BOOT against one ordering variable, dropping
# hosts where either side is missing (NEVER imputed). Reports n and which
# excluded.
# ==========================================================================

CORR_VARS = [
    ("datasheet_rob", "rob", "datasheet ROB"),
    ("datasheet_lsq", "lsq", "datasheet LSQ"),
    ("measured_dram_latency", "dram_ns", "measured DRAM latency (ns)"),
    ("measured_mlp_knee", "mlp_knee", "measured MLP knee (N)"),
]


def compute_correlations(records):
    out = []
    for key, field, label in CORR_VARS:
        xs, ys, used, dropped = [], [], [], []
        for r in records:
            xv = r.get(field)
            yv = r.get("core_boot_p50")
            if xv is None or yv is None:
                why = []
                if yv is None:
                    why.append("no CORE_BOOT")
                if xv is None:
                    why.append("no %s" % label)
                dropped.append((r["host"], ", ".join(why)))
                continue
            xs.append(float(xv))
            ys.append(float(yv))
            used.append(r["host"])
        rho, n = spearman(xs, ys)
        out.append({
            "key": key, "field": field, "label": label,
            "rho": rho, "n": n, "used": used, "dropped": dropped,
        })
    return out


def best_orderer(correlations):
    """Variable whose |rho| best orders CORE_BOOT among those with n>=3 and a
    defined rho. Returns the correlation dict or None."""
    cand = [c for c in correlations if c["rho"] is not None and c["n"] >= 3]
    if not cand:
        return None
    return max(cand, key=lambda c: abs(c["rho"]))


# ==========================================================================
# Verdict engine. Pure function of the ingested data; emits per-rule findings.
# ==========================================================================

def evaluate_t1(host):
    """T1 TMA -> (verdict, detail). verdict in {reject_h1, inconclusive,
    h1_consistent, skipped}."""
    t = _test(host, "t1_tma")
    if t.get("status") != "ok":
        return "skipped", t.get("reason", "skipped")
    slots = t.get("slots") or {}
    mem = slots.get("memory_bound")
    core = slots.get("core_bound")
    if mem is None or core is None:
        return "skipped", "t1 present but missing memory_bound/core_bound split"
    # These come from shell scripts scraping toplev/topdown output, so guard
    # against string-typed numbers: a TypeError here would abort the whole run.
    if not isinstance(mem, (int, float)) or not isinstance(core, (int, float)):
        return "skipped", "t1 slots non-numeric (memory_bound/core_bound not numbers)"
    tool = t.get("tool", "?")
    detail = "tool=%s memory_bound=%.1f%% core_bound=%.1f%%" % (
        tool, 100.0 * mem, 100.0 * core)
    if mem >= T1_MEM_REJECT and core <= T1_CORE_OK:
        return "reject_h1", detail + " => Backend->Memory dominates, ROB-full small"
    if core > mem and core > T1_CORE_OK:
        return "h1_consistent", detail + " => core/ROB-full stalls dominate"
    return "inconclusive", detail + " => neither rule cleanly fires"


def evaluate_t2(host):
    """T2 freq fit -> (verdict, detail). verdict in {support_h2, inconclusive,
    skipped}. Large clock-invariant b supports H2; large a rebuts the blog's
    'not clock-bound'."""
    t = _test(host, "t2_freq")
    if t.get("status") != "ok":
        return "skipped", t.get("reason", "skipped"), None
    fit = t.get("fit")
    if not fit or fit.get("a") is None or fit.get("b") is None:
        pts = t.get("points")
        if pts:
            fit = fit_reciprocal_freq(
                [(p.get("f_ghz"), p.get("core_boot_ms_p50")) for p in pts])
        if not fit:
            return "skipped", "t2 present but no fit and < 2 usable points", None
    a, b = float(fit["a"]), float(fit["b"])
    r2 = fit.get("r2")
    # Estimate CORE_BOOT at nominal/top freq to express b as a fraction.
    ghz = host.get("env", {}).get("nominal_ghz")
    fmax = None
    if t.get("points"):
        fs = [p.get("f_ghz") for p in t["points"] if p.get("f_ghz")]
        if fs:
            fmax = max(fs)
    fref = ghz or fmax
    detail = "a=%.1f ms*GHz (compute) b=%.1f ms (clk-invariant)" % (a, b)
    if r2 is not None:
        detail += " r2=%.3f" % r2
    # FIT-VALIDITY GATE: the a/f+b model only supports H2 when the fit actually
    # holds (r2 >= T2_FIT_R2) AND a is physical (a > 0: boot falls with clock).
    # a <= 0 means boot rises with the clock -- the model does not hold and b is
    # not a "clock-invariant floor". r2 ~ 0 means b is noise. A flat memory-bound
    # boot-vs-freq curve has b ~ 100% of boot by construction, so without this
    # gate support_h2 would fire essentially unconditionally on any memory-bound
    # box -- the cardinal "passes by accident" failure.
    if a <= 0:
        return ("inconclusive",
                detail + " => UNPHYSICAL a<=0 (boot does not fall with clock); "
                "a/f+b model does not hold, b is not a clock-invariant floor",
                fit)
    if r2 is None or r2 < T2_FIT_R2:
        return ("inconclusive",
                detail + " => fit r2 below %.2f; b is fit noise, not a "
                "clock-invariant floor" % T2_FIT_R2,
                fit)
    if fref:
        boot_at_ref = a / fref + b
        frac = b / boot_at_ref if boot_at_ref > 0 else 0.0
        detail += " ; b is %.0f%% of CORE_BOOT@%.2fGHz" % (100.0 * frac, fref)
        if frac >= T2_B_LARGE_FRAC and b > 0:
            return "support_h2", detail + " => large clock-invariant floor", fit
    # Fall back: if a is also large vs b, that separately rebuts "not clock-bound".
    return "inconclusive", detail, fit


def _move(verdict_detail, delta, channel):
    if delta is None:
        return "skipped", "%s present but no delta" % channel
    # delta may arrive string-typed from shell-scraped output; guard so abs() and
    # the comparison do not raise and abort the whole report.
    if not isinstance(delta, (int, float)):
        return "skipped", "%s delta non-numeric (%r)" % (channel, delta)
    if abs(delta) >= T3_MOVE_MS:
        return "support_h2", "%s delta=%.1f ms (>= %.1f) => CORE_BOOT moves, ROB fixed" % (
            channel, delta, T3_MOVE_MS)
    return "no_move", "%s delta=%.1f ms (< %.1f) => no movement" % (
        channel, delta, T3_MOVE_MS)


def evaluate_t3(host):
    """T3 collinearity breakers -> dict of channel -> (verdict, detail)."""
    res = {}
    numa = _test(host, "t3_numa")
    if numa.get("status") == "ok":
        res["numa"] = _move(None, numa.get("delta_ms"), "remote-NUMA")
    else:
        res["numa"] = ("skipped", numa.get("reason", "skipped"))
    huge = _test(host, "t3_hugepage")
    if huge.get("status") == "ok":
        d = huge.get("delta_ms")
        if d is None and huge.get("points"):
            vals = [p.get("core_boot_ms_p50") for p in huge["points"]
                    if isinstance(p.get("core_boot_ms_p50"), (int, float))]
            if len(vals) >= 2:
                d = max(vals) - min(vals)
        res["hugepage"] = _move(None, d, "hugepage")
    else:
        res["hugepage"] = ("skipped", huge.get("reason", "skipped"))
    pf = _test(host, "t3_prefetch")
    if pf.get("status") == "ok":
        res["prefetch"] = _move(None, pf.get("delta_ms"), "prefetch-disable")
    else:
        res["prefetch"] = ("skipped", pf.get("reason", "skipped"))
    return res


def overall_verdict(hosts, records, correlations, best, per_host):
    """Apply the verdict rules across all hosts. Returns (verdict, rule, lines).
    verdict in {REJECT_H1_SUPPORT_H2, H1_SURVIVES, INCONCLUSIVE}."""
    lines = []
    support_h2 = []   # list of (rule, host) supporting H2.
    reject_h1 = []    # hosts where T1 rejects H1.
    h1_consistent = []

    for h in hosts:
        host = h["host"]
        t1v, t1d = per_host[host]["t1"]
        if t1v == "reject_h1":
            reject_h1.append(host)
            lines.append("  T1@%s: REJECT H1 -- %s" % (host, t1d))
        elif t1v == "h1_consistent":
            h1_consistent.append(host)
            lines.append("  T1@%s: H1-consistent -- %s" % (host, t1d))

        t2v, t2d = per_host[host]["t2"][0], per_host[host]["t2"][1]
        if t2v == "support_h2":
            support_h2.append(("T2 large clock-invariant b", host))
            lines.append("  T2@%s: SUPPORT H2 -- %s" % (host, t2d))

        for ch, (cv, cd) in per_host[host]["t3"].items():
            if cv == "support_h2":
                support_h2.append(("T3 %s movement" % ch, host))
                lines.append("  T3-%s@%s: SUPPORT H2 -- %s" % (ch, host, cd))

    # T4: does measured latency / MLP knee order CORE_BOOT better than ROB?
    rob_corr = next((c for c in correlations if c["field"] == "rob"), None)
    lat_corr = next((c for c in correlations if c["field"] == "dram_ns"), None)
    knee_corr = next((c for c in correlations if c["field"] == "mlp_knee"), None)
    t4_supports = False
    if best is not None and best["field"] in ("dram_ns", "mlp_knee"):
        # Only counts if a measured variable strictly out-orders ROB and ROB's
        # rho is actually defined to compare against.
        if rob_corr and rob_corr["rho"] is not None:
            if abs(best["rho"]) > abs(rob_corr["rho"]):
                t4_supports = True
                support_h2.append(("T4 measured %s out-orders ROB" % best["label"], "all"))
                lines.append(
                    "  T4: SUPPORT H2 -- %s rho=%.3f orders CORE_BOOT better than "
                    "datasheet ROB rho=%.3f (n=%d)" % (
                        best["label"], best["rho"], rob_corr["rho"], best["n"]))
        elif rob_corr is None or rob_corr["rho"] is None:
            # ROB correlation undefined (e.g. all-same ROB or n<2): a measured
            # variable ordering CORE_BOOT is suggestive but we do not claim the
            # comparison rule fired.
            lines.append(
                "  T4: %s best orders CORE_BOOT (rho=%.3f, n=%d) but datasheet-"
                "ROB rho is undefined -> comparison rule not fired" % (
                    best["label"], best["rho"], best["n"]))

    # Decision.
    if reject_h1 and not h1_consistent:
        rule = ("PLAN verdict rule: REJECT H1 because T1 shows "
                "Backend->Memory-Bound >= %.0f%% of slots while ROB/RS-full "
                "core-bound stalls <= %.0f%% on %s." % (
                    100 * T1_MEM_REJECT, 100 * T1_CORE_OK, ", ".join(reject_h1)))
        if support_h2:
            rule += (" H2 additionally SUPPORTED by: %s." % "; ".join(
                "%s (%s)" % (r, hh) for (r, hh) in support_h2))
        return "REJECT_H1_SUPPORT_H2", rule, lines

    if not reject_h1 and support_h2:
        # If some host's PRIMARY T1 directly supported H1 (h1_consistent) while
        # secondary H2 signals fired, the evidence CONFLICTS -- do not silently
        # override the decisive primary test with a single collinearity breaker,
        # and never assert "no T1 PMU rejection available" when T1 ran and was
        # h1-consistent. Report it as INCONCLUSIVE (conflicting evidence), the
        # same way coexisting reject_h1 + h1_consistent is treated.
        if h1_consistent:
            rule = ("CONFLICTING evidence: primary T1 was H1-consistent (core/ROB-"
                    "full dominates) on %s, yet secondary H2 signals fired: %s. A "
                    "single collinearity breaker does not override the decisive "
                    "primary test; treated as INCONCLUSIVE pending a re-run." % (
                        ", ".join(h1_consistent),
                        "; ".join("%s (%s)" % (r, hh) for (r, hh) in support_h2)))
            return "INCONCLUSIVE", rule, lines
        rule = ("PLAN verdict rule: SUPPORT H2 (no T1 PMU rejection available on "
                "any host, but the collinearity breakers / correlation fired): %s." %
                "; ".join("%s (%s)" % (r, hh) for (r, hh) in support_h2))
        return "REJECT_H1_SUPPORT_H2", rule, lines

    # H1 survives only if ROB/RS-full dominates T1 AND CORE_BOOT still tracks ROB
    # after controlling for measured latency. We can only ever weakly assert the
    # second clause with tiny n; flag as surprising and demand a re-run.
    if h1_consistent and not reject_h1 and not support_h2:
        rob_ok = rob_corr and rob_corr["rho"] is not None and rob_corr["rho"] > 0.5
        if rob_ok:
            rule = ("PLAN verdict rule: H1 SURVIVES -- ROB/RS-full stalls "
                    "dominate T1 on %s AND CORE_BOOT still rank-tracks datasheet "
                    "ROB (rho=%.3f, n=%d). SURPRISING: demand a fresh-host re-run."
                    % (", ".join(h1_consistent), rob_corr["rho"], rob_corr["n"]))
            return "H1_SURVIVES", rule, lines

    rule = ("No verdict rule fired cleanly (insufficient runnable tests / tiny "
            "n). Treated as INCONCLUSIVE; see per-test status and confounds.")
    return "INCONCLUSIVE", rule, lines


# ==========================================================================
# Rendering: ASCII tables, hand-built SVG, optional matplotlib PNGs.
# ==========================================================================

def _fmt(v, nd=1, dash="-"):
    if v is None:
        return dash
    if isinstance(v, float):
        return ("%." + str(nd) + "f") % v
    return str(v)


def render_hw_table(records):
    cols = [
        ("host", "host", 14),
        ("label", "core", 34),
        ("ghz", "GHz", 5),
        ("rob", "ROB", 5),
        ("lsq", "LSQ", 5),
        ("lfb_mshr", "LFB/MSHR", 9),
        ("l2_tlb", "L2TLB", 6),
        ("l2_kib", "L2(KiB)", 8),
        ("dram_ns", "DRAMns", 7),
        ("mlp_knee", "MLPknee", 8),
        ("core_boot_p50", "CORE_p50", 9),
    ]
    head = "| " + " | ".join(h.ljust(w) for (_, h, w) in cols) + " |"
    sep = "|" + "|".join("-" * (w + 2) for (_, _, w) in cols) + "|"
    rows = [head, sep]
    for r in records:
        cells = []
        for (k, _, w) in cols:
            v = r.get(k)
            if k in ("dram_ns", "core_boot_p50", "ghz"):
                cells.append(_fmt(v, 1).ljust(w))
            else:
                cells.append(_fmt(v, 0).ljust(w))
        rows.append("| " + " | ".join(cells) + " |")
    return "\n".join(rows)


def render_corr_table(correlations, best):
    rows = ["| CORE_BOOT vs                  | rho    | n | hosts used                |",
            "|------------------------------|--------|---|---------------------------|"]
    for c in correlations:
        rho = "n/a" if c["rho"] is None else "%+.3f" % c["rho"]
        used = ", ".join(c["used"]) if c["used"] else "(none)"
        flag = ""
        if best is not None and c["key"] == best["key"]:
            flag = "  <-- best orderer"
        rows.append("| %-28s | %-6s | %d | %-25s |%s" % (
            c["label"], rho, c["n"], used[:25], flag))
    return "\n".join(rows)


def hand_svg(records, path):
    """Hand-build a tiny SVG scatter of CORE_BOOT (y) vs datasheet ROB (x) and
    vs measured DRAM latency (x), side by side. No deps. Skips points with
    missing values. Returns True if at least one panel had >=2 points."""
    def panel(svg, ox, oy, w, h, title, xs, ys, xlab, ylab):
        svg.append('<g>')
        svg.append('<rect x="%d" y="%d" width="%d" height="%d" fill="none" '
                   'stroke="#888"/>' % (ox, oy, w, h))
        svg.append('<text x="%d" y="%d" font-size="13" font-family="monospace">'
                   '%s</text>' % (ox, oy - 8, title))
        svg.append('<text x="%d" y="%d" font-size="10" font-family="monospace">'
                   '%s</text>' % (ox + w // 2 - 30, oy + h + 16, xlab))
        svg.append('<text x="%d" y="%d" font-size="10" font-family="monospace" '
                   'transform="rotate(-90 %d %d)">%s</text>' % (
                       ox - 26, oy + h // 2, ox - 26, oy + h // 2, ylab))
        if len(xs) < 2:
            svg.append('<text x="%d" y="%d" font-size="11" '
                       'font-family="monospace" fill="#a00">n=%d (need >=2)'
                       '</text>' % (ox + 10, oy + h // 2, len(xs)))
            svg.append('</g>')
            return
        xmin, xmax = min(xs), max(xs)
        ymin, ymax = min(ys), max(ys)
        xr = (xmax - xmin) or 1.0
        yr = (ymax - ymin) or 1.0
        for i in range(len(xs)):
            px = ox + 8 + (xs[i] - xmin) / xr * (w - 16)
            py = oy + h - 8 - (ys[i] - ymin) / yr * (h - 16)
            svg.append('<circle cx="%.1f" cy="%.1f" r="4" fill="#06c"/>' % (px, py))
        # OLS line.
        res = least_squares(xs, ys)
        if res:
            m, c, _ = res
            x0, x1 = xmin, xmax
            y0, y1 = m * x0 + c, m * x1 + c
            p0x = ox + 8 + (x0 - xmin) / xr * (w - 16)
            p0y = oy + h - 8 - (y0 - ymin) / yr * (h - 16)
            p1x = ox + 8 + (x1 - xmin) / xr * (w - 16)
            p1y = oy + h - 8 - (y1 - ymin) / yr * (h - 16)
            svg.append('<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" '
                       'stroke="#c00" stroke-dasharray="4 3"/>' % (p0x, p0y, p1x, p1y))
        svg.append('</g>')

    def pairs(field):
        xs, ys = [], []
        for r in records:
            xv, yv = r.get(field), r.get("core_boot_p50")
            if xv is not None and yv is not None:
                xs.append(float(xv)); ys.append(float(yv))
        return xs, ys

    rob_x, rob_y = pairs("rob")
    lat_x, lat_y = pairs("dram_ns")
    W, H = 760, 320
    svg = ['<?xml version="1.0" encoding="UTF-8"?>',
           '<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" '
           'viewBox="0 0 %d %d">' % (W, H, W, H),
           '<rect width="%d" height="%d" fill="white"/>' % (W, H),
           '<text x="20" y="22" font-size="14" font-family="monospace">'
           'CORE_BOOT p50 vs ordering variables (hand-built SVG)</text>']
    panel(svg, 60, 50, 280, 210, "vs datasheet ROB", rob_x, rob_y,
          "datasheet ROB", "CORE_BOOT ms")
    panel(svg, 440, 50, 280, 210, "vs measured DRAM ns", lat_x, lat_y,
          "DRAM ns/load", "CORE_BOOT ms")
    svg.append('</svg>')
    with open(path, "w") as f:
        f.write("\n".join(svg))
    return len(rob_x) >= 2 or len(lat_x) >= 2


def try_matplotlib_png(records, path):
    """Best-effort PNG via matplotlib if importable. Returns path or None."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception:
        return None
    fig, axes = plt.subplots(1, 2, figsize=(9, 3.4))
    for ax, field, lab in ((axes[0], "rob", "datasheet ROB"),
                           (axes[1], "dram_ns", "measured DRAM ns/load")):
        xs, ys, labels = [], [], []
        for r in records:
            xv, yv = r.get(field), r.get("core_boot_p50")
            if xv is not None and yv is not None:
                xs.append(float(xv)); ys.append(float(yv)); labels.append(r["host"])
        ax.scatter(xs, ys, c="#0066cc")
        for i in range(len(xs)):
            ax.annotate(labels[i], (xs[i], ys[i]), fontsize=7,
                        xytext=(3, 3), textcoords="offset points")
        if len(xs) >= 2:
            res = least_squares(xs, ys)
            if res:
                m, c, _ = res
                lo, hi = min(xs), max(xs)
                ax.plot([lo, hi], [m * lo + c, m * hi + c], "r--", lw=1)
        ax.set_xlabel(lab)
        ax.set_ylabel("CORE_BOOT p50 (ms)")
        ax.set_title("CORE_BOOT vs " + lab, fontsize=9)
    fig.tight_layout()
    try:
        fig.savefig(path, dpi=110)
    except Exception:
        plt.close(fig)
        return None
    plt.close(fig)
    return path


# ==========================================================================
# REPORT.md writer.
# ==========================================================================

def write_report(out_path, hosts, records, correlations, best, per_host,
                 verdict, rule, verdict_lines, warnings, plots, results_dir):
    L = []
    L.append("# Window/ROB falsification: CORE_BOOT vs the microarchitecture")
    L.append("")
    L.append("Generated by `scripts/analyze.py` from `%s`. Mechanical verdict; "
             "no value below is imputed -- a test that did not run on a host is "
             "excluded from its aggregate and listed as skipped." % results_dir)
    L.append("")
    L.append("Hypotheses under test:")
    L.append("")
    L.append("- **H1 (window-bound):** a bigger ROB/LSQ would cut CORE_BOOT; the "
             "window is the binding resource.")
    L.append("- **H2 (memory-bound):** CORE_BOOT is set by effective memory "
             "access time (DRAM/L2 latency + TLB/page-walk), imperfectly hidden "
             "by *true* MLP (LFB/MSHR count, NOT the ROB) + prefetch, plus a "
             "clock-scaling compute remainder. ROB is a non-causal correlate.")
    L.append("")

    # Overall verdict up top.
    L.append("## Overall verdict")
    L.append("")
    badge = {"REJECT_H1_SUPPORT_H2": "REJECT H1 / SUPPORT H2",
             "H1_SURVIVES": "H1 SURVIVES (surprising)",
             "INCONCLUSIVE": "INCONCLUSIVE"}.get(verdict, verdict)
    L.append("**%s**" % badge)
    L.append("")
    L.append(rule)
    L.append("")
    n_boot = sum(1 for r in records if r.get("core_boot_p50") is not None)
    L.append("- n with CORE_BOOT: %d host(s). This is a tiny sample; rank "
             "correlations below are reported, not oversold." % n_boot)
    L.append("- Confounds: bigger cores ship bigger ROBs *and* faster memory "
             "systems (design balance), so a raw ROB<->CORE_BOOT rank match is "
             "expected under BOTH hypotheses; only the collinearity breakers "
             "(T2/T3) and the measured-latency comparison (T4) distinguish them.")
    L.append("")
    if verdict_lines:
        L.append("Rule firings:")
        L.append("")
        L.append("```")
        L.extend(verdict_lines)
        L.append("```")
        L.append("")

    # Hardware + results table.
    L.append("## Hardware + results table")
    L.append("")
    L.append("Datasheet columns (ROB/LSQ) are vendor / third-party figures, not "
             "measured here. DRAM ns and MLP knee are measured by `latency.c` / "
             "`mlp.c` on each host. LFB/MSHR is left blank where no citable count "
             "exists -- it is never invented.")
    L.append("")
    L.append(render_hw_table(records))
    L.append("")
    L.append("Datasheet citations:")
    L.append("")
    seen = set()
    for r in records:
        if r["core"] and r["core"] not in seen and r.get("cite"):
            seen.add(r["core"])
            L.append("- %s: %s" % (r["label"], r["cite"]))
    L.append("")

    # Correlations.
    L.append("## Spearman rank correlations (CORE_BOOT vs orderers)")
    L.append("")
    L.append("Each row drops hosts missing either value (never imputed); `n` is "
             "the surviving pair count.")
    L.append("")
    L.append(render_corr_table(correlations, best))
    L.append("")
    if best is not None:
        L.append("Best orderer of CORE_BOOT (|rho|, n>=3): **%s** (rho=%+.3f, "
                 "n=%d)." % (best["label"], best["rho"], best["n"]))
    else:
        L.append("No correlation reached n>=3 with a defined rho; ordering is "
                 "not adjudicated by rank correlation at this sample size.")
    L.append("")
    for c in correlations:
        if c["dropped"]:
            drops = "; ".join("%s (%s)" % (h, why) for (h, why) in c["dropped"])
            L.append("- %s: dropped %s." % (c["label"], drops))
    L.append("")

    # Plots.
    L.append("## Plots")
    L.append("")
    if plots.get("svg"):
        L.append("- Hand-built SVG scatter: `%s`." % plots["svg"])
    if plots.get("png"):
        L.append("- matplotlib PNG: `%s`." % plots["png"])
    if not plots.get("svg") and not plots.get("png"):
        L.append("- No plots emitted (fewer than 2 plottable points and no "
                 "matplotlib). Data tables above stand in.")
    L.append("")

    # Per-test detail.
    L.append("## Per-host, per-test detail")
    L.append("")
    for h in hosts:
        host = h["host"]
        L.append("### %s  (%s, %s)" % (host, h.get("sku", "?"), h.get("arch", "?")))
        L.append("")
        cb = core_boot_p50(h)
        if cb is None:
            cbinfo = h.get("core_boot") or {}
            reason = cbinfo.get("reason", "absent")
            L.append("- CORE_BOOT: skipped (%s)." % reason)
        else:
            cbo = h.get("core_boot", {})
            L.append("- CORE_BOOT p50 = %.1f ms (p95=%s, n=%s)." % (
                cb, _fmt(cbo.get("p95"), 1), _fmt(cbo.get("n"), 0)))
        t1v, t1d = per_host[host]["t1"]
        L.append("- T1 TMA: %s -- %s" % (t1v, t1d))
        t2v, t2d, _ = per_host[host]["t2"]
        L.append("- T2 freq fit: %s -- %s" % (t2v, t2d))
        for ch, (cv, cd) in per_host[host]["t3"].items():
            L.append("- T3 %s: %s -- %s" % (ch, cv, cd))
        rec = next((r for r in records if r["host"] == host), {})
        L.append("- T4 measured: DRAM=%s ns/load, MLP knee=%s N." % (
            _fmt(rec.get("dram_ns"), 1), _fmt(rec.get("mlp_knee"), 0)))
        L.append("")

    if warnings:
        L.append("## Ingest warnings")
        L.append("")
        for w in warnings:
            L.append("- %s" % w)
        L.append("")

    L.append("## Methodology / honesty notes")
    L.append("")
    L.append("- CORE_BOOT is `runsc create` + `runsc start` of `/bin/true` from "
             "`cmd/bench-runsc`, Sentry pinned GOMAXPROCS=4 via `taskset -c 0-3`, "
             ">=50 timed iters after >=5 warmups, p50 reported.")
    L.append("- T1 memory_bound/core_bound are the level-3 backend split "
             "(toplev on Intel, topdown-tool on Arm); thresholds: reject H1 when "
             "memory_bound >= %.0f%% and core_bound <= %.0f%%." % (
                 100 * T1_MEM_REJECT, 100 * T1_CORE_OK))
    L.append("- T2 fits CORE_BOOT(f)=a/f+b; b is the clock-invariant memory "
             "floor; b >= %.0f%% of CORE_BOOT@nominal supports H2." % (
                 100 * T2_B_LARGE_FRAC))
    L.append("- T3 counts a CORE_BOOT shift >= %.1f ms under remote-NUMA / "
             "hugepage / prefetch-toggle (ROB fixed) as movement supporting H2."
             % T3_MOVE_MS)
    L.append("- T4 Spearman compares datasheet ROB/LSQ against measured DRAM "
             "latency / MLP knee as orderers of CORE_BOOT.")
    L.append("- No measurement is fabricated or interpolated. Skipped tests are "
             "excluded, never imputed.")
    L.append("")

    with open(out_path, "w") as f:
        f.write("\n".join(L) + "\n")
    return out_path


# ==========================================================================
# Driver.
# ==========================================================================

def analyze(results_dir, out_path, plots_dir):
    hosts, warnings = load_hosts(results_dir)
    records = [build_record(h) for h in hosts]
    correlations = compute_correlations(records)
    best = best_orderer(correlations)

    per_host = {}
    for h in hosts:
        host = h["host"]
        # Wrap per-host evaluation so a single malformed host JSON degrades to a
        # skipped row instead of aborting the entire report for the good hosts.
        try:
            t1 = evaluate_t1(h)
        except Exception as e:
            t1 = ("skipped", "t1 evaluation error: %s" % e)
            warnings.append("host %s: T1 evaluation error: %s" % (host, e))
        try:
            t2 = evaluate_t2(h)
        except Exception as e:
            t2 = ("skipped", "t2 evaluation error: %s" % e, None)
            warnings.append("host %s: T2 evaluation error: %s" % (host, e))
        try:
            t3 = evaluate_t3(h)
        except Exception as e:
            t3 = {}
            warnings.append("host %s: T3 evaluation error: %s" % (host, e))
        per_host[host] = {"t1": t1, "t2": t2, "t3": t3}

    verdict, rule, vlines = overall_verdict(hosts, records, correlations, best,
                                            per_host)

    os.makedirs(plots_dir, exist_ok=True)
    plots = {}
    svg_path = os.path.join(plots_dir, "core_boot_scatter.svg")
    if hand_svg(records, svg_path):
        plots["svg"] = svg_path
    png_path = os.path.join(plots_dir, "core_boot_scatter.png")
    png = try_matplotlib_png(records, png_path)
    if png:
        plots["png"] = png

    write_report(out_path, hosts, records, correlations, best, per_host,
                 verdict, rule, vlines, warnings, plots, results_dir)

    return {
        "n_hosts": len(hosts),
        "n_with_core_boot": sum(1 for r in records if r.get("core_boot_p50") is not None),
        "verdict": verdict,
        "best_orderer": best["field"] if best else None,
        "out": out_path,
        "warnings": warnings,
    }


# ==========================================================================
# Self-test: 2 synthetic hosts through the full pipeline, on a temp dir.
# ==========================================================================

def _synth_intel_metal():
    """A synthetic Intel metal host engineered to fire the REJECT-H1 rules:
    big ROB (512) but memory-bound T1, large clock-invariant b, NUMA + hugepage
    movement, and slower CORE_BOOT than the lower-ROB-but-faster-memory peer
    (so ROB rank-DISORDERS CORE_BOOT while measured DRAM latency orders it)."""
    return {
        "schema": "window-bench/host/v1",
        "host": "c7i-metal", "arch": "amd64", "sku": "c7i.metal-24xl",
        "core": "intel-golden-cove",
        "env": {"uname_r": "6.17.0-aws", "pagesize": 4096,
                "governor": "performance", "turbo": "off",
                "gomaxprocs": 4, "numa_nodes": 2, "nominal_ghz": 3.2},
        "core_boot": {"status": "ok", "unit": "ms",
                      "p50": 134.0, "p95": 150.0, "n": 50},
        "tests": {
            "t1_tma": {"status": "ok", "tool": "toplev",
                       "slots": {"frontend_bound": 0.10, "bad_speculation": 0.05,
                                 "retiring": 0.20, "backend_bound": 0.65,
                                 "memory_bound": 0.55, "core_bound": 0.08}},
            "t2_freq": {"status": "ok",
                        "points": [{"f_ghz": 1.5, "core_boot_ms_p50": 165.0},
                                   {"f_ghz": 2.0, "core_boot_ms_p50": 150.0},
                                   {"f_ghz": 2.6, "core_boot_ms_p50": 140.0},
                                   {"f_ghz": 3.2, "core_boot_ms_p50": 134.0}]},
            "t3_numa": {"status": "ok", "local_ms_p50": 134.0,
                        "remote_ms_p50": 146.0, "delta_ms": 12.0},
            "t3_hugepage": {"status": "ok",
                            "points": [{"page": "4k", "core_boot_ms_p50": 134.0},
                                       {"page": "2m", "core_boot_ms_p50": 126.0}],
                            "delta_ms": 8.0},
            "t3_prefetch": {"status": "ok", "on_ms_p50": 134.0,
                            "off_ms_p50": 141.0, "delta_ms": 7.0},
            "t4_latency": {"status": "ok", "bench": "latency",
                           "curve": [{"wss_bytes": 4096, "ns_per_load": 1.3},
                                     {"wss_bytes": 268435456, "ns_per_load": 135.9}]},
            "t4_mlp": {"status": "ok", "bench": "mlp",
                       "curve": [{"n": 1, "loads_per_ns": 0.007},
                                 {"n": 20, "loads_per_ns": 0.10}],
                       "knee_n": 20, "peak_loads_per_ns": 0.10},
        },
    }


def _synth_graviton_xlarge():
    """A synthetic Graviton3 VM with the PMU tests skipped (virtualized events),
    lower ROB (256), but FASTER CORE_BOOT and FASTER measured DRAM than Intel --
    so datasheet ROB anti-orders CORE_BOOT while measured DRAM latency orders it
    correctly (drives T4 to support H2). Also exercises the skipped paths."""
    return {
        "schema": "window-bench/host/v1",
        "host": "c7g-xlarge", "arch": "arm64", "sku": "c7g.xlarge",
        "core": "arm-neoverse-v1",
        "env": {"uname_r": "6.17.0-aws", "pagesize": 4096,
                "governor": "performance", "turbo": "n/a",
                "gomaxprocs": 4, "numa_nodes": 1, "nominal_ghz": 2.6},
        "core_boot": {"status": "ok", "unit": "ms",
                      "p50": 118.0, "p95": 132.0, "n": 50},
        "tests": {
            "t1_tma": {"status": "skipped",
                       "reason": "PMU events virtualized on xlarge VM"},
            "t2_freq": {"status": "skipped",
                        "reason": "cpufreq not exposed on Graviton VM"},
            "t3_numa": {"status": "skipped", "reason": "single NUMA node"},
            "t3_hugepage": {"status": "ok",
                            "points": [{"page": "4k", "core_boot_ms_p50": 118.0},
                                       {"page": "2m", "core_boot_ms_p50": 117.0}],
                            "delta_ms": 1.0},
            "t3_prefetch": {"status": "skipped", "reason": "Intel-only MSR"},
            "t4_latency": {"status": "ok", "bench": "latency",
                           "curve": [{"wss_bytes": 4096, "ns_per_load": 1.1},
                                     {"wss_bytes": 268435456, "ns_per_load": 108.0}]},
            "t4_mlp": {"status": "ok", "bench": "mlp",
                       "curve": [{"n": 1, "loads_per_ns": 0.009},
                                 {"n": 16, "loads_per_ns": 0.12}],
                       "knee_n": 16, "peak_loads_per_ns": 0.12},
        },
    }


def _synth_graviton4_metal():
    """A third synthetic host so at least one correlation reaches n>=3 and the
    T4 best-orderer / comparison rule actually fires. Neoverse V2 (ROB 320),
    CORE_BOOT between the other two, measured DRAM ordered consistently with
    boot. PMU/T2/T3 skipped (so it does not add H1/H2 verdict signals)."""
    return {
        "schema": "window-bench/host/v1",
        "host": "c7g4-metal", "arch": "arm64", "sku": "c8g.metal-24xl",
        "core": "arm-neoverse-v2",
        "env": {"uname_r": "6.17.0-aws", "pagesize": 4096,
                "governor": "performance", "turbo": "n/a",
                "gomaxprocs": 4, "numa_nodes": 1, "nominal_ghz": 2.7},
        "core_boot": {"status": "ok", "unit": "ms",
                      "p50": 126.0, "p95": 138.0, "n": 50},
        "tests": {
            "t1_tma": {"status": "skipped", "reason": "PMU events virtualized"},
            "t2_freq": {"status": "skipped", "reason": "cpufreq not exposed"},
            "t3_numa": {"status": "skipped", "reason": "single NUMA node"},
            "t3_hugepage": {"status": "skipped", "reason": "THP not writable"},
            "t3_prefetch": {"status": "skipped", "reason": "Intel-only MSR"},
            "t4_latency": {"status": "ok", "bench": "latency",
                           "curve": [{"wss_bytes": 4096, "ns_per_load": 1.2},
                                     {"wss_bytes": 268435456, "ns_per_load": 120.0}]},
            "t4_mlp": {"status": "ok", "bench": "mlp",
                       "curve": [{"n": 1, "loads_per_ns": 0.008},
                                 {"n": 18, "loads_per_ns": 0.11}],
                       "knee_n": 18, "peak_loads_per_ns": 0.11},
        },
    }


def _synth_h1_survives_host():
    """A host engineered to drive the H1_SURVIVES branch: T1 directly H1-consistent
    (core/ROB-full > memory), NO H2 signals (T2/T3 skipped), big ROB + slow boot
    so ROB positively rank-orders CORE_BOOT."""
    return {
        "schema": "window-bench/host/v1",
        "host": "h1box", "arch": "amd64", "sku": "x.metal",
        "core": "intel-golden-cove",
        "env": {"uname_r": "6.17.0-aws", "pagesize": 4096,
                "governor": "performance", "turbo": "off",
                "gomaxprocs": 4, "numa_nodes": 1, "nominal_ghz": 3.2},
        "core_boot": {"status": "ok", "unit": "ms", "p50": 140.0, "n": 50},
        "tests": {
            "t1_tma": {"status": "ok", "tool": "toplev",
                       "slots": {"frontend_bound": 0.10, "bad_speculation": 0.05,
                                 "retiring": 0.20, "backend_bound": 0.65,
                                 "memory_bound": 0.15, "core_bound": 0.50}},
            "t2_freq": {"status": "skipped", "reason": "cpufreq not exposed"},
            "t3_numa": {"status": "skipped", "reason": "single node"},
            "t3_hugepage": {"status": "skipped", "reason": "n/a"},
            "t3_prefetch": {"status": "skipped", "reason": "n/a"},
        },
    }


def selftest():
    tmp = tempfile.mkdtemp(prefix="windowbench-selftest-")
    rdir = os.path.join(tmp, "results")
    pdir = os.path.join(tmp, "plots")
    os.makedirs(rdir)
    out = os.path.join(tmp, "REPORT.md")
    with open(os.path.join(rdir, "results-c7i-metal-amd64.json"), "w") as f:
        json.dump(_synth_intel_metal(), f)
    with open(os.path.join(rdir, "results-c7g-xlarge-arm64.json"), "w") as f:
        json.dump(_synth_graviton_xlarge(), f)
    # Third host so a correlation reaches n>=3 and best_orderer / T4 rule fires.
    with open(os.path.join(rdir, "results-c7g4-metal-arm64.json"), "w") as f:
        json.dump(_synth_graviton4_metal(), f)

    summary = analyze(rdir, out, pdir)

    # Assertions: prove the pipeline ran and the rules fired as designed.
    ok = True
    msgs = []

    def check(cond, label):
        nonlocal ok
        msgs.append(("PASS" if cond else "FAIL") + " " + label)
        if not cond:
            ok = False

    check(summary["n_hosts"] == 3, "ingested 3 synthetic hosts")
    check(summary["n_with_core_boot"] == 3, "all hosts have CORE_BOOT")
    check(os.path.exists(out), "REPORT.md written")

    # Re-derive the pieces to assert on them directly.
    hosts, _ = load_hosts(rdir)
    records = [build_record(h) for h in hosts]
    by_host = {r["host"]: r for r in records}
    check(by_host["c7i-metal"]["dram_ns"] == 135.9,
          "Intel DRAM plateau derived from max-WSS curve point (135.9 ns)")
    check(by_host["c7g-xlarge"]["mlp_knee"] == 16,
          "Graviton MLP knee read (16)")

    corrs = compute_correlations(records)
    rob = next(c for c in corrs if c["field"] == "rob")
    lat = next(c for c in corrs if c["field"] == "dram_ns")
    # Three hosts now -> n>=3 for ROB + DRAM, so best_orderer/T4 can fire.
    check(rob["n"] == 3 and lat["n"] == 3, "ROB + DRAM correlations reach n=3")
    best = best_orderer(corrs)
    check(best is not None and best["n"] >= 3,
          "best_orderer resolves with n>=3 (T4 comparison branch reachable)")

    # T1 must reject H1 on Intel (memory 55% >= 40%, core 8% <= 10%).
    t1v, _ = evaluate_t1(_synth_intel_metal())
    check(t1v == "reject_h1", "T1 rejects H1 on the Intel synthetic")
    # T1 on Graviton is skipped.
    t1g, _ = evaluate_t1(_synth_graviton_xlarge())
    check(t1g == "skipped", "T1 skipped on the Graviton VM synthetic")
    # T2 supports H2 on Intel (large b fraction).
    t2v, _, fit = evaluate_t2(_synth_intel_metal())
    check(t2v == "support_h2", "T2 supports H2 on Intel (large clk-invariant b)")
    check(fit is not None and fit["b"] > 0, "T2 a/b fit recovered from points")
    # T3 NUMA + hugepage + prefetch all move on Intel.
    t3 = evaluate_t3(_synth_intel_metal())
    check(t3["numa"][0] == "support_h2", "T3 NUMA moves on Intel")
    check(t3["hugepage"][0] == "support_h2", "T3 hugepage moves on Intel")
    check(t3["prefetch"][0] == "support_h2", "T3 prefetch moves on Intel")
    # T3 on Graviton: hugepage no_move (1 ms < 3), others skipped.
    t3g = evaluate_t3(_synth_graviton_xlarge())
    check(t3g["hugepage"][0] == "no_move", "T3 hugepage does not move on Graviton")
    check(t3g["numa"][0] == "skipped", "T3 NUMA skipped on Graviton")

    # Overall verdict must be REJECT_H1_SUPPORT_H2.
    check(summary["verdict"] == "REJECT_H1_SUPPORT_H2",
          "overall verdict = REJECT_H1_SUPPORT_H2")

    # A plot must have been produced (>=2 points).
    check(os.path.exists(os.path.join(pdir, "core_boot_scatter.svg")),
          "hand-built SVG emitted")

    # --- adversarial-branch coverage (the defects these exist to validate) ---

    # T2 must NOT fire support_h2 on an unphysical/low-r2 fit. (a) negative-a /
    # rising-with-clock curve; (b) near-flat noisy curve. Both -> inconclusive.
    neg_a_host = {
        "host": "x", "arch": "amd64", "core": "intel-golden-cove",
        "env": {"nominal_ghz": 3.2},
        "tests": {"t2_freq": {"status": "ok", "points": [
            {"f_ghz": 1.5, "core_boot_ms_p50": 130.0},
            {"f_ghz": 2.0, "core_boot_ms_p50": 132.0},
            {"f_ghz": 3.2, "core_boot_ms_p50": 134.0}]}}}  # boot RISES w/ clock => a<0
    t2neg, t2negd, _ = evaluate_t2(neg_a_host)
    check(t2neg != "support_h2",
          "T2 does NOT support H2 on an unphysical a<=0 / low-r2 fit")

    # H1_SURVIVES branch: T1 h1-consistent on a host, no H2 signals, ROB positively
    # orders CORE_BOOT. Build a 2-host set where ROB rho>0.5 and only h1box has T1.
    sdir = os.path.join(tmp, "survive"); os.makedirs(sdir)
    with open(os.path.join(sdir, "results-h1box-amd64.json"), "w") as f:
        json.dump(_synth_h1_survives_host(), f)
    # A faster, lower-ROB peer so ROB rank-tracks CORE_BOOT (both ascend together).
    peer = _synth_graviton_xlarge()
    peer["host"] = "peerbox"
    with open(os.path.join(sdir, "results-peerbox-arm64.json"), "w") as f:
        json.dump(peer, f)
    ssum = analyze(sdir, os.path.join(tmp, "SURV.md"), os.path.join(tmp, "sp"))
    # h1box: ROB 512 boot 140; peerbox(V1): ROB 256 boot 118 -> ROB rho=+1>0.5,
    # T1 h1-consistent, no support_h2 -> H1_SURVIVES.
    check(ssum["verdict"] == "H1_SURVIVES",
          "H1_SURVIVES fires (T1 h1-consistent + ROB orders boot, no H2 signals)")

    # Malformed-slot host must NOT abort the run: analyze() completes and the bad
    # host degrades to skipped while the good host still reports.
    mdir = os.path.join(tmp, "malformed"); os.makedirs(mdir)
    bad = _synth_intel_metal()
    bad["host"] = "badbox"
    bad["tests"]["t1_tma"]["slots"]["memory_bound"] = "0.5"   # string, not number
    bad["tests"]["t1_tma"]["slots"]["core_bound"] = "0.08"
    with open(os.path.join(mdir, "results-badbox-amd64.json"), "w") as f:
        json.dump(bad, f)
    with open(os.path.join(mdir, "results-good-arm64.json"), "w") as f:
        json.dump(_synth_graviton_xlarge(), f)
    msum = analyze(mdir, os.path.join(tmp, "MAL.md"), os.path.join(tmp, "mp"))
    check(msum["n_hosts"] == 2,
          "malformed-slot host does not abort analyze() (both hosts ingested)")
    t1bad, t1baddet = evaluate_t1(bad)
    check(t1bad == "skipped",
          "string-typed T1 slots degrade to skipped, not a TypeError crash")

    # Robustness: an empty results dir must not crash and yields INCONCLUSIVE.
    edir = os.path.join(tmp, "empty"); os.makedirs(edir)
    es = analyze(edir, os.path.join(tmp, "EMPTY.md"), os.path.join(tmp, "ep"))
    check(es["n_hosts"] == 0 and es["verdict"] == "INCONCLUSIVE",
          "empty results dir -> 0 hosts, INCONCLUSIVE (no crash)")

    print("\n".join(msgs))
    print("")
    print("self-test: %s  (report at %s)" % ("OK" if ok else "FAILED", out))
    print("verdict=%s best_orderer=%s n_hosts=%d" % (
        summary["verdict"], summary["best_orderer"], summary["n_hosts"]))
    return 0 if ok else 1


def main(argv):
    ap = argparse.ArgumentParser(description="Aggregate window/ROB bench JSONs.")
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(here)   # .window-bench
    ap.add_argument("--results-dir", default=os.path.join(root, "results"))
    ap.add_argument("--out", default=os.path.join(root, "REPORT.md"))
    ap.add_argument("--plots-dir", default=os.path.join(root, "plots"))
    ap.add_argument("--selftest", action="store_true",
                    help="run the synthetic 2-host self-test and exit")
    args = ap.parse_args(argv)

    if args.selftest:
        return selftest()

    summary = analyze(args.results_dir, args.out, args.plots_dir)
    print("analyzed %d host(s); %d with CORE_BOOT; verdict=%s; report=%s" % (
        summary["n_hosts"], summary["n_with_core_boot"], summary["verdict"],
        summary["out"]))
    if summary["warnings"]:
        for w in summary["warnings"]:
            sys.stderr.write("warning: %s\n" % w)
    if summary["n_hosts"] == 0:
        sys.stderr.write("no results-*.json found under %s\n" % args.results_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
