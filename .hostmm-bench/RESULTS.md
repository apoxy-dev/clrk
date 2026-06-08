# hostmm / membarrier cold-boot re-test (2026-06-07)

Re-test of the blog claim in `apoxy-cloud//run/blog/content/posts/gvisor-cold-start-where-the-time-goes.mdx`
that `hostmm.init`'s membarrier cost "scales with GOMAXPROCS because the syscall walks every thread."

## Setup

Four AWS EC2 hosts, all Ubuntu 24.04, kernel **6.17.0-1017-aws**, 4 KiB pages, `nohz_full` empty,
`sync_runqueues_membarrier_state` present in kallsyms (post-refactor generic membarrier impl):

| host | arch | vCPUs | uarch |
|---|---|---|---|
| c7g.xlarge | arm64 | 4 | Graviton 3 (Neoverse V1) |
| c7i.xlarge | amd64 | 4 | Sapphire Rapids |
| c7g.16xlarge | arm64 | 64 | Graviton 3 |
| c7a.16xlarge | amd64 | 64 | EPYC (blog's GP-sweep host) |

Spike: `cmd/bench-runsc` (recovered from `stash@{0}`), plus a `-probe` fast path that prints
GOMAXPROCS/NumCPU/Threads and exits before main, so `GODEBUG=inittrace=1 ./bench-runsc -probe`
measures `hostmm.init` in the bench *parent* (whose import graph includes `pkg/sentry/hostmm` via
`maincli`) at high N without a full boot.

## Verdict

The blog's mechanism is **wrong**. `REGISTER_PRIVATE_EXPEDITED`'s cost is a **single
`synchronize_rcu()` grace period**, not per-thread work. It is a near-fixed floor that:
- is **flat across GOMAXPROCS** (refutes monotonic scaling and the "GP=64=13ms noise" framing — 13ms
  IS the floor on c7a.16xlarge, occupied by every GP),
- **tracks online CPU count** (≈5-7 ms on 4-vCPU hosts, ≈13-17 ms on 64-vCPU hosts),
- **collapses ≈200-500×** under `rcu_expedited` (to ≈25 µs), uniformly across GP — the receipt.

## E1/E4 — `hostmm.init` clock (median ms, N=40), normal RCU

| host (vCPU) | GP1 | GP2 | GP4 | GP8 | GP16 | GP32 | GP64 |
|---|---|---|---|---|---|---|---|
| c7g.xlarge (4) | 5.6 | 7.0 | 5.0 | 5.3 | 5.4 | 5.5 | 4.8 |
| c7i.xlarge (4) | 6.6 | 6.1 | 5.8 | 5.9 | 5.7 | 7.2 | 5.3 |
| c7g.16xlarge (64) | 17.0 | 15.0 | 14.0 | 14.5 | 16.5 | 15.0 | 17.5 |
| c7a.16xlarge (64) | 13.5 | 13.0 | 13.0 | 13.0 | 16.0 | 14.5 | 12.0 |

Threads at startup (median) grow only 4→~14 and saturate well below GOMAXPROCS (GP64 → ~14, not 64),
yet hostmm.init is flat → not per-thread.

## E2 — decisive RCU A/B (`rcu_expedited`)

`hostmm.init` p50, GOMAXPROCS=4: c7g.xlarge 5.0 → **0.024 ms**; c7a.16xlarge 13.0 → **0.046 ms**.

CORE_BOOT p50 (create+start), GP=4 (taskset 0-3), normal → expedited:

| host | normal | expedited | Δ |
|---|---|---|---|
| c7g.xlarge | 159.9 | 125.2 | −34.7 |
| c7i.xlarge | 144.5 | 110.6 | −33.9 |
| c7g.16xlarge | 199.0 | 118.7 | −80.4 |
| c7a.16xlarge | 184.6 | 105.9 | −78.7 |

At identical Sentry GP=4, the 64-vCPU hosts show a *larger* RCU delta — grace-period latency tracks
`num_online_cpus`, not GOMAXPROCS.

## E3 — kernel attribution (bpftrace/funclatency)

c7a.16xlarge (64c): `synchronize_rcu` avg 12 425 µs ≈ `sync_runqueues_membarrier_state` avg 12 469 µs
≈ `membarrier(cmd=16)` 8-16 ms. c7g.xlarge (4c): synchronize_rcu 9 328 µs ≈ sync_runqueues 9 396 µs.
The register spends ~100% of its time in the grace period; IPI fan-out is µs. One grace period per
register (count ≈ launches).

## E4 — CORE_BOOT sweep (c7a.16xlarge), demoted to a control

normal: GP1 273, GP2 209, GP4 186, GP8 184, GP16 194, GP32 201, GP64 206.
expedited: GP1 190, GP2 130, GP4 106, GP8 102, GP16 113, GP32 119, GP64 130.
- GP=1 penalty survives expedited → single-core fork-chain serialization, not membarrier.
- GP8→64 rise survives expedited → Go-runtime overhead at high P, not membarrier.
- normal−expedited gap (~80 ms) is the RCU cost.

A full boot fires **9 `REGISTER_PRIVATE_EXPEDITED` calls** (bpftrace count) — every process in the
self-execing runsc fork chain re-runs hostmm.init.

## E5 — lazy-init patch (validated)

Patch: gate `REGISTER_PRIVATE_EXPEDITED` behind a `sync.Once` triggered from
`Have/ProcessMemoryBarrier` (only KVM's `UseHostProcessMemoryBarrier` consumes it; systrap/ptrace use
`UseHostGlobalMemoryBarrier` = `MEMBARRIER_CMD_GLOBAL`). See `membarrier_patched.go`.

Patched `hostmm.init`: c7g.xlarge **0.000 ms**, c7a.16xlarge **0.001 ms** (from 5 / 13 ms).

CORE_BOOT baseline → patched (normal RCU, systrap):

| host | GP | baseline | patched | Δ |
|---|---|---|---|---|
| c7g.xlarge | 4 | 158.8 | 135.5 | −23.3 |
| c7i.xlarge | 4 | 145.2 | 118.1 | −27.1 |
| c7g.16xlarge | 4 | 190.5 | 134.4 | −56.2 |
| c7g.16xlarge | 64 | 203.8 | 137.4 | −66.4 |
| c7a.16xlarge | 4 | 186.0 | 122.6 | −63.4 |
| c7a.16xlarge | 64 | 204.7 | 144.9 | −59.8 |

## Kernel source confirmation (v6.17 `kernel/sched/membarrier.c`)

`sync_runqueues_membarrier_state`: early `smp_mb()` iff `mm_users==1 || num_online_cpus()==1`; else
`synchronize_rcu()` once; then `on_each_cpu_mask(ipi_sync_rq_state)` only for CPUs where
`rq->curr->mm == mm`.

## Bare-metal addendum (c7g.metal 64c, c7i.metal-24xl 96c — same kernel 6.17.0-aws)

Settles whether hostmm/RCU explains the blog's "bare metal ~30% slower than xlarge."

**Mechanism holds on metal.** `hostmm.init` p50 (normal, N=40) is FLAT across GOMAXPROCS:
c7g.metal ~19-21 ms (GP 1→64), c7i.metal-24xl ~17-21 ms (GP 1→96). `rcu_expedited` collapses it:
c7g.metal 20→0.02 ms (~1000x), c7i.metal-24xl ~18→0.12-0.18 ms (~120x). E3 bpftrace:
synchronize_rcu ≈ sync_runqueues_membarrier_state — c7g.metal 17.6/17.9 ms, c7i.metal-24xl 18.6/19.0 ms.

**CORRECTION — metal is NOT slower than a same-core VM.** The prior "c7g.metal ~20 ms > c7g.16xlarge
~15 ms" was a gVisor-path measurement artifact (the full register path has ~+/-10 ms spread). A
gVisor-free controlled re-test — `membarrier(MEMBARRIER_CMD_GLOBAL)` (= one `synchronize_rcu()` per
call, kernel-confirmed), N=500, metal vs same-core VM on the SAME AMI/kernel — refutes the earlier
claim. See "Controlled same-core metal-vs-VM RCU experiment" below. The "real cores make the grace
period longer" line was WRONG: at equal core count metal ~= VM.

**The metal-vs-xlarge gap is entirely the RCU tax.** CORE_BOOT p50 (GP=4, taskset 0-3), normal→expedited:
- c7g.metal: 218 → 114 (RCU = 104 ms); c7g.xlarge: 160 → 125 (RCU = 35 ms)
- c7i.metal-24xl: 186 → 85 (RCU = 101 ms); c7i.xlarge: 144 → 111 (RCU = 34 ms)

Native-GP CORE (metal max cores vs xlarge GP4): c7g.metal 213 vs c7g.xlarge 160 = **33% slower**;
c7i.metal-24xl 192 vs c7i.xlarge 144 = **33% slower** (reproduces the blog). Decomposition: metal's
NON-RCU boot is FASTER (113 vs 125 on c7g; 102 vs 111 on c7i — more real cores), but its RCU tax
(~100 vs ~35 ms) more than offsets that, and the ~65 ms differential is the whole gap. The hostmm
sync.Once patch (−56-66 ms CORE on the VMs) targets exactly this tax → metal goes from slowest toward
fastest.

## Controlled same-core metal-vs-VM RCU experiment (2026-06-07, round 2)

Isolates `synchronize_rcu` from gVisor to answer "is metal slower than a VM of the SAME core count?"
Probe: `membarrier(MEMBARRIER_CMD_GLOBAL)` — kernel runs `if (num_online_cpus()>1) synchronize_rcu();`
(kernel/sched/membarrier.c v6.17), so a GLOBAL loop = one grace period per call, repeatable, no gVisor.
Both hosts in each pair booted the SAME Ubuntu 24.04 AMI (kernel 6.17.0-1017-aws) => identical HZ=1000,
jiffies_till_{first,next}_fqs=3, TREE_RCU/NOCB => any delta is runtime, not config.

`synchronize_rcu` p50 (ms, N=500):

| pair (same core count) | metal idle | VM idle | metal busy | VM busy | metal REGISTER | VM REGISTER |
|---|---|---|---|---|---|---|
| arm64 64c (c7g.metal vs c7g.16xlarge) | 10.00 (10.0-10.0) | 9.46 (6.0-10.0) | 24.5 | 24.0 | 19.7 | 18.6 |
| amd64 96c (c7i.metal-24xl vs c7i.24xlarge) | 10.00 (10.0-10.0) | 8.00 (6.0-10.0) | 22.0 | 19.0 | 16.4 | 12.6 |

(idle column shows p5-p95 in parens.)

- **At equal core count, metal ~= VM.** arm64: identical (10.0 vs 9.46). amd64: metal ~2 ms higher
  MEDIAN but the CEILING is identical (both p95 = 10.0). Metal is pinned at the ~10 ms floor; the VM has
  a faster lower tail (p5 6 ms) that pulls its median down. The earlier ~5 ms "metal tax" was noise in
  the gVisor full-boot path (REGISTER spread p5-p95 ~13-24 ms on both).
- **The residual is NOT deep C-states.** amd64 hosts have intel_idle (POLL/C1/C1E/C6); arm64 hosts have
  cpuidle driver=none. On the amd64 metal: disabling C1E+C6 changed nothing (10.0 -> 10.0); forcing
  POLL-only (cores never halt) dropped it to **7.0 ms, BELOW the VM**. So the small gap is a halt-idle
  timing effect on how fast the RCU grace-period kthread / FQS loop advances when CPUs halt — not a
  per-core fan-in tax and not C-state depth. Where the idle path matches (arm64, both driver=none) the
  gap is ~0.
- **~10 ms floor = ~3 FQS passes** at HZ=1000; kprobes confirm ~1 grace period per call (@gp ~= @sync
  ~= 205, @fqsloop ~= 213). rcu:rcu_grace_period tracepoints absent (CONFIG_RCU_TRACE off) so passes
  came from kprobes, not tracepoints.

Bottom line: the REAL large effect is core-count scaling of the grace period (metal/64-96c ~10 ms vs
4-vCPU xlarge ~5 ms) — unchanged, and what the blog's metal section attributes the slowdown to. The
metal-vs-SAME-core-VM difference is ~0-2 ms, a halt-idle tail effect, overstated before.
Harness: .hostmm-bench/rcubench.c, rcu_run_host.sh, cstate_test.sh, rcu_results/.
