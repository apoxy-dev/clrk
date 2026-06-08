# Falsifying the window/ROB hypothesis for gVisor cold boot

## The claim under test

The blog post (`apoxy-cloud//run/blog/content/posts/gvisor-cold-start-where-the-time-goes.mdx`,
section "The cloud is slower than the laptop, and it's mostly memory-level parallelism") argues that
`runsc` cold-boot time (CORE_BOOT = `runsc create` + `runsc start` of `/bin/true`) across cores is
gated by the **microarchitectural window** — reorder buffer (ROB) / load-store queue depth — because
a bigger window overlaps more outstanding DRAM misses (MLP). Its evidence is a rank match between
published ROB sizes and measured CORE_BOOT:

| Core | ROB | CORE p50 (blog) |
|---|---|---|
| Apple Firestorm (M1 Pro) | ~630 | 78 |
| Intel Golden Cove (SPR, c7i) | 512 | 134 |
| ARM Neoverse V2 (Axion) | 320 | 129 |
| ARM Neoverse V1 (Graviton 3, c7g) | 256 | 156 |

## Pre-registered hypotheses

- **H1 (window-bound):** a larger ROB/LSQ would reduce CORE_BOOT; the window is the binding resource.
- **H2 (memory-bound):** CORE_BOOT is set by effective memory access time (DRAM/L2 latency + TLB /
  page-walk cost), imperfectly hidden by *true* MLP (LFB/MSHR/transaction-queue count — NOT the ROB)
  and prefetch, plus a clock-scaling compute remainder. ROB is a non-causal correlate (design balance:
  bigger cores ship bigger ROBs *and* better memory systems).

This is adversarial: the job is to find the result that proves H2's author wrong, not to confirm H2.

## CORE_BOOT = single source of truth

Reuse `cmd/bench-runsc` (in this repo, recovered from `stash@{0}`). It mirrors the worker's runsc
invocation, self-execs as runsc, times create+start = CORE_BOOT, parses `--debug-log` boot markers.
Build natively on each host from `.hostmm-bench/clrk-src.tar.gz`. Pin Sentry GOMAXPROCS to 4 via
`taskset -c 0-3` for every run; confirm effective GOMAXPROCS from the `# Parent GOMAXPROCS=` line and
the loader `CPUs:` debug marker. >=50 timed iters after >=5 warmups; report p50 and p95.

## Experiments (decisive + cheapest first)

### Test 1 (PRIMARY, can kill H1 on one box): top-down PMU attribution of boot
Run `runsc create+start+delete` in a tight loop pinned to cores 0-3 on an idle bare-metal box, count
PMU under it system-wide on those cores.
- Intel (c7i.metal): `toplev.py --level 3 --no-mux` from Andi Kleen's pmu-tools; plus raw
  `CYCLE_ACTIVITY.STALLS_L3_MISS`, `CYCLE_ACTIVITY.STALLS_TOTAL` (the SPR/Golden-Cove any-memory
  stall proxy; `STALLS_MEM_ANY` was dropped after Skylake/CSL and does not exist on SPR),
  `DTLB_LOAD_MISSES.WALK_ACTIVE`. Each candidate event is probed before the run.
- Arm (c7g.metal, Graviton 3 / Neoverse V1): Arm `topdown-tool`; plus `STALL_BACKEND`,
  `STALL_BACKEND_MEM`, `L1D_TLB_REFILL`, `DTLB_WALK`.
Question: are boot cycles **Backend-Bound -> Memory-Bound** (parked on L3/DRAM) or **Backend-Bound ->
Core-Bound** (ROB/RS/dispatch full)? Memory-bound-dominant + small core-bound => a bigger ROB cannot
help => H1 falsified on one machine.

### Test 2: frequency sweep (split compute vs memory, ROB constant)
`cpupower frequency-set` to several P-states, measure CORE_BOOT at each, fit `CORE_BOOT(f) = a/f + b`.
`a/f` = clock-scaling compute fraction; `b` = clock-invariant memory-latency floor. Large `b` supports
H2 (and a large `a` separately rebuts the blog's "not clock-bound"). Graviton may lack cpufreq — record.

### Test 3: collinearity breakers (move memory, hold core/ROB fixed)
- **Remote-NUMA injection** (multi-socket metal only): `numactl --cpunodebind=0 --membind=1` adds
  ~50-100 ns load latency with an *identical* core. Movement => latency is the lever.
- **Hugepage sweep**: 4K vs THP `always` (2M) vs `hugetlbfs` (1G) backing the Sentry; CORE_BOOT vs page
  size with ROB fixed isolates the TLB/page-walk channel.
- **Prefetcher disable** (Intel + root + `msr`): toggle MSR `0x1A4`; a jump => prefetch was hiding the
  latency the ROB gets credit for.

### Test 4: replace the proxy, then correlate
Run two microbenches on every host for *measured* memory latency and *measured* MLP, build a hardware
table (clock, ROB, LSQ, **LFB/MSHR or txn-queue count**, L1/L2 TLB entries, L2/L3 size; cite
datasheets / Chips-and-Cheese / Dougall), then Spearman-rank CORE_BOOT against datasheet ROB,
datasheet LSQ, measured DRAM latency, and measured MLP knee. Report which orders CORE_BOOT best.
n is tiny — report, don't oversell.

## Microbenches

- `latency.c` — randomized dependent pointer-chase; ns/load vs working-set 4 KB..>=4x LLC. Single
  dependent chain, randomized permutation cycle (defeat stride prefetch), `volatile`/asm barrier.
  SANITY GATE: curve must show plateaus near host L1/L2/L3/DRAM latencies; else the bench is wrong.
- `mlp.c` — N independent interleaved chains in one thread; sweep N upward; loads/ns vs N. Knee ~=
  effective MLP (LFB/MSHR limit). SANITY GATE: knee in ~8-40; latency x knee ~= achievable BW.
Both: `taskset` one core, fix freq where possible, large iters, discard warmup, median+spread,
`-O2 -fno-tree-vectorize`, compiled+run natively per arch.

## Where each test runs (don't fight the environment)
| Test | M1 Pro (local) | c7i.metal (Intel) | c7g.metal (Arm) | c7i/c7g.xlarge (VM) |
|---|---|---|---|---|
| CORE_BOOT | no (gVisor linux-only) | yes | yes | yes |
| T1 PMU TMA | no (Apple PMU n/a) | yes (toplev) | yes (topdown-tool) | skip (events virtualized) |
| T2 freq sweep | n/a | yes | maybe (record) | maybe |
| T3 NUMA | n/a | yes (multi-socket) | maybe | no (single node) |
| T3 hugepage | n/a | yes | yes | yes |
| T3 prefetch MSR | n/a | yes | no (Intel-only) | no |
| T4 microbench | YES (native arm64) | yes | yes | yes |

## Verdict criteria (apply mechanically)
- **Reject H1** if T1 boot cycles are Backend->Memory-Bound (>=~40% of stall slots) while core/dispatch
  (ROB/RS-full) stalls are small (<=~10%). A bigger ROB cannot help a core parked on DRAM.
- **Support H2** if any: large clock-invariant `b` (T2); CORE_BOOT moves under remote-NUMA (T3, ROB
  fixed); CORE_BOOT moves under hugepage sweep (T3, ROB fixed); CORE_BOOT rank-correlates better with
  measured latency / MLP knee than with datasheet ROB (T4).
- **H1 survives only if** ROB/RS-full stalls dominate T1 AND CORE_BOOT still tracks ROB after
  controlling for measured latency. If so, flag as surprising, demand a fresh-host re-run.

## Deliverables
1. Portable bench bundle, one host, emits `results-<hostname>-<arch>.json` (raw + env + per-test
   status incl. `skipped: reason`).
2. `latency.c`, `mlp.c` (validated against sanity gates before trusting).
3. PMU scripts (Intel `toplev` + Arm `topdown-tool`).
4. `analyze.py` — ingest all per-host JSON, build hw+results table, correlations + fits + plots, write
   `REPORT.md`.
5. `REPORT.md` — per-test verdict + data + single overall H1-vs-H2 verdict with the decision rule and
   explicit n/confounds.

## Methodology rules
>=50 iters / >=5 warmups for CORE_BOOT; p50+p95. Pin cores (`taskset -c 0-3`), `nice -n -20 chrt
--fifo 50`, governor `performance` where exposed. Record per host: `uname -r`, `getconf PAGESIZE`,
NUMA topo (`lscpu`,`numactl -H`), effective Sentry GOMAXPROCS, governor/turbo, available PMU events.
NEVER fabricate/interpolate — unrunnable test => `skipped` + reason in JSON and report.

## Task checklist
- [x] T0  Build microbenches (`latency.c`,`mlp.c`); validate sanity gates locally on M1 Pro.
- [x] T0  PMU scripts (`pmu_intel.sh` toplev, `pmu_arm.sh` topdown-tool), sweeps (`freq`,`numa`,
          `hugepage`,`prefetch`), `run_host.sh` orchestrator emitting per-host JSON, `analyze.py`.
- [x] T0  Adversarial review of every artifact (compiler-elision, prefetch-defeat, PMU event names,
          fabrication guards, skipped-with-reason).
- [x] HW  Confirm host matrix + spend; provision (reuse `.hostmm-bench` provisioning + keypair/SG).
- [x] T1  PMU TMA on c7i.metal (decisive) + c7g.metal.
- [x] T2  Frequency sweep.
- [x] T3  NUMA injection (metal), hugepage sweep (all), prefetch MSR (Intel).
- [x] T4  Microbenches on every host (incl. M1 Pro); Spearman correlations.
- [x] AGG `analyze.py` -> table + plots + `REPORT.md` verdict.
- [x] DOC Update the blog if the verdict warrants; tear down; update memory.
