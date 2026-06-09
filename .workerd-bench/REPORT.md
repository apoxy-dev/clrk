# workerd exec-path cold start: exec -> first response byte

**Question.** When CLRK dispatches a workerd-based agent the warm-pool way -- `runsc exec`
into an already-booted gVisor sandbox instead of cold-booting a new one -- how long from the
*start of the exec* until the *first response byte* comes back from the V8 isolate?

This is the exec-path analogue of the `/bin/true` cold-boot spike (`cmd/bench-runsc`,
[[project-runsc-coldstart-spike]]): same runsc surface (systrap, `--network=plugin`), but the
exec'd workload is Cloudflare **workerd** serving a trivial echo worker instead of `/bin/true`.

## Setup

- **Hosts (small, on purpose):** arm64 `c7g.large` (Graviton3) and amd64 `c7a.large` (AMD EPYC
  9R14). 2 vCPU / ~3.8 GiB each, Ubuntu 24.04, kernel 6.17.0-aws. Representative of a small
  worker pod, not a benchmark monster.
- **workerd:** release `v1.20260606.1`, official `workerd-linux-{arm64,64}` binary. `ldd` shows
  it needs **only glibc** (`libc`, `libm`, `ld-linux`) -- libc++ is static-linked -- so any
  glibc>=2.35 rootfs with a shell works. We used `node:bookworm-slim` (glibc 2.36); the rootfs
  only supplies libs + the `sleep infinity` init and does **not** touch the workerd measurement.
- **Worker:** `export default { async fetch(){ return new Response("ok") } }` -- the runtime
  floor (no user code, no bindings). One service, one socket, one module.
- **runsc surface:** `--platform=systrap --network=plugin --ignore-cgroups`, clrk's exact flags.
  workerd listens on a **unix domain socket** (overridden per-iteration via `--socket-addr
  http=unix:/tmp/wd-<iter>.sock`) so loopback never touches the plugin netstack and there is no
  TCP port-reuse / TIME_WAIT variance.
- **Tooling:** `cmd/bench-runsc -mode=exec-workerd` (host driver) + `cmd/workerd-runner` (the
  static helper that is exec'd in). n = 100 warm + 30 cold per host; 3 warm warmups discarded.

## How the timeline is measured (two clock domains, never subtracted across)

The exec'd `workerd-runner` writes a one-byte marker to stdout the instant it starts; the host
stamps that byte's arrival on the **host** monotonic clock -> `exec_dispatch`, with no
cross-clock subtraction. Everything after (spawn, listen, first byte) is measured on the
in-sandbox **guest** monotonic clock and reported by the runner.

| segment | clock | what it covers |
| --- | --- | --- |
| `exec_dispatch` | host | `runsc exec` RPC + sentry task creation + **the runner's own Go runtime bring-up** |
| `entry->spawn` | guest | runner work before workerd fork+exec (~0.2 ms, negligible) |
| `workerd spawn->listen` | guest | workerd process start + V8 init + snapshot deserialize + top-level eval + bind/listen |
| `listen->first_byte` | guest | accept + first-request handler compile/eval + response flush |
| **`END-TO-END exec->1B`** | host+guest | `exec_dispatch` + the guest entry->first-byte chain |

`exec_dispatch` includes the runner's Go startup under gVisor, so it is an *upper bound* on the
pure runsc-exec cost; the clean, runner-independent isolate number is **`spawn->first_byte`**.

## Results (p50, milliseconds)

### Warm-binary (reused warm sandbox, steady state, n=100)

| segment | arm64 c7g p50 | p99 | amd64 c7a p50 | p99 |
| --- | --- | --- | --- | --- |
| exec_dispatch | 27.2 | 40.4 | 25.2 | 38.9 |
| workerd spawn->listen | 28.4 | 44.4 | 26.4 | 45.6 |
| listen->first_byte | 1.0 | 2.2 | 0.6 | 1.1 |
| workerd spawn->1B (isolate cold start) | 29.4 | 45.3 | 27.2 | 46.2 |
| **END-TO-END exec->1B** | **57.1** | 70.9 | **53.2** | 78.2 |

(arm64 min 45.8 / amd64 min 45.8.)

### Cold-binary (fresh sandbox each, workerd faults cold through the gofer, n=30)

| segment | arm64 c7g p50 | p99 | amd64 c7a p50 | p99 |
| --- | --- | --- | --- | --- |
| exec_dispatch | 36.6 | 49.2 | 35.6 | 47.2 |
| workerd spawn->listen | 37.9 | 49.3 | 33.0 | 39.3 |
| listen->first_byte | 1.4 | 2.4 | 0.7 | 1.2 |
| workerd spawn->1B | 39.2 | 50.5 | 33.7 | 39.8 |
| **END-TO-END exec->1B** | **78.9** | 92.0 | **69.2** | 86.6 |

(arm64 min 59.5 / amd64 min 57.0.)

## Takeaways

1. **workerd runs cleanly under gVisor** -- every one of 260 execs returned `HTTP/1.1 200 OK`.
   The KJ event loop uses epoll, not io_uring, so the Bun-on-gVisor failure class does not apply,
   and leaving `RLIMIT_AS` unset let V8 reserve its CodeRange/pointer-cage without the OOM abort.
2. **Exec -> first byte is ~53-57 ms (warm) / ~69-79 ms (cold) p50** on a small 2-vCPU box.
   For reference, a bare `runsc exec` of `/bin/true` is ~20 ms and a full cold sandbox boot
   (create+start) is ~80-110 ms -- so warm-pool exec of a workerd isolate (~55 ms) stays well
   under a cold boot, and the isolate itself adds ~27-30 ms over the bare exec path.
3. **The workerd isolate cold start (`spawn->first_byte`) is ~27 ms (warm) / ~34-39 ms (cold)**,
   and is almost entirely `spawn->listen` (process + V8 + snapshot + top-level eval). The first
   request's handler (`listen->first_byte`) is ~1 ms -- the trivial worker compiles instantly;
   a real agent bundle would grow `spawn->listen` (and possibly `listen->first_byte` via lazy
   compile).
4. **Cold-binary costs ~16-22 ms more than warm-binary at p50** (arm64 57->79, amd64 53->69).
   That gap -- workerd's ~80 MB ELF faulting cold through the gofer into a fresh sentry, plus a
   cold exec path -- *is* the value of pre-warming workerd into a pool member (e.g. one throwaway
   exec at pool-fill time). It splits across `exec_dispatch` (+~10 ms) and `spawn->listen` (+~7-9 ms).
5. **arm64 vs amd64:** the AMD EPYC box is a touch faster end-to-end (53 vs 57 warm, 69 vs 79
   cold p50); both are 2-vCPU, and gVisor cold start is frontend/scheduling-sensitive
   ([[project-gvisor-coldboot-frontend-not-mlp]]), so a larger core would lower absolute numbers.

## Warm-request latency (once the isolate is up)

The ~27 ms above is a *one-time* process + isolate cold start, not a per-request cost. To
separate the two, the runner can issue N requests against the same workerd process
(`-requests N`, fresh unix connection each, dial -> first byte). 15 processes x 500 requests:

| bucket | min | p50 | p90 | p99 | max | n |
| --- | --- | --- | --- | --- | --- | --- |
| **arm64 c7g** first request (cold isolate) | 1.31 | 1.84 | 3.92 | 4.09 | 4.09 | 15 |
| **arm64 c7g** warm requests | 0.32 | **0.66** | 1.16 | 2.16 | 4.84 | 7485 |
| **amd64 c7a** first request (cold isolate) | 0.57 | 1.16 | 1.46 | 1.55 | 1.55 | 15 |
| **amd64 c7a** warm requests | 0.16 | **0.28** | 0.42 | 0.85 | 3.45 | 7485 |

So once the isolate is warm, workerd answers in **sub-millisecond p50** (0.28 ms amd64 / 0.66 ms
arm64) -- and that already includes a fresh unix connection through gVisor's netstack per
request. The cold start (~27 ms) is paid **once per workerd process**; every request after is
~40-100x cheaper. The "first request (cold isolate)" row (~1-2 ms) is just the first request
*after* the socket is listening -- the trivial worker's handler compiles instantly; the real
cold cost lives in `spawn->listen`, not the first request. This is exactly why warm-pool /
isolate-reuse matters: amortize the one-time ~27 ms across many sub-ms requests.

## Echo floor vs a non-trivial worker (where the payload lands)

To see what a real agent bundle costs, the same sweep was run against a generated
non-trivial worker (`worker/gen_heavy.py`): a ~1 MB module (~12k functions collected into a
dispatch table) with real top-level init (build a 20k-entry Map, compile 64 regexes, freeze a
config) and a handler that does per-request work (dispatch through 1500 functions + 10x SHA-256
over a 64 KB buffer via Web Crypto + JSON.stringify). Synthetic but representative; 15 procs x
500 requests.

p50, milliseconds (warm sandbox):

| segment | amd64 echo | amd64 heavy | arm64 echo | arm64 heavy |
| --- | --- | --- | --- | --- |
| workerd spawn->listen (bundle parse + top-level init) | 25.2 | **84.8** | 25.5 | **105.9** |
| listen->first byte (first request: lazy compile) | 1.4 | 12.0 | 2.0 | 19.0 |
| workerd spawn->1B (isolate cold start) | 26.8 | 96.4 | 27.8 | 125.0 |
| END-TO-END exec->1B | 53.2 | **122.6** | 56.5 | **152.4** |
| **warm request** (steady handler) | 0.30 | **1.05** | 0.67 | **1.91** |
| warm request p99 | 0.81 | 2.64 | 2.12 | 4.90 |

Reading it:

- **The bundle is a cold-start tax, not a per-request tax.** Going from a trivial worker to a
  1 MB bundle moved `spawn->listen` by **+60 ms (amd64) / +80 ms (arm64)** -- that's V8 parsing
  the bundle and running module init -- and the first request by another +10-17 ms (lazy
  compilation of the handler's code paths). End-to-end cold start roughly **doubled** (53->123 ms
  amd64, 57->152 ms arm64).
- **The warm request barely moved by comparison.** 640 KB of SHA-256 + dispatch + JSON per
  request added only **+0.75 ms (amd64) / +1.2 ms (arm64)** over the echo floor -- warm p50 is
  ~1-2 ms even for the heavy handler. So real handler work shows up as low-single-digit-ms warm
  latency, while bundle size dominates the one-time cold start.
- Practical implication: for a workerd-based agent, the lever that matters for cold start is
  **bundle size** (and whether you can pre-compile / snapshot it), not handler complexity; and
  isolate reuse amortizes the now-larger (~100-150 ms) cold start across ~1-2 ms warm requests.

## Caveats

- `exec_dispatch` is inflated by the runner being a Go program (Go runtime init under gVisor);
  a non-instrumented workerd-direct exec would show lower dispatch. Trust `spawn->first_byte`
  for the runner-independent isolate cost.
- Minimal echo worker = **runtime floor**, not a representative agent.
- One request then workerd is killed = cold-isolate first-request latency (correct for cold
  start), **not** steady-state request latency.
- Warm-binary reuses one sentry, so its page cache is hot -- optimistic for a never-used pool
  member; the cold-binary sweep is the honest first-dispatch number. Both are reported.

## Reproduce

`provision.sh` (2 small VMs, SSM-resolved AMIs) -> `drive.sh <ip> <label>` (ship, build,
smoke, sweep, pull) -> `teardown.sh`. The bench: `bench-runsc -mode=exec-workerd
-workerd-image=<glibc image> -warm-iters=N -cold-iters=M`. Per-iteration rows land in
`results/<label>/results/rows.<label>.jsonl`.
