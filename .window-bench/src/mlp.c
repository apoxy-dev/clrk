/*
 * mlp.c -- effective outstanding-miss concurrency (true memory-level parallelism).
 *
 * In ONE thread we chase N INDEPENDENT pointer chains, advancing EVERY chain
 * exactly once per round (round-robin). Because the chains share no data
 * dependency, the out-of-order core can have up to N loads outstanding at once
 * -- but only as many as its true memory-level parallelism allows (line-fill
 * buffers / MSHRs / transaction-queue entries, NOT the reorder buffer).
 *
 * Aggregate loads/ns rises with N until that hardware limit, then plateaus. The
 * KNEE is the effective MLP. With a working set far past the LLC every hop is a
 * DRAM miss, so the plateau also exposes single-core random-access memory
 * bandwidth: (loads/ns at knee) * (bytes transferred per hop) ~= GB/s. NOTE the
 * transferred size is the host's PHYSICAL cache-line size (64 B on x86 / Neoverse,
 * 128 B on Apple Silicon), NOT the LINE_BYTES=128 stride: a hop only touches the
 * first word of each 128 B slot, so on a 64 B-line host the line that fills is
 * 64 B. We do not compute GB/s in code; if derived downstream, multiply by the
 * real line size, not LINE_BYTES, or it is 2x too high on the x86/Arm targets.
 *
 * THREE things make the measurement honest, each fixing a real bug that
 * manifested as a non-monotonic or knee-less curve:
 *
 *  (1) Register-disjoint independent loads. The naive inner loop
 *      `for c: off[c] = *(base+off[c])` over a runtime trip count keeps the
 *      cursors in a stack array; the store-then-reload of off[c] serializes the
 *      chains through store-to-load forwarding and exposes only ONE in-flight
 *      miss. We instead advance the round in fixed-width register blocks
 *      (8-wide, then a 4/2/1 remainder) whose K loads are named scalars the
 *      compiler keeps in registers, and the blocks issue BACK-TO-BACK within a
 *      round (we do not drain one block before starting the next), so up to N
 *      loads are simultaneously in flight -- not just the block width.
 *
 *  (2) Contiguous spatially-disjoint per-chain stripes. Chain c owns the
 *      CONTIGUOUS slice [c*nseg, (c+1)*nseg); the N concurrently-active cursors
 *      live in far-apart regions of the buffer (offsets 0, nseg, 2*nseg, ...),
 *      so a high-N run cannot manufacture cross-chain spatial/prefetch reuse.
 *      The trap is the STRIDED ownership {c, c+N, c+2N, ...}: there the N
 *      synchronized cursors sit at N CONSECUTIVE global slots (e.g. slots 0..7
 *      = one contiguous 1 KB span), so the N simultaneously-issued loads land on
 *      adjacent/same cache lines and get cross-chain spatial/prefetch reuse,
 *      collapsing the apparent per-load latency far below DRAM at large N and
 *      erasing the knee. The shipped code uses the contiguous layout (see the
 *      inline comment at the per-N loop); do NOT "fix" it to strided.
 *
 *  (3) Bounded non-revisiting hops. Rounds are capped to one (partial) pass of
 *      the shortest per-chain cycle (and to HOP_CAP), so no chain loops back
 *      onto its own now-cached lines: every hop in every timed rep is a fresh
 *      line = cold DRAM miss, identical conditions to the latency.c DRAM
 *      plateau, at every N.
 *
 * Build:  cc -O2 -fno-tree-vectorize -pthread -o mlp mlp.c
 * ASCII only. Pure POSIX. CLOCK_MONOTONIC.
 *
 * Usage:  mlp [json_path] [Nlist] [reps]
 *   json_path : write JSON curve here (optional; "-" or omitted => none).
 *   Nlist     : comma-separated chain counts (optional; default sweep).
 *   reps      : odd timed reps per N (optional; default 5).
 * Env:
 *   MLP_BUF_MB : buffer size in MiB (default 1536; >=64). Raise on big-LLC hosts
 *                ONLY with hugepages available -- the buffer is MADV_HUGEPAGE'd
 *                on Linux, but if THP is off, oversizing past a few GB pushes
 *                the chase into a 4K page-walk regime that is NOT pure DRAM
 *                latency and contaminates the cross-host comparison.
 *   MLP_DEBUG  : if set, print per-N effective ns/load on stderr.
 *
 * Pin to one core externally with taskset -c <cpu> on Linux; a best-effort
 * sched_setaffinity(0) is attempted as a backstop.
 */

#if defined(__linux__)
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#if defined(__linux__)
#include <sched.h>
#include <sys/mman.h>
#endif

#ifndef LINE_BYTES
#define LINE_BYTES 128
#endif

#define MAX_CHAINS 64

typedef struct slot {
    size_t next;                 /* byte offset (within buffer) of successor */
    char   pad[LINE_BYTES - sizeof(size_t)];
} slot_t;

/* One volatile sink per chain so no chain can be proven dead and dropped. */
volatile size_t g_sink[MAX_CHAINS];

static inline uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static uint64_t rng_state = 0xD1B54A32D192ED03ull;
static inline uint64_t xrand(void) {
    uint64_t x = rng_state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    rng_state = x;
    return x * 0x2545F4914F6CDD1Dull;
}

static int cmp_u64(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

/*
 * Build a Sattolo single cycle over the global-slot indices listed in idx[0..m).
 * Links buf[idx[k]].next -> byte offset of idx[k+1] in the SHARED buffer, so a
 * chain started at idx[0] visits exactly its m stripe slots in one cycle and
 * never crosses into another chain's stripe.
 */
static void build_chain(slot_t *buf, size_t *idx, size_t m) {
    for (size_t i = m - 1; i > 0; i--) {
        size_t j = (size_t)(xrand() % i);
        size_t t = idx[i]; idx[i] = idx[j]; idx[j] = t;
    }
    for (size_t k = 0; k < m; k++) {
        size_t cur = idx[k];
        size_t nxt = idx[(k + 1) % m];
        buf[cur].next = nxt * sizeof(slot_t);
    }
}

static void try_pin(void) {
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(0, &set);
    (void)sched_setaffinity(0, sizeof(set), &set);
#endif
}

/* A dependent load: read the successor byte offset stored at base+off. */
#define HOP(off) (*(const size_t *)(base + (off)))

/* Compiler barrier: forbid the optimizer from sinking the phase-2 stores back
 * up into phase 1 (which would re-create a single serial dependency chain). */
#if defined(__GNUC__)
#define COMPILER_BARRIER() __asm__ __volatile__("" ::: "memory")
#else
#define COMPILER_BARRIER() do { } while (0)
#endif

/*
 * Run `rounds` round-robin rounds over `nchains` chains, advancing off[] in
 * place. Each round advances EVERY chain by exactly one hop.
 *
 * The round is two phases with ONE uniform code path for every N (no per-N
 * block cascade, so the loads/ns curve has no code-path discontinuity at N mod
 * 8 -- an 8/4/2/1 block cascade measurably perturbs N that are not multiples of
 * the widest block). Phase 1 issues all N dependent loads, unrolled by 4, into a
 * register-disjoint local array `cur[]`; each unrolled group's 4 loads carry no
 * dependency on one another and the four-wide stride lets the compiler keep them
 * in distinct registers, so up to N loads are outstanding before any result is
 * consumed -- exposing the full N-way overlap the core can sustain. A volatile
 * compiler barrier separates the phases so phase 2's commit cannot be threaded
 * back into phase 1 (which would re-fuse the N streams into one serial chain).
 *
 * off[] persists across calls so warmup + every timed rep walk FORWARD through
 * the long per-chain cycle (no revisits), keeping every hop a cold DRAM miss.
 */
static uint64_t run_mlp(const char *base, size_t *off, int nchains,
                        size_t rounds) {
    size_t cur[MAX_CHAINS];
    uint64_t t0 = now_ns();
    for (size_t s = 0; s < rounds; s++) {
        /* Phase 1: issue all N dependent loads (unrolled by 4). */
        int c = 0;
        for (; c + 4 <= nchains; c += 4) {
            size_t a = HOP(off[c+0]);
            size_t b = HOP(off[c+1]);
            size_t d = HOP(off[c+2]);
            size_t e = HOP(off[c+3]);
            cur[c+0] = a; cur[c+1] = b; cur[c+2] = d; cur[c+3] = e;
        }
        for (; c < nchains; c++) cur[c] = HOP(off[c]);
        COMPILER_BARRIER();
        /* Phase 2: commit cursors for the next round. */
        for (c = 0; c < nchains; c++) off[c] = cur[c];
    }
    uint64_t t1 = now_ns();

    for (int c = 0; c < nchains; c++) g_sink[c] = off[c];
    return t1 - t0;
}

int main(int argc, char **argv) {
    try_pin();

    /* One shared buffer, sized so that EACH chain's own distinct-line footprint
     * stays far past the LLC even at the largest N. Default 1536 MB (1.5 GB):
     * a chain owns ~slots/N distinct lines, so at N=64 each chain still treads
     * ~24M/64 ~= 384k lines ~= 48 MB -- well past the 12 MB M1 Pro L2 and the
     * ~32 MB Graviton-3 / ~100+ MB SPR L3 -- so every hop remains a cold DRAM
     * miss at every N. (A 256 MB buffer is too small: at N=48 each chain's
     * slots/N footprint shrinks below the M1 L2 and the per-chain chase turns
     * into L2 hits, producing a spurious throughput cliff.) Raise via MLP_BUF_MB
     * on a host with an even larger LLC. */
    size_t buf_mb = 1536;
    const char *env_mb = getenv("MLP_BUF_MB");
    if (env_mb && *env_mb) {
        unsigned long v = strtoul(env_mb, NULL, 10);
        if (v >= 64) buf_mb = (size_t)v;
    }
    const size_t buf_bytes = buf_mb << 20;
    size_t n = buf_bytes / sizeof(slot_t);

    /* Default chain-count sweep. Override with a comma-separated list in argv[2]. */
    static const int default_Ns[] = {1, 2, 3, 4, 6, 8, 10, 12, 16, 20, 24, 32, 48, 64};
    int Ns_buf[MAX_CHAINS];
    const int *Ns;
    int nN;
    const char *nlist = (argc > 2 && argv[2][0]) ? argv[2] : NULL;
    if (nlist) {
        int k = 0;
        char *copy = strdup(nlist);
        if (!copy) { fprintf(stderr, "oom nlist\n"); return 1; }
        char *save = NULL;
        for (char *tok = strtok_r(copy, ",", &save);
             tok && k < MAX_CHAINS; tok = strtok_r(NULL, ",", &save)) {
            int v = atoi(tok);
            if (v >= 1 && v <= MAX_CHAINS) Ns_buf[k++] = v;
        }
        free(copy);
        if (k == 0) { fprintf(stderr, "no valid N parsed\n"); return 1; }
        Ns = Ns_buf;
        nN = k;
    } else {
        Ns = default_Ns;
        nN = (int)(sizeof(default_Ns) / sizeof(default_Ns[0]));
    }

    int reps = 5;
    if (argc > 3 && argv[3][0]) {
        int r = atoi(argv[3]);
        if (r >= 1 && r <= 15) reps = (r % 2) ? r : r + 1;   /* force odd */
    }

    /* Per-chain hop count = min(cycle_length partial pass, HOP_CAP). Each chain
     * walks one partial pass of its own Sattolo cycle with NO revisits, so every
     * hop touches a fresh line = guaranteed cold DRAM miss -- identical to the
     * latency.c DRAM plateau, at every N. HOP_CAP also caps the slow low-N
     * points so the sweep stays inside the validation budget. */
    const size_t HOP_CAP = 1ull << 21;   /* 2,097,152 rounds ceiling */

    const char *json_path = (argc > 1 && argv[1][0] && strcmp(argv[1], "-")) ? argv[1] : NULL;
    FILE *jf = NULL;
    if (json_path) {
        jf = fopen(json_path, "w");
        if (!jf) { fprintf(stderr, "cannot open %s\n", json_path); return 1; }
    }

    slot_t *buf = NULL;
    if (posix_memalign((void **)&buf, 4096, n * sizeof(slot_t)) != 0 || !buf) {
        fprintf(stderr, "oom buf %zu bytes\n", n * sizeof(slot_t));
        return 1;
    }
#if defined(__linux__) && defined(MADV_HUGEPAGE)
    /* Best-effort 2M backing so that raising MLP_BUF_MB on a big-LLC host does
     * not regress the latency baseline into a TLB/page-walk-dominated regime
     * (e.g. 6 GB at 4K = ~1.5M pages >> ~3K TLB entries). Harmless if THP is
     * unavailable; the chase is still a valid DRAM-miss curve, just with the 4K
     * page-walk component the user opted into by oversizing the buffer. */
    (void)madvise(buf, n * sizeof(slot_t), MADV_HUGEPAGE);
#endif
    memset(buf, 0, n * sizeof(slot_t));

    printf("# mlp.c  line=%d B  buf=%zu MB  slots=%zu  hop_cap=%zu  reps=%d\n",
           LINE_BYTES, buf_bytes >> 20, n, HOP_CAP, reps);
    printf("# N LOADS_PER_NS\n");

    if (jf) fprintf(jf, "{\n  \"bench\": \"mlp\",\n  \"line_bytes\": %d,\n"
                        "  \"buf_bytes\": %zu,\n  \"hop_cap\": %zu,\n"
                        "  \"reps\": %d,\n  \"curve\": [\n",
                    LINE_BYTES, buf_bytes, HOP_CAP, reps);

    /* Scratch index array reused per chain build. */
    size_t *idx = (size_t *)malloc(n * sizeof(size_t));
    if (!idx) { fprintf(stderr, "oom idx\n"); return 1; }

    double lpns[MAX_CHAINS];   /* per-N throughput, for the post-sweep knee fit */

    for (int ni = 0; ni < nN; ni++) {
        int N = Ns[ni];

        /* Chain c owns a CONTIGUOUS, spatially-disjoint slice of nseg = n/N
         * consecutive slots [c*nseg, (c+1)*nseg). The chains never share a cache
         * line, so high-N runs cannot manufacture cross-chain spatial reuse (the
         * trap that a strided "every chain scattered across the whole buffer"
         * layout falls into: the N simultaneously-active cursors there walk
         * neighbouring memory regions and get cross-chain L2 hits, collapsing the
         * apparent per-load latency far below DRAM at large N). With the default
         * 1.5 GB buffer each slice is n/N * LINE_BYTES bytes = ~24 MB even at
         * N=64 -- past every LLC in the host matrix -- so each chain's Sattolo
         * cycle stays DRAM-resident and every hop is a cold miss. */
        size_t off[MAX_CHAINS];

        size_t nseg = n / (size_t)N;            /* slots per contiguous slice */
        size_t cyclen = nseg;                   /* per-chain cycle length */
        for (int c = 0; c < N; c++) {
            size_t base0 = (size_t)c * nseg;
            for (size_t i = 0; i < nseg; i++) idx[i] = base0 + i;
            build_chain(buf, idx, nseg);
            off[c] = base0 * sizeof(slot_t);    /* enter at the slice base */
        }

        /* Rounds per rep so that warmup + all reps fit in ONE non-revisiting
         * pass of the shortest cycle: (reps+1)*rounds <= cyclen. Also bound by
         * HOP_CAP to cap the slow low-N points. This guarantees every hop in
         * every timed rep is a fresh line = cold DRAM miss. */
        size_t budget = cyclen / (size_t)(reps + 1);
        size_t rounds = budget < HOP_CAP ? budget : HOP_CAP;
        if (rounds == 0) rounds = 1;

        /* Warmup: advance off[] one round-block forward (faults + primes), and
         * leaves off[] positioned so the timed reps continue onto fresh lines. */
        run_mlp((const char *)buf, off, N, rounds);

        uint64_t samples[16];
        for (int r = 0; r < reps; r++) {
            samples[r] = run_mlp((const char *)buf, off, N, rounds);
        }
        qsort(samples, reps, sizeof(uint64_t), cmp_u64);
        uint64_t med_ns = samples[reps / 2];
        size_t did = rounds * (size_t)N;
        double loads_per_ns = (double)did / (double)med_ns;
        lpns[ni] = loads_per_ns;

        printf("%d %.5f\n", N, loads_per_ns);
        fflush(stdout);
        if (getenv("MLP_DEBUG"))
            fprintf(stderr, "DBG N=%d rounds=%zu cyclen=%zu did=%zu med_ns=%llu "
                    "ns_per_load=%.3f\n", N, rounds, cyclen, did,
                    (unsigned long long)med_ns, (double)med_ns / (double)did);

        if (jf) {
            fprintf(jf, "    {\"n\": %d, \"loads_per_ns\": %.5f}%s\n",
                    N, loads_per_ns, (ni == nN - 1) ? "" : ",");
        }
    }

    /* Effective MLP = the half-efficiency elbow of the latency-hiding curve.
     * In the ideal (fully-overlapped) regime aggregate throughput scales like
     * N * lpns_1 (each added chain hides one more full DRAM latency), so the
     * ideal marginal gain per added chain is lpns_1. The knee is the largest N
     * whose marginal gain per chain still clears half that ideal slope; beyond
     * it the core has run out of fill buffers / MSHRs / txn-queue entries and
     * extra chains barely help. This elbow is robust to the slow, never-quite-
     * flat tail that a pure "% of peak throughput" rule mis-reads as no knee. */
    double peak_lpns = 0.0;
    for (int ni = 0; ni < nN; ni++)
        if (lpns[ni] > peak_lpns) peak_lpns = lpns[ni];

    /* The half-efficiency knee compares each step's marginal gain against the
     * IDEAL single-chain slope lpns_1 = throughput at N=1 (= 1/DRAM latency).
     * That anchor is only valid when the sweep actually starts at N=1; with a
     * custom Nlist that starts above 1, lpns[0] is an ALREADY-overlapped
     * multi-chain throughput, the 0.5*lpns_1 threshold is inflated ~N0x, no
     * marginal slope can clear it, and the "knee" degenerates to the first
     * point -- a meaningless artifact. So we require Ns[0]==1; otherwise we
     * emit knee_n as null with a reason rather than a fabricated elbow. */
    int has_n1 = (Ns[0] == 1);
    int knee_N = -1;
    if (has_n1) {
        double lpns_1 = lpns[0];             /* single-chain throughput = 1/DRAM */
        const double KNEE_SLOPE_FRAC = 0.5;  /* half-efficiency cutoff */
        knee_N = Ns[0];
        for (int ni = 1; ni < nN; ni++) {
            double dN = (double)(Ns[ni] - Ns[ni - 1]);
            double slope = (lpns[ni] - lpns[ni - 1]) / dN;   /* marginal gain/chain */
            if (slope >= KNEE_SLOPE_FRAC * lpns_1) {
                knee_N = Ns[ni];             /* still scaling efficiently */
            }
        }
    }

    if (has_n1)
        printf("# knee_N=%d  peak_loads_per_ns=%.5f\n", knee_N, peak_lpns);
    else
        printf("# knee_N=null (sweep must start at N=1 to anchor 1/DRAM "
               "baseline)  peak_loads_per_ns=%.5f\n", peak_lpns);
    if (jf) {
        if (has_n1) {
            fprintf(jf, "  ],\n  \"knee_n\": %d,\n  "
                        "\"peak_loads_per_ns\": %.5f\n}\n",
                    knee_N, peak_lpns);
        } else {
            fprintf(jf, "  ],\n  \"knee_n\": null,\n"
                        "  \"knee_status\": \"skipped\",\n"
                        "  \"knee_reason\": \"sweep must start at N=1 to anchor "
                        "1/DRAM baseline\",\n"
                        "  \"peak_loads_per_ns\": %.5f\n}\n",
                    peak_lpns);
        }
        fclose(jf);
    }

    free(idx);
    free(buf);
    return 0;
}
