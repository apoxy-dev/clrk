/*
 * mlp.c -- effective outstanding-miss concurrency (true MLP).
 *
 * In ONE thread we chase N INDEPENDENT pointer chains round-robin. Because the
 * chains share no data dependency, the out-of-order core can have up to N loads
 * outstanding at once -- but only as many as its true memory-level parallelism
 * allows (line-fill buffers / MSHRs / transaction-queue entries, NOT the ROB).
 *
 * Aggregate loads/ns rises with N until that hardware limit, then plateaus. The
 * KNEE is the effective MLP. With a working set far past the LLC every hop is a
 * DRAM miss, so the plateau also exposes single-core memory bandwidth:
 *   (loads/ns at knee) * (cache line bytes) ~= GB/s.
 *
 * The N chains are interleaved over ONE shared buffer (each chain a disjoint
 * Sattolo cycle covering a 1/N stripe). Per-chain running offsets live in a
 * small array; the round-robin step reads each chain's current node, which only
 * depends on that same chain's previous node -- so the N dependent streams are
 * mutually independent and overlap.
 *
 * Build:  cc -O2 -fno-tree-vectorize -pthread -o mlp mlp.c
 * ASCII only. Pure POSIX + pthreads. CLOCK_MONOTONIC.
 * Pin externally with taskset -c <cpu> on Linux; performance-QoS on Darwin.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>

#if defined(__linux__)
#define _GNU_SOURCE
#include <sched.h>
#endif

#if defined(__APPLE__)
#include <pthread.h>
#include <pthread/qos.h>
#endif

#ifndef LINE_BYTES
#define LINE_BYTES 128
#endif

typedef struct slot {
    size_t next;                 /* byte offset (within buffer) of successor */
    char   pad[LINE_BYTES - sizeof(size_t)];
} slot_t;

/* One volatile sink per chain so no chain can be proven dead and dropped. */
#define MAX_CHAINS 64
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
    sched_setaffinity(0, sizeof(set), &set);
#elif defined(__APPLE__)
    pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
#endif
}

/*
 * Run `steps` round-robin batches over `nchains` chains, advancing off[] in
 * place. off[] persists across calls so warmup + every timed rep walk FORWARD
 * through the long per-chain cycle (no revisits within the sweep), keeping every
 * hop a cold DRAM miss across all reps.
 *
 * Independence is the whole game. Naive code (`for c: off[c]=*(base+off[c])`)
 * lets the compiler scalarize the chains into ONE result register, fusing N
 * independent streams into a single serial dependency chain -- which destroys
 * the MLP we are trying to measure. We force genuine overlap by reading every
 * chain's current offset into its OWN register, issuing all N dependent loads
 * back-to-back (no inter-chain dependency, so the core keeps up to N misses
 * outstanding), then writing the results back. A two-phase load-then-store split
 * over a tiny fixed micro-batch keeps the N loads register-disjoint without
 * relying on fragile auto-unroll heuristics. The volatile compiler barrier
 * between phases stops the optimizer from re-fusing the streams.
 */
#if defined(__GNUC__)
#define COMPILER_BARRIER() __asm__ __volatile__("" ::: "memory")
#else
#define COMPILER_BARRIER() do { } while (0)
#endif

static uint64_t run_mlp(const char *base, size_t *off, int nchains,
                        size_t steps) {
    uint64_t t0 = now_ns();
    for (size_t s = 0; s < steps; s++) {
        /* Phase 1: issue all N dependent loads into register-disjoint temps.
         * cur[c] depends only on chain c's own previous offset, so the N loads
         * have no cross-chain dependency and overlap up to the core's MLP. */
        size_t cur[MAX_CHAINS];
        for (int c = 0; c < nchains; c++) {
            cur[c] = *(const size_t *)(base + off[c]);
        }
        COMPILER_BARRIER();
        /* Phase 2: commit. Separate from phase 1 so the optimizer cannot thread
         * the loads through a single register. */
        for (int c = 0; c < nchains; c++) {
            off[c] = cur[c];
        }
    }
    uint64_t t1 = now_ns();

    for (int c = 0; c < nchains; c++) g_sink[c] = off[c];
    return t1 - t0;
}

int main(int argc, char **argv) {
    try_pin();

    /* One shared buffer, far larger than any LLC in the host matrix. Default
     * 256 MB: > M1 Pro 12 MB L2, > Graviton-3 32 MB L3, > ~2x SPR ~100 MB L3,
     * and it reproduces the clean ~140 ns DRAM plateau of latency.c without the
     * extra TLB/page-walk inflation a multi-GB buffer adds. The harness can
     * raise it for a bigger-LLC host (or lower it on a memory-tight VM) via
     * MLP_BUF_MB. Each chain owns a slot set STRIDED across the WHOLE buffer, so
     * its byte footprint is the full buffer at every N -- every hop is a cold
     * DRAM miss. */
    size_t buf_mb = 256;
    const char *env_mb = getenv("MLP_BUF_MB");
    if (env_mb && *env_mb) {
        unsigned long v = strtoul(env_mb, NULL, 10);
        if (v >= 64) buf_mb = (size_t)v;
    }
    const size_t buf_bytes = buf_mb << 20;
    size_t n = buf_bytes / sizeof(slot_t);

    static const int Ns[] = {1, 2, 3, 4, 6, 8, 10, 12, 16, 20, 24, 32, 48, 64};
    const int nN = (int)(sizeof(Ns) / sizeof(Ns[0]));
    const int reps = 3;

    /* Per-chain hop count = min(cycle_length, HOP_CAP). Each chain walks EXACTLY
     * one (partial) pass of its own Sattolo cycle with NO revisits, so every hop
     * touches a fresh line = guaranteed cold DRAM miss -- identical conditions to
     * the latency.c DRAM plateau, at every N. (The earlier bug: a fixed total
     * load budget made steps exceed the per-chain cycle length at high N, so
     * chains looped back onto their own now-cached lines and throughput jumped
     * 6x -- a cache artifact, not real MLP. Bounding hops to <= cycle length
     * kills it.) HOP_CAP also caps the slow low-N points so the sweep stays
     * inside the validation budget. */
    const size_t HOP_CAP = 1ull << 21;   /* 2,097,152 hops/chain ceiling */

    const char *json_path = (argc > 1) ? argv[1] : NULL;
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

    double lpns[64];   /* per-N throughput, for the post-sweep knee fit */

    for (int ni = 0; ni < nN; ni++) {
        int N = Ns[ni];

        /* Chain c owns the STRIDED slot set {c, c+N, c+2N, ...}. Every chain's
         * Sattolo cycle is therefore scattered across the WHOLE 256MB buffer,
         * not a contiguous sub-stripe -- so each chain keeps missing to DRAM no
         * matter how large N gets. (Contiguous stripes shrink to a cache-
         * resident footprint at high N and silently turn DRAM misses into L2
         * hits, inflating loads/ns and erasing the knee. This is the fix.) */
        size_t off[MAX_CHAINS];

        size_t cyclen = n;   /* min cycle length across chains (n/N, rounded down) */
        for (int c = 0; c < N; c++) {
            size_t m = 0;
            for (size_t g = (size_t)c; g < n; g += (size_t)N) idx[m++] = g;
            build_chain(buf, idx, m);
            off[c] = (size_t)c * sizeof(slot_t);   /* enter at slot c */
            if (m < cyclen) cyclen = m;
        }

        /* steps per rep so that warmup + all reps fit in ONE non-revisiting pass
         * of the shortest cycle: (reps+1)*steps <= cyclen. Also bound by HOP_CAP
         * to cap the slow low-N points. This guarantees every hop in every timed
         * rep is a fresh line = cold DRAM miss (no cross-rep cache reuse). */
        size_t budget = cyclen / (size_t)(reps + 1);
        size_t steps = budget < HOP_CAP ? budget : HOP_CAP;
        if (steps == 0) steps = 1;

        /* Warmup: advance off[] one step-block forward (faults + primes), and
         * leaves off[] positioned so the timed reps continue onto fresh lines. */
        run_mlp((const char *)buf, off, N, steps);

        uint64_t samples[16];
        for (int r = 0; r < reps; r++) {
            samples[r] = run_mlp((const char *)buf, off, N, steps);
        }
        qsort(samples, reps, sizeof(uint64_t), cmp_u64);
        uint64_t med_ns = samples[reps / 2];
        size_t did = steps * (size_t)N;
        double loads_per_ns = (double)did / (double)med_ns;
        lpns[ni] = loads_per_ns;

        printf("%d %.4f\n", N, loads_per_ns);
        fflush(stdout);
        /* Opt-in per-N diagnostic (effective aggregate ns/load = DRAM latency
         * divided by realized overlap); set MLP_DEBUG=1 to see it on-host. */
        if (getenv("MLP_DEBUG"))
            fprintf(stderr, "DBG N=%d steps=%zu cyclen=%zu did=%zu med_ns=%llu "
                    "ns_per_load=%.3f\n", N, steps, cyclen, did,
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

    double lpns_1 = lpns[0];                 /* single-chain throughput = 1/DRAM */
    const double KNEE_SLOPE_FRAC = 0.5;      /* half-efficiency cutoff */
    int knee_N = Ns[0];
    for (int ni = 1; ni < nN; ni++) {
        double dN = (double)(Ns[ni] - Ns[ni - 1]);
        double slope = (lpns[ni] - lpns[ni - 1]) / dN;   /* marginal gain/chain */
        if (slope >= KNEE_SLOPE_FRAC * lpns_1) {
            knee_N = Ns[ni];                 /* still scaling efficiently */
        }
    }

    printf("# knee_N=%d  peak_loads_per_ns=%.4f\n", knee_N, peak_lpns);
    if (jf) {
        fprintf(jf, "  ],\n  \"knee_n\": %d,\n  \"peak_loads_per_ns\": %.5f\n}\n",
                knee_N, peak_lpns);
        fclose(jf);
    }

    free(idx);
    free(buf);
    return 0;
}
