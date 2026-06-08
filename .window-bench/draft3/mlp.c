/*
 * mlp.c -- effective outstanding-miss concurrency (true memory-level
 * parallelism) in a single core.
 *
 * We chase N INDEPENDENT random pointer chains in ONE thread, interleaved
 * round-robin. Because the chains are independent, the core's load unit can
 * have up to N misses outstanding at once -- limited by the real LFB / MSHR /
 * transaction-queue count (NOT the ROB, as long as N stays well below window
 * depth). Aggregate throughput (loads/ns) rises with N until that miss-buffer
 * limit saturates: the KNEE of that curve is the core's effective MLP.
 *
 * CRITICAL implementation detail: the N chain cursors must live in REGISTERS,
 * not in a stack array indexed by a loop variable. An array-indexed inner loop
 * (lc[c] = buf[lc[c]]) serializes through store-to-load forwarding on the
 * stack and does NOT expose N independent in-flight loads to the core -- it
 * produces a fake, knee-less "scaling" curve. We therefore dispatch to
 * fixed-width unrolled kernels (KERNEL(K) macro) where each of the K cursors
 * is a distinct local scalar the compiler keeps in a register, so the K loads
 * in the body are visibly independent and issue concurrently. Arbitrary N is
 * built by running floor(N/8) eight-wide kernels plus a remainder kernel,
 * all advancing in lockstep over the same timed iteration count.
 *
 * Working set is well beyond any LLC (default 256 MiB) so every step misses to
 * DRAM. Each chain is a disjoint single Sattolo cycle carved from one buffer.
 *
 * Build: cc -O2 -fno-tree-vectorize -pthread -o mlp mlp.c
 *
 * Output: one line per N "N LOADS_PER_NS" on stderr, plus JSON on stdout.
 * ASCII only. Pure POSIX + clock_gettime(CLOCK_MONOTONIC).
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#if defined(__linux__)
#include <sched.h>
#endif

typedef uint64_t idx_t;

#define MAX_CHAINS 64
volatile uint64_t g_sink = 0;

static double now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1e9 + (double)ts.tv_nsec;
}

static uint64_t prng_state = 0x243f6a8885a308d3ULL;
static uint64_t xrand(void) {
    uint64_t x = prng_state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    prng_state = x;
    return x * 0x2545F4914F6CDD1DULL;
}

/* Sattolo single-cycle permutation over [base, base+n), absolute indices. */
static void sattolo_range(idx_t *arr, size_t base, size_t n) {
    size_t i;
    for (i = 0; i < n; i++) arr[base + i] = (idx_t)(base + i);
    for (i = n - 1; i > 0; i--) {
        size_t j = (size_t)(xrand() % i);
        idx_t tmp = arr[base + i];
        arr[base + i] = arr[base + j];
        arr[base + j] = tmp;
    }
}

/* Fixed-width register-cursor kernels. Each advances K independent chains for
 * `steps` rounds; the K loads in the body have no data dependency on each
 * other, so the core issues them concurrently (true MLP). Cursors are passed
 * in/out by pointer but copied to locals first so they stay in registers
 * across the hot loop. Returns nothing; updates caller cursors. */

/* We write each kernel explicitly to keep cursors as named scalars. */

static void chase1(const idx_t *buf, idx_t *cur, uint64_t steps) {
    idx_t a = cur[0];
    uint64_t k;
    for (k = 0; k < steps; k++) { a = buf[a]; }
    cur[0] = a;
}
static void chase2(const idx_t *buf, idx_t *cur, uint64_t steps) {
    idx_t a = cur[0], b = cur[1];
    uint64_t k;
    for (k = 0; k < steps; k++) { a = buf[a]; b = buf[b]; }
    cur[0] = a; cur[1] = b;
}
static void chase3(const idx_t *buf, idx_t *cur, uint64_t steps) {
    idx_t a = cur[0], b = cur[1], c = cur[2];
    uint64_t k;
    for (k = 0; k < steps; k++) { a = buf[a]; b = buf[b]; c = buf[c]; }
    cur[0] = a; cur[1] = b; cur[2] = c;
}
static void chase4(const idx_t *buf, idx_t *cur, uint64_t steps) {
    idx_t a = cur[0], b = cur[1], c = cur[2], d = cur[3];
    uint64_t k;
    for (k = 0; k < steps; k++) { a = buf[a]; b = buf[b]; c = buf[c]; d = buf[d]; }
    cur[0] = a; cur[1] = b; cur[2] = c; cur[3] = d;
}
static void chase8(const idx_t *buf, idx_t *cur, uint64_t steps) {
    idx_t a = cur[0], b = cur[1], c = cur[2], d = cur[3];
    idx_t e = cur[4], f = cur[5], g = cur[6], h = cur[7];
    uint64_t k;
    for (k = 0; k < steps; k++) {
        a = buf[a]; b = buf[b]; c = buf[c]; d = buf[d];
        e = buf[e]; f = buf[f]; g = buf[g]; h = buf[h];
    }
    cur[0]=a; cur[1]=b; cur[2]=c; cur[3]=d;
    cur[4]=e; cur[5]=f; cur[6]=g; cur[7]=h;
}

/* Dispatch K (1..8) to the right register kernel. For K in {5,6,7} we fall
 * back to a 4+remainder split so cursors still stay in registers. */
static void chase_block(const idx_t *buf, idx_t *cur, int K, uint64_t steps) {
    switch (K) {
        case 1: chase1(buf, cur, steps); break;
        case 2: chase2(buf, cur, steps); break;
        case 3: chase3(buf, cur, steps); break;
        case 4: chase4(buf, cur, steps); break;
        case 5: chase4(buf, cur, steps); chase1(buf, cur + 4, steps); break;
        case 6: chase4(buf, cur, steps); chase2(buf, cur + 4, steps); break;
        case 7: chase4(buf, cur, steps); chase3(buf, cur + 4, steps); break;
        case 8: chase8(buf, cur, steps); break;
        default: break;
    }
}

/* Run N independent chains for `steps` rounds. We advance the chains in 8-wide
 * register blocks; within each block the 8 loads are independent. To expose ALL
 * N concurrently (not just 8 at a time) we DO NOT finish one block before
 * starting the next -- instead we step every block once per round, in a single
 * outer loop, so up to N loads are simultaneously in flight. Implemented by
 * looping rounds on the outside and blocks on the inside with steps=1 inner,
 * which would reintroduce call overhead; to avoid that we instead split steps
 * into chunks and interleave at chunk granularity (a chunk is short enough that
 * the miss-buffer stays saturated across blocks). */
static void chase_all(const idx_t *buf, idx_t *cur, int N, uint64_t steps) {
    /* Chunked interleave: each pass advances every chain by `chunk` rounds.
     * With chunk small relative to the MSHR drain time, the load/store unit
     * sees blocks back-to-back and keeps up to N misses outstanding. */
    const uint64_t chunk = 64;
    uint64_t done = 0;
    while (done < steps) {
        uint64_t this_chunk = steps - done;
        if (this_chunk > chunk) this_chunk = chunk;
        int off = 0;
        while (off < N) {
            int K = N - off;
            if (K > 8) K = 8;
            chase_block(buf, cur + off, K, this_chunk);
            off += K;
        }
        done += this_chunk;
    }
}

int main(int argc, char **argv) {
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(0, &set);
    (void)sched_setaffinity(0, sizeof(set), &set);
#endif

    size_t total_mib = 256;
    if (argc > 1) {
        long v = atol(argv[1]);
        if (v >= 64 && v <= 8192) total_mib = (size_t)v;
    }
    size_t total_bytes = total_mib * 1024UL * 1024UL;
    size_t total_n = total_bytes / sizeof(idx_t);

    idx_t *buf = (idx_t *)malloc(total_n * sizeof(idx_t));
    if (!buf) {
        fprintf(stderr, "alloc failed for %zu bytes\n", total_n * sizeof(idx_t));
        return 1;
    }

    const int Ns[] = {1, 2, 3, 4, 6, 8, 10, 12, 16, 20, 24, 32, 48, 64};
    const int n_Ns = sizeof(Ns) / sizeof(Ns[0]);

    /* Total dependent loads across all chains per run. */
    const uint64_t total_loads = 60ULL * 1000ULL * 1000ULL; /* 60M loads/run */
    const int reps = 9; /* odd; more reps -> the median rejects scheduler blips */

    printf("[\n");
    fflush(stdout);

    int ni;
    for (ni = 0; ni < n_Ns; ni++) {
        int N = Ns[ni];
        if (N > MAX_CHAINS) continue;

        /* N disjoint equal Sattolo cycles spanning the whole buffer, so the
         * aggregate footprint is total_bytes for every N (uniform DRAM-miss
         * assumption); concurrency N is the only variable. */
        size_t slab = total_n / (size_t)N;
        if (slab < 2) { fprintf(stderr, "skip N=%d (slab too small)\n", N); continue; }
        int c;
        for (c = 0; c < N; c++) sattolo_range(buf, (size_t)c * slab, slab);

        /* Fault in + warm the whole buffer. */
        {
            uint64_t w = 0; size_t t;
            for (t = 0; t < total_n; t++) w += buf[t];
            g_sink ^= w;
        }

        /* steps per chain so that N*steps ~= total_loads. */
        uint64_t steps = total_loads / (uint64_t)N;
        if (steps < 1) steps = 1;

        idx_t base_cur[MAX_CHAINS];
        for (c = 0; c < N; c++) base_cur[c] = (idx_t)((size_t)c * slab);

        /* Warmup (untimed). */
        {
            idx_t wc[MAX_CHAINS];
            for (c = 0; c < N; c++) wc[c] = base_cur[c];
            chase_all(buf, wc, N, 4096);
            for (c = 0; c < N; c++) g_sink ^= wc[c];
        }

        double samples[16];
        int rr;
        for (rr = 0; rr < reps; rr++) {
            idx_t lc[MAX_CHAINS];
            for (c = 0; c < N; c++) lc[c] = base_cur[c];

            double t0 = now_ns();
            chase_all(buf, lc, N, steps);
            double t1 = now_ns();

            for (c = 0; c < N; c++) g_sink ^= lc[c]; /* defeat elision */

            uint64_t did = (uint64_t)N * steps;
            samples[rr] = (double)did / (t1 - t0); /* loads per ns */
        }

        int a, b;
        for (a = 0; a < reps; a++)
            for (b = a + 1; b < reps; b++)
                if (samples[b] < samples[a]) {
                    double tmp = samples[a]; samples[a] = samples[b]; samples[b] = tmp;
                }
        double median = samples[reps / 2];
        double best = samples[reps - 1];

        fprintf(stderr, "N=%-3d %8.4f loads/ns  (max %.4f)\n", N, median, best);
        printf("  {\"n\": %d, \"loads_per_ns\": %.5f, \"max_loads_per_ns\": %.5f}%s\n",
               N, median, best, (ni + 1 < n_Ns) ? "," : "");
        fflush(stdout);
    }

    printf("]\n");
    fflush(stdout);

    if (g_sink == 0xfeedfaceULL) fprintf(stderr, "sink=%llu\n",
                                         (unsigned long long)g_sink);
    free(buf);
    return 0;
}
