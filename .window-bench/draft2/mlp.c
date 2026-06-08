/*
 * mlp.c -- effective outstanding-miss concurrency (true memory-level parallelism).
 *
 * In ONE thread, chase N INDEPENDENT pointer chains simultaneously, interleaved
 * round-robin, so the core can have up to N DRAM misses in flight at once. The
 * working set is far beyond LLC (default 256 MB) so essentially every step
 * misses to DRAM. As N grows, aggregate loads/ns rises until the core's
 * outstanding-miss budget (line-fill buffers / MSHRs / transaction queue) is
 * saturated -- the KNEE -- after which loads/ns plateaus.
 *
 * The number of parallel chains N is the *independence* axis. Crucially, the
 * N pointers are stored in N separate live registers/locals, and each
 * round-robin step issues one load per chain before depending on any of them,
 * so the loads in a single round are mutually independent and can overlap. This
 * is the dual of latency.c (which has exactly one in-flight miss).
 *
 * Each chain gets its OWN disjoint Sattolo cycle over a distinct, contiguous
 * 1/N slice of the buffer, so chains can never collide and never re-fetch each
 * other's lines (which would manufacture fake cache hits and inflate loads/ns
 * at large N). The timed window is capped so no chain wraps its slice more than
 * a couple of times, keeping every load a genuine DRAM miss. The whole buffer
 * (default 256 MB) is touched regardless of N, so aggregate misses stay in DRAM
 * territory; the only thing that changes with N is how many are in flight.
 *
 * Pure POSIX + pthreads. clock_gettime(CLOCK_MONOTONIC). -O2 -fno-tree-vectorize.
 * Pin via sched_setaffinity on Linux; external taskset otherwise.
 *
 * ASCII only.
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <sched.h>
#include <unistd.h>

#define LINE 64u
#define MAX_CHAINS 64

static uint64_t prng_state = 0x243f6a8885a308d3ULL;
static inline uint64_t prng_next(void) {
    uint64_t x = prng_state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    prng_state = x;
    return x;
}

static void pin_cpu(int cpu) {
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    if (sched_setaffinity(0, sizeof(set), &set) != 0) {
        /* Non-fatal: external taskset may already pin us. */
    }
#else
    (void)cpu;
#endif
}

static inline double now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1e9 + (double)ts.tv_nsec;
}

static int cmp_double(const void *a, const void *b) {
    double da = *(const double *)a, db = *(const double *)b;
    return (da > db) - (da < db);
}

/*
 * Build a single Sattolo cycle over the slots [slot0, slot0+nseg) of the flat
 * buffer. The "next" link for a slot is stored as a FLAT element index, so the
 * chase reads flat[cur] directly. perm is a scratch array of length nseg.
 * Returns the flat element index to start chasing this segment from.
 */
static size_t build_segment(uint64_t *flat, size_t slot0, size_t nseg,
                            size_t stride, size_t *perm) {
    for (size_t i = 0; i < nseg; i++) perm[i] = slot0 + i; /* absolute slot ids */
    for (size_t i = nseg - 1; i >= 1; i--) {
        size_t j = (size_t)(prng_next() % (uint64_t)i);
        size_t t = perm[i]; perm[i] = perm[j]; perm[j] = t;
    }
    /* Link slot (slot0+i) -> perm[i] as a single cycle within the segment.
     * Following slot0 -> flat[slot0*stride] -> ... visits all nseg slots. */
    for (size_t i = 0; i < nseg; i++) {
        flat[(slot0 + i) * stride] = perm[i] * stride;
    }
    return slot0 * stride; /* start the chain at the segment's base slot */
}

volatile uint64_t g_sink = 0;

/*
 * Chase `nchains` independent chains round-robin for `rounds` rounds. Each
 * round issues exactly nchains loads that are mutually independent (each uses
 * its own cursor from the previous round), so the core can have up to nchains
 * DRAM misses in flight at once. Total loads = nchains * rounds.
 *
 * ONE uniform code path for every N (no per-N switch, so the loads/ns curve has
 * no code-path discontinuity). Within a round we advance chains in unrolled
 * blocks using SCALAR register temporaries: the flat[] loads inside a block
 * carry no data dependency on one another, and blocks within a round are
 * likewise mutually independent, so the core can keep up to nchains misses in
 * flight at once. cur[] is touched only to carry each chain's cursor across
 * rounds -- that is the intended per-chain dependency, and the L1 store/reload
 * is a few cycles, negligible against ~150 ns DRAM.
 *
 * CRITICAL (why the block temporaries matter, and why we cover the remainder
 * with register blocks too): writing the inner round as
 * `for (c..) cur[c] = flat[cur[c]]` over a runtime trip count keeps cur[] in
 * memory and the store-then-reload of cur[c] serializes the chains through the
 * stack, throttling MLP. An earlier version handled the post-4-block remainder
 * (N in {1,2,3,6,10}) with exactly that serial 1-wide loop, which made small N
 * (esp. N=2,3) artificially slow and inflated the apparent MLP scaling, pushing
 * the knee rightward. We therefore drain the remainder with a 2-wide register
 * block and a final single scalar, so EVERY chain advances via register-resident
 * independent loads. N=1 is genuinely serial (one in-flight miss) -- correct.
 *
 * All sweep N (1,2,3,4,6,8,10,12,16,20,24,32,48,64) are covered: 4-wide blocks,
 * then a 2-wide block, then 0/1 scalar.
 */
static double run_mlp(uint64_t *flat, int nchains, uint64_t rounds,
                      const size_t *starts) {
    uint64_t cur[MAX_CHAINS];
    for (int c = 0; c < nchains; c++) cur[c] = starts[c];

    uint64_t acc = 0;
    double t0 = now_ns();
    for (uint64_t r = 0; r < rounds; r++) {
        int c = 0;
        for (; c + 4 <= nchains; c += 4) {
            uint64_t a = cur[c+0], b = cur[c+1], d = cur[c+2], e = cur[c+3];
            a = flat[a]; b = flat[b]; d = flat[d]; e = flat[e];
            cur[c+0] = a; cur[c+1] = b; cur[c+2] = d; cur[c+3] = e;
        }
        if (c + 2 <= nchains) {
            uint64_t a = cur[c+0], b = cur[c+1];
            a = flat[a]; b = flat[b];
            cur[c+0] = a; cur[c+1] = b;
            c += 2;
        }
        if (c < nchains) {
            cur[c] = flat[cur[c]];
        }
    }
    double t1 = now_ns();

    for (int c = 0; c < nchains; c++) acc ^= cur[c];
    g_sink = acc; /* prevent elision of the whole chase */

    double total_loads = (double)rounds * (double)nchains;
    return total_loads / (t1 - t0); /* loads per ns */
}

int main(int argc, char **argv) {
    pin_cpu(0);

    /* Working set well beyond LLC so every step misses to DRAM. Sized so that
     * even the most-divided case (N=64) leaves each chain a segment far larger
     * than the L2/SLC (segment = wss/N = 32 MB at N=64 here, vs ~12 MB L2 on
     * Apple Firestorm and ~2 MB/core L2 on the server parts), keeping every load
     * a genuine DRAM miss rather than an L2 hit. Override via argv[2]. */
    size_t wss = 1536ull * 1024 * 1024; /* 1.5 GB */
    const char *json_path = NULL;
    if (argc > 1) json_path = argv[1];
    if (argc > 2) wss = (size_t)strtoull(argv[2], NULL, 10);

    size_t stride = LINE / sizeof(uint64_t);
    size_t nslots = wss / LINE;
    size_t nelems = nslots * stride;

    uint64_t *flat = (uint64_t *)malloc(nelems * sizeof(uint64_t));
    if (!flat) { fprintf(stderr, "OOM buf\n"); return 1; }
    memset(flat, 0, nelems * sizeof(uint64_t)); /* fault in pages */

    static const int Ns[] = {1, 2, 3, 4, 6, 8, 10, 12, 16, 20, 24, 32, 48, 64};
    const int nN = (int)(sizeof(Ns) / sizeof(Ns[0]));

    /* Scratch permutation array, sized for the largest possible segment. */
    size_t *perm = (size_t *)malloc(nslots * sizeof(size_t));
    if (!perm) { fprintf(stderr, "OOM perm\n"); return 1; }

    FILE *jf = NULL;
    if (json_path) {
        jf = fopen(json_path, "w");
        if (!jf) { fprintf(stderr, "cannot open %s\n", json_path); return 1; }
    }

    /*
     * For each N we build N disjoint Sattolo segments ONCE (a single random
     * order over the buffer, partitioned into N contiguous slices), then run
     * `reps` timed passes and take the median for stability. rounds is capped so
     * no chain wraps its segment (per_chain <= nseg): each chain visits each of
     * its lines at most once, so a pass is all cold DRAM misses. Each segment
     * (= wss/N bytes) stays far larger than any cache even at N=64, so cross-rep
     * cache reuse is bounded by the cache size, negligible vs the per-segment
     * footprint -- the median pass remains DRAM-latency-bound. The total footprint
     * touched is sum over chains <= nslots, so chains never collide and we never
     * manufacture cross-chain cache hits.
     *
     * Building once per N (not once per rep) is deliberate: the Sattolo build is
     * O(nslots) and re-running it every rep dominates wall time without changing
     * the measured load latency.
     */
    const uint64_t target_total = 18ull * 1000ull * 1000ull; /* ~18M loads/run */
    const int reps = 5;

    printf("# mlp.c  loads/ns vs number of independent chains N (WSS=%zu bytes)\n", wss);
    printf("# N LOADS_PER_NS\n");
    if (jf) fprintf(jf, "{\"bench\":\"mlp\",\"wss_bytes\":%zu,\"line_bytes\":%u,\"points\":[", wss, LINE);

    for (int ni = 0; ni < nN; ni++) {
        int N = Ns[ni];
        size_t nseg = nslots / (size_t)N;            /* slots per disjoint segment */
        uint64_t per_chain = target_total / (uint64_t)N;
        if (per_chain > nseg) per_chain = nseg;      /* wrap-safe: <= one pass */
        if (per_chain < 1) per_chain = 1;
        uint64_t rounds = per_chain;                 /* rounds == loads per chain */

        size_t starts[MAX_CHAINS];
        for (int c = 0; c < N; c++) {
            size_t slot0 = (size_t)c * nseg;         /* disjoint, contiguous slice */
            starts[c] = build_segment(flat, slot0, nseg, stride, perm);
        }

        double samples[16];
        int nsamp = 0;
        for (int r = 0; r < reps; r++) {
            double lpn = run_mlp(flat, N, rounds, starts);
            samples[nsamp++] = lpn;
        }
        qsort(samples, nsamp, sizeof(double), cmp_double);
        double median = samples[nsamp / 2];

        printf("%d %.4f\n", N, median);
        fflush(stdout);
        if (jf) {
            fprintf(jf, "%s{\"n\":%d,\"loads_per_ns\":%.6f}", (ni ? "," : ""), N, median);
            fflush(jf);
        }
    }
    free(perm);

    if (jf) { fprintf(jf, "]}\n"); fclose(jf); }
    free(flat);
    return 0;
}
