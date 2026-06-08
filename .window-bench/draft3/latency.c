/*
 * latency.c -- load-to-use latency vs working-set size via a randomized
 * dependent pointer chase (Sattolo's algorithm).
 *
 * Each buffer slot holds the INDEX of the next slot to visit. The cycle is a
 * single random permutation (Sattolo) so consecutive addresses are
 * pseudo-random -- this defeats the stride/next-line prefetcher. The chase is
 * strictly dependent: load N's address is produced by load N-1, so the core
 * cannot overlap misses. The steady-state per-step time is therefore the
 * effective load-to-use latency at that working-set size.
 *
 * Build: cc -O2 -fno-tree-vectorize -pthread -o latency latency.c
 *
 * Output: one line per working-set size "WSS_BYTES NS_PER_LOAD" on stderr,
 * plus a machine-readable JSON array on stdout.
 *
 * ASCII only. Pure POSIX + clock_gettime(CLOCK_MONOTONIC). Pin externally with
 * taskset where available (no-op on macOS); a best-effort sched_setaffinity is
 * attempted on Linux.
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <errno.h>

#if defined(__linux__)
#include <sched.h>
#endif

/* Element of the chase. We use uint64_t indices, padded so that one element
 * occupies exactly one 64-byte slot is NOT what we want -- we want each step to
 * touch a distinct cache line at random, which the permutation already gives.
 * Keep the element a bare 8-byte index so the buffer is densely packed; the
 * Sattolo permutation guarantees random line-to-line jumps. */
typedef uint64_t idx_t;

/* Global sink so the optimizer cannot prove the chase is dead. */
volatile uint64_t g_sink = 0;

static double now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1e9 + (double)ts.tv_nsec;
}

/* xorshift64* PRNG -- deterministic, fast, no libc rand lock contention. */
static uint64_t prng_state = 0x9e3779b97f4a7c15ULL;
static uint64_t xrand(void) {
    uint64_t x = prng_state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    prng_state = x;
    return x * 0x2545F4914F6CDD1DULL;
}

/* Build a single permutation cycle over n elements with Sattolo's algorithm.
 * After this, following arr[i] -> arr[arr[i]] -> ... visits all n elements
 * exactly once before returning to the start: one giant random cycle. */
static void sattolo(idx_t *arr, size_t n) {
    size_t i;
    for (i = 0; i < n; i++) arr[i] = (idx_t)i; /* identity */
    for (i = n - 1; i > 0; i--) {
        /* j uniform in [0, i-1] -- strictly less than i (this is what makes it
         * a single cycle rather than a generic permutation). */
        size_t j = (size_t)(xrand() % i);
        idx_t tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }
}

/* Chase `steps` dependent loads starting at index 0. Returns the final index
 * (written to the global sink by the caller) so nothing can be elided. */
static idx_t chase(const idx_t *arr, uint64_t steps) {
    idx_t p = 0;
    uint64_t k;
    for (k = 0; k < steps; k++) {
        p = arr[p];
    }
    return p;
}

int main(int argc, char **argv) {
#if defined(__linux__)
    /* Best-effort pin to CPU 0 if no external taskset was used. Harmless if it
     * fails (e.g. taskset already restricted the mask). */
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(0, &set);
    (void)sched_setaffinity(0, sizeof(set), &set);
#endif
    (void)argc; (void)argv;

    /* Working-set sizes in bytes: 4K .. 256M (>= 4x the largest LLC we target).
     * Each is rounded down to a whole number of idx_t elements. */
    const size_t sizes[] = {
        4096UL, 8192UL, 16384UL, 32768UL, 65536UL, 131072UL, 262144UL,
        524288UL, 1048576UL, 2097152UL, 4194304UL, 8388608UL, 16777216UL,
        33554432UL, 67108864UL, 134217728UL, 268435456UL
    };
    const size_t nsizes = sizeof(sizes) / sizeof(sizes[0]);

    /* Total dependent loads timed per measurement. We aim for a fixed work
     * budget so small sets loop many times over the same cycle (fine: it is
     * still a dependent chain) and large sets walk a long cycle. */
    /* Per-size timed work. Small sets (L1) finish a 40M-load chain in ~50 ms;
     * DRAM-resident sets at ~130 ns/load finish 40M loads in ~5 s, so we cap
     * the large-set budget separately to keep the whole 17-size sweep inside
     * the validation time box without losing timing stability (40M / 6M loads
     * both give sub-1% run-to-run spread for a dependent chain). */
    const uint64_t target_loads = 40ULL * 1000ULL * 1000ULL; /* 40M loads */
    const uint64_t big_loads    = 6ULL * 1000ULL * 1000ULL;  /* >= 8 MiB sets */
    const uint64_t min_loads    = 4ULL * 1000ULL * 1000ULL;  /* floor */
    const int reps = 7; /* odd -> clean median */

    /* Allocate once at the largest size, reuse for smaller (rebuild perm). */
    size_t max_n = sizes[nsizes - 1] / sizeof(idx_t);
    idx_t *buf = (idx_t *)malloc(max_n * sizeof(idx_t));
    if (!buf) {
        fprintf(stderr, "alloc failed for %zu bytes\n", max_n * sizeof(idx_t));
        return 1;
    }

    printf("[\n");
    fflush(stdout);

    size_t si;
    for (si = 0; si < nsizes; si++) {
        size_t bytes = sizes[si];
        size_t n = bytes / sizeof(idx_t);
        if (n < 2) continue;

        /* Fresh single-cycle permutation for this size. */
        sattolo(buf, n);

        /* Touch the whole buffer once so pages are faulted in before timing. */
        uint64_t warm = 0;
        size_t t;
        for (t = 0; t < n; t++) warm += buf[t];
        g_sink ^= warm;

        /* Choose step count: smaller budget once the working set spills past
         * the LLC (each load is ~100+ ns there), larger budget while cheap. */
        uint64_t steps = (bytes >= (8UL << 20)) ? big_loads : target_loads;
        if (steps < min_loads) steps = min_loads;

        /* Warmup chase (not timed) to settle TLB / caches for small sets. */
        {
            idx_t r = chase(buf, n * 4 + 1024);
            g_sink ^= r;
        }

        double best_ns_per_load = 1e30;
        double samples[16];
        int r;
        for (r = 0; r < reps; r++) {
            double t0 = now_ns();
            idx_t fin = chase(buf, steps);
            double t1 = now_ns();
            g_sink ^= fin; /* defeat elision */
            double nspl = (t1 - t0) / (double)steps;
            samples[r] = nspl;
            if (nspl < best_ns_per_load) best_ns_per_load = nspl;
        }

        /* Median of reps. */
        int a, b;
        for (a = 0; a < reps; a++)
            for (b = a + 1; b < reps; b++)
                if (samples[b] < samples[a]) {
                    double tmp = samples[a]; samples[a] = samples[b]; samples[b] = tmp;
                }
        double median = samples[reps / 2];

        fprintf(stderr, "%-12zu %8.3f ns/load  (min %.3f)\n",
                bytes, median, best_ns_per_load);
        printf("  {\"wss_bytes\": %zu, \"ns_per_load\": %.4f, \"min_ns_per_load\": %.4f}%s\n",
               bytes, median, best_ns_per_load, (si + 1 < nsizes) ? "," : "");
        fflush(stdout);
    }

    printf("]\n");
    fflush(stdout);

    /* Force the sink to be observable. */
    if (g_sink == 0xdeadbeefULL) fprintf(stderr, "sink=%llu\n",
                                         (unsigned long long)g_sink);

    free(buf);
    return 0;
}
