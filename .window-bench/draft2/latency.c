/*
 * latency.c -- load-to-use latency vs working-set size (pointer chasing).
 *
 * Builds a single random permutation cycle over the buffer using Sattolo's
 * algorithm so each element points to a pseudo-random other element. Chasing
 * the cycle yields a strictly dependent load chain (each load's address comes
 * from the previous load's value), which defeats the stride/next-line
 * prefetcher and exposes the true load-to-use latency at each cache level.
 *
 * As the working-set size (WSS) grows past each cache level, ns/load rises in
 * plateaus near L1, L2, LLC/SLC, and DRAM latency.
 *
 * Pure POSIX + pthreads. clock_gettime(CLOCK_MONOTONIC). -O2 -fno-tree-vectorize.
 * Pin via sched_setaffinity on Linux; on other platforms rely on external
 * pinning (taskset is supplied by the harness on Linux hosts).
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

/* One slot per cache-line-ish granularity. We chase indices (not raw
 * pointers) so the element is a single uint64_t holding the next index.
 * Stride between active slots is one cache line so each dependent load
 * touches a fresh line; the prefetcher cannot predict the random order. */
#define LINE 64u

/* xorshift64 PRNG -- deterministic, no libc rand contention. */
static uint64_t prng_state = 0x9e3779b97f4a7c15ULL;
static inline uint64_t prng_next(void) {
    uint64_t x = prng_state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    prng_state = x;
    return x;
}

/* Pin to a single core on Linux. No-op elsewhere (external taskset). */
static void pin_cpu(int cpu) {
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    if (sched_setaffinity(0, sizeof(set), &set) != 0) {
        /* Non-fatal: harness taskset may already pin us. */
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
 * Build a single Hamiltonian cycle over `nslots` slots using Sattolo's
 * algorithm. The slot array `buf` is laid out at LINE stride: slot i lives at
 * byte offset i*LINE, and buf_index(i) returns the uint64_t element index in
 * the flat uint64_t array. Each slot stores the index of the next slot to
 * visit. Sattolo guarantees a single cycle of length nslots (no fixed points,
 * no sub-cycles).
 */
static void build_cycle(uint64_t *flat, size_t nslots) {
    size_t stride = LINE / sizeof(uint64_t); /* elems per line */
    /* Identity: slot i -> next = i (we will permute). */
    for (size_t i = 0; i < nslots; i++) {
        flat[i * stride] = i; /* temporarily store own slot id */
    }
    /* perm[i] = the slot we will chain to after slot i, built as a single
     * cycle. We implement Sattolo over a permutation array, then write the
     * "next" link for each slot. */
    /* Use a temp permutation array of slot ids. */
    size_t *perm = (size_t *)malloc(nslots * sizeof(size_t));
    if (!perm) {
        fprintf(stderr, "OOM perm\n");
        exit(1);
    }
    for (size_t i = 0; i < nslots; i++) perm[i] = i;
    /* Sattolo: for i from n-1 down to 1, j in [0, i-1], swap perm[i],perm[j]. */
    for (size_t i = nslots - 1; i >= 1; i--) {
        size_t j = (size_t)(prng_next() % (uint64_t)i); /* j in [0, i-1] */
        size_t t = perm[i];
        perm[i] = perm[j];
        perm[j] = t;
    }
    /* perm is now a single cycle when read as a successor function:
     * follow perm[0] -> perm[perm[0]] ... visits all nslots. But Sattolo as
     * written above produces a single cyclic permutation in array form. We
     * convert it into a "next" linkage: next[i] = perm[i]. */
    for (size_t i = 0; i < nslots; i++) {
        flat[i * stride] = perm[i] * stride; /* store next as a flat elem idx */
    }
    free(perm);
}

/* Global sink so the compiler cannot elide the chase. */
volatile uint64_t g_sink = 0;

int main(int argc, char **argv) {
    pin_cpu(0);

    /* Sweep of working-set sizes in bytes. >= 4x largest LLC (256MB top). */
    static const size_t sizes[] = {
        4u * 1024, 8u * 1024, 16u * 1024, 32u * 1024, 64u * 1024,
        128u * 1024, 256u * 1024, 512u * 1024,
        1u * 1024 * 1024, 2u * 1024 * 1024, 4u * 1024 * 1024, 8u * 1024 * 1024,
        16u * 1024 * 1024, 32u * 1024 * 1024, 64u * 1024 * 1024,
        128u * 1024 * 1024, 256u * 1024 * 1024,
    };
    const int nsizes = (int)(sizeof(sizes) / sizeof(sizes[0]));

    const char *json_path = (argc > 1) ? argv[1] : NULL;
    FILE *jf = NULL;
    if (json_path) {
        jf = fopen(json_path, "w");
        if (!jf) { fprintf(stderr, "cannot open %s\n", json_path); return 1; }
    }

    /* Target a fixed number of dependent loads per timed run, regardless of
     * WSS, so each measurement amortizes ~tens of millions of loads. 12M strictly
     * dependent loads is far more than enough to swamp timer/startup overhead
     * (at DRAM latency that is ~1.7 s of pure chasing) while keeping the full
     * 17-point sweep inside the ~90 s local-validation budget. The on-host runs
     * can raise this for tighter error bars. */
    const uint64_t target_loads = 12ull * 1000ull * 1000ull; /* 12M loads */
    const int reps = 5;
    const int warmup_reps = 1;

    printf("# latency.c  M=ns/load vs working-set size (pointer chase, Sattolo)\n");
    printf("# WSS_BYTES NS_PER_LOAD\n");
    if (jf) fprintf(jf, "{\"bench\":\"latency\",\"line_bytes\":%u,\"points\":[", LINE);

    for (int si = 0; si < nsizes; si++) {
        size_t wss = sizes[si];
        size_t nslots = wss / LINE;
        if (nslots < 2) nslots = 2;
        size_t stride = LINE / sizeof(uint64_t);
        size_t nelems = nslots * stride;

        uint64_t *flat = (uint64_t *)malloc(nelems * sizeof(uint64_t));
        if (!flat) {
            fprintf(stderr, "OOM buf wss=%zu\n", wss);
            if (jf) fclose(jf);
            return 1;
        }
        /* Touch every byte to fault pages in before timing. */
        memset(flat, 0, nelems * sizeof(uint64_t));
        build_cycle(flat, nslots);

        /* Loads per rep: enough total loads, but at least one full cycle. */
        uint64_t loads_per_rep = target_loads;
        if (loads_per_rep < nslots) loads_per_rep = nslots;

        double samples[64];
        int nsamp = 0;

        for (int r = 0; r < warmup_reps + reps; r++) {
            /* Walk one full pass to warm TLB/caches before the timed window
             * (the timed window itself is long enough this barely matters for
             * large WSS, but it stabilizes small WSS). */
            uint64_t idx = 0;
            double t0 = now_ns();
            uint64_t i = 0;
            /* Unrolled-by-1 strict dependent chain. */
            for (; i < loads_per_rep; i++) {
                idx = flat[idx];
            }
            double t1 = now_ns();
            g_sink = idx; /* prevent elision */
            if (r >= warmup_reps) {
                double ns_per_load = (t1 - t0) / (double)loads_per_rep;
                samples[nsamp++] = ns_per_load;
            }
        }

        qsort(samples, nsamp, sizeof(double), cmp_double);
        double median = samples[nsamp / 2];

        printf("%zu %.4f\n", wss, median);
        fflush(stdout);
        if (jf) {
            fprintf(jf, "%s{\"wss_bytes\":%zu,\"ns_per_load\":%.6f}",
                    (si ? "," : ""), wss, median);
            fflush(jf);
        }
        free(flat);
    }

    if (jf) {
        fprintf(jf, "]}\n");
        fclose(jf);
    }
    return 0;
}
