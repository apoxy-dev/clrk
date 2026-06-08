/*
 * latency.c -- load-to-use latency vs working-set size (dependent pointer chase).
 *
 * Builds a single random permutation cycle (Sattolo's algorithm) over the
 * cache-line-granular slots of a buffer, then chases the cycle for many
 * millions of strictly dependent loads. Each load's address comes from the
 * previous load's value, so the out-of-order window cannot overlap the misses:
 * what we measure is the effective load-to-use latency at that working set.
 *
 * The random permutation defeats the stride/next-line prefetchers. We stride by
 * a full cache line so consecutive hops land on distinct lines (no intra-line
 * reuse hiding the miss). A volatile global sink prevents the compiler from
 * eliding the dependent chain.
 *
 * Build:  cc -O2 -fno-tree-vectorize -pthread -o latency latency.c
 * ASCII only. Pure POSIX + pthreads. CLOCK_MONOTONIC.
 *
 * Pin to one core externally with taskset -c <cpu> on Linux. On Darwin we ask
 * the scheduler for a performance-core QoS; there is no hard pin, so we run a
 * single thread and take the median over reps to suppress migration noise.
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

/* Cache-line stride. 128 bytes covers Apple Silicon (128 B lines) and is a
 * safe multiple of the 64 B lines on x86 / Neoverse: each hop still lands on a
 * fresh line there too. */
#ifndef LINE_BYTES
#define LINE_BYTES 128
#endif

/* One slot per cache line. We store the byte offset of the next slot so the
 * chase is a single dependent load + add. */
typedef struct slot {
    size_t next;                 /* byte offset of the successor slot */
    char   pad[LINE_BYTES - sizeof(size_t)];
} slot_t;

/* Volatile sink so the compiler cannot prove the chase is dead. */
volatile size_t g_sink;

static inline uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* xorshift64* -- deterministic, no libc rand() lock, good enough to shuffle. */
static uint64_t rng_state = 0x9E3779B97F4A7C15ull;
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
 * Build a single Hamiltonian cycle over n slots via Sattolo's algorithm.
 * Sattolo guarantees one cycle of length n (a full permutation cycle), unlike
 * Fisher-Yates which can produce shorter sub-cycles. We then thread the "next"
 * offset of each slot to follow that cycle.
 */
static void build_cycle(slot_t *buf, size_t n) {
    size_t *perm = (size_t *)malloc(n * sizeof(size_t));
    if (!perm) { fprintf(stderr, "oom perm\n"); exit(1); }
    for (size_t i = 0; i < n; i++) perm[i] = i;
    /* Sattolo: i from n-1 down to 1, swap with j in [0, i). */
    for (size_t i = n - 1; i > 0; i--) {
        size_t j = (size_t)(xrand() % i);   /* strictly less than i */
        size_t t = perm[i]; perm[i] = perm[j]; perm[j] = t;
    }
    /* perm[] is now a single cycle: perm[k] -> perm[k+1] (mod n). Link them. */
    for (size_t k = 0; k < n; k++) {
        size_t cur = perm[k];
        size_t nxt = perm[(k + 1) % n];
        buf[cur].next = nxt * sizeof(slot_t);
    }
    free(perm);
}

/* Chase `loads` dependent hops starting at byte offset 0. Returns final offset
 * (also parked in the volatile sink) so nothing is dead code. */
static size_t chase(const char *base, size_t loads) {
    size_t off = 0;
    for (size_t i = 0; i < loads; i++) {
        off = *(const size_t *)(base + off);   /* dependent load */
    }
    g_sink = off;
    return off;
}

static void try_pin(void) {
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(0, &set);
    /* Best effort; if taskset already pinned us this is a no-op-ish. */
    sched_setaffinity(0, sizeof(set), &set);
#elif defined(__APPLE__)
    /* No hard affinity on Darwin; request a performance core. */
    pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
#endif
}

int main(int argc, char **argv) {
    try_pin();

    /* Working-set sweep, bytes. Spans L1 -> well past the 12 MB M1 Pro L2 /
     * any server LLC (256 MB is >> 4x the largest LLC in the host matrix). */
    static const size_t sizes[] = {
        4ull<<10, 8ull<<10, 16ull<<10, 32ull<<10, 64ull<<10, 128ull<<10,
        256ull<<10, 512ull<<10, 1ull<<20, 2ull<<20, 4ull<<20, 8ull<<20,
        16ull<<20, 32ull<<20, 64ull<<20, 128ull<<20, 256ull<<20,
    };
    const int nsizes = (int)(sizeof(sizes) / sizeof(sizes[0]));

    /* Target a fixed number of dependent loads per timed rep regardless of
     * working set, so each S sees the same chain length. ~64M hops keeps even
     * the ~100+ ns DRAM points under a few seconds while giving a stable
     * median. Small sets reuse the same lines from cache, which is the point. */
    const size_t target_loads = 64ull << 20;   /* 67,108,864 hops */
    const int reps = 5;

    const char *json_path = (argc > 1) ? argv[1] : NULL;
    FILE *jf = NULL;
    if (json_path) {
        jf = fopen(json_path, "w");
        if (!jf) { fprintf(stderr, "cannot open %s\n", json_path); return 1; }
    }

    printf("# latency.c  line=%d B  target_loads=%zu  reps=%d\n",
           LINE_BYTES, target_loads, reps);
    printf("# WSS_BYTES NS_PER_LOAD\n");

    if (jf) fprintf(jf, "{\n  \"bench\": \"latency\",\n  \"line_bytes\": %d,\n"
                        "  \"target_loads\": %zu,\n  \"reps\": %d,\n  \"curve\": [\n",
                    LINE_BYTES, target_loads, reps);

    for (int si = 0; si < nsizes; si++) {
        size_t bytes = sizes[si];
        size_t n = bytes / sizeof(slot_t);
        if (n < 2) n = 2;   /* need at least a 2-cycle */

        /* Page-aligned allocation so the page-walk channel is representative. */
        slot_t *buf = NULL;
        if (posix_memalign((void **)&buf, 4096, n * sizeof(slot_t)) != 0 || !buf) {
            fprintf(stderr, "oom buf at %zu bytes\n", n * sizeof(slot_t));
            continue;
        }
        memset(buf, 0, n * sizeof(slot_t));
        build_cycle(buf, n);

        const char *base = (const char *)buf;

        /* Warmup: walk the whole cycle once to fault pages + prime TLB/cache
         * state to the steady working set, then a short timed-shape warmup. */
        chase(base, n);
        chase(base, target_loads / 8);

        uint64_t samples[16];
        for (int r = 0; r < reps; r++) {
            uint64_t t0 = now_ns();
            chase(base, target_loads);
            uint64_t t1 = now_ns();
            samples[r] = t1 - t0;
        }
        qsort(samples, reps, sizeof(uint64_t), cmp_u64);
        uint64_t med_ns = samples[reps / 2];
        double ns_per_load = (double)med_ns / (double)target_loads;

        printf("%zu %.3f\n", bytes, ns_per_load);
        fflush(stdout);

        if (jf) {
            fprintf(jf, "    {\"wss_bytes\": %zu, \"ns_per_load\": %.4f}%s\n",
                    bytes, ns_per_load, (si == nsizes - 1) ? "" : ",");
        }

        free(buf);
    }

    if (jf) {
        fprintf(jf, "  ]\n}\n");
        fclose(jf);
    }
    return 0;
}
