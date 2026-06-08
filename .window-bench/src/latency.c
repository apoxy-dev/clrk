/*
 * latency.c -- load-to-use latency vs working-set size (dependent pointer chase).
 *
 * Builds a single random permutation cycle (Sattolo's algorithm) over the
 * cache-line-granular slots of a buffer, then chases that cycle for many
 * millions of strictly dependent loads. Each load's address comes from the
 * previous load's value, so the out-of-order window cannot overlap the misses:
 * what we measure is the effective load-to-use latency at that working set.
 *
 * The single random cycle defeats the stride/next-line prefetcher. Each slot
 * occupies a full cache line (LINE_BYTES), so consecutive hops land on distinct
 * lines (no intra-line reuse hiding the miss). A volatile global sink prevents
 * the compiler from eliding the dependent chain. The chain is genuinely
 * serial (one address depends on the prior load), which is intrinsically
 * non-vectorizable; -fno-tree-vectorize is honored by gcc on the Linux hosts
 * and harmlessly ignored by clang here.
 *
 * Build:  cc -O2 -fno-tree-vectorize -pthread -o latency latency.c
 * ASCII only. Pure POSIX. CLOCK_MONOTONIC.
 *
 * Usage:  latency [json_path] [sizelist] [reps]
 *   json_path  : write JSON curve here (optional; "-" or omitted => none).
 *   sizelist   : comma-separated working-set sizes in bytes (optional; default
 *                is the built-in 4K..256M sweep). Suffixes k/K, m/M accepted.
 *                The buffer is rounded down to a whole number of cache-line
 *                slots; the JSON's wss_bytes reports that REALIZED footprint
 *                (n*sizeof(slot_t)), not the nominal request, so reported and
 *                measured WSS always agree. A size whose allocation fails is
 *                emitted as an explicit {"ns_per_load":null,"status":"skipped"}
 *                row -- never silently dropped, and never leaving a trailing
 *                comma that would corrupt the JSON array.
 *   reps       : odd number of timed reps per size (optional; default 5).
 *
 * Pin to one core externally with taskset -c <cpu> on Linux. A best-effort
 * sched_setaffinity(0) is attempted on Linux as a backstop. On Darwin there is
 * no hard affinity; we run a single thread and take the median over reps to
 * suppress migration noise.
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
    /* Sattolo: i from n-1 down to 1, swap with j strictly in [0, i). */
    for (size_t i = n - 1; i > 0; i--) {
        size_t j = (size_t)(xrand() % i);   /* strictly less than i */
        size_t t = perm[i]; perm[i] = perm[j]; perm[j] = t;
    }
    /* perm[] read in order is one cycle: link perm[k] -> perm[k+1] (mod n). */
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
    /* Best effort; if taskset already pinned us this is harmless. */
    (void)sched_setaffinity(0, sizeof(set), &set);
#endif
}

/* Parse one size token with optional k/K/m/M suffix. Returns 0 on bad input. */
static size_t parse_size(const char *s) {
    char *end = NULL;
    unsigned long long v = strtoull(s, &end, 10);
    if (end == s) return 0;
    if (*end == 'k' || *end == 'K') v <<= 10;
    else if (*end == 'm' || *end == 'M') v <<= 20;
    else if (*end == 'g' || *end == 'G') v <<= 30;
    return (size_t)v;
}

int main(int argc, char **argv) {
    try_pin();

    /* Default working-set sweep, bytes. Spans L1 -> well past the 12 MB M1 Pro
     * L2 and any server LLC (256 MB is >> 4x the largest LLC in the host
     * matrix). The caller can override with a comma-separated list in argv[2]. */
    static const size_t default_sizes[] = {
        4ull<<10, 8ull<<10, 16ull<<10, 32ull<<10, 64ull<<10, 128ull<<10,
        256ull<<10, 512ull<<10, 1ull<<20, 2ull<<20, 4ull<<20, 8ull<<20,
        16ull<<20, 32ull<<20, 64ull<<20, 128ull<<20, 256ull<<20,
    };

    /* Resolve the size list. If argv[2] is present, parse it; else use default. */
    size_t sizes_buf[128];
    const size_t *sizes;
    int nsizes;
    const char *sizelist = (argc > 2 && argv[2][0]) ? argv[2] : NULL;
    if (sizelist) {
        int k = 0;
        char *copy = strdup(sizelist);
        if (!copy) { fprintf(stderr, "oom sizelist\n"); return 1; }
        char *save = NULL;
        for (char *tok = strtok_r(copy, ",", &save);
             tok && k < (int)(sizeof(sizes_buf)/sizeof(sizes_buf[0]));
             tok = strtok_r(NULL, ",", &save)) {
            size_t v = parse_size(tok);
            if (v >= sizeof(slot_t) * 2) sizes_buf[k++] = v;
        }
        free(copy);
        if (k == 0) { fprintf(stderr, "no valid sizes parsed\n"); return 1; }
        sizes = sizes_buf;
        nsizes = k;
    } else {
        sizes = default_sizes;
        nsizes = (int)(sizeof(default_sizes) / sizeof(default_sizes[0]));
    }

    /* Target a fixed number of dependent loads per timed rep regardless of
     * working set, so each S sees the same chain length. ~64M hops keeps even
     * the ~100+ ns DRAM points to a few seconds while giving a stable median.
     * Small sets reuse the same lines from cache, which is the point. */
    const size_t target_loads = 64ull << 20;   /* 67,108,864 hops */

    int reps = 5;
    if (argc > 3 && argv[3][0]) {
        int r = atoi(argv[3]);
        if (r >= 1 && r <= 15) reps = (r % 2) ? r : r + 1;   /* force odd */
    }

    const char *json_path = (argc > 1 && argv[1][0] && strcmp(argv[1], "-")) ? argv[1] : NULL;
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

    /* Track whether any row has been emitted yet so the trailing-comma decision
     * keys off ACTUAL emission, not the loop index. A failed/skipped size emits
     * an explicit {"status":"skipped"} row (never a silent omission) and never
     * leaves a dangling comma before the closing ']'. Writing the separator
     * BEFORE each row keeps the array well-formed regardless of which sizes
     * fail. */
    int emitted = 0;

    for (int si = 0; si < nsizes; si++) {
        size_t bytes = sizes[si];
        size_t n = bytes / sizeof(slot_t);
        if (n < 2) n = 2;   /* need at least a 2-cycle */
        /* Realized footprint after rounding to whole slots; this is what is
         * actually touched and what we report as wss_bytes so reported and
         * measured WSS always agree (matters only for custom non-128-multiple
         * tokens; the default sweep is exact). */
        size_t realized = n * sizeof(slot_t);

        /* Page-aligned allocation so the page-walk channel is representative. */
        slot_t *buf = NULL;
        if (posix_memalign((void **)&buf, 4096, realized) != 0 || !buf) {
            fprintf(stderr, "oom buf at %zu bytes\n", realized);
            /* Record the unrunnable point explicitly so the curve is auditable
             * and the array stays valid JSON (separator-before-row). */
            if (jf) {
                fprintf(jf, "%s    {\"wss_bytes\": %zu, \"ns_per_load\": null, "
                            "\"status\": \"skipped\", \"reason\": \"alloc failed\"}",
                        emitted ? ",\n" : "", realized);
                emitted++;
            }
            continue;
        }
        memset(buf, 0, realized);
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

        printf("%zu %.4f\n", realized, ns_per_load);
        fflush(stdout);

        if (jf) {
            fprintf(jf, "%s    {\"wss_bytes\": %zu, \"ns_per_load\": %.4f}",
                    emitted ? ",\n" : "", realized, ns_per_load);
            emitted++;
        }

        free(buf);
    }
    if (jf) fprintf(jf, "\n");

    if (jf) {
        fprintf(jf, "  ]\n}\n");
        fclose(jf);
    }
    return 0;
}
