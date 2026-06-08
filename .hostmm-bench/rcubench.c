/* rcubench: isolate synchronize_rcu latency from gVisor.
 *
 * MEMBARRIER_CMD_GLOBAL is `if (num_online_cpus()>1) synchronize_rcu();` in the
 * kernel (kernel/sched/membarrier.c), so a GLOBAL loop measures one full RCU
 * grace period per call, repeatably, in a single process.
 *
 * MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED (gVisor's actual hostmm.init call)
 * runs synchronize_rcu only on the FIRST registration per mm, so we fork a fresh
 * multi-threaded child per sample to measure the full registration path.
 *
 * usage: rcubench <global|register> <N> [nthreads]
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <linux/membarrier.h>

static long mb(int cmd) { return syscall(SYS_membarrier, cmd, 0, 0); }
static double now_ms(void) {
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

static volatile int g_stop = 0;
static void *spin(void *a) {
	(void)a;
	while (!__atomic_load_n(&g_stop, __ATOMIC_RELAXED)) {
		struct timespec t = {0, 500000};
		nanosleep(&t, 0);
	}
	return 0;
}

int main(int argc, char **argv) {
	const char *mode = argc > 1 ? argv[1] : "global";
	int N = argc > 2 ? atoi(argv[2]) : 300;
	int nthr = argc > 3 ? atoi(argv[3]) : 4;
	if (nthr > 64) nthr = 64;

	long q = mb(MEMBARRIER_CMD_QUERY);
	fprintf(stderr, "# mode=%s N=%d nthr=%d query=0x%lx ncpu=%ld\n",
		mode, N, nthr, q, sysconf(_SC_NPROCESSORS_ONLN));

	if (strcmp(mode, "global") == 0) {
		if (mb(MEMBARRIER_CMD_GLOBAL) < 0) {
			fprintf(stderr, "# GLOBAL unsupported errno=%d\n", errno);
			return 2;
		}
		for (int i = 0; i < N; i++) {
			double a = now_ms();
			long r = mb(MEMBARRIER_CMD_GLOBAL);
			double b = now_ms();
			printf("%.4f\n", b - a);
			if (r < 0) fprintf(stderr, "# err i=%d errno=%d\n", i, errno);
		}
	} else {
		for (int i = 0; i < N; i++) {
			pid_t pid = fork();
			if (pid == 0) {
				pthread_t th[64];
				for (int t = 0; t < nthr; t++) pthread_create(&th[t], 0, spin, 0);
				struct timespec s = {0, 2000000};
				nanosleep(&s, 0);
				double a = now_ms();
				long r = mb(MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED);
				double b = now_ms();
				if (r < 0) { fprintf(stderr, "# child errno=%d\n", errno); _exit(1); }
				printf("%.4f\n", b - a);
				fflush(stdout);
				_exit(0);
			} else {
				int st;
				waitpid(pid, &st, 0);
			}
		}
	}
	return 0;
}
