/* Reproducer for a polling worker starving timed waiters under Callgrind. */
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <time.h>
#include <valgrind/callgrind.h>

static atomic_int finished;

static void *poll_worker(void *unused) {
    struct timespec now;
    while (atomic_load(&finished) < 3) {
        clock_gettime(CLOCK_MONOTONIC, &now);
    }
    return NULL;
}

static void *sleep_worker(void *unused) {
    const struct timespec delay = { .tv_sec = 0, .tv_nsec = 1000000 };
    nanosleep(&delay, NULL);
    atomic_fetch_add(&finished, 1);
    return NULL;
}

int main(void) {
    pthread_t workers[4];
    puts("starting four threads");
    fflush(stdout);
    CALLGRIND_START_INSTRUMENTATION;
    pthread_create(&workers[0], NULL, poll_worker, NULL);
    for (int i = 1; i < 4; i++) pthread_create(&workers[i], NULL, sleep_worker, NULL);
    for (int i = 0; i < 4; i++) pthread_join(workers[i], NULL);
    CALLGRIND_STOP_INSTRUMENTATION;
    puts("all three sleepers and polling worker finished");
    return 0;
}
