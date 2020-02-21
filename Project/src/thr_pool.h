#ifndef THR_POOL_H
#define THR_POOL_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

typedef struct job_t {
    struct job_t *next_job;
    void *(*job_func) (void *);
    void *job_arg;
} job_t;

typedef struct active {
    bool is_active;
    pthread_t tid;
} active_t;


typedef struct thr_pool {
    uint32_t pool_nthreads;
    pthread_mutex_t pool_mutex;
    pthread_cond_t pool_queue_empty;
    pthread_cond_t pool_cleanup;
    pthread_cond_t pool_barrier;
    job_t *pool_head;
    job_t *pool_tail;
    active_t *active_threads;
    uint32_t pool_active_workers;
    uint32_t pool_workers;
    uint32_t pool_idle;
    int pool_flags;
    bool pool_destroy;
    bool pool_wait;
} thr_pool_t;

thr_pool_t* thr_pool_create(uint32_t);

int thr_pool_queue(thr_pool_t *, void *(*func) (void *), void *);

void thr_pool_barrier(thr_pool_t *);

int thr_pool_destroy(thr_pool_t *pool);

#endif