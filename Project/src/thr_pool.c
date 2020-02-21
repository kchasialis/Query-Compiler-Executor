#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include "alloc_free.h"
#include "dbg.h"
#include "thr_pool.h"

#pragma GCC diagnostic ignored "-Wincompatible-pointer-types"

#define POOL_WAIT 0x01
#define POOL_DESTROY 0x02

thr_pool_t* thr_pool_create(uint32_t nthreads) {

    thr_pool_t *thr_pool = MALLOC(thr_pool_t, 1);
    check_mem(thr_pool);

    thr_pool->pool_nthreads = nthreads;
    pthread_mutex_init(&thr_pool->pool_mutex, NULL);
    pthread_cond_init(&thr_pool->pool_queue_empty, NULL);
    pthread_cond_init(&thr_pool->pool_cleanup, NULL);
    pthread_cond_init(&thr_pool->pool_barrier, NULL);
    thr_pool->pool_wait = false;
    thr_pool->pool_destroy = false;
    thr_pool->pool_head = NULL;
    thr_pool->pool_tail = NULL;
    thr_pool->pool_idle = 0;
    thr_pool->pool_workers = 0;
    thr_pool->pool_active_workers = 0;
    thr_pool->active_threads = MALLOC(active_t, nthreads);
    check_mem(thr_pool->active_threads);
    for (ssize_t i = 0 ; i < nthreads ; i++) {
        thr_pool->active_threads[i].is_active = false;
    }

    return thr_pool;

    error:
        return NULL;
}

static void worker_cleanup(thr_pool_t *pool) {

    /*Pool is being destroyed*/
    pool->pool_workers--;
    if (pool->pool_workers == 0) {
        pthread_cond_broadcast(&pool->pool_cleanup);
    }
    pthread_mutex_unlock(&pool->pool_mutex);
}

static void job_cleanup(thr_pool_t *pool) {

    pthread_t current = pthread_self();
    ssize_t index = 0;
    pthread_mutex_lock(&pool->pool_mutex);
    for (ssize_t i = 0 ; i < (ssize_t) pool->pool_active_workers ; i++) {
        if (pool->active_threads[i].tid == current) {
            pool->active_threads[i].is_active = false;
            index = i;
        }
    }
    for (ssize_t i = index ; i < (ssize_t) pool->pool_active_workers - 1; i++) {
        pool->active_threads[i] = pool->active_threads[i + 1];
    }
    pool->pool_active_workers--;

    if (pool->pool_wait) {
        if (pool->pool_head == NULL && pool->pool_active_workers == 0) {
            pool->pool_wait = false;
            pthread_cond_broadcast(&pool->pool_barrier);
        }
    }
}

static void *worker_thread(void *argm) {
    
    thr_pool_t *pool = (thr_pool_t *) argm;
    void *(*func)(void *);
    job_t *job;

    pthread_mutex_lock(&pool->pool_mutex);
    pthread_cleanup_push(worker_cleanup, pool);
    while (1) {
        pool->pool_idle++;
        if (pool->pool_wait) {
            if (pool->pool_head == NULL && pool->pool_active_workers == 0) {
                pool->pool_wait = false;
                pthread_cond_broadcast(&pool->pool_barrier);
            }
        }
        while (pool->pool_head == NULL && !pool->pool_destroy) {   //While queue is empty
            pthread_cond_wait(&pool->pool_queue_empty, &pool->pool_mutex); 
        }
        pool->pool_idle--;
        if (pool->pool_destroy) {
            break;
        }
        job = pool->pool_head;
        if (job) {
            /*Just in case*/
            func = job->job_func;
            argm = job->job_arg;
            pool->pool_head = job->next_job;
            if (pool->pool_head == NULL) {
                pool->pool_tail = NULL;
            }
            pool->active_threads[pool->pool_active_workers].is_active = true;
            pool->active_threads[pool->pool_active_workers].tid = pthread_self();
            pool->pool_active_workers++;
            pthread_mutex_unlock(&pool->pool_mutex);
            pthread_cleanup_push(job_cleanup, pool);
            FREE(job);    

            func(argm);

            pthread_cleanup_pop(1);
        }
    }
    pthread_cleanup_pop(1);
    return NULL;
}

int thr_pool_queue(thr_pool_t *pool, void *(*func) (void *), void *arg) {

    job_t *job = MALLOC(job_t, 1);
    check_mem(job);

    job->job_arg = arg;
    job->job_func = func;
    job->next_job = NULL;

    int error = 0;
    /*Add the job to the queue */
    error = pthread_mutex_lock(&pool->pool_mutex);
    check(error == 0, "");

    if (!pool->pool_head) {
        pool->pool_head = job;
    } else {
        pool->pool_tail->next_job = job;
    }
    pool->pool_tail = job;

    if (pool->pool_idle > 0) {
        error = pthread_cond_signal(&pool->pool_queue_empty);
        check(error == 0, "");
    } 
    else if (pool->pool_workers < pool->pool_nthreads && !pool->active_threads[pool->pool_active_workers].is_active) {
        pool->pool_workers++;
        pthread_t tmp;
        error = pthread_create(&tmp, NULL, worker_thread, pool);
        check(error == 0, "");
    }

    error = pthread_mutex_unlock(&pool->pool_mutex);
    check(error == 0, "");

    return 0;
    error:
        return -1;
}

void thr_pool_barrier(thr_pool_t *pool) {

    pthread_mutex_lock(&pool->pool_mutex);
    pthread_cleanup_push(pthread_mutex_unlock, &pool->pool_mutex);
    while (pool->pool_head != NULL || pool->pool_active_workers != 0) {
        pool->pool_wait = true;
        pthread_cond_wait(&pool->pool_barrier, &pool->pool_mutex);
    }
    pthread_cleanup_pop(1);
}

int thr_pool_destroy(thr_pool_t *pool) {

    int error;
    error = pthread_mutex_lock(&pool->pool_mutex);
    pthread_cleanup_push(pthread_mutex_unlock, &pool->pool_mutex);
    check(error == 0, "");
  
    pool->pool_destroy = true;
    error = pthread_cond_broadcast(&pool->pool_queue_empty);
    check(error == 0, "");

    /*Cancel all active workers */
    for (size_t i = 0 ; i < pool->pool_active_workers ; i++) {
        if (pool->active_threads[i].is_active) {
            error = pthread_cancel(pool->active_threads[i].tid);
            check(error == 0, "");
        }
    }
    
    while (pool->pool_active_workers != 0) {
        pool->pool_wait = true;
        error = pthread_cond_wait(&pool->pool_barrier, &pool->pool_mutex);
        check(error == 0, "");
    } 

    while (pool->pool_workers != 0) {
        error = pthread_cond_wait(&pool->pool_cleanup, &pool->pool_mutex);
        check(error == 0, "");
    }

    pthread_cleanup_pop(1);

    FREE(pool->active_threads);
    FREE(pool);
    
    return 0;
    error:
        return -1;
}