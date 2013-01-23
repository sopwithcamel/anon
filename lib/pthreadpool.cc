/* Metis
 * Yandong Mao, Robert Morris, Frans Kaashoek
 * Copyright (c) 2012 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, subject to the conditions listed
 * in the Metis LICENSE file. These conditions include: you must preserve this
 * copyright notice, and you cannot mention the copyright holders in
 * advertising related to the Software without their permission.  The Software
 * is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This notice is a
 * summary of the Metis LICENSE file; the license in that file is legally
 * binding.
 */
#include "thread.hh"
#include "mr-types.hh"
#include "bench.hh"
#include "cpumap.hh"
#include "threadinfo.hh"
#include <assert.h>
#include <string.h>

struct  __attribute__ ((aligned(JOS_CLINE))) thread_pool_t {
    void *volatile a_;
    void *(*volatile f_) (void *);
    volatile char pending_;
    pthread_t tid_;
    volatile bool running_;

    template <typename T>
    void set_task(void *arg, T &f) {
        a_ = arg;
        f_ = f;
        mfence();
        pending_ = true;
    }

    void wait_finish() {
        while (running_)
            nop_pause();
    }

    void wait_running() {
        while (pending_)
            nop_pause();
    }
    void run_next_task() {
        while (!pending_)
            nop_pause();
        running_ = true;
        pending_ = false;
        f_(a_);
        running_ = false;
    }
};

namespace {

thread_pool_t tp_[JOS_NCPU];
bool tp_created_ = false;
int ncore_ = 0;
cpu_set_t cset1, cset2;

void *mthread_exit(void *) {
    pthread_exit(NULL);
}

void *mthread_entry(void *args) {
    threadinfo *ti = threadinfo::current();
    ti->cur_core_ = ptr2int<int>(args);
    while (true)
        tp_[ti->cur_core_].run_next_task();
}
}

void mthread_create(pthread_t * tid, int lid, void *(*start_routine) (void *),
  	            void *arg) {
    assert(tp_created_);
    if (lid == main_core)
        start_routine(arg);
    else {
        tp_[lid].wait_finish();
        tp_[lid].set_task(arg, start_routine);
        tp_[lid].wait_running();
    }
}

void mthread_join(pthread_t tid, int lid, void **retval) {
    tp_[lid].wait_finish();
    if (retval)
        *retval = 0;
}

void mthread_init(int ncore) {
    if (tp_created_)
        return;

    // eeks. specific to jedi nodes
    CPU_ZERO(&cset1);
    CPU_ZERO(&cset2);
    uint32_t i;
    for (i = 0; i < 6; ++i)
        CPU_SET(i, &cset1);
    for (i = 12; i < 18; ++i)
        CPU_SET(i, &cset1);
    for (i = 6; i < 12; ++i)
        CPU_SET(i, &cset2);
    for (i = 18; i < 24; ++i)
        CPU_SET(i, &cset2);

    threadinfo *ti = threadinfo::current();
    cpumap_init();
    ncore_ = ncore;
    ti->cur_core_ = main_core;
    assert(affinity_set(cpumap_physical_cpuid(main_core)) == 0);
    tp_created_ = true;
    bzero(tp_, sizeof(tp_));
    for (int i = 0; i < ncore_; ++i) {
        if (i == main_core)
            tp_[i].tid_ = pthread_self();
        else
            assert(pthread_create(&tp_[i].tid_, NULL, mthread_entry, int2ptr(i)) == 0);
        if (i % 2 == 0) {
            pthread_setaffinity_np(tp_[i].tid_, sizeof(cpu_set_t), &cset1);
        } else {
            pthread_setaffinity_np(tp_[i].tid_, sizeof(cpu_set_t), &cset2);
        }
    }
}

void mthread_finalize(void) {
    if (!tp_created_)
        return;
    for (int i = 0; i < ncore_; ++i)
	if (i != main_core)
	    mthread_create(NULL, i, mthread_exit, NULL);
    for (int i = 0; i < ncore_; ++i)
	if (i != main_core)
	    pthread_join(tp_[i].tid_, NULL);
    tp_created_ = false;
}
