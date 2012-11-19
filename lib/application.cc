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
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <inttypes.h>
#include <iostream>

#include "appbase.hh"
#include "bench.hh"
#include "cpumap.hh"
#include "thread.hh"
#include "map_cbt_manager.hh"
#include "array.hh"
#include "HashUtil.h"

//mapreduce_appbase *static_appbase::the_app_ = NULL;

namespace {
void pprint(const char *key, uint64_t v, const char *delim) {
    std::cout << key << "\t" << v << delim;
}

void cprint(const char *key, uint64_t v, const char *delim) {
    pprint(key, cycle_to_ms(v), delim);
}
}

mapreduce_appbase::mapreduce_appbase() 
    : ncore_(), ntree_(), total_sample_time_(),
      total_map_time_(), total_finalize_time_(),
      total_real_time_(), clean_(true),
      next_task_(), phase_(), m_(NULL) {
}

mapreduce_appbase::~mapreduce_appbase() {
}

void mapreduce_appbase::initialize() {
    threadinfo::initialize();
}

void mapreduce_appbase::deinitialize() {
    mthread_finalize();
}

map_cbt_manager *mapreduce_appbase::create_map_cbt_manager() {
    map_cbt_manager *m = new map_cbt_manager();
    m->init(library_name_, ncore_, ntree_);
    return m;
};

int mapreduce_appbase::map_worker() {
    int n, next;
    cpu_set_t cset;
    CPU_ZERO(&cset);
    CPU_SET(threadinfo::current()->cur_core_, &cset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cset);
    pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &cset);
    sem_wait(&m_->phase_semaphore_);
/*
    for (uint32_t i = 0; i < JOS_NCPU; ++i)
        if (CPU_ISSET(i, &cset))
            fprintf(stderr, "%d: CPU %d\n", pthread_self(), i);
*/
    for (n = 0; (next = next_task()) < int(ma_.size()); ++n) {
        map_function(ma_.at(next));
    }
    m_->flush_buffered_paos();
    sem_post(&m_->phase_semaphore_);
    return n;
}

int mapreduce_appbase::finalize_worker() {
    m_->finalize();
    return 0;
}

void *mapreduce_appbase::base_worker(void *x) {
    mapreduce_appbase *app = (mapreduce_appbase *)x;
    threadinfo *ti = threadinfo::current();
    prof_worker_start(app->phase_, ti->cur_core_);
    int n = 0;
    const char *name = NULL;
    switch (app->phase_) {
        case MAP:
            n = app->map_worker();
            name = "map";
            break;
        case FINALIZE:
            n = app->finalize_worker();
            name = "finalize";
            break;
        default:
            assert(0);
    }
    dprintf("total %d %s tasks executed in thread %ld(%d)\n",
            n, name, pthread_self(), ti->cur_core_);
    prof_worker_end(app->phase_, ti->cur_core_);
    return 0;
}

void mapreduce_appbase::run_phase(int phase, int ncore, uint64_t &t) {
    uint64_t t0 = read_tsc();
    prof_phase_stat st;
    bzero(&st, sizeof(st));
    prof_phase_init(&st);
    pthread_t tid[JOS_NCPU];
    phase_ = phase;
    for (int i = 0; i < ncore; ++i) {
        if (i == main_core)
            continue;
        mthread_create(&tid[i], i, base_worker, this);
    }
    mthread_create(&tid[main_core], main_core, base_worker, this);
    for (int i = 0; i < ncore; ++i) {
        if (i == main_core)
            continue;
        void *ret;
        mthread_join(tid[i], i, &ret);
    }
    prof_phase_end(&st);
    t += read_tsc() - t0;
}

int mapreduce_appbase::sched_run() {
    assert(threadinfo::initialized() &&
            "Call mapreduce_appbase::initialize first");
//    static_appbase::set_app(this);
    assert(clean_);
    clean_ = false;
    const int max_ncore = get_core_count();
    assert(ncore_ <= max_ncore);
    if (!ncore_)
        ncore_ = max_ncore;
    if (!ntree_)
        ntree_ = 1;

    // initialize threads
    mthread_init(ncore_);

    // pre-split
    ma_.clear();
    while (true) {
        split_t* ma = new split_t();
        if (split(ma, ncore_))
            ma_.push_back(*ma);
        else {
            delete ma;
            break;
        }
    }

    m_ = create_map_cbt_manager();

    uint64_t real_start = read_tsc();
    uint64_t map_time = 0;
    uint64_t finalize_time = 0;
    // map phase
    run_phase(MAP, ncore_, map_time);
    m_->finish_phase(MAP);
    // finalize phase
    run_phase(FINALIZE, ncore_, finalize_time);
    set_final_result();
    total_map_time_ += map_time;
    total_finalize_time_ += finalize_time;
    total_real_time_ += read_tsc() - real_start;
    return 0;
}

void mapreduce_appbase::print_stats(void) {
    prof_print(ncore_);
    uint64_t sum_time = total_sample_time_ + total_map_time_;

    std::cout << "Runtime in millisecond [" << ncore_ << " cores]\n\t";
#define SEP "\t"
    cprint("Sample:", total_sample_time_, SEP);
    cprint("Map:", total_map_time_, SEP);
    cprint("Finalize:", total_finalize_time_, SEP);
    cprint("Sum:", sum_time, SEP);
    cprint("Real:", total_real_time_, "\n");

    std::cout << "Number of Tasks of last Metis run\n\t";
	pprint("Map:", ma_.size(), "\n");
}

void mapreduce_appbase::map_emit(void *k, void *v, int keylen) {
    unsigned hash = HashUtil::MurmurHash(k, keylen, 42);
    m_->emit(k, v, keylen, hash);
}

void mapreduce_appbase::reset() {
    if (m_) {
        delete m_;
        m_ = NULL;
    }
    clean_ = true;
}

void mapreduce_appbase::print_record(FILE* f, const char* key, void* v) {
    printf("Please overload modify_function\n");
}

void mapreduce_appbase::print_results_header() {
    printf("Default results header\n");
}

void mapreduce_appbase::sort(uint32_t uleft, uint32_t uright) {
    int32_t i, j, stack_pointer = -1;
    int32_t left = uleft;
    int32_t right = uright;
    int32_t* rstack = new int32_t[128];
    PartialAgg *swap, *temp;
    PartialAgg** arr = &m_->results_[0];

    ResultComparator sorter(m_->ops(), this);

    while (true) {
        if (right - left <= 7) {
            for (j = left + 1; j <= right; j++) {
                swap = arr[j];
                i = j - 1;
                if (i < 0) {
                    fprintf(stderr, "Noo");
                    assert(false);
                }
                while (i >= left && sorter(swap, arr[i])) {
                    arr[i + 1] = arr[i];
                    i--;
                }
                arr[i + 1] = swap;
            }
            if (stack_pointer == -1) {
                break;
            }
            right = rstack[stack_pointer--];
            left = rstack[stack_pointer--];
        } else {
            int median = (left + right) >> 1;
            i = left + 1;
            j = right;

            swap = arr[median];
            arr[median] = arr[i];
            arr[i] = swap;

            if (sorter(arr[right], arr[left])) {
                swap = arr[left];
                arr[left] = arr[right];
                arr[right] = swap;
            }
            if (sorter(arr[right], arr[i])) {
                swap = arr[i];
                arr[i] = arr[right];
                arr[right] = swap;
            }
            if (sorter(arr[i], arr[left])) {
                swap = arr[left];
                arr[left] = arr[i];
                arr[i] = swap;
            }
            temp = arr[i];
            while (true) {
                while (sorter(arr[++i], temp));
                while (sorter(temp, arr[--j]));
                if (j < i) {
                    break;
                }
                swap = arr[i];
                arr[i] = arr[j];
                arr[j] = swap;
            }
            arr[left + 1] = arr[j];
            arr[j] = temp;
            if (right - i + 1 >= j - left) {
                rstack[++stack_pointer] = i;
                rstack[++stack_pointer] = right;
                right = j - 1;
            } else {
                rstack[++stack_pointer] = left;
                rstack[++stack_pointer] = j - 1;
                left = i;
            }
        }
    }
    delete[] rstack;
}

void mapreduce_appbase::print_top(size_t ndisp) {
    ndisp = std::min(ndisp, m_->results_.size());
    const Operations* ops = m_->ops();
/*
    typedef std::priority_queue<PartialAgg*, std::vector<PartialAgg*>,
            ResultComparator> heap_t;    
    ResultComparator rev_comparator(m_->ops(), this, true);
    ResultComparator comparator(m_->ops(), this);
    heap_t h(comparator);
    
    for (size_t i = 0; i < ndisp; ++i) {
        PartialAgg *p = m_->results_[i];
        h.push(p);
    }
    for (size_t i = ndisp; i < m_->results_.size(); i++) {
        PartialAgg *p = m_->results_[i];
        if (rev_comparator(const_cast<PartialAgg*&>(h.top()), p)) {
            h.pop();
            h.push(p);
        }
    }
    std::vector<PartialAgg*> top_ndisp;
    for (size_t i = 0; i < ndisp; ++i) {
        PartialAgg *p = h.top();
        top_ndisp.push_back(p);
        h.pop();
    }
    for (size_t i = 0; i < ndisp; ++i) {
        PartialAgg *p = top_ndisp[ndisp - i - 1];
        print_record(stdout, ops->getKey(p), ops->getValue(p));
    }
    top_ndisp.clear();
*/
    for (uint32_t i = 0; i < ndisp; i++) {
        PartialAgg *p = m_->results_[i];
        print_record(stdout, ops->getKey(p), ops->getValue(p));
    }
}

void mapreduce_appbase::output_all(FILE *fout) {
    const Operations* ops = m_->ops();
    for (uint32_t i = 0; i < m_->results_.size(); i++) {
        PartialAgg *p = m_->results_[i];
        print_record(fout, ops->getKey(p), ops->getValue(p));
    }
}
void mapreduce_appbase::free_results() {
    const Operations* ops = m_->ops();
    for (size_t i = 0; i < m_->results_.size(); ++i) {
        ops->destroyPAO(m_->results_[i]);
    }
    m_->results_.clear();
}

void mapreduce_appbase::set_final_result() {
    sort(0, m_->results_.size() - 1);
}

