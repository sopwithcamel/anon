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
#ifndef APPBASE_HH_
#define APPBASE_HH_ 1

#include <dlfcn.h>
#include <tbb/blocked_range.h>
#include <semaphore.h>

#include "mr-types.hh"
#include "profile.hh"
#include "bench.hh"
#include "PartialAgg.h"

struct mapreduce_appbase;
struct map_cbt_manager;

struct static_appbase;

struct map_manager {
    map_manager() : ops_(NULL) {}

    ~map_manager() {
        sem_destroy(&phase_semaphore_);
    }
    
    const Operations* ops() const {
        assert(ops_);
        return ops_;
    }
    virtual bool emit(void *key, void *val, size_t keylen, unsigned hash) = 0;
    virtual void flush_buffered_paos() {}
    virtual void finish_phase(int phase) {}
    virtual void finalize() {}

  protected:
    bool link_user_map(const std::string& soname) {
        const char* err;
        void* handle;
        handle = dlopen(soname.c_str(), RTLD_NOW);
        if (!handle) {
            fputs(dlerror(), stderr);
            return false;
        }

        Operations* (*create_ops_obj)() = (Operations* (*)())dlsym(handle,
                "__libminni_create_ops");
        if ((err = dlerror()) != NULL) {
            fprintf(stderr, "Error locating symbol __libminni_create_ops\
                    in %s\n", err);
            exit(-1);
        }
        ops_ = create_ops_obj();
        return true;
    }

  public:
    sem_t phase_semaphore_;
    // results
    std::vector<PartialAgg*> results_;
    pthread_mutex_t results_mutex_;

  protected:
    uint32_t ncore_;
    Operations* ops_;
};

struct mapreduce_appbase {
    struct ResultComparator {
        explicit ResultComparator(const Operations* o, mapreduce_appbase* a,
                bool rev = false) :
                reverse(rev), ops(o), app(a) {}
        bool operator()(PartialAgg* const& lhs,
                PartialAgg* const& rhs) const {
            bool ret =  app->result_compare(ops->getKey(lhs),
                    ops->getValue(lhs),
                    ops->getKey(rhs), ops->getValue(rhs));
            if (reverse)
                return !ret;
            return ret;
        }
        bool reverse;
        const Operations* ops;
        mapreduce_appbase* app;
    };

    struct free_paos {
        std::vector<PartialAgg*>& my_results;
        const Operations* ops;
        void operator()(const tbb::blocked_range<size_t>& r) const {
            for(size_t i = r.begin(); i != r.end(); ++i)
                ops->destroyPAO(my_results[i]);
        }
        free_paos(std::vector<PartialAgg*>& results,
                const Operations* o) :
            my_results(results), ops(o) {}
    };

    mapreduce_appbase();
    virtual void map_function(split_t *) = 0;
    virtual bool split(split_t *ret, int ncore) = 0;
    virtual ~mapreduce_appbase();
    virtual bool result_compare(const char* k1, const void* v1, 
            const char* k2, const void* v2) = 0;

    /* @brief: default partition function that partition keys into reduce/group
     * buckets */
    virtual unsigned partition(void *k, int length) {
        size_t h = 5381;
        const char *x = (const char *) k;
        for (int i = 0; i < length; ++i)
	    h = ((h << 5) + h) + unsigned(x[i]);
        return h % unsigned(-1);
    } 
    /* @brief: set the number of cores to use. We use all cores by default. */
    void set_ncore(int ncore) {
        ncore_ = ncore;
    }
    /* @brief: set the number of trees to use. We use one tree by default. */
    void set_ntrees(int ntree) {
        ntree_ = ntree;
    }
    void set_ops(Operations* ops) {
        ops_ = ops;
    }
    static void initialize();
    static void deinitialize();
    int sched_run();
    void print_stats();
    virtual void print_results_header();
    virtual void print_top(size_t ndisp);
    virtual void output_all(FILE *fout);
    void free_results();
    /* @brief: called in user defined map function. If keycopy function is
        used, Metis calls the keycopy function for each new key, and user
        can free the key when this function returns. */
    void map_emit(void *key, void *val, int key_length);
    void sort(uint32_t uleft, uint32_t uright);

    void set_skip_results_processing(bool val) {
        skip_results_processing_ = val;
    }

    /* internal use only */
  protected:
    // responsible for insertion into CBT
    int map_worker();
    // responsible for getting results from CBT and inserting into results
    int finalize_worker();
    // launcher function for worker threads
    static void *base_worker(void *arg);
    void run_phase(int phase, int ncore, uint64_t &t);
    map_manager* create_map_manager();
    virtual void print_record(FILE* f, const char* key, void* v);
    void set_final_result();
    void reset();

  private:
    int ncore_;   
    int ntree_;
    Operations* ops_;
    uint64_t total_sample_time_;
    uint64_t total_map_time_;
    uint64_t total_finalize_time_;
    uint64_t total_real_time_;
    bool clean_;

    bool skip_results_processing_;
    
    int next_task() {
        return atomic_add32_ret(&next_task_);
    }
    int next_task_;
    int phase_;
    xarray<split_t> ma_;
    map_manager *m_;
};

#endif
