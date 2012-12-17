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

#include "mr-types.hh"
#include "profile.hh"
#include "bench.hh"
#include "PartialAgg.h"

struct mapreduce_appbase;
struct map_cbt_manager;

struct static_appbase;

struct mapreduce_appbase {
    struct ResultComparator {
        explicit ResultComparator(const Operations* o, mapreduce_appbase* a,
                bool rev = false) :
                reverse(rev), ops(o), app(a) {}
        bool operator()(PartialAgg*& lhs,
                PartialAgg*& rhs) {
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

    mapreduce_appbase();
    virtual void map_function(split_t *) = 0;
    virtual bool split(split_t *ret, int ncore) = 0;
    virtual ~mapreduce_appbase();
    virtual bool result_compare(const char* k1, const void* v1, 
            const char* k2, const void* v2) = 0;

    /* @brief: set the number of cores to use. We use all cores by default. */
    void set_ncore(int ncore) {
        ncore_ = ncore;
    }
    /* @brief: set the number of trees to use. We use one tree by default. */
    void set_ntrees(int ntree) {
        ntree_ = ntree;
    }
    void set_library_name(const std::string& libname) {
        library_name_ = libname;
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

    /* internal use only */
  protected:
    // responsible for insertion into CBT
    int map_worker();
    // responsible for getting results from CBT and inserting into results
    int finalize_worker();
    // launcher function for worker threads
    static void *base_worker(void *arg);
    void run_phase(int phase, int ncore, uint64_t &t);
    map_cbt_manager* create_map_cbt_manager();

    virtual void print_record(FILE* f, const char* key, void* v);
    void set_final_result();
    void reset();

  private:
    int ncore_;   
    int ntree_;
    std::string library_name_;
    uint64_t total_sample_time_;
    uint64_t total_map_time_;
    uint64_t total_finalize_time_;
    uint64_t total_real_time_;
    bool clean_;
    
    int next_task() {
        return atomic_add32_ret(&next_task_);
    }
    int next_task_;
    int phase_;
    xarray<split_t> ma_;
    map_cbt_manager *m_;
};

#endif
