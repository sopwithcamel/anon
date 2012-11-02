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
struct map_cbt_manager_base;

struct static_appbase;

struct mapreduce_appbase {
    mapreduce_appbase();
    virtual void map_function(split_t *) = 0;
    virtual bool split(split_t *ret, int ncore) = 0;
    virtual ~mapreduce_appbase();

    /* @brief: default partition function that partition keys into reduce/group
     * buckets */
    virtual unsigned partition(void *k, int length) {
        size_t h = 5381;
        const char *x = (const char *) k;
        for (int i = 0; i < length; ++i)
	    h = ((h << 5) + h) + unsigned(x[i]);
        return h % unsigned(-1);
    } 
    /* @brief: set the number of cores to use. Metis uses all cores by default. */
    void set_ncore(int ncore) {
        ncore_ = ncore;
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

    /* internal use only */
  protected:
    // responsible for insertion into CBT
    int map_worker();
    // responsible for getting results from CBT and inserting into results
    int finalize_worker();
    // launcher function for worker threads
    static void *base_worker(void *arg);
    void run_phase(int phase, int ncore, uint64_t &t);
    map_cbt_manager_base* create_map_cbt_manager();

    virtual void print_record(FILE* f, const char* key, void* v);
    void set_final_result();
    void reset();

  private:
    int ncore_;   
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
    map_cbt_manager_base *m_;
    xarray<PartialAgg*> results_;
};

#endif
