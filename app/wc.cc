/* Copyright (c) 2007, Stanford University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of Stanford University nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY STANFORD UNIVERSITY ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL STANFORD UNIVERSITY BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <gperftools/profiler.h>
#include <gperftools/heap-profiler.h>
#include <assert.h>
#include <fcntl.h>
#include <ctype.h>
#include <time.h>
#include <strings.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sched.h>
#include "appbase.hh"
#include "defsplitter.hh"
#include "tokenizers.hh"
#include "bench.hh"
#include "wc.hh"
#include "wc_boost.h"

#define DEFAULT_NDISP 10

/* Hadoop print all the key/value paris at the end.  This option 
 * enables wordcount to print all pairs for fair comparison. */
//#define HADOOP

enum { with_value_modifier = 1 };

struct wc : public mapreduce_appbase {
    wc(const char *f, int nsplit) : s_(f, nsplit) {}
    bool split(split_t *ma, int ncores) {
        return s_.split(ma, ncores, " \t\r\n\0");
    }
    int key_compare(const void *s1, const void *s2) {
        return strcmp((const char *) s1, (const char *) s2);
    }
    void map_function(split_t *ma) {
        char k[1024];
        size_t klen;
        do {
            split_word sw(ma);
            while (sw.fill(k, 1024, klen)) {
                k[klen] = '\0';
                map_emit(k, (void *)(intptr_t)1, klen);
                memset(k, 0, klen);
            }
        } while (s_.get_split_chunk(ma));
    }
    bool result_compare(const char* k1, const void* v1, 
            const char* k2, const void* v2) {
        if ((intptr_t)v1 > (intptr_t)v2)
            return true;
        return false;
    }

    void print_results_header() {
        printf("\nwordcount: results\n");
    }

    void print_record(FILE* f, const char* key, void* v) {
            fprintf(f, "%15s - %d\n", key, ptr2int<unsigned>(v));
    }
  private:
    defsplitter s_;
};

static void usage(char *prog) {
    printf("usage: %s <filename> [options]\n", prog);
    printf("options:\n");
    printf("  -p #procs : # of processors to use\n");
    printf("  -m #map tasks : # of map tasks (pre-split input before MR)\n");
    printf("  -r #reduce tasks : # of reduce tasks\n");
    printf("  -l ntops : # of top val. pairs to display\n");
    printf("  -q : quiet output (for batch test)\n");
    printf("  -x : use PAOs with pointers\n");
    printf("  -o filename : save output to a file\n");
    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
    int nprocs = 0, map_tasks = 0, ndisp = 5, ntrees = 0;
    int pointer_mode = 0;
    int quiet = 0;
    int c;
    if (argc < 2)
        usage(argv[0]);
    char *fn = argv[1];
    FILE *fout = NULL;

    while ((c = getopt(argc - 1, argv + 1, "p:t:s:l:m:r:qxo:")) != -1) {
        switch (c) {
            case 'p':
                nprocs = atoi(optarg);
                break;
            case 't':
                ntrees = atoi(optarg);
                break;
            case 'l':
                ndisp = atoi(optarg);
                break;
            case 'm':
                map_tasks = atoi(optarg);
                break;
            case 'q':
                quiet = 1;
                break;
            case 'x':
                pointer_mode = 1;
                break;
            case 'o':
                fout = fopen(optarg, "w+");
                if (!fout) {
                    fprintf(stderr, "unable to open %s: %s\n", optarg,
                            strerror(errno));
                    exit(EXIT_FAILURE);
                }
                break;
            default:
                usage(argv[0]);
                exit(EXIT_FAILURE);
                break;
        }
    }
    mapreduce_appbase::initialize();
    /* get input file */
    wc app(fn, map_tasks);
    app.set_ncore(nprocs);
    app.set_ntrees(ntrees);
    Operations* ops;
    if (pointer_mode)
        ops = new WCBoostOperations();
    else
        ops = new WCPlainOperations();
    app.set_ops(ops);
    app.set_results_out(fout);

//    ProfilerStart("/tmp/anon.perf");
    app.sched_run();
//    app.print_stats();
    /* get the number of results to display */
    if (!quiet)
        app.print_results_header();
        app.print_top(ndisp);
    if (fout) {
        app.output_all(fout);
        fclose(fout);
    }
    app.free_results();
//    ProfilerStop();
    mapreduce_appbase::deinitialize();
    return 0;
}
