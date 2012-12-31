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
#include "bench.hh"
#include "wc.hh"

#define DEFAULT_NDISP 10

/* Hadoop print all the key/value paris at the end.  This option 
 * enables wordcount to print all pairs for fair comparison. */
//#define HADOOP

enum { with_value_modifier = 1 };

static int alphanumeric;

struct split_digram {
    split_digram(split_t *ma) :
            ma_(ma), len_(0),
            bytewise_(false),
            last_(NULL), last_len_(0) {
        assert(ma_ && ma_->data);
        str_ = ma_->data;
    }

    bool fill(char *k, size_t maxlen, size_t &klen) {
        char* spl;
        size_t chunk_length = ma_->chunk_end_offset -
            ma_->chunk_start_offset;

        if (bytewise_) {
            char *d = ma_->data;
            klen = 0;
            // last_ should not be NULL since this branch oughtn't be entered
            // first
            assert(last_);

            for (; len_ < chunk_length && !isalnum(d[len_]); ++len_);
            if (len_ == chunk_length) {
                return false;
            }

            // First copy the last string
            strncpy(k, last_, last_len_);
            klen = last_len_;
            k[klen++] = '-';
            k[klen] = 0;

            last_ = &d[len_];

            for (; len_ < chunk_length && isalnum(d[len_]); ++len_) {
                k[klen++] = d[len_];
            }
            k[klen] = 0;
            last_len_ = klen - last_len_ - 1;

            return true;
        }

        const char* stop = " .-=_\t\n\r\'\"?,;`:!*()-\0\uFEFF";

        if (last_ == NULL) {
            // split token
            spl = strtok_r(str_, stop, &saveptr1);
            if (spl == NULL)
                return false;
            last_ = spl;
            last_len_ = strlen(last_);
            str_ = NULL;
        }
        strncpy(k, last_, last_len_);
        klen = last_len_;

        k[klen++] = '-';
        k[klen] = '\0';

        // split for second token
        spl = strtok_r(NULL, stop, &saveptr1);
        int l = strlen(spl);
        strncat(k, spl, l);
        klen += l;

        // update last_ and last_len_ for later
        last_ = spl;
        last_len_ = l;

        if (spl + maxlen > ma_->data + chunk_length) {
            bytewise_ = true;
            len_ = (spl - ma_->data) + klen;
        }
        return true;
    }

  private:
    split_t* ma_;
    char* str_;
    char* saveptr1;
    size_t len_;
    bool bytewise_;
    char* last_;
    size_t last_len_;
};

struct dg : public mapreduce_appbase {
    dg(const char *f, int nsplit) : s_(f, nsplit) {}
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
            split_digram sd(ma);
            while (sd.fill(k, 1024, klen)) {
                map_emit(k, (void *)1, klen);
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
    printf("  -a : alphanumeric word count\n");
    printf("  -o filename : save output to a file\n");
    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
    int nprocs = 0, map_tasks = 0, ndisp = 5, ntrees = 0;
    int quiet = 0;
    int c;
    if (argc < 2)
        usage(argv[0]);
    char *fn = argv[1];
    FILE *fout = NULL;

    while ((c = getopt(argc - 1, argv + 1, "p:t:s:l:m:r:qao:")) != -1) {
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
            case 'a':
                alphanumeric = 1;
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
    dg app(fn, map_tasks);
    app.set_ncore(nprocs);
    app.set_ntrees(ntrees);
    Operations* ops = new WCPlainOperations();
    app.set_ops(ops);

//    ProfilerStart("/tmp/anon.perf");
    app.sched_run();
    app.print_stats();
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
