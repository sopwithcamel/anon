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
#include "overlap_splitter.hh"
#include "tokenizers.hh"
#include "bench.hh"
#include "wc.hh"
#include "wc_boost.h"

#define DEFAULT_NDISP 10

struct maxlen : public mapreduce_appbase {
    maxlen(const char *f, int nsplit) : s_(f, nsplit) {}
    bool split(split_t *ma, int ncores) {
        return s_.split(ma, ncores, " \t\r\n\0");
    }
    int key_compare(const void *s1, const void *s2) {
        return strcmp((const char *) s1, (const char *) s2);
    }
    void map_function(split_t *ma) {
        char k[1024];
        char key[1024];
        size_t klen;
        do {
            split_digram sd(ma);
            while (sd.fill(k, 1024, klen)) {
                if (klen <= 64) {
                    sprintf(key, "%lu", strlen(k));
                    map_emit(key, (void *)1, klen);
                }
                memset(k, 0, klen);
                memset(key, 0, 4);
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
    overlap_splitter s_;
};

static void usage(char *prog) {
    printf("usage: %s <filename> [options]\n", prog);
    printf("options:\n");
    printf("  -p #procs : # of processors to use\n");
    printf("  -m #map tasks : # of map tasks (pre-split input before MR)\n");
    printf("  -r #reduce tasks : # of reduce tasks\n");
    printf("  -l ntops : # of top val. pairs to display\n");
    printf("  -q : quiet output (for batch test)\n");
    printf("  -x : pointer mode for digram count\n");
    printf("  -o filename : save output to a file\n");
    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
    int nprocs = 0, map_tasks = 0, ndisp = 50, ntrees = 0;
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
    maxlen app(fn, map_tasks);
    app.set_ncore(nprocs);
    app.set_ntrees(ntrees);
    Operations* ops;
    if (pointer_mode)
        ops = new WCBoostOperations();
    else
        ops = new WCPlainOperations();
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
