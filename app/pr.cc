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
#include "pr.hh"

#define DEFAULT_NDISP 10

struct pr : public mapreduce_appbase {
    pr(const char *f, int nsplit) : s_(f, nsplit) {}
    bool split(split_t *ma, int ncores) {
        return s_.split(ma, ncores, " \t\r\n\0");
    }
    int key_compare(const void *s1, const void *s2) {
        return strcmp((const char *) s1, (const char *) s2);
    }
    void map_function(split_t *ma) {
        uint32_t max_record_len = 67108864;
        uint32_t max_ptrs = max_record_len >> 4;
        char* rec = (char*)malloc(max_record_len);
        char** neigh_ptrs = (char**)malloc(max_ptrs * sizeof(char*));

        size_t record_len;
        bool not_empty = true;
        do {
            split_large_record sd(ma, s_.overlap(), "\n");
            do {
                not_empty = sd.fill(rec, max_record_len, record_len);

                char *saveptr, *r;
                uint32_t neighbor_ctr = 0;                
                // add a null character terminator
                rec[record_len] = 0;

                // get the first token as the key
                char* k = strtok_r(rec, " ", &saveptr);
                uint32_t klen = strlen(k);
                char *spl;
                PageRankPAO::PRValue v;
                // tokenize the record
                do {
                    spl = strtok_r(NULL, " ", &saveptr);
                    if (spl) {
                        neigh_ptrs[neighbor_ctr++] = spl;
                    }
                } while (spl);
                // create a PAO for the followed
                v.rank = 0;
/*
                v.num_neigh = neighbor_ctr;
                v.neigh = (char*)malloc(v.num_neigh * KEYLEN);
                for (uint32_t i = 0; i < v.num_neigh; ++i)
                    strcpy(v.neigh + i * KEYLEN, neigh_ptrs[i]);
*/
                v.num_neigh = 0;
                v.neigh = NULL;
                map_emit(k, &v, klen);

                // emit PAOs for each of the followers
                float pr_given = (float)1.0 / neighbor_ctr;
                for (uint32_t i = 0; i < neighbor_ctr; ++i) {
                    v.rank = pr_given;
                    v.neigh = 0;
                    v.num_neigh = 0;
                    map_emit(neigh_ptrs[i], &v, strlen(neigh_ptrs[i]));
                }                    
            } while(not_empty);
        } while (s_.get_split_chunk(ma));
    }
    bool result_compare(const char* k1, const void* v1, 
            const char* k2, const void* v2) {
        if ((intptr_t)v1 > (intptr_t)v2)
            return true;
        return false;
    }

    void print_results_header() {
        printf("\npagerank: results\n");
    }

    void print_record(FILE* f, const char* key, void* v) {
            fprintf(f, "%15s - %d\n", key, ptr2int<unsigned>(v));
    }
  private:
    large_overlap_splitter s_;
};

static void usage(char *prog) {
    printf("usage: %s <filename> [options]\n", prog);
    printf("options:\n");
    printf("  -p #procs : # of processors to use\n");
    printf("  -m #map tasks : # of map tasks (pre-split input before MR)\n");
    printf("  -r #reduce tasks : # of reduce tasks\n");
    printf("  -l ntops : # of top val. pairs to display\n");
    printf("  -q : quiet output (for batch test)\n");
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
    pr app(fn, map_tasks);
    app.set_ncore(nprocs);
    app.set_ntrees(ntrees);
    Operations* ops = new PageRankOperations();
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
