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
#include "bench.hh"
#include "nnptr.hh"

#define DEFAULT_NDISP 10

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
    // cluster images using phash
    img_cluster ic(fn, map_tasks);
    ic.set_ncore(nprocs);
    ic.set_ntrees(ntrees);
    Operations* ic_ops = new ICPtrOperations();
    ic.set_ops(ic_ops);
    ic.set_skip_results_processing(true);
    ic.sched_run();
    fprintf(stderr, "IC produced %ld keys\n", ic.results().size());

    // run brute-force nn within clusters
    nearest_neighbor nn(ic.results(), map_tasks);
    nn.set_ncore(nprocs);
    nn.set_ntrees(ntrees);
    Operations* nn_ops = new NNPlainOperations();
    nn.set_ops(nn_ops);
    // no need to sort results
    ic.set_skip_results_processing(true);
    nn.sched_run();

    // get the number of results to display
    if (!quiet)
        nn.print_results_header();
        nn.print_top(ndisp);
    if (fout) {
        nn.output_all(fout);
        fclose(fout);
    }
    ic.free_results();
    nn.free_results();
//    ProfilerStop();
    mapreduce_appbase::deinitialize();
    return 0;
}
