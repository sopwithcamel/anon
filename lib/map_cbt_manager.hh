#ifndef MAP_CBT_MANAGER_HH_
#define MAP_CBT_MANAGER_HH_ 1

#include <inttypes.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <vector>
#include <deque>

#include "array.hh"
#include "test_util.hh"
#include "appbase.hh"
#include "bufferpool.hh"
#include "threadinfo.hh"
#include "CompressTree.h"
#include "HashUtil.h"
#include "PartialAgg.h"

using namespace google::protobuf::io;

struct args_struct;

/* @brief: A map manager using the CBT as the internal data structure */
struct map_cbt_manager : public map_manager {
    map_cbt_manager();
    ~map_cbt_manager();
    void init(Operations* ops, uint32_t ncore, uint32_t ntree);
    bool emit(void *key, void *val, size_t keylen, unsigned hash);
    void flush_buffered_paos();
    void finish_phase(int phase);
    void finalize();
  private:
    static void *worker(void *arg);
    static void *random_input_worker(void *arg);
    void submit_array(uint32_t treeid, PAOArray* buf);

  private:
    const uint32_t kInsertAtOnce;

    uint32_t ntree_;
    cbt::CompressTree** cbt_;

    // buffer pool
    PAOArray** buffered_paos_;
    bufferpool* bufpool_;

    // threads for insertion into CBTs
    pthread_t* tid_;
    pthread_mutex_t* cbt_queue_mutex_;
    pthread_cond_t* cbt_queue_empty_;
    std::vector<std::deque<PAOArray*>*> cbt_queue_;    
};

map_cbt_manager::map_cbt_manager() :
        kInsertAtOnce(100000),
        buffered_paos_(NULL) {
} 

map_cbt_manager::~map_cbt_manager() {
    sem_destroy(&phase_semaphore_);
    // clean up buffers
    delete[] buffered_paos_;
    delete bufpool_;

    // clean up CBTs
    for (uint32_t j = 0; j < ntree_; ++j) {
        delete cbt_[j];
        pthread_mutex_destroy(&cbt_queue_mutex_[j]);
        pthread_cond_destroy(&cbt_queue_empty_[j]);
    }
    delete[] cbt_;
    delete[] cbt_queue_mutex_;
    delete[] cbt_queue_empty_;
}

void map_cbt_manager::init(Operations* ops, uint32_t ncore, uint32_t ntree) {
    ops_ = ops;
    ncore_ = ncore;
    ntree_ = ntree;

    sem_init(&phase_semaphore_, 0, ncore_);
    // create CBTs
    cbt_ = new cbt::CompressTree*[ntree_];
    cbt_queue_mutex_ = new pthread_mutex_t[ntree_];
    cbt_queue_empty_ = new pthread_cond_t[ntree_];

    uint32_t fanout = 8;
    uint32_t buffer_size = 125829120;// 31457280;
    uint32_t pao_size = 64;
    for (uint32_t j = 0; j < ntree_; ++j) {
        cbt_[j] = new cbt::CompressTree(2, fanout, 1000, buffer_size,
                pao_size, ops_);
        pthread_mutex_init(&cbt_queue_mutex_[j], NULL);
        pthread_cond_init(&cbt_queue_empty_[j], NULL);

        std::deque<PAOArray*>* d = new std::deque<PAOArray*>();
        cbt_queue_.push_back(d);
    }

    // set up workers for insertion into CBTs
    tid_ = new pthread_t[ntree_];
    // sending two arguments to workers
    std::vector<args_struct*> args;

    // create a pool of buffers
    bufpool_ = new bufferpool(ops_, kInsertAtOnce, ncore_ * ntree_ * 3);
    buffered_paos_ = new PAOArray*[ncore_ * ntree_];
    for (uint32_t j = 0; j < ncore_ * ntree_; ++j) {
        buffered_paos_[j] = bufpool_->get_buffer();
    }

    // set all cpus in cpu mask
    cpu_set_t cset1, cset2;
    CPU_ZERO(&cset1);
    CPU_ZERO(&cset2);
    for (uint32_t i = 4; i < 6; ++i)
        CPU_SET(i, &cset1);
    for (uint32_t i = 12; i < 18; ++i)
        CPU_SET(i, &cset1);
    for (uint32_t i = 6; i < 12; ++i)
        CPU_SET(i, &cset2);
    for (uint32_t i = 18; i < 24; ++i)
        CPU_SET(i, &cset2);
    for (uint32_t j = 0; j < ntree_; ++j) {
        args_struct* a = new args_struct(2);
        a->argv[0] = (void*)this;
        a->argv[1] = (void*)((intptr_t)j);
        args.push_back(a);
        pthread_create(&tid_[j], NULL, worker, a);
        if (j < ntree_ / 2)
            pthread_setaffinity_np(tid_[j], sizeof(cpu_set_t), &cset1);
        else
            pthread_setaffinity_np(tid_[j], sizeof(cpu_set_t), &cset2);
    }

    // results mutex
    pthread_mutex_init(&results_mutex_, NULL);
}

void map_cbt_manager::submit_array(uint32_t treeid, PAOArray* buf) {
    pthread_mutex_lock(&cbt_queue_mutex_[treeid]);
    cbt_queue_[treeid]->push_back(buf);
    pthread_cond_signal(&cbt_queue_empty_[treeid]);
    pthread_mutex_unlock(&cbt_queue_mutex_[treeid]);
}

bool map_cbt_manager::emit(void *k, void *v, size_t keylen, unsigned hash) {
    uint32_t treeid = hash % ntree_;
    uint32_t coreid = threadinfo::current()->cur_core_;
    uint32_t bufid = coreid * ntree_ + treeid;
    PAOArray* buf = buffered_paos_[bufid];
    uint32_t ind = buf->index();
    ops()->setKey(buf->list()[ind], (char*)k);
    ops()->setValue(buf->list()[ind], v);
    buf->set_index(ind + 1);

    if (buf->index() == kInsertAtOnce) {
        submit_array(treeid, buf);
        // get new buffer from pool
        buffered_paos_[bufid] = bufpool_->get_buffer();
    }
//    fprintf(stderr, "[%ld], inserted at %d\n", pthread_self(), ind);
    return true;
}

void map_cbt_manager::flush_buffered_paos() {
    uint32_t coreid = threadinfo::current()->cur_core_;
    for (uint32_t treeid = 0; treeid < ntree_; ++treeid) {
        uint32_t bufid = coreid * ntree_ + treeid;
        PAOArray* buf = buffered_paos_[bufid];
        submit_array(treeid, buf);
    }
}

void map_cbt_manager::finish_phase(int phase) {
    switch (phase) {
        case MAP:
            for (uint32_t treeid = 0; treeid < ntree_; ++treeid) {
                pthread_join(tid_[treeid], NULL);
            }
            break;
        case FINALIZE:
            break;
        default:
            assert(0);
    }
}

void* map_cbt_manager::worker(void *x) {
    args_struct* a = (args_struct*)x;
    map_cbt_manager* m = (map_cbt_manager*)(a->argv[0]);
    uint32_t treeid = (intptr_t)(a->argv[1]);
    std::deque<PAOArray*>* q = m->cbt_queue_[treeid];
    cpu_set_t cset;
    pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &cset);

    while (q->empty()) {
        pthread_mutex_lock(&m->cbt_queue_mutex_[treeid]);
        pthread_cond_wait(&m->cbt_queue_empty_[treeid],
                &m->cbt_queue_mutex_[treeid]);
        pthread_mutex_unlock(&m->cbt_queue_mutex_[treeid]);

        while (!q->empty()) {
            // remove array from queue
            pthread_mutex_lock(&m->cbt_queue_mutex_[treeid]);
            PAOArray* buf = q->front();
            q->pop_front();
            pthread_mutex_unlock(&m->cbt_queue_mutex_[treeid]);

            // perform insertion
            m->cbt_[treeid]->bulk_insert(buf->list(), buf->index());

            // return buffer to pool
            m->bufpool_->return_buffer(buf);
        }

        int ret;
        sem_getvalue(&m->phase_semaphore_, &ret);
        if (ret == (int)m->ncore_)
            break;
    }
    return 0;
}

void map_cbt_manager::finalize() {
    uint32_t coreid = threadinfo::current()->cur_core_;
    if (coreid >= ntree_)
        return;
    uint32_t treeid = coreid;

    PAOArray* buf = buffered_paos_[coreid];
    uint64_t num_read;
    bool remain;
    do {
        remain = cbt_[treeid]->bulk_read(buf->list(), num_read,
                kInsertAtOnce);
        // copy results
        pthread_mutex_lock(&results_mutex_);
        results_.insert(results_.end(), &buf->list()[0],
                &buf->list()[num_read]);
        pthread_mutex_unlock(&results_mutex_);
    } while (remain);
}

#endif
