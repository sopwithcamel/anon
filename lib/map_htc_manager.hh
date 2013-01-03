#ifndef MAP_HTC_MANAGER_HH_
#define MAP_HTC_MANAGER_HH_ 1

#include <inttypes.h>
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

struct args_struct;

struct HashCompare {
    static size_t hash(const char* key)
    {
        size_t a = HashUtil::BobHash(key, strlen(key), 42);
        return a;
    }
    //! True if strings are equal
    static bool equal(const char* s1, const char* s2) {
        return (s1 && s2 && strcmp(s1, s2) == 0);
    }
};

typedef tbb::concurrent_hash_map<const char*, PartialAgg*,
        HashCompare> Hashtable;

struct Aggregate {
    Hashtable* ht;
    bool destroyMerged_;
    const Operations* const ops;

    Aggregate(Hashtable* ht_, bool destroy,
            const Operations* const ops) :
        ht(ht_),
        destroyMerged_(destroy),
        ops(ops) {}
    void operator()(const tbb::blocked_range<PartialAgg**> r) const
    {
        PartialAgg* new_pao = NULL;
        for (PartialAgg** it=r.begin(); it != r.end(); ++it) {
            Hashtable::accessor a;
            if (!new_pao)
                ops->createPAO(NULL, &new_pao);
            ops->setKey(new_pao, (char*)ops->getKey(*it));
            char* k = (char*)(ops->getKey(new_pao));
            if (ht->insert(a, k)) { // wasn't present
                void* v = ops->getValue(*it);
                ops->setValue(new_pao, v);
                a->second = new_pao;
                new_pao = NULL;
            } else { // already present
                ops->merge(a->second, *it);
            }
        }
        if (new_pao) {
            ops->destroyPAO(new_pao);
            new_pao = NULL;
        }
    }
};

/* @brief: A map manager using the CBT as the internal data structure */
struct map_htc_manager : public map_manager {
    map_htc_manager();
    ~map_htc_manager();
    void init(Operations* ops, uint32_t ncore);
    bool emit(void *key, void *val, size_t keylen, unsigned hash);
    void flush_buffered_paos();
    void finish_phase(int phase);
    void finalize();
  private:
    static void *worker(void *arg);
    static void *random_input_worker(void *arg);
    void submit_array(PAOArray* buf);
  private:
    const uint32_t kInsertAtOnce;
    typedef tbb::concurrent_hash_map<const char*, PartialAgg*,
            HashCompare> Hashtable;
    Hashtable* htc_;

    // buffer pool
    PAOArray** buffered_paos_;
    bufferpool* bufpool_;

    // thread for insertion into HTC
    pthread_t tid_;
    pthread_mutex_t htc_queue_mutex_;
    pthread_cond_t htc_queue_empty_;
    std::deque<PAOArray*> htc_queue_;    

    // random input generation
    uint32_t num_unique_keys_;
    uint32_t key_length_;
    uint32_t num_fillers_;
    uint32_t num_full_loops_;
    uint32_t part_loop_;
    std::vector<char*> fillers_;
};

map_htc_manager::map_htc_manager() :
        kInsertAtOnce(100000),
        buffered_paos_(NULL) {
} 

map_htc_manager::~map_htc_manager() {
    // clean up buffers
    delete[] buffered_paos_;
    delete bufpool_;

    // clean up CBTs
    delete htc_;
    pthread_mutex_destroy(&htc_queue_mutex_);
    pthread_cond_destroy(&htc_queue_empty_);
}

void map_htc_manager::init(Operations* ops, uint32_t ncore) {
    ops_ = ops;
    ncore_ = ncore;

    sem_init(&phase_semaphore_, 0, ncore_);
    // create CBTs
    htc_ = new Hashtable();

    pthread_mutex_init(&htc_queue_mutex_, NULL);
    pthread_cond_init(&htc_queue_empty_, NULL);

    // sending two arguments to workers
    std::vector<args_struct*> args;

    // create a pool of buffers
    bufpool_ = new bufferpool(ops_, kInsertAtOnce, ncore_ * 3);
    buffered_paos_ = new PAOArray*[ncore_];
    for (uint32_t j = 0; j < ncore_ ; ++j) {
        buffered_paos_[j] = bufpool_->get_buffer();
    }

    // set all cpus in cpu mask
    cpu_set_t cset;
    CPU_ZERO(&cset);
    for (uint32_t i = 0; i < JOS_NCPU; ++i)
        CPU_SET(i, &cset);

    args_struct* a = new args_struct(2);
    a->argv[0] = (void*)this;
    args.push_back(a);
    pthread_create(&tid_, NULL, worker, a);
    pthread_setaffinity_np(tid_, sizeof(cpu_set_t), &cset);

    // results mutex
    pthread_mutex_init(&results_mutex_, NULL);
}

void map_htc_manager::submit_array(PAOArray* buf) {
    pthread_mutex_lock(&htc_queue_mutex_);
    htc_queue_.push_back(buf);
    pthread_cond_signal(&htc_queue_empty_);
    pthread_mutex_unlock(&htc_queue_mutex_);
}

bool map_htc_manager::emit(void *k, void *v, size_t keylen, unsigned hash) {
    uint32_t coreid = threadinfo::current()->cur_core_;
    uint32_t bufid = coreid;
    PAOArray* buf = buffered_paos_[bufid];
    uint32_t ind = buf->index();
    ops()->setKey(buf->list()[ind], (char*)k);
    ops()->setValue(buf->list()[ind], v);
    buf->set_index(ind + 1);

    if (buf->index() == kInsertAtOnce) {
        submit_array(buf);
        // get new buffer from pool
        buffered_paos_[bufid] = bufpool_->get_buffer();
    }
//    fprintf(stderr, "[%ld], inserted at %d\n", pthread_self(), ind);
    return true;
}

void map_htc_manager::flush_buffered_paos() {
    uint32_t coreid = threadinfo::current()->cur_core_;
    uint32_t bufid = coreid;
    PAOArray* buf = buffered_paos_[bufid];
    submit_array(buf);
}

void map_htc_manager::finish_phase(int phase) {
    switch (phase) {
        case MAP:
            pthread_join(tid_, NULL);
            break;
        case FINALIZE:
            break;
        default:
            assert(0);
    }
}

void* map_htc_manager::worker(void *x) {
    args_struct* a = (args_struct*)x;
    map_htc_manager* m = (map_htc_manager*)(a->argv[0]);
    std::deque<PAOArray*>& q = m->htc_queue_;
    cpu_set_t cset;
    pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &cset);
    tbb::task_scheduler_init init(tbb::task_scheduler_init::automatic);

    while (true) {
        pthread_mutex_lock(&m->htc_queue_mutex_);
        while (q.empty())
            pthread_cond_wait(&m->htc_queue_empty_,
                    &m->htc_queue_mutex_);
        pthread_mutex_unlock(&m->htc_queue_mutex_);

        while (!q.empty()) {
            // remove array from queue
            pthread_mutex_lock(&m->htc_queue_mutex_);
            PAOArray* buf = q.front();
            q.pop_front();
            pthread_mutex_unlock(&m->htc_queue_mutex_);

            // perform insertion
            uint32_t recv_length = buf->index();
            
            tbb::parallel_for(tbb::blocked_range<PartialAgg**>(buf->list(),
                    buf->list() + recv_length, 100),
                    Aggregate(m->htc_, /*destroy_pao = */false,
                    m->ops()));

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

void map_htc_manager::finalize() {
    uint32_t coreid = threadinfo::current()->cur_core_;
    Hashtable::iterator my_work = htc_->begin();
    std::vector<PartialAgg*> temp;
    for (uint32_t i = 0; i < coreid; ++i)
        my_work++;

    for (Hashtable::iterator it = my_work; ;) {
        temp.push_back(it->second);
        for (uint32_t j = 0; j < ncore_; ++j) {
            ++it;
            if (it == htc_->end())
                goto exit_loop;
        }
    }
exit_loop:
    pthread_mutex_lock(&results_mutex_);
    results_.insert(results_.end(), temp.begin(),
            temp.end());
    pthread_mutex_unlock(&results_mutex_);
    temp.clear();
}

#endif
