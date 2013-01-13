#ifndef MAP_SH_MANAGER_HH_
#define MAP_SH_MANAGER_HH_ 1

#include <google/sparse_hash_map>
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

struct Murmur {
    uint32_t operator()(const char* key) const
    {
        return HashUtil::MurmurHash(key, strlen(key), 42);
    }
};
struct eqstr {
    bool operator()(const char* s1, const char* s2) const
    {
        return (s1 && s2 && strcmp(s1, s2) == 0);
    }
};

typedef google::sparse_hash_map<const char*, PartialAgg*, Murmur, 
        eqstr> Hash;

/* @brief: A map manager using the SH as the internal data structure */
struct map_sh_manager : public map_manager {
    map_sh_manager();
    ~map_sh_manager();
    void init(Operations* ops, uint32_t ncore, uint32_t ntables);
    bool emit(void *key, void *val, size_t keylen, unsigned hash);
    void flush_buffered_paos();
    void finish_phase(int phase);
    void finalize();
  private:
    static void *worker(void *arg);
    void submit_array(uint32_t treeid, PAOArray* buf);

  private:
    const uint32_t kInsertAtOnce;

    uint32_t ntables_;
    cbt::CompressTree** cbt_;
    Hash** sh_;

    // buffer pool
    PAOArray** buffered_paos_;
    bufferpool* bufpool_;

    // threads for insertion into SHs
    pthread_t* tid_;
    pthread_mutex_t* sh_queue_mutex_;
    pthread_cond_t* sh_queue_empty_;
    std::vector<std::deque<PAOArray*>*> sh_queue_;    
};

map_sh_manager::map_sh_manager() :
        kInsertAtOnce(10000),
        buffered_paos_(NULL) {
} 

map_sh_manager::~map_sh_manager() {
    sem_destroy(&phase_semaphore_);
    // clean up buffers
    delete[] buffered_paos_;
    delete bufpool_;

    // clean up SHs
    for (uint32_t j = 0; j < ntables_; ++j) {
        delete sh_[j];
        pthread_mutex_destroy(&sh_queue_mutex_[j]);
        pthread_cond_destroy(&sh_queue_empty_[j]);
    }
    delete[] sh_;
    delete[] sh_queue_mutex_;
    delete[] sh_queue_empty_;
}

void map_sh_manager::init(Operations* ops, uint32_t ncore, uint32_t ntables) {
    ops_ = ops;
    ncore_ = ncore;
    ntables_ = ntables;

    sem_init(&phase_semaphore_, 0, ncore_);
    // create SHs
    sh_ = new Hash*[ntables_];
    sh_queue_mutex_ = new pthread_mutex_t[ntables_];
    sh_queue_empty_ = new pthread_cond_t[ntables_];

    for (uint32_t j = 0; j < ntables_; ++j) {
        sh_[j] = new Hash();
        pthread_mutex_init(&sh_queue_mutex_[j], NULL);
        pthread_cond_init(&sh_queue_empty_[j], NULL);

        std::deque<PAOArray*>* d = new std::deque<PAOArray*>();
        sh_queue_.push_back(d);
    }

    // set up workers for insertion into SHs
    tid_ = new pthread_t[ntables_];
    // sending two arguments to workers
    std::vector<args_struct*> args;

    // create a pool of buffers
    bufpool_ = new bufferpool(ops_, kInsertAtOnce, ncore_ * ntables_ * 3);
    buffered_paos_ = new PAOArray*[ncore_ * ntables_];
    for (uint32_t j = 0; j < ncore_ * ntables_; ++j) {
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
    for (uint32_t j = 0; j < ntables_; ++j) {
        args_struct* a = new args_struct(2);
        a->argv[0] = (void*)this;
        a->argv[1] = (void*)((intptr_t)j);
        args.push_back(a);
        pthread_create(&tid_[j], NULL, worker, a);
        if (j < ntables_ / 2)
            pthread_setaffinity_np(tid_[j], sizeof(cpu_set_t), &cset1);
        else
            pthread_setaffinity_np(tid_[j], sizeof(cpu_set_t), &cset2);
    }

    // results mutex
    pthread_mutex_init(&results_mutex_, NULL);
}

void map_sh_manager::submit_array(uint32_t treeid, PAOArray* buf) {
    pthread_mutex_lock(&sh_queue_mutex_[treeid]);
    sh_queue_[treeid]->push_back(buf);
    pthread_cond_signal(&sh_queue_empty_[treeid]);
    pthread_mutex_unlock(&sh_queue_mutex_[treeid]);
}

bool map_sh_manager::emit(void *k, void *v, size_t keylen, unsigned hash) {
    uint32_t treeid = hash % ntables_;
    uint32_t coreid = threadinfo::current()->cur_core_;
    uint32_t bufid = coreid * ntables_ + treeid;
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

void map_sh_manager::flush_buffered_paos() {
    uint32_t coreid = threadinfo::current()->cur_core_;
    for (uint32_t treeid = 0; treeid < ntables_; ++treeid) {
        uint32_t bufid = coreid * ntables_ + treeid;
        PAOArray* buf = buffered_paos_[bufid];
        submit_array(treeid, buf);
    }
}

void map_sh_manager::finish_phase(int phase) {
    switch (phase) {
        case MAP:
            for (uint32_t treeid = 0; treeid < ntables_; ++treeid) {
                pthread_mutex_lock(&sh_queue_mutex_[treeid]);
                pthread_cond_signal(&sh_queue_empty_[treeid]);
                pthread_mutex_unlock(&sh_queue_mutex_[treeid]);

                pthread_join(tid_[treeid], NULL);
            }
            break;
        case FINALIZE:
            break;
        default:
            assert(0);
    }
}

void* map_sh_manager::worker(void *x) {
    args_struct* a = (args_struct*)x;
    map_sh_manager* m = (map_sh_manager*)(a->argv[0]);
    uint32_t treeid = (intptr_t)(a->argv[1]);
    std::deque<PAOArray*>* q = m->sh_queue_[treeid];
    cpu_set_t cset;
    pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &cset);

    while (true) {
        pthread_mutex_lock(&m->sh_queue_mutex_[treeid]);
        pthread_cond_wait(&m->sh_queue_empty_[treeid],
                &m->sh_queue_mutex_[treeid]);
        pthread_mutex_unlock(&m->sh_queue_mutex_[treeid]);

        while (!q->empty()) {
            // remove array from queue
            pthread_mutex_lock(&m->sh_queue_mutex_[treeid]);
            PAOArray* buf = q->front();
            q->pop_front();
            pthread_mutex_unlock(&m->sh_queue_mutex_[treeid]);

            // perform insertion
            PartialAgg** arr = buf->list();
            uint32_t ind = buf->index();

            PartialAgg* new_pao = NULL;
            std::pair<Hash::iterator, bool> ret;
            for (uint32_t i = 0; i < ind; ++i) {
                if (!new_pao)
                    m->ops()->createPAO(NULL, &new_pao);

                // read the key from buffer and set the key in the new PAO.
                // This involves a key copy because the PAO in the buffer will
                // be reused
                char* key_from_buf = (char*)(m->ops()->getKey(arr[i]));
                m->ops()->setKey(new_pao, key_from_buf);

                char* key_from_new_pao = (char*)(m->ops()->getKey(new_pao));
                // try to insert the key value pair
                ret = m->sh_[treeid]->insert(
                        std::make_pair<char*, PartialAgg*>(
                        key_from_new_pao, new_pao));
                Hash::iterator ins_it = ret.first;
                if (ret.second) { // insertion was successful
                    // make a copy of the value as well
                    void* v = m->ops()->getValue(arr[i]);
                    m->ops()->setValue(new_pao, v);
                    ins_it->second = new_pao;
                    new_pao = NULL;
                } else { // already present
                    m->ops()->merge(ins_it->second, arr[i]);
                }
            }

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

void map_sh_manager::finalize() {
    uint32_t coreid = threadinfo::current()->cur_core_;
    if (coreid >= ntables_)
        return;
    uint32_t tableid;

    PAOArray* buf = buffered_paos_[coreid];
    Hash::iterator it;
    PartialAgg** arr = buf->list();
    for (tableid = coreid; tableid < ntables_; tableid += ncore_) {
        uint32_t ind = 0;
        for (it = sh_[tableid]->begin(); it != sh_[tableid]->end(); ++it) {
            arr[ind++] = it->second;
            if (ind > kInsertAtOnce) {
                // copy results
                pthread_mutex_lock(&results_mutex_);
                results_.insert(results_.end(), &buf->list()[0],
                        &buf->list()[ind - 1]);
                pthread_mutex_unlock(&results_mutex_);
                ind = 0;
            }
        }
    }
}

#endif
