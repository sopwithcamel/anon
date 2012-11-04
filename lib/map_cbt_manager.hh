#ifndef MAP_CBT_MANAGER_HH_
#define MAP_CBT_MANAGER_HH_ 1

#include <dlfcn.h>
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
#include "PartialAgg.h"

using namespace google::protobuf::io;

struct map_cbt_manager_base {
    virtual ~map_cbt_manager_base() {}
    virtual void init(const std::string& libname, uint32_t ncore) = 0;
    virtual bool emit(void *key, void *val, size_t keylen, unsigned hash) = 0;
    virtual void finalize() = 0;
    virtual const Operations* ops() const = 0;
};

struct args_struct {
  public:
    explicit args_struct(uint32_t c) {
        argc = c;
        argv = new void*[c];
    }
    ~args_struct() {
        delete[] argv;
    }
    void** argv;
    uint32_t argc;
};

/* @brief: A map manager using the CBT as the internal data structure */
struct map_cbt_manager : public map_cbt_manager_base {
    map_cbt_manager();
    ~map_cbt_manager();
    void init(const std::string& libname, uint32_t ncore);
    bool emit(void *key, void *val, size_t keylen, unsigned hash);
    void finalize();
    const Operations* ops() const {
        assert(ops_);
        return ops_;
    }        
  private:
    bool link_user_map(const std::string& libname);
    static void *worker(void *arg);

  private:
    const uint32_t kInsertAtOnce;

    uint32_t ntrees_;
    uint32_t ncore_;
    cbt::CompressTree** cbt_;
    Operations* ops_;

    // buffer pool
    PAOArray** buffered_paos_;
    bufferpool* bufpool_;

    // threads for insertion into CBTs
    pthread_t* tid_;
    pthread_mutex_t* cbt_worker_list_lock_;
    pthread_cond_t* cbt_queue_empty_;
    std::vector<std::deque<PAOArray*>*> cbt_queue_;    
};

map_cbt_manager::map_cbt_manager() :
        kInsertAtOnce(100000), ops_(NULL),
        buffered_paos_(NULL) {
} 

map_cbt_manager::~map_cbt_manager() {
    // clean up buffers
    delete[] buffered_paos_;
    delete bufpool_;

    // clean up CBTs
    for (uint32_t j = 0; j < ntrees_; ++j) {
        delete cbt_[j];
        pthread_mutex_destroy(&cbt_worker_list_lock_[j]);
        pthread_cond_destroy(&cbt_queue_empty_[j]);
    }
    delete[] cbt_;
    delete[] cbt_worker_list_lock_;
    delete[] cbt_queue_empty_;
}

void map_cbt_manager::init(const std::string& libname, uint32_t ncore) {
    assert(link_user_map(libname));
    ncore_ = ncore;
    ntrees_ = 1;

    // create CBTs
    cbt_ = new cbt::CompressTree*[ntrees_];
    cbt_worker_list_lock_ = new pthread_mutex_t[ntrees_];
    cbt_queue_empty_ = new pthread_cond_t[ntrees_];

    uint32_t fanout = 8;
    uint32_t buffer_size = 31457280;
    uint32_t pao_size = 20;
    for (uint32_t j = 0; j < ntrees_; ++j) {
        cbt_[j] = new cbt::CompressTree(2, fanout, 1000, buffer_size,
                pao_size, ops_);
        pthread_mutex_init(&cbt_worker_list_lock_[j], NULL);
        pthread_cond_init(&cbt_queue_empty_[j], NULL);

        std::deque<PAOArray*>* d = new std::deque<PAOArray*>();
        cbt_queue_.push_back(d);
    }

    // set up workers for insertion into CBTs
    tid_ = new pthread_t[ntrees_];
    // sending two arguments to workers
    std::vector<args_struct*> args;
    for (uint32_t j = 0; j < ntrees_; ++j) {
        args_struct* a = new args_struct(2);
        a->argv[0] = (void*)this;
        a->argv[1] = (void*)((intptr_t)j);
        args.push_back(a);
        pthread_create(&tid_[j], NULL, worker, a);
    }

    // create a pool of buffers
    bufpool_ = new bufferpool(ops_, kInsertAtOnce, ncore_ * ntrees_ * 2);
    buffered_paos_ = new PAOArray*[ncore_ * ntrees_];
    for (uint32_t j = 0; j < ncore_ * ntrees_; ++j) {
        buffered_paos_[j] = bufpool_->get_buffer();
    }
    for (uint32_t j = 0; j < ntrees_; ++j)
        delete args[j];
}

bool map_cbt_manager::emit(void *k, void *v, size_t keylen, unsigned hash) {
    uint32_t treeid = hash % ntrees_;
    uint32_t coreid = threadinfo::current()->cur_core_;
    uint32_t bufid = coreid * ntrees_ + treeid;
    PAOArray* buf = buffered_paos_[bufid];
    uint32_t ind = buf->index();
    ops()->setKey(buf->list()[ind], (char*)k);
    buf->set_index(ind + 1);

    if (buf->index() == kInsertAtOnce) {

        pthread_mutex_lock(&cbt_worker_list_lock_[treeid]);
        cbt_queue_[treeid]->push_back(buf);
        pthread_cond_signal(&cbt_queue_empty_[treeid]);
        pthread_mutex_unlock(&cbt_worker_list_lock_[treeid]);

        // get new buffer from pool
        buffered_paos_[bufid] = bufpool_->get_buffer();
    }
//    fprintf(stderr, "[%ld], inserted at %d\n", pthread_self(), ind);
    return true;
}

bool map_cbt_manager::link_user_map(const std::string& soname) { 
    const char* err;
    void* handle;
    handle = dlopen(soname.c_str(), RTLD_NOW);
    if (!handle) {
        fputs(dlerror(), stderr);
        return false;
    }

    Operations* (*create_ops_obj)() = (Operations* (*)())dlsym(handle,
            "__libminni_create_ops");
    if ((err = dlerror()) != NULL) {
        fprintf(stderr, "Error locating symbol __libminni_create_ops\
                in %s\n", err);
        exit(-1);
    }
    ops_ = create_ops_obj();
    return true;
}

void* map_cbt_manager::worker(void *x) {
    args_struct* a = (args_struct*)x;
    map_cbt_manager* m = (map_cbt_manager*)(a->argv[0]);
    uint32_t treeid = (intptr_t)(a->argv[1]);
    std::deque<PAOArray*>* q = m->cbt_queue_[treeid];

    while (q->empty()) {
        pthread_mutex_lock(&m->cbt_worker_list_lock_[treeid]);
        pthread_cond_wait(&m->cbt_queue_empty_[treeid],
                &m->cbt_worker_list_lock_[treeid]);
        pthread_mutex_unlock(&m->cbt_worker_list_lock_[treeid]);

        while (!q->empty()) {
            // remove array from queue
            pthread_mutex_lock(&m->cbt_worker_list_lock_[treeid]);
            PAOArray* buf = q->front();
            q->pop_front();
            pthread_mutex_unlock(&m->cbt_worker_list_lock_[treeid]);

            // perform insertion
            m->cbt_[treeid]->bulk_insert(buf->list(), buf->index());

            // return buffer to pool
            m->bufpool_->return_buffer(buf);
        }
    }
    return 0;
}

void map_cbt_manager::finalize() {
    uint32_t treeid = 0; //hash & (ncore_ - 1);
    uint32_t coreid = threadinfo::current()->cur_core_;
    PAOArray* buf = buffered_paos_[coreid * ntrees_ + treeid];
    uint64_t num_read;

    for (uint32_t i = 0; i < ntrees_; ++i) {
        bool remain = true;
        do {
            pthread_mutex_lock(&cbt_worker_list_lock_[i]);
            remain = cbt_[i]->bulk_read(buf->list(), num_read, kInsertAtOnce);
            pthread_mutex_unlock(&cbt_worker_list_lock_[i]);
        // copy results
        } while (remain);
    }
}

#endif
