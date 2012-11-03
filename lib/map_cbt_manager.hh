#ifndef MAP_CBT_MANAGER_HH_
#define MAP_CBT_MANAGER_HH_ 1

#include <dlfcn.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

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

  private:
    const uint32_t kInsertAtOnce;

    uint32_t ntrees_;
    uint32_t ncore_;
    pthread_spinlock_t* cbt_lock_;
    cbt::CompressTree** cbt_;
    Operations* ops_;

    PAOArray** buffered_paos_;
    bufferpool* bufpool_;
};

map_cbt_manager::map_cbt_manager() :
        kInsertAtOnce(100000), ops_(NULL),
        buffered_paos_(NULL) {
} 

map_cbt_manager::~map_cbt_manager() {
    for (uint32_t j = 0; j < ncore_ * ntrees_; ++j) {
        bufpool_->destroy(buffered_paos_[j]);
    }

    for (uint32_t j = 0; j < ntrees_; ++j) {
        delete cbt_[j];
        pthread_spin_destroy(&cbt_lock_[j]);
    }
    delete[] buffered_paos_;
    delete[] cbt_;
    delete[] cbt_lock_;
}

void map_cbt_manager::init(const std::string& libname, uint32_t ncore) {
    assert(link_user_map(libname));
    ncore_ = ncore;
    ntrees_ = 1;

    buffered_paos_ = new PAOArray*[ncore_ * ntrees_];
    cbt_ = new cbt::CompressTree*[ntrees_];
    cbt_lock_ = new pthread_spinlock_t[ntrees_];

    uint32_t fanout = 8;
    uint32_t buffer_size = 31457280;
    uint32_t pao_size = 20;

    for (uint32_t j = 0; j < ntrees_; ++j) {
        cbt_[j] = new cbt::CompressTree(2, fanout, 1000, buffer_size,
                pao_size, ops_);
        pthread_spin_init(&cbt_lock_[j], PTHREAD_PROCESS_PRIVATE);
    }

    bufpool_ = new bufferpool();
    for (uint32_t j = 0; j < ncore_ * ntrees_; ++j) {
        buffered_paos_[j] = bufpool_->construct(ops_, kInsertAtOnce);
    }
}

bool map_cbt_manager::emit(void *k, void *v, size_t keylen, unsigned hash) {
    uint32_t treeid = 0; //hash & (ncore_ - 1);
    uint32_t coreid = threadinfo::current()->cur_core_;
    PAOArray* buf = buffered_paos_[coreid * ntrees_ + treeid];
    uint32_t ind = buf->index();
    ops()->setKey(buf->list()[ind], (char*)k);
    buf->set_index(ind + 1);

    if (buf->index() == kInsertAtOnce) {
        pthread_spin_lock(&cbt_lock_[treeid]);
        assert(cbt_[treeid]->bulk_insert(buf->list(), buf->index()));
        buf->init();
        pthread_spin_unlock(&cbt_lock_[treeid]);
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

void map_cbt_manager::finalize() {
    uint32_t treeid = 0; //hash & (ncore_ - 1);
    uint32_t coreid = threadinfo::current()->cur_core_;
    PAOArray* buf = buffered_paos_[coreid * ntrees_ + treeid];
    uint64_t num_read;

    for (uint32_t i = 0; i < ntrees_; ++i) {
        bool remain = true;
        do {
            pthread_spin_lock(&cbt_lock_[treeid]);
            remain = cbt_[i]->bulk_read(buf->list(), num_read, kInsertAtOnce);
            pthread_spin_unlock(&cbt_lock_[treeid]);
        // copy results
        } while (remain);
    }
}

#endif
