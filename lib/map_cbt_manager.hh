#ifndef MAP_CBT_MANAGER_HH_
#define MAP_CBT_MANAGER_HH_ 1

#include <dlfcn.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "array.hh"
#include "test_util.hh"
#include "appbase.hh"
#include "threadinfo.hh"
#include "CompressTree.h"
#include "PartialAgg.h"

using namespace google::protobuf::io;

struct map_cbt_manager_base {
    virtual ~map_cbt_manager_base() {}
    virtual void init(const std::string& libname, uint32_t ncore) = 0;
    virtual bool emit(void *key, void *val, size_t keylen, unsigned hash,
            uint32_t core) = 0;
    virtual void finalize() = 0;
    virtual const Operations* ops() const = 0;
};

/* @brief: A map manager using the CBT as the internal data structure */
struct map_cbt_manager : public map_cbt_manager_base {
    map_cbt_manager();
    ~map_cbt_manager();
    void init(const std::string& libname, uint32_t ncore);
    bool emit(void *key, void *val, size_t keylen, unsigned hash,
            uint32_t core);
    void finalize();
    const Operations* ops() const {
        assert(ops_);
        return ops_;
    }        
  private:
    bool link_user_map(const std::string& libname);

  private:
    const uint32_t kInsertAtOnce;

    uint32_t ncore_;
    pthread_spinlock_t cbt_lock_;
    cbt::CompressTree* cbt_;
    Operations* ops_;

    uint32_t* num_buffered_paos_;
    PartialAgg*** buffered_paos_;
};

map_cbt_manager::map_cbt_manager() :
        kInsertAtOnce(100000), ops_(NULL),
        num_buffered_paos_(NULL), buffered_paos_(NULL) {
} 

map_cbt_manager::~map_cbt_manager() {
    for (uint32_t j = 0; j < ncore_; ++j) {
        for (uint32_t i = 0; i < kInsertAtOnce; ++i)
            ops()->destroyPAO(buffered_paos_[j][i]);
        delete[] buffered_paos_[j];
    }
    delete[] buffered_paos_;
    delete[] num_buffered_paos_;
    pthread_spin_destroy(&cbt_lock_);
}

void map_cbt_manager::init(const std::string& libname, uint32_t ncore) {
    assert(link_user_map(libname));
    ncore_ = ncore;
    assert(ncore_ > 0);
    num_buffered_paos_ = new uint32_t[ncore_];
    bzero(num_buffered_paos_, ncore * sizeof(uint32_t));
    buffered_paos_ = new PartialAgg**[ncore_];
    for (uint32_t j = 0; j < ncore_; ++j) {
        buffered_paos_[j] = new PartialAgg*[kInsertAtOnce];
        for (uint32_t i = 0; i < kInsertAtOnce; ++i)
            ops()->createPAO(NULL, &buffered_paos_[j][i]);
    }

    uint32_t fanout = 8;
    uint32_t buffer_size = 31457280;
    uint32_t pao_size = 20;
    cbt_ = new cbt::CompressTree(2, fanout, 1000, buffer_size, pao_size, ops_);
    pthread_spin_init(&cbt_lock_, PTHREAD_PROCESS_PRIVATE);

/*
    std::stringstream ss;
    ss << "Hello";
    IstreamInputStream* ii = new IstreamInputStream(&ss);
    CodedInputStream* ci = new CodedInputStream(ii);
*/
}

bool map_cbt_manager::emit(void *k, void *v, size_t keylen, unsigned hash,
        uint32_t core) {
    core = threadinfo::current()->cur_core_;
    PartialAgg** buf = buffered_paos_[core];
    uint32_t& ind = num_buffered_paos_[core];
//    fprintf(stderr, "[%ld], inserted at %d on core [%d]\n", pthread_self(), ind, core);
    ops()->setKey(buf[ind], (char*)k);
    if (++ind == kInsertAtOnce) {
        pthread_spin_lock(&cbt_lock_);
        assert(cbt_->bulk_insert(buf, ind));
        ind = 0;
        pthread_spin_unlock(&cbt_lock_);
    }
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
    bool remain;
    uint64_t num_read;
    uint32_t core = threadinfo::current()->cur_core_;
    PartialAgg** buf = buffered_paos_[core];

    do {
        pthread_spin_lock(&cbt_lock_);
        remain = cbt_->bulk_read(buf, num_read, kInsertAtOnce);
        pthread_spin_unlock(&cbt_lock_);

        // copy results
    } while (remain);
}

#endif
