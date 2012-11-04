#ifndef MAP_CBT_MANAGER_HH_
#define MAP_CBT_MANAGER_HH_ 1

#include <dlfcn.h>
#include <inttypes.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <zmq.hpp>

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
    PAOArray* get_new_buffer() {
        return bufpool_->construct(ops_, kInsertAtOnce);
    }
    void return_buffer(PAOArray* b) {
        bufpool_->destroy(b);
    }
    static void *worker(void *arg);

  private:
    const uint32_t kInsertAtOnce;

    uint32_t ntrees_;
    uint32_t ncore_;
    pthread_spinlock_t* cbt_lock_;
    cbt::CompressTree** cbt_;
    Operations* ops_;

    // buffer pool
    PAOArray** buffered_paos_;
    bufferpool* bufpool_;

    // zeromq stuff
    zmq::context_t* context_;
    std::vector<zmq::socket_t*> client_socket_;

    // threads for insertion into CBTs
    pthread_t* tid_;
};

map_cbt_manager::map_cbt_manager() :
        kInsertAtOnce(100000), ops_(NULL),
        buffered_paos_(NULL) {
} 

map_cbt_manager::~map_cbt_manager() {
    // clean up buffers
    for (uint32_t j = 0; j < ncore_ * ntrees_; ++j) {
        bufpool_->destroy(buffered_paos_[j]);
    }
    delete[] buffered_paos_;

    // clean up zeromq stuff
    delete context_;
    for (uint32_t j = 0; j < ncore_ * ntrees_; ++j) {
        delete client_socket_[j];
    }

    // clean up CBTs
    for (uint32_t j = 0; j < ntrees_; ++j) {
        delete cbt_[j];
        pthread_spin_destroy(&cbt_lock_[j]);
    }
    delete[] cbt_;
    delete[] cbt_lock_;
}

void map_cbt_manager::init(const std::string& libname, uint32_t ncore) {
    assert(link_user_map(libname));
    ncore_ = ncore;
    ntrees_ = 1;

    // create CBTs
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

    // set up workers for insertion into CBTs

    // set up zeromq stuff. we're using no I/O threads since we're going to use
    // an inproc transport; this transport also requires the communicating
    // threads to share the context
    context_ = new zmq::context_t(0);
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
    bufpool_ = new bufferpool();
    buffered_paos_ = new PAOArray*[ncore_ * ntrees_];
    for (uint32_t j = 0; j < ncore_ * ntrees_; ++j) {
        buffered_paos_[j] = get_new_buffer();
    }

    // create sending sockets and connect
    std::string ep = "inproc://cbtins";
    for (uint32_t i = 0; i < ncore_; ++i) {
        for (uint32_t j = 0; j < ntrees_; ++j) {
            zmq::socket_t* s = new zmq::socket_t(*context_, ZMQ_REQ);
            std::stringstream ss;
            ss << j;
            std::string eqj = ep + ss.str();
            s->connect(eqj.c_str());
            client_socket_.push_back(s);
        }
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
        // Send buffer for emptying
        zmq::message_t request(sizeof(void*));
        memcpy((void*)request.data(), &buf, sizeof(void*));        
//        fprintf(stderr, "Sending address %p %p\n", buf, request.data());
        int ret;
        do {
            ret = client_socket_[bufid]->send(request);
        } while (ret != 0 && zmq_errno() == EINTR);

        //  Wait for reply...
        zmq::message_t reply;
        do {
            ret = client_socket_[bufid]->recv(&reply);
        } while (ret != 0 && zmq_errno() == EINTR);

        // get new buffer from pool
        buffered_paos_[bufid] = get_new_buffer();
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

    // create a request-reply socket
    zmq::socket_t socket(*(m->context_), ZMQ_REP);

    // create unique endpoint;
    std::stringstream ss;
    ss << treeid;
    std::string ep = "inproc://cbtins" + ss.str();

    // accept connections over inproc
    socket.bind(ep.c_str());

    while (true) {
        zmq::message_t request;
        int ret;
        do {
            ret = socket.recv(&request);
        } while (ret != 0 && zmq_errno() == EINTR);

        //  Send reply back to client
        zmq::message_t reply (4);
        memcpy((void *)reply.data(), "True", 4);
        do {
            ret = socket.send(reply);
        } while (ret != 0 && zmq_errno() == EINTR);

        PAOArray* buf;
        memcpy(&buf, (void*)request.data(), sizeof(void*));
//        fprintf(stderr, "Received address %p\n", buf);
        m->cbt_[treeid]->bulk_insert(buf->list(), buf->index());

        // initialize buffer and free
        buf->init();
        m->return_buffer(buf);

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
            pthread_spin_lock(&cbt_lock_[i]);
            remain = cbt_[i]->bulk_read(buf->list(), num_read, kInsertAtOnce);
            pthread_spin_unlock(&cbt_lock_[i]);
        // copy results
        } while (remain);
    }
}

#endif
