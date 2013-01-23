#ifndef MAP_NSORT_MANAGER_HH_
#define MAP_NSORT_MANAGER_HH_ 1

#include <inttypes.h>
#include <vector>
#include <deque>

#include "array.hh"
#include "test_util.hh"
#include "appbase.hh"
#include "bufferpool.hh"
#include "nsort.h"
#include "threadinfo.hh"
#include "CompressTree.h"
#include "HashUtil.h"
#include "PartialAgg.h"

struct args_struct;

// buffer used to serialize PAOs and release them to nsort and to collect the
// results back from nsort
struct nsort_buffer {
  public:
    struct ns_list {
      public:
        ns_list(uint32_t siz) {
            l = new char[siz];
            s = 0;
        }
        ~ns_list() {
            if (l) {
                delete[] l;
                l = NULL;
            }
        }
        char* l;
        uint32_t s;
    };
    nsort_buffer() : kBufferSize(8192), kMaxLists(8192) {
    }
    ~nsort_buffer() {
    }
    uint32_t add_buf() {
        ns_list* l = new ns_list(kBufferSize);
        bufs_.push_back(l);
        return bufs_.size() - 1;
    }
    void clear() {
        for (uint32_t i = 0; i < bufs_.size(); ++i) {
            delete[] bufs_[i];
        }
        bufs_.clear();
    }

    const uint32_t kBufferSize;
    const uint32_t kMaxLists;
    std::vector<ns_list*> bufs_;
};

/* @brief: A map manager using the CBT as the internal data structure */
struct map_nsort_manager : public map_manager {
    map_nsort_manager();
    ~map_nsort_manager();
    void init(Operations* ops, uint32_t ncore);
    bool emit(void *key, void *val, size_t keylen, unsigned hash);
    void flush_buffered_paos();
    void finish_phase(int phase);
    void finalize();
  private:
    static void *worker(void *arg);
    static void *random_input_worker(void *arg);
    void submit_array(PAOArray* buf);
    void error_exit(const char *func, int err, unsigned context);
  private:
    const uint32_t kInsertAtOnce;

    // nsort
    unsigned nsort_context_;
    pthread_mutex_t nsort_context_mutex_;
    bool nsort_all_results_read_;
    sem_t nsort_buffer_semaphore_;

    // buffer pool
    PAOArray** buffered_paos_;
    bufferpool* bufpool_;

    // thread for insertion into HTC
    pthread_t tid_;
    pthread_mutex_t nsort_queue_mutex_;
    pthread_cond_t nsort_queue_empty_;
    std::deque<nsort_buffer*> nsort_queue_;    
};

map_nsort_manager::map_nsort_manager() :
        kInsertAtOnce(100000), nsort_all_results_read_(false),
        buffered_paos_(NULL) {
} 

map_nsort_manager::~map_nsort_manager() {
    // clean up buffers
    delete[] buffered_paos_;
    delete bufpool_;
    sem_destroy(&nsort_buffer_semaphore_);

    pthread_mutex_destroy(&nsort_context_mutex_);
    pthread_mutex_destroy(&nsort_queue_mutex_);
    pthread_cond_destroy(&nsort_queue_empty_);
}

void map_nsort_manager::init(Operations* ops, uint32_t ncore) {
    ops_ = ops;
    ncore_ = ncore;

    sem_init(&nsort_buffer_semaphore_, 0, ncore_ * 10);

    pthread_mutex_init(&nsort_queue_mutex_, NULL);
    pthread_cond_init(&nsort_queue_empty_, NULL);

    // sending two arguments to workers
    std::vector<args_struct*> args;

    // create a pool of buffers
    bufpool_ = new bufferpool(ops_, kInsertAtOnce, ncore_ * 3);
    buffered_paos_ = new PAOArray*[ncore_];
    for (uint32_t j = 0; j < ncore_ ; ++j) {
        buffered_paos_[j] = bufpool_->get_buffer();
    }

    pthread_mutex_init(&nsort_context_mutex_, NULL);

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

void map_nsort_manager::submit_array(PAOArray* buf) {

    // allocate buffer to serialize the PAOArray into
    sem_wait(&nsort_buffer_semaphore_);
    nsort_buffer* nbuf = new nsort_buffer();
    
    uint32_t offset = 0;
    uint32_t buf_id = nbuf->add_buf();
    char* ser_paos = nbuf->bufs_[buf_id]->l;
    uint32_t lim = nbuf->kBufferSize * 0.8;

    for (uint32_t i = 0; i < buf->index(); ++i) {
        PartialAgg* pao = buf->list()[i];
        uint32_t siz = ops()->getSerializedSize(pao);
        ops()->serialize(pao, ser_paos + offset, siz);
        offset += siz;
        ser_paos[offset++]='\n';
        if (offset > lim) {
            nbuf->bufs_[buf_id]->s = offset;
            buf_id = nbuf->add_buf();
            ser_paos = nbuf->bufs_[buf_id]->l;
            offset = 0;
        }
    }
    nbuf->bufs_[buf_id]->s = offset;

    pthread_mutex_lock(&nsort_queue_mutex_);
    nsort_queue_.push_back(nbuf);
    pthread_cond_signal(&nsort_queue_empty_);
//    fprintf(stderr, "qs: %d\t", nsort_queue_.size());
    pthread_mutex_unlock(&nsort_queue_mutex_);

    // return buffer to pool immediately
    bufpool_->return_buffer(buf);
}


void map_nsort_manager::error_exit(const char *func, int err,
        unsigned context) {
    fprintf(stderr, "%s returns %d/%s\n",
            func, err, nsort_message(&context));
    nsort_end(&context);
    exit(1);
}

bool map_nsort_manager::emit(void *k, void *v, size_t keylen, unsigned hash) {
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

void map_nsort_manager::flush_buffered_paos() {
    uint32_t coreid = threadinfo::current()->cur_core_;
    uint32_t bufid = coreid;
    PAOArray* buf = buffered_paos_[bufid];
    submit_array(buf);
}

void map_nsort_manager::finish_phase(int phase) {
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

void* map_nsort_manager::worker(void *x) {
    args_struct* a = (args_struct*)x;
    map_nsort_manager* m = (map_nsort_manager*)(a->argv[0]);
    std::deque<nsort_buffer*>& q = m->nsort_queue_;
    cpu_set_t cset;
    pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &cset);

    // initialize nsort
    int err;
    // specify that newline is used as the separator and that the key is the
    // first field. The separator is actually supposed to separate _fields_ in
    // a record as well, but we can't handle this right now. I think it should
    // be ok if it sorts using a concatenation of the key and the value
    err = nsort_define("-format:sep='\n' -key:pos=1 -processes=8 -temp_file=/localfs/hamur -statistics",
            0, NULL, &m->nsort_context_);
    if (err < 0)
        m->error_exit("nsort_define()", err, m->nsort_context_);

    while (true) {
        pthread_mutex_lock(&m->nsort_queue_mutex_);
        while (q.empty())
            pthread_cond_wait(&m->nsort_queue_empty_,
                    &m->nsort_queue_mutex_);
        pthread_mutex_unlock(&m->nsort_queue_mutex_);

        while (!q.empty()) {
            // remove array from queue
            pthread_mutex_lock(&m->nsort_queue_mutex_);
            nsort_buffer* nbuf = q.front();
            q.pop_front();
            pthread_mutex_unlock(&m->nsort_queue_mutex_);

            // release records to nsort
            int err;
            uint32_t nlists = nbuf->bufs_.size();
            for (uint32_t i = 0; i < nlists; ++i) {
                nsort_buffer::ns_list* nl = nbuf->bufs_[i];
                if ((err = nsort_release_recs(nl->l, nl->s,
                                &m->nsort_context_)) < 0)
                    m->error_exit("nsort_release_recs()", err, m->nsort_context_);
            }

            // deallocate
            delete nbuf;
            sem_post(&m->nsort_buffer_semaphore_);
        }

        int ret;
        sem_getvalue(&m->phase_semaphore_, &ret);
        if (ret == (int)m->ncore_)
            break;
    }

    // finish
    if ((err = nsort_release_end(&m->nsort_context_)) < 0)
        m->error_exit("nsort_release_end()", err, m->nsort_context_);

    return 0;
}

void map_nsort_manager::finalize() {
    // get new buffer
    PAOArray* pa = bufpool_->get_buffer();
    nsort_buffer* nbuf = new nsort_buffer();
    for (uint32_t i = 0; i < nbuf->kMaxLists; ++i)
        nbuf->add_buf();

    bool more_results = true;
    do {
        // get results from nsort
        pthread_mutex_lock(&nsort_context_mutex_);
        // check if all results have been read
        if (!nsort_all_results_read_) {
            int err;
            // read results from nsort
            for (uint32_t i = 0; i < nbuf->kMaxLists; ++i) {
                nsort_buffer::ns_list* ns = nbuf->bufs_[i];
                ns->s = nbuf->kBufferSize;
                if ((err = nsort_return_recs(&(ns->l), (size_t*)&(ns->s),
                                &nsort_context_)) < 0)
                    error_exit("nsort_return_recs()", err, nsort_context_);
            }

            if (err == NSORT_END_OF_OUTPUT) {
                nsort_all_results_read_ = true;
                fprintf(stderr, "%s\n", nsort_get_stats(&nsort_context_));
                nsort_end(&nsort_context_);
                more_results = false;
            }
        } else
            more_results = false;
        pthread_mutex_unlock(&nsort_context_mutex_);

        if (more_results) {
/*
            // deserialize from sorted buffer into paoarray
            uint32_t offset = 0;
            uint32_t ind = 0;
            char* b = nbuf->buf();
            while (offset < nbuf->size()) {
                PartialAgg* pao = pa->list()[ind];
                ops()->deserialize(pao, b + offset);
                offset += ops()->getSerializedSize(pao);
                ++offset;
                ++ind;
            }

            // insert deserialized paos into results
            pthread_mutex_lock(&results_mutex_);
            results_.insert(results_.end(), pa->list(),
                    pa->list() + ind);
            pthread_mutex_unlock(&results_mutex_);
*/
        } else
            break;
    } while (true);

//    delete nbuf;
    bufpool_->return_buffer(pa);
}

#endif
