#ifndef BUFFERPOOL_HH
#define BUFFERPOOL_HH 1

#include <assert.h>
#include <pthread.h>
#include <boost/pool/object_pool.hpp>

#include "PartialAgg.h"

struct PAOArray {
  public:
    explicit PAOArray(const Operations* ops, uint32_t max) :
            ops_(ops), kMaxListSize(max),
            list_(NULL), index_(0) {
        list_ = new PartialAgg*[kMaxListSize];
        for (uint32_t i = 0; i < kMaxListSize; ++i)
            ops_->createPAO(NULL, &list_[i]);
    }
    ~PAOArray() {
        for (uint32_t i = 0; i < kMaxListSize; ++i)
            ops_->destroyPAO(list_[i]);
        delete[] list_;
    }
    void init() {
        index_ = 0;
    }
    PartialAgg*& operator[](uint32_t ind) {
        assert(ind < kMaxListSize);
        return list_[ind];
    }
    PartialAgg** list() {
        return list_;
    }
    uint32_t index() {
        return index_;
    }
    void set_index(uint32_t ind) {
        index_ = ind;
    }
  private:
    const Operations* ops_;
    const uint32_t kMaxListSize; 
    PartialAgg** list_;
    uint32_t index_;
};

struct bufferpool {
    explicit bufferpool(const Operations* ops, uint32_t max_elements_per_array,
            uint32_t max_num_arrays) :
            ops_(ops),
            max_elements_per_array_(max_elements_per_array),
            max_number_of_arrays_(max_num_arrays) {
        pthread_mutex_init(&lock_, NULL);
        pthread_cond_init(&empty_, NULL);
        for (uint32_t i = 0; i < max_number_of_arrays_; ++i) {
            PAOArray* pa = new PAOArray(ops_, max_elements_per_array_);
            arrays_.push_back(pa);
        }
    }
    ~bufferpool() {
        pthread_mutex_destroy(&lock_);
        pthread_cond_destroy(&empty_);
        for (uint32_t i = 0; i < max_number_of_arrays_; ++i) {
            PAOArray* pa = arrays_.front();
            arrays_.pop_front();
            delete pa;
        }
    }
    PAOArray* get_buffer() {
        pthread_mutex_lock(&lock_);
        while (arrays_.empty()) {
            pthread_cond_wait(&empty_, &lock_);
        }
        PAOArray* ret = arrays_.front();
        arrays_.pop_front();
        pthread_mutex_unlock(&lock_);
        return ret;
    }
    void return_buffer(PAOArray* a) {
        a->init();
        pthread_mutex_lock(&lock_);
        arrays_.push_back(a);
        pthread_cond_signal(&empty_);
        pthread_mutex_unlock(&lock_);
    }
  private:
    const Operations* ops_;
    const uint32_t max_elements_per_array_;
    const uint32_t max_number_of_arrays_;
    std::deque<PAOArray*> arrays_;
    pthread_mutex_t lock_;
    pthread_cond_t empty_;
};

#endif  // BUFFERPOOL_HH
