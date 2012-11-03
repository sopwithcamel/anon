#ifndef BUFFERPOOL_HH
#define BUFFERPOOL_HH 1

#include <assert.h>
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

typedef boost::object_pool<PAOArray> bufferpool;

#endif  // BUFFERPOOL_HH
