/* Metis
 * Yandong Mao, Robert Morris, Frans Kaashoek
 * Copyright (c) 2012 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, subject to the conditions listed
 * in the Metis LICENSE file. These conditions include: you must preserve this
 * copyright notice, and you cannot mention the copyright holders in
 * advertising related to the Software without their permission.  The Software
 * is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This notice is a
 * summary of the Metis LICENSE file; the license in that file is legally
 * binding.
 */
#ifndef DEFSPLITTER_HH_
#define DEFSPLITTER_HH_ 1
#include <string.h>
#include <pthread.h>
#include <algorithm>
#include <ctype.h>
#include "mr-types.hh"
#include <asm/mman.h>

struct defsplitter {
    defsplitter(char *d, size_t size, size_t nsplit)
        : d_(d), size_(size), nsplit_(nsplit), pos_(0) {
    }
    defsplitter(const char *f, size_t nsplit) :
            f_(NULL), nsplit_(nsplit), pos_(0) {
        f_ = fopen(f, "r");
        assert(f_);
        fseek(f_, 0, SEEK_END);
        size_ = ftell(f_);
        rewind(f_);

        pthread_mutex_init(&flock_, NULL);
    }
    ~defsplitter() {
        fclose(f_);
        pthread_mutex_destroy(&flock_);
    }
    int prefault() {
        int sum = 0;
        for (size_t i = 0; i < size_; i += 4096)
            sum += d_[i];
        return sum;
    }
    bool get_split_chunk(split_t* ma);
    bool split(split_t *ma, int ncore, const char *stop, size_t align = 0);
    size_t size() const {
        return size_;
    }

  private:
    FILE* f_;
    pthread_mutex_t flock_;
    char *d_;
    size_t size_;
    int nsplit_;
    size_t pos_;
};

bool defsplitter::get_split_chunk(split_t* ma) {
    if (ma->chunk_end_offset >= ma->split_end_offset)
        return false;
    pthread_mutex_lock(&flock_);
    // seek to end of current chunk
    fseek(f_, ma->chunk_end_offset, SEEK_SET);
    // read in buffer
    size_t read_length = std::min(ma->kBufferSize,
            ma->split_end_offset - ma->chunk_end_offset);
    size_t ret = fread(ma->data, sizeof(char), read_length, f_);
    pthread_mutex_unlock(&flock_);
    if (ret != read_length) {
        perror("fread");
        assert(false);
    }
    ma->chunk_start_offset = ma->chunk_end_offset;
    ma->chunk_end_offset += read_length;
    return true;
}

bool defsplitter::split(split_t *ma, int ncores, const char *stop, size_t align) {
    int max = std::max((size_t)1, size_ >> 12); // divide by 4096
    if (nsplit_ > max) {
        nsplit_ = max;
    }
    if (nsplit_ == 0)
        nsplit_ = std::min(max, ncores * def_nsplits_per_core);
    if (pos_ >= size_)
        return false;
    size_t length = std::min(size_ - pos_, size_ / nsplit_);
//    if (length < size_ - pos_)
//        length = round_up(length, 4096); 

    ma->split_start_offset = ma->chunk_start_offset = ma->chunk_end_offset = pos_;
    ma->split_end_offset = pos_ + length;

    get_split_chunk(ma);
    pos_ += length;
    return true;
}

#endif
