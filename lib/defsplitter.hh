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

struct defsplitter {
    defsplitter(char *d, size_t size, size_t nsplit)
        : d_(d), size_(size), nsplit_(nsplit), pos_(0) {
    }
    defsplitter(const char *f, size_t nsplit) :
            fd_(-1), nsplit_(nsplit), pos_(0) {
        assert((fd_ = open(f, O_RDONLY)) >= 0);
        struct stat fst;
        assert(fstat(fd_, &fst) == 0);
        size_ = fst.st_size;
    }
    ~defsplitter() {
        assert(close(fd_) == 0);
    }
    int prefault() {
        int sum = 0;
        for (size_t i = 0; i < size_; i += 4096)
            sum += d_[i];
        return sum;
    }
    void* mmap_split(size_t offset, size_t length);
    bool split(split_t *ma, int ncore, const char *stop, size_t align = 0);
    void trim(size_t sz) {
        assert(sz <= size_);
        size_ = sz;
    }
    size_t size() const {
        return size_;
    }

  private:
    int fd_;
    char *d_;
    size_t size_;
    int nsplit_;
    size_t pos_;
};

void* defsplitter::mmap_split(size_t offset, size_t length) {
    assert(fd_ > 0);
    void* d = mmap(0, length, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd_, offset);
    assert(d_);
    return d;
}

bool defsplitter::split(split_t *ma, int ncores, const char *stop, size_t align) {
    int max = size_ >> 12; // divide by 4096
    if (nsplit_ > max) {
        nsplit_ = max;
    }
    if (nsplit_ == 0)
        nsplit_ = std::min(max, ncores * def_nsplits_per_core);
    if (pos_ >= size_)
        return false;
    size_t length = std::min(size_ - pos_, size_ / nsplit_);
    if (length < size_ - pos_)
        length = round_up(length, 4096); 
    ma->data = mmap_split(pos_, length);
    ma->length = length;
    pos_ += length;

/*
    if (align) {
        ma->length = round_down(ma->length, align);
        assert(ma->length);
    }
*/
    return true;
}

struct split_word {
    split_word(split_t *ma) : ma_(ma), len_(0), end_address_(NULL),
            bytewise_(false) {
        assert(ma_ && ma_->data);
        str_ = (char*)ma_->data;
        end_address_ = (char*)ma_->data + ma_->length;
    }
/*
    char *fill(char *k, size_t maxlen, size_t &klen) {
        char *d = (char *)ma_->data;
        klen = 0;
        for (; pos_ < ma_->length && !letter(d[pos_]); ++pos_);
        if (pos_ == ma_->length)
            return NULL;
        char *index = &d[pos_];
        for (; pos_ < ma_->length && letter(d[pos_]); ++pos_) {
//            k[klen++] = toupper(d[pos_]);
            k[klen++] = d[pos_];
//            assert(klen < maxlen);
        }
        k[klen] = 0;
        return index;
    }
*/
    bool fill(char *k, size_t maxlen, size_t &klen) {
        char* spl;
        if (len_ >= ma_->length)
            return false;

        if (bytewise_) {
            char *d = (char *)ma_->data;
            klen = 0;
            for (; len_ < ma_->length && !letter(d[len_]); ++len_);
            if (len_ == ma_->length)
                return false;
            for (; len_ < ma_->length && letter(d[len_]); ++len_) {
                k[klen++] = d[len_];
            }
            k[klen] = 0;
            return true;
        }

        // split token
        spl = strtok_r(str_, " \n\r\0", &saveptr1);
        if (spl == NULL)
            return false;
        int l = strlen(k);
        strncpy(k, spl, l);
        klen = l;
        str_ = NULL;
        if (spl + maxlen > end_address_) {
            bytewise_ = true;
            len_ = (spl - (char*)ma_->data) + klen;
        }
        return true;
    }
  private:
    bool letter(char c) {
//        return toupper(c) >= 'A' && toupper(c) <= 'Z';
        return c >= 'a' && c <= 'z';
    }
    split_t* ma_;
    char* str_;
    char* saveptr1;
    size_t len_;
    char* end_address_;
    bool bytewise_;
};

#endif
