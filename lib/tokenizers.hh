#ifndef TOKENIZER_HH
#define TOKENIZER_HH
#include <string.h>
#include <pthread.h>
#include <algorithm>
#include <ctype.h>
#include "mr-types.hh"
#include <asm/mman.h>

struct split_word {
    split_word(split_t *ma) :
            ma_(ma), len_(0),
            bytewise_(true) {
        assert(ma_ && ma_->data);
        str_ = ma_->data;
    }

    bool fill(char *k, size_t maxlen, size_t &klen) {
        char* spl;
        size_t chunk_length = ma_->chunk_end_offset -
            ma_->chunk_start_offset;

        if (bytewise_) {
            char *d = ma_->data;
            klen = 0;
            for (; len_ < chunk_length && !isalnum(d[len_]); ++len_);
            if (len_ == chunk_length) {
                return false;
            }
            for (; len_ < chunk_length && isalnum(d[len_]); ++len_) {
                k[klen++] = d[len_];
            }
            k[klen] = 0;
            return true;
        }

        // split token
//        spl = strtok_r(str_, " \t\n\r\0", &saveptr1);
        spl = strtok_r(str_, " .-=_\t\n\r\'\"?,;`:!*()-\0\uFEFF", &saveptr1);
        if (spl == NULL)
            return false;
        int l = strlen(spl);
        strncpy(k, spl, l);
        klen = l;
        str_ = NULL;
        if (spl + maxlen > ma_->data + chunk_length) {
            bytewise_ = true;
            len_ = (spl - ma_->data) + klen;
        }
        return true;
    }

  private:
    split_t* ma_;
    char* str_;
    char* saveptr1;
    size_t len_;
    bool bytewise_;
};

struct split_record {
    split_record(split_t *ma, size_t overlap, const char* stop) :
            ma_(ma), len_(0),
            bytewise_(false),
            overlap_(overlap) {
        assert(ma_ && ma_->data);
        strcpy(stop_, stop);
        str_ = ma_->data;

        // if it is not the first split, then move forward until we hit a
        // newline
        if (ma_->split_start_offset > 0) {
            for (; *str_ != '\n'; ++str_);
            ++str_;
        }
    }

    bool fill(char *k, size_t maxlen, size_t &klen) {
        char* spl;
        size_t chunk_length = ma_->chunk_end_offset -
            ma_->chunk_start_offset;

        if (bytewise_) {
            char *d = ma_->data;
            klen = 0;
            // skip over non-interesting items
            for (; len_ < chunk_length && !isalnum(d[len_]); ++len_);

            // copy interesting letters
            for (; len_ < chunk_length && isalnum(d[len_]); ++len_) {
                k[klen++] = d[len_];
            }
            k[klen] = 0;
            if (len_ + overlap_ > chunk_length) {
                // we return false after finding the first newline in the
                // overlap area
                if (d[len_] == '\n')
                    return false;
            }
            return true;
        }

        // split
        spl = strtok_r(str_, stop_, &saveptr1);
        str_ = NULL;

        int l = strlen(spl);
        strncpy(k, spl, l);
        klen = l;

        // if we're close to the overlap zone, switch to bytewise (and
        // non-modifying) mode
        if (spl + overlap_ + maxlen > ma_->data + chunk_length) {
            bytewise_ = true;
            len_ = (spl - ma_->data) + klen;
        }
        return true;
    }

  private:
    split_t* ma_;
    char* str_;
    char stop_[256];
    char* saveptr1;
    size_t len_;
    bool bytewise_;
    size_t overlap_;
};

struct split_large_record {
    split_large_record(split_t *ma, size_t overlap, const char* stop) :
            ma_(ma), len_(0),
            bytewise_(false),
            overlap_(overlap) {
        assert(ma_ && ma_->data);
        strcpy(stop_, stop);
        str_ = ma_->data;

        // if it is not the first split, then move forward until we hit a
        // newline
        if (ma_->split_start_offset > 0) {
            for (; *str_ != '\n'; ++str_);
            ++str_;
        }
    }

    bool fill(char *k, size_t maxlen, size_t &klen) {
        char* spl;
        size_t chunk_length = ma_->chunk_end_offset -
            ma_->chunk_start_offset;

        // split
        spl = strtok_r(str_, stop_, &saveptr1);
        str_ = NULL;

        int l = strlen(spl);
        strncpy(k, spl, l);
        klen = l;

        // check if we're into the overlap zone
        if (spl + l > ma_->data + chunk_length)
            return false;
        return true;
    }

  private:
    split_t* ma_;
    char* str_;
    char stop_[256];
    char* saveptr1;
    size_t len_;
    bool bytewise_;
    size_t overlap_;
};

struct split_digram {
    split_digram(split_t *ma) :
            ma_(ma), len_(0),
            bytewise_(false),
            last_(NULL), last_len_(0) {
        assert(ma_ && ma_->data);
        str_ = ma_->data;
    }

    bool fill(char *k, size_t maxlen, size_t &klen) {
        char* spl;
        size_t chunk_length = ma_->chunk_end_offset -
            ma_->chunk_start_offset;

        if (bytewise_) {
            char *d = ma_->data;
            klen = 0;
            // last_ should not be NULL since this branch oughtn't be entered
            // first
            assert(last_);

            for (; len_ < chunk_length && !isalnum(d[len_]); ++len_);
            if (len_ == chunk_length) {
                return false;
            }

            // First copy the last string
            strncpy(k, last_, last_len_);
            klen = last_len_;
            k[klen++] = '-';
            k[klen] = 0;

            last_ = &d[len_];

            for (; len_ < chunk_length && isalnum(d[len_]); ++len_) {
                k[klen++] = d[len_];
            }
            k[klen] = 0;
            last_len_ = klen - last_len_ - 1;

            return true;
        }

        const char* stop = " .-=_\t\n\r\'\"?,;`:!*()-\0\uFEFF";

        if (last_ == NULL) {
            // split token
            spl = strtok_r(str_, stop, &saveptr1);
            if (spl == NULL)
                return false;
            last_ = spl;
            last_len_ = strlen(last_);
            str_ = NULL;
        }
        strncpy(k, last_, last_len_);
        klen = last_len_;

        k[klen++] = '-';
        k[klen] = '\0';

        // split for second token
        spl = strtok_r(NULL, stop, &saveptr1);
        if (spl == NULL)
            return false;
        int l = strlen(spl);
        strncat(k, spl, l);
        klen += l;

        // update last_ and last_len_ for later
        last_ = spl;
        last_len_ = l;

        if (spl + maxlen > ma_->data + chunk_length) {
            bytewise_ = true;
            len_ = (spl - ma_->data) + klen;
        }
        return true;
    }

  private:
    split_t* ma_;
    char* str_;
    char* saveptr1;
    size_t len_;
    bool bytewise_;
    char* last_;
    size_t last_len_;
};

#endif  // TOKENIZER_HH
