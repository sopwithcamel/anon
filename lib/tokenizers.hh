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
    bool letter(char c) {
        return isalnum(c);
    }

    split_t* ma_;
    char* str_;
    char* saveptr1;
    size_t len_;
    bool bytewise_;
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
