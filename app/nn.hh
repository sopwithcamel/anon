#ifndef NN_HH_
#define NN_HH_ 1

#include <algorithm>
#include "overlap_splitter.hh"
#include "appbase.hh"
#include "ic.hh"
#include "nn_plain.h"

struct split_ic {
    split_ic(split_t *ma, size_t overlap) :
            ma_(ma), len_(0),
            bytewise_(false),
            overlap_(overlap) {
        assert(ma_ && ma_->data);
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

        const char* stop = " \t\n";

        // split
        spl = strtok_r(str_, stop, &saveptr1);
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
    char* saveptr1;
    size_t len_;
    bool bytewise_;
    size_t overlap_;
};

struct img_cluster : public mapreduce_appbase {
    img_cluster(const char *f, int nsplit) : s_(f, nsplit) {}
    bool split(split_t *ma, int ncores) {
        return s_.split(ma, ncores, " \t\r\n\0");
    }
    int key_compare(const void *s1, const void *s2) {
        return strcmp((const char *) s1, (const char *) s2);
    }
    void map_function(split_t *ma) {
        char k[64], v[64], rotv[64];
        uint32_t kc = 0;
        size_t klen, vlen;
        int step = 4;
        size_t hash_len = 20;
        int prefix_len = hash_len - step;
        int num_rot = hash_len / step;
        bool not_empty = true;
        img_hash_pair_t* p = new img_hash_pair_t();
        do {
            split_ic sd(ma, s_.overlap());
            do {
                not_empty = sd.fill(k, 64, klen);
                // terminate string
                k[klen] = '\0';
                assert(not_empty); // can't end after key
                not_empty = sd.fill(v, 64, vlen);
                if (++kc % 100000 == 0)
                    fprintf(stderr, ".");
                if (vlen < hash_len) {
                    int d = hash_len - vlen;
                    for (int i = hash_len - 1; i >= d; --i)
                        v[i] = v[i - d];
                    for (int i = 0; i < d; ++i)
                        v[i] = '0';
                        
                }
                v[hash_len] = '\0';
                for (int i = 0; i < num_rot; ++i) {
                    memset(rotv, 0, prefix_len + 1);
                    int j = 0;
                    int st = i * step;
                    do {
                        rotv[j] = v[(st + j) % hash_len];
                        ++j;
                    } while (j < prefix_len);
                    p->img = k;
                    p->hash = v;
                    map_emit(rotv, (void*)p, prefix_len);
                }
                memset(k, 0, klen);
                memset(v, 0, vlen);
            } while(not_empty);
        } while (s_.get_split_chunk(ma));
        delete p;
    }
    bool result_compare(const char* k1, const void* v1, 
            const char* k2, const void* v2) {
        assert(false && "Not required"); 
        return false;
    }

    void print_results_header() {}
    void print_record(FILE* f, const char* key, void* v) {
        fprintf(f, "%15s - %d\n", key, ptr2int<unsigned>(v));
    }
  private:
    overlap_splitter s_;
};

struct nearest_neighbor : public mapreduce_appbase {
    nearest_neighbor(const std::vector<PartialAgg*>& input, int nsplit) :
            input_(input), nsplit_(nsplit), pos_(0) {
        input_size_ = input_.size();
    }

    bool split(split_t *ma, int ncores) {
        if (nsplit_ == 0)
            nsplit_ = ncores * def_nsplits_per_core;
        if (pos_ >= input_size_)
            return false;
        size_t req_units = input_size_ / nsplit_;
        size_t length = std::min(input_size_ - pos_, req_units);
        ma->split_start_offset = pos_;
        ma->split_end_offset = pos_ + length;
        pos_ += length;
        return true;
    }

    int key_compare(const void *s1, const void *s2) {
        return strcmp((const char *) s1, (const char *) s2);
    }

    uint32_t hamming_distance(const char* s1, const char* s2) {
        // TODO: implement
        return 0;
    }

    void map_function(split_t *ma) {
        img_dist_pair_t* id = new img_dist_pair_t();
        for (uint32_t i = ma->split_start_offset;
                i < ma->split_end_offset; ++i) {
            ICPlainPAO* p = (ICPlainPAO*)input_[i];
            uint32_t n = p->num_neighbors();
            for (uint32_t i = 0; i < n; ++i) {
                for (uint32_t j = 0; j < n; ++j) {
                    if (i == j) continue;
                    img_hash_pair_t ih1 = p->neighbor(i);
                    img_hash_pair_t ih2 = p->neighbor(j);
                    map_emit(ih1.img, ih2.img,
                            hamming_distance(ih1.hash, ih2.hash));
                }
            }
        }
        delete id;
    }
    bool result_compare(const char* k1, const void* v1, 
            const char* k2, const void* v2) {}
    void print_results_header() {}
    void print_record(FILE* f, const char* key, void* v) {}

    const std::vector<PartialAgg*>& input_;
    int nsplit_;
    // tracks position during split generation
    size_t pos_;
    size_t input_size_;
};

#endif  // NN_HH_
