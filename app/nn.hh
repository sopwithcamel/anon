#ifndef NN_HH_
#define NN_HH_ 1

#include <algorithm>
#include "overlap_splitter.hh"
#include "appbase.hh"
#include "ic.hh"
#include "nn_plain.h"
#include "tokenizers.hh"

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
            split_record sd(ma, s_.overlap(), " \t\n");
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
    void print_record(FILE* f, const char* key, void* v) {}

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
        uint32_t ret = 0;
        for (uint32_t i = 0; i < HASHLEN; ++i)
            if (s1[i] != s2[i]) ++ret;
        return ret;
    }

    void map_function(split_t *ma) {
        img_dist_pair_t* id = new img_dist_pair_t();
        for (uint32_t i = ma->split_start_offset;
                i < ma->split_end_offset; ++i) {
            ICPlainPAO* p = (ICPlainPAO*)input_[i];
            uint32_t n = p->num_neighbors();
            if (n == 1)
                continue;
            for (uint32_t j = 0; j < n; ++j) {
                for (uint32_t k = 0; k < n; ++k) {
                    if (j == k) continue;
                    img_hash_pair_t ih1 = p->neighbor(j);
                    img_hash_pair_t ih2 = p->neighbor(k);
                    id->img = ih2.img;
                    id->dist = hamming_distance(ih1.hash, ih2.hash);
                    map_emit(ih1.img, id, IDLEN - 1);
                }
            }
        }
        delete id;
    }

    bool result_compare(const char* k1, const void* v1, 
            const char* k2, const void* v2) {}

    void print_results_header() {
        printf("\nnearest neighbor: results\n");
    }

    void print_record(FILE* f, const char* key, void* v) {
        fprintf(f, "%15s - %15s\n", key, (char*)v);
    }

    const std::vector<PartialAgg*>& input_;
    int nsplit_;
    // tracks position during split generation
    size_t pos_;
    size_t input_size_;
};

#endif  // NN_HH_
