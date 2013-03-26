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
        size_t klen, vlen;
        int step = 4;
        size_t hash_len = 20;
        int prefix_len = hash_len - step;
        int num_rot = hash_len / step;
        bool not_empty = true;
        ICValue* ic_value = new ICValue();
        do {
            split_record sd(ma, s_.overlap(), " \t\n");
            do {
                not_empty = sd.fill(k, 64, klen);
                // terminate string
                k[klen] = '\0';
                assert(klen < IDLEN);
                assert(not_empty); // can't end after key
                not_empty = sd.fill(v, 64, vlen);
                assert(vlen >= 10);
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
                    ic_value->num_neighbors_ = 1;
                    strcpy(ic_value->neigh_[0].img, k);
                    strcpy(ic_value->neigh_[0].hash, v);
                    map_emit(rotv, (void*)ic_value, prefix_len);
                }
                memset(k, 0, klen);
                memset(v, 0, vlen);
            } while(not_empty);
        } while (s_.get_split_chunk(ma));
        delete ic_value;
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
    nearest_neighbor(map_manager* m, int nsplit) :
            m_(m), nsplit_(nsplit), cur_split_(0) {
    }

    bool split(split_t *ma, int ncores) {
        assert(nsplit_ > 0);
        if (cur_split_++ == nsplit_)
            return false;
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
        // create buffer for fetching PAOs
        uint64_t buf_size = 100000;
        uint64_t num_read;
        PartialAgg** buf = new PartialAgg*[buf_size];
        
        NNPlainPAO::NNValue* nnv = new NNPlainPAO::NNValue();
        bool ret;
        do {
            ret = m_->get_paos(buf, num_read, buf_size);
            for (uint32_t i = 0; i < num_read; ++i) {
                ICPlainPAO* p = (ICPlainPAO*)buf[i];
                uint32_t n = p->num_neighbors();
                if (n > 1) {
                    for (uint32_t j = 0; j < n; ++j) {
                        for (uint32_t k = 0; k < n; ++k) {
                            if (j == k) continue;
                            img_hash_pair_t ih1 = p->neighbor(j);
                            img_hash_pair_t ih2 = p->neighbor(k);
                            strncpy(nnv->nn, ih2.img, IDLEN - 1);
                            nnv->hamming_dist = hamming_distance(ih1.hash, ih2.hash);
                            map_emit(ih1.img, nnv, IDLEN - 1);
                        }
                    }
                }
                m_->ops()->destroyPAO(p);
            }
        } while (ret);
        delete nnv;
    }

    bool result_compare(const char* k1, const void* v1, 
            const char* k2, const void* v2) { assert(false && "Not required"); return false; }

    void print_results_header() {
        printf("\nnearest neighbor: results\n");
    }

    void print_record(FILE* f, const char* key, void* v) {
        fprintf(f, "%15s - %15s\n", key, (char*)v);
    }

    map_manager* m_;
    int nsplit_;
    // mutex for 
    pthread_mutex_t mm_mutex_;
    // tracks number of splits during split generation
    int cur_split_;
};

#endif  // NN_HH_
