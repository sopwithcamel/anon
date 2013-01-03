#include "PartialAgg.h"

#define KEYLEN      12

class NNPlainPAO : public PartialAgg
{
    friend class NNPlainOperations;
  public:
	NNPlainPAO(char* wrd) {
        memset(key, 0, KEYLEN);
        if (wrd) {
            strncpy(key, wrd, KEYLEN - 1);
            hamming_dist = 1;
        } else {
            hamming_dist = 1;
        }    
    }
	~NNPlainPAO() {
    }
  private:
    char key[KEYLEN];
    char nn[KEYLEN];
    uint32_t hamming_dist;
};

typedef struct {
    char* img;
    uint32_t dist;
} img_dist_pair_t;

class NNPlainOperations : public Operations {
  public:
    Operations::SerializationMethod getSerializationMethod() const {
        return Operations::HAND;
    }

    const char* getKey(PartialAgg* p) const {
        return ((NNPlainPAO*)p)->key;
    }

    bool setKey(PartialAgg* p, char* k) const {
        NNPlainPAO* wp = (NNPlainPAO*)p;
        memset(wp->key, 0, KEYLEN);
        strncpy(wp->key, k, KEYLEN - 1);
        return true;
    }

    void* getValue(PartialAgg* p) const {
        NNPlainPAO* wp = (NNPlainPAO*)p;
        return (void*)(intptr_t)(wp->hamming_dist);
    }

    void setValue(PartialAgg* p, void* v) const {
        NNPlainPAO* wp = (NNPlainPAO*)p;
        wp->hamming_dist = (intptr_t)v;
    }

    bool sameKey(PartialAgg* p1, PartialAgg* p2) const {
        return (!strcmp(((NNPlainPAO*)p1)->key, ((NNPlainPAO*)p2)->key));
    }

	size_t createPAO(Token* t, PartialAgg** p) const {
        if (t == NULL)
            p[0] = new NNPlainPAO(NULL);
        else	
            assert(false && "Not defined");
        return 1;
    }

    bool destroyPAO(PartialAgg* p) const {
        NNPlainPAO* wp = (NNPlainPAO*)p;
        delete wp;
        return true;
    }

	bool merge(PartialAgg* p, PartialAgg* mg) const {
        NNPlainPAO* wp = (NNPlainPAO*)p;
        NNPlainPAO* wmp = (NNPlainPAO*)mg;
        if (wp->hamming_dist > wmp->hamming_dist)
            wp->hamming_dist = wmp->hamming_dist;
        return true;
    }

    inline uint32_t getSerializedSize(PartialAgg* p) const {
        return sizeof(NNPlainPAO);
    }

    inline bool serialize(PartialAgg* p,
            std::string* output) const {
        assert(false && "Not implemented");
        return true;
    }

    inline bool serialize(PartialAgg* p,
            char* output, size_t size) const {
        memcpy(output, (void*)p, sizeof(NNPlainPAO));
        return true;
    }
    inline bool deserialize(PartialAgg* p,
            const std::string& input) const {
        assert(false && "Not implemented");
        return true;
    }

    inline bool deserialize(PartialAgg* p,
            const char* input, size_t size) const {
        memcpy((void*)p, (void*)input, sizeof(NNPlainPAO));
        return true;
    }

  private:
    size_t dividePAO(const PartialAgg& p, PartialAgg** pl) const {}
};
