#include "PartialAgg.h"

#define IDLEN   16

class NNPlainPAO : public PartialAgg
{
    friend class NNPlainOperations;
  public:
    struct NNValue {
        char nn[IDLEN];
        uint32_t hamming_dist;
    };

	NNPlainPAO(char* wrd) {
        memset(key, 0, IDLEN);
        if (wrd) {
            strncpy(key, wrd, IDLEN - 1);
        }
    }
	~NNPlainPAO() {
    }
  private:
    char key[IDLEN];
    NNValue value_;
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
        memset(wp->key, 0, IDLEN);
        strncpy(wp->key, k, IDLEN - 1);
        return true;
    }

    void* getValue(PartialAgg* p) const {
        NNPlainPAO* wp = (NNPlainPAO*)p;
        return &(wp->value_);
    }

    void setValue(PartialAgg* p, void* v) const {
        NNPlainPAO* wp = (NNPlainPAO*)p;
        NNPlainPAO::NNValue* nnv = (NNPlainPAO::NNValue*)v;
        strncpy(wp->value_.nn, nnv->nn, IDLEN - 1);
        wp->value_.hamming_dist = nnv->hamming_dist;
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
        if (wp->value_.hamming_dist > wmp->value_.hamming_dist)
            wp->value_.hamming_dist = wmp->value_.hamming_dist;
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
};
