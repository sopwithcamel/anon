#include "PartialAgg.h"

#define HASHLEN      20
#define IDLEN       16
#define NEIGH       10

typedef struct {
    char* img;
    char* hash;
} img_hash_pair_t;

struct Neighbor {
    char img[IDLEN];
    char hash[HASHLEN];
};

struct ICValue {
    uint32_t num_neighbors_;
    Neighbor neigh_[NEIGH];
};


class ICPlainPAO : public PartialAgg
{
    friend class ICPlainOperations;
  public:
	ICPlainPAO(char* wrd) {
        memset(this, 0,
                sizeof(key_) + sizeof(uint32_t) +
                NEIGH * sizeof(Neighbor));
        if (wrd) {
            strncpy(key_, wrd, HASHLEN - 1);
        }
    }
	~ICPlainPAO() {
    }
    uint32_t num_neighbors() const {
        return value_.num_neighbors_;
    }
    img_hash_pair_t neighbor(uint32_t i) const {
        assert(i < value_.num_neighbors_);
        img_hash_pair_t ih;
        ih.img = (char*)value_.neigh_[i].img;
        ih.hash = (char*)value_.neigh_[i].hash;
        return ih;
    }
  private:
    char key_[HASHLEN];
    ICValue value_;
};

class ICPlainOperations : public Operations {
  public:
    Operations::SerializationMethod getSerializationMethod() const {
        return Operations::HAND;
    }

    const char* getKey(PartialAgg* p) const {
        return ((ICPlainPAO*)p)->key_;
    }

    bool setKey(PartialAgg* p, char* k) const {
        ICPlainPAO* wp = (ICPlainPAO*)p;
        strncpy(wp->key_, k, HASHLEN - 1);
        return true;
    }

    void* getValue(PartialAgg* p) const {
        ICPlainPAO* icp = (ICPlainPAO*)p;
        return &(icp->value_);
    }

    void setValue(PartialAgg* p, void* v) const {
        ICPlainPAO* wp = (ICPlainPAO*)p;
        ICValue* icv = (ICValue*)v;
        wp->value_.num_neighbors_ = icv->num_neighbors_;
        for (uint32_t i = 0; i < icv->num_neighbors_; ++i) {
            wp->value_.neigh_[i] = icv->neigh_[i];
        }
    }

    bool sameKey(PartialAgg* p1, PartialAgg* p2) const {
        return (!strcmp(((ICPlainPAO*)p1)->key_, ((ICPlainPAO*)p2)->key_));
    }

	size_t createPAO(Token* t, PartialAgg** p) const {
        if (t == NULL)
            p[0] = new ICPlainPAO(NULL);
        else	
            assert(false && "Shouldn't come here");
        return 1;
    }

    bool destroyPAO(PartialAgg* p) const {
        ICPlainPAO* wp = (ICPlainPAO*)p;
        delete wp;
        return true;
    }

	bool merge(PartialAgg* p, PartialAgg* mg) const {
        ICValue* pv = &((ICPlainPAO*)p)->value_;
        ICValue* mv = &((ICPlainPAO*)mg)->value_;
        uint32_t n = pv->num_neighbors_;
        uint32_t m = mv->num_neighbors_;
        if (n + m > NEIGH) {
            m = NEIGH - n;
        }
            
        for (uint32_t i = 0; i < m; ++i) {
            pv->neigh_[n + i] = mv->neigh_[i];
        }
        pv->num_neighbors_ += m;
        return true;
    }

    inline uint32_t getSerializedSize(PartialAgg* p) const {
        ICPlainPAO* icp = (ICPlainPAO*)p;
        return sizeof(icp->key_) + sizeof(uint32_t) +
                icp->value_.num_neighbors_ *
                sizeof(Neighbor);
    }

    inline bool serialize(PartialAgg* p,
            std::string* output) const {
        assert(false && "Not implemented");
        return true;
    }

    inline bool serialize(PartialAgg* p,
            char* output, size_t size) const {
        ICPlainPAO* icp = (ICPlainPAO*)p;
        uint32_t off = sizeof(icp->key_) + sizeof(uint32_t);
        uint32_t neigh_size = sizeof(Neighbor) *
                icp->value_.num_neighbors_;
        memcpy(output, (void*)icp, off);
        memcpy(output + off, icp->value_.neigh_, neigh_size);
        return true;
    }
    inline bool deserialize(PartialAgg* p,
            const std::string& input) const {
        return true;
    }

    inline bool deserialize(PartialAgg* p,
            const char* input, size_t size) const {
        ICPlainPAO* icp = (ICPlainPAO*)p;
        uint32_t off = sizeof(icp->key_) + sizeof(uint32_t);
        memcpy(icp, (void*)input, off);
        uint32_t neigh_size = sizeof(Neighbor) *
                icp->value_.num_neighbors_;
        memcpy(icp->value_.neigh_, input + off, neigh_size);
        return true;
    }
};
