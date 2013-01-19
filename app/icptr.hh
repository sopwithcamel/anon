#include "PartialAgg.h"

#define HASHLEN      20
#define IDLEN       16
#define NEIGH       10

typedef struct {
    char* img;
    char* hash;
} img_hash_pair_t;

struct Neighbor {
    char* img;
    char* hash;
};

struct ICValue {
    uint32_t num_neighbors_;
    Neighbor neigh_[NEIGH];
};

class ICPtrPAO : public PartialAgg
{
    friend class ICPtrOperations;
  public:
	ICPtrPAO(char* wrd) {
        memset(this, 0,
                sizeof(key_) + sizeof(uint32_t) +
                NEIGH * sizeof(Neighbor));
        if (wrd) {
            strncpy(key_, wrd, HASHLEN - 1);
        }
    }
	~ICPtrPAO() {
        for (uint32_t i = 0; i < value_.num_neighbors_; ++i) {
            free(value_.neigh_[i].img);
            free(value_.neigh_[i].hash);
        }
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

class ICPtrOperations : public Operations {
  public:
    Operations::SerializationMethod getSerializationMethod() const {
        return Operations::HAND;
    }

    const char* getKey(PartialAgg* p) const {
        return ((ICPtrPAO*)p)->key_;
    }

    bool setKey(PartialAgg* p, char* k) const {
        ICPtrPAO* wp = (ICPtrPAO*)p;
        strncpy(wp->key_, k, HASHLEN - 1);
        return true;
    }

    void* getValue(PartialAgg* p) const {
        ICPtrPAO* icp = (ICPtrPAO*)p;
        return &(icp->value_);
    }

    void setValue(PartialAgg* p, void* v) const {
        ICPtrPAO* wp = (ICPtrPAO*)p;
        ICValue* icv = (ICValue*)v;
        wp->value_.num_neighbors_ = icv->num_neighbors_;
        for (uint32_t i = 0; i < icv->num_neighbors_; ++i) {
            wp->value_.neigh_[i].img = (char*)malloc(IDLEN);
            strncpy(wp->value_.neigh_[i].img, icv->neigh_[i].img, IDLEN - 1);
            wp->value_.neigh_[i].hash = (char*)malloc(HASHLEN);
            strncpy(wp->value_.neigh_[i].hash, icv->neigh_[i].hash,
                    HASHLEN - 1);
        }
    }

    bool sameKey(PartialAgg* p1, PartialAgg* p2) const {
        return (!strcmp(((ICPtrPAO*)p1)->key_, ((ICPtrPAO*)p2)->key_));
    }

	size_t createPAO(Token* t, PartialAgg** p) const {
        if (t == NULL)
            p[0] = new ICPtrPAO(NULL);
        else	
            assert(false && "Shouldn't come here");
        return 1;
    }

    bool destroyPAO(PartialAgg* p) const {
        ICPtrPAO* wp = (ICPtrPAO*)p;
        delete wp;
        return true;
    }

	bool merge(PartialAgg* p, PartialAgg* mg) const {
        ICValue* pv = &((ICPtrPAO*)p)->value_;
        ICValue* mv = &((ICPtrPAO*)mg)->value_;
        uint32_t n = pv->num_neighbors_;
        uint32_t m = mv->num_neighbors_;
        if (n + m > NEIGH) {
            m = NEIGH - n;
        }
            
        for (uint32_t i = 0; i < m; ++i) {
            pv->neigh_[n + i] = mv->neigh_[i];
            mv->neigh_[i].img = NULL;
            mv->neigh_[i].hash = NULL;
        }
        pv->num_neighbors_ += m;
        mv->num_neighbors_ = 0;
        return true;
    }

    inline uint32_t getSerializedSize(PartialAgg* p) const {
        assert(false && "Not implemented");
        return true;
    }

    inline bool serialize(PartialAgg* p,
            std::string* output) const {
        assert(false && "Not implemented");
        return true;
    }

    inline bool serialize(PartialAgg* p,
            char* output, size_t size) const {
        assert(false && "Not implemented");
        return true;
    }
    inline bool deserialize(PartialAgg* p,
            const std::string& input) const {
        return true;
    }

    inline bool deserialize(PartialAgg* p,
            const char* input, size_t size) const {
        assert(false && "Not implemented");
        return true;
    }

  private:
    size_t dividePAO(const PartialAgg& p, PartialAgg** pl) const {}
};
