#include "PartialAgg.h"
#include <vector>

#define HASHLEN      20
#define IDLEN       16

typedef struct {
    char* img;
    char* hash;
} img_hash_pair_t;

struct Neighbor {
    char* img;
    char* hash;
};

struct ICValue {
    std::vector<Neighbor> neigh_;
};

class ICPtrPAO : public PartialAgg
{
    friend class ICPtrOperations;
  public:
	ICPtrPAO(char* wrd) {
        memset(this, 0,
                sizeof(key_) + sizeof(uint32_t));
        value_.neigh_.clear();
        if (wrd) {
            strncpy(key_, wrd, HASHLEN - 1);
        }
    }
	~ICPtrPAO() {
        for (uint32_t i = 0; i < value_.neigh_.size(); ++i) {
            free(value_.neigh_[i].img);
            free(value_.neigh_[i].hash);
        }
        value_.neigh_.clear();
    }
    uint32_t num_neighbors() const {
        return value_.neigh_.size();
    }
    img_hash_pair_t neighbor(uint32_t i) const {
        assert(i < value_.neigh_.size());
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
        for (uint32_t i = 0; i < wp->value_.neigh_.size(); ++i) {
            free(wp->value_.neigh_[i].img);
            free(wp->value_.neigh_[i].hash);
        }
        wp->value_.neigh_.clear();

        ICValue* icv = (ICValue*)v;
        for (uint32_t i = 0; i < icv->neigh_.size(); ++i) {
            Neighbor n;
            n.img = (char*)malloc(IDLEN);
            strncpy(n.img, icv->neigh_[i].img, IDLEN - 1);
            n.hash = (char*)malloc(HASHLEN);
            strncpy(n.hash, icv->neigh_[i].hash,
                    HASHLEN - 1);
            wp->value_.neigh_.push_back(n);
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
        uint32_t n = pv->neigh_.size();
        uint32_t m = mv->neigh_.size();
            
        for (uint32_t i = 0; i < m; ++i) {
            Neighbor n = mv->neigh_[i];
            pv->neigh_.push_back(n);
            mv->neigh_[i].img = NULL;
            mv->neigh_[i].hash = NULL;
        }
        mv->neigh_.clear();
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
