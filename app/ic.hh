#include "PartialAgg.h"

#define HASHLEN      20
#define IDLEN       16
#define NEIGH       10

typedef struct {
    char* img;
    char* hash;
} img_hash_pair_t;

class ICPlainPAO : public PartialAgg
{
    friend class ICPlainOperations;
  public:
    struct Neighbor {
        char img[IDLEN];
        char hash[HASHLEN];
    };
	ICPlainPAO(char* wrd) : num_neighbors_(0) {
        memset(this, 0,
                sizeof(key_) + sizeof(num_neighbors_) +
                NEIGH * sizeof(Neighbor));
        if (wrd) {
            strncpy(key_, wrd, HASHLEN - 1);
        }
    }
	~ICPlainPAO() {
    }
    uint32_t num_neighbors() const {
        return num_neighbors_;
    }
    img_hash_pair_t neighbor(uint32_t i) const {
        assert(i < num_neighbors_);
        img_hash_pair_t ih;
        ih.img = (char*)neigh_[i].img;
        ih.hash = (char*)neigh_[i].hash;
        return ih;
    }
  private:
    char key_[HASHLEN];
    uint32_t num_neighbors_;
    Neighbor neigh_[NEIGH];
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
        assert(false && "Not implemented yet");
    }

    void setValue(PartialAgg* p, void* v) const {
        ICPlainPAO* wp = (ICPlainPAO*)p;
        img_hash_pair_t* ih = (img_hash_pair_t*)v;
        strncpy(wp->neigh_[0].img, ih->img, IDLEN - 1);
        strncpy(wp->neigh_[0].hash, ih->hash, HASHLEN - 1);
        wp->num_neighbors_ = 1;
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
        ICPlainPAO* wp = (ICPlainPAO*)p;
        ICPlainPAO* wmp = (ICPlainPAO*)mg;
        uint32_t n = wp->num_neighbors_;
        uint32_t m = wmp->num_neighbors_;
        if (n + m > NEIGH) {
            m = NEIGH - n;
        }
            
        for (uint32_t i = 0; i < m; ++i) {
            wp->neigh_[n + i] = wmp->neigh_[i];
        }
        wp->num_neighbors_ += m;
        return true;
    }

    inline uint32_t getSerializedSize(PartialAgg* p) const {
        ICPlainPAO* icp = (ICPlainPAO*)p;
        return sizeof(icp->key_) + sizeof(uint32_t) + icp->num_neighbors_ *
                sizeof(ICPlainPAO::Neighbor);
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
        uint32_t neigh_size = sizeof(ICPlainPAO::Neighbor) *
                icp->num_neighbors_;
        memcpy(output, (void*)icp, off);
        memcpy(output + off, icp->neigh_, neigh_size);
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
        uint32_t neigh_size = sizeof(ICPlainPAO::Neighbor) *
                icp->num_neighbors_;
        memcpy(icp->neigh_, input + off, neigh_size);
        return true;
    }

  private:
    size_t dividePAO(const PartialAgg& p, PartialAgg** pl) const {}
};
