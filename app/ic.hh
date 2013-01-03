#include "PartialAgg.h"

#define HASHLEN      20
#define IDLEN       16
#define NEIGH       4

class ICPlainPAO : public PartialAgg
{
    friend class ICPlainOperations;
  public:
	ICPlainPAO(char* wrd) : num_neighbors(0) {
        memset(this, 0, HASHLEN + NEIGH * IDLEN + HASHLEN * NEIGH);
        if (wrd) {
            strncpy(key, wrd, HASHLEN - 1);
        }
    }
	~ICPlainPAO() {
    }
  private:
    char key[HASHLEN];
    char neigh[NEIGH][IDLEN];
    char hashes[NEIGH][HASHLEN + 4];
    uint32_t num_neighbors;
};

typedef struct {
    char* img;
    char* hash;
} img_hash_pair_t;

class ICPlainOperations : public Operations {
  public:
    Operations::SerializationMethod getSerializationMethod() const {
        return Operations::HAND;
    }

    const char* getKey(PartialAgg* p) const {
        return ((ICPlainPAO*)p)->key;
    }

    bool setKey(PartialAgg* p, char* k) const {
        ICPlainPAO* wp = (ICPlainPAO*)p;
        strncpy(wp->key, k, HASHLEN - 1);
        return true;
    }

    void* getValue(PartialAgg* p) const {
        assert(false && "Not implemented yet");
    }

    void setValue(PartialAgg* p, void* v) const {
        ICPlainPAO* wp = (ICPlainPAO*)p;
        img_hash_pair_t* ih = (img_hash_pair_t*)v;
        strncpy(wp->neigh[0], ih->img, IDLEN - 1);
        strncpy(wp->hashes[0], ih->hash, HASHLEN);
    }

    bool sameKey(PartialAgg* p1, PartialAgg* p2) const {
        return (!strcmp(((ICPlainPAO*)p1)->key, ((ICPlainPAO*)p2)->key));
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
        uint32_t n = wp->num_neighbors;
        uint32_t m = wmp->num_neighbors;
        if (n + m > NEIGH) {
            fprintf(stderr, "*");
            m = NEIGH - n;
        }
            
        for (uint32_t i = 0; i < m; ++i) {
            strncpy(wp->neigh[n + i], wmp->neigh[i], IDLEN - 1);
            strncpy(wp->hashes[n + i], wmp->hashes[i], HASHLEN);
        }
        return true;
    }

    inline uint32_t getSerializedSize(PartialAgg* p) const {
        return sizeof(ICPlainPAO);
    }

    inline bool serialize(PartialAgg* p,
            std::string* output) const {
        assert(false && "Not implemented");
        return true;
    }

    inline bool serialize(PartialAgg* p,
            char* output, size_t size) const {
        memcpy(output, (void*)p, sizeof(ICPlainPAO));
        return true;
    }
    inline bool deserialize(PartialAgg* p,
            const std::string& input) const {
        return true;
    }

    inline bool deserialize(PartialAgg* p,
            const char* input, size_t size) const {
        memcpy((void*)p, (void*)input, sizeof(ICPlainPAO));
        return true;
    }

  private:
    size_t dividePAO(const PartialAgg& p, PartialAgg** pl) const {}
};
