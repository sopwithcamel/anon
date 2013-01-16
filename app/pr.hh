#include "PartialAgg.h"

#define KEYLEN      10

class PageRankPAO : public PartialAgg
{
    friend class PageRankOperations;
  public:
    struct PRValue {
        float rank;
        char* neigh;
        uint32_t num_neigh;
    };
	PageRankPAO(char* wrd) {
        memset(key, 0, KEYLEN);
        pr.rank = 0;
        pr.neigh = NULL;
        pr.num_neigh = 0;
    }
	~PageRankPAO() {
    }
  private:
    char key[KEYLEN];
    PRValue pr;
};

class PageRankOperations : public Operations {
  public:
    Operations::SerializationMethod getSerializationMethod() const {
        return Operations::HAND;
    }

    const char* getKey(PartialAgg* p) const {
        return ((PageRankPAO*)p)->key;
    }

    bool setKey(PartialAgg* p, char* k) const {
        PageRankPAO* wp = (PageRankPAO*)p;
        memset(wp->key, 0, KEYLEN);
        strncpy(wp->key, k, KEYLEN - 1);
        return true;
    }

    void* getValue(PartialAgg* p) const {
        PageRankPAO* wp = (PageRankPAO*)p;
        return &(wp->pr);
    }

    void setValue(PartialAgg* p, void* v) const {
        PageRankPAO* wp = (PageRankPAO*)p;
        memcpy(&(wp->pr), v, sizeof(PageRankPAO::PRValue));
    }

    bool sameKey(PartialAgg* p1, PartialAgg* p2) const {
        return (!strcmp(((PageRankPAO*)p1)->key, ((PageRankPAO*)p2)->key));
    }

	size_t createPAO(Token* t, PartialAgg** p) const {
        PageRankPAO* new_pao;
        if (t == NULL)
            p[0] = new PageRankPAO(NULL);
        else	
            assert(false && "Not handled");
        return 1;
    }

    bool destroyPAO(PartialAgg* p) const {
        PageRankPAO* wp = (PageRankPAO*)p;
        delete wp;
        return true;
    }

	bool merge(PartialAgg* p, PartialAgg* mg) const {
        PageRankPAO* pp = (PageRankPAO*)p;
        PageRankPAO* pmg = (PageRankPAO*)mg;
        pp->pr.rank += pmg->pr.rank;
        if (pmg->pr.neigh) {
            pp->pr.neigh = pmg->pr.neigh;
            pp->pr.num_neigh = pmg->pr.num_neigh;
        }        
        return true;
    }

    inline uint32_t getSerializedSize(PartialAgg* p) const {
        PageRankPAO* wp = (PageRankPAO*)p;
        return KEYLEN + sizeof(PageRankPAO::PRValue) + 1;
    }

    inline bool serialize(PartialAgg* p,
            std::string* output) const {
        return true;
    }

    inline bool serialize(PartialAgg* p,
            char* output, size_t size) const {
        PageRankPAO* wp = (PageRankPAO*)p;
        memcpy(output, &wp, sizeof(uint32_t));
        strcpy(&output[sizeof(uint32_t)], wp->key);
        return true;
    }
    inline bool deserialize(PartialAgg* p,
            const std::string& input) const {
        return true;
    }

    inline bool deserialize(PartialAgg* p,
            const char* input, size_t size) const {
        PageRankPAO* wp = (PageRankPAO*)p;
        memcpy(&wp->pr, input, sizeof(uint32_t));
        strcpy(wp->key, &input[sizeof(uint32_t)]);
        return true;
    }

  private:
    size_t dividePAO(const PartialAgg& p, PartialAgg** pl) const {}
};
