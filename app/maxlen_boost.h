#include "PartialAgg.h"
#include <boost/serialization/binary_object.hpp> 

class MaxLenBoostPAO : public PartialAgg {
    friend class MaxLenBoostOperations;
  public:
	MaxLenBoostPAO() {}
	~MaxLenBoostPAO() {}
  private:
    char* key;
    uint32_t length;
};

class MaxLenBoostOperations : public Operations {
  public:
    Operations::SerializationMethod getSerializationMethod() const {
        return Operations::BOOST;
    }

    const char* getKey(PartialAgg* p) const {
        MaxLenBoostPAO* wp = (MaxLenBoostPAO*)p;
        return wp->key;
    }

    bool setKey(PartialAgg* p, char* k) const {
        MaxLenBoostPAO* wp = (MaxLenBoostPAO*)p;
        wp->key = (char*)malloc(strlen(k) + 1);
        strcpy(wp->key, k);
        return true;
    }

    void* getValue(PartialAgg* p) const {
        MaxLenBoostPAO* wp = (MaxLenBoostPAO*)p;
        return (void*)(intptr_t)(wp->length);
    }

    void setValue(PartialAgg*p, void* v) const {
        MaxLenBoostPAO* wp = (MaxLenBoostPAO*)p;
        wp->length = (intptr_t)v;
    }

    bool sameKey(PartialAgg* p1, PartialAgg* p2) const {
        return (!strcmp(((MaxLenBoostPAO*)p1)->key, ((MaxLenBoostPAO*)p2)->key));
    }

	size_t createPAO(Token* t, PartialAgg** p) const {
        if (t == NULL) {
            p[0] = new MaxLenBoostPAO();
        }
        return 1;
    }

    bool destroyPAO(PartialAgg* p) const {
        MaxLenBoostPAO* wp = (MaxLenBoostPAO*)p;
        delete wp;
        return true;
    }

	bool merge(PartialAgg* p, PartialAgg* mg) const {
        MaxLenBoostPAO* wp = (MaxLenBoostPAO*)p;
        MaxLenBoostPAO* wmp = (MaxLenBoostPAO*)mg;
        wp->length = std::max(wp->length, wmp->length);
        return true;
    }

    inline uint32_t getSerializedSize(PartialAgg* p) const {
        assert(false && "Not implemented yet");
        return 0;
    }

    inline bool serialize(PartialAgg* p, std::string* output) const {
        assert(false && "Not implemented yet");
        return false;
    }

    inline bool serialize(PartialAgg* p, char* output, size_t size) const {
        assert(false && "Not implemented yet");
        return false;
    }

    inline bool deserialize(PartialAgg* p, const std::string& input) const {
        assert(false && "Not implemented yet");
        return false;
    }

    inline bool deserialize(PartialAgg* p, const char* input,
            size_t size) const {
        assert(false && "Not implemented yet");
        return false;
    }

  private:
    size_t dividePAO(const PartialAgg& p, PartialAgg** p_list) const {}
};
