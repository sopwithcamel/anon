#include "PartialAgg.h"
#include <boost/serialization/binary_object.hpp> 

class WCBoostPAO : public PartialAgg {
    friend class WCBoostOperations;
  public:
	WCBoostPAO() {}
	~WCBoostPAO() {}
  private:
    char* key;
    uint32_t count;
};

class WCBoostOperations : public Operations {
  public:
    Operations::SerializationMethod getSerializationMethod() const {
        return Operations::BOOST;
    }

    const char* getKey(PartialAgg* p) const {
        WCBoostPAO* wp = (WCBoostPAO*)p;
        return wp->key;
    }

    bool setKey(PartialAgg* p, char* k) const {
        WCBoostPAO* wp = (WCBoostPAO*)p;
        wp->key = (char*)malloc(strlen(k) + 1);
        strcpy(wp->key, k);
        return true;
    }

    void* getValue(PartialAgg* p) const {
        WCBoostPAO* wp = (WCBoostPAO*)p;
        return (void*)(intptr_t)(wp->count);
    }

    void setValue(PartialAgg*p, void* v) const {
        WCBoostPAO* wp = (WCBoostPAO*)p;
        wp->count = (intptr_t)v;
    }

    bool sameKey(PartialAgg* p1, PartialAgg* p2) const {
        return (!strcmp(((WCBoostPAO*)p1)->key, ((WCBoostPAO*)p2)->key));
    }

	size_t createPAO(Token* t, PartialAgg** p) const {
        if (t == NULL) {
            p[0] = new WCBoostPAO();
        }
        return 1;
    }

    bool destroyPAO(PartialAgg* p) const {
        WCBoostPAO* wp = (WCBoostPAO*)p;
        delete wp;
        return true;
    }

	bool merge(PartialAgg* p, PartialAgg* mg) const {
        WCBoostPAO* wp = (WCBoostPAO*)p;
        WCBoostPAO* wmp = (WCBoostPAO*)mg;
        wp->count += wmp->count;
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
