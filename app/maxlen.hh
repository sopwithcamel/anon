#include "PartialAgg.h"

#define KEYLEN      60

class MaxLenPlainPAO : public PartialAgg
{
    friend class MaxLenPlainOperations;
  public:
	MaxLenPlainPAO(char* wrd) {
        memset(key, 0, KEYLEN);
        if (wrd) {
            strncpy(key, wrd, KEYLEN - 1);
            length = 1;
        } else {
            length = 1;
        }    
    }
	~MaxLenPlainPAO() {
    }
  private:
    char key[KEYLEN];
    uint32_t length;
};

class MaxLenPlainOperations : public Operations {
  public:
    Operations::SerializationMethod getSerializationMethod() const {
        return Operations::HAND;
    }

    const char* getKey(PartialAgg* p) const {
        return ((MaxLenPlainPAO*)p)->key;
    }

    bool setKey(PartialAgg* p, char* k) const {
        MaxLenPlainPAO* wp = (MaxLenPlainPAO*)p;
        memset(wp->key, 0, KEYLEN);
        strncpy(wp->key, k, KEYLEN - 1);
        return true;
    }

    void* getValue(PartialAgg* p) const {
        MaxLenPlainPAO* wp = (MaxLenPlainPAO*)p;
        return (void*)(intptr_t)(wp->length);
    }

    void setValue(PartialAgg* p, void* v) const {
        MaxLenPlainPAO* wp = (MaxLenPlainPAO*)p;
        wp->length = (intptr_t)v;
    }

    bool sameKey(PartialAgg* p1, PartialAgg* p2) const {
        return (!strcmp(((MaxLenPlainPAO*)p1)->key, ((MaxLenPlainPAO*)p2)->key));
    }

	size_t createPAO(Token* t, PartialAgg** p) const {
        MaxLenPlainPAO* new_pao;
        if (t == NULL)
            new_pao = new MaxLenPlainPAO(NULL);
        else	
            new_pao = new MaxLenPlainPAO((char*)t->tokens[0]);
        p[0] = new_pao;	
        return 1;
    }

    bool destroyPAO(PartialAgg* p) const {
        MaxLenPlainPAO* wp = (MaxLenPlainPAO*)p;
        delete wp;
        return true;
    }

	bool merge(PartialAgg* p, PartialAgg* mg) const {
        ((MaxLenPlainPAO*)p)->length = std::max(((MaxLenPlainPAO*)p)->length,
                ((MaxLenPlainPAO*)mg)->length);
        return true;
    }

    inline uint32_t getSerializedSize(PartialAgg* p) const {
        return sizeof(MaxLenPlainPAO);
    }

    inline bool serialize(PartialAgg* p,
            std::string* output) const {
        return true;
    }

    inline bool serialize(PartialAgg* p,
            char* output, size_t size) const {
        memcpy(output, (void*)p, sizeof(MaxLenPlainPAO));
        return true;
    }
    inline bool deserialize(PartialAgg* p,
            const std::string& input) const {
        return true;
    }

    inline bool deserialize(PartialAgg* p,
            const char* input, size_t size) const {
        memcpy((void*)p, (void*)input, sizeof(MaxLenPlainPAO));
        return true;
    }

  private:
    size_t dividePAO(const PartialAgg& p, PartialAgg** pl) const {}
};
