#include "PartialAgg.h"

#define KEYLEN      64

class WCPlainPAO : public PartialAgg
{
    friend class WCPlainOperations;
  public:
	WCPlainPAO(char* wrd) {
        memset(key, 0, KEYLEN);
        if (wrd) {
            strncpy(key, wrd, KEYLEN - 1);
            count = 1;
        } else {
            count = 1;
        }    
    }
	~WCPlainPAO() {
    }
  private:
    char key[KEYLEN];
    uint32_t count;
};

class WCPlainOperations : public Operations {
  public:
    Operations::SerializationMethod getSerializationMethod() const {
        return Operations::HAND;
    }

    const char* getKey(PartialAgg* p) const {
        return ((WCPlainPAO*)p)->key;
    }

    bool setKey(PartialAgg* p, char* k) const {
        WCPlainPAO* wp = (WCPlainPAO*)p;
        memset(wp->key, 0, KEYLEN);
        strncpy(wp->key, k, KEYLEN - 1);
        return true;
    }

    void* getValue(PartialAgg* p) const {
        WCPlainPAO* wp = (WCPlainPAO*)p;
        return (void*)(intptr_t)(wp->count);
    }

    void setValue(PartialAgg* p, void* v) const {
        WCPlainPAO* wp = (WCPlainPAO*)p;
        wp->count = (intptr_t)v;
    }

    bool sameKey(PartialAgg* p1, PartialAgg* p2) const {
        return (!strcmp(((WCPlainPAO*)p1)->key, ((WCPlainPAO*)p2)->key));
    }

	size_t createPAO(Token* t, PartialAgg** p) const {
        WCPlainPAO* new_pao;
        if (t == NULL)
            new_pao = new WCPlainPAO(NULL);
        else	
            new_pao = new WCPlainPAO((char*)t->tokens[0]);
        p[0] = new_pao;	
        return 1;
    }

    bool destroyPAO(PartialAgg* p) const {
        WCPlainPAO* wp = (WCPlainPAO*)p;
        delete wp;
        return true;
    }

	bool merge(PartialAgg* p, PartialAgg* mg) const {
        ((WCPlainPAO*)p)->count += ((WCPlainPAO*)mg)->count;
        return true;
    }

    inline uint32_t getSerializedSize(PartialAgg* p) const {
        WCPlainPAO* wp = (WCPlainPAO*)p;
        return strlen(wp->key) + sizeof(uint32_t) + 1;
    }

    inline bool serialize(PartialAgg* p,
            std::string* output) const {
        return true;
    }

    inline bool serialize(PartialAgg* p,
            char* output, size_t size) const {
        WCPlainPAO* wp = (WCPlainPAO*)p;
        memcpy(output, &wp->count, sizeof(uint32_t));
        strcpy(&output[sizeof(uint32_t)], wp->key);
        return true;
    }
    inline bool deserialize(PartialAgg* p,
            const std::string& input) const {
        return true;
    }

    inline bool deserialize(PartialAgg* p,
            const char* input, size_t size) const {
        WCPlainPAO* wp = (WCPlainPAO*)p;
        memcpy(&wp->count, input, sizeof(uint32_t));
        strcpy(wp->key, &input[sizeof(uint32_t)]);
        return true;
    }

  private:
    size_t dividePAO(const PartialAgg& p, PartialAgg** pl) const {}
};
