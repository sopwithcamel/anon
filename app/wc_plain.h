#include "PartialAgg.h"

#define KEYLEN      12

class WCPlainOperations : public Operations {
  public:
    Operations::SerializationMethod getSerializationMethod() const;
    const char* getKey(PartialAgg* p) const;
    bool setKey(PartialAgg* p, char* k) const;
    void* getValue(PartialAgg* p) const;
    void setValue(PartialAgg* p, void* v) const;
    bool sameKey(PartialAgg* p1, PartialAgg* p2) const;
	size_t createPAO(Token* t, PartialAgg** p) const;
    bool destroyPAO(PartialAgg* p) const;
	bool merge(PartialAgg* p, PartialAgg* mg) const;
    inline uint32_t getSerializedSize(PartialAgg* p) const;
    inline bool serialize(PartialAgg* p,
            std::string* output) const;
    inline bool serialize(PartialAgg* p,
            char* output, size_t size) const;
    inline bool deserialize(PartialAgg* p,
            const std::string& input) const;
    inline bool deserialize(PartialAgg* p,
            const char* input, size_t size) const;
  private:
    size_t dividePAO(const PartialAgg& p, PartialAgg** pl) const {}
};

class WCPlainPAO : public PartialAgg
{
    friend class WCPlainOperations;
  public:
	WCPlainPAO(char* wrd);
	~WCPlainPAO();
  private:
    char key[KEYLEN];
    uint32_t count;
};
