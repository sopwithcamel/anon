#include "wc_plain.h"
#include <string.h>

WCPlainPAO::WCPlainPAO(char* wrd) {
    memset(key, 0, KEYLEN);
    if (wrd) {
        strcpy(key, wrd);
        count = 1;
    } else {
        count = 1;
    }    
}

WCPlainPAO::~WCPlainPAO() {
}

Operations::SerializationMethod WCPlainOperations::getSerializationMethod() const {
    return Operations::HAND;
}

const char* WCPlainOperations::getKey(PartialAgg* p) const {
    return ((WCPlainPAO*)p)->key;
}

bool WCPlainOperations::setKey(PartialAgg* p, char* k) const
{
	WCPlainPAO* wp = (WCPlainPAO*)p;
    strcpy(wp->key, k);
    return true;
}

void* WCPlainOperations::getValue(PartialAgg* p) const {
	WCPlainPAO* wp = (WCPlainPAO*)p;
    return (void*)(intptr_t)(wp->count);
}

void WCPlainOperations::setValue(PartialAgg* p, void* v) const {
	WCPlainPAO* wp = (WCPlainPAO*)p;
    wp->count = (intptr_t)v;
}

bool WCPlainOperations::sameKey(PartialAgg* p1, PartialAgg* p2) const
{
    return (!strcmp(((WCPlainPAO*)p1)->key, ((WCPlainPAO*)p2)->key));
}

size_t WCPlainOperations::createPAO(Token* t, PartialAgg** p) const
{
	WCPlainPAO* new_pao;
	if (t == NULL)
		new_pao = new WCPlainPAO(NULL);
	else	
		new_pao = new WCPlainPAO((char*)t->tokens[0]);
	p[0] = new_pao;	
	return 1;
}

bool WCPlainOperations::destroyPAO(PartialAgg* p) const
{
    WCPlainPAO* wp = (WCPlainPAO*)p;
	delete wp;
    return true;
}

inline uint32_t WCPlainOperations::getSerializedSize(PartialAgg* p) const {
    return sizeof(WCPlainPAO);
}

bool WCPlainOperations::merge(PartialAgg* v, PartialAgg* mg) const
{
	((WCPlainPAO*)v)->count += ((WCPlainPAO*)mg)->count;
    return true;
}

inline bool WCPlainOperations::serialize(PartialAgg* p,
        std::string* output) const {
    return true;
}

inline bool WCPlainOperations::serialize(PartialAgg* p,
        char* output, size_t size) const {
    memcpy(output, (void*)p, sizeof(WCPlainPAO));
    return true;
}

inline bool WCPlainOperations::deserialize(PartialAgg* p,
        const std::string& input) const
{
    return true;
}

inline bool WCPlainOperations::deserialize(PartialAgg* p,
        const char* input, size_t size) const
{
    memcpy((void*)p, (void*)input, sizeof(WCPlainPAO));
    return true;
}

REGISTER(WCPlainOperations);
