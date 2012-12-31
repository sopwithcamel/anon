#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include "PartialAgg.h"
#include "wcproto.pb.h"

class WCProtoPAO : public PartialAgg {
    friend class WCProtoOperations;
  public:
	WCProtoPAO() {
        pb.set_count(1);
    }
	~WCProtoPAO() {
    }
  private:
    wordcount::pao pb;
};

class WCProtoOperations : public Operations {
  public:
    Operations::SerializationMethod getSerializationMethod() const {
        return Operations::PROTOBUF;
    }

    const char* getKey(PartialAgg* p) const {
        WCProtoPAO* wp = (WCProtoPAO*)p;
        return wp->pb.key().c_str();
    }

    bool setKey(PartialAgg* p, char* k) const {
        WCProtoPAO* wp = (WCProtoPAO*)p;
        wp->pb.set_key(k);
        return true;
    }

    void* getValue(PartialAgg* p) const {
        WCProtoPAO* wp = (WCProtoPAO*)p;
        return (void*)(intptr_t)(wp->pb.count());
    }

    void setValue(PartialAgg*p, void* v) const {
        WCProtoPAO* wp = (WCProtoPAO*)p;
        wp->pb.set_count((intptr_t)v);
    }

    bool sameKey(PartialAgg* p1, PartialAgg* p2) const {
        WCProtoPAO* wp1 = (WCProtoPAO*)p1;
        WCProtoPAO* wp2 = (WCProtoPAO*)p2;
        return (!wp1->pb.key().compare(wp2->pb.key()));
    }

	size_t createPAO(Token* t, PartialAgg** p) const {
        if (t == NULL) {
            p[0] = new WCProtoPAO();
        } else {
            WCProtoPAO* wp = (WCProtoPAO*)(p[0]);
            wp->pb.set_key((char*)t->tokens[0]);
            wp->pb.set_count(1);
        }
        return 1;
    }

    bool destroyPAO(PartialAgg* p) const {
        WCProtoPAO* wp = (WCProtoPAO*)p;
        delete wp;
        return true;
    }

	bool merge(PartialAgg* p, PartialAgg* mg) const {
        WCProtoPAO* wp = (WCProtoPAO*)p;
        WCProtoPAO* wmp = (WCProtoPAO*)mg;
        wp->pb.set_count(wp->pb.count() + wmp->pb.count());
        return true;
    }

    inline uint32_t getSerializedSize(PartialAgg* p) const {
        WCProtoPAO* wp = (WCProtoPAO*)p;
        return wp->pb.ByteSize();
    }

    inline bool serialize(PartialAgg* p, std::string* output) const {
        WCProtoPAO* wp = (WCProtoPAO*)p;
        wp->pb.SerializeToString(output);
        return true;
    }

    inline bool serialize(PartialAgg* p, char* output, size_t size) const {
        WCProtoPAO* wp = (WCProtoPAO*)p;
        memset((void*)output, 0, size);
        wp->pb.SerializeToArray(output, size);
        return true;
    }

    inline bool deserialize(PartialAgg* p, const std::string& input) const {
        WCProtoPAO* wp = (WCProtoPAO*)p;
        return wp->pb.ParseFromString(input);
    }

    inline bool deserialize(PartialAgg* p, const char* input,
            size_t size) const {
        WCProtoPAO* wp = (WCProtoPAO*)p;
        return wp->pb.ParseFromArray(input, size);
    }

  private:
    size_t dividePAO(const PartialAgg& p, PartialAgg** p_list) const {}
};
