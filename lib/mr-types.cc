#include "mr-types.hh"

size_t split_t::kBufferSize = 67108864;

split_t::split_t() : 
        data(NULL),
        split_start_offset(0), split_end_offset(0),
        chunk_start_offset(0), chunk_end_offset(0) {
    bzero(this, sizeof(split_t));
    data = (char*)malloc(kBufferSize);
}

split_t::~split_t() {
    if (data)
        free(data);
}
