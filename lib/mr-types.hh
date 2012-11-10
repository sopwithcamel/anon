/* Metis
 * Yandong Mao, Robert Morris, Frans Kaashoek
 * Copyright (c) 2012 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, subject to the conditions listed
 * in the Metis LICENSE file. These conditions include: you must preserve this
 * copyright notice, and you cannot mention the copyright holders in
 * advertising related to the Software without their permission.  The Software
 * is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This notice is a
 * summary of the Metis LICENSE file; the license in that file is legally
 * binding.
 */
#ifndef MR_TYPES_HH_
#define MR_TYPES_HH_

#include <sys/mman.h>
#include <stdlib.h>
#include <inttypes.h>
#include <assert.h>
#include <string.h>
#include "array.hh"

struct split_t {
    split_t();
    ~split_t();
    static size_t kBufferSize;
    char* data;
    size_t split_start_offset;
    size_t split_end_offset;
    size_t chunk_start_offset;
    size_t chunk_end_offset;
};

enum task_type_t {
    MAP,
    FINALIZE,
};

/* suggested number of map tasks per core. */
enum { def_nsplits_per_core = 16 };

#endif
