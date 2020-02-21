#ifndef FILTER_H
#define FILTER_H

#include "queries.h"
#include "utilities.h"
#include "queries.h"

mid_result** execute_filter(predicate *pred, uint32_t *relations, metadata *metadata_arr, mid_result **mid_results_array);

#endif