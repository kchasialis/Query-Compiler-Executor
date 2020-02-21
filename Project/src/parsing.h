#ifndef PARSING_H
#define PARSING_H

#define _GNU_SOURCE
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "structs.h"
#include "queries.h"
#include "filter.h"

query *parser();

#endif