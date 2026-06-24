/*-------------------------------------------------------------------------
 *
 * opentenbase_tool.h
 *
 * Copyright (c) 2024-2024, Tencent Holdings Ltd.
 * 
 * src/bin/opentenbase_tool/opentenbase_tool.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef OPENTENBASE_TOOL_H
#define OPENTENBASE_TOOL_H

#include "postgres.h"

#include "common/fe_memutils.h"
#include "getopt_long.h"

extern int checkpoint_rewind(int argc, char **argv);


#endif
