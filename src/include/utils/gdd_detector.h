/*-------------------------------------------------------------------------
 *
 * gdd_detector.h
 *	  Global DeadLock Detector - Detector Algorithm
 *
 *
 * Copyright (c) 2018-2020 VMware, Inc. or its affiliates.
 * Copyright (c) 2021-Present OpenTenBase development team, Tencent
 *
 * IDENTIFICATION
 *	  src/include/utils/gdd_detector.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef GDD_DETECTOR_H
#define GDD_DETECTOR_H

#include "nodes/pg_list.h"
#include "lib/stringinfo.h"

/*macros about space allocation and release*/
#define INIT(x)\
do{\
	x = NULL;\
	x##_count = 0;\
	x##_size = 0;\
}while(0);

#define RPALLOC(x)\
do{\
    if (x##_size < x##_count+1)\
    {\
        int temp_size = (x##_size > 0) ? x##_size : 1;\
        if (NULL == x)\
        {\
			x = palloc0(2*temp_size*sizeof(*x));\
		}\
        else\
        {\
        	x = repalloc(x, 2*temp_size*sizeof(*x));\
        }\
    	x##_size = 2*temp_size;\
    }\
}while(0);

#define PALLOC(x, y)\
do{\
    RPALLOC(x);\
    x[x##_count] = y;\
    x##_count++;\
}while(0);

#define RFREE(x)\
do{\
    if (x##_size > 0)\
    {\
        pfree(x);\
    }\
    x = NULL;\
    x##_count = 0;\
    x##_size = 0;\
}while(0);

typedef struct GddCtx		GddCtx;
typedef struct GddPair		GddPair;
typedef struct GddMap		GddMap;
typedef struct GddMapIter	GddMapIter;
typedef struct GddListIter	GddListIter;
typedef struct GddEdge		GddEdge;
typedef struct GddVert		GddVert;
typedef struct GddGraph		GddGraph;
typedef struct GddStat		GddStat;
typedef struct TransactionInfo TransactionInfo;

extern GddCtx *GddCtxNew(void);
extern void InitGddCtxTransaction(GddCtx *ctx, int txn_index);
extern void InitGddTxnPidNodeInfo(GddCtx *ctx);
extern GddEdge *GddCtxAddEdge(GddCtx *ctx, Oid node, int from, int to, bool solid);
extern void GddCtxReduce(GddCtx *ctx);
extern List *GddCtxBreakDeadLock(GddCtx *ctx);
extern bool GddCtxEmpty(GddCtx *ctx);

#endif   /* GDD_DETECTOR_H */
