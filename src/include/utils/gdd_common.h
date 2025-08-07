/*-------------------------------------------------------------------------
 *
 * gdd_common.h
 *	  some global deadlock detector(gdd) and pg_unlock extension's common functions
 *
 * Copyright (c) 2021-Present OpenTenBase development team, Tencent
 *
 * IDENTIFICATION
 *	  src/include/utils/gdd_common.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef GDD_COMMON_H
#define GDD_COMMON_H

#include "postgres.h"
#include "gdd_detectorpriv.h"

typedef struct
{
    char ***slot;	/*slot[i][j] stores value of row i, colum j*/
    int slot_count;	/*number of rows*/
    int slot_size;
    int attnum;
} TupleTableSlots;

typedef struct
{
    char **slot;     /*slot[i] stores value of colum i*/
    int attnum;
} TupleTableRecord;

typedef void (*tuple_handle_function_cb) (void *context, TupleTableRecord *tuple);
typedef void (*kill_txn_function_cb) (void *context, int txn_id);

extern char *RecordGetvalue(TupleTableRecord *result, int field_num);
extern Datum execute_on_single_node(Oid node, const char *query, int attnum, TupleTableSlots *tuples);
extern char *TTSgetvalue(TupleTableSlots *result, int tup_num, int field_num);
extern void DropTupleTableSlots(TupleTableSlots *Slots);

extern void BreakDeadLock(GddCtx *ctx, kill_txn_function_cb kill_txn_function);
extern void BuildWaitGraph(GddCtx *ctx, tuple_handle_function_cb tuple_handle_func);

#endif   /* GDD_COMMON_H */
