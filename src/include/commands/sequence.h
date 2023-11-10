/*-------------------------------------------------------------------------
 *
 * sequence.h
 *      prototypes for sequence.c.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/commands/sequence.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SEQUENCE_H
#define SEQUENCE_H

#include "access/xlogreader.h"
#include "catalog/objectaddress.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "parser/parse_node.h"
#include "storage/relfilenode.h"

#ifdef PGXC
#include "utils/relcache.h"
#include "gtm/gtm_c.h"
#include "access/xact.h"
#endif

typedef struct FormData_pg_sequence_data
{
    int64        last_value;
    int64        log_cnt;
    bool        is_called;
} FormData_pg_sequence_data;

typedef FormData_pg_sequence_data *Form_pg_sequence_data;

/*
 * Columns of a sequence relation
 */

#define SEQ_COL_LASTVAL            1
#define SEQ_COL_LOG                2
#define SEQ_COL_CALLED            3

#define SEQ_COL_FIRSTCOL        SEQ_COL_LASTVAL
#define SEQ_COL_LASTCOL            SEQ_COL_CALLED

/* XLOG stuff */
#define XLOG_SEQ_LOG            0x00

typedef struct xl_seq_rec
{
    RelFileNode node;
    /* SEQUENCE TUPLE DATA FOLLOWS AT THE END */
} xl_seq_rec;

extern int64 nextval_internal(Oid relid, bool check_permissions);
extern Datum nextval(PG_FUNCTION_ARGS);
extern List *sequence_options(Oid relid);

#ifdef __OPENTENBASE__
extern ObjectAddress DefineSequence(ParseState *pstate, CreateSeqStmt *seq,
								bool exists_ok);
extern bool PrecheckDefineSequence(CreateSeqStmt *seq);
#else
extern ObjectAddress DefineSequence(ParseState *pstate, CreateSeqStmt *stmt);
#endif
extern ObjectAddress AlterSequence(ParseState *pstate, AlterSeqStmt *stmt);
extern void DeleteSequenceTuple(Oid relid);
extern void ResetSequence(Oid seq_relid);
extern void ResetSequenceCaches(void);

extern void seq_redo(XLogReaderState *rptr);
extern void seq_desc(StringInfo buf, XLogReaderState *rptr);
extern const char *seq_identify(uint8 info);
extern void seq_mask(char *pagedata, BlockNumber blkno);

#ifdef XCP
#define DEFAULT_CACHEVAL    1
extern int SequenceRangeVal;
#endif
#ifdef PGXC
/*
 * List of actions that registered the callback.
 * This is listed here and not in sequence.c because callback can also
 * be registered in dependency.c and tablecmds.c as sequences can be dropped
 * or renamed in cascade.
 */
typedef enum
{
    GTM_CREATE_SEQ,
    GTM_DROP_SEQ
} GTM_SequenceDropType;

extern bool IsTempSequence(Oid relid);
extern char *GetGlobalSeqName(Relation rel, const char *new_seqname, const char *new_schemaname);
#ifdef __OPENTENBASE__
extern void RenameDatabaseSequence(const char* oldname, const char* newname);
#endif
#endif

#endif                            /* SEQUENCE_H */
