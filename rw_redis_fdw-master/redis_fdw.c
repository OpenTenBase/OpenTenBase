/*-------------------------------------------------------------------------
 *
 *                Foreign-data wrapper for Redis
 *
 * Copyright (c) 2015 Leon Dang, Nahanni Systems Inc
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer
 *    in this position and unchanged.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * MODULE IDENTIFICATION
 *                redis_fdw/redis_fdw.c
 *
 *-------------------------------------------------------------------------
 */

#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include <hiredis/hiredis.h>

#include "postgres.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/indexing.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"

#if PG_VERSION_NUM < 120000
#include "optimizer/var.h"
#else
#include "access/table.h"
#include "optimizer/optimizer.h"
#endif

#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
 * Postgres version dependent functions
 */

#if PG_VERSION_NUM < 120000
#define RFDW_VERDEP_ExecStoreTuple(t,s,b) ExecStoreTuple(t,s,InvalidBuffer,b)
#define TABLE_OPEN(t,l) heap_open(t,l)
#define TABLE_CLOSE(t,l) heap_close(t,l)
#else
#define RFDW_VERDEP_ExecStoreTuple(t,s,b) ExecStoreHeapTuple(t,s,b)
#define TABLE_OPEN(t,l) table_open(t,l)
#define TABLE_CLOSE(t,l) table_close(t,l)
#endif

#ifndef ALLOCSET_DEFAULT_SIZES
#define ALLOCSET_DEFAULT_SIZES \
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE
#endif

#if PG_VERSION_NUM < 130000
#define PG_LIST_NEXT(l,lc) lnext(lc)
#else
#define PG_LIST_NEXT(l,lc) lnext(l,lc)
#endif



PG_MODULE_MAGIC;

#ifdef DO_DEBUG
#define DEBUG(x)	elog x
#else
#define DEBUG(x)
#endif

#define DEBUG_LEVEL INFO

#define ERR_CLEANUP(reply,conn,eparams)	do { 			\
		if ((reply) != NULL) freeReplyObject(reply); 	\
		if ((conn) != NULL) redisFree(conn);			\
		reply = NULL; 									\
		conn = NULL; 									\
		elog eparams; 									\
	} while (0)

Datum redis_fdw_handler(PG_FUNCTION_ARGS);
Datum redis_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(redis_fdw_handler);
PG_FUNCTION_INFO_V1(redis_fdw_validator);

/*
 * FDW callback routines
 */
static void redisGetForeignRelSize(PlannerInfo *root,
        RelOptInfo *baserel,
        Oid foreigntableid);

static void redisGetForeignPaths(PlannerInfo *root,
        RelOptInfo *baserel,
        Oid foreigntableid);

static ForeignScan *redisGetForeignPlan(PlannerInfo *root,
        RelOptInfo *baserel,
        Oid foreigntableid,
        ForeignPath *best_path,
        List *tlist,
#if PG_VERSION_NUM < 90500
        List *scan_clauses
#else
        List *scan_clauses,
        Plan *outer_plan
#endif
);

static void redisBeginForeignScan(ForeignScanState *node,
        int eflags);

static TupleTableSlot *redisIterateForeignScan(ForeignScanState *node);

static void redisReScanForeignScan(ForeignScanState *node);

static void redisEndForeignScan(ForeignScanState *node);

#ifdef WRITE_API
static void redisAddForeignUpdateTargets(Query *parsetree,
        RangeTblEntry *target_rte,
        Relation target_relation);

static List *redisPlanForeignModify(PlannerInfo *root,
        ModifyTable *plan,
        Index resultRelation,
        int subplan_index);

static void redisBeginForeignModify(ModifyTableState *mtstate,
        ResultRelInfo *rinfo,
        List *fdw_private,
        int subplan_index,
        int eflags);

static TupleTableSlot *redisExecForeignInsert(EState *estate,
        ResultRelInfo *rinfo,
        TupleTableSlot *slot,
        TupleTableSlot *planSlot);

static TupleTableSlot *redisExecForeignUpdate(EState *estate,
        ResultRelInfo *rinfo,
        TupleTableSlot *slot,
        TupleTableSlot *planSlot);

static TupleTableSlot *redisExecForeignDelete(EState *estate,
        ResultRelInfo *rinfo,
        TupleTableSlot *slot,
        TupleTableSlot *planSlot);

static void redisEndForeignModify(EState *estate,
        ResultRelInfo *rinfo);

static void redisExplainForeignModify(ModifyTableState *mtstate,
        ResultRelInfo *rinfo,
        List *fdw_private,
        int subplan_index,
        struct ExplainState *es);

static int redisIsForeignRelUpdatable(Relation rel);

#endif /* WRITE_API */

static void redisExplainForeignScan(ForeignScanState *node,
        struct ExplainState *es);


/*
 * Structures used internally by this FDW.
 */

struct redis_fdw_option {
	const char *optname;
	Oid        optctx;
};

#define OPT_HOST      "host"
#define OPT_PORT      "port"
#define OPT_TIMEOUT   "timeout"
#define OPT_PASSWORD  "password"
#define OPT_DATABASE  "database"
#define OPT_KEY       "key"
#define OPT_CHANNEL   "channel"
#define OPT_KEYPREFIX "keyprefix"
#define OPT_TABLETYPE "tabletype"
#define OPT_REDIS     "redis"
#define OPT_READONLY  "readonly"

static struct redis_fdw_option valid_options[] =
{
	/* Connection options */
	{OPT_HOST, ForeignServerRelationId},
	{OPT_PORT, ForeignServerRelationId},
	{OPT_TIMEOUT, ForeignServerRelationId},
	{OPT_PASSWORD, UserMappingRelationId},
	{OPT_DATABASE, ForeignTableRelationId},

	/* Table options */
	{OPT_KEY, ForeignTableRelationId},
	{OPT_CHANNEL, ForeignTableRelationId},
	{OPT_KEYPREFIX, ForeignTableRelationId},
	{OPT_TABLETYPE, ForeignTableRelationId},

	{OPT_READONLY, ForeignTableRelationId},

	/* Columns */
	{OPT_REDIS, AttributeRelationId},

	/* Sentinel */
	{NULL, InvalidOid}
};


/*
 * The supported data types (and hence table types).
 */
enum redis_data_type {
	PG_REDIS_STRING,  /* SET, GET */
	PG_REDIS_HSET,    /* HSET, HGET */
	PG_REDIS_HMSET,   /* HMSET, HMGET */
	PG_REDIS_LIST,    /* LSET, LINDEX, LRANGE, RPUSH, LPOP */
	PG_REDIS_SET,     /* SADD, SMEMBERS, SREM */
	PG_REDIS_ZSET,    /* XADD, ZREVRANGE, ZREM */
	PG_REDIS_LEN,     /* STRLEN, LLEN, HLEN, SCARD, ZCARD, DBSIZE */
	PG_REDIS_TTL,     /* (select) TTL and (update) EXPIRE/PERSIST */
	PG_REDIS_PUBLISH, /* PUBLISH */
	PG_REDIS_KEYS,    /* KEYS * */

	PG_REDIS_INVALID,
};


/* bit masks for param_flags */
#define PARAM_KEY          0x0001
#define PARAM_FIELD        0x0002
#define PARAM_ARRAY_FIELD  0x0004
#define PARAM_MEMBER       0x0008
#define PARAM_MEMBERS      0x0010
#define PARAM_INDEX        0x0020
#define PARAM_SCORE        0x0040
#define PARAM_EXPIRY       0x0080
#define PARAM_VALUE        0x0100
#define PARAM_TABLE_TYPE   0x0200
#define PARAM_CHANNEL      0x0400
#define PARAM_MESSAGE      0x0800

/*
 * column names/ids that this module accepts
 */
enum var_field {
	VAR_KEY,
	VAR_FIELD,
	VAR_ARRAY_FIELD,
	VAR_S_VALUE,
	VAR_SARRAY_VALUE,
	VAR_I_VALUE,
	VAR_MEMBER,
	VAR_MEMBERS,
	VAR_EXPIRY,
	VAR_INDEX,
	VAR_SCORE,
	VAR_SZ_CARD,
	VAR_SCARD,
	VAR_ZCARD,
	VAR_LEN,
	VAR_TABLE_TYPE,
	VAR_CHANNEL,
	VAR_MESSAGE,
};

static const char *FIELD_NAMES[] = {
	[VAR_KEY]          = "key",
	[VAR_FIELD]        = "field",
	[VAR_ARRAY_FIELD]  = "field",
	[VAR_S_VALUE]      = "value",
	[VAR_SARRAY_VALUE] = "value",
	[VAR_I_VALUE]      = "value",
	[VAR_MEMBER]       = "member",
	[VAR_MEMBERS]      = "members",
	[VAR_EXPIRY]       = "expiry",
	[VAR_INDEX]        = "index",
	[VAR_SCORE]        = "score",
	[VAR_SZ_CARD]      = "szcard",
	[VAR_SCARD]        = "scard",
	[VAR_ZCARD]        = "zcard",
	[VAR_LEN]          = "len",
	[VAR_TABLE_TYPE]   = "tabletype",

	/* publish */
	[VAR_CHANNEL]      = "channel",
	[VAR_MESSAGE]      = "message"
};

/* supported column types */
enum {
	R_INT,
	R_TEXT,
	R_FLOAT,
	R_MTEXT,
	R_MINT
};

enum redis_op {
	ROP_INVALID = -1,
	ROP_NEQ,			/* <> */
	ROP_EQ,				/* = */
	ROP_LT,				/* < */
	ROP_LTE,			/* <= */
	ROP_GT,				/* > */
	ROP_GTE,			/* >= */
	ROP_ARRAYELEMS		/* @> */
};

enum redis_cmd {
	REDIS_NONE,
	REDIS_GET,
	REDIS_HGETALL,
	REDIS_HGET,
	REDIS_HMGET,
	REDIS_LINDEX,
	REDIS_LRANGE,
	REDIS_SISMEMBER,
	REDIS_SMEMBERS,
	REDIS_ZRANK,
	REDIS_ZRANGE,
	REDIS_ZRANGEBYSCORE,
	REDIS_TTL,
	REDIS_KEYS,

	REDIS_SCARD,
	REDIS_ZCARD,
	REDIS_HLEN,
	REDIS_LLEN,
	REDIS_STRLEN,
	REDIS_LEN,    /* any of the len types above */

	REDIS_DBSIZE,

	REDIS_EXPIRE,
	REDIS_PERSIST,
	REDIS_SET,
	REDIS_HSET,
	REDIS_RPUSH,
	REDIS_SADD,
	REDIS_ZADD,

	REDIS_PUBLISH,
	REDIS_PSNUMSUB,         /* PUBSUB NUMSUB */

	REDIS_DISCARD_RESULT,   /* discard the result obtained from redis */
};

struct redis_column {
	int     var_field;      /* if -1, then column is not in output */
	int     orig_var_field; /* a copy of var_field */
	int     pgattnum;
	Oid     pgtype;
	Oid     pgtypmod;
	regproc typoutput;  /* type output conversion function */
	regproc typinput;   /* type input conversion function */
};

/*
 * column definition per table schema.
 */
struct redis_table {
	int colcnt;
	struct redis_column *columns;

	/* indices into which column has the attribute */
	int key;             /* also PUBLISH.channel */
	int field;
	int array_field;     /* [] text */
	int s_value;         /* string value / publish.message */
	int sarray_value;    /* [] text */
	int i_value;         /* int64 value */
	int member;
	int members;         /* [] text */
	int expiry;
	int index;
	int score;
	int szcard;          /* scard or zcard */
	int len;
	int table_type;
};


/* right hand side Param of an expression */
struct redis_param_desc {
	void          *param;
	int            paramid;
	enum var_field var_field;      /* field associated with param */
	enum redis_op  op;             /* operation on param */
	char          *value;          /* stashed value, if any */
	struct redis_param_desc *next;
};

/* where clause predicate values */
struct where_conds {
	char   *field;      /* hset/hmset field */
	char   *s_value;    /* value or member */
	int64_t i_value;    /* value or member */

	int64_t min;        /* list index, zset index, zset score */
	int64_t max;
	int     min_op;
	int     max_op;

	char   *table_type; /* for len table only */
};

/*
 * FDW state information for RelOptInfo.fdw_private and
 * ForeignScanState.fdw_state.
 */
struct redis_fdw_ctx {
	redisContext *r_ctx;
	redisReply *r_reply;

	char *host;
	int   port;
	char *password;
	int   database;
	enum redis_data_type table_type;

	char *key;                    /* can be set in table opt or a column */
	char *keyprefix;              /* keys are prefixed with this */
	char *pfxkey;                 /* keyprefix+key cached */

	struct where_conds where_conds;
	int64_t expiry;               /* expiry parameter */

	unsigned long rowcount;       /* rows returned by redis */
	unsigned long rowsdone;       /* rows already read from redis */

	enum redis_cmd cmd;
	CmdType        psql_cmd;      /* postgres' plan command */

	struct redis_table rtable;    /* definition of the pgsql table */

	bool *pushdown_conds;         /* conditions that can be pushed to redis */
	struct redis_param_desc *params; /* list of parameters needed for query */

	int param_flags;              /* fields provided as params */
	int where_flags;              /* fields provided in WHERE */

	/* Cached information */
	ForeignTable  *table;
	ForeignServer *server;
	PlannerInfo *root;

	MemoryContext temp_ctx;       /* short-lived memory for data mods */

	/* for resjunks */
	AttrNumber key_attno;
	AttrNumber field_attno;
	AttrNumber index_attno;
	AttrNumber member_attno;
	AttrNumber value_attno;

	AttInMetadata *attmeta;       /* for returning slot tuples */
	char **slot_values;
};

#ifdef DO_DEBUG
static char *
reply_type_str(int n)
{
	switch (n) {
	case REDIS_REPLY_STRING:
		return "STRING";
	case REDIS_REPLY_ARRAY:
		return "ARRAY";
	case REDIS_REPLY_INTEGER:
		return "INT";
	case REDIS_REPLY_NIL:
		return "NIL";
	case REDIS_REPLY_STATUS:
		return "STATUS";
	case REDIS_REPLY_ERROR:
		return "ERROR";
	}
	return "UNKNOWN";
}

static void
dump_reply(redisReply *r, int level)
{
	char prefix[64];
	int i;
	redisReply *elem;

	if (r == NULL)
		return;

	for (i = 0; i < level && i < 63; i++) {
		prefix[i] = ' ';
	}
	prefix[i] = '\0';

	DEBUG((DEBUG_LEVEL,
	    "  Reply: %s%s len(%u) str(%s) int(%lld) elements(%lu)",
	    prefix, reply_type_str(r->type), (unsigned)r->len,
	    r->str ? r->str : "", r->integer, r->elements));
	if (r->elements > 0) {
		for (i = 0; i < r->elements; i++) {
			elem = r->element[i];
			dump_reply(elem, level+1);
		}
	}
}
#else

#define dump_reply(...)

#endif /* DO_DEBUG */

/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
redis_is_valid_opt(const char *option, Oid context)
{
	struct redis_fdw_option *opt;

	for (opt = valid_options; opt->optname != NULL; opt++) {
		if (context == opt->optctx && strcmp(opt->optname, option) == 0)
			return true;
	}
	return false;
}


/*
 * Converts a postgresql array into a redis command to be used with
 * hiredis.redisCommandArgv.
 * argc is the number of items already in argv.
 * max is the length of argv.
 * Returns total "argc" if ok, else -1
 */
static int
psqlarray_to_rediscmd(char *src, int argc, int max,
                      char **argv, size_t *argvlen)
{
	int curlen;
	char *start;
	char *sp;

	if (*src == 0)
		return -1;

	curlen = 0;
	if (*src == '{')
		src++;
	start = src;
	while (argc < max) {
		switch (*src) {
		case '}':
		case '\0':
			if (curlen > 0) {
				argv[argc] = start;
				argvlen[argc] = curlen;
				argc++;
				curlen = 0;
				*src = '\0';
			}
			goto done;
		case '"':
			/* read until end of quote */
			src++;
			sp = start = src;
			while (*src != 0 && *src != '"') {
				if (*src == '\\')
					/* escape; transfer next character to here */
					src++;
				*sp = *src;

				if (*src == '\0')
					break;

				src++;
				sp++;
				curlen++;
			}
			*sp = '\0';
			argv[argc] = start;
			argvlen[argc] = curlen;
			argc++;
			src++;
			curlen = 0;
			break;
		case ',':
			/* completed previous one */
			*src = '\0';
			argv[argc] = start;
			argvlen[argc] = curlen;
			argc++;
			curlen = 0;
			src++;
			start = src;
			break;
		default:
			curlen++;
			src++;
			break;
		}
	}
done:
	return argc;
}

static int
redisarray_to_psqlarray(redisReply *reply, char **fields, char **values)
{
	StringInfo res_f = NULL;
	StringInfo res_v;
	char *buf;
	char *s;
	int i, n;
	int nescapes;

	if (fields != NULL) {
		res_f = makeStringInfo();
		appendStringInfoChar(res_f,'{');
	}
	res_v = makeStringInfo();
	appendStringInfoChar(res_v,'{');
	for (i = 0; i < reply->elements; i++) {
		redisReply *elem = reply->element[i];

		if (fields != NULL) {
			/* field and then val */
			if (i > 0 && (i & 0x01) == 0)
				appendStringInfoChar(res_f,',');
			else if (i > 2 && (i & 0x01) == 1)
				appendStringInfoChar(res_v,',');
		} else if (i > 0) {
				appendStringInfoChar(res_v,',');
		}

		if (elem->type == REDIS_REPLY_ARRAY) {
			DEBUG((DEBUG_LEVEL, "nested array from redis not supported"));
			return -1;
		}

		switch (elem->type) {
		case REDIS_REPLY_STATUS:
		case REDIS_REPLY_STRING:
			/* escape quotes */
			nescapes = 0;
			for (n = 0; n < elem->len; n++) {
				if (elem->str[n] == '"' || elem->str[n] == '\\')
					nescapes++;
			}
			s = palloc(elem->len + nescapes + 4);
			buf = s;
			*buf++ = '"';
			for (n = 0; n < elem->len; n++) {
				if (elem->str[n] == '"' || elem->str[n] == '\\')
					*buf++ = '\\';
				*buf++ = elem->str[n];
			}
			*buf++ = '"';
			*buf = '\0';
			if (fields != NULL && (i & 0x01) == 0)
				appendStringInfoString(res_f, s);
			else
				appendStringInfoString(res_v, s);
			pfree(s);
			break;

		case REDIS_REPLY_INTEGER:
			if (fields != NULL && (i & 0x01) == 0)
				appendStringInfo(res_f, "%lld", elem->integer);
			else
				appendStringInfo(res_v, "%lld", elem->integer);
			break;

		case REDIS_REPLY_NIL:
			if (fields != NULL && (i & 0x01) == 0)
				appendStringInfo(res_f, "NULL");
			else
				appendStringInfo(res_v, "NULL");
			break;
		default:
			break;
		}
	}
	if (fields != NULL) {
		appendStringInfoChar(res_f,'}');
		*fields = res_f->data;
	}
	appendStringInfoChar(res_v,'}');
	*values = res_v->data;

	return 0;
}


static redisReply *
redis_do_hmget(redisContext *ctx, char *key, char *fields)
{
	int argc;
	char **argv;
	size_t *argvlen;

	argv = (char **)palloc(sizeof(char *) * 32);
	argvlen = (size_t *)palloc(sizeof(size_t) * 32);
	argv[0] = "HMGET";
	argvlen[0] = 5;
	argv[1] = key;
	argvlen[1] = strlen(argv[1]);
	argc = psqlarray_to_rediscmd(fields, 2, 32, argv, argvlen);
	if (argc < 0) {
		DEBUG((DEBUG_LEVEL, "parsing of psql array failed for HMGET"));
		return NULL;
	}

	return (redisReply *)redisCommandArgv(ctx, argc, (const char **)argv, argvlen);
}

static Const *
redis_serialize_rtable(struct redis_fdw_ctx *rctx)
{
	bytea *result;
	Datum conval;

	result = (bytea *)palloc(VARHDRSZ + sizeof(struct redis_table));
	SET_VARSIZE(result, sizeof(struct redis_table));
	memcpy(VARDATA(result), &(rctx->rtable), sizeof(struct redis_table));
	conval = PointerGetDatum(result);
	return makeConst(BYTEAOID, -1, InvalidOid, -1, conval, false, false);
}

static Const *
redis_serialize_rtable_cols(struct redis_fdw_ctx *rctx)
{
	bytea *result;
	Datum conval;
	size_t len;

	len = rctx->rtable.colcnt * sizeof(struct redis_column);
	result = (bytea *)palloc(VARHDRSZ + len);
	SET_VARSIZE(result, len);
	memcpy(VARDATA(result), rctx->rtable.columns, len);
	conval = PointerGetDatum(result);
	return makeConst(BYTEAOID, -1, InvalidOid, -1, conval, false, false);
}

static void
redis_deserialize_rtable(struct redis_fdw_ctx *rctx, Const *conval)
{
	bytea *val;

	Assert((conval != NULL && conval->consttype == BYTEAOID));

	val = DatumGetByteaP(conval->constvalue);

	memcpy(&rctx->rtable, VARDATA(val), sizeof(struct redis_table));

	DEBUG((DEBUG1, "unmarshaled rtable, varsize %d:\n"
	   "\tcolcnt: %d\n"
	   "\tkey: %d\n"
	   "\tfield: %d\n"
	   "\taray_field: %d\n"
	   "\ts_value: %d\n"
	   "\tsarray_value: %d\n"
	   "\ti_value: %d\n"
	   "\tmember: %d\n"
	   "\tmembers: %d\n"
	   "\texpiry: %d\n"
	   "\tindex: %d\n"
	   "\tscore: %d\n"
	   "\tszcard: %d\n"
	   "\tlen: %d\n"
	   "\ttable_type: %d",
	   VARSIZE(val),
	   rctx->rtable.colcnt,
	   rctx->rtable.key,
	   rctx->rtable.field,
	   rctx->rtable.array_field,
	   rctx->rtable.s_value,
	   rctx->rtable.sarray_value,
	   rctx->rtable.i_value,
	   rctx->rtable.member,
	   rctx->rtable.members,
	   rctx->rtable.expiry,
	   rctx->rtable.index,
	   rctx->rtable.score,
	   rctx->rtable.szcard,
	   rctx->rtable.len,
	   rctx->rtable.len
	));
}

static void
redis_deserialize_rtable_cols(struct redis_fdw_ctx *rctx, Const *conval)
{
	bytea *val;
	int    i;

	Assert((conval != NULL && conval->consttype == BYTEAOID));

	val = DatumGetByteaP(conval->constvalue);

	DEBUG((DEBUG1, "unmarshaled rtable columns %s\n, varsize %d",
	   nodeToString(conval), VARSIZE(val)));

	rctx->rtable.columns = (struct redis_column *)palloc(VARSIZE(val));
	memcpy(rctx->rtable.columns, VARDATA(val), VARSIZE(val));

	for (i = 0; i < rctx->rtable.colcnt; i++) {
		DEBUG((DEBUG_LEVEL, "  col var_field(%d:%s) -> pgtype(%d)",
		    rctx->rtable.columns[i].var_field,
		    FIELD_NAMES[rctx->rtable.columns[i].var_field],
		    rctx->rtable.columns[i].pgtype));
	}
}

static Const *
serializeString(const char *s)
{
	if (s == NULL)
		return makeNullConst(TEXTOID, -1, InvalidOid);
	else
		return makeConst(TEXTOID, -1, InvalidOid, -1,
		                 PointerGetDatum(cstring_to_text(s)), false, false);
}

#define serializeInt32(n) \
	makeConst(INT4OID, -1, InvalidOid, sizeof(int32), \
	          Int32GetDatum((int32)n), false, true)

#define serializeInt64(n) \
	makeConst(INT4OID, -1, InvalidOid, sizeof(int64), \
	          Int64GetDatum((int64)n), false, true)

static List *
redis_serialize_fdw(struct redis_fdw_ctx *rctx)
{
	List *result = NIL;
	int len = 0;
	struct redis_param_desc *param;
	struct where_conds *wc;

	result = lappend(result, redis_serialize_rtable(rctx));
	result = lappend(result, redis_serialize_rtable_cols(rctx));

	result = lappend(result, serializeString(rctx->host));
	result = lappend(result, serializeInt32(rctx->port));
	result = lappend(result, serializeString(rctx->password));
	result = lappend(result, serializeInt32(rctx->database));
	result = lappend(result, serializeInt32(rctx->table_type));

	result = lappend(result, serializeString(rctx->key));
	result = lappend(result, serializeString(rctx->keyprefix));

	wc = &rctx->where_conds;
	result = lappend(result, serializeString(wc->field));
	result = lappend(result, serializeString(wc->s_value));
	result = lappend(result, serializeString(wc->table_type));
	result = lappend(result, serializeInt64(wc->i_value));
	result = lappend(result, serializeInt64(wc->min));
	result = lappend(result, serializeInt64(wc->max));
	result = lappend(result, serializeInt32(wc->min_op));
	result = lappend(result, serializeInt32(wc->max_op));

	result = lappend(result, serializeInt32(rctx->psql_cmd));
	result = lappend(result, serializeInt32(rctx->param_flags));
	result = lappend(result, serializeInt32(rctx->where_flags));

	/* iterate through each param and marshal accordingly */
	for (param = rctx->params; param != NULL; param = param->next)
		len++;

	result = lappend(result, serializeInt32(len));

	for (param = rctx->params; param != NULL; param = param->next) {
		/* keep paramid which is the location of the parameter */
		Assert(param->param != NULL);
		result = lappend(result, serializeInt32(param->paramid));
		result = lappend(result, serializeInt32(param->var_field));
		result = lappend(result, serializeInt32(param->op));
	}

	return result;
}

static char *
deserializeString(Const *constant)
{
	if (constant->constisnull)
		return NULL;
	else
		return text_to_cstring(DatumGetTextP(constant->constvalue));
}

static int64
deserializeInt(Const *constant)
{
	if (constant->constlen <= 4)
		return (int64)DatumGetInt32(constant->constvalue);
	else
		return (int64)DatumGetInt64(constant->constvalue);
}


static struct redis_fdw_ctx *
redis_deserialize_fdw(List *list)
{
	struct redis_fdw_ctx *rctx;
	ListCell *cell = list_head(list);
	int len;
	struct redis_param_desc *param, **pptr;
	struct where_conds *wc;

	DEBUG((DEBUG_LEVEL, "___ %s", __FUNCTION__));

	rctx = (struct redis_fdw_ctx *)palloc0(sizeof(struct redis_fdw_ctx));

	redis_deserialize_rtable(rctx, lfirst(cell));
	cell = PG_LIST_NEXT(list, cell);

	redis_deserialize_rtable_cols(rctx, lfirst(cell));
	cell = PG_LIST_NEXT(list, cell);

	rctx->host = deserializeString(lfirst(cell));
	cell = PG_LIST_NEXT(list, cell);

	rctx->port = (int)deserializeInt(lfirst(cell));
	cell = PG_LIST_NEXT(list, cell);

	rctx->password = deserializeString(lfirst(cell));
	cell = PG_LIST_NEXT(list, cell);

	rctx->database = (int)deserializeInt(lfirst(cell));
	cell = PG_LIST_NEXT(list, cell);

	rctx->table_type = (enum redis_data_type)deserializeInt(lfirst(cell));
	cell = PG_LIST_NEXT(list, cell);

	rctx->key = deserializeString(lfirst(cell));
	cell = PG_LIST_NEXT(list, cell);
	rctx->keyprefix = deserializeString(lfirst(cell));
	cell = PG_LIST_NEXT(list, cell);

	wc = &rctx->where_conds;
	wc->field = deserializeString(lfirst(cell));
	cell = PG_LIST_NEXT(list, cell);
	wc->s_value = deserializeString(lfirst(cell));
	cell = PG_LIST_NEXT(list, cell);
	wc->table_type = deserializeString(lfirst(cell));
	cell = PG_LIST_NEXT(list, cell);

	wc->i_value = deserializeInt(lfirst(cell));
	cell = PG_LIST_NEXT(list, cell);

	wc->min = deserializeInt(lfirst(cell));
	cell = PG_LIST_NEXT(list, cell);
	wc->max = deserializeInt(lfirst(cell));
	cell = PG_LIST_NEXT(list, cell);

	wc->min_op = (int)deserializeInt(lfirst(cell));
	cell = PG_LIST_NEXT(list, cell);
	wc->max_op = (int)deserializeInt(lfirst(cell));
	cell = PG_LIST_NEXT(list, cell);

	rctx->psql_cmd = (int)deserializeInt(lfirst(cell));
	cell = PG_LIST_NEXT(list, cell);
	rctx->param_flags = (int)deserializeInt(lfirst(cell));
	cell = PG_LIST_NEXT(list, cell);
	rctx->where_flags = (int)deserializeInt(lfirst(cell));
	cell = PG_LIST_NEXT(list, cell);

	len = (int)deserializeInt(lfirst(cell));
	cell = PG_LIST_NEXT(list, cell);

	rctx->params = NULL;
	pptr = &rctx->params;
	for (; len > 0; len--) {
		param = (struct redis_param_desc *)
		        palloc(sizeof(struct redis_param_desc));
		param->paramid = (int)deserializeInt(lfirst(cell));
		cell = PG_LIST_NEXT(list, cell);
		param->var_field = (enum var_field)deserializeInt(lfirst(cell));
		cell = PG_LIST_NEXT(list, cell);
		param->op = (enum redis_op)deserializeInt(lfirst(cell));
		cell = PG_LIST_NEXT(list, cell);

		param->param = NULL;
		param->value = NULL;

		/* add to the end per lappend */
		*pptr = param;
		param->next = NULL;
		pptr = &param->next;
	}

	return rctx;
}


static int
redis_opername_to_rop(const char *op)
{
	if (strcmp(op, "<>") == 0)
		return ROP_NEQ;
	if (strcmp(op, "=") == 0)
		return ROP_EQ;
	if (strcmp(op, "<") == 0)
		return ROP_LT;
	if (strcmp(op, "<=") == 0)
		return ROP_LTE;
	if (strcmp(op, ">") == 0)
		return ROP_GT;
	if (strcmp(op, ">=") == 0)
		return ROP_GTE;
	if (strcmp(op, "@>") == 0)
		return ROP_ARRAYELEMS;

	return ROP_INVALID;
}


/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
redis_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	routine->GetForeignRelSize = redisGetForeignRelSize;
	routine->GetForeignPaths = redisGetForeignPaths;
	routine->GetForeignPlan = redisGetForeignPlan;
	routine->BeginForeignScan = redisBeginForeignScan;
	routine->IterateForeignScan = redisIterateForeignScan;
	routine->ReScanForeignScan = redisReScanForeignScan;
	routine->EndForeignScan = redisEndForeignScan;

#ifdef WRITE_API
	/* Functions for updating foreign tables */
	routine->AddForeignUpdateTargets = redisAddForeignUpdateTargets;
	routine->PlanForeignModify = redisPlanForeignModify;
	routine->BeginForeignModify = redisBeginForeignModify;
	routine->ExecForeignInsert = redisExecForeignInsert;
	routine->ExecForeignUpdate = redisExecForeignUpdate;
	routine->ExecForeignDelete = redisExecForeignDelete;
	routine->EndForeignModify = redisEndForeignModify;
	routine->IsForeignRelUpdatable = redisIsForeignRelUpdatable;

	/* Support functions for EXPLAIN */
	routine->ExplainForeignModify = redisExplainForeignModify;
#endif
	routine->ExplainForeignScan = redisExplainForeignScan;

	PG_RETURN_POINTER(routine);
}


Datum
redis_fdw_validator(PG_FUNCTION_ARGS)
{
	List       *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid         catalog = PG_GETARG_OID(1);
	char       *host = NULL;
	int	        port = 0;
	char       *password = NULL;
	int         database = 0;
	enum redis_data_type tabletype = PG_REDIS_STRING;
	char       *keyprefix = NULL;
	char       *key = NULL;
	ListCell   *cell;

	DEBUG((DEBUG_LEVEL, "redis_fdw_validator"));

	/*
	 * Check that only options supported by redis_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach(cell, options_list) {
		DefElem    *def = (DefElem *) lfirst(cell);

		if (!redis_is_valid_opt(def->defname, catalog)) {
			struct redis_fdw_option *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname != NULL; opt++) {
				if (catalog == opt->optctx)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
									 opt->optname);
			}

			ereport(ERROR,
			       (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
			        errmsg("invalid option \"%s\"", def->defname),
			        errhint("Valid options in this context are: %s", 
			                buf.len ? buf.data : "<none>")
			        ));
		}

		if (strcmp(def->defname, OPT_HOST) == 0 ||
		    strcmp(def->defname, "address") == 0 ) {
			if (host)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting or redundant options: %s (%s)",
				               def->defname, defGetString(def))
				        ));

			host = defGetString(def);
		} else if (strcmp(def->defname, OPT_PORT) == 0) {
			if (port > 0)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting or redundant options: %s (%s)", 
				                OPT_PORT, defGetString(def))
				        ));

			port = atoi(defGetString(def));
			if (port <= 0)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("invalid value: %s (%s)", 
				                OPT_PORT, defGetString(def))
				        ));
		} else if (strcmp(def->defname, OPT_PASSWORD) == 0) {
			if (password)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
				        errmsg("conflicting or redundant options: %s",
			                   OPT_PASSWORD)
				       ));

			password = defGetString(def);
		} else if (strcmp(def->defname, OPT_DATABASE) == 0) {
			if (database)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting or redundant options: database "
				                "(%s)", defGetString(def))
				        ));

			database = atoi(defGetString(def));
		} else if (strcmp(def->defname, OPT_KEY) == 0 ||
		           strcmp(def->defname, OPT_CHANNEL) == 0) {
			if (key)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting options: %s (%s)",
				             def->defname, defGetString(def))
			            ));
			key = defGetString(def);
		} else if (strcmp(def->defname, OPT_KEYPREFIX) == 0) {
			if (keyprefix)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting options: %s (%s)",
				             OPT_KEYPREFIX, defGetString(def))
				       ));

			keyprefix = defGetString(def);
		} else if (strcmp(def->defname, OPT_TABLETYPE) == 0) {
			char *typeval = defGetString(def);
			if (tabletype)
				ereport(ERROR,
				        (errcode(ERRCODE_SYNTAX_ERROR),
				         errmsg("conflicting or redundant options: %s (%s)",
				         OPT_TABLETYPE, typeval)));
			if (strcmp(typeval, "string") == 0)
				tabletype = PG_REDIS_STRING;
			else if (strcmp(typeval, "hash") == 0 ||
			         strcmp(typeval, "hset") == 0)
				tabletype = PG_REDIS_HSET;
			else if (strcmp(typeval, "mhash") == 0 ||
			         strcmp(typeval, "hmset") == 0)
				tabletype = PG_REDIS_HMSET;
			else if (strcmp(typeval, "keys") == 0)
				tabletype = PG_REDIS_KEYS;
			else if (strcmp(typeval, "list") == 0)
				tabletype = PG_REDIS_LIST;
			else if (strcmp(typeval,"publish") == 0)
				tabletype = PG_REDIS_PUBLISH;
			else if (strcmp(typeval,"set") == 0)
				tabletype = PG_REDIS_SET;
			else if (strcmp(typeval,"len") == 0)
				tabletype = PG_REDIS_LEN;
			else if (strcmp(typeval,"ttl") == 0)
				tabletype = PG_REDIS_TTL;
			else if (strcmp(typeval,"zset") == 0)
				tabletype = PG_REDIS_ZSET;
			else
				ereport(ERROR,
				       (errcode(ERRCODE_SYNTAX_ERROR),
				        errmsg("invalid tabletype (%s) - must be hash/hset, "
				               "mhash/hmset, list, set, scard, zset or zcard",
				               typeval)));
		}
	}

	PG_RETURN_VOID();
}

static void
verify_pgtable_coltype(int expected_type, Oid type, char *colname,
    char *tablename)
{

#define EINVDATA(x)	ereport(ERROR, \
	(errcode(ERRCODE_FDW_INVALID_DATA_TYPE), errmsg x))

	switch (expected_type) {
	case R_INT:
		if (type != INT8OID && type != INT4OID)
			EINVDATA(("column \"%s\" of foreign table \"%s\" must be INT",
			           colname, tablename));
		break;

	case R_TEXT:
		if (type != TEXTOID && type != VARCHAROID)
			EINVDATA(("column \"%s\" of foreign table \"%s\" must be TEXT",
			           colname, tablename));
		break;

	case R_FLOAT:
		if (type != FLOAT8OID && type != FLOAT4OID)
			EINVDATA(("column \"%s\" of foreign table \"%s\" must be FLOAT",
			           colname, tablename));
		break;

	case R_MTEXT:
		if (type != TEXTARRAYOID)
			EINVDATA(("column \"%s\" of foreign table \"%s\" must be TEXTARRAY",
			           colname, tablename));
		break;

	case R_MINT:
		if (type != INT4ARRAYOID)
			EINVDATA(("column \"%s\" of foreign table \"%s\" must be INTARRAY",
			           colname, tablename));
		break;
	}
}

/*
 * Get postgresql columns to determine table schema
 */
static void
get_psql_columns(Oid foreigntableid, struct redis_fdw_ctx *rctx)
{
	Relation rel;
	TupleDesc tupdesc;
	Oid relid;
	struct redis_table *rtable;
	int i;
	char *colname, *tablename;

	rtable = &rctx->rtable;

	if (rtable->columns != NULL)
		return;

	rel = TABLE_OPEN(foreigntableid, NoLock);
	relid = RelationGetRelid(rel);
	tupdesc = rel->rd_att;

	/* number of PostgreSQL columns */
	rtable->colcnt = tupdesc->natts;

	tablename = get_rel_name(foreigntableid);

	rtable->columns = (struct redis_column *)palloc(
	                       sizeof(struct redis_column) * rtable->colcnt);

	/* loop through foreign table columns */
	for (i = 0; i < tupdesc->natts; i++) {
#if PG_VERSION_NUM < 110000
		Form_pg_attribute att_tuple = tupdesc->attrs[i];
#else
		Form_pg_attribute att_tuple = &tupdesc->attrs[i];
#endif
		List *options;
		ListCell *option;
		bool optvalid;

		colname = NameStr(att_tuple->attname);

		/* check if there is a redis_param remapping of the column name */
		options = GetForeignColumnOptions(relid, att_tuple->attnum);
		foreach (option, options) {
			DefElem *def = (DefElem *)lfirst(option);

			if (strcmp(def->defname, OPT_REDIS) == 0) {
				colname = ((Value *)(def->arg))->val.str;

				DEBUG((DEBUG_LEVEL, "table %s column remapped (%s) -> (%s)",
				        tablename, NameStr(att_tuple->attname), colname));

				break;
			}
		}

		rtable->columns[i].pgtype = att_tuple->atttypid;
		rtable->columns[i].pgtypmod = att_tuple->atttypmod;
		rtable->columns[i].pgattnum = att_tuple->attnum;

		DEBUG((DEBUG_LEVEL,
		      "table %s column (%s) index (%d)", tablename, colname, i));

		optvalid = false;

		/* get PostgreSQL column number of parameters */
		if (strcmp(colname, "key") == 0) {
			/* key is a column if TABLE does not have a "key" option defined */
			verify_pgtable_coltype(R_TEXT, att_tuple->atttypid,
			                       colname, tablename);
			rtable->key = i+1;
			rtable->columns[i].var_field = VAR_KEY;
			optvalid = true;
		} else if (strcmp(colname, "field") == 0) {
			if (rctx->table_type == PG_REDIS_HSET ||
			    rctx->table_type == PG_REDIS_HMSET) {
				if (att_tuple->atttypid == TEXTOID ||
				   att_tuple->atttypid == VARCHAROID) {
					rtable->field = i+1;
					rtable->columns[i].var_field = VAR_FIELD;
					optvalid = true;
				} else if (att_tuple->atttypid == TEXTARRAYOID) {
					rtable->array_field = i+1;
					rtable->columns[i].var_field = VAR_ARRAY_FIELD;
					optvalid = true;
				}
			}
		} else if (strcmp(colname, "value") == 0) {
			if (rctx->table_type == PG_REDIS_STRING ||
			    rctx->table_type == PG_REDIS_HSET ||
			    rctx->table_type == PG_REDIS_HMSET ||
			    rctx->table_type == PG_REDIS_LIST) {
				if (att_tuple->atttypid == TEXTOID ||
				   att_tuple->atttypid == VARCHAROID) {
					rtable->s_value = i+1;
					rtable->columns[i].var_field = VAR_S_VALUE;
				} else if (att_tuple->atttypid == TEXTARRAYOID) {
					rtable->sarray_value = i+1;
					rtable->columns[i].var_field = VAR_SARRAY_VALUE;
				} else {
					rtable->i_value = i+1;
					rtable->columns[i].var_field = VAR_I_VALUE;
				}
				optvalid = true;
			}
		} else if (strcmp(colname, "members") == 0) {
			if (rctx->table_type == PG_REDIS_SET) {
				verify_pgtable_coltype(R_MTEXT, att_tuple->atttypid,
				                       colname, tablename);
				rtable->members = i+1;
				rtable->columns[i].var_field = VAR_MEMBERS;
				optvalid = true;
			}
		} else if (strcmp(colname, "member") == 0) {
			if (rctx->table_type == PG_REDIS_SET ||
			    rctx->table_type == PG_REDIS_ZSET) {
				verify_pgtable_coltype(R_TEXT, att_tuple->atttypid,
				                       colname, tablename);
				rtable->member = i+1;
				rtable->columns[i].var_field = VAR_MEMBER;
				optvalid = true;
			}
		} else if (strcmp(colname, "channel") == 0) {
			if (rctx->table_type == PG_REDIS_PUBLISH) {
				verify_pgtable_coltype(R_TEXT, att_tuple->atttypid,
				                       colname, tablename);
				rtable->key = i+1;
				rtable->columns[i].var_field = VAR_CHANNEL;
				optvalid = true;
			}
		} else if (strcmp(colname, "message") == 0) {
			if (rctx->table_type == PG_REDIS_PUBLISH) {
				verify_pgtable_coltype(R_TEXT, att_tuple->atttypid,
				                       colname, tablename);
				rtable->s_value = i+1;
				rtable->columns[i].var_field = VAR_MESSAGE;
				optvalid = true;
			}
		} else if (strcmp(colname, "expiry") == 0) {
			verify_pgtable_coltype(R_INT, att_tuple->atttypid,
			                       colname, tablename);
			rtable->expiry = i+1;
			rtable->columns[i].var_field = VAR_EXPIRY;
			optvalid = true;
		} else if (strcmp(colname, "index") == 0) {
			if (rctx->table_type == PG_REDIS_LIST ||
			    rctx->table_type == PG_REDIS_ZSET) {
				verify_pgtable_coltype(R_INT, att_tuple->atttypid,
				                       colname, tablename);
				rtable->index = i+1;
				rtable->columns[i].var_field = VAR_INDEX;
				optvalid = true;
			}
		} else if (strcmp(colname, "score") == 0) {
			if (rctx->table_type == PG_REDIS_ZSET) {
				verify_pgtable_coltype(R_INT, att_tuple->atttypid,
				                       colname, tablename);
				rtable->score = i+1;
				rtable->columns[i].var_field = VAR_SCORE;
				optvalid = true;
			}
		} else if (strcmp(colname, "len") == 0) {
			if (rctx->table_type == PG_REDIS_LEN ||
			    rctx->table_type == PG_REDIS_PUBLISH) {
				verify_pgtable_coltype(R_INT, att_tuple->atttypid,
				                       colname, tablename);
				rtable->len = i+1;
				rtable->columns[i].var_field = VAR_LEN;
				optvalid = true;
			}
		} else if (strcmp(colname, "tabletype") == 0) {
			if (rctx->table_type == PG_REDIS_LEN) {
				verify_pgtable_coltype(R_TEXT, att_tuple->atttypid,
				                       colname, tablename);
				rtable->table_type = i+1;
				rtable->columns[i].var_field = VAR_TABLE_TYPE;
				optvalid = true;
			}
		}

		if (!optvalid) {
			ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_COLUMN_NAME),
				errmsg("unrecognized/invalid column \"%s\" for "
			           "foreign table \"%s\"",
			           colname, tablename)));
		}

		rtable->columns[i].orig_var_field = rtable->columns[i].var_field;

		/* ignore dropped columns */
		if (att_tuple->attisdropped) {
			/* column will be marked as NULL in scan results */
			rtable->columns[i].var_field = -1;
			continue;
		}
	}
	TABLE_CLOSE(rel, NoLock);
}

/*
 * Validate table schema depending on table type and redis options.
 */
static void
validate_redis_opts(struct redis_fdw_ctx *rctx)
{
	struct redis_table *tbl = &rctx->rtable;

#define EDYNPARAM(x)	ereport(ERROR, \
	(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED), errmsg x))

	/* extract table columns */
	get_psql_columns(rctx->table->relid, rctx);

	if (rctx->key == NULL && tbl->key <= 0)
		ereport(ERROR,
		   (errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
		    errmsg("key not defined")));

	switch (rctx->table_type) {
	case PG_REDIS_STRING:
		/*
		 * TABLE (key TEXT, value TEXT, expiry INT)
		 */
		if (tbl->s_value <= 0)
			EDYNPARAM(("STRING: value column required"));
		break;
	case PG_REDIS_HSET:
		/*
		 * TABLE (key TEXT, field TEXT, value TEXT, expiry INT)
		 * TABLE (key TEXT, field TEXT, value INT64, expiry INT)
		 */
		if (tbl->field <= 0 || (tbl->s_value <= 0 && tbl->i_value <= 0))
			EDYNPARAM(("HSET: field + value columns required"));
		break;
	case PG_REDIS_HMSET:
		/*
		 * TABLE TABLE (key TEXT, field TEXT[], value TEXT[], expiry INT)
		 */
		if (tbl->array_field <= 0 || tbl->sarray_value <= 0)
			EDYNPARAM(("HMSET: field[] + value[] columns required"));

		break;
	case PG_REDIS_LIST:
		/*
		 * TABLE (key TEXT, value TEXT, index INT, expiry INT)
		 */
		if ((tbl->s_value <= 0 && tbl->i_value <= 0) || tbl->index <= 0)
			EDYNPARAM(("LIST: value + index columns required"));
		break;
	case PG_REDIS_SET:
		/*
		 * TABLE (key TEXT, members TEXT[], expiry INT)
		 * TABLE (key TEXT, member TEXT, expiry INT)
		 */
		if (tbl->members <= 0 && tbl->member <= 0)
			EDYNPARAM(("SET: members[] or member columns required"));
		break;
	case PG_REDIS_LEN:
		/*
		 * TABLE (key TEXT, tabletype TEXT, len INT, expiry INT)
		 */
		if (tbl->len <= 0 && tbl->table_type <= 0)
			EDYNPARAM(("LEN: len and tabletype columns required"));
		break;
	case PG_REDIS_ZSET:
		/*
		 * TABLE (key TEXT, score INT, member TEXT, index INT, expiry INT)
		 */
		if (tbl->score <= 0 || tbl->member <= 0 || tbl->index <= 0)
			EDYNPARAM(("ZSET: score + member + index columns required"));
		break;
	case PG_REDIS_TTL:
		/*
		 * TABLE (key TEXT, expiry INT)
		 */
		if (tbl->expiry <= 0)
			EDYNPARAM(("TTL: expiry column required"));
		break;
	case PG_REDIS_PUBLISH:
		/*
		 * TABLE (channel TEXT, message TEXT, len INT)
		 *   len = number of subscribers to channel
		 */
		if (tbl->s_value <= 0)
			EDYNPARAM(("PUBLISH: message column required"));
		break;
	case PG_REDIS_KEYS:
		/*
		 * Only key column is permitted.
		 */
		break;
	default:
		// shouldn't get here
		elog(ERROR, "impossible table type %d", rctx->table_type);
	}
}


/* get the type's input/output functions */
static int
pgsql_get_typio(Oid type, regproc *typoutput, regproc *typinput)
{
	HeapTuple tuple;

	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "redis_fdw: cache lookup failed for type %u", type);

	if (typoutput)
		*typoutput = ((Form_pg_type)GETSTRUCT(tuple))->typoutput;
	if (typinput)
		*typinput = ((Form_pg_type)GETSTRUCT(tuple))->typinput;

	ReleaseSysCache(tuple);

	return 0;
}

/*
 * datumToString
 * 		Convert a Datum to a string by calling the type output function.
 * 		Returns the result or NULL if it cannot be converted to Oracle SQL.
 */
static char *
datumToString(Datum datum, Oid type)
{
	regproc typoutput;

	pgsql_get_typio(type, &typoutput, NULL);

	switch (type) {
	case TEXTOID:
	case CHAROID:
	case BPCHAROID:
	case VARCHAROID:
	case TEXTARRAYOID:
	case ANYARRAYOID:
	case NAMEOID:
	case INT8OID:
	case INT2OID:
	case INT4OID:
	case OIDOID:
	case FLOAT4OID:
	case FLOAT8OID:
	case NUMERICOID:
		return DatumGetCString(OidFunctionCall1(typoutput, datum));

	default:
		return NULL;
	}

	return NULL;
}

/*
 * This macro is used by redis_parse_where to identify PostgreSQL
 * types that can be assigned in redis
 */
#define canHandleType(x) ((x) == TEXTOID || (x) == CHAROID ||				\
	(x) == BPCHAROID || (x) == VARCHAROID || (x) == NAMEOID ||				\
	(x) == INT8OID || (x) == INT2OID || (x) == INT4OID || 					\
	(x) == OIDOID || (x) == FLOAT4OID || (x) == FLOAT8OID || 				\
	(x) == NUMERICOID || (x) == TEXTARRAYOID || (x) == ANYARRAYOID)


/*
 * Identify  the variable provided in the query
 *  Accepted variables:
 *      key
 *      field    (hset, hget, hsetnx)
 *      field    (hmset, hmget)
 *      member   (srem, sismember, zrem, zrank)
 *      index    (zrange, zrevrange)
 *
 */
static int
redis_get_var(struct redis_fdw_ctx *rctx, RelOptInfo *foreignrel, Var *var)
{
	struct redis_table *rtable = &rctx->rtable;

	if (var->varno == foreignrel->relid && var->varlevelsup == 0) {
		/* Var belongs to foreign table */

		/* we cannot handle system columns */
		if (var->varattno < 0)
			return -1;

		/* what variable is this */
		if (rtable->key == var->varattno)
			return VAR_KEY;
		if (rtable->field == var->varattno)
			return VAR_FIELD;
		if (rtable->array_field == var->varattno)
			return VAR_ARRAY_FIELD;
		if (rtable->s_value == var->varattno)
			return VAR_S_VALUE;
		if (rtable->sarray_value == var->varattno)
			return VAR_SARRAY_VALUE;
		if (rtable->i_value == var->varattno)
			return VAR_I_VALUE;
		if (rtable->member == var->varattno)
			return VAR_MEMBER;
		if (rtable->members == var->varattno)
			return VAR_MEMBERS;
		if (rtable->expiry == var->varattno)
			return VAR_EXPIRY;
		if (rtable->index == var->varattno)
			return VAR_INDEX;
		if (rtable->score == var->varattno)
			return VAR_SCORE;
		if (rtable->len == var->varattno)
			return VAR_LEN;
		if (rtable->table_type == var->varattno)
			return VAR_TABLE_TYPE;
	}
	return -1;
}

/*
 * Parse the WHERE clause of the SELECT statement and convert to conditional
 * predicates for redis command
 *
 * Permitted expressions:
 *    col <op> [const|param]
 *   - text ops:    =
 *   - integer ops: >, >=, =, <, <=
 *   - array ops:   @>, =
 *
 * Only AND boolean is permitted   TODO permit OR in the future to enable
 *                                      multiple results from single connection
 */
static bool
redis_parse_where(struct redis_fdw_ctx *rctx, RelOptInfo *foreignrel,
    Expr *expr)
{
	Const *constant;
	Param *param;
	Expr  *subexpr;
	OpExpr *oper;
	BoolExpr *boolexpr;

	HeapTuple tuple;
	ListCell *cell;
	Oid       rightargtype;
	Form_pg_operator form;
	char      *arg;
	int       leftidx;
	int       rop;

	DEBUG((DEBUG_LEVEL, "___ %s", __FUNCTION__));

	if (expr == NULL)
		return false;

	switch (nodeTag(expr)) {
	case T_OpExpr:
		oper = (OpExpr *)expr;

		/* Retrieve information about the operator from system catalog. */
		tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(oper->opno));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for operator %u", oper->opno);
		form = (Form_pg_operator) GETSTRUCT(tuple);
		rightargtype = form->oprright;

		rop = redis_opername_to_rop(NameStr(form->oprname));

		DEBUG((DEBUG_LEVEL,
		      "T_OpExpr %s[%d], leftargtype: %d, rightargtype: %d",
		      NameStr(form->oprname), rop, form->oprleft, rightargtype));

		ReleaseSysCache(tuple);

		if (!canHandleType(rightargtype))
			return false;

		if (rop == ROP_INVALID)
			return false;

		if (((rightargtype == TEXTARRAYOID || rightargtype == ANYARRAYOID) &&
		      rop == ROP_ARRAYELEMS) ||
		    (rightargtype != TEXTARRAYOID && rightargtype != ANYARRAYOID &&
		     rop != ROP_ARRAYELEMS)) {
			/* left argument must be a column variable */
			subexpr = linitial(oper->args);

			if (subexpr == NULL) {
				DEBUG((DEBUG_LEVEL, "no left side parameter"));
				return false;
			}
			if (subexpr->type != T_Var) {
				ereport(ERROR,
			        (errcode(ERRCODE_FDW_ERROR),
			         errmsg("left side of operation must be a column name")));
			}

			/* determine redis column variable name */
			leftidx = redis_get_var(rctx, foreignrel, (Var *)subexpr);
			if (leftidx < 0) {
				DEBUG((DEBUG_LEVEL, "unable to find left index"));
				return false;
			}

			/* right side must be a parameter or constant */
			subexpr = lsecond(oper->args);
			if (subexpr == NULL) {
				DEBUG((DEBUG_LEVEL, "no right side parameter"));
				return false;
			}

			DEBUG((DEBUG_LEVEL, "subexpr type %d: %s",
			      subexpr->type, nodeToString(subexpr)));

			switch (leftidx) {
			case VAR_KEY:
				rctx->where_flags |= PARAM_KEY | PARAM_CHANNEL;
				break;
			case VAR_FIELD:
				rctx->where_flags |= PARAM_FIELD;
				break;
			case VAR_ARRAY_FIELD:
				rctx->where_flags |= PARAM_ARRAY_FIELD;
				break;
			case VAR_MEMBER:
				rctx->where_flags |= PARAM_MEMBER;
				break;
			case VAR_INDEX:
				rctx->where_flags |= PARAM_INDEX;
				break;
			case VAR_SCORE:
				rctx->where_flags |= PARAM_SCORE;
				break;
			case VAR_TABLE_TYPE:
				rctx->where_flags |= PARAM_TABLE_TYPE;
				break;
			case VAR_S_VALUE:
				if (rctx->table_type == PG_REDIS_LIST) {
					rctx->where_flags |= PARAM_VALUE;
					break;
				}
				/* fall through for other table types */
			default:
				/*
				DEBUG((DEBUG_LEVEL, "unhandled left index: %d", leftidx));
				return false;
				*/
				ereport(ERROR,
				   (errcode(ERRCODE_FDW_ERROR),
				   errmsg("conditional left variable %s not permitted for table",
				          FIELD_NAMES[leftidx])));
			}

			if ((rctx->where_flags & (PARAM_SCORE | PARAM_INDEX)) ==
			    (PARAM_SCORE | PARAM_INDEX))
				ereport(ERROR,
			        (errcode(ERRCODE_FDW_ERROR),
			         errmsg("index and score are mutually exclusive")));


			if (subexpr->type == T_Param || subexpr->type == T_RelabelType ||
			    subexpr->type == T_FuncExpr) {
				struct redis_param_desc *pd;

				param = (Param *)subexpr;

				/* stash the parameter until we're ready to query */
				pd = (struct redis_param_desc *)palloc0(
				          sizeof(struct redis_param_desc));
				pd->var_field = leftidx;
				pd->paramid = param->paramid;
				pd->op = rop;
				pd->param = param;
				pd->value = NULL;
				pd->next = rctx->params;
				rctx->params = pd;

				DEBUG((DEBUG_LEVEL,
				      "parameter (%s type=%d, id=%d) stashed: param %p %s",
				      FIELD_NAMES[leftidx], param->paramtype,
				      param->paramid, param, nodeToString(param)));
			} else if (subexpr->type == T_Const) {
				constant = (Const *)subexpr;

				arg = datumToString(constant->constvalue, constant->consttype);

				DEBUG((DEBUG_LEVEL, "parameter (%s) assigned to %s",
				       arg, FIELD_NAMES[leftidx]));

				switch (leftidx) {
				case VAR_KEY:
				case VAR_CHANNEL:
					rctx->key = arg;
					break;
				case VAR_FIELD:
					rctx->where_conds.field = arg;
					break;
				case VAR_ARRAY_FIELD:
					rctx->where_conds.field = arg;
					break;
				case VAR_MEMBER:
				case VAR_S_VALUE:
					rctx->where_conds.s_value = arg;
					break;
				case VAR_INDEX:
				case VAR_SCORE:
					if (rop == ROP_EQ || rop == ROP_LT || rop == ROP_LTE) {
						rctx->where_conds.max = atoll(arg);
						rctx->where_conds.max_op = rop;
					} else if (rop == ROP_GT || rop == ROP_GTE) {
						rctx->where_conds.min = atoll(arg);
						rctx->where_conds.min_op = rop;
					} else {
						ereport(ERROR,
					        (errcode(ERRCODE_FDW_ERROR),
					         errmsg("operation unhandled for arg %s", arg)));
					}
					break;
				case VAR_TABLE_TYPE:
					rctx->where_conds.table_type = arg;
					break;
				default:
					DEBUG((DEBUG_LEVEL, "leftidx %d not a valid qual",
					      leftidx));
					return false;
				}

			} else {
				DEBUG((DEBUG_LEVEL, "unable to process subexpr type %d: %s",
				       subexpr->type, nodeToString(subexpr)));
				return false;
			}
		}

		break;

	case T_BoolExpr:
		boolexpr = (BoolExpr *)expr;

		DEBUG((DEBUG_LEVEL, "boolean expression %d, is-AND? %d",
		       boolexpr->boolop, boolexpr->boolop == AND_EXPR));

		if (boolexpr->boolop != AND_EXPR)
			return false;

		foreach(cell, boolexpr->args) {
			if (!redis_parse_where(rctx, foreignrel, (Expr *)lfirst(cell)))
				return false;
		}

		break;

	default:
		DEBUG((DEBUG_LEVEL, "type not handled %d", expr->type));
		return false;
	}

	return true;
}

static char *
redis_opt_string(DefElem *def, const char *key, char **field)
{
	if (strcmp(def->defname, key) == 0) {
		*field = defGetString(def);
		return *field;
	}
	return NULL;
}

static enum redis_data_type
redis_str_to_tabletype(const char *v) {
	if (strcmp(v, "string") == 0)
		return PG_REDIS_STRING;
	else if (strcmp(v, "hash") == 0)
		return PG_REDIS_HSET;
	else if (strcmp(v, "hmset") == 0 || strcmp(v, "mhash") == 0)
		return PG_REDIS_HMSET;
	else if (strcmp(v, "list") == 0)
		return PG_REDIS_LIST;
	else if (strcmp(v, "set") == 0)
		return PG_REDIS_SET;
	else if (strcmp(v, "zset") == 0)
		return PG_REDIS_ZSET;
	else if (strcmp(v, "len") == 0)
		return PG_REDIS_LEN;
	else if (strcmp(v, "ttl") == 0)
		return PG_REDIS_TTL;
	else if (strcmp(v, "publish") == 0)
		return PG_REDIS_PUBLISH;
	else if (strcmp(v, "keys") == 0)
		return PG_REDIS_KEYS;

	return PG_REDIS_INVALID;
}

static void
redis_get_table_options(Oid foreigntableid, struct redis_fdw_ctx *rctx)
{
	UserMapping *mapping;
	List     *options;
	ListCell *lc;
	bool o_port, o_db, o_ttype;

	o_port = o_db = o_ttype = false;

	/* Lookup foreign table catalog info. */
	rctx->table = GetForeignTable(foreigntableid);
	rctx->server = GetForeignServer(rctx->table->serverid);
	mapping = GetUserMapping(GetUserId(), rctx->table->serverid);

	/* parse options  */
	options = NIL;
	options = list_concat(options, rctx->server->options);
	if (mapping != NULL)
		 options = list_concat(options, mapping->options);
	options = list_concat(options, rctx->table->options);

	rctx->port = 6379;
	rctx->database = 0;
	foreach (lc, options) {
		DefElem *def = (DefElem *) lfirst(lc);
		char *v;

		if (rctx->key == NULL) {
			if (redis_opt_string(def, OPT_KEY, &rctx->key) != NULL) {
				rctx->where_flags |= PARAM_KEY;
				continue;
			}
			if (redis_opt_string(def, OPT_CHANNEL, &rctx->key) != NULL) {
				/* try "channel" instead for publish */
				rctx->where_flags |= PARAM_KEY;
				continue;
			}
		}

		if (rctx->keyprefix == NULL &&
		   redis_opt_string(def, OPT_KEYPREFIX, &rctx->keyprefix) != NULL)
			continue;

		if (rctx->host == NULL) {
			/* allow host or address */
			if (redis_opt_string(def, OPT_HOST, &rctx->host) == NULL)
				redis_opt_string(def, "address", &rctx->host);
			if (rctx->host != NULL)
				continue;
		}

		if (rctx->password == NULL &&
		   redis_opt_string(def, OPT_PASSWORD, &rctx->password) != NULL)
			continue;

		if (!o_port && redis_opt_string(def, OPT_PORT, &v) != NULL) {
			rctx->port = atoi(v);
			if (rctx->port <= 0)
				ereport(ERROR,
				        (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				         errmsg("invalid port (%s)", v)));
			o_port = true;
			continue;
		}

		if (!o_db && redis_opt_string(def, OPT_DATABASE, &v) != NULL) {
			rctx->database = atoi(v);
			if (rctx->database < 0)
				ereport(ERROR,
				        (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				         errmsg("invalid database (%s)", v)));
			o_db = true;
			continue;
		}

		if (!o_ttype && redis_opt_string(def, OPT_TABLETYPE, &v) != NULL) {
			rctx->table_type = redis_str_to_tabletype(v);
			if (rctx->table_type == PG_REDIS_INVALID)
				ereport(ERROR,
				        (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				         errmsg("invalid tabletype (%s)", v)));
			o_ttype = true;
		}
	}


	/* get columns information of table */
	get_psql_columns(foreigntableid, rctx);

	if (rctx->host == NULL)
		rctx->host = "127.0.0.1";

	rctx->where_conds.min = rctx->where_conds.max = -1;

	/* validate that the required columns are there for the table type */
	validate_redis_opts(rctx);
}

static redisContext *
redis_do_connect(struct redis_fdw_ctx *rctx)
{
	redisContext *ctx;
	redisReply *reply;
	struct timeval timeout = {1, 0};

	if (rctx->host[0] == '/') {
		ctx = redisConnectUnixWithTimeout(rctx->host, timeout);
	} else {
		ctx = redisConnectWithTimeout(rctx->host, rctx->port, timeout);
	}
	if (ctx == NULL) {
		ereport(ERROR,
		        (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
		         errmsg("redisConnectWithTimeout failed; no ctx returned")));
	} else if (ctx->err) {
		ereport(ERROR,
		        (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
		         errmsg("failed to connect to Redis: %d", ctx->err)));
	}

	if (rctx->password != NULL) {
		reply = redisCommand(ctx, "AUTH %s", rctx->password);
		if (reply == NULL) {
			redisFree(ctx);
			ereport(ERROR,
		        (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
		         errmsg("Redis authentication error: %d", ctx->err)));
		}

		freeReplyObject(reply);
	}

	if (rctx->database > 0) {
		reply = redisCommand(ctx, "SELECT %d", rctx->database);
		if (reply == NULL) {
			redisFree(ctx);

			ereport(ERROR,
		        (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
		         errmsg("Redis select database %d eror: %d", rctx->database,
			            ctx->err)));
		}
		freeReplyObject(reply);
	}

	rctx->r_ctx = ctx;

	return (ctx);
}

/*
 * redisGetForeignRelSize
 *		Estimate # of rows and width of the result of the scan
 */
static void
redisGetForeignRelSize(PlannerInfo *root,
					   RelOptInfo *baserel,
					   Oid foreigntableid)
{
	struct redis_fdw_ctx *rctx;
	redisContext *ctx;
	redisReply *reply;
	List     *conditions = baserel->baserestrictinfo;
	ListCell *lc;
	int       i;

	DEBUG((DEBUG_LEVEL, "*** %s", __FUNCTION__));

	/*
	 * Use redis_fdw_ctx to pass various information to subsequent functions
	 */
	rctx = (struct redis_fdw_ctx *) palloc0(sizeof(struct redis_fdw_ctx));
	rctx->root = root;

	redis_get_table_options(foreigntableid, rctx);

	if (rctx->key != NULL) {
		if (rctx->keyprefix != NULL) {
			rctx->pfxkey = (char *)palloc(strlen(rctx->keyprefix) +
			                              strlen(rctx->key) + 1);
			sprintf(rctx->pfxkey, "%s%s", rctx->keyprefix, rctx->key);
		} else {
			rctx->pfxkey = rctx->key;
		}
	}

	ctx = redis_do_connect(rctx);
	reply = NULL;

	/* if key has not yet been determined, then use DBSIZE */
	switch (rctx->table_type) {
	case PG_REDIS_STRING:
		baserel->rows = 1;
		break;

	case PG_REDIS_HSET:
		if (rctx->pfxkey != NULL)
			reply = redisCommand(ctx, "HLEN %s", rctx->pfxkey);
		else
			reply = redisCommand(ctx, "DBSIZE");
		break;

	case PG_REDIS_LIST:
		if (rctx->pfxkey != NULL)
			reply = redisCommand(ctx, "LLEN %s", rctx->pfxkey);
		else
			reply = redisCommand(ctx, "DBSIZE");
		break;
	
	case PG_REDIS_SET:
		if (rctx->pfxkey != NULL)
			reply = redisCommand(ctx, "SCARD %s", rctx->pfxkey);
		else
			reply = redisCommand(ctx, "DBSIZE");
		break;
	case PG_REDIS_ZSET:
		if (rctx->pfxkey != NULL)
			reply = redisCommand(ctx, "ZCARD %s", rctx->pfxkey);
		else
			reply = redisCommand(ctx, "DBSIZE");
		break;

	case PG_REDIS_HMSET:
	case PG_REDIS_LEN:
	case PG_REDIS_TTL:
	case PG_REDIS_PUBLISH:
		baserel->rows = 1;
		break;
	case PG_REDIS_KEYS:
		reply = redisCommand(ctx, "DBSIZE");
		break;
	default:
		break;
	}

	if (reply != NULL) {
		Assert(reply->type == REDIS_REPLY_INTEGER);
		baserel->rows = reply->integer;
		DEBUG((DEBUG_LEVEL, "number of items for key: %lld", reply->integer));
		freeReplyObject(reply);
	}

	if (conditions != NIL)
		rctx->pushdown_conds = (bool *)palloc(
		                              sizeof(bool) * list_length(conditions));

	rctx->where_conds.min = -1;
	rctx->where_conds.min_op = ROP_INVALID;
	rctx->where_conds.max = -1;
	rctx->where_conds.max_op = ROP_INVALID;

	i = 0;
	foreach (lc, conditions) {
		rctx->pushdown_conds[i] = redis_parse_where(rctx, baserel,
		                                ((RestrictInfo *)lfirst(lc))->clause);
		i++;
	}

	if (rctx->table_type != PG_REDIS_KEYS) {
		if ((rctx->where_flags & PARAM_KEY) == 0)
			ereport(ERROR,
			   (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
			    errmsg("\"%s\" missing in table option and WHERE clause",
			    rctx->table_type == PG_REDIS_PUBLISH ? "channel" : "key")));
	}

	redisFree(ctx);
	baserel->fdw_private = (void *) rctx;
}

/*
 * redisGetForeignPaths
 *      Create possible access paths for a scan on the foreign table
 *
 *      Currently we don't support any push-down feature, so there is only one
 *      possible access path, which simply returns all records in redis.
 */
static void
redisGetForeignPaths(PlannerInfo *root,
					 RelOptInfo *baserel,
					 Oid foreigntableid)
{
	struct redis_fdw_ctx *rctx;
	Cost startup_cost, total_cost;

	rctx = (struct redis_fdw_ctx *)baserel->fdw_private;

	DEBUG((DEBUG_LEVEL, "*** %s", __FUNCTION__));

	rctx->psql_cmd = root->parse->commandType;
	DEBUG((DEBUG_LEVEL, "psql_cmd: %d", rctx->psql_cmd));

	if (rctx->host[0] == '/')
		startup_cost = 2;
	else if (strcmp(rctx->host, "127.0.0.1") == 0 ||
	    strcmp(rctx->host, "localhost") == 0)
		startup_cost = 10;
	else
		startup_cost = 25;

	total_cost = startup_cost + baserel->rows;

	/* Create a ForeignPath node and add it as only possible path */
	add_path(baserel,
	   (Path *)create_foreignscan_path(root, baserel,
#if PG_VERSION_NUM >= 90600
	       NULL,    /* default pathtarget */
#endif
	       baserel->rows,
	       startup_cost,
	       total_cost,
	       NIL,     /* no pathkeys */
	       NULL,    /* no outer rel either */
#if PG_VERSION_NUM >= 90500
	       NULL,    /* no extra plan */
#endif
	       NIL));   /* no fdw_private data */
}

static ForeignScan *
redisGetForeignPlan(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid,
					ForeignPath *best_path,
					List *tlist,
#if PG_VERSION_NUM < 90500
					List *scan_clauses
#else
					List *scan_clauses,
					Plan *outer_plan
#endif
                   )
{
	struct redis_fdw_ctx *rctx;
	List *fdw_private;
	List *keep_clauses = NIL;
	List *params_list = NIL;
	ListCell *lc1, *lc2;
	struct redis_param_desc *rparam;

	rctx = (struct redis_fdw_ctx *)baserel->fdw_private;

	DEBUG((DEBUG_LEVEL, "*** %s", __FUNCTION__));

	fdw_private = redis_serialize_fdw(rctx);

	/* keep only those clauses not handled by redis */
	foreach(lc1, scan_clauses) {
		int i = 0;

		foreach(lc2, baserel->baserestrictinfo) {
			if (equal(lfirst(lc1), lfirst(lc2)) && !rctx->pushdown_conds[i]) {
				keep_clauses = lcons(lfirst(lc1), keep_clauses);
				break;
			}
			i++;
		}
	}

	/* remove the RestrictInfo node from all remaining clauses */
	keep_clauses = extract_actual_clauses(keep_clauses, false);

	DEBUG((DEBUG_LEVEL, "keep_clauses %s", nodeToString(keep_clauses)));

	/* Provide the list of parameters that redis can accept downstream.
	 *
	 * The actual values of the parameters are evaulated in
	 * IterateForeignScan when the query is executed.
	 */
	for (rparam = rctx->params; rparam != NULL; rparam = rparam->next)
		params_list = lappend(params_list, rparam->param);

	return make_foreignscan(tlist, keep_clauses, baserel->relid,
	                        params_list,
#if PG_VERSION_NUM < 90500
	                        fdw_private
#else
	                        fdw_private,
	                        NIL,             /* no custom tlist */
	                        NIL,             /* no remote quals */
	                        outer_plan
#endif
	                       );
}

static void
redisBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *fsplan = (ForeignScan *)node->ss.ps.plan;
	List *fdw_private = fsplan->fdw_private;
	EState   *estate;
	List     *exec_exprs;
	ListCell *lc;
	struct redis_param_desc *param;
	struct redis_fdw_ctx *rctx;
	int i;

	DEBUG((DEBUG_LEVEL, "*** %s", __FUNCTION__));

	rctx = redis_deserialize_fdw(fdw_private);
	node->fdw_state = (void *)rctx;

	DEBUG((DEBUG_LEVEL, " fdw_exprs: %s", nodeToString(fsplan->fdw_exprs)));

	/*
	 * Prepare parameters that need to be extracted from psql query.
	 */
#if PG_VERSION_NUM >= 100000
	exec_exprs = ExecInitExprList(fsplan->fdw_exprs, (PlanState *)node);
#else
	exec_exprs = (List *)ExecInitExpr((Expr *)fsplan->fdw_exprs,
	                                  (PlanState *)node);
#endif

	DEBUG((DEBUG_LEVEL, " exec_exprs: %s", nodeToString(exec_exprs)));

	DEBUG((DEBUG_LEVEL, "rctx: host %s, port %d\n"
	   "\tkey: %s\n"
	   "\tkeyprefix: %s\n"
	   "\tfield: %s, s_value: %s, table_type-param: %s\n"
	   "\ti_min: %ld, i_max: %ld\n"
	   "\ti_min_op: %d, i_max_op: %d\n",
	    rctx->host, rctx->port,
	    rctx->key,
	    rctx->keyprefix ? rctx->keyprefix : "",
	    rctx->where_conds.field ? rctx->where_conds.field : "NULL",
	    rctx->where_conds.s_value ? rctx->where_conds.s_value : "NULL",
	    rctx->where_conds.table_type ? rctx->where_conds.table_type : "NULL",
	    rctx->where_conds.min, rctx->where_conds.max,
	    rctx->where_conds.min_op, rctx->where_conds.max_op
	));


	/* get the type output functions for the parameters */
	for (i = 0; i < rctx->rtable.colcnt; i++) {
		pgsql_get_typio(rctx->rtable.columns[i].pgtype,
		                &rctx->rtable.columns[i].typoutput,
		                &rctx->rtable.columns[i].typinput);
	}

	DEBUG((DEBUG_LEVEL, "# params in exec_exprs: %d", list_length(exec_exprs)));

	/*
	 * stash parameter expressions until IterateForeignScan() where
	 * PostgreSQL will have the parameter results available for extraction.
	 */
	param = rctx->params;
	foreach(lc, exec_exprs) {
		ExprState *expr = (ExprState *) lfirst(lc);

		if (expr == NULL) {
			DEBUG((DEBUG_LEVEL, "  expr state NULL"));
			param = param->next;
			continue;
		}

		Assert(param != NULL);

		param->param = expr;
		param = param->next;
	}

	rctx->attmeta =
	           TupleDescGetAttInMetadata(node->ss.ss_currentRelation->rd_att);
	rctx->rowsdone = 0;

	estate = node->ss.ps.state;

	/* create a memory context for short-lived memory */
	rctx->temp_ctx = AllocSetContextCreate(estate->es_query_cxt,
		"redis_fdw temporary data",
#if PG_VERSION_NUM < 110000
		ALLOCSET_SMALL_MINSIZE,
		ALLOCSET_SMALL_INITSIZE,
		ALLOCSET_SMALL_MAXSIZE
#else
		ALLOCSET_DEFAULT_SIZES
#endif
		);
}

static int
redis_get_reply(redisReply *reply, char **str, int64_t *intval, bool *isnil) {
	*isnil = false;
	switch (reply->type) {
	case REDIS_REPLY_INTEGER:
		if (intval != NULL)
			*intval = reply->integer;
		break;
	case REDIS_REPLY_STRING:
		if (str != NULL)
			*str = reply->str;
		break;
	case REDIS_REPLY_NIL:
		if (isnil != NULL)
			*isnil = true;
		break;
	default:
		ereport(ERROR,
		   (errcode(ERRCODE_FDW_ERROR),
		   errmsg("unsupported reply type (%d) for command", reply->type)));
	}
	return reply->type;
}

static TupleTableSlot *
redisIterateForeignScan(ForeignScanState *node)
{
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	MemoryContext old_ctx;
	HeapTuple tuple;
	struct redis_fdw_ctx *rctx = (struct redis_fdw_ctx *)node->fdw_state;
	redisReply *reply;

	char vbuf[32];
	char *value = NULL;

	/* processed fields from redis result record */
	char *field = NULL;
	char *table_type = NULL;
	char *s_value = NULL;
	bool nil_value = false;
	int64_t i_value = 0;
	int64_t index = 0;
	char   *s_score = NULL;
	int64_t score = 0;

	int i;

	DEBUG((DEBUG_LEVEL, "*** %s", __FUNCTION__));

	MemoryContextReset(rctx->temp_ctx);

	if (rctx->rowsdone == 0 && rctx->rowcount == 0) {
		struct redis_param_desc *param;
		Datum datum;
		bool is_null;
		redisContext *ctx;

		/* param_flags contains Consts from parse_where() */
		if (rctx->where_flags & PARAM_EXPIRY) {
			elog(ERROR, "expiry not supported in WHERE clause");
		}

		/* fetch parameters and assign to context conditionals */
		for (param = rctx->params; param; param = param->next) {
			ExprState *expr_state = param->param;

#if PG_VERSION_NUM < 100000
			datum = ExecEvalExpr(expr_state, econtext, &is_null, NULL);
#else
			datum = ExecEvalExpr(expr_state, econtext, &is_null);
#endif

			if (is_null) {
				param->value = NULL;
			} else {
				param->value = datumToString(datum,
				                       exprType((Node *)expr_state->expr));

				DEBUG((DEBUG_LEVEL, "param %d \"%s\" value: %s",
				       param->paramid, FIELD_NAMES[param->var_field],
				       param->value));

				switch (param->var_field) {
				case VAR_KEY:
				case VAR_CHANNEL:
					rctx->where_flags |= PARAM_KEY | PARAM_CHANNEL;
					rctx->key = param->value;
					break;
				case VAR_FIELD:
					rctx->where_flags |= PARAM_FIELD;
					rctx->where_conds.field = param->value;
					break;
				case VAR_MEMBER:
					rctx->where_flags |= PARAM_MEMBER;
					rctx->where_conds.s_value = param->value;
					break;
				case VAR_MESSAGE:
					rctx->where_flags |= PARAM_MESSAGE;
					rctx->where_conds.s_value = param->value;
					break;
				case VAR_ARRAY_FIELD:
					rctx->where_flags |= PARAM_ARRAY_FIELD;
					rctx->where_conds.field = param->value;
					break;
				case VAR_MEMBERS:
					rctx->where_flags |= PARAM_MEMBERS;
					rctx->where_conds.s_value = param->value;
					break;
				case VAR_EXPIRY:
					elog(ERROR, "expiry not permitted in WHERE clause");
					break;
				case VAR_INDEX:
				case VAR_SCORE:
					rctx->where_flags |= PARAM_INDEX;
					if (param->op == ROP_EQ ||
					    param->op == ROP_LT ||
					    param->op == ROP_LTE) {
						rctx->where_conds.max = atoll(param->value);
						rctx->where_conds.max_op = param->op;
					} else if (param->op == ROP_GT || param->op == ROP_GTE) {
						rctx->where_conds.min = atoll(param->value);
						rctx->where_conds.min_op = param->op;
					} else {
						elog(ERROR, "(should not get here) invalid op %d",
						     param->op);
					}
					break;
				case VAR_TABLE_TYPE:
					rctx->where_conds.table_type = param->value;
					break;
				default:
					elog(ERROR, "condition not permitted: %s %s",
					     FIELD_NAMES[param->var_field], param->value);
				}
			}
		}

		/* Ensure key exists for key-mandatory tables */
		if (rctx->table_type != PG_REDIS_LEN && rctx->table_type != PG_REDIS_KEYS) {
			if (rctx->key == NULL)
				ereport(ERROR,
				    (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
				     errmsg("key is NULL")));
		}

		if (rctx->pfxkey == NULL) {
			if (rctx->keyprefix != NULL) {
				rctx->pfxkey = (char *)palloc(strlen(rctx->keyprefix) +
				                              strlen(rctx->key) + 1);
				sprintf(rctx->pfxkey, "%s%s", rctx->keyprefix, rctx->key);
			} else {
				rctx->pfxkey = rctx->key;
			}
		}

		/*
		 * For UPDATE and DELETE, shortcircuit return values so that only
		 * one row is provided, otherwise PgSQL will iterate through one
		 * row at a time while calling the Update/Delete handler if
		 * only "key" is specified in the WHERE clause.
		 */
		if (rctx->psql_cmd == CMD_UPDATE || rctx->psql_cmd == CMD_DELETE) {
			DEBUG((DEBUG_LEVEL,
			       "  short circuiting iterator for UPDATE/DELETE"));

			rctx->rowcount = 0;
			rctx->rowsdone = 1;

			if (rctx->where_flags & PARAM_FIELD) {
				DEBUG((DEBUG_LEVEL, "  setting field (%s)", rctx->where_conds.field));
				field = rctx->where_conds.field;
			}

			if (rctx->where_flags & PARAM_MEMBER) {
				DEBUG((DEBUG_LEVEL, "  setting member (%s)", rctx->where_conds.s_value));
				s_value = rctx->where_conds.s_value;
			}

			if (rctx->where_flags & PARAM_VALUE) {
				DEBUG((DEBUG_LEVEL, "  setting value (%s)", rctx->where_conds.s_value));
				s_value = rctx->where_conds.s_value;
			}

			if (rctx->where_flags & PARAM_INDEX) {
				if (rctx->where_conds.max_op != ROP_EQ) {
					elog(ERROR,
					     "UPDATE/DELETE only allows INDEX \"=\" operator");
				}

				DEBUG((DEBUG_LEVEL, "  setting index (%ld)", rctx->where_conds.max));
				index = rctx->where_conds.max;
			}

			old_ctx = MemoryContextSwitchTo(rctx->temp_ctx);
			ExecClearTuple(slot);
			goto fill_slot;
		}

		ctx = redis_do_connect(rctx);

		/* prefetch expiry if expiry column exists */
		if (rctx->rtable.expiry > 0 && rctx->table_type != PG_REDIS_TTL) {
			reply = redisCommand(ctx, "TTL %s", rctx->pfxkey);
			if (reply == NULL) {
				ERR_CLEANUP(rctx->r_reply, rctx->r_ctx,
				    (ERROR, "NULL reply from redis"));

			} else if (reply->type == REDIS_REPLY_ERROR) {
				char *errmsg;

				errmsg = pstrdup(reply->str);
				ERR_CLEANUP(reply, rctx->r_ctx,
				    (ERROR, "redis replied error on TTL: %s", errmsg));
			}

			rctx->expiry = reply->integer;
			freeReplyObject(reply);
			reply = NULL;
		}

		/* build query to send to redis */
		switch (rctx->table_type) {
		case PG_REDIS_STRING:
			/* GET */
			DEBUG((DEBUG_LEVEL, "GET %s", rctx->pfxkey));
			rctx->r_reply = redisCommand(ctx, "GET %s", rctx->pfxkey);
			rctx->cmd = REDIS_GET;
			break;
		case PG_REDIS_HSET:
			if (rctx->rtable.field <= 0 || rctx->where_conds.field == NULL) {
				/*
				 * field not in WHERE, then get all fields+values for key.
				 * redis will return an array of field, value, ...
				 */
				DEBUG((DEBUG_LEVEL, "HGETALL %s", rctx->pfxkey));
				rctx->r_reply = redisCommand(ctx, "HGETALL %s", rctx->pfxkey);
				rctx->cmd = REDIS_HGETALL;
			} else {
				field = rctx->where_conds.field;
				DEBUG((DEBUG_LEVEL, "HGET %s %s", rctx->pfxkey, field));
				rctx->r_reply = redisCommand(ctx, "HGET %s %s",
				                             rctx->pfxkey, field);
				rctx->cmd = REDIS_HGET;
			}
			break;
		case PG_REDIS_HMSET:
			/* convert into redis array for request */
			if (rctx->where_conds.field != NULL) {
				DEBUG((DEBUG_LEVEL, "HMGET %s %s", rctx->pfxkey,
				       rctx->where_conds.field));
				rctx->r_reply = redis_do_hmget(ctx, rctx->pfxkey,
				                           pstrdup(rctx->where_conds.field));
				rctx->cmd = REDIS_HMGET;
			} else {
				DEBUG((DEBUG_LEVEL, "HGETALL %s", rctx->pfxkey));
				rctx->r_reply = redisCommand(ctx, "HGETALL %s", rctx->pfxkey);
				rctx->cmd = REDIS_HGETALL;
			}
			if (rctx->r_reply == NULL)
				ERR_CLEANUP(rctx->r_reply, rctx->r_ctx,
				    (ERROR, "NULL reply from redis"));
			break;
		case PG_REDIS_LIST:
			if (rctx->where_conds.s_value != NULL) {
				DEBUG((DEBUG_LEVEL, "Ignoring get list item by value"));
				rctx->cmd = REDIS_DISCARD_RESULT;
				rctx->rowcount = 0;
				rctx->rowsdone = 1;
			} else if (rctx->where_conds.max >= 0) {
				DEBUG((DEBUG_LEVEL, "LINDEX %s %ld", rctx->pfxkey,
				       rctx->where_conds.max));
				rctx->r_reply = redisCommand(ctx, "LINDEX %s %ld",
				                         rctx->pfxkey, rctx->where_conds.max);
				rctx->cmd = REDIS_LINDEX;
			} else {
				int64_t n;

				DEBUG((DEBUG_LEVEL, "LLEN %s", rctx->pfxkey));
				rctx->r_reply = redisCommand(ctx, "LLEN %s", rctx->pfxkey);
				if (rctx->r_reply == NULL)
					ERR_CLEANUP(rctx->r_reply, rctx->r_ctx,
					    (ERROR, "NULL reply from redis"));

				n = rctx->r_reply->integer;
				if (n <= 0) {
					rctx->cmd = REDIS_DISCARD_RESULT;
					rctx->rowcount = 0;
					rctx->rowsdone = 1;
				} else {
					DEBUG((DEBUG_LEVEL, "LRANGE %s %d %ld",
					      rctx->pfxkey, 0, n));
					rctx->r_reply = redisCommand(ctx, "LRANGE %s %d %lld",
					                             rctx->pfxkey, 0, n);
					rctx->cmd = REDIS_LRANGE;
				}
			}
			break;
		case PG_REDIS_SET:
			if (rctx->where_flags & PARAM_MEMBER) {
				DEBUG((DEBUG_LEVEL, "SISMEMBER %s %s", rctx->pfxkey,
				      rctx->where_conds.s_value));
				rctx->r_reply = redisCommand(ctx, "SISMEMBER %s %s",
				                     rctx->pfxkey, rctx->where_conds.s_value);
				rctx->cmd = REDIS_SISMEMBER;
			} else {
				DEBUG((DEBUG_LEVEL, "SMEMBERS %s", rctx->pfxkey));
				rctx->r_reply = redisCommand(ctx, "SMEMBERS %s", rctx->pfxkey);
				rctx->cmd = REDIS_SMEMBERS;
			}
			break;
		case PG_REDIS_ZSET:
			/*
			 * zrange rules:
			 *    start   stop
			 *      n1     n2    inclusive from >= n1 and <= n2
			 *      -1     n2    only n2
			 *      0      -1    all values
			 *
			 * Column output filters:
			 *
			 * - if WHERE member = 's', then perform ZRANK <key> <member>
			 * - else perform ZRANGE <key> <min> <max> WITHSCORES
			 */

			if (rctx->where_flags & PARAM_MEMBER) {
				DEBUG((DEBUG_LEVEL, "ZRANK %s %s", rctx->pfxkey,
				      rctx->where_conds.s_value));
				rctx->r_reply = redisCommand(ctx, "ZRANK %s %s",
				                     rctx->pfxkey, rctx->where_conds.s_value);
				rctx->cmd = REDIS_ZRANK;
			} else if (rctx->where_flags & PARAM_SCORE) {
				if (rctx->where_conds.min < 0)
					rctx->where_conds.min = 0;
				if (rctx->where_conds.min_op == ROP_GT)
					rctx->where_conds.min++;
				if (rctx->where_conds.max_op == ROP_LT)
					rctx->where_conds.max--;
				else if (rctx->where_conds.max_op == ROP_EQ)
					rctx->where_conds.min = rctx->where_conds.max;

				if (rctx->where_conds.max >= rctx->where_conds.min) {
					DEBUG((DEBUG_LEVEL,
					      "ZRANGEBYSCORE %s %ld %ld WITHSCORES",
					       rctx->pfxkey,
					       rctx->where_conds.min, rctx->where_conds.max));
					rctx->r_reply = redisCommand(ctx,
					         "ZRANGEBYSCORE %s %ld %ld WITHSCORES",
					         rctx->pfxkey,
					         rctx->where_conds.min, rctx->where_conds.max);
				} else {
					DEBUG((DEBUG_LEVEL,
					      "ZRANGEBYSCORE %s %ld +inf WITHSCORES",
					       rctx->pfxkey, rctx->where_conds.min));
					rctx->r_reply = redisCommand(ctx,
					         "ZRANGEBYSCORE %s %ld +inf WITHSCORES",
					         rctx->pfxkey, rctx->where_conds.min);
				}
				rctx->cmd = REDIS_ZRANGEBYSCORE;
			} else {
				if (rctx->where_conds.min < 0)
					rctx->where_conds.min = 0;
				if (rctx->where_conds.min_op == ROP_GT)
					rctx->where_conds.min++;
				if (rctx->where_conds.max_op == ROP_LT)
					rctx->where_conds.max--;
				else if (rctx->where_conds.max_op == ROP_EQ)
					rctx->where_conds.min = rctx->where_conds.max;

				DEBUG((DEBUG_LEVEL, "ZRANGE %s %ld %ld WITHSCORES",
				       rctx->pfxkey,
				       rctx->where_conds.min, rctx->where_conds.max));
				rctx->r_reply = redisCommand(ctx,
				                "ZRANGE %s %ld %ld WITHSCORES", rctx->pfxkey,
				                rctx->where_conds.min, rctx->where_conds.max);
				rctx->cmd = REDIS_ZRANGE;
			}

			break;

		case PG_REDIS_LEN:
			if (rctx->pfxkey && rctx->where_conds.table_type) {
				table_type = rctx->where_conds.table_type;

				/* table type is in qual */
				if (strcmp(table_type, "string") == 0) {
					DEBUG((DEBUG_LEVEL, "STRLEN %s", rctx->pfxkey));
					rctx->r_reply = redisCommand(ctx, "STRLEN %s",
					                             rctx->pfxkey);
				} else if (strcmp(table_type, "hash") == 0) {
					DEBUG((DEBUG_LEVEL, "HLEN %s", rctx->pfxkey));
					rctx->r_reply = redisCommand(ctx, "HLEN %s",
					                             rctx->pfxkey);
				} else if (strcmp(table_type, "list") == 0) {
					DEBUG((DEBUG_LEVEL, "LLEN %s", rctx->pfxkey));
					rctx->r_reply = redisCommand(ctx, "LLEN %s",
					                             rctx->pfxkey);
				} else if (strcmp(table_type, "set") == 0) {
					DEBUG((DEBUG_LEVEL, "SCARD %s", rctx->pfxkey));
					rctx->r_reply = redisCommand(ctx, "SCARD %s",
					                             rctx->pfxkey);
				} else if (strcmp(table_type, "zset") == 0) {
					DEBUG((DEBUG_LEVEL, "ZCARD %s", rctx->pfxkey));
					rctx->r_reply = redisCommand(ctx, "ZCARD %s",
					                             rctx->pfxkey);
				} else {
					ereport(ERROR,
					    (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
					     errmsg("permitted redis table types: "
					            "string, hash, list, set, zset")));
				}
				rctx->cmd = REDIS_LEN;
			} else {
				DEBUG((DEBUG_LEVEL, "DBSIZE"));
				rctx->r_reply = redisCommand(ctx, "DBSIZE");
				rctx->expiry = 0;
				rctx->cmd = REDIS_DBSIZE;
			}
			break;
		case PG_REDIS_TTL:
			DEBUG((DEBUG_LEVEL, "TTL %s", rctx->pfxkey));
			rctx->r_reply = redisCommand(ctx, "TTL %s", rctx->pfxkey);
			rctx->cmd = REDIS_TTL;
			break;
		case PG_REDIS_PUBLISH:
			DEBUG((DEBUG_LEVEL, "PUBSUB NUMSUB %s", rctx->pfxkey));
			rctx->r_reply = redisCommand(ctx, "PUBSUB NUMSUB %s", rctx->pfxkey);
			rctx->cmd = REDIS_PSNUMSUB;
			break;
		case PG_REDIS_KEYS:
			if (NULL != rctx->key) {
				DEBUG((DEBUG_LEVEL, "KEYS %s", rctx->key));
				rctx->r_reply = redisCommand(ctx, "KEYS %s", rctx->key);
			} else {
				DEBUG((DEBUG_LEVEL, "KEYS *"));
				rctx->r_reply = redisCommand(ctx, "KEYS *");
			}
			rctx->cmd = REDIS_KEYS;
			break;

		default:
			break;
		}

		if (rctx->r_reply != NULL) {
			switch (rctx->r_reply->type) {
			case REDIS_REPLY_INTEGER:
			case REDIS_REPLY_STRING:
				rctx->rowcount = 1;
				break;
			case REDIS_REPLY_ARRAY:
				rctx->rowcount = rctx->r_reply->elements;
				DEBUG((DEBUG_LEVEL, "redis array elements: %ld",
				      rctx->rowcount));
				break;
			case REDIS_REPLY_STATUS:
				DEBUG((DEBUG_LEVEL, "redis status: %s", rctx->r_reply->str));
				break;
			case REDIS_REPLY_NIL:
				DEBUG((DEBUG_LEVEL, "redis nil reply"));
				rctx->rowcount = 0;
				break;
			default:
				ERR_CLEANUP(rctx->r_reply, rctx->r_ctx, 
				    (ERROR, "redis error: %s", rctx->r_reply->str));
			}

			dump_reply(rctx->r_reply, 0);
		} else {
			ereport(ERROR,
		        (errcode(ERRCODE_FDW_ERROR), errmsg("redis null reply")));
		}
	}

	/* initialize virtual tuple */
	old_ctx = MemoryContextSwitchTo(rctx->temp_ctx);
	ExecClearTuple(slot);

	if (rctx->rowcount <= 0 || rctx->cmd == REDIS_DISCARD_RESULT) {
		if (rctx->r_reply != NULL) {
			freeReplyObject(rctx->r_reply);
			rctx->r_reply = NULL;
		}

		if (rctx->r_ctx != NULL) {
			redisFree(rctx->r_ctx);
			rctx->r_ctx = NULL;
		}

		MemoryContextSwitchTo(old_ctx);
		return slot;
	}

	reply = rctx->r_reply;

	/* extract one record from the result */
	switch (rctx->cmd) {
	case REDIS_GET:
	case REDIS_HGET:
	case REDIS_LINDEX:
	case REDIS_SCARD:
	case REDIS_ZCARD:
	case REDIS_TTL:
	case REDIS_DBSIZE:
	case REDIS_LEN:
	case REDIS_HLEN:
	case REDIS_LLEN:
	case REDIS_STRLEN:
		redis_get_reply(reply, &s_value, &i_value, &nil_value);
		rctx->rowcount--;
		rctx->rowsdone++;
		break;

	case REDIS_ZRANK:
		redis_get_reply(reply, &s_value, &index, &nil_value);
		rctx->rowcount--;
		rctx->rowsdone++;
		break;

	case REDIS_LRANGE:
		Assert(rctx->r_reply->elements >= rctx->rowsdone+1);
		Assert(rctx->r_reply->element != NULL);

		rctx->rowcount--;
		rctx->where_conds.max = rctx->rowsdone;
		reply = rctx->r_reply->element[rctx->rowsdone++];
		redis_get_reply(reply, &s_value, &i_value, &nil_value);
		break;

	case REDIS_SISMEMBER:
		redis_get_reply(reply, &s_value, &i_value, &nil_value);

		/* If no match, then set nil_value to true */
		nil_value = (i_value == 0);
		s_value = rctx->where_conds.s_value;
		rctx->rowcount--;
		rctx->rowsdone++;
		break;

	case REDIS_HMGET:
		/* Redis returns an array of values; convert to psql array */
		redisarray_to_psqlarray(rctx->r_reply, NULL, &s_value);
		rctx->rowcount = 0;
		rctx->rowsdone++;
		break;

	case REDIS_ZRANGE:
		if (rctx->where_conds.max_op == ROP_EQ)
			index = rctx->where_conds.max;
		else
			index = rctx->where_conds.min + rctx->rowsdone/2;
		/* fall-through */
	case REDIS_ZRANGEBYSCORE:
		Assert(rctx->r_reply->elements >= rctx->rowsdone+1);
		Assert(rctx->r_reply->element != NULL);

		/*
		 * if cmd == ZRANGEBYSCORE, then index is not usable because
		 * redis doesn't return the index of the item XXX return -1 or NULL?
		 */

		reply = rctx->r_reply->element[rctx->rowsdone++];
		redis_get_reply(reply, &s_value, &i_value, &nil_value);
		
		reply = rctx->r_reply->element[rctx->rowsdone++];
		redis_get_reply(reply, &s_score, &score, &nil_value);
		rctx->rowcount -= 2;
		break;

	case REDIS_SMEMBERS:
		reply = rctx->r_reply->element[rctx->rowsdone];
		redis_get_reply(reply, &s_value, &i_value, &nil_value);
		rctx->rowcount--;
		rctx->rowsdone++;
		break;

	case REDIS_HGETALL:
		if (rctx->table_type == PG_REDIS_HSET) {
			Assert(rctx->r_reply->elements >= rctx->rowsdone+1);
			Assert(rctx->r_reply->element != NULL);

			reply = rctx->r_reply->element[rctx->rowsdone++];
			field = reply->str;
		
			reply = rctx->r_reply->element[rctx->rowsdone++];
			redis_get_reply(reply, &s_value, &i_value, &nil_value);
			rctx->rowcount -= 2;
		} else {
			/* HMSET - return as two arrays: field[] and value[] */
			redisarray_to_psqlarray(rctx->r_reply,
			                        &rctx->where_conds.field, &s_value);
			rctx->rowcount = 0;
			rctx->rowsdone++;
		}
		break;

	case REDIS_PSNUMSUB:
		Assert(rctx->r_reply->elements > 1);
		Assert(rctx->r_reply->element != NULL);

		/* first array item is channel, second is #subscribed */
		i_value = rctx->r_reply->element[1]->integer;
		rctx->rowsdone = 1;
		rctx->rowcount = 0;
		break;

	case REDIS_KEYS:
		reply = rctx->r_reply->element[rctx->rowsdone];
		redis_get_reply(reply, &s_value, &i_value, &nil_value);
		rctx->rowcount--;
		rctx->rowsdone++;
		rctx->key = s_value;
		break;

	default:
		break;
	}

	if (nil_value) {
		if (rctx->r_reply != NULL) {
			freeReplyObject(rctx->r_reply);
			rctx->r_reply = NULL;
		}
		if (rctx->r_ctx != NULL) {
			redisFree(rctx->r_ctx);
			rctx->r_ctx = NULL;
		}
		rctx->rowcount = 0;
		rctx->rowsdone = 0;

		MemoryContextSwitchTo(old_ctx);
		return slot;
	}

fill_slot:
	rctx->slot_values = (char **)palloc(sizeof(char*)*rctx->rtable.colcnt);
	for (i = 0; i < rctx->rtable.colcnt; i++) {
		if (rctx->rtable.columns[i].var_field < 0) {
			rctx->slot_values[i] = NULL;
			continue;
		}

		switch (rctx->rtable.columns[i].var_field) {
		case VAR_KEY:
		case VAR_CHANNEL:
			value = rctx->key;
			break;
		case VAR_FIELD:
			value = field;
			break;
		case VAR_ARRAY_FIELD:
			value = rctx->where_conds.field;
			break;
		case VAR_S_VALUE:
		case VAR_MESSAGE:
			value = s_value;
			break;
		case VAR_SARRAY_VALUE:
			value = s_value;
			break;
		case VAR_I_VALUE:
			snprintf(vbuf, sizeof(vbuf), "%ld", i_value);
			value = pstrdup(vbuf);
			break;
		case VAR_MEMBER:
			if (rctx->cmd == REDIS_ZRANK) {
				value = rctx->where_conds.s_value;
			} else {
				value = s_value;
			}
			break;
		case VAR_MEMBERS:
			break;
		case VAR_EXPIRY:
			if (rctx->cmd == REDIS_TTL) {
				snprintf(vbuf, sizeof(vbuf), "%ld", i_value);
				value = pstrdup(vbuf);
			} else {
				snprintf(vbuf, sizeof(vbuf), "%ld", rctx->expiry);
				value = pstrdup(vbuf);
			}
			break;
		case VAR_INDEX:
			if (rctx->psql_cmd == CMD_UPDATE ||
			    rctx->psql_cmd == CMD_DELETE) {
				if (!(rctx->where_flags & PARAM_INDEX)) {
					value = NULL;
				} else {
					snprintf(vbuf, sizeof(vbuf), "%ld", index);
					value = pstrdup(vbuf);
				}
			} else if (rctx->table_type == PG_REDIS_LIST) {
				snprintf(vbuf, sizeof(vbuf), "%ld", rctx->where_conds.max);
				value = pstrdup(vbuf);
			} else if ( rctx->table_type == PG_REDIS_ZSET) {
				snprintf(vbuf, sizeof(vbuf), "%ld", index);
				value = pstrdup(vbuf);
			} else {
				snprintf(vbuf, sizeof(vbuf), "%d", 0);
				value = pstrdup(vbuf);
			}
			break;
		case VAR_SCORE:
			if (rctx->table_type == PG_REDIS_ZSET) {
				if (s_score != NULL)
					value = pstrdup(s_score);
				else  {
					snprintf(vbuf, sizeof(vbuf), "%ld", score);
					value = pstrdup(vbuf);
				}
			} else {
				snprintf(vbuf, sizeof(vbuf), "%d", 0);
				value = pstrdup(vbuf);
			}
			break;
		case VAR_LEN:
			snprintf(vbuf, sizeof(vbuf), "%ld", i_value);
			value = pstrdup(vbuf);
			break;
		case VAR_TABLE_TYPE:
			if (table_type != NULL)
				value = table_type;
			else
				value = "*";
			break;
		default:
			DEBUG((DEBUG_LEVEL,
			       "should not get here - unknown col[%d] varfield %d",
			       i, rctx->rtable.columns[i].var_field));
		}

		rctx->slot_values[i] = value;
	}

	tuple = BuildTupleFromCStrings(rctx->attmeta, rctx->slot_values);
	RFDW_VERDEP_ExecStoreTuple(tuple, slot, false);

	MemoryContextSwitchTo(old_ctx);
	return slot;
}

static void
redisReScanForeignScan(ForeignScanState *node)
{
	struct redis_fdw_ctx *rctx;

	DEBUG((DEBUG_LEVEL, "*** %s", __FUNCTION__));

	rctx = (struct redis_fdw_ctx *)node->fdw_state;

	if (rctx->r_reply != NULL) {
		freeReplyObject(rctx->r_reply);
		rctx->r_reply = NULL;
	}

	if (rctx->r_ctx != NULL) {
		redisFree(rctx->r_ctx);
		rctx->r_ctx = NULL;
	}

	rctx->rowcount = 0;
	rctx->rowsdone = 0;
}

static void
redisEndForeignScan(ForeignScanState *node)
{
	struct redis_fdw_ctx *rctx;

	DEBUG((DEBUG_LEVEL, "*** %s", __FUNCTION__));

	rctx = (struct redis_fdw_ctx *)node->fdw_state;
	if (rctx->r_reply != NULL) {
		freeReplyObject(rctx->r_reply);
		rctx->r_reply = NULL;
	}

	if (rctx->r_ctx != NULL) {
		redisFree(rctx->r_ctx);
		rctx->r_ctx = NULL;
	}

	rctx->rowcount = 0;
	rctx->rowsdone = 0;
}

#ifdef WRITE_API

/*
 * redisAddForeignUpdateTargets
 *		Add resjunk column(s) needed for update/delete on a foreign table
 */
static void
redisAddForeignUpdateTargets(Query *parsetree,
							 RangeTblEntry *target_rte,
							 Relation target_relation)
{
	Oid relid = RelationGetRelid(target_relation);
	TupleDesc tupdesc = target_relation->rd_att;
	int i;
	ForeignTable *tbl;
	List *options;
	ListCell *option;
	enum redis_data_type table_type = PG_REDIS_INVALID;
	int param_flags = 0;

	DEBUG((DEBUG_LEVEL, "*** %s", __FUNCTION__));

	/* identify table types; error if they are not updatable */
	tbl = GetForeignTable(relid);
	options = NIL;
	options = list_concat(options, tbl->options);
	foreach (option, options) {
		DefElem *def = (DefElem *) lfirst(option);
		char *v;

		redis_opt_string(def, OPT_TABLETYPE, &v);	
		if (v != NULL) {
			table_type = redis_str_to_tabletype(v);
			if (table_type == PG_REDIS_INVALID)
				ereport(ERROR,
				        (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				         errmsg("invalid tabletype (%s)", v)));
			break;
		}

		redis_opt_string(def, OPT_KEY, &v);
		if (v != NULL)
			param_flags = PARAM_KEY;
	}

	if (table_type == PG_REDIS_INVALID)
		ereport(ERROR,
		        (errcode(ERRCODE_FDW_ERROR), errmsg("table type not found")));

	if (table_type == PG_REDIS_KEYS)
		ereport(ERROR,
		        (errcode(ERRCODE_FDW_ERROR), errmsg("table read-only")));

	if (table_type == PG_REDIS_PUBLISH)
		ereport(ERROR,
		        (errcode(ERRCODE_FDW_ERROR),
		        errmsg("only INSERT is permitted for PUBLISH")));

	/* fetch columns that are keys for the table to be used in UPDATE/DELETE
	 *  where clause,
	 *     e.g. STRING: key
	 *     HASH: key field fields
	 *     LIST: key index value
	 *     SET: key member
	 *     ZSET: key score member
	 */
	for (i = 0; i < tupdesc->natts; i++) {
#if PG_VERSION_NUM < 110000
		Form_pg_attribute att = tupdesc->attrs[i];
#else
		Form_pg_attribute att = &tupdesc->attrs[i];
#endif
		AttrNumber attrno = att->attnum;
		Var *var;
		TargetEntry *tle;
		char *colname;
		int   colkey;
		bool  skip;

		colname = NameStr(att->attname);

		/* check if there is a redis_param remapping of the column name */
		options = GetForeignColumnOptions(relid, attrno);
		foreach (option, options) {
			DefElem *def = (DefElem *)lfirst(option);

			if (strcmp(def->defname, OPT_REDIS) == 0) {
				colname = ((Value *)(def->arg))->val.str;

				DEBUG((DEBUG_LEVEL, "column remapped (%s) -> (%s)",
				        NameStr(att->attname), colname));

				break;
			}
		}


		/* allowed "where" attributes */
		colkey = 0;
		if (strcmp(colname, "key") == 0) {
			colkey = PARAM_KEY;
			param_flags |= PARAM_KEY;
		} else if (strcmp(colname, "field") == 0) {
			colkey = PARAM_FIELD;
			param_flags |= PARAM_FIELD;
		} else if (strcmp(colname, "index") == 0) {
			colkey = PARAM_INDEX;
			param_flags |= PARAM_INDEX;
		} else if (strcmp(colname, "member") == 0) {
			colkey = PARAM_MEMBER;
			param_flags |= PARAM_MEMBER;
		} else if (strcmp(colname, "value") == 0) {
			colkey = PARAM_VALUE;
			param_flags |= PARAM_VALUE;
		} else {
			/* skip */
			continue;
		}

		skip = false;

		/* is this "WHERE" attr permitted for the table type? */
		switch (table_type) {
		case PG_REDIS_STRING:
			if (colkey & ~PARAM_KEY)
				skip = true;
			break;
		case PG_REDIS_HSET:
		case PG_REDIS_HMSET:
			if (colkey & ~(PARAM_KEY | PARAM_FIELD))
				skip = true;
			break;
		case PG_REDIS_LIST:
			if (colkey & ~(PARAM_KEY | PARAM_INDEX | PARAM_VALUE))
				skip = true;	
			break;
		case PG_REDIS_SET:
		case PG_REDIS_ZSET:
			if (colkey & ~(PARAM_KEY | PARAM_MEMBER))
				skip = true;	
			break;
		default:
			/* shouldn't get here */
			break;
		}

		/* column is not a searchable key */
		if (skip)
			continue;

		/* make a Var representing the desired value */
		var = makeVar(parsetree->resultRelation,
			attrno,
			att->atttypid,
			att->atttypmod,
			att->attcollation,
			0);

		/* wrap it in a resjunk TLE */
		tle = makeTargetEntry((Expr *)var,
			list_length(parsetree->targetList) + 1,
			pstrdup(colname),
			true);
		parsetree->targetList = lappend(parsetree->targetList, tle);
	}
}

static List *
redisPlanForeignModify(PlannerInfo *root,
					   ModifyTable *plan,
					   Index resultRelation,
					   int subplan_index)
{
	struct redis_fdw_ctx *rctx;
	CmdType        operation = plan->operation;
	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
	Relation       rel = NULL;
	Bitmapset     *tmpset;
	List          *targetAttrs = NIL;
	List          *returningList = NIL;
	ListCell      *lc;
	int            i;

	DEBUG((DEBUG_LEVEL, "*** %s", __FUNCTION__));

	rctx = (struct redis_fdw_ctx *)palloc0(sizeof(struct redis_fdw_ctx));

	/* check if foreign table is scanned */
	if (resultRelation < root->simple_rel_array_size &&
	    root->simple_rel_array[resultRelation] != NULL) {
		struct redis_fdw_ctx *src;

		DEBUG((DEBUG_LEVEL, "foreign table already scanned"));

		/* copy parsed fields */
		src = (struct redis_fdw_ctx *)
		      root->simple_rel_array[resultRelation]->fdw_private;

		memcpy(rctx, src, sizeof(struct redis_fdw_ctx));

		rctx->where_flags = src->where_flags;

		/* identify if mandatory WHERE clauses have been provided */

		if ((src->where_flags & PARAM_KEY) == 0)
			ereport(ERROR,
		        (errcode(ERRCODE_FDW_COLUMN_NAME_NOT_FOUND),
			     errmsg("mandatory \"WHERE key = x\" or table option \"key\"")));

		if (operation == CMD_UPDATE) {
			switch (src->table_type) {
			case PG_REDIS_STRING:
			case PG_REDIS_TTL:
				break;
			case PG_REDIS_HSET:
				if ((src->where_flags & PARAM_FIELD) != PARAM_FIELD)
					ereport(ERROR,
				       (errcode(ERRCODE_FDW_COLUMN_NAME_NOT_FOUND),
					    errmsg("mandatory \"WHERE key = x AND field = x\"")));
				break;
			case PG_REDIS_LIST:
				if ((src->where_flags & PARAM_INDEX) != PARAM_INDEX)
					ereport(ERROR,
				        (errcode(ERRCODE_FDW_COLUMN_NAME_NOT_FOUND),
					     errmsg("mandatory \"WHERE key = x AND index = x\"")));

				break;
			case PG_REDIS_SET:
			case PG_REDIS_ZSET:
				if ((src->where_flags & PARAM_MEMBER) != PARAM_MEMBER)
					ereport(ERROR,
				        (errcode(ERRCODE_FDW_COLUMN_NAME_NOT_FOUND),
					     errmsg("mandatory \"WHERE key = x AND member = x\"")));
				break;
			case PG_REDIS_PUBLISH:
				/* INSERT only */
				break;
			default:
				ereport(ERROR,
				     (errcode(ERRCODE_FDW_ERROR),
					  errmsg("table type %s is read-only",
				             FIELD_NAMES[src->table_type])));
				break;
			}
		}

		rctx->r_ctx = NULL;
		rctx->r_reply = NULL;

		rctx->root = root;
		rctx->host = src->host ? pstrdup(src->host) : NULL;
		rctx->password = src->password ? pstrdup(src->password) : NULL;
		rctx->key = src->key ? pstrdup(src->key) : NULL;
		rctx->keyprefix = src->keyprefix ? pstrdup(src->keyprefix) : NULL;
		rctx->pfxkey = src->pfxkey ? pstrdup(src->pfxkey) : NULL;

		rctx->rowcount = 0;
		rctx->rowsdone = 0;

		rctx->pushdown_conds = NULL;
		rctx->params = NULL;

		rctx->rtable.columns = (struct redis_column *)
		             palloc(sizeof(struct redis_column) * src->rtable.colcnt);
		memcpy(rctx->rtable.columns, src->rtable.columns,
		       sizeof(struct redis_column) * src->rtable.colcnt);
	} else {
		rctx->root = root;
		redis_get_table_options(rte->relid, rctx);
	}

	/* identify which parameters are being set/provided */
	switch (operation) {
	case CMD_INSERT:
		DEBUG((DEBUG_LEVEL, "*** %s (INSERT)", __FUNCTION__));

		if (rctx->table_type == PG_REDIS_TTL) {
			elog(ERROR, "cannot insert into table");
		}

		for (i = 0; i < rctx->rtable.colcnt; i++) {
			/* add param */
			struct redis_param_desc *param;

			param = (struct redis_param_desc *)
	       		 palloc(sizeof(struct redis_param_desc));

			param->var_field = rctx->rtable.columns[i].orig_var_field;
			param->paramid = i;
			param->param = NULL;
			param->value = NULL;

			switch (param->var_field) {
			case VAR_KEY:
			case VAR_CHANNEL:
				rctx->param_flags |= PARAM_KEY | PARAM_CHANNEL;
				break;
			case VAR_FIELD:
				rctx->param_flags |= PARAM_FIELD;
				break;
			case VAR_ARRAY_FIELD:
				rctx->param_flags |= PARAM_ARRAY_FIELD;
				break;
			case VAR_MEMBER:
				rctx->param_flags |= PARAM_MEMBER;
				break;
			case VAR_INDEX:
				rctx->param_flags |= PARAM_INDEX;
				break;
			case VAR_SCORE:
				rctx->param_flags |= PARAM_SCORE;
				break;
			case VAR_EXPIRY:
				rctx->param_flags |= PARAM_EXPIRY;
				break;
			case VAR_S_VALUE:
			case VAR_I_VALUE:
				rctx->param_flags |= PARAM_VALUE;
				break;
			case VAR_MESSAGE:
				rctx->param_flags |= PARAM_MESSAGE;
			default:
				DEBUG((DEBUG_LEVEL, "skipping parameter"));
				break;
			}

			DEBUG((DEBUG_LEVEL, "insert parameter %d %s",
			       i, FIELD_NAMES[param->var_field]));

			param->next = rctx->params;
			rctx->params = param;
		}
		break;
	case CMD_UPDATE:
		DEBUG((DEBUG_LEVEL, "*** %s (UPDATE)", __FUNCTION__));

		/*
		 * Core code already has some lock on each rel being planned,
		 * so we can use NoLock here.
		 */
		rel = TABLE_OPEN(rte->relid, NoLock);

		/* figure out which attributes are affected */
#if PG_VERSION_NUM < 90500
		tmpset = bms_copy(rte->modifiedCols);
#else
		tmpset = bms_copy(rte->updatedCols);
#endif

		while ((i = bms_first_member(tmpset)) >= 0) {
			/* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
			AttrNumber attno = i + FirstLowInvalidHeapAttributeNumber;

			if (attno <= InvalidAttrNumber)  /* shouldn't happen */
				elog(ERROR, "system-column update is not supported");
			targetAttrs = lappend_int(targetAttrs, attno);
		}
		TABLE_CLOSE(rel, NoLock);

		i = 0;
		foreach(lc, targetAttrs) {
			struct redis_param_desc *param;
			bool skip = false;

			/* find the corresponding redis entry */
			while (rctx->rtable.columns[i].pgattnum < lfirst_int(lc))
				i++;

			/* add param */
			param = (struct redis_param_desc *)
			        palloc(sizeof(struct redis_param_desc));

			param->var_field = rctx->rtable.columns[i].orig_var_field;
			param->paramid = i;
			param->param = NULL;
			param->value = NULL;

			switch (param->var_field) {
			case VAR_KEY:
				elog(ERROR, "put key in WHERE clause");
				break;
			case VAR_FIELD:
				/* TODO: use LUA inline-script to rename field:
				 * e.g. EVAL "local v = redis.call('hget',KEYS[1],ARGV[1]);
				 *            redis.call('hsetnx',KEYS[1],ARGV[2],v);
				 *            return redis.call('hdel',KEYS[1],ARGV[1]);"
				 *      1 keyname field1 field2
				 */
				elog(ERROR, "put field in WHERE clause");
				break;
			case VAR_ARRAY_FIELD:
				elog(ERROR, "put field in WHERE clause");
				break;
			case VAR_MEMBER:
				rctx->param_flags |= PARAM_MEMBER;
				break;
			case VAR_INDEX:
				rctx->param_flags |= PARAM_INDEX;
				break;
			case VAR_SCORE:
				rctx->param_flags |= PARAM_SCORE;
				break;
			case VAR_EXPIRY:
				rctx->param_flags |= PARAM_EXPIRY;
				break;
			case VAR_S_VALUE:
			case VAR_I_VALUE:
				rctx->param_flags |= PARAM_VALUE;
				break;
			case VAR_TABLE_TYPE:
				elog(ERROR, "put tabletype in WHERE clause");
				break;
			default:
				DEBUG((DEBUG_LEVEL, "skipping non-updateable parameter"));
				skip = true;
				break;
			}

			if (!skip) {
				DEBUG((DEBUG_LEVEL, "update parameter %d %s",
				       i, FIELD_NAMES[param->var_field]));

				param->next = rctx->params;
				rctx->params = param;
			}
		}
	default:
		break;
	}

	/* extract the relevant RETURNING list if any */
	if (plan->returningLists) {
		Bitmapset *attrs_used = NULL;

		returningList = (List *) list_nth(plan->returningLists, subplan_index);

		/* get all the attributes mentioned there */
		pull_varattnos((Node *) returningList, resultRelation, &attrs_used);

		/* disregard unused columns */
		for (i = 0; i < rctx->rtable.colcnt; i++) {
			rctx->rtable.columns[i].orig_var_field =
			    rctx->rtable.columns[i].var_field;

			if (!bms_is_member(rctx->rtable.columns[i].pgattnum -
			                       FirstLowInvalidHeapAttributeNumber,
			                  attrs_used)) {
			    rctx->rtable.columns[i].var_field = -1;
			}
		}
	}

	return redis_serialize_fdw(rctx);
}

static void
redisBeginForeignModify(ModifyTableState *mtstate,
						ResultRelInfo *rinfo,
						List *fdw_private,
						int subplan_index,
						int eflags)
{
	struct redis_fdw_ctx *rctx;
	EState    *estate;
	CmdType   operation = mtstate->operation;
	int i;

	DEBUG((DEBUG_LEVEL, "*** %s", __FUNCTION__));

	rctx = redis_deserialize_fdw(fdw_private);
	rinfo->ri_FdwState = rctx;

	estate = mtstate->ps.state;

	/* get the type input/output functions for the parameters */
	for (i = 0; i < rctx->rtable.colcnt; i++) {
		pgsql_get_typio(rctx->rtable.columns[i].pgtype,
		                &rctx->rtable.columns[i].typoutput,
		                &rctx->rtable.columns[i].typinput);
	}

	/* get resjunk attribute numbers */
	if (operation == CMD_UPDATE || operation == CMD_DELETE) {
		/* Find the ctid resjunk column in the subplan's result */
		Plan *subplan = mtstate->mt_plans[subplan_index]->plan;

		rctx->key_attno = ExecFindJunkAttributeInTlist(subplan->targetlist,
		                                               "key");
		rctx->field_attno = ExecFindJunkAttributeInTlist(subplan->targetlist,
		                                               "field");
		rctx->index_attno = ExecFindJunkAttributeInTlist(subplan->targetlist,
		                                               "index");
		rctx->member_attno = ExecFindJunkAttributeInTlist(subplan->targetlist,
		                                               "member");
		rctx->value_attno = ExecFindJunkAttributeInTlist(subplan->targetlist,
		                                               "value");
	}

	/* connect */
	redis_do_connect(rctx);

	/* create a memory context for short-lived memory */
	rctx->temp_ctx = AllocSetContextCreate(estate->es_query_cxt,
		"redis_fdw temporary data",
		ALLOCSET_DEFAULT_SIZES);
}

static TupleTableSlot *
redisExecForeignInsert(EState *estate,
					   ResultRelInfo *rinfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot)
{
	struct redis_fdw_ctx *rctx;
	struct redis_param_desc *param;
	HeapTuple tuple;
	MemoryContext old_ctx;
	redisReply *reply = NULL;
	redisReply *expreply = NULL;
	char vbuf[32];

	bool newkey = false;
	char *str = NULL;
	char *val = NULL;
	char *retval = NULL;
	int64_t ival = 0;
	int64_t score = 0;

	int i;

	DEBUG((DEBUG_LEVEL, "*** %s", __FUNCTION__));

	rctx = (struct redis_fdw_ctx *)rinfo->ri_FdwState;

	rctx->rowcount++;

	if (rctx->attmeta == NULL)
		rctx->attmeta = TupleDescGetAttInMetadata(slot->tts_tupleDescriptor);

	MemoryContextReset(rctx->temp_ctx);

	rctx->expiry = 0;
	for (param = rctx->params; param != NULL; param = param->next) {
		Datum datum = 0;
		bool isnull;

		datum = slot_getattr(slot,
		              rctx->rtable.columns[param->paramid].pgattnum, &isnull);

		if (!isnull)
			param->value = DatumGetCString(OidFunctionCall1(
			                 rctx->rtable.columns[param->paramid].typoutput,
		   	                 datum));

		switch (param->var_field) {
		case VAR_KEY:
		case VAR_CHANNEL:
			if (isnull)
				ERR_CLEANUP(rctx->r_reply, rctx->r_ctx,
				           (ERROR, "%s must not be NULL",
				            param->var_field == VAR_KEY ? "key" : "channel"));
			rctx->key = param->value;
			newkey = true;
			break;
		case VAR_FIELD:
			if (isnull)
				ERR_CLEANUP(rctx->r_reply, rctx->r_ctx,
				            (ERROR, "field must not be NULL"));
			str = param->value;
			break;
		case VAR_MEMBER:
			if (isnull)
				ERR_CLEANUP(rctx->r_reply, rctx->r_ctx,
				            (ERROR, "member must not be NULL"));
			val = param->value;
			break;
		case VAR_INDEX:
			/* TODO for LIST: can't easily insert into slot at index in one go,
			         might make it so that if index is provided, then use LSET
			 */
			if (!isnull)
				rctx->where_conds.min = atoll(param->value);
			break;
		case VAR_S_VALUE:
			if (isnull)
				ERR_CLEANUP(rctx->r_reply, rctx->r_ctx,
				            (ERROR, "value must not be NULL"));
			val = param->value;
			break;
		case VAR_I_VALUE:
			if (isnull)
				ERR_CLEANUP(rctx->r_reply, rctx->r_ctx,
				            (ERROR, "value must not be NULL"));
			ival = atoll(param->value);
			break;
		case VAR_SCORE:
			if (isnull)
				ERR_CLEANUP(rctx->r_reply, rctx->r_ctx,
				            (ERROR, "score must not be NULL"));
			score = atoll(param->value);
			break;
		case VAR_EXPIRY:
			if (!isnull) {
				rctx->expiry = atoll(param->value);
				if (rctx->expiry < 0)
					ERR_CLEANUP(rctx->r_reply, rctx->r_ctx,
					    (ERROR, "invalid value for expiry %s", param->value));
			}
			break;
		case VAR_MESSAGE:
			if (isnull)
				ERR_CLEANUP(rctx->r_reply, rctx->r_ctx,
				            (ERROR, "message must not be NULL"));
			val = param->value;
			break;
		default:
			break;
		}

		DEBUG((DEBUG_LEVEL, "insert parameter %s value %s",
		    FIELD_NAMES[param->var_field], param->value));
	}

	if (rctx->pfxkey == NULL || newkey) {
		if (newkey && rctx->pfxkey != NULL) {
			pfree(rctx->pfxkey);
		}

		if (rctx->keyprefix != NULL) {
			rctx->pfxkey = (char *)palloc(strlen(rctx->keyprefix) +
			                              strlen(rctx->key) + 1);
			sprintf(rctx->pfxkey, "%s%s", rctx->keyprefix, rctx->key);
		} else {
			rctx->pfxkey = pstrdup(rctx->key);
		}
	}

	/* do INSERT */
	switch (rctx->table_type) {
	case PG_REDIS_STRING:
		rctx->cmd = REDIS_SET;
		if (rctx->expiry > 0) {
			if (val) {
				DEBUG((DEBUG_LEVEL, "SET %s %s EX %ld",
				       rctx->pfxkey, val, rctx->expiry));
				reply = redisCommand(rctx->r_ctx, "SET %s %s EX %d",
				    rctx->pfxkey, val, (int)rctx->expiry);
			} else {
				DEBUG((DEBUG_LEVEL, "SET %s %ld EX %ld",
				      rctx->pfxkey, ival, rctx->expiry));
				reply = redisCommand(rctx->r_ctx, "SET %s %ld EX %d",
				    rctx->pfxkey, ival, (int)rctx->expiry);
			}
			rctx->expiry = 0;
		} else {
			if (val) {
				DEBUG((DEBUG_LEVEL, "SET %s %s", rctx->pfxkey, val));
				reply = redisCommand(rctx->r_ctx, "SET %s %s",
				    rctx->pfxkey, val);
			} else {
				DEBUG((DEBUG_LEVEL, "SET %s %ld", rctx->pfxkey, ival));
				reply = redisCommand(rctx->r_ctx, "SET %s %lld",
				    rctx->pfxkey, ival);
			}
		}
		break;
	case PG_REDIS_HSET:
		rctx->cmd = REDIS_HSET;

		if (val) {
			DEBUG((DEBUG_LEVEL, "HSET %s %s %s", rctx->pfxkey, str, val));
			reply = redisCommand(rctx->r_ctx, "HSET %s %s %s",
			                     rctx->pfxkey, str, val);
		} else {
			DEBUG((DEBUG_LEVEL, "HSET %s %s %s", rctx->pfxkey, str, val));
			reply = redisCommand(rctx->r_ctx, "HSET %s %s %s",
			                     rctx->pfxkey, str, val);
		}
		break;
	case PG_REDIS_LIST:
		rctx->cmd = REDIS_RPUSH;

		/* INSERT (key, value, <unused>index) */
		if (val) {
			DEBUG((DEBUG_LEVEL, "RPUSH %s %s", rctx->pfxkey, val));
			reply = redisCommand(rctx->r_ctx, "RPUSH %s %s", rctx->pfxkey, val);
		} else {
			DEBUG((DEBUG_LEVEL, "RPUSH %s %ld", rctx->pfxkey, ival));
			reply = redisCommand(rctx->r_ctx, "RPUSH %s %lld",
			                     rctx->pfxkey, ival);
		}
		break;
	case PG_REDIS_SET:
		/* INSERT (key, value) */
		rctx->cmd = REDIS_SADD;

		if (val) {
			DEBUG((DEBUG_LEVEL, "SADD %s %s", rctx->pfxkey, val));
			reply = redisCommand(rctx->r_ctx, "SADD %s %s", rctx->pfxkey, val);
		} else {
			DEBUG((DEBUG_LEVEL, "SADD %s %ld", rctx->pfxkey, ival));
			reply = redisCommand(rctx->r_ctx, "SADD %s %lld",
			                     rctx->pfxkey, ival);
		}
		break;
	case PG_REDIS_ZSET:
		/* INSERT (key, score, member) */
		rctx->cmd = REDIS_ZADD;

		if (val) {
			DEBUG((DEBUG_LEVEL, "ZADD %s %ld %s", rctx->pfxkey, score, val));
			reply = redisCommand(rctx->r_ctx, "ZADD %s %lld %s",
			                     rctx->pfxkey, score, val);
		} else {
			DEBUG((DEBUG_LEVEL, "ZADD %s %ld %ld", rctx->pfxkey, score, ival));
			reply = redisCommand(rctx->r_ctx, "ZADD %s %lld %lld",
			                     rctx->pfxkey, score, ival);
		}

		break;
	case PG_REDIS_TTL:
		/* INSERT (key, expiry) */
		if (rctx->expiry > 0) {
			rctx->cmd = REDIS_EXPIRE;
			DEBUG((DEBUG_LEVEL, "EXPIRE %s %ld", rctx->pfxkey, rctx->expiry));
			reply = redisCommand(rctx->r_ctx, "EXPIRE %s %d",
		                     rctx->pfxkey, (int)rctx->expiry);
		} else {
			/* remove expiry */
			rctx->cmd = REDIS_PERSIST;
			DEBUG((DEBUG_LEVEL, "PERSIST %s", rctx->pfxkey));
			reply = redisCommand(rctx->r_ctx, "PSERSIST %s", rctx->pfxkey);
		}
		rctx->expiry = 0;
		break;
	case PG_REDIS_PUBLISH:
		/* INSERT (channel, message) */
		rctx->cmd = REDIS_PUBLISH;
		DEBUG((DEBUG_LEVEL, "PUBLISH %s %s", rctx->pfxkey, val));
		reply = redisCommand(rctx->r_ctx, "PUBLISH %s %s",
		                     rctx->pfxkey, val);

		if (reply != NULL && reply->type != REDIS_REPLY_ERROR)
			ival = reply->integer;

		rctx->expiry = 0;
		break;
	default:
		elog(ERROR, "insert on non-writable table %d", rctx->table_type);
	}

	if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
		char *errmsg;

		errmsg = reply ? pstrdup(reply->str) : "";
		ERR_CLEANUP(reply, rctx->r_ctx,
		            (ERROR, "Redis cmd failed: %s", errmsg));
	}

	freeReplyObject(reply);

	if (rctx->expiry > 0) {
		DEBUG((DEBUG_LEVEL, "EXPIRE %s %ld", rctx->pfxkey, rctx->expiry));
		expreply = redisCommand(rctx->r_ctx, "EXPIRE %s %d",
		                     rctx->pfxkey, (int)rctx->expiry);

		if (expreply == NULL) {
			redisFree(rctx->r_ctx);
			ereport(ERROR,
			   (errcode(ERRCODE_FDW_ERROR),
			    errmsg("EXPIRE reply NULL")));
		} else if (expreply->type == REDIS_REPLY_ERROR) {
			freeReplyObject(expreply);
			redisFree(rctx->r_ctx);

			ereport(ERROR,
			   (errcode(ERRCODE_FDW_ERROR),
			    errmsg("Redis EXIPIRE cmd failed: %s", expreply->str)));
		}
		freeReplyObject(expreply);
	}

	old_ctx = MemoryContextSwitchTo(rctx->temp_ctx);

	/* empty the result slot */
	ExecClearTuple(slot);

	rctx->slot_values = (char **)palloc(sizeof(char*)*rctx->rtable.colcnt);

	/* get RETURNING values */
	for (i = 0; i < rctx->rtable.colcnt; i++) {
		/* insert NULL for dropped columns */
		if (rctx->rtable.columns[i].var_field < 0) {
			rctx->slot_values[i] = NULL;
			continue;
		}

		switch (rctx->rtable.columns[i].var_field) {
		case VAR_KEY:
		case VAR_CHANNEL:
			retval = rctx->key;
			break;
		case VAR_FIELD:
			retval = str;
			break;
		case VAR_ARRAY_FIELD:
			break;
		case VAR_S_VALUE:
		case VAR_MESSAGE:
		case VAR_MEMBER:
			retval = val;
			break;
		case VAR_SARRAY_VALUE:
			break;
		case VAR_I_VALUE:
			snprintf(vbuf, sizeof(vbuf), "%ld", ival);
			retval = pstrdup(vbuf);
			break;
		case VAR_MEMBERS:
			break;
		case VAR_EXPIRY:
			snprintf(vbuf, sizeof(vbuf), "%ld", rctx->expiry);
			retval = pstrdup(vbuf);
			break;
		case VAR_INDEX:
			snprintf(vbuf, sizeof(vbuf), "%ld", rctx->where_conds.min);
			retval = pstrdup(vbuf);
			break;
		case VAR_SCORE:
			snprintf(vbuf, sizeof(vbuf), "%ld", score);
			retval = pstrdup(vbuf);
			break;
		case VAR_SCARD:
		case VAR_ZCARD:
			snprintf(vbuf, sizeof(vbuf), "0");
			retval = pstrdup(vbuf);
			break;
		case VAR_LEN:
			snprintf(vbuf, sizeof(vbuf), "%ld", ival);
			retval = pstrdup(vbuf);
			break;
		default:
			DEBUG((DEBUG_LEVEL,
			       "should not get here - unknown col[%d] varfield %d",
			       i, rctx->rtable.columns[i].var_field));
		}

		rctx->slot_values[i] = retval;
	}

	tuple = BuildTupleFromCStrings(rctx->attmeta, rctx->slot_values);
	RFDW_VERDEP_ExecStoreTuple(tuple, slot, false);

	MemoryContextSwitchTo(old_ctx);
	return slot;
}

struct redis_fdw_resjunk {
	char *key;
	char *field;
	char *member;
	int64_t index;

	int   hasval;     /* which field has a value (param-bits) */
};

static void
redis_get_resjunks(struct redis_fdw_ctx *rctx, TupleTableSlot *planSlot,
                   struct redis_fdw_resjunk *rj)
{
	Datum datum;
	bool isnull;

	memset(rj, 0, sizeof(struct redis_fdw_resjunk));

	if (AttributeNumberIsValid(rctx->key_attno)) {
		datum = ExecGetJunkAttribute(planSlot, rctx->key_attno, &isnull);
		if (!isnull) {
			rj->hasval |= PARAM_KEY;
			rj->key = DatumGetCString(OidFunctionCall1(
		             rctx->rtable.columns[rctx->rtable.key-1].typoutput, 
		             datum));
			DEBUG((DEBUG_LEVEL, "update/delete WHERE key = %s", rj->key));
		}
	}

	if (AttributeNumberIsValid(rctx->field_attno)) {
		datum = ExecGetJunkAttribute(planSlot, rctx->field_attno, &isnull);
		if (!isnull) {
			rj->hasval |= PARAM_FIELD;
			rj->field = DatumGetCString(OidFunctionCall1(
		               rctx->rtable.columns[rctx->rtable.field-1].typoutput, 
		               datum));
			DEBUG((DEBUG_LEVEL, "update/delete WHERE field = %s", rj->field));
		}
	}

	if (AttributeNumberIsValid(rctx->index_attno)) {
		char *s;
		datum = ExecGetJunkAttribute(planSlot, rctx->index_attno, &isnull);
		if (!isnull) {
			rj->hasval |= PARAM_INDEX;
			s = DatumGetCString(OidFunctionCall1(
		               rctx->rtable.columns[rctx->rtable.index-1].typoutput, 
		               datum));
			rj->index = atoll(s);
			DEBUG((DEBUG_LEVEL, "update/delete WHERE index = %ld [%s]",
			      rj->index, s));
		}
	}

	if (AttributeNumberIsValid(rctx->member_attno)) {
		datum = ExecGetJunkAttribute(planSlot, rctx->member_attno, &isnull);
		if (!isnull) {
			rj->hasval |= PARAM_MEMBER;
			rj->member = DatumGetCString(OidFunctionCall1(
		               rctx->rtable.columns[rctx->rtable.member-1].typoutput, 
		               datum));
			DEBUG((DEBUG_LEVEL, "update/delete WHERE member = %s", rj->member));
		}
	}

	if (AttributeNumberIsValid(rctx->value_attno)) {
		datum = ExecGetJunkAttribute(planSlot, rctx->value_attno, &isnull);
		if (!isnull) {
			rj->hasval |= PARAM_VALUE;
			rj->member = DatumGetCString(OidFunctionCall1(
		               rctx->rtable.columns[rctx->rtable.s_value-1].typoutput, 
		               datum));
			DEBUG((DEBUG_LEVEL, "update/delete WHERE value = %s", rj->member));
		}
	}
}


static TupleTableSlot *
redisExecForeignUpdate(EState *estate,
					   ResultRelInfo *rinfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot)
{
	/*
	 * Do pretty much the same as INSERT, but with additional parameters
	 * to restrict which fields get updated.
	 */
	struct redis_fdw_ctx *rctx;
	struct redis_param_desc *param;
	HeapTuple tuple;
	MemoryContext old_ctx;
	redisReply *reply = NULL;
	redisReply *expreply = NULL;
	char vbuf[32];

	struct redis_fdw_resjunk resjunk;

	char *key = NULL;
	char *member = NULL;
	char *val = NULL;
	int64_t ival = 0;
	int64_t score = 0;
	int64_t index = 0;
	char *retval = NULL;

	int set_params = 0; /* param flags for SET "param" = x */

	int i;

	DEBUG((DEBUG_LEVEL, "*** %s", __FUNCTION__));

	rctx = (struct redis_fdw_ctx *)rinfo->ri_FdwState;

	rctx->rowcount++;

	if (rctx->attmeta == NULL)
		rctx->attmeta = TupleDescGetAttInMetadata(slot->tts_tupleDescriptor);

	MemoryContextReset(rctx->temp_ctx);

	/*
	 * Get resjunk columns which are the paramters provided for the
	 * WHERE clause (from IterateForeignScan results)
	 */
	redis_get_resjunks(rctx, planSlot, &resjunk);

	/*
	 * Parse provided update SET values
	 */
	for (param = rctx->params; param != NULL; param = param->next) {
		Datum datum = 0;
		bool isnull;

		datum = slot_getattr(slot,
		     rctx->rtable.columns[param->paramid].pgattnum, &isnull);

		if (!isnull)
			param->value = DatumGetCString(OidFunctionCall1(
		   	                 rctx->rtable.columns[param->paramid].typoutput,
			                 datum));

		switch (param->var_field) {
		case VAR_KEY:
			/* TODO: if key is provided, then key is going to be renamed */
			if (!isnull)
				key = param->value;
			break;
		case VAR_FIELD:
			ERR_CLEANUP(rctx->r_reply, rctx->r_ctx,
			            (ERROR, "put field in WHERE clause"));
			break;
		case VAR_MEMBER:
			if (!isnull) {
				member = param->value;
				set_params |= PARAM_MEMBER;
			}
			break;
		case VAR_INDEX:
			/* LSET */
			if (!isnull) {
				index = atoll(param->value);
				set_params |= PARAM_INDEX;
			}
			break;
		case VAR_S_VALUE:
			if (!isnull) {
				val = param->value;
				set_params |= PARAM_VALUE;
			}
			break;
		case VAR_I_VALUE:
			if (!isnull) {
				ival = atoll(param->value);
				set_params |= PARAM_VALUE;
			}
			break;
		case VAR_SCORE:
			if (!isnull) {
				score = atoll(param->value);
				set_params |= PARAM_SCORE;
			}
			break;
		case VAR_EXPIRY:
			if (!isnull) {
				rctx->expiry = atoll(param->value);
				if (rctx->expiry > 0)
					set_params |= PARAM_EXPIRY;
			}
			break;
		default:
			break;
		}

		DEBUG((DEBUG_LEVEL, "update parameter %s value %s",
		    FIELD_NAMES[param->var_field], param->value));
	}

	if ((resjunk.hasval & PARAM_KEY) == 0) {
		if (!rctx->key) {
			ereport(ERROR,
			   (errcode(ERRCODE_FDW_ERROR),
			    errmsg("key not provided")));
		}
		resjunk.key = rctx->key;
	}

	if (rctx->pfxkey == NULL) {
		if (rctx->keyprefix != NULL) {
			rctx->pfxkey = (char *)palloc(strlen(rctx->keyprefix) +
			                              strlen(resjunk.key) + 1);
			sprintf(rctx->pfxkey, "%s%s", rctx->keyprefix, resjunk.key);
		} else {
			rctx->pfxkey = pstrdup(resjunk.key);
		}
		DEBUG((DEBUG_LEVEL, "prefixed resjunk key %s", rctx->pfxkey));
	}

	/* do UPDATE */
	switch (rctx->table_type) {
	case PG_REDIS_STRING:
		/* UPDATE SET [ expiry=x, ] [ value=x ] [ WHERE key = x ] */
		if (set_params & PARAM_EXPIRY) {
			if (set_params & PARAM_VALUE) {
				if (val) {
					DEBUG((DEBUG_LEVEL, "SET %s %s EX %ld",
					       rctx->pfxkey, val, rctx->expiry));
					reply = redisCommand(rctx->r_ctx, "SET %s %s EX %d",
					                     rctx->pfxkey, val, (int)rctx->expiry);
				} else {
					DEBUG((DEBUG_LEVEL, "SET %s %ld EX %ld",
					      rctx->pfxkey, ival, rctx->expiry));
					reply = redisCommand(rctx->r_ctx, "SET %s %ld EX %d",
					                    rctx->pfxkey, ival, (int)rctx->expiry);
				}
			} else {
				/* only updating expiry for key */
				DEBUG((DEBUG_LEVEL, "(SET) EXPIRE %s %ld",
				       rctx->pfxkey, rctx->expiry));
				reply = redisCommand(rctx->r_ctx, "EXPIRE %s %d",
				                     rctx->pfxkey, (int)rctx->expiry);
			}
		} else {
			if (set_params & PARAM_VALUE) {
				/* assign new value to key */
				if (val) {
					DEBUG((DEBUG_LEVEL, "SET %s %s ", rctx->pfxkey, val));
					reply = redisCommand(rctx->r_ctx, "SET %s %s",
					                     rctx->pfxkey, val);
				} else {
					DEBUG((DEBUG_LEVEL, "SET %s %ld", rctx->pfxkey, ival));
					reply = redisCommand(rctx->r_ctx, "SET %s %lld",
					                     rctx->pfxkey, ival);
				}
			} else if (key != NULL && resjunk.key != NULL) {
				/* rename key */
				DEBUG((DEBUG_LEVEL, "(SET) RENAME %s %s",
				       rctx->pfxkey, key));
				reply = redisCommand(rctx->r_ctx, "RENAME %s %s",
				                     rctx->pfxkey, key);

				resjunk.key = key;
			} else {
				ERR_CLEANUP(rctx->r_reply, rctx->r_ctx,
				   (ERROR, "key/value/expiry must be provided"));
			}
		}
		set_params &= ~PARAM_EXPIRY;
		break;
	case PG_REDIS_HSET:
		/* UPDATE SET value=x, [expiry=x] WHERE [key=x] [& field=x] */
		if ((set_params & PARAM_EXPIRY) == 0 && resjunk.field == NULL)
			ERR_CLEANUP(rctx->r_reply, rctx->r_ctx,
			   (ERROR, "field must be provided in WHERE clause"));

		/* TODO: only update if HEXISTS */
		if (set_params & PARAM_VALUE) {
			if (val) {
				DEBUG((DEBUG_LEVEL, "HSET %s %s %s",
				                    rctx->pfxkey, resjunk.field, val));
				reply = redisCommand(rctx->r_ctx, "HSET %s %s %s",
				                    rctx->pfxkey, resjunk.field, val);
			} else {
				DEBUG((DEBUG_LEVEL, "HSET %s %s %s",
				                    rctx->pfxkey, resjunk.field, val));
				reply = redisCommand(rctx->r_ctx, "HSET %s %s %s",
				                     rctx->pfxkey, resjunk.field, val);
			}
		} else {
			if (set_params & PARAM_EXPIRY)
				goto do_expiry;

			ERR_CLEANUP(rctx->r_reply, rctx->r_ctx,
			            (ERROR, "value must be provided"));
		}
		break;
	case PG_REDIS_LIST:
		/*
		 * UPDATE value [index] WHERE key = x [AND index = x]
		 *   SET index has precendence over WHERE index
		 */
		if ((resjunk.hasval & PARAM_INDEX) == 0 &&
		    (set_params & PARAM_INDEX) == 0 &&
		    (set_params & PARAM_EXPIRY) == 0)
			ERR_CLEANUP(rctx->r_reply, rctx->r_ctx,
			    (ERROR, "index must be provided in SET or WHERE"));

		if (set_params & PARAM_INDEX)
			resjunk.index = index;

		if ((set_params & PARAM_VALUE) &&
		    ((resjunk.hasval & PARAM_INDEX) || (set_params & PARAM_INDEX))) {
			if (val) {
				DEBUG((DEBUG_LEVEL, "LSET %s %ld %s",
				                    rctx->pfxkey, resjunk.index, val));
				reply = redisCommand(rctx->r_ctx, "LSET %s %lld %s",
				    rctx->pfxkey, resjunk.index, val);
			} else {
				DEBUG((DEBUG_LEVEL, "LSET %s %ld %ld",
				       rctx->pfxkey, resjunk.index, ival));
				reply = redisCommand(rctx->r_ctx, "LSET %s %lld %lld",
				                     rctx->pfxkey, resjunk.index, ival);
			}
		} else {
			if (set_params & PARAM_EXPIRY)
				goto do_expiry;

			ERR_CLEANUP(rctx->r_reply, rctx->r_ctx,
			    (ERROR, "value and index must be provided"));
		}
		break;
	case PG_REDIS_SET:
		/* UPDATE member WHERE key = x [ AND member = x ] */

		/* remove old member and add new member to rename */
		if (set_params & PARAM_MEMBER) {
			if (resjunk.hasval & PARAM_MEMBER) {
				/* remove old member */
				DEBUG((DEBUG_LEVEL, "SREM %s %s",
				      rctx->pfxkey, resjunk.member));
				reply = redisCommand(rctx->r_ctx, "SREM %s %s",
			                         rctx->pfxkey, resjunk.member);
				if (reply->type == REDIS_REPLY_ERROR || reply->integer == 0) {
					ERR_CLEANUP(reply, rctx->r_ctx,
					    (ERROR, "member %s does not exist", resjunk.member));
				}
			}
			DEBUG((DEBUG_LEVEL, "SADD %s %s", rctx->pfxkey, member));
			reply = redisCommand(rctx->r_ctx, "SADD %s %s",
			                     rctx->pfxkey, member);
		} else {
			if (set_params & PARAM_EXPIRY)
				goto do_expiry;

			ERR_CLEANUP(rctx->r_reply, rctx->r_ctx,
			    (ERROR, "member must be provided"));
		}
		break;
	case PG_REDIS_ZSET:
		/* UPDATE member, score WHERE key=x */

		if (!(set_params & PARAM_SCORE)) {
			ERR_CLEANUP(reply, rctx->r_ctx, (ERROR, "score must be provided"));
		}

		if (set_params & PARAM_MEMBER) {
			DEBUG((DEBUG_LEVEL, "ZADD %s %ld %s", rctx->pfxkey, score, member));
			if (resjunk.hasval & PARAM_MEMBER && strcmp(member, resjunk.member) != 0) {
				/* remove old member */
				DEBUG((DEBUG_LEVEL, "ZREM %s %s",
				      rctx->pfxkey, resjunk.member));
				reply = redisCommand(rctx->r_ctx, "ZREM %s %s",
			                         rctx->pfxkey, resjunk.member);
				if (reply->type == REDIS_REPLY_ERROR || reply->integer == 0) {
					ERR_CLEANUP(reply, rctx->r_ctx,
					    (ERROR, "member %s does not exist", resjunk.member));
				}
			}
			reply = redisCommand(rctx->r_ctx, "ZADD %s %lld %s",
			                     rctx->pfxkey, score, member);
		} else {
			if (set_params & PARAM_EXPIRY)
				goto do_expiry;

			ERR_CLEANUP(reply, rctx->r_ctx, (ERROR, "member must be provided"));
		}

		break;
	case PG_REDIS_TTL:
		/* UPDATE SET expiry = x WHERE key = x */
		if (set_params & PARAM_EXPIRY) {
			DEBUG((DEBUG_LEVEL, "EXPIRE %s %ld", rctx->pfxkey, rctx->expiry));
			reply = redisCommand(rctx->r_ctx, "EXPIRE %s %d",
		                     rctx->pfxkey, (int)rctx->expiry);
		} else {
			/* remove expiry */
			DEBUG((DEBUG_LEVEL, "PERSIST %s", rctx->pfxkey));
			reply = redisCommand(rctx->r_ctx, "PSERSIST %s", rctx->pfxkey);
		}
		set_params &= ~PARAM_EXPIRY;
		break;
	default:
		ERR_CLEANUP(rctx->r_reply, rctx->r_ctx,
		        (ERROR, "update on non-writable table %d", rctx->table_type));
	}

	if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
		if (reply != NULL)
			freeReplyObject(reply);
		redisFree(rctx->r_ctx);
		ereport(ERROR,
		   (errcode(ERRCODE_FDW_ERROR),
		    errmsg("Redis cmd failed: %s", reply ? reply->str : "")));
	}

	freeReplyObject(reply);

do_expiry:
	if (set_params & PARAM_EXPIRY) {
		if (rctx->expiry > 0) {
			DEBUG((DEBUG_LEVEL, "EXPIRE %s %ld", rctx->pfxkey, rctx->expiry));
			expreply = redisCommand(rctx->r_ctx, "EXPIRE %s %d",
			                     rctx->key, (int)rctx->expiry);
		} else {
			DEBUG((DEBUG_LEVEL, "PERSIST %s", rctx->pfxkey));
			expreply = redisCommand(rctx->r_ctx, "PERSIST %s", rctx->key);
		}

		if (expreply == NULL) {
			redisFree(rctx->r_ctx);
			ereport(ERROR,
			   (errcode(ERRCODE_FDW_ERROR),
			    errmsg("EXPIRE/PERSIST reply NULL")));
		} else if (expreply->type == REDIS_REPLY_ERROR) {
			freeReplyObject(expreply);
			redisFree(rctx->r_ctx);

			ereport(ERROR,
			   (errcode(ERRCODE_FDW_ERROR),
			    errmsg("Redis EXPIRE/PERSIST cmd failed: %s", expreply->str)));
		}
		freeReplyObject(expreply);
	}

	old_ctx = MemoryContextSwitchTo(rctx->temp_ctx);

	/* empty the result slot */
	ExecClearTuple(slot);

	rctx->slot_values = (char **)palloc(sizeof(char*)*rctx->rtable.colcnt);

	/* get RETURNING values */
	for (i = 0; i < rctx->rtable.colcnt; i++) {
		/* insert NULL for dropped columns */
		if (rctx->rtable.columns[i].var_field < 0) {
			rctx->slot_values[i] = NULL;
			continue;
		}

		switch (rctx->rtable.columns[i].var_field) {
		case VAR_KEY:
			retval = rctx->pfxkey;
			break;
		case VAR_FIELD:
			retval = resjunk.field;
			break;
		case VAR_ARRAY_FIELD:
			break;
		case VAR_S_VALUE:
			retval = val;
			break;
		case VAR_SARRAY_VALUE:
			break;
		case VAR_I_VALUE:
			snprintf(vbuf, sizeof(vbuf), "%ld", ival);
			retval = pstrdup(vbuf);
			break;
		case VAR_MEMBER:
			retval = member;
			break;
		case VAR_MEMBERS:
			break;
		case VAR_EXPIRY:
			snprintf(vbuf, sizeof(vbuf), "%ld", rctx->expiry);
			retval = pstrdup(vbuf);
			break;
		case VAR_INDEX:
			snprintf(vbuf, sizeof(vbuf), "%ld", resjunk.index);
			retval = pstrdup(vbuf);
			break;
		case VAR_SCORE:
			snprintf(vbuf, sizeof(vbuf), "%ld", score);
			retval = pstrdup(vbuf);
			break;
		case VAR_SCARD:
		case VAR_ZCARD:
			snprintf(vbuf, sizeof(vbuf), "0");
			retval = pstrdup(vbuf);
			break;
		default:
			DEBUG((DEBUG_LEVEL,
			       "should not get here - unknown col[%d] varfield %d",
			       i, rctx->rtable.columns[i].var_field));
		}

		rctx->slot_values[i] = retval;
	}

	tuple = BuildTupleFromCStrings(rctx->attmeta, rctx->slot_values);
	RFDW_VERDEP_ExecStoreTuple(tuple, slot, false);

	MemoryContextSwitchTo(old_ctx);
	return slot;
}

static TupleTableSlot *
redisExecForeignDelete(EState *estate,
					   ResultRelInfo *rinfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot)
{
	struct redis_fdw_ctx *rctx;
	MemoryContext old_ctx;
	HeapTuple tuple;
	redisReply *reply = NULL;
	char vbuf[32];
	struct redis_fdw_resjunk resjunk;
	char *retval = NULL;
	int i;

	DEBUG((DEBUG_LEVEL, "*** %s", __FUNCTION__));

	rctx = (struct redis_fdw_ctx *)rinfo->ri_FdwState;

	rctx->rowcount++;

	if (rctx->attmeta == NULL)
		rctx->attmeta = TupleDescGetAttInMetadata(slot->tts_tupleDescriptor);

	MemoryContextReset(rctx->temp_ctx);

	/*
	 * Get resjunk columns which are the paramters provided for the
	 * WHERE clause.
	 */
	redis_get_resjunks(rctx, planSlot, &resjunk);

	if ((resjunk.hasval & PARAM_KEY) == 0) {
		if (!rctx->key) {
			ereport(ERROR,
			   (errcode(ERRCODE_FDW_ERROR),
			    errmsg("key not provided")));
		}
		resjunk.key = rctx->key;
	}

	if (rctx->pfxkey == NULL) {
		if (rctx->keyprefix != NULL) {
			rctx->pfxkey = (char *)palloc(strlen(rctx->keyprefix) +
			                              strlen(resjunk.key) + 1);
			sprintf(rctx->pfxkey, "%s%s", rctx->keyprefix, resjunk.key);
		} else {
			rctx->pfxkey = pstrdup(resjunk.key);
		}
		DEBUG((DEBUG_LEVEL, "prefixed resjunk key %s", rctx->pfxkey));
	}

	/* do DELETE */
	switch (rctx->table_type) {
	case PG_REDIS_STRING:
		/* DELETE WHERE key = x */
		DEBUG((DEBUG_LEVEL, "DEL %s", rctx->pfxkey));
		reply = redisCommand(rctx->r_ctx, "DEL %s", rctx->pfxkey);
		break;
	case PG_REDIS_HSET:
		/* DELETE WHERE key=x [& field=x] */
		if ((rctx->where_flags & PARAM_FIELD) == 0) {
			/* delete key */
			DEBUG((DEBUG_LEVEL, "DEL %s", rctx->pfxkey));
			reply = redisCommand(rctx->r_ctx, "DEL %s", rctx->pfxkey);
		} else {
			DEBUG((DEBUG_LEVEL, "HDEL %s %s", rctx->pfxkey, resjunk.field));
			reply = redisCommand(rctx->r_ctx, "HDEL %s %s",
			                     rctx->pfxkey, resjunk.field);
		}
		break;
	case PG_REDIS_LIST:
		/*
		 * DELETE WHERE key = x AND [index = x | value = x]
		 */
		if ((rctx->where_flags & PARAM_VALUE)) {
			/* delete value */
			DEBUG((DEBUG_LEVEL, "LREM %s 1 %s", rctx->pfxkey, resjunk.member));
			reply = redisCommand(rctx->r_ctx, "LREM %s 1 %s",
			                     rctx->pfxkey, resjunk.member);
		} else if ((rctx->where_flags & PARAM_INDEX) == 0) {
			/* delete key */
			DEBUG((DEBUG_LEVEL, "DEL %s", rctx->pfxkey));
			reply = redisCommand(rctx->r_ctx, "DEL %s", rctx->pfxkey);
		} else if (resjunk.index == 0) {
			/* pop out first item to remove */
			DEBUG((DEBUG_LEVEL, "LPOP %s", rctx->pfxkey));
			reply = redisCommand(rctx->r_ctx, "LPOP %s", rctx->pfxkey);
			if (reply->type == REDIS_REPLY_ERROR) {
				char *errmsg;

				errmsg = pstrdup(reply->str);
				ERR_CLEANUP(reply, rctx->r_ctx,
				    (ERROR, "redis replied error on LPOP: %s", errmsg));
			}
		} else {

#define REDIS_LIST_DEL_MAGIC ":::redis-fdw-marked-for-deletion:::"

			/* rename the index into something unique */
			reply = redisCommand(rctx->r_ctx, "LSET %s %lld %s",
			    rctx->pfxkey, resjunk.index, REDIS_LIST_DEL_MAGIC);
			if (reply->type == REDIS_REPLY_ERROR) {
				char *errmsg;

				errmsg = pstrdup(reply->str);
				ERR_CLEANUP(reply, rctx->r_ctx,
				    (ERROR, "redis replied error on LSET: %s", errmsg));
			}
			freeReplyObject(reply);

			DEBUG((DEBUG_LEVEL, "LREM %s %ld %s",
			     rctx->pfxkey, resjunk.index, REDIS_LIST_DEL_MAGIC));
			reply = redisCommand(rctx->r_ctx, "LREM %s %lld %s",
			    rctx->pfxkey, resjunk.index, REDIS_LIST_DEL_MAGIC);
		}
		break;
	case PG_REDIS_SET:
		/* DELETE WHERE key = x AND member = x */
		if ((rctx->where_flags & PARAM_MEMBER) == 0) {
			/* delete key */
			DEBUG((DEBUG_LEVEL, "DEL %s", rctx->pfxkey));
			reply = redisCommand(rctx->r_ctx, "DEL %s", rctx->pfxkey);
		} else {
			DEBUG((DEBUG_LEVEL, "SREM %s %s", rctx->pfxkey, resjunk.member));
			reply = redisCommand(rctx->r_ctx, "SREM %s %s",
			                     rctx->pfxkey, resjunk.member);
		}
		break;
	case PG_REDIS_ZSET:
		/* DELETE WHERE key=x AND member = x */
		if ((rctx->where_flags & PARAM_MEMBER) == 0) {
			/* delete key */
			DEBUG((DEBUG_LEVEL, "DEL %s", rctx->pfxkey));
			reply = redisCommand(rctx->r_ctx, "DEL %s", rctx->pfxkey);
		} else {
			DEBUG((DEBUG_LEVEL, "ZREM %s %s", rctx->pfxkey, resjunk.member));
			reply = redisCommand(rctx->r_ctx, "ZREM %s %s",
			                     rctx->pfxkey, resjunk.member);
		}
		break;
	default:
		ERR_CLEANUP(rctx->r_reply, rctx->r_ctx,
		            (ERROR, "cannot delete from table of this type"));
	}

	if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
		char *errmsg;

		errmsg = reply ? pstrdup(reply->str) : "";
		ERR_CLEANUP(reply, rctx->r_ctx,
		            (ERROR, "Redis cmd failed: %s", errmsg));
	}

	if (reply->type == REDIS_REPLY_INTEGER) {
		DEBUG((DEBUG_LEVEL, "Redis deletion returned %lld", reply->integer));
		if (reply->integer == 0) {
			resjunk.key = NULL;
		}
	} else {
		DEBUG((DEBUG_LEVEL, "Unexpected redis reply type %s",
		      reply_type_str(reply->type)));
	}

	freeReplyObject(reply);

	old_ctx = MemoryContextSwitchTo(rctx->temp_ctx);

	/* empty the result slot */
	ExecClearTuple(slot);

	rctx->slot_values = (char **)palloc(sizeof(char*)*rctx->rtable.colcnt);

	/* get RETURNING values */
	for (i = 0; i < rctx->rtable.colcnt; i++) {
		/* insert NULL for dropped columns */
		if (rctx->rtable.columns[i].var_field < 0) {
			rctx->slot_values[i] = NULL;
			continue;
		}

		retval = NULL;
		switch (rctx->rtable.columns[i].var_field) {
		case VAR_KEY:
			retval = resjunk.key;
			break;
		case VAR_FIELD:
			retval = resjunk.field;
			break;
		case VAR_ARRAY_FIELD:
			break;
		case VAR_S_VALUE:
			break;
		case VAR_SARRAY_VALUE:
			break;
		case VAR_I_VALUE:
			break;
		case VAR_MEMBER:
			retval = resjunk.member;
			break;
		case VAR_MEMBERS:
			break;
		case VAR_EXPIRY:
			break;
		case VAR_INDEX:
			snprintf(vbuf, sizeof(vbuf), "%ld", resjunk.index);
			retval = vbuf;
			break;
		case VAR_SCORE:
			break;
		case VAR_SCARD:
		case VAR_ZCARD:
			break;
		default:
			DEBUG((DEBUG_LEVEL,
			       "should not get here - unknown col[%d] varfield %d",
			       i, rctx->rtable.columns[i].var_field));
		}

		rctx->slot_values[i] = retval;
	}

	tuple = BuildTupleFromCStrings(rctx->attmeta, rctx->slot_values);
	RFDW_VERDEP_ExecStoreTuple(tuple, slot, false);

	MemoryContextSwitchTo(old_ctx);
	return slot;
}

static void
redisEndForeignModify(EState *estate,
					  ResultRelInfo *rinfo)
{
	struct redis_fdw_ctx *rctx;

	DEBUG((DEBUG_LEVEL, "*** %s", __FUNCTION__));

	rctx = (struct redis_fdw_ctx *)rinfo->ri_FdwState;

	if (rctx) {
		if (rctx->r_reply != NULL) {
			freeReplyObject(rctx->r_reply);
			rctx->r_reply = NULL;
		}

		if (rctx->r_ctx != NULL) {
			redisFree(rctx->r_ctx);
			rctx->r_ctx = NULL;
		}
	}
}

static int
redisIsForeignRelUpdatable(Relation rel)
{
	ListCell *cell;

	DEBUG((DEBUG_LEVEL, "*** %s", __FUNCTION__));

	/* loop foreign table options */
	foreach(cell, GetForeignTable(RelationGetRelid(rel))->options)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		if (strcmp(def->defname, OPT_READONLY) == 0)
			return 0;

		if (strcmp(def->defname, OPT_TABLETYPE) == 0) {
			char *typeval = defGetString(def);
			if (strcmp(typeval, "len") == 0 ||
			    strcmp(typeval, "hmset") == 0 ||
			    strcmp(typeval, "mhash") == 0 ||
			    strcmp(typeval, "keys") == 0)
				return 0;
		}
	}

	return (1 << CMD_UPDATE) | (1 << CMD_INSERT) | (1 << CMD_DELETE);
}

static void
redisExplainForeignModify(ModifyTableState *mtstate,
						  ResultRelInfo *rinfo,
						  List *fdw_private,
						  int subplan_index,
						  struct ExplainState *es)
{
	DEBUG((DEBUG_LEVEL, "*** %s", __FUNCTION__));
}

#endif /* WRITE_API */

static void
redisExplainForeignScan(ForeignScanState *node,
						struct ExplainState *es)
{
	DEBUG((DEBUG_LEVEL, "*** %s", __FUNCTION__));
}

