/*-------------------------------------------------------------------------
 *
 * spi.h
 *				Server Programming Interface public declarations
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/spi.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SPI_H
#define SPI_H

#include "commands/trigger.h"
#include "lib/ilist.h"
#include "nodes/parsenodes.h"
#include "utils/portal.h"


typedef struct SPITupleTable
{
	MemoryContext tuptabcxt;	/* memory context of result table */
	uint64		alloced;		/* # of alloced vals */
	uint64		free;			/* # of free vals */
	TupleDesc	tupdesc;		/* tuple descriptor */
	HeapTuple  *vals;			/* tuples */
	slist_node	next;			/* link for internal bookkeeping */
	SubTransactionId subid;		/* subxact in which tuptable was created */
#ifdef _PG_ORCL_
	TupleDesc	*tuple_descs;		/* tuple descriptors */
	int32		**tuple_typmods;
#endif
} SPITupleTable;

/* Optional arguments for SPI_execute_plan_extended */
typedef struct SPIExecuteOptions
{
	ParamListInfo params;
	bool		read_only;
	bool		no_snapshots;
	uint64		tcount;
	DestReceiver *dest;
	ResourceOwner owner;
} SPIExecuteOptions;

#ifdef _PG_ORCL_

#define NumCursorAttr (4)

#define CursorAttr_isopen		0
#define CursorAttr_found		1
#define CursorAttr_notfound		2
#define CursorAttr_rowcount		3

typedef struct
{
	NameData	cursor;
	Datum		datum[NumCursorAttr];
	bool		isnull[NumCursorAttr];
	bool		is_valid;
} CursorInfoData;

#define NumExceptionAttr		(3)
#define ExceptionAttr_state		0
#define ExceptionAttr_errm		1
#define ExceptionAttr_code		2

typedef struct
{
	Datum   datum[NumExceptionAttr];
	bool    isnull[NumExceptionAttr];
	bool    is_valid;
} ExceptionInfoData;

/*
 * pl_exception_state
 *
 * When starting up a subtransaction for exeception handling, save current
 * transaction some variables here. After releasing the subtransaction, will
 * restore all from here and free the struct.
 *
 * When COMMIT or ROLLBACK in oraplsql, and starting a new transaction, will
 * recreate these 'state', as previously starting subtransaction.
 */
typedef struct pl_exception_state
{
	MemoryContext	mem_context;
	ResourceOwner	res_owner;
	ExprContext		*expr_context;
	ErrorData		*err_data;

	int				 excep_sub_level;
	struct pl_exception_state	*prev;
} pl_exception_state;

/*
 * Used to identity which plsql call SPI, this is used to avoid cross-used
 * plpgsql and oraplsql.
 */
typedef enum
{
	PG_PLSQL = 0,
	ORA_PLSQL = 1,
	OTHER_PLSQL = 2
} PLType;
#endif

/* Plans are opaque structs for standard users of SPI */
typedef struct _SPI_plan *SPIPlanPtr;

#define SPI_ERROR_CONNECT		(-1)
#define SPI_ERROR_COPY			(-2)
#define SPI_ERROR_OPUNKNOWN		(-3)
#define SPI_ERROR_UNCONNECTED	(-4)
#define SPI_ERROR_CURSOR		(-5)	/* not used anymore */
#define SPI_ERROR_ARGUMENT		(-6)
#define SPI_ERROR_PARAM			(-7)
#define SPI_ERROR_TRANSACTION	(-8)
#define SPI_ERROR_NOATTRIBUTE	(-9)
#define SPI_ERROR_NOOUTFUNC		(-10)
#define SPI_ERROR_TYPUNKNOWN	(-11)
#define SPI_ERROR_REL_DUPLICATE (-12)
#define SPI_ERROR_REL_NOT_FOUND (-13)

#define SPI_OK_CONNECT			1
#define SPI_OK_FINISH			2
#define SPI_OK_FETCH			3
#define SPI_OK_UTILITY			4
#define SPI_OK_SELECT			5
#define SPI_OK_SELINTO			6
#define SPI_OK_INSERT			7
#define SPI_OK_DELETE			8
#define SPI_OK_UPDATE			9
#define SPI_OK_CURSOR			10
#define SPI_OK_INSERT_RETURNING 11
#define SPI_OK_DELETE_RETURNING 12
#define SPI_OK_UPDATE_RETURNING 13
#define SPI_OK_REWRITTEN		14
#define SPI_OK_REL_REGISTER		15
#define SPI_OK_REL_UNREGISTER	16
#define SPI_OK_TD_REGISTER		17
#define SPI_OK_MERGE			18

#define SPI_OPT_NONATOMIC		(1 << 0)

/* These used to be functions, now just no-ops for backwards compatibility */
#define SPI_push()	((void) 0)
#define SPI_pop()	((void) 0)
#define SPI_push_conditional()	false
#define SPI_pop_conditional(pushed) ((void) 0)
#define SPI_restore_connection()	((void) 0)

extern PGDLLIMPORT uint64 SPI_processed;
extern PGDLLIMPORT Oid SPI_lastoid;
extern PGDLLIMPORT SPITupleTable *SPI_tuptable;
extern PGDLLIMPORT int SPI_result;

extern char *spi_query_string;
extern pl_exception_state *plpgsql_ExcepStack;

extern int	SPI_connect(void);
extern int	SPI_connect_ext(int options);
extern int	SPI_finish(void);
extern bool SPI_set_current_child_atomic(bool val);
extern bool SPI_get_parent_atomic(void);
extern bool SPI_has_tuptable(void);
#ifdef _PG_ORCL_
extern bool SPI_get_parent_savepoint(void);
extern void SPI_internal_xact(bool val);
extern bool SPI_get_internal_xact(void);
extern bool spi_printtup_tupdesc(HeapTuple tup, TupleDesc tupdesc, int32 *typmods);
extern bool SPI_deform_function_outvalues(Oid foid, Datum *values, bool *nulls, int32 **typmods);
extern void SPI_internal_dest_shutdown(void);
#endif
extern int	SPI_execute(const char *src, bool read_only, long tcount);
extern int SPI_execute_plan(SPIPlanPtr plan, Datum *Values, const char *Nulls,
				 bool read_only, long tcount);
extern int	SPI_execute_plan_extended(SPIPlanPtr plan,
									  const SPIExecuteOptions *options);
extern int SPI_execute_plan_with_paramlist(SPIPlanPtr plan,
								ParamListInfo params,
								bool read_only, long tcount);
extern int	SPI_exec(const char *src, long tcount);
extern int SPI_execp(SPIPlanPtr plan, Datum *Values, const char *Nulls,
		  long tcount);
extern int SPI_execute_snapshot(SPIPlanPtr plan,
					 Datum *Values, const char *Nulls,
					 Snapshot snapshot,
					 Snapshot crosscheck_snapshot,
					 bool read_only, bool fire_triggers, long tcount);
extern int SPI_execute_with_args(const char *src,
					  int nargs, Oid *argtypes,
					  Datum *Values, const char *Nulls,
					  bool read_only, long tcount);
extern int	SPI_execute_with_receiver(const char *src,
									  ParamListInfo params,
									  bool read_only, long tcount,
									  DestReceiver *dest);
extern SPIPlanPtr SPI_prepare(const char *src, int nargs, Oid *argtypes);
extern SPIPlanPtr SPI_prepare_cursor(const char *src, int nargs, Oid *argtypes,
				   int cursorOptions);
extern SPIPlanPtr SPI_prepare_params(const char *src,
				   ParserSetupHook parserSetup,
				   void *parserSetupArg,
				   int cursorOptions);
extern int	SPI_keepplan(SPIPlanPtr plan);
extern SPIPlanPtr SPI_saveplan(SPIPlanPtr plan);
extern int	SPI_freeplan(SPIPlanPtr plan);

extern Oid	SPI_getargtypeid(SPIPlanPtr plan, int argIndex);
extern int	SPI_getargcount(SPIPlanPtr plan);
extern bool SPI_is_cursor_plan(SPIPlanPtr plan);
extern bool SPI_plan_is_valid(SPIPlanPtr plan);
extern const char *SPI_result_code_string(int code);

extern List *SPI_plan_get_plan_sources(SPIPlanPtr plan);
extern CachedPlan *SPI_plan_get_cached_plan(SPIPlanPtr plan);
extern void SPI_plan_set_plan_in_use(SPIPlanPtr plan, bool is_in_use);

extern HeapTuple SPI_copytuple(HeapTuple tuple);
extern HeapTupleHeader SPI_returntuple(HeapTuple tuple, TupleDesc tupdesc);
extern HeapTuple SPI_modifytuple(Relation rel, HeapTuple tuple, int natts,
				int *attnum, Datum *Values, const char *Nulls);
extern int	SPI_fnumber(TupleDesc tupdesc, const char *fname);
extern char *SPI_fname(TupleDesc tupdesc, int fnumber);
extern char *SPI_getvalue(HeapTuple tuple, TupleDesc tupdesc, int fnumber);
extern Datum SPI_getbinval(HeapTuple tuple, TupleDesc tupdesc, int fnumber, bool *isnull);
extern char *SPI_gettype(TupleDesc tupdesc, int fnumber);
extern Oid	SPI_gettypeid(TupleDesc tupdesc, int fnumber);
extern char *SPI_getrelname(Relation rel);
extern char *SPI_getnspname(Relation rel);
extern void *SPI_palloc(Size size);
extern void *SPI_repalloc(void *pointer, Size size);
extern void SPI_pfree(void *pointer);
extern Datum SPI_datumTransfer(Datum value, bool typByVal, int typLen);
extern void SPI_freetuple(HeapTuple pointer);
extern void SPI_freetuptable(SPITupleTable *tuptable);

extern Portal SPI_cursor_open(const char *name, SPIPlanPtr plan,
				Datum *Values, const char *Nulls, bool read_only);
extern Portal SPI_cursor_open_with_args(const char *name,
						  const char *src,
						  int nargs, Oid *argtypes,
						  Datum *Values, const char *Nulls,
						  bool read_only, int cursorOptions);
extern Portal SPI_cursor_open_with_paramlist(const char *name, SPIPlanPtr plan,
							   ParamListInfo params, bool read_only);
extern Portal SPI_cursor_parse_open_with_paramlist(const char *name,
												   const char *src,
												   ParamListInfo params,
												   bool read_only,
												   int cursorOptions);
extern Portal SPI_cursor_find(const char *name);
extern void SPI_cursor_fetch(Portal portal, bool forward, long count);
extern void SPI_cursor_move(Portal portal, bool forward, long count);
extern void SPI_scroll_cursor_fetch(Portal, FetchDirection direction, long count);
extern void SPI_scroll_cursor_move(Portal, FetchDirection direction, long count);
extern void SPI_cursor_close(Portal portal);

extern int	SPI_register_relation(EphemeralNamedRelation enr);
extern int	SPI_unregister_relation(const char *name);
extern int	SPI_register_trigger_data(TriggerData *tdata);

extern void SPI_start_transaction(bool is_trans_blk);
extern void SPI_commit(void);
extern void SPI_rollback(void);
extern bool SPI_AtRollbackTo(int level);

extern void SPICleanup(void);
extern void AtEOXact_SPI(bool isCommit);
extern void AtEOSubXact_SPI(bool isCommit, SubTransactionId mySubid);
extern bool SPI_inside_nonatomic_context(void);

#ifdef PGXC
extern int SPI_execute_direct(const char *src, char *nodename);
extern void SPI_push_error(void *err);
extern void SPI_trunc_errstack(void);
extern void SPI_reset_errstack(void);
extern List *SPI_get_errstack(void);
extern int SPI_connect_level(void);

extern void SPI_internal_dest_startup(void);

extern void spi_start_retcursor(void);

extern void spi_end_retcursor(void);
extern CursorInfoData *spi_get_cursor(int *cnt, int spi_connected);
extern void spi_set_cursor(char *curname, Datum *datum, bool *isnull, int spi_connected);
extern void spi_return_implicit_cursor(Datum *values, bool *isnull);
extern bool spi_get_implicit_cursor_defval(Datum **values, bool **isnull);
extern bool spi_get_implicit_cursor_curval(Datum **values, bool **isnull);
extern void spi_set_implicit_cursor_curval(Datum *values, bool *isnull);
extern MemoryContext SPI_get_procxt(int level);
extern ExceptionInfoData *SPI_current_get_exception(void);
extern void SPI_current_store_exception(Datum *datum, bool *isnull);
extern void SPI_current_clean_exception(void);
extern bool SPI_parent_dest_startup(DestReceiver *self, int operation, TupleDesc typeinfo);
extern void SPI_internal_dest_startup(void);
extern void SPI_set_and_check_pl_conflict(PLType pltype);
extern void SPI_set_saved_mctxt(MemoryContext c);
#endif

extern MemoryContext   ErrStackContext;
#ifdef _PG_ORCL_
extern bool function_xact_control;
#endif

extern Query * SPI_get_query(const char *src, bool is_prepare);
extern void SPI_set_support_savepoint(bool support_savepoint);
extern bool SPI_get_support_savepoint(void);
extern void SPI_unbind_xact(void);
extern bool SPI_is_bind_xact(void);

extern void plpgsql_push_exception_state(MemoryContext oldcontext, ResourceOwner oldowner,
								ExprContext *old_eval_econtext);
extern void plpgsql_get_exception_state(MemoryContext *oldcontext, ResourceOwner *oldowner, char *origin_err);
extern void plpgsql_pop_exception_state(void);
extern void rebuild_subtransaction(MemoryContext oldcontext, int num, int *spi_levels);
extern bool is_spi_current_null(void);

#endif							/* SPI_H */
