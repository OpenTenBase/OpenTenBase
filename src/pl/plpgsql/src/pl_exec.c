/*-------------------------------------------------------------------------
 *
 * pl_exec.c		- Executor for the PL/pgSQL
 *			  procedural language
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/pl/plpgsql/src/pl_exec.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <ctype.h>

#include "access/htup_details.h"
#include "access/transam.h"
#include "access/tupconvert.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "executor/execExpr.h"
#include "executor/spi.h"
#include "executor/spi_priv.h"
#include "executor/tstoreReceiver.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "parser/scansup.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#ifdef XCP
#include "pgxc/pgxc.h"
#endif
#ifdef __OPENTENBASE__
#include "catalog/pgxc_class.h"
#include "utils/guc.h"
#endif
#ifdef __OPENTENBASE_C__
#include "executor/execDispatchFragment.h"
#endif

#include "plpgsql.h"
#include "executor/resultCache.h"
#include "parser/parse_func.h"
#include "catalog/pg_trigger.h"
#include "pgstat.h"
/*
 * All plpgsql function executions within a single transaction share the same
 * executor EState for evaluating "simple" expressions.  Each function call
 * creates its own "eval_econtext" ExprContext within this estate for
 * per-evaluation workspace.  eval_econtext is freed at normal function exit,
 * and the EState is freed at transaction end (in case of error, we assume
 * that the abort mechanisms clean it all up).  Furthermore, any exception
 * block within a function has to have its own eval_econtext separate from
 * the containing function's, so that we can clean up ExprContext callbacks
 * properly at subtransaction exit.  We maintain a stack that tracks the
 * individual econtexts so that we can clean up correctly at subxact exit.
 *
 * This arrangement is a bit tedious to maintain, but it's worth the trouble
 * so that we don't have to re-prepare simple expressions on each trip through
 * a function.  (We assume the case to optimize is many repetitions of a
 * function within a transaction.)
 *
 * However, there's no value in trying to amortize simple expression setup
 * across multiple executions of a DO block (inline code block), since there
 * can never be any.  If we use the shared EState for a DO block, the expr
 * state trees are effectively leaked till end of transaction, and that can
 * add up if the user keeps on submitting DO blocks.  Therefore, each DO block
 * has its own simple-expression EState, which is cleaned up at exit from
 * plpgsql_inline_handler().  DO blocks still use the simple_econtext_stack,
 * though, so that subxact abort cleanup does the right thing.
 *
 * (However, if a DO block executes COMMIT or ROLLBACK, then exec_stmt_commit
 * or exec_stmt_rollback will unlink it from the DO's simple-expression EState
 * and create a new shared EState that will be used thenceforth.  The original
 * EState will be cleaned up when we get back to plpgsql_inline_handler.  This
 * is a bit ugly, but it isn't worth doing better, since scenarios like this
 * can't result in indefinite accumulation of state trees.)
 */
typedef struct SimpleEcontextStackEntry
{
	ExprContext *stack_econtext;	/* a stacked econtext */
	SubTransactionId xact_subxid;	/* ID for current subxact */
	struct SimpleEcontextStackEntry *next;	/* next stack entry up */
} SimpleEcontextStackEntry;

static EState *shared_simple_eval_estate = NULL;
static SimpleEcontextStackEntry *simple_econtext_stack = NULL;

/*
 * In addition to the shared simple-eval EState, we have a shared resource
 * owner that holds refcounts on the CachedPlans for any "simple" expressions
 * we have evaluated in the current transaction.  This allows us to avoid
 * continually grabbing and releasing a plan refcount when a simple expression
 * is used over and over.  (DO blocks use their own resowner, in exactly the
 * same way described above for shared_simple_eval_estate.)
 */
static ResourceOwner shared_simple_eval_resowner = NULL;

/*
 * Memory management within a plpgsql function generally works with three
 * contexts:
 *
 * 1. Function-call-lifespan data, such as variable values, is kept in the
 * "main" context, a/k/a the "SPI Proc" context established by SPI_connect().
 * This is usually the CurrentMemoryContext while running code in this module
 * (which is not good, because careless coding can easily cause
 * function-lifespan memory leaks, but we live with it for now).
 *
 * 2. Some statement-execution routines need statement-lifespan workspace.
 * A suitable context is created on-demand by get_stmt_mcontext(), and must
 * be reset at the end of the requesting routine.  Error recovery will clean
 * it up automatically.  Nested statements requiring statement-lifespan
 * workspace will result in a stack of such contexts, see push_stmt_mcontext().
 *
 * 3. We use the eval_econtext's per-tuple memory context for expression
 * evaluation, and as a general-purpose workspace for short-lived allocations.
 * Such allocations usually aren't explicitly freed, but are left to be
 * cleaned up by a context reset, typically done by exec_eval_cleanup().
 *
 * These macros are for use in making short-lived allocations:
 */
#define get_eval_mcontext(estate) \
	((estate)->eval_econtext->ecxt_per_tuple_memory)
#define eval_mcontext_alloc(estate, sz) \
	MemoryContextAlloc(get_eval_mcontext(estate), sz)
#define eval_mcontext_alloc0(estate, sz) \
	MemoryContextAllocZero(get_eval_mcontext(estate), sz)

#ifdef __OPENTENBASE__
extern int PGDLLIMPORT g_in_plpgsql_exec_fun;
extern bool PGDLLIMPORT PlpgsqlDebugPrint;
#endif
extern int max_remote_query_fetch;

/*
 * We use a session-wide hash table for caching cast information.
 *
 * Once built, the compiled expression trees (cast_expr fields) survive for
 * the life of the session.  At some point it might be worth invalidating
 * those after pg_cast changes, but for the moment we don't bother.
 *
 * The evaluation state trees (cast_exprstate) are managed in the same way as
 * simple expressions (i.e., we assume cast expressions are always simple).
 *
 * As with simple expressions, DO blocks don't use the shared hash table but
 * must have their own.  This isn't ideal, but we don't want to deal with
 * multiple simple_eval_estates within a DO block.
 */
typedef struct					/* lookup key for cast info */
{
	/* NB: we assume this struct contains no padding bytes */
	Oid			srctype;		/* source type for cast */
	Oid			dsttype;		/* destination type for cast */
	int32		srctypmod;		/* source typmod for cast */
	int32		dsttypmod;		/* destination typmod for cast */
} plpgsql_CastHashKey;

typedef struct					/* cast_hash table entry */
{
	plpgsql_CastHashKey key;	/* hash key --- MUST BE FIRST */
	Expr	   *cast_expr;		/* cast expression, or NULL if no-op cast */
	/* ExprState is valid only when cast_lxid matches current LXID */
	ExprState  *cast_exprstate; /* expression's eval tree */
	bool		cast_in_use;	/* true while we're executing eval tree */
	LocalTransactionId cast_lxid;
	MemoryContext eval_expr_ctx;
} plpgsql_CastHashEntry;

static MemoryContext shared_cast_context = NULL;
static HTAB *shared_cast_hash = NULL;

/************************************************************
 * Local function forward declarations
 ************************************************************/
static void coerce_function_result_tuple(PLpgSQL_execstate *estate,
							 TupleDesc tupdesc);
static void plpgsql_exec_error_callback(void *arg);
static void copy_plpgsql_datums(PLpgSQL_execstate *estate,
					PLpgSQL_function *func);
static MemoryContext get_stmt_mcontext(PLpgSQL_execstate *estate);
static void push_stmt_mcontext(PLpgSQL_execstate *estate);
static void pop_stmt_mcontext(PLpgSQL_execstate *estate);

static int	exec_toplevel_block(PLpgSQL_execstate *estate,
								PLpgSQL_stmt_block *block);
static int exec_stmt_block(PLpgSQL_execstate *estate,
				PLpgSQL_stmt_block *block);
static int exec_stmts(PLpgSQL_execstate *estate,
		   List *stmts);
static int exec_stmt_assign(PLpgSQL_execstate *estate,
				 PLpgSQL_stmt_assign *stmt);
static int exec_stmt_perform(PLpgSQL_execstate *estate,
				  PLpgSQL_stmt_perform *stmt);
static int exec_stmt_call(PLpgSQL_execstate *estate,
				  PLpgSQL_stmt_call *stmt);
static int exec_stmt_getdiag(PLpgSQL_execstate *estate,
				  PLpgSQL_stmt_getdiag *stmt);
static int exec_stmt_if(PLpgSQL_execstate *estate,
			 PLpgSQL_stmt_if *stmt);
static int exec_stmt_case(PLpgSQL_execstate *estate,
			   PLpgSQL_stmt_case *stmt);
static int exec_stmt_loop(PLpgSQL_execstate *estate,
			   PLpgSQL_stmt_loop *stmt);
static int exec_stmt_while(PLpgSQL_execstate *estate,
				PLpgSQL_stmt_while *stmt);
static int exec_stmt_fori(PLpgSQL_execstate *estate,
			   PLpgSQL_stmt_fori *stmt);
static int exec_stmt_fors(PLpgSQL_execstate *estate,
			   PLpgSQL_stmt_fors *stmt);
static int exec_stmt_forc(PLpgSQL_execstate *estate,
			   PLpgSQL_stmt_forc *stmt);
static int exec_stmt_foreach_a(PLpgSQL_execstate *estate,
					PLpgSQL_stmt_foreach_a *stmt);
static int exec_stmt_open(PLpgSQL_execstate *estate,
			   PLpgSQL_stmt_open *stmt);
static int exec_stmt_fetch(PLpgSQL_execstate *estate,
				PLpgSQL_stmt_fetch *stmt);
static int exec_stmt_close(PLpgSQL_execstate *estate,
				PLpgSQL_stmt_close *stmt);
static int exec_stmt_exit(PLpgSQL_execstate *estate,
			   PLpgSQL_stmt_exit *stmt);
static int exec_stmt_return(PLpgSQL_execstate *estate,
				 PLpgSQL_stmt_return *stmt);
static int exec_stmt_return_next(PLpgSQL_execstate *estate,
					  PLpgSQL_stmt_return_next *stmt);
static int exec_stmt_return_query(PLpgSQL_execstate *estate,
					   PLpgSQL_stmt_return_query *stmt);
static int exec_stmt_raise(PLpgSQL_execstate *estate,
				PLpgSQL_stmt_raise *stmt);
static int exec_stmt_assert(PLpgSQL_execstate *estate,
				 PLpgSQL_stmt_assert *stmt);
static int exec_stmt_execsql(PLpgSQL_execstate *estate,
				  PLpgSQL_stmt_execsql *stmt);
static int exec_stmt_dynexecute(PLpgSQL_execstate *estate,
					 PLpgSQL_stmt_dynexecute *stmt);
static int exec_stmt_dynfors(PLpgSQL_execstate *estate,
				  PLpgSQL_stmt_dynfors *stmt);
static int exec_stmt_commit(PLpgSQL_execstate *estate,
				 PLpgSQL_stmt_commit *stmt);
static int exec_stmt_rollback(PLpgSQL_execstate *estate,
				PLpgSQL_stmt_rollback *stmt);
static int exec_stmt_set(PLpgSQL_execstate *estate,
                  PLpgSQL_stmt_set *stmt);

static void plpgsql_estate_setup(PLpgSQL_execstate *estate,
								 PLpgSQL_function *func,
								 ReturnSetInfo *rsi,
								 EState *simple_eval_estate,
								 ResourceOwner simple_eval_resowner);
static void exec_eval_cleanup(PLpgSQL_execstate *estate);

static void exec_prepare_plan(PLpgSQL_execstate *estate,
							  PLpgSQL_expr *expr, int cursorOptions);
static void exec_simple_check_plan(PLpgSQL_execstate *estate, PLpgSQL_expr *expr);
static void exec_save_simple_expr(PLpgSQL_expr *expr, CachedPlan *cplan);
static void exec_check_rw_parameter(PLpgSQL_expr *expr, int target_dno);
static bool contains_target_param(Node *node, int *target_dno);
static bool exec_eval_simple_expr(PLpgSQL_execstate *estate,
					  PLpgSQL_expr *expr,
					  Datum *result,
					  bool *isNull,
					  Oid *rettype,
					  int32 *rettypmod);

static void exec_assign_expr(PLpgSQL_execstate *estate,
				 PLpgSQL_datum *target,
				 PLpgSQL_expr *expr);
static bool exec_check_rec_distkey(PLpgSQL_execstate *estate, PLpgSQL_recfield *recfield,
                 Oid diskey_type);
static void exec_assign_c_string(PLpgSQL_execstate *estate,
					 PLpgSQL_datum *target,
					 const char *str);
static void exec_assign_value(PLpgSQL_execstate *estate,
				  PLpgSQL_datum *target,
				  Datum value, bool isNull,
				  Oid valtype, int32 valtypmod);
static void exec_eval_datum(PLpgSQL_execstate *estate,
				PLpgSQL_datum *datum,
				Oid *typeid,
				int32 *typetypmod,
				Datum *value,
				bool *isnull);
static int exec_eval_integer(PLpgSQL_execstate *estate,
				  PLpgSQL_expr *expr,
				  bool *isNull);
static bool exec_eval_boolean(PLpgSQL_execstate *estate,
				  PLpgSQL_expr *expr,
				  bool *isNull);
static Datum exec_eval_expr(PLpgSQL_execstate *estate,
			   PLpgSQL_expr *expr,
			   bool *isNull,
			   Oid *rettype,
			   int32 *rettypmod);
static int exec_run_select(PLpgSQL_execstate *estate,
				PLpgSQL_expr *expr, long maxtuples, Portal *portalP);
static int exec_for_query(PLpgSQL_execstate *estate, PLpgSQL_stmt_forq *stmt,
			   Portal portal, bool prefetch_ok);
static ParamListInfo setup_param_list(PLpgSQL_execstate *estate,
				 PLpgSQL_expr *expr);
static ParamExternData *plpgsql_param_fetch(ParamListInfo params,
					int paramid, bool speculative,
					ParamExternData *workspace);
static void plpgsql_param_compile(ParamListInfo params, Param *param,
					  ExprState *state,
					  Datum *resv, bool *resnull);
static void plpgsql_param_eval_var(ExprState *state, ExprEvalStep *op,
					   ExprContext *econtext);
static void plpgsql_param_eval_var_ro(ExprState *state, ExprEvalStep *op,
						  ExprContext *econtext);
static void plpgsql_param_eval_recfield(ExprState *state, ExprEvalStep *op,
							ExprContext *econtext);
static void plpgsql_param_eval_generic(ExprState *state, ExprEvalStep *op,
						   ExprContext *econtext);
static void plpgsql_param_eval_generic_ro(ExprState *state, ExprEvalStep *op,
							  ExprContext *econtext);
static void exec_move_row(PLpgSQL_execstate *estate,
			  PLpgSQL_variable *target,
			  HeapTuple tup, TupleDesc tupdesc);
static ExpandedRecordHeader *make_expanded_record_for_rec(PLpgSQL_execstate *estate,
							 PLpgSQL_rec *rec,
							 TupleDesc srctupdesc,
							 ExpandedRecordHeader *srcerh);
static void exec_move_row_from_fields(PLpgSQL_execstate *estate,
						  PLpgSQL_variable *target,
						  ExpandedRecordHeader *newerh,
						  Datum *values, bool *nulls,
						  TupleDesc tupdesc);
static bool compatible_tupdescs(TupleDesc src_tupdesc, TupleDesc dst_tupdesc);
static HeapTuple make_tuple_from_row(PLpgSQL_execstate *estate,
					PLpgSQL_row *row,
					TupleDesc tupdesc);
static TupleDesc deconstruct_composite_datum(Datum value,
							HeapTupleData *tmptup);
static void exec_move_row_from_datum(PLpgSQL_execstate *estate,
						 PLpgSQL_variable *target,
						 Datum value);
static void instantiate_empty_record_variable(PLpgSQL_execstate *estate,
								  PLpgSQL_rec *rec);
static char *convert_value_to_string(PLpgSQL_execstate *estate,
									 Datum value, Oid valtype);
static inline Datum exec_cast_value(PLpgSQL_execstate *estate,
									Datum value, bool *isnull,
									Oid valtype, int32 valtypmod,
									Oid reqtype, int32 reqtypmod);
static Datum do_cast_value(PLpgSQL_execstate *estate,
						   Datum value, bool *isnull,
						   Oid valtype, int32 valtypmod,
						   Oid reqtype, int32 reqtypmod);
static plpgsql_CastHashEntry *get_cast_hashentry(PLpgSQL_execstate *estate,
				   Oid srctype, int32 srctypmod,
				   Oid dsttype, int32 dsttypmod);
static void exec_init_tuple_store(PLpgSQL_execstate *estate);
static void exec_set_found(PLpgSQL_execstate *estate, bool state);
static void plpgsql_create_econtext(PLpgSQL_execstate *estate);
static void plpgsql_destroy_econtext(PLpgSQL_execstate *estate);
static void assign_simple_var(PLpgSQL_execstate *estate, PLpgSQL_var *var,
				  Datum newvalue, bool isnull, bool freeable);
static void assign_text_var(PLpgSQL_execstate *estate, PLpgSQL_var *var,
				const char *str);
static void assign_record_var(PLpgSQL_execstate *estate, PLpgSQL_rec *rec,
				  ExpandedRecordHeader *erh);
static ParamListInfo exec_eval_using_params(PLpgSQL_execstate *estate,
					   List *params);
static Portal exec_dynquery_with_params(PLpgSQL_execstate *estate,
						  PLpgSQL_expr *dynquery, List *params,
						  const char *portalname, int cursorOptions);
static char *format_expr_params(PLpgSQL_execstate *estate,
				   const PLpgSQL_expr *expr);
static char *format_preparedparamsdata(PLpgSQL_execstate *estate,
									   ParamListInfo paramLI);
static PLpgSQL_variable *make_callstmt_target(PLpgSQL_execstate *estate,
											  PLpgSQL_expr *expr);
static void exec_check_stmts(PLpgSQL_function *func, List *stmts, FuncCheckData *chkdata);
static void exec_check_stmt_block(PLpgSQL_function *func, PLpgSQL_stmt_block *block, FuncCheckData *chkdata);
static void exec_check_expr(PLpgSQL_function *func, PLpgSQL_expr *expr, FuncCheckData *chkdata);

/* ----------
 * plpgsql_exec_function	Called by the call handler for
 *				function execution.
 *
 * This is also used to execute inline code blocks (DO blocks).  The only
 * difference that this code is aware of is that for a DO block, we want
 * to use a private simple_eval_estate and a private simple_eval_resowner,
 * which are created and passed in by the caller.  For regular functions,
 * pass NULL, which implies using shared_simple_eval_estate and
 * shared_simple_eval_resowner.  (When using a private simple_eval_estate,
 * we must also use a private cast hashtable, but that's taken care of
 * within plpgsql_estate_setup.)
 * procedure_resowner is a resowner that will survive for the duration
 * of execution of this function/procedure.  It is needed only if we
 * are doing non-atomic execution and there are CALL or DO statements
 * in the function; otherwise it can be NULL.  We use it to hold refcounts
 * on the CALL/DO statements' plans.
 * ----------
 */
Datum
plpgsql_exec_function(PLpgSQL_function *func, FunctionCallInfo fcinfo,
					  EState *simple_eval_estate,
					  ResourceOwner simple_eval_resowner,
					  ResourceOwner procedure_resowner,
					  bool atomic, FuncCheckData *tgchkdata)
{
	PLpgSQL_execstate estate;
	ErrorContextCallback plerrcontext;
	int			i;
	int			rc;
	int plpgsql_level = SetEnterPlpgsqlFunc();

	/*
	 * Setup the execution state
	 */
	plpgsql_estate_setup(&estate, func, (ReturnSetInfo *) fcinfo->resultinfo,
						 simple_eval_estate, simple_eval_resowner);
	estate.procedure_resowner = procedure_resowner;
	estate.atomic = atomic;
	estate.check_pullup = tgchkdata ? &tgchkdata->pullup : NULL;
	estate.check_target_oid = tgchkdata ? tgchkdata->relid : InvalidOid;
	estate.check_tgtype = tgchkdata ? tgchkdata->tgtype : 0;

	/*
	 * Setup error traceback support for ereport()
	 */
	plerrcontext.callback = plpgsql_exec_error_callback;
	plerrcontext.arg = &estate;
	plerrcontext.previous = error_context_stack;
	error_context_stack = &plerrcontext;

	/*
	 * Make local execution copies of all the datums
	 */
	estate.err_text = gettext_noop("during initialization of execution state");
	copy_plpgsql_datums(&estate, func);

	if (estate.check_pullup)
	{
		Relation relation;
		PLpgSQL_rec *rec_new, *rec_old;

		if (estate.func->fn_is_trigger != PLPGSQL_DML_TRIGGER)
		{
			elog(ERROR, "function is not created for trigger execution");
		}

		if (!OidIsValid(tgchkdata->relid))
		{
			elog(ERROR, "check trigger context is set, but trigger's target relation is not provided");
		}

		relation = relation_open(tgchkdata->relid, NoLock);

		rec_new = (PLpgSQL_rec *) (estate.datums[func->new_varno]);
		rec_new->rectypeid = relation->rd_att->tdtypeid;

		rec_old = (PLpgSQL_rec *) (estate.datums[func->old_varno]);
		rec_old->rectypeid = relation->rd_att->tdtypeid;

		relation_close(relation, NoLock);
	}

	/*
	 * Store the actual call argument values into the appropriate variables
	 */
	estate.err_text = gettext_noop("while storing call arguments into local variables");
	for (i = 0; i < func->fn_nargs; i++)
	{
		int			n = func->fn_argvarnos[i];

		if (n < 0 || n >= estate.ndatums)
			continue;

		switch (estate.datums[n]->dtype)
		{
			case PLPGSQL_DTYPE_VAR:
				{
					PLpgSQL_var *var = (PLpgSQL_var *) estate.datums[n];

					assign_simple_var(&estate, var,
									  fcinfo->arg[i],
									  fcinfo->argnull[i],
									  false);

					/*
					 * Force any array-valued parameter to be stored in
					 * expanded form in our local variable, in hopes of
					 * improving efficiency of uses of the variable.  (This is
					 * a hack, really: why only arrays? Need more thought
					 * about which cases are likely to win.  See also
					 * typisarray-specific heuristic in exec_assign_value.)
					 *
					 * Special cases: If passed a R/W expanded pointer, assume
					 * we can commandeer the object rather than having to copy
					 * it.  If passed a R/O expanded pointer, just keep it as
					 * the value of the variable for the moment.  (We'll force
					 * it to R/W if the variable gets modified, but that may
					 * very well never happen.)
					 */
					if (!var->isnull && var->datatype->typisarray)
					{
						if (VARATT_IS_EXTERNAL_EXPANDED_RW(DatumGetPointer(var->value)))
						{
							/* take ownership of R/W object */
							assign_simple_var(&estate, var,
											  TransferExpandedObject(var->value,
																	 estate.datum_context),
											  false,
											  true);
						}
						else if (VARATT_IS_EXTERNAL_EXPANDED_RO(DatumGetPointer(var->value)))
						{
							/* R/O pointer, keep it as-is until assigned to */
						}
						else
						{
							/* flat array, so force to expanded form */
							assign_simple_var(&estate, var,
											  expand_array(var->value,
														   estate.datum_context,
														   NULL),
											  false,
											  true);
						}
					}
				}
				break;

			case PLPGSQL_DTYPE_REC:
				{
					PLpgSQL_rec *rec = (PLpgSQL_rec *) estate.datums[n];

					if (!fcinfo->argnull[i])
					{
						/* Assign row value from composite datum */
						exec_move_row_from_datum(&estate,
												 (PLpgSQL_variable *) rec,
												 fcinfo->arg[i]);
					}
					else
					{
						/* If arg is null, treat it as an empty row */
						exec_move_row(&estate, (PLpgSQL_variable *) rec,
									  NULL, NULL);
					}
					/* clean up after exec_move_row() */
					exec_eval_cleanup(&estate);
				}
				break;

			default:
				elog(ERROR, "unrecognized dtype: %d", func->datums[i]->dtype);
		}
	}

	estate.err_text = gettext_noop("during function entry");

	/*
	 * Set the magic variable FOUND to false
	 */
	exec_set_found(&estate, false);

	/*
	 * Let the instrumentation plugin peek at this function
	 */
	if (*plpgsql_plugin_ptr && (*plpgsql_plugin_ptr)->func_beg)
		((*plpgsql_plugin_ptr)->func_beg) (&estate, func);

	/*
	 * Now call the toplevel block of statements
	 */
	estate.err_text = NULL;
	rc = exec_toplevel_block(&estate, func->action);

	if (estate.check_pullup)
	{
		SetExitPlpgsqlFunc(plpgsql_level);
		return (Datum) 0;
	}

	if (rc != PLPGSQL_RC_RETURN)
	{
		estate.err_text = NULL;
		ereport(ERROR,
				(errcode(ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT),
				 errmsg("control reached end of function without RETURN")));
	}

	/*
	 * We got a return value - process it
	 */
	estate.err_text = gettext_noop("while casting return value to function's return type");

	fcinfo->isnull = estate.check_pullup != NULL ? false : estate.retisnull;

	if (estate.retisset)
	{
		ReturnSetInfo *rsi = estate.rsi;

		/* Check caller can handle a set result */
		if (!rsi || !IsA(rsi, ReturnSetInfo) ||
			(rsi->allowedModes & SFRM_Materialize) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("set-valued function called in context that cannot accept a set")));
		rsi->returnMode = SFRM_Materialize;

		/* If we produced any tuples, send back the result */
		if (estate.tuple_store)
		{
			MemoryContext oldcxt;

			rsi->setResult = estate.tuple_store;
			oldcxt = MemoryContextSwitchTo(estate.tuple_store_cxt);
			rsi->setDesc = CreateTupleDescCopy(estate.tuple_store_desc);
			MemoryContextSwitchTo(oldcxt);
		}
		estate.retval = (Datum) 0;
		fcinfo->isnull = true;
	}
	else if (!estate.retisnull)
	{
		if (!func->fn_rettype)
			ereport(ERROR,
					(errmsg("cannot return a value from a procedure")));

		/*
		 * Cast result value to function's declared result type, and copy it
		 * out to the upper executor memory context.  We must treat tuple
		 * results specially in order to deal with cases like rowtypes
		 * involving dropped columns.
		 */
		if (estate.retistuple)
		{
			/* Don't need coercion if rowtype is known to match */
			if (func->fn_rettype == estate.rettype &&
				func->fn_rettype != RECORDOID)
			{
				/*
				 * Copy the tuple result into upper executor memory context.
				 * However, if we have a R/W expanded datum, we can just
				 * transfer its ownership out to the upper context.
				 */
				estate.retval = SPI_datumTransfer(estate.retval,
												  false,
												  -1);
			}
			else
			{
				/*
				 * Need to look up the expected result type.  XXX would be
				 * better to cache the tupdesc instead of repeating
				 * get_call_result_type(), but the only easy place to save it
				 * is in the PLpgSQL_function struct, and that's too
				 * long-lived: composite types could change during the
				 * existence of a PLpgSQL_function.
				 */
				Oid			resultTypeId;
				TupleDesc	tupdesc;

				switch (get_call_result_type(fcinfo, &resultTypeId, &tupdesc))
				{
					case TYPEFUNC_COMPOSITE:
						/* got the expected result rowtype, now coerce it */
						coerce_function_result_tuple(&estate, tupdesc);
						break;
					case TYPEFUNC_COMPOSITE_DOMAIN:
						/* got the expected result rowtype, now coerce it */
						coerce_function_result_tuple(&estate, tupdesc);
						/* and check domain constraints */
						/* XXX allowing caching here would be good, too */
						domain_check(estate.retval, false, resultTypeId,
									 NULL, NULL);
						break;
					case TYPEFUNC_RECORD:

						/*
						 * Failed to determine actual type of RECORD.  We
						 * could raise an error here, but what this means in
						 * practice is that the caller is expecting any old
						 * generic rowtype, so we don't really need to be
						 * restrictive.  Pass back the generated result as-is.
						 */
						estate.retval = SPI_datumTransfer(estate.retval,
														  false,
														  -1);
						break;
					default:
						/* shouldn't get here if retistuple is true ... */
						elog(ERROR, "return type must be a row type");
						break;
				}
			}
		}
		else
		{
			/* Scalar case: use exec_cast_value */
			estate.retval = exec_cast_value(&estate,
											estate.retval,
											&fcinfo->isnull,
											estate.rettype,
											-1,
											func->fn_rettype,
											-1);

			/*
			 * If the function's return type isn't by value, copy the value
			 * into upper executor memory context.  However, if we have a R/W
			 * expanded datum, we can just transfer its ownership out to the
			 * upper executor context.
			 */
			if (!fcinfo->isnull && !func->fn_retbyval)
				estate.retval = SPI_datumTransfer(estate.retval,
												  false,
												  func->fn_rettyplen);
		}
	}

	estate.err_text = gettext_noop("during function exit");

	/*
	 * Let the instrumentation plugin peek at this function
	 */
	if (*plpgsql_plugin_ptr && (*plpgsql_plugin_ptr)->func_end)
		((*plpgsql_plugin_ptr)->func_end) (&estate, func);

	/* Clean up any leftover temporary memory */
	plpgsql_destroy_econtext(&estate);
	exec_eval_cleanup(&estate);
	/* stmt_mcontext will be destroyed when function's main context is */

	/*
	 * Pop the error context stack
	 */
	error_context_stack = plerrcontext.previous;

	/*
	 * Return the function's result
	 */

#ifdef __OPENTENBASE__
	SetExitPlpgsqlFunc(plpgsql_level);
	if (PlpgsqlDebugPrint)
		elog(LOG,
			"[PLPGSQL] plpgsql_exec_function end g_in_plpgsql_exec_fun:%d",
			g_in_plpgsql_exec_fun);
#endif

	return estate.retval;
}

/*
 * Helper for plpgsql_exec_function: coerce composite result to the specified
 * tuple descriptor, and copy it out to upper executor memory.  This is split
 * out mostly for cosmetic reasons --- the logic would be very deeply nested
 * otherwise.
 *
 * estate->retval is updated in-place.
 */
static void
coerce_function_result_tuple(PLpgSQL_execstate *estate, TupleDesc tupdesc)
{
	HeapTuple	rettup;
	TupleDesc	retdesc;
	TupleConversionMap *tupmap;

	/* We assume exec_stmt_return verified that result is composite */
	Assert(type_is_rowtype(estate->rettype));

	/* We can special-case expanded records for speed */
	if (VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(estate->retval)))
	{
		ExpandedRecordHeader *erh = (ExpandedRecordHeader *) DatumGetEOHP(estate->retval);

		Assert(erh->er_magic == ER_MAGIC);

		/* Extract record's TupleDesc */
		retdesc = expanded_record_get_tupdesc(erh);

		/* check rowtype compatibility */
		tupmap = convert_tuples_by_position(retdesc,
											tupdesc,
											gettext_noop("returned record type does not match expected record type"));

		/* it might need conversion */
		if (tupmap)
		{
			rettup = expanded_record_get_tuple(erh);
			Assert(rettup);
			rettup = execute_attr_map_tuple(rettup, tupmap, NULL);

			/*
			 * Copy tuple to upper executor memory, as a tuple Datum.  Make
			 * sure it is labeled with the caller-supplied tuple type.
			 */
			estate->retval = PointerGetDatum(SPI_returntuple(rettup, tupdesc));
			/* no need to free map, we're about to return anyway */
		}
		else
		{
			/*
			 * We need only copy result into upper executor memory context.
			 * However, if we have a R/W expanded datum, we can just transfer
			 * its ownership out to the upper executor context.
			 */
			estate->retval = SPI_datumTransfer(estate->retval,
											   false,
											   -1);
		}
	}
	else
	{
		/* Convert composite datum to a HeapTuple and TupleDesc */
		HeapTupleData tmptup;

		retdesc = deconstruct_composite_datum(estate->retval, &tmptup);
		rettup = &tmptup;

		/* check rowtype compatibility */
		tupmap = convert_tuples_by_position(retdesc,
											tupdesc,
											gettext_noop("returned record type does not match expected record type"));

		/* it might need conversion */
		if (tupmap)
			rettup = execute_attr_map_tuple(rettup, tupmap, NULL);

		/*
		 * Copy tuple to upper executor memory, as a tuple Datum.  Make sure
		 * it is labeled with the caller-supplied tuple type.
		 */
		estate->retval = PointerGetDatum(SPI_returntuple(rettup, tupdesc));

		/* no need to free map, we're about to return anyway */

		ReleaseTupleDesc(retdesc);
	}
}


/* ----------
 * plpgsql_exec_trigger		Called by the call handler for
 *				trigger execution.
 * ----------
 */
HeapTuple
plpgsql_exec_trigger(PLpgSQL_function *func,
					 TriggerData *trigdata)
{
	PLpgSQL_execstate estate;
	ErrorContextCallback plerrcontext;
	int			i;
	int			rc;
	TupleDesc	tupdesc;
	PLpgSQL_var *var;
	PLpgSQL_rec *rec_new,
			   *rec_old;
	HeapTuple	rettup;

	int plpgsql_level = SetEnterPlpgsqlFunc();

	/*
	 * Setup the execution state
	 */
	plpgsql_estate_setup(&estate, func, NULL, NULL, NULL);
	/* TODO */
	//estate.trigdata = trigdata;

	/*
	 * Setup error traceback support for ereport()
	 */
	plerrcontext.callback = plpgsql_exec_error_callback;
	plerrcontext.arg = &estate;
	plerrcontext.previous = error_context_stack;
	error_context_stack = &plerrcontext;

	/*
	 * Make local execution copies of all the datums
	 */
	estate.err_text = gettext_noop("during initialization of execution state");
	copy_plpgsql_datums(&estate, func);

	/*
	 * Put the OLD and NEW tuples into record variables
	 *
	 * We make the tupdescs available in both records even though only one may
	 * have a value.  This allows parsing of record references to succeed in
	 * functions that are used for multiple trigger types.  For example, we
	 * might have a test like "if (TG_OP = 'INSERT' and NEW.foo = 'xyz')",
	 * which should parse regardless of the current trigger type.
	 */
	tupdesc = RelationGetDescr(trigdata->tg_relation);

	rec_new = (PLpgSQL_rec *) (estate.datums[func->new_varno]);
	rec_old = (PLpgSQL_rec *) (estate.datums[func->old_varno]);

	rec_new->erh = make_expanded_record_from_tupdesc(tupdesc,
													 estate.datum_context);
	rec_old->erh = make_expanded_record_from_exprecord(rec_new->erh,
													   estate.datum_context);

	if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
	{
		/*
		 * Per-statement triggers don't use OLD/NEW variables
		 */
	}
	else if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
	{
		expanded_record_set_tuple(rec_new->erh, trigdata->tg_trigtuple, false);
	}
	else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
	{
		expanded_record_set_tuple(rec_new->erh, trigdata->tg_newtuple, false);
		expanded_record_set_tuple(rec_old->erh, trigdata->tg_trigtuple, false);
	}
	else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
	{
		expanded_record_set_tuple(rec_old->erh, trigdata->tg_trigtuple, false);
	}
	else
		elog(ERROR, "unrecognized trigger action: not INSERT, DELETE, or UPDATE");

	/* Make transition tables visible to this SPI connection */
	rc = SPI_register_trigger_data(trigdata);
	Assert(rc >= 0);

	/*
	 * Assign the special tg_ variables
	 */

	var = (PLpgSQL_var *) (estate.datums[func->tg_op_varno]);
	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		assign_text_var(&estate, var, "INSERT");
	else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		assign_text_var(&estate, var, "UPDATE");
	else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
		assign_text_var(&estate, var, "DELETE");
	else if (TRIGGER_FIRED_BY_TRUNCATE(trigdata->tg_event))
		assign_text_var(&estate, var, "TRUNCATE");
	else
		elog(ERROR, "unrecognized trigger action: not INSERT, DELETE, UPDATE, or TRUNCATE");

	var = (PLpgSQL_var *) (estate.datums[func->tg_name_varno]);
	assign_simple_var(&estate, var,
					  DirectFunctionCall1(namein,
										  CStringGetDatum(trigdata->tg_trigger->tgname)),
					  false, true);

	var = (PLpgSQL_var *) (estate.datums[func->tg_when_varno]);
	if (TRIGGER_FIRED_BEFORE(trigdata->tg_event))
		assign_text_var(&estate, var, "BEFORE");
	else if (TRIGGER_FIRED_AFTER(trigdata->tg_event))
		assign_text_var(&estate, var, "AFTER");
	else if (TRIGGER_FIRED_INSTEAD(trigdata->tg_event))
		assign_text_var(&estate, var, "INSTEAD OF");
	else
		elog(ERROR, "unrecognized trigger execution time: not BEFORE, AFTER, or INSTEAD OF");

	var = (PLpgSQL_var *) (estate.datums[func->tg_level_varno]);
	if (TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		assign_text_var(&estate, var, "ROW");
	else if (TRIGGER_FIRED_FOR_STATEMENT(trigdata->tg_event))
		assign_text_var(&estate, var, "STATEMENT");
	else
		elog(ERROR, "unrecognized trigger event type: not ROW or STATEMENT");

	var = (PLpgSQL_var *) (estate.datums[func->tg_relid_varno]);
	assign_simple_var(&estate, var,
					  ObjectIdGetDatum(trigdata->tg_relation->rd_id),
					  false, false);

	var = (PLpgSQL_var *) (estate.datums[func->tg_relname_varno]);
	assign_simple_var(&estate, var,
					  DirectFunctionCall1(namein,
										  CStringGetDatum(RelationGetRelationName(trigdata->tg_relation))),
					  false, true);

	var = (PLpgSQL_var *) (estate.datums[func->tg_table_name_varno]);
	assign_simple_var(&estate, var,
					  DirectFunctionCall1(namein,
										  CStringGetDatum(RelationGetRelationName(trigdata->tg_relation))),
					  false, true);

	var = (PLpgSQL_var *) (estate.datums[func->tg_table_schema_varno]);
	assign_simple_var(&estate, var,
					  DirectFunctionCall1(namein,
										  CStringGetDatum(get_namespace_name(
																			 RelationGetNamespace(
																								  trigdata->tg_relation)))),
					  false, true);

	var = (PLpgSQL_var *) (estate.datums[func->tg_nargs_varno]);
	assign_simple_var(&estate, var,
					  Int16GetDatum(trigdata->tg_trigger->tgnargs),
					  false, false);

	var = (PLpgSQL_var *) (estate.datums[func->tg_argv_varno]);
	if (trigdata->tg_trigger->tgnargs > 0)
	{
		/*
		 * For historical reasons, tg_argv[] subscripts start at zero not one.
		 * So we can't use construct_array().
		 */
		int			nelems = trigdata->tg_trigger->tgnargs;
		Datum	   *elems;
		int			dims[1];
		int			lbs[1];

		elems = palloc(sizeof(Datum) * nelems);
		for (i = 0; i < nelems; i++)
			elems[i] = CStringGetTextDatum(trigdata->tg_trigger->tgargs[i]);
		dims[0] = nelems;
		lbs[0] = 0;

		assign_simple_var(&estate, var,
						  PointerGetDatum(construct_md_array(elems, NULL,
															 1, dims, lbs,
															 TEXTOID,
															 -1, false, 'i')),
						  false, true);
	}
	else
	{
		assign_simple_var(&estate, var, (Datum) 0, true, false);
	}

	estate.err_text = gettext_noop("during function entry");

	/*
	 * Set the magic variable FOUND to false
	 */
	exec_set_found(&estate, false);

	/*
	 * Let the instrumentation plugin peek at this function
	 */
	if (*plpgsql_plugin_ptr && (*plpgsql_plugin_ptr)->func_beg)
		((*plpgsql_plugin_ptr)->func_beg) (&estate, func);

	/*
	 * Now call the toplevel block of statements
	 */
	estate.err_text = NULL;
	rc = exec_toplevel_block(&estate, func->action);
	if (rc != PLPGSQL_RC_RETURN)
	{
		estate.err_text = NULL;
		ereport(ERROR,
				(errcode(ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT),
				 errmsg("control reached end of trigger procedure without RETURN")));
	}

	estate.err_text = gettext_noop("during function exit");

	if (estate.retisset)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("trigger procedure cannot return a set")));

	/*
	 * Check that the returned tuple structure has the same attributes, the
	 * relation that fired the trigger has. A per-statement trigger always
	 * needs to return NULL, so we ignore any return value the function itself
	 * produces (XXX: is this a good idea?)
	 *
	 * XXX This way it is possible, that the trigger returns a tuple where
	 * attributes don't have the correct atttypmod's length. It's up to the
	 * trigger's programmer to ensure that this doesn't happen. Jan
	 */
	if (estate.retisnull || !TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		rettup = NULL;
	else
	{
		TupleDesc	retdesc;
		TupleConversionMap *tupmap;

		/* We assume exec_stmt_return verified that result is composite */
		Assert(type_is_rowtype(estate.rettype));

		/* We can special-case expanded records for speed */
		if (VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(estate.retval)))
		{
			ExpandedRecordHeader *erh = (ExpandedRecordHeader *) DatumGetEOHP(estate.retval);

			Assert(erh->er_magic == ER_MAGIC);

			/* Extract HeapTuple and TupleDesc */
			rettup = expanded_record_get_tuple(erh);
			Assert(rettup);
			retdesc = expanded_record_get_tupdesc(erh);

			if (retdesc != RelationGetDescr(trigdata->tg_relation))
			{
				/* check rowtype compatibility */
				tupmap = convert_tuples_by_position(retdesc,
													RelationGetDescr(trigdata->tg_relation),
													gettext_noop("returned row structure does not match the structure of the triggering table"));
				/* it might need conversion */
				if (tupmap)
					rettup = execute_attr_map_tuple(rettup, tupmap, NULL);
				/* no need to free map, we're about to return anyway */
			}

			/*
			 * Copy tuple to upper executor memory.  But if user just did
			 * "return new" or "return old" without changing anything, there's
			 * no need to copy; we can return the original tuple (which will
			 * save a few cycles in trigger.c as well as here).
			 */
			if (rettup != trigdata->tg_newtuple &&
				rettup != trigdata->tg_trigtuple)
				rettup = SPI_copytuple(rettup);
		}
		else
		{
			/* Convert composite datum to a HeapTuple and TupleDesc */
			HeapTupleData tmptup;

			retdesc = deconstruct_composite_datum(estate.retval, &tmptup);
			rettup = &tmptup;

			/* check rowtype compatibility */
			tupmap = convert_tuples_by_position(retdesc,
												RelationGetDescr(trigdata->tg_relation),
												gettext_noop("returned row structure does not match the structure of the triggering table"));
			/* it might need conversion */
			if (tupmap)
				rettup = execute_attr_map_tuple(rettup, tupmap, NULL);

			ReleaseTupleDesc(retdesc);
			/* no need to free map, we're about to return anyway */

			/* Copy tuple to upper executor memory */
			rettup = SPI_copytuple(rettup);
		}
	}

	/*
	 * Let the instrumentation plugin peek at this function
	 */
	if (*plpgsql_plugin_ptr && (*plpgsql_plugin_ptr)->func_end)
		((*plpgsql_plugin_ptr)->func_end) (&estate, func);

	/* Clean up any leftover temporary memory */
	plpgsql_destroy_econtext(&estate);
	exec_eval_cleanup(&estate);
	/* stmt_mcontext will be destroyed when function's main context is */

	/*
	 * Pop the error context stack
	 */
	error_context_stack = plerrcontext.previous;

	SetExitPlpgsqlFunc(plpgsql_level);
	if (PlpgsqlDebugPrint)
		elog(LOG,
			 "[PLPGSQL] plpgsql_exec_trigger end g_in_plpgsql_exec_fun:%d",
			 g_in_plpgsql_exec_fun);

	/*
	 * Return the trigger's result
	 */
	return rettup;
}

/* ----------
 * plpgsql_exec_event_trigger		Called by the call handler for
 *				event trigger execution.
 * ----------
 */
void
plpgsql_exec_event_trigger(PLpgSQL_function *func, EventTriggerData *trigdata)
{
	PLpgSQL_execstate estate;
	ErrorContextCallback plerrcontext;
	int			rc;
	PLpgSQL_var *var;

	/*
	 * Setup the execution state
	 */
	plpgsql_estate_setup(&estate, func, NULL, NULL, NULL);
	/* TODO */
	//estate.evtrigdata = trigdata;

	/*
	 * Setup error traceback support for ereport()
	 */
	plerrcontext.callback = plpgsql_exec_error_callback;
	plerrcontext.arg = &estate;
	plerrcontext.previous = error_context_stack;
	error_context_stack = &plerrcontext;

	/*
	 * Make local execution copies of all the datums
	 */
	estate.err_text = gettext_noop("during initialization of execution state");
	copy_plpgsql_datums(&estate, func);

	/*
	 * Assign the special tg_ variables
	 */
	var = (PLpgSQL_var *) (estate.datums[func->tg_event_varno]);
	assign_text_var(&estate, var, trigdata->event);

	var = (PLpgSQL_var *) (estate.datums[func->tg_tag_varno]);
	assign_text_var(&estate, var, trigdata->tag);

	/*
	 * Let the instrumentation plugin peek at this function
	 */
	if (*plpgsql_plugin_ptr && (*plpgsql_plugin_ptr)->func_beg)
		((*plpgsql_plugin_ptr)->func_beg) (&estate, func);

	/*
	 * Now call the toplevel block of statements
	 */
	estate.err_text = NULL;
	rc = exec_toplevel_block(&estate, func->action);
	if (rc != PLPGSQL_RC_RETURN)
	{
		estate.err_text = NULL;
		ereport(ERROR,
				(errcode(ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT),
				 errmsg("control reached end of trigger procedure without RETURN")));
	}

	estate.err_text = gettext_noop("during function exit");

	/*
	 * Let the instrumentation plugin peek at this function
	 */
	if (*plpgsql_plugin_ptr && (*plpgsql_plugin_ptr)->func_end)
		((*plpgsql_plugin_ptr)->func_end) (&estate, func);

	/* Clean up any leftover temporary memory */
	plpgsql_destroy_econtext(&estate);
	exec_eval_cleanup(&estate);
	/* stmt_mcontext will be destroyed when function's main context is */

	/*
	 * Pop the error context stack
	 */
	error_context_stack = plerrcontext.previous;

	return;
}

/*
 * error context callback to let us supply a call-stack traceback
 */
static void
plpgsql_exec_error_callback(void *arg)
{
	PLpgSQL_execstate *estate = (PLpgSQL_execstate *) arg;

	if (estate->err_text != NULL)
	{
		/*
		 * We don't expend the cycles to run gettext() on err_text unless we
		 * actually need it.  Therefore, places that set up err_text should
		 * use gettext_noop() to ensure the strings get recorded in the
		 * message dictionary.
		 *
		 * If both err_text and err_stmt are set, use the err_text as
		 * description, but report the err_stmt's line number.  When err_stmt
		 * is not set, we're in function entry/exit, or some such place not
		 * attached to a specific line number.
		 */
		if (estate->err_stmt != NULL)
		{
			/*
			 * translator: last %s is a phrase such as "during statement block
			 * local variable initialization"
			 */
#ifdef __OPENTENBASE__
			if (plpgsql_contenxt_line_num_print)
			{
				errcontext("PL/pgSQL function %s line %d %s",
				           estate->func->fn_signature,
				           estate->err_stmt->lineno,
				           _(estate->err_text));
			}
			else
			{
				errcontext("PL/pgSQL function %s %s",
				           estate->func->fn_signature,
				           _(estate->err_text));
			}
#endif
		}
		else
		{
			/*
			 * translator: last %s is a phrase such as "while storing call
			 * arguments into local variables"
			 */
			errcontext("PL/pgSQL function %s %s",
					estate->func->fn_signature,
					_(estate->err_text));
		}
	}
	else if (estate->err_stmt != NULL)
	{
		/* translator: last %s is a plpgsql statement type name */
#ifdef __OPENTENBASE__
		if (plpgsql_contenxt_line_num_print)
		{
			errcontext("PL/pgSQL function %s line %d at %s",
			           estate->func->fn_signature,
			           estate->err_stmt->lineno,
			           plpgsql_stmt_typename(estate->err_stmt));
		}
		else
		{
			errcontext("PL/pgSQL function %s at %s",
			           estate->func->fn_signature,
			           plpgsql_stmt_typename(estate->err_stmt));
		}
#endif
	}
	else
	{
		errcontext("PL/pgSQL function %s",
				estate->func->fn_signature);
	}
}


/* ----------
 * Support function for initializing local execution variables
 * ----------
 */
static void
copy_plpgsql_datums(PLpgSQL_execstate *estate,
					PLpgSQL_function *func)
{
	int			ndatums = estate->ndatums;
	PLpgSQL_datum **indatums;
	PLpgSQL_datum **outdatums;
	char	   *workspace;
	char	   *ws_next;
	int			i;

	/* Allocate local datum-pointer array */
	estate->datums = (PLpgSQL_datum **)
		palloc(sizeof(PLpgSQL_datum *) * ndatums);

	/*
	 * To reduce palloc overhead, we make a single palloc request for all the
	 * space needed for locally-instantiated datums.
	 */
	workspace = palloc(func->copiable_size);
	ws_next = workspace;

	/* Fill datum-pointer array, copying datums into workspace as needed */
	indatums = func->datums;
	outdatums = estate->datums;
	for (i = 0; i < ndatums; i++)
	{
		PLpgSQL_datum *indatum = indatums[i];
		PLpgSQL_datum *outdatum;

		/* This must agree with plpgsql_finish_datums on what is copiable */
		switch (indatum->dtype)
		{
			case PLPGSQL_DTYPE_VAR:
				outdatum = (PLpgSQL_datum *) ws_next;
				memcpy(outdatum, indatum, sizeof(PLpgSQL_var));
				ws_next += MAXALIGN(sizeof(PLpgSQL_var));
				break;

			case PLPGSQL_DTYPE_REC:
				outdatum = (PLpgSQL_datum *) ws_next;
				memcpy(outdatum, indatum, sizeof(PLpgSQL_rec));
				ws_next += MAXALIGN(sizeof(PLpgSQL_rec));
				break;

			case PLPGSQL_DTYPE_ROW:
			case PLPGSQL_DTYPE_RECFIELD:
			case PLPGSQL_DTYPE_ARRAYELEM:

				/*
				 * These datum records are read-only at runtime, so no need to
				 * copy them (well, RECFIELD and ARRAYELEM contain cached
				 * data, but we'd just as soon centralize the caching anyway).
				 */
				outdatum = indatum;
				break;

			default:
				elog(ERROR, "unrecognized dtype: %d", indatum->dtype);
				outdatum = NULL;	/* keep compiler quiet */
				break;
		}

		outdatums[i] = outdatum;
	}

	Assert(ws_next == workspace + func->copiable_size);
}

/*
 * Create a memory context for statement-lifespan variables, if we don't
 * have one already.  It will be a child of stmt_mcontext_parent, which is
 * either the function's main context or a pushed-down outer stmt_mcontext.
 */
static MemoryContext
get_stmt_mcontext(PLpgSQL_execstate *estate)
{
	if (estate->stmt_mcontext == NULL)
	{
		estate->stmt_mcontext =
			AllocSetContextCreate(estate->stmt_mcontext_parent,
								  "PLpgSQL per-statement data",
								  ALLOCSET_DEFAULT_SIZES);
	}
	return estate->stmt_mcontext;
}

/*
 * Push down the current stmt_mcontext so that called statements won't use it.
 * This is needed by statements that have statement-lifespan data and need to
 * preserve it across some inner statements.  The caller should eventually do
 * pop_stmt_mcontext().
 */
static void
push_stmt_mcontext(PLpgSQL_execstate *estate)
{
	/* Should have done get_stmt_mcontext() first */
	Assert(estate->stmt_mcontext != NULL);
	/* Assert we've not messed up the stack linkage */
	Assert(MemoryContextGetParent(estate->stmt_mcontext) == estate->stmt_mcontext_parent);
	/* Push it down to become the parent of any nested stmt mcontext */
	estate->stmt_mcontext_parent = estate->stmt_mcontext;
	/* And make it not available for use directly */
	estate->stmt_mcontext = NULL;
}

/*
 * Undo push_stmt_mcontext().  We assume this is done just before or after
 * resetting the caller's stmt_mcontext; since that action will also delete
 * any child contexts, there's no need to explicitly delete whatever context
 * might currently be estate->stmt_mcontext.
 */
static void
pop_stmt_mcontext(PLpgSQL_execstate *estate)
{
	/* We need only pop the stack */
	estate->stmt_mcontext = estate->stmt_mcontext_parent;
	estate->stmt_mcontext_parent = MemoryContextGetParent(estate->stmt_mcontext);
}


/*
 * Subroutine for exec_stmt_block: does any condition in the condition list
 * match the current exception?
 */
static bool
exception_matches_conditions(ErrorData *edata, PLpgSQL_condition *cond)
{
	for (; cond != NULL; cond = cond->next)
	{
		int			sqlerrstate = cond->sqlerrstate;

		/*
		 * OTHERS matches everything *except* query-canceled and
		 * assert-failure.  If you're foolish enough, you can match those
		 * explicitly.
		 */
		if (sqlerrstate == 0)
		{
			if (edata->sqlerrcode != ERRCODE_QUERY_CANCELED &&
				edata->sqlerrcode != ERRCODE_ASSERT_FAILURE)
				return true;
		}
		/* Exact match? */
		else if (edata->sqlerrcode == sqlerrstate)
			return true;
		/* Category match? */
		else if (ERRCODE_IS_CATEGORY(sqlerrstate) &&
				 ERRCODE_TO_CATEGORY(edata->sqlerrcode) == sqlerrstate)
			return true;
	}
	return false;
}

/* ----------
 * exec_toplevel_block			Execute the toplevel block
 *
 * This is intentionally equivalent to executing exec_stmts() with a
 * list consisting of the one statement.  One tiny difference is that
 * we do not bother to save the entry value of estate->err_stmt;
 * that's assumed to be NULL.
 * ----------
 */
static int
exec_toplevel_block(PLpgSQL_execstate *estate, PLpgSQL_stmt_block *block)
{
	int			rc;

	estate->err_stmt = (PLpgSQL_stmt *) block;

	/* Let the plugin know that we are about to execute this statement */
	if (*plpgsql_plugin_ptr && (*plpgsql_plugin_ptr)->stmt_beg)
		((*plpgsql_plugin_ptr)->stmt_beg) (estate, (PLpgSQL_stmt *) block);

	CHECK_FOR_INTERRUPTS();

	rc = exec_stmt_block(estate, block);

	/* Let the plugin know that we have finished executing this statement */
	if (*plpgsql_plugin_ptr && (*plpgsql_plugin_ptr)->stmt_end)
		((*plpgsql_plugin_ptr)->stmt_end) (estate, (PLpgSQL_stmt *) block);

	estate->err_stmt = NULL;

	return rc;
}


/* ----------
 * exec_stmt_block			Execute a block of statements
 * ----------
 */
static int
exec_stmt_block(PLpgSQL_execstate *estate, PLpgSQL_stmt_block *block)
{
	volatile int rc = -1;
	int			i;

	/*
	 * First initialize all variables declared in this block
	 */
	estate->err_text = gettext_noop("during statement block local variable initialization");

	for (i = 0; i < block->n_initvars; i++)
	{
		int			n = block->initvarnos[i];
		PLpgSQL_datum *datum = estate->datums[n];

		if (estate->check_pullup)
			continue;

		/*
		 * The set of dtypes handled here must match plpgsql_add_initdatums().
		 */
		switch (datum->dtype)
		{
			case PLPGSQL_DTYPE_VAR:
				{
					PLpgSQL_var *var = (PLpgSQL_var *) datum;

					/*
					 * Free any old value, in case re-entering block, and
					 * initialize to NULL
					 */
					assign_simple_var(estate, var, (Datum) 0, true, false);

					if (var->default_val == NULL)
					{
						/*
						 * If needed, give the datatype a chance to reject
						 * NULLs, by assigning a NULL to the variable. We
						 * claim the value is of type UNKNOWN, not the var's
						 * datatype, else coercion will be skipped. (Do this
						 * before the notnull check to be consistent with
						 * exec_assign_value.)
						 */
						if (var->datatype->typtype == TYPTYPE_DOMAIN)
							exec_assign_value(estate,
											  (PLpgSQL_datum *) var,
											  (Datum) 0,
											  true,
											  UNKNOWNOID,
											  -1);

						if (var->notnull)
							ereport(ERROR,
									(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
									 errmsg("variable \"%s\" declared NOT NULL cannot default to NULL",
											var->refname)));
					}
					else
					{
						exec_assign_expr(estate, (PLpgSQL_datum *) var,
										 var->default_val);
					}
				}
				break;

			case PLPGSQL_DTYPE_REC:
				{
					PLpgSQL_rec *rec = (PLpgSQL_rec *) datum;

					if (rec->erh)
						DeleteExpandedObject(ExpandedRecordGetDatum(rec->erh));
					rec->erh = NULL;
				}
				break;

			default:
				elog(ERROR, "unrecognized dtype: %d", datum->dtype);
		}
	}

	if (block->exceptions)
	{
		/*
		 * Execute the statements in the block's body inside a sub-transaction
		 */
		MemoryContext oldcontext = CurrentMemoryContext;
		ResourceOwner oldowner = CurrentResourceOwner;
		ExprContext *old_eval_econtext = estate->eval_econtext;
		ErrorData  *save_cur_error = estate->cur_error;
		MemoryContext stmt_mcontext;
		int			plpgsql_level;

		estate->err_text = gettext_noop("during statement block entry");

		/*
		 * We will need a stmt_mcontext to hold the error data if an error
		 * occurs.  It seems best to force it to exist before entering the
		 * subtransaction, so that we reduce the risk of out-of-memory during
		 * error recovery, and because this greatly simplifies restoring the
		 * stmt_mcontext stack to the correct state after an error.  We can
		 * ameliorate the cost of this by allowing the called statements to
		 * use this mcontext too; so we don't push it down here.
		 */
		stmt_mcontext = get_stmt_mcontext(estate);

		plpgsql_level = SetEnterPlpgsqlFunc();

		if (estate->check_pullup)
		{
			ListCell   *e;

			foreach(e, block->exceptions->exc_list)
			{
				PLpgSQL_exception *exception = (PLpgSQL_exception *) lfirst(e);
				exec_stmts(estate, exception->action);
			}
		}

		BeginInternalSubTransaction(NULL);
		/* Want to run statements inside function's memory context */
		MemoryContextSwitchTo(oldcontext);

		estate->handle_exceptions = false;

		PG_TRY();
		{
			/*
			 * We need to run the block's statements with a new eval_econtext
			 * that belongs to the current subtransaction; if we try to use
			 * the outer econtext then ExprContext shutdown callbacks will be
			 * called at the wrong times.
			 */
			plpgsql_create_econtext(estate);

			estate->err_text = NULL;

			/* Run the block's statements */
			rc = exec_stmts(estate, block->body);

			estate->err_text = gettext_noop("during statement block exit");

			/*
			 * If the block ended with RETURN, we may need to copy the return
			 * value out of the subtransaction eval_context.  We can avoid a
			 * physical copy if the value happens to be a R/W expanded object.
			 */
			if (rc == PLPGSQL_RC_RETURN &&
				!estate->retisset &&
				!estate->retisnull)
			{
				int16		resTypLen;
				bool		resTypByVal;

				get_typlenbyval(estate->rettype, &resTypLen, &resTypByVal);
				estate->retval = datumTransfer(estate->retval,
											   resTypByVal, resTypLen);
			}

			/* Commit the inner transaction, return to outer xact context */
			ReleaseCurrentSubTransaction();
			MemoryContextSwitchTo(oldcontext);
			CurrentResourceOwner = oldowner;

			/* Assert that the stmt_mcontext stack is unchanged */
			Assert(stmt_mcontext == estate->stmt_mcontext);

			/*
			 * Revert to outer eval_econtext.  (The inner one was
			 * automatically cleaned up during subxact exit.)
			 */
			estate->eval_econtext = old_eval_econtext;
		}
		PG_CATCH();
		{
			ErrorData  *edata;
			ListCell   *e;

			estate->err_text = gettext_noop("during exception cleanup");

			/* Save error info in our stmt_mcontext */
			MemoryContextSwitchTo(stmt_mcontext);
			edata = CopyErrorData();
			FlushErrorState();

			/* Abort the inner transaction */
			RollbackAndReleaseCurrentSubTransaction();
			MemoryContextSwitchTo(oldcontext);
			CurrentResourceOwner = oldowner;

			/*
			 * Set up the stmt_mcontext stack as though we had restored our
			 * previous state and then done push_stmt_mcontext().  The push is
			 * needed so that statements in the exception handler won't
			 * clobber the error data that's in our stmt_mcontext.
			 */
			estate->stmt_mcontext_parent = stmt_mcontext;
			estate->stmt_mcontext = NULL;

			/*
			 * Now we can delete any nested stmt_mcontexts that might have
			 * been created as children of ours.  (Note: we do not immediately
			 * release any statement-lifespan data that might have been left
			 * behind in stmt_mcontext itself.  We could attempt that by doing
			 * a MemoryContextReset on it before collecting the error data
			 * above, but it seems too risky to do any significant amount of
			 * work before collecting the error.)
			 */
			MemoryContextDeleteChildren(stmt_mcontext);

			/* Revert to outer eval_econtext */
			estate->eval_econtext = old_eval_econtext;

			/*
			 * Must clean up the econtext too.  However, any tuple table made
			 * in the subxact will have been thrown away by SPI during subxact
			 * abort, so we don't need to (and mustn't try to) free the
			 * eval_tuptable.
			 */
			estate->eval_tuptable = NULL;
			exec_eval_cleanup(estate);

			/* Mark handling exceptions */
			estate->handle_exceptions = true;

			/* Look for a matching exception handler */
			foreach(e, block->exceptions->exc_list)
			{
				PLpgSQL_exception *exception = (PLpgSQL_exception *) lfirst(e);

				if (exception_matches_conditions(edata, exception->conditions))
				{
					/*
					 * Initialize the magic SQLSTATE and SQLERRM variables for
					 * the exception block; this also frees values from any
					 * prior use of the same exception. We needn't do this
					 * until we have found a matching exception.
					 */
					PLpgSQL_var *state_var;
					PLpgSQL_var *errm_var;

					state_var = (PLpgSQL_var *)
						estate->datums[block->exceptions->sqlstate_varno];
					errm_var = (PLpgSQL_var *)
						estate->datums[block->exceptions->sqlerrm_varno];

					assign_text_var(estate, state_var,
									unpack_sql_state(edata->sqlerrcode));
					assign_text_var(estate, errm_var, edata->message);

					/*
					 * Also set up cur_error so the error data is accessible
					 * inside the handler.
					 */
					estate->cur_error = edata;

					estate->err_text = NULL;

					rc = exec_stmts(estate, exception->action);

					estate->handle_exceptions = false;

					break;
				}
			}

			/*
			 * Restore previous state of cur_error, whether or not we executed
			 * a handler.  This is needed in case an error got thrown from
			 * some inner block's exception handler.
			 */
			estate->cur_error = save_cur_error;

			/* If no match found, re-throw the error */
			if (e == NULL)
			{
				SetExitPlpgsqlFunc(plpgsql_level);
				ReThrowError(edata);
			}
			else
				FreeErrorData(edata);

			/* Restore stmt_mcontext stack and release the error data */
			pop_stmt_mcontext(estate);
			MemoryContextReset(stmt_mcontext);
		}
		PG_END_TRY();

		SetExitPlpgsqlFunc(plpgsql_level);
		Assert(save_cur_error == estate->cur_error);
	}
	else
	{
		/*
		 * Just execute the statements in the block's body
		 */
		estate->err_text = NULL;

		rc = exec_stmts(estate, block->body);
	}

	estate->err_text = NULL;

	/*
	 * Handle the return code.
	 */
	switch (rc)
	{
		case PLPGSQL_RC_OK:
		case PLPGSQL_RC_RETURN:
		case PLPGSQL_RC_CONTINUE:
			return rc;

		case PLPGSQL_RC_EXIT:

			/*
			 * This is intentionally different from the handling of RC_EXIT
			 * for loops: to match a block, we require a match by label.
			 */
			if (estate->exitlabel == NULL)
				return PLPGSQL_RC_EXIT;
			if (block->label == NULL)
				return PLPGSQL_RC_EXIT;
			if (strcmp(block->label, estate->exitlabel) != 0)
				return PLPGSQL_RC_EXIT;
			estate->exitlabel = NULL;
			return PLPGSQL_RC_OK;

		default:
			elog(ERROR, "unrecognized rc: %d", rc);
	}

	return PLPGSQL_RC_OK;
}


/* ----------
 * exec_stmts			Iterate over a list of statements
 *				as long as their return code is OK
 * ----------
 */
static int
exec_stmts(PLpgSQL_execstate *estate, List *stmts)
{
	PLpgSQL_stmt *save_estmt = estate->err_stmt;
	ListCell   *s;

	if (stmts == NIL)
	{
		/*
		 * Ensure we do a CHECK_FOR_INTERRUPTS() even though there is no
		 * statement.  This prevents hangup in a tight loop if, for instance,
		 * there is a LOOP construct with an empty body.
		 */
		CHECK_FOR_INTERRUPTS();
		return PLPGSQL_RC_OK;
	}

	foreach(s, stmts)
	{
		PLpgSQL_stmt *stmt = (PLpgSQL_stmt *) lfirst(s);
		int			rc;

		estate->err_stmt = stmt;

		/* Let the plugin know that we are about to execute this statement */
		if (*plpgsql_plugin_ptr && (*plpgsql_plugin_ptr)->stmt_beg)
			((*plpgsql_plugin_ptr)->stmt_beg) (estate, stmt);

		CHECK_FOR_INTERRUPTS();

		switch (stmt->cmd_type)
		{
			case PLPGSQL_STMT_BLOCK:
				rc = exec_stmt_block(estate, (PLpgSQL_stmt_block *) stmt);
				break;

			case PLPGSQL_STMT_ASSIGN:
				rc = exec_stmt_assign(estate, (PLpgSQL_stmt_assign *) stmt);
				break;

			case PLPGSQL_STMT_PERFORM:
				rc = exec_stmt_perform(estate, (PLpgSQL_stmt_perform *) stmt);
				break;

			case PLPGSQL_STMT_CALL:
				rc = exec_stmt_call(estate, (PLpgSQL_stmt_call *) stmt);
				break;

			case PLPGSQL_STMT_GETDIAG:
				rc = exec_stmt_getdiag(estate, (PLpgSQL_stmt_getdiag *) stmt);
				break;

			case PLPGSQL_STMT_IF:
				rc = exec_stmt_if(estate, (PLpgSQL_stmt_if *) stmt);
				break;

			case PLPGSQL_STMT_CASE:
				rc = exec_stmt_case(estate, (PLpgSQL_stmt_case *) stmt);
				break;

			case PLPGSQL_STMT_LOOP:
				rc = exec_stmt_loop(estate, (PLpgSQL_stmt_loop *) stmt);
				break;

			case PLPGSQL_STMT_WHILE:
				rc = exec_stmt_while(estate, (PLpgSQL_stmt_while *) stmt);
				break;

			case PLPGSQL_STMT_FORI:
				rc = exec_stmt_fori(estate, (PLpgSQL_stmt_fori *) stmt);
				break;

			case PLPGSQL_STMT_FORS:
				rc = exec_stmt_fors(estate, (PLpgSQL_stmt_fors *) stmt);
				break;

			case PLPGSQL_STMT_FORC:
				rc = exec_stmt_forc(estate, (PLpgSQL_stmt_forc *) stmt);
				break;

			case PLPGSQL_STMT_FOREACH_A:
				rc = exec_stmt_foreach_a(estate, (PLpgSQL_stmt_foreach_a *) stmt);
				break;

			case PLPGSQL_STMT_EXIT:
				rc = exec_stmt_exit(estate, (PLpgSQL_stmt_exit *) stmt);
				break;

			case PLPGSQL_STMT_RETURN:
				rc = exec_stmt_return(estate, (PLpgSQL_stmt_return *) stmt);
				break;

			case PLPGSQL_STMT_RETURN_NEXT:
				rc = exec_stmt_return_next(estate, (PLpgSQL_stmt_return_next *) stmt);
				break;

			case PLPGSQL_STMT_RETURN_QUERY:
				rc = exec_stmt_return_query(estate, (PLpgSQL_stmt_return_query *) stmt);
				break;

			case PLPGSQL_STMT_RAISE:
				rc = exec_stmt_raise(estate, (PLpgSQL_stmt_raise *) stmt);
				break;

			case PLPGSQL_STMT_ASSERT:
				rc = exec_stmt_assert(estate, (PLpgSQL_stmt_assert *) stmt);
				break;

			case PLPGSQL_STMT_EXECSQL:
				rc = exec_stmt_execsql(estate, (PLpgSQL_stmt_execsql *) stmt);
				break;

			case PLPGSQL_STMT_DYNEXECUTE:
				rc = exec_stmt_dynexecute(estate, (PLpgSQL_stmt_dynexecute *) stmt);
				break;

			case PLPGSQL_STMT_DYNFORS:
				rc = exec_stmt_dynfors(estate, (PLpgSQL_stmt_dynfors *) stmt);
				break;

			case PLPGSQL_STMT_OPEN:
				rc = exec_stmt_open(estate, (PLpgSQL_stmt_open *) stmt);
				break;

			case PLPGSQL_STMT_FETCH:
				rc = exec_stmt_fetch(estate, (PLpgSQL_stmt_fetch *) stmt);
				break;

			case PLPGSQL_STMT_CLOSE:
				rc = exec_stmt_close(estate, (PLpgSQL_stmt_close *) stmt);
				break;

			case PLPGSQL_STMT_COMMIT:
				rc = exec_stmt_commit(estate, (PLpgSQL_stmt_commit *) stmt);
				break;

			case PLPGSQL_STMT_ROLLBACK:
				rc = exec_stmt_rollback(estate, (PLpgSQL_stmt_rollback *) stmt);
				break;

			case PLPGSQL_STMT_SET:
				rc = exec_stmt_set(estate, (PLpgSQL_stmt_set *) stmt);
				break;

			default:
				/* point err_stmt to parent, since this one seems corrupt */
				estate->err_stmt = save_estmt;
				elog(ERROR, "unrecognized cmd_type: %d", stmt->cmd_type);
				rc = -1;		/* keep compiler quiet */
		}

		/* Let the plugin know that we have finished executing this statement */
		if (*plpgsql_plugin_ptr && (*plpgsql_plugin_ptr)->stmt_end)
			((*plpgsql_plugin_ptr)->stmt_end) (estate, stmt);

		if (rc != PLPGSQL_RC_OK)
		{
			estate->err_stmt = save_estmt;
			return rc;
		}
	}							/* end of loop over statements */

	estate->err_stmt = save_estmt;
	return PLPGSQL_RC_OK;
}


/* ----------
 * exec_stmt_assign			Evaluate an expression and
 *					put the result into a variable.
 * ----------
 */
static int
exec_stmt_assign(PLpgSQL_execstate *estate, PLpgSQL_stmt_assign *stmt)
{
	Assert(stmt->varno >= 0);

	exec_assign_expr(estate, estate->datums[stmt->varno], stmt->expr);

	return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt_perform		Evaluate query and discard result (but set
 *							FOUND depending on whether at least one row
 *							was returned).
 * ----------
 */
static int
exec_stmt_perform(PLpgSQL_execstate *estate, PLpgSQL_stmt_perform *stmt)
{
	PLpgSQL_expr *expr = stmt->expr;

	if (estate->check_pullup)
	{
		*estate->check_pullup = true;
		return PLPGSQL_RC_OK;
	}

	(void) exec_run_select(estate, expr, 0, NULL);
	exec_set_found(estate, (estate->eval_processed != 0));
	exec_eval_cleanup(estate);

	return PLPGSQL_RC_OK;
}

/*
 * exec_stmt_call
 */
static int
exec_stmt_call(PLpgSQL_execstate *estate, PLpgSQL_stmt_call *stmt)
{
	PLpgSQL_expr *expr = stmt->expr;
	LocalTransactionId before_lxid;
	LocalTransactionId after_lxid;
	bool		pushed_active_snap = false;
	ParamListInfo paramLI;
	SPIExecuteOptions options;
	int			rc;

	if (estate->check_pullup)
	{
		*estate->check_pullup = true;
		return PLPGSQL_RC_OK;
	}

	/*
	 * Make a plan if we don't have one already.
	 */
	if (expr->plan == NULL || estate->check_pullup)
	{
		exec_prepare_plan(estate, expr, 0);

		/*
		 * A CALL or DO can never be a simple expression.
		 */
		Assert(!expr->expr_simple_expr);

		/*
		 * Force target to be recalculated whenever the plan changes, in
		 * case the procedure's argument list has changed.
		 */
		stmt->target = NULL;
	}

	/*
	 * We construct a DTYPE_ROW datum representing the plpgsql variables
	 * associated with the procedure's output arguments.  Then we can use
	 * exec_move_row() to do the assignments.
	 */
	if (stmt->is_call && stmt->target == NULL)
		stmt->target = make_callstmt_target(estate, expr);

	paramLI = setup_param_list(estate, expr);

	before_lxid = MyProc->lxid;

	/*
	 * The procedure call could end transactions, which would upset the
	 * snapshot management in SPI_execute*, so handle snapshots here instead.
	 * Set a snapshot only for non-read-only procedures, similar to SPI
	 * behavior.
	 */
	if (!estate->readonly_func)
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		pushed_active_snap = true;
	}

	/*
	 * If we have a procedure-lifespan resowner, use that to hold the refcount
	 * for the plan.  This avoids refcount leakage complaints if the called
	 * procedure ends the current transaction.
	 */
	memset(&options, 0, sizeof(options));
	options.params = paramLI;
	options.read_only = estate->readonly_func;
	options.no_snapshots = true;	/* disable SPI's snapshot management */
	options.owner = estate->procedure_resowner;

	rc = SPI_execute_plan_extended(expr->plan, &options);

	if (rc < 0)
		elog(ERROR, "SPI_execute_plan_extended failed executing query \"%s\": %s",
			 expr->query, SPI_result_code_string(rc));

    after_lxid = MyProc->lxid;

    if (before_lxid == after_lxid)
    {
        /*
         * If we are still in the same transaction after the call, pop the
         * snapshot that we might have pushed.  (If it's a new transaction,
         * then all the snapshots are gone already.)
         */
        if (pushed_active_snap)
            PopActiveSnapshot();
    }
    else
    {
        /*
         * If we are in a new transaction after the call, we need to reset
         * some internal state.
         */
        estate->simple_eval_estate = NULL;
		estate->simple_eval_resowner = NULL;
        plpgsql_create_econtext(estate);
    }

    /*
     * Check result rowcount; if there's one row, assign procedure's output
     * values back to the appropriate variables.
     */
	if (SPI_processed == 1)
	{
		SPITupleTable *tuptab = SPI_tuptable;

		if (!stmt->is_call)
			elog(ERROR, "DO statement returned a row");

		exec_move_row(estate, stmt->target, tuptab->vals[0], tuptab->tupdesc);
	}
	else if (SPI_processed > 1)
		elog(ERROR, "procedure call returned more than one row");

	exec_eval_cleanup(estate);
	SPI_freetuptable(SPI_tuptable);

	return PLPGSQL_RC_OK;
}

/*
 * We construct a DTYPE_ROW datum representing the plpgsql variables
 * associated with the procedure's output arguments.  Then we can use
 * exec_move_row() to do the assignments.
 */
static PLpgSQL_variable *
make_callstmt_target(PLpgSQL_execstate *estate, PLpgSQL_expr *expr)
{
	List	   *plansources;
	CachedPlanSource *plansource;
	Node	   *node;
	FuncExpr   *funcexpr;
	HeapTuple	func_tuple;
	List	   *funcargs;
	Oid		   *argtypes;
	char	  **argnames;
	char	   *argmodes;
	MemoryContext oldcontext;
	PLpgSQL_row *row;
	int			nfields;
	int			i;
	ListCell   *lc;

	/* Use eval_mcontext for any cruft accumulated here */
	oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));

	/*
	 * Get the parsed CallStmt, and look up the called procedure
	 */
	plansources = SPI_plan_get_plan_sources(expr->plan);
	if (list_length(plansources) != 1)
		elog(ERROR, "query for CALL statement is not a CallStmt");
	plansource = (CachedPlanSource *) linitial(plansources);
	if (list_length(plansource->query_list) != 1)
		elog(ERROR, "query for CALL statement is not a CallStmt");
	node = linitial_node(Query, plansource->query_list)->utilityStmt;
	if (node == NULL || !IsA(node, CallStmt))
		elog(ERROR, "query for CALL statement is not a CallStmt");

	funcexpr = ((CallStmt *) node)->funcexpr;

	func_tuple = SearchSysCache1(PROCOID,
								 ObjectIdGetDatum(funcexpr->funcid));
	if (!HeapTupleIsValid(func_tuple))
		elog(ERROR, "cache lookup failed for function %u",
			 funcexpr->funcid);

	/*
	 * Extract function arguments, and expand any named-arg notation
	 */
	funcargs = expand_function_arguments(funcexpr->args,
										 funcexpr->funcresulttype,
										 func_tuple);

	/*
	 * Get the argument names and modes, too
	 */
	get_func_arg_info(func_tuple, &argtypes, &argnames, &argmodes);

	ReleaseSysCache(func_tuple);

	/*
	 * Begin constructing row Datum; keep it in fn_cxt so it's adequately
	 * long-lived.
	 */
	MemoryContextSwitchTo(estate->func->fn_cxt);

	row = (PLpgSQL_row *) palloc0(sizeof(PLpgSQL_row));
	row->dtype = PLPGSQL_DTYPE_ROW;
	row->refname = "(unnamed row)";
	row->lineno = -1;
	row->varnos = (int *) palloc(sizeof(int) * list_length(funcargs));

	MemoryContextSwitchTo(get_eval_mcontext(estate));

	/*
	 * Examine procedure's argument list.  Each output arg position should be
	 * an unadorned plpgsql variable (Datum), which we can insert into the row
	 * Datum.
	 */
	nfields = 0;
	i = 0;
	foreach(lc, funcargs)
	{
		Node	   *n = lfirst(lc);

		if (argmodes &&
			(argmodes[i] == PROARGMODE_INOUT ||
			 argmodes[i] == PROARGMODE_OUT))
		{
			if (IsA(n, Param))
			{
				Param	   *param = (Param *) n;

				/* paramid is offset by 1 (see make_datum_param()) */
				row->varnos[nfields++] = param->paramid - 1;
			}
			else
			{
				/* report error using parameter name, if available */
				if (argnames && argnames[i] && argnames[i][0])
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("procedure parameter \"%s\" is an output parameter but corresponding argument is not writable",
									argnames[i])));
				else
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("procedure parameter %d is an output parameter but corresponding argument is not writable",
									i + 1)));
			}
		}
		i++;
	}

	row->nfields = nfields;

	MemoryContextSwitchTo(oldcontext);

	return (PLpgSQL_variable *) row;
}

/* ----------
 * exec_stmt_getdiag					Put internal PG information into
 *										specified variables.
 * ----------
 */
static int
exec_stmt_getdiag(PLpgSQL_execstate *estate, PLpgSQL_stmt_getdiag *stmt)
{
	ListCell   *lc;

	/*
	 * GET STACKED DIAGNOSTICS is only valid inside an exception handler.
	 *
	 * Note: we trust the grammar to have disallowed the relevant item kinds
	 * if not is_stacked, otherwise we'd dump core below.
	 */
	if (stmt->is_stacked && estate->cur_error == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER),
				 errmsg("GET STACKED DIAGNOSTICS cannot be used outside an exception handler")));

	foreach(lc, stmt->diag_items)
	{
		PLpgSQL_diag_item *diag_item = (PLpgSQL_diag_item *) lfirst(lc);
		PLpgSQL_datum *var = estate->datums[diag_item->target];

		switch (diag_item->kind)
		{
			case PLPGSQL_GETDIAG_ROW_COUNT:
				exec_assign_value(estate, var,
								  UInt64GetDatum(estate->eval_processed),
								  false, INT8OID, -1);
				break;

			case PLPGSQL_GETDIAG_RESULT_OID:
				exec_assign_value(estate, var,
								  ObjectIdGetDatum(estate->eval_lastoid),
								  false, OIDOID, -1);
				break;

			case PLPGSQL_GETDIAG_ERROR_CONTEXT:
				exec_assign_c_string(estate, var,
									 estate->cur_error->context);
				break;

			case PLPGSQL_GETDIAG_ERROR_DETAIL:
				exec_assign_c_string(estate, var,
									 estate->cur_error->detail);
				break;

			case PLPGSQL_GETDIAG_ERROR_HINT:
				exec_assign_c_string(estate, var,
									 estate->cur_error->hint);
				break;

			case PLPGSQL_GETDIAG_RETURNED_SQLSTATE:
				exec_assign_c_string(estate, var,
									 unpack_sql_state(estate->cur_error->sqlerrcode));
				break;

			case PLPGSQL_GETDIAG_COLUMN_NAME:
				exec_assign_c_string(estate, var,
									 estate->cur_error->column_name);
				break;

			case PLPGSQL_GETDIAG_CONSTRAINT_NAME:
				exec_assign_c_string(estate, var,
									 estate->cur_error->constraint_name);
				break;

			case PLPGSQL_GETDIAG_DATATYPE_NAME:
				exec_assign_c_string(estate, var,
									 estate->cur_error->datatype_name);
				break;

			case PLPGSQL_GETDIAG_MESSAGE_TEXT:
				exec_assign_c_string(estate, var,
									 estate->cur_error->message);
				break;

			case PLPGSQL_GETDIAG_TABLE_NAME:
				exec_assign_c_string(estate, var,
									 estate->cur_error->table_name);
				break;

			case PLPGSQL_GETDIAG_SCHEMA_NAME:
				exec_assign_c_string(estate, var,
									 estate->cur_error->schema_name);
				break;

			case PLPGSQL_GETDIAG_CONTEXT:
				{
					char	   *contextstackstr;
					MemoryContext oldcontext;

					/* Use eval_mcontext for short-lived string */
					oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
					contextstackstr = GetErrorContextStack();
					MemoryContextSwitchTo(oldcontext);

					exec_assign_c_string(estate, var, contextstackstr);
				}
				break;

			default:
				elog(ERROR, "unrecognized diagnostic item kind: %d",
					 diag_item->kind);
		}
	}

	exec_eval_cleanup(estate);

	return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt_if				Evaluate a bool expression and
 *					execute the true or false body
 *					conditionally.
 * ----------
 */
static int
exec_stmt_if(PLpgSQL_execstate *estate, PLpgSQL_stmt_if *stmt)
{
	bool		value;
	bool		isnull;
	ListCell   *lc;

	if (estate->check_pullup)
	{
		exec_stmts(estate, stmt->then_body);
		foreach(lc, stmt->elsif_list)
		{
			PLpgSQL_if_elsif *elif = (PLpgSQL_if_elsif *) lfirst(lc);
			exec_stmts(estate, elif->stmts);
		}
		exec_stmts(estate, stmt->else_body);
		return PLPGSQL_RC_OK;
	}

	value = exec_eval_boolean(estate, stmt->cond, &isnull);
	exec_eval_cleanup(estate);
	if (!isnull && value)
		return exec_stmts(estate, stmt->then_body);

	foreach(lc, stmt->elsif_list)
	{
		PLpgSQL_if_elsif *elif = (PLpgSQL_if_elsif *) lfirst(lc);

		value = exec_eval_boolean(estate, elif->cond, &isnull);
		exec_eval_cleanup(estate);
		if (!isnull && value)
			return exec_stmts(estate, elif->stmts);
	}

	return exec_stmts(estate, stmt->else_body);
}


/*-----------
 * exec_stmt_case
 *-----------
 */
static int
exec_stmt_case(PLpgSQL_execstate *estate, PLpgSQL_stmt_case *stmt)
{
	PLpgSQL_var *t_var = NULL;
	bool		isnull;
	ListCell   *l;

	if (estate->check_pullup)
	{
		if (stmt->t_expr != NULL)
		{
			exec_prepare_plan(estate, stmt->t_expr, CURSOR_OPT_PARALLEL_OK);
		}
		foreach(l, stmt->case_when_list)
		{
			PLpgSQL_case_when *cwt = (PLpgSQL_case_when *) lfirst(l);
			exec_stmts(estate, cwt->stmts);
		}
		exec_stmts(estate, stmt->else_stmts);

		return PLPGSQL_RC_OK;
	}

	if (stmt->t_expr != NULL)
	{
		/* simple case */
		Datum		t_val;
		Oid			t_typoid;
		int32		t_typmod;

		t_val = exec_eval_expr(estate, stmt->t_expr,
							   &isnull, &t_typoid, &t_typmod);

		t_var = (PLpgSQL_var *) estate->datums[stmt->t_varno];

		/*
		 * When expected datatype is different from real, change it. Note that
		 * what we're modifying here is an execution copy of the datum, so
		 * this doesn't affect the originally stored function parse tree. (In
		 * theory, if the expression datatype keeps changing during execution,
		 * this could cause a function-lifespan memory leak.  Doesn't seem
		 * worth worrying about though.)
		 */
		if (t_var->datatype->typoid != t_typoid ||
			t_var->datatype->atttypmod != t_typmod)
			t_var->datatype = plpgsql_build_datatype(t_typoid,
													 t_typmod,
													 estate->func->fn_input_collation);

		/* now we can assign to the variable */
		exec_assign_value(estate,
						  (PLpgSQL_datum *) t_var,
						  t_val,
						  isnull,
						  t_typoid,
						  t_typmod);

		exec_eval_cleanup(estate);
	}

	/* Now search for a successful WHEN clause */
	foreach(l, stmt->case_when_list)
	{
		PLpgSQL_case_when *cwt = (PLpgSQL_case_when *) lfirst(l);
		bool		value;

		value = exec_eval_boolean(estate, cwt->expr, &isnull);
		exec_eval_cleanup(estate);
		if (!isnull && value)
		{
			/* Found it */

			/* We can now discard any value we had for the temp variable */
			if (t_var != NULL)
				assign_simple_var(estate, t_var, (Datum) 0, true, false);

			/* Evaluate the statement(s), and we're done */
			return exec_stmts(estate, cwt->stmts);
		}
	}

	/* We can now discard any value we had for the temp variable */
	if (t_var != NULL)
		assign_simple_var(estate, t_var, (Datum) 0, true, false);

	/* SQL2003 mandates this error if there was no ELSE clause */
	if (!stmt->have_else)
		ereport(ERROR,
				(errcode(ERRCODE_CASE_NOT_FOUND),
				 errmsg("case not found"),
				 errhint("CASE statement is missing ELSE part.")));

	/* Evaluate the ELSE statements, and we're done */
	return exec_stmts(estate, stmt->else_stmts);
}


/* ----------
 * exec_stmt_loop			Loop over statements until
 *					an exit occurs.
 * ----------
 */
static int
exec_stmt_loop(PLpgSQL_execstate *estate, PLpgSQL_stmt_loop *stmt)
{
	if (estate->check_pullup)
	{
		exec_stmts(estate, stmt->body);
		return PLPGSQL_RC_OK;
	}

	for (;;)
	{
		int			rc = exec_stmts(estate, stmt->body);

		switch (rc)
		{
			case PLPGSQL_RC_OK:
				break;

			case PLPGSQL_RC_EXIT:
				if (estate->exitlabel == NULL)
					return PLPGSQL_RC_OK;
				if (stmt->label == NULL)
					return PLPGSQL_RC_EXIT;
				if (strcmp(stmt->label, estate->exitlabel) != 0)
					return PLPGSQL_RC_EXIT;
				estate->exitlabel = NULL;
				return PLPGSQL_RC_OK;

			case PLPGSQL_RC_CONTINUE:
				if (estate->exitlabel == NULL)
					/* anonymous continue, so re-run the loop */
					break;
				else if (stmt->label != NULL &&
						 strcmp(stmt->label, estate->exitlabel) == 0)
					/* label matches named continue, so re-run loop */
					estate->exitlabel = NULL;
				else
					/* label doesn't match named continue, so propagate upward */
					return PLPGSQL_RC_CONTINUE;
				break;

			case PLPGSQL_RC_RETURN:
				return rc;

			default:
				elog(ERROR, "unrecognized rc: %d", rc);
		}
	}
}


/* ----------
 * exec_stmt_while			Loop over statements as long
 *					as an expression evaluates to
 *					true or an exit occurs.
 * ----------
 */
static int
exec_stmt_while(PLpgSQL_execstate *estate, PLpgSQL_stmt_while *stmt)
{
	if (estate->check_pullup)
	{
		exec_stmts(estate, stmt->body);
		return PLPGSQL_RC_OK;
	}

	for (;;)
	{
		int			rc;
		bool		value;
		bool		isnull;

		value = exec_eval_boolean(estate, stmt->cond, &isnull);
		exec_eval_cleanup(estate);

		if (isnull || !value)
			break;

		rc = exec_stmts(estate, stmt->body);

		switch (rc)
		{
			case PLPGSQL_RC_OK:
				break;

			case PLPGSQL_RC_EXIT:
				if (estate->exitlabel == NULL)
					return PLPGSQL_RC_OK;
				if (stmt->label == NULL)
					return PLPGSQL_RC_EXIT;
				if (strcmp(stmt->label, estate->exitlabel) != 0)
					return PLPGSQL_RC_EXIT;
				estate->exitlabel = NULL;
				return PLPGSQL_RC_OK;

			case PLPGSQL_RC_CONTINUE:
				if (estate->exitlabel == NULL)
					/* anonymous continue, so re-run loop */
					break;
				else if (stmt->label != NULL &&
						 strcmp(stmt->label, estate->exitlabel) == 0)
					/* label matches named continue, so re-run loop */
					estate->exitlabel = NULL;
				else
					/* label doesn't match named continue, propagate upward */
					return PLPGSQL_RC_CONTINUE;
				break;

			case PLPGSQL_RC_RETURN:
				return rc;

			default:
				elog(ERROR, "unrecognized rc: %d", rc);
		}
	}

	return PLPGSQL_RC_OK;
}


/* ----------
 * exec_stmt_fori			Iterate an integer variable
 *					from a lower to an upper value
 *					incrementing or decrementing by the BY value
 * ----------
 */
static int
exec_stmt_fori(PLpgSQL_execstate *estate, PLpgSQL_stmt_fori *stmt)
{
	PLpgSQL_var *var;
	Datum		value;
	bool		isnull;
	Oid			valtype;
	int32		valtypmod;
	int32		loop_value;
	int32		end_value;
	int32		step_value;
	bool		found = false;
	int			rc = PLPGSQL_RC_OK;

	if (estate->check_pullup)
	{
		return exec_stmts(estate, stmt->body);
	}

	var = (PLpgSQL_var *) (estate->datums[stmt->var->dno]);

	/*
	 * Get the value of the lower bound
	 */
	value = exec_eval_expr(estate, stmt->lower,
						   &isnull, &valtype, &valtypmod);
	value = exec_cast_value(estate, value, &isnull,
							valtype, valtypmod,
							var->datatype->typoid,
							var->datatype->atttypmod);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("lower bound of FOR loop cannot be null")));
	loop_value = DatumGetInt32(value);
	exec_eval_cleanup(estate);

	/*
	 * Get the value of the upper bound
	 */
	value = exec_eval_expr(estate, stmt->upper,
						   &isnull, &valtype, &valtypmod);
	value = exec_cast_value(estate, value, &isnull,
							valtype, valtypmod,
							var->datatype->typoid,
							var->datatype->atttypmod);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("upper bound of FOR loop cannot be null")));
	end_value = DatumGetInt32(value);
	exec_eval_cleanup(estate);

	/*
	 * Get the step value
	 */
	if (stmt->step)
	{
		value = exec_eval_expr(estate, stmt->step,
							   &isnull, &valtype, &valtypmod);
		value = exec_cast_value(estate, value, &isnull,
								valtype, valtypmod,
								var->datatype->typoid,
								var->datatype->atttypmod);
		if (isnull)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("BY value of FOR loop cannot be null")));
		step_value = DatumGetInt32(value);
		exec_eval_cleanup(estate);
		if (step_value <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("BY value of FOR loop must be greater than zero")));
	}
	else
		step_value = 1;

	/*
	 * Now do the loop
	 */
	for (;;)
	{
		/*
		 * Check against upper bound
		 */
		if (stmt->reverse)
		{
			if (loop_value < end_value)
				break;
		}
		else
		{
			if (loop_value > end_value)
				break;
		}

		found = true;			/* looped at least once */

		/*
		 * Assign current value to loop var
		 */
		assign_simple_var(estate, var, Int32GetDatum(loop_value), false, false);

		/*
		 * Execute the statements
		 */
		rc = exec_stmts(estate, stmt->body);

		if (rc == PLPGSQL_RC_RETURN)
			break;				/* break out of the loop */
		else if (rc == PLPGSQL_RC_EXIT)
		{
			if (estate->exitlabel == NULL)
				/* unlabelled exit, finish the current loop */
				rc = PLPGSQL_RC_OK;
			else if (stmt->label != NULL &&
					 strcmp(stmt->label, estate->exitlabel) == 0)
			{
				/* labelled exit, matches the current stmt's label */
				estate->exitlabel = NULL;
				rc = PLPGSQL_RC_OK;
			}

			/*
			 * otherwise, this is a labelled exit that does not match the
			 * current statement's label, if any: return RC_EXIT so that the
			 * EXIT continues to propagate up the stack.
			 */
			break;
		}
		else if (rc == PLPGSQL_RC_CONTINUE)
		{
			if (estate->exitlabel == NULL)
				/* unlabelled continue, so re-run the current loop */
				rc = PLPGSQL_RC_OK;
			else if (stmt->label != NULL &&
					 strcmp(stmt->label, estate->exitlabel) == 0)
			{
				/* label matches named continue, so re-run loop */
				estate->exitlabel = NULL;
				rc = PLPGSQL_RC_OK;
			}
			else
			{
				/*
				 * otherwise, this is a named continue that does not match the
				 * current statement's label, if any: return RC_CONTINUE so
				 * that the CONTINUE will propagate up the stack.
				 */
				break;
			}
		}

		/*
		 * Increase/decrease loop value, unless it would overflow, in which
		 * case exit the loop.
		 */
		if (stmt->reverse)
		{
			if (loop_value < (PG_INT32_MIN + step_value))
				break;
			loop_value -= step_value;
		}
		else
		{
			if (loop_value > (PG_INT32_MAX - step_value))
				break;
			loop_value += step_value;
		}
	}

	/*
	 * Set the FOUND variable to indicate the result of executing the loop
	 * (namely, whether we looped one or more times). This must be set here so
	 * that it does not interfere with the value of the FOUND variable inside
	 * the loop processing itself.
	 */
	exec_set_found(estate, found);

	return rc;
}


/* ----------
 * exec_stmt_fors			Execute a query, assign each
 *					tuple to a record or row and
 *					execute a group of statements
 *					for it.
 * ----------
 */
static int
exec_stmt_fors(PLpgSQL_execstate *estate, PLpgSQL_stmt_fors *stmt)
{
	Portal		portal;
	int			rc;

	/*
	 * Open the implicit cursor for the statement using exec_run_select
	 */
	exec_run_select(estate, stmt->query, 0, &portal);

	if (estate->check_pullup)
		return PLPGSQL_RC_OK;

	/*
	 * Execute the loop
	 */
	rc = exec_for_query(estate, (PLpgSQL_stmt_forq *) stmt, portal, true);

	/*
	 * Close the implicit cursor
	 */
	SPI_cursor_close(portal);

	return rc;
}


/* ----------
 * exec_stmt_forc			Execute a loop for each row from a cursor.
 * ----------
 */
static int
exec_stmt_forc(PLpgSQL_execstate *estate, PLpgSQL_stmt_forc *stmt)
{
	PLpgSQL_var *curvar;
	MemoryContext stmt_mcontext = NULL;
	char	   *curname = NULL;
	PLpgSQL_expr *query;
	ParamListInfo paramLI;
	Portal		portal;
	int			rc;

	/* ----------
	 * Get the cursor variable and if it has an assigned name, check
	 * that it's not in use currently.
	 * ----------
	 */
	curvar = (PLpgSQL_var *) (estate->datums[stmt->curvar]);
	if (!curvar->isnull)
	{
		MemoryContext oldcontext;

		/* We only need stmt_mcontext to hold the cursor name string */
		stmt_mcontext = get_stmt_mcontext(estate);
		oldcontext = MemoryContextSwitchTo(stmt_mcontext);
		curname = TextDatumGetCString(curvar->value);
		MemoryContextSwitchTo(oldcontext);

		if (SPI_cursor_find(curname) != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_CURSOR),
					 errmsg("cursor \"%s\" already in use", curname)));
	}

	/* ----------
	 * Open the cursor just like an OPEN command
	 *
	 * Note: parser should already have checked that statement supplies
	 * args iff cursor needs them, but we check again to be safe.
	 * ----------
	 */
	if (stmt->argquery != NULL)
	{
		/* ----------
		 * OPEN CURSOR with args.  We fake a SELECT ... INTO ...
		 * statement to evaluate the args and put 'em into the
		 * internal row.
		 * ----------
		 */
		PLpgSQL_stmt_execsql set_args;

		if (curvar->cursor_explicit_argrow < 0)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("arguments given for cursor without arguments")));

		memset(&set_args, 0, sizeof(set_args));
		set_args.cmd_type = PLPGSQL_STMT_EXECSQL;
		set_args.lineno = stmt->lineno;
		set_args.sqlstmt = stmt->argquery;
		set_args.into = true;
		/* XXX historically this has not been STRICT */
		set_args.target = (PLpgSQL_variable *)
			(estate->datums[curvar->cursor_explicit_argrow]);

		if (exec_stmt_execsql(estate, &set_args) != PLPGSQL_RC_OK)
			elog(ERROR, "open cursor failed during argument processing");
	}
	else
	{
		if (curvar->cursor_explicit_argrow >= 0)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("arguments required for cursor")));
	}

	query = curvar->cursor_explicit_expr;
	Assert(query);

	if (query->plan == NULL || estate->check_pullup)
		exec_prepare_plan(estate, query, curvar->cursor_options);

	if (estate->check_pullup)
		return PLPGSQL_RC_OK;

	/*
	 * Set up ParamListInfo for this query
	 */
	paramLI = setup_param_list(estate, query);

	/*
	 * Open the cursor (the paramlist will get copied into the portal)
	 */
	portal = SPI_cursor_open_with_paramlist(curname, query->plan,
											paramLI,
											estate->readonly_func);
	if (portal == NULL)
		elog(ERROR, "could not open cursor: %s",
			 SPI_result_code_string(SPI_result));

	/*
	 * If cursor variable was NULL, store the generated portal name in it
	 */
	if (curname == NULL)
		assign_text_var(estate, curvar, portal->name);

	/*
	 * Clean up before entering exec_for_query
	 */
	exec_eval_cleanup(estate);
	if (stmt_mcontext)
		MemoryContextReset(stmt_mcontext);

	/*
	 * Execute the loop.  We can't prefetch because the cursor is accessible
	 * to the user, for instance via UPDATE WHERE CURRENT OF within the loop.
	 */
	rc = exec_for_query(estate, (PLpgSQL_stmt_forq *) stmt, portal, false);

	/* ----------
	 * Close portal, and restore cursor variable if it was initially NULL.
	 * ----------
	 */
	SPI_cursor_close(portal);

	if (curname == NULL)
		assign_simple_var(estate, curvar, (Datum) 0, true, false);

	return rc;
}


/* ----------
 * exec_stmt_foreach_a			Loop over elements or slices of an array
 *
 * When looping over elements, the loop variable is the same type that the
 * array stores (eg: integer), when looping through slices, the loop variable
 * is an array of size and dimensions to match the size of the slice.
 * ----------
 */
static int
exec_stmt_foreach_a(PLpgSQL_execstate *estate, PLpgSQL_stmt_foreach_a *stmt)
{
	ArrayType  *arr;
	Oid			arrtype;
	int32		arrtypmod;
	PLpgSQL_datum *loop_var;
	Oid			loop_var_elem_type;
	bool		found = false;
	int			rc = PLPGSQL_RC_OK;
	MemoryContext stmt_mcontext;
	MemoryContext oldcontext;
	ArrayIterator array_iterator;
	Oid			iterator_result_type;
	int32		iterator_result_typmod;
	Datum		value;
	bool		isnull;

	/* get the value of the array expression */
	value = exec_eval_expr(estate, stmt->expr, &isnull, &arrtype, &arrtypmod);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("FOREACH expression must not be null")));

	/*
	 * Do as much as possible of the code below in stmt_mcontext, to avoid any
	 * leaks from called subroutines.  We need a private stmt_mcontext since
	 * we'll be calling arbitrary statement code.
	 */
	stmt_mcontext = get_stmt_mcontext(estate);
	push_stmt_mcontext(estate);
	oldcontext = MemoryContextSwitchTo(stmt_mcontext);

	/* check the type of the expression - must be an array */
	if (!OidIsValid(get_element_type(arrtype)))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("FOREACH expression must yield an array, not type %s",
						format_type_be(arrtype))));

	/*
	 * We must copy the array into stmt_mcontext, else it will disappear in
	 * exec_eval_cleanup.  This is annoying, but cleanup will certainly happen
	 * while running the loop body, so we have little choice.
	 */
	arr = DatumGetArrayTypePCopy(value);

	/* Clean up any leftover temporary memory */
	exec_eval_cleanup(estate);

	/* Slice dimension must be less than or equal to array dimension */
	if (stmt->slice < 0 || stmt->slice > ARR_NDIM(arr))
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("slice dimension (%d) is out of the valid range 0..%d",
						stmt->slice, ARR_NDIM(arr))));

	/* Set up the loop variable and see if it is of an array type */
	loop_var = estate->datums[stmt->varno];
	if (loop_var->dtype == PLPGSQL_DTYPE_REC ||
		loop_var->dtype == PLPGSQL_DTYPE_ROW)
	{
		/*
		 * Record/row variable is certainly not of array type, and might not
		 * be initialized at all yet, so don't try to get its type
		 */
		loop_var_elem_type = InvalidOid;
	}
	else
		loop_var_elem_type = get_element_type(plpgsql_exec_get_datum_type(estate,
																		  loop_var));

	/*
	 * Sanity-check the loop variable type.  We don't try very hard here, and
	 * should not be too picky since it's possible that exec_assign_value can
	 * coerce values of different types.  But it seems worthwhile to complain
	 * if the array-ness of the loop variable is not right.
	 */
	if (stmt->slice > 0 && loop_var_elem_type == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("FOREACH ... SLICE loop variable must be of an array type")));
	if (stmt->slice == 0 && loop_var_elem_type != InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("FOREACH loop variable must not be of an array type")));

	/* Create an iterator to step through the array */
	array_iterator = array_create_iterator(arr, stmt->slice, NULL);

	/* Identify iterator result type */
	if (stmt->slice > 0)
	{
		/* When slicing, nominal type of result is same as array type */
		iterator_result_type = arrtype;
		iterator_result_typmod = arrtypmod;
	}
	else
	{
		/* Without slicing, results are individual array elements */
		iterator_result_type = ARR_ELEMTYPE(arr);
		iterator_result_typmod = arrtypmod;
	}

	/* Iterate over the array elements or slices */
	while (array_iterate(array_iterator, &value, &isnull))
	{
		found = true;			/* looped at least once */

		/* exec_assign_value and exec_stmts must run in the main context */
		MemoryContextSwitchTo(oldcontext);

		/* Assign current element/slice to the loop variable */
		exec_assign_value(estate, loop_var, value, isnull,
						  iterator_result_type, iterator_result_typmod);

		/* In slice case, value is temporary; must free it to avoid leakage */
		if (stmt->slice > 0)
			pfree(DatumGetPointer(value));

		/*
		 * Execute the statements
		 */
		rc = exec_stmts(estate, stmt->body);

		/* Handle the return code */
		if (rc == PLPGSQL_RC_RETURN)
			break;				/* break out of the loop */
		else if (rc == PLPGSQL_RC_EXIT)
		{
			if (estate->exitlabel == NULL)
				/* unlabelled exit, finish the current loop */
				rc = PLPGSQL_RC_OK;
			else if (stmt->label != NULL &&
					 strcmp(stmt->label, estate->exitlabel) == 0)
			{
				/* labelled exit, matches the current stmt's label */
				estate->exitlabel = NULL;
				rc = PLPGSQL_RC_OK;
			}

			/*
			 * otherwise, this is a labelled exit that does not match the
			 * current statement's label, if any: return RC_EXIT so that the
			 * EXIT continues to propagate up the stack.
			 */
			break;
		}
		else if (rc == PLPGSQL_RC_CONTINUE)
		{
			if (estate->exitlabel == NULL)
				/* unlabelled continue, so re-run the current loop */
				rc = PLPGSQL_RC_OK;
			else if (stmt->label != NULL &&
					 strcmp(stmt->label, estate->exitlabel) == 0)
			{
				/* label matches named continue, so re-run loop */
				estate->exitlabel = NULL;
				rc = PLPGSQL_RC_OK;
			}
			else
			{
				/*
				 * otherwise, this is a named continue that does not match the
				 * current statement's label, if any: return RC_CONTINUE so
				 * that the CONTINUE will propagate up the stack.
				 */
				break;
			}
		}

		MemoryContextSwitchTo(stmt_mcontext);
	}

	/* Restore memory context state */
	MemoryContextSwitchTo(oldcontext);
	pop_stmt_mcontext(estate);

	/* Release temporary memory, including the array value */
	MemoryContextReset(stmt_mcontext);

	/*
	 * Set the FOUND variable to indicate the result of executing the loop
	 * (namely, whether we looped one or more times). This must be set here so
	 * that it does not interfere with the value of the FOUND variable inside
	 * the loop processing itself.
	 */
	exec_set_found(estate, found);

	return rc;
}


/* ----------
 * exec_stmt_exit			Implements EXIT and CONTINUE
 *
 * This begins the process of exiting / restarting a loop.
 * ----------
 */
static int
exec_stmt_exit(PLpgSQL_execstate *estate, PLpgSQL_stmt_exit *stmt)
{
	if (estate->check_pullup)
		return PLPGSQL_RC_CONTINUE;

	/*
	 * If the exit / continue has a condition, evaluate it
	 */
	if (stmt->cond != NULL)
	{
		bool		value;
		bool		isnull;

		value = exec_eval_boolean(estate, stmt->cond, &isnull);
		exec_eval_cleanup(estate);
		if (isnull || value == false)
			return PLPGSQL_RC_OK;
	}

	estate->exitlabel = stmt->label;
	if (stmt->is_exit)
		return PLPGSQL_RC_EXIT;
	else
		return PLPGSQL_RC_CONTINUE;
}


/* ----------
 * exec_stmt_return			Evaluate an expression and start
 *					returning from the function.
 *
 * Note: The result may be in the eval_mcontext.  Therefore, we must not
 * do exec_eval_cleanup while unwinding the control stack.
 * ----------
 */
static int
exec_stmt_return(PLpgSQL_execstate *estate, PLpgSQL_stmt_return *stmt)
{
	/*
	 * If processing a set-returning PL/pgSQL function, the final RETURN
	 * indicates that the function is finished producing tuples.  The rest of
	 * the work will be done at the top level.
	 */
	if (estate->retisset)
		return PLPGSQL_RC_RETURN;

	/* initialize for null result */
	estate->retval = (Datum) 0;
	estate->retisnull = true;
	estate->rettype = InvalidOid;

	/*
	 * Special case path when the RETURN expression is a simple variable
	 * reference; in particular, this path is always taken in functions with
	 * one or more OUT parameters.
	 *
	 * This special case is especially efficient for returning variables that
	 * have R/W expanded values: we can put the R/W pointer directly into
	 * estate->retval, leading to transferring the value to the caller's
	 * context cheaply.  If we went through exec_eval_expr we'd end up with a
	 * R/O pointer.  It's okay to skip MakeExpandedObjectReadOnly here since
	 * we know we won't need the variable's value within the function anymore.
	 */
	if (stmt->retvarno >= 0)
	{
		PLpgSQL_datum *retvar = estate->datums[stmt->retvarno];

		switch (retvar->dtype)
		{
			case PLPGSQL_DTYPE_VAR:
				{
					PLpgSQL_var *var = (PLpgSQL_var *) retvar;

					estate->retval = var->value;
					estate->retisnull = var->isnull;
					estate->rettype = var->datatype->typoid;

					/*
					 * A PLpgSQL_var could not be of composite type, so
					 * conversion must fail if retistuple.  We throw a custom
					 * error mainly for consistency with historical behavior.
					 * For the same reason, we don't throw error if the result
					 * is NULL.  (Note that plpgsql_exec_trigger assumes that
					 * any non-null result has been verified to be composite.)
					 */
					if (estate->retistuple && !estate->retisnull)
						ereport(ERROR,
								(errcode(ERRCODE_DATATYPE_MISMATCH),
								 errmsg("cannot return non-composite value from function returning composite type")));
				}
				break;

			case PLPGSQL_DTYPE_REC:
				{
					PLpgSQL_rec *rec = (PLpgSQL_rec *) retvar;

					/* If record is empty, we return NULL not a row of nulls */
					if (rec->erh && !ExpandedRecordIsEmpty(rec->erh))
					{
						estate->retval = ExpandedRecordGetDatum(rec->erh);
						estate->retisnull = false;
						estate->rettype = rec->rectypeid;
					}
				}
				break;

			case PLPGSQL_DTYPE_ROW:
				{
					PLpgSQL_row *row = (PLpgSQL_row *) retvar;
					int32		rettypmod;

					/* We get here if there are multiple OUT parameters */
					exec_eval_datum(estate,
									(PLpgSQL_datum *) row,
									&estate->rettype,
									&rettypmod,
									&estate->retval,
									&estate->retisnull);
				}
				break;

			default:
				elog(ERROR, "unrecognized dtype: %d", retvar->dtype);
		}

		return PLPGSQL_RC_RETURN;
	}

	if (stmt->expr != NULL)
	{
		int32		rettypmod;

		estate->retval = exec_eval_expr(estate, stmt->expr,
										&(estate->retisnull),
										&(estate->rettype),
										&rettypmod);

		/*
		 * As in the DTYPE_VAR case above, throw a custom error if a non-null,
		 * non-composite value is returned in a function returning tuple.
		 */
		if (estate->retistuple && !estate->retisnull &&
			!type_is_rowtype(estate->rettype))
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("cannot return non-composite value from function returning composite type")));

		return PLPGSQL_RC_RETURN;
	}

	/*
	 * Special hack for function returning VOID: instead of NULL, return a
	 * non-null VOID value.  This is of dubious importance but is kept for
	 * backwards compatibility.  We don't do it for procedures, though.
	 */
	if (estate->fn_rettype == VOIDOID &&
		estate->func->fn_prokind != PROKIND_PROCEDURE)
	{
		estate->retval = (Datum) 0;
		estate->retisnull = false;
		estate->rettype = VOIDOID;
	}

	return PLPGSQL_RC_RETURN;
}

/* ----------
 * exec_stmt_return_next		Evaluate an expression and add it to the
 *								list of tuples returned by the current
 *								SRF.
 * ----------
 */
static int
exec_stmt_return_next(PLpgSQL_execstate *estate,
					  PLpgSQL_stmt_return_next *stmt)
{
	TupleDesc	tupdesc;
	int			natts;
	HeapTuple	tuple;
	MemoryContext oldcontext;

	if (!estate->retisset)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("cannot use RETURN NEXT in a non-SETOF function")));

	if (estate->tuple_store == NULL)
		exec_init_tuple_store(estate);

	/* tuple_store_desc will be filled by exec_init_tuple_store */
	tupdesc = estate->tuple_store_desc;
	natts = tupdesc->natts;

	/*
	 * Special case path when the RETURN NEXT expression is a simple variable
	 * reference; in particular, this path is always taken in functions with
	 * one or more OUT parameters.
	 *
	 * Unlike exec_statement_return, there's no special win here for R/W
	 * expanded values, since they'll have to get flattened to go into the
	 * tuplestore.  Indeed, we'd better make them R/O to avoid any risk of the
	 * casting step changing them in-place.
	 */
	if (stmt->retvarno >= 0)
	{
		PLpgSQL_datum *retvar = estate->datums[stmt->retvarno];

		switch (retvar->dtype)
		{
			case PLPGSQL_DTYPE_VAR:
				{
					PLpgSQL_var *var = (PLpgSQL_var *) retvar;
					Datum		retval = var->value;
					bool		isNull = var->isnull;
					Form_pg_attribute attr = TupleDescAttr(tupdesc, 0);

					if (natts != 1)
						ereport(ERROR,
								(errcode(ERRCODE_DATATYPE_MISMATCH),
								 errmsg("wrong result type supplied in RETURN NEXT")));

					/* let's be very paranoid about the cast step */
					retval = MakeExpandedObjectReadOnly(retval,
														isNull,
														var->datatype->typlen);

					/* coerce type if needed */
					retval = exec_cast_value(estate,
											 retval,
											 &isNull,
											 var->datatype->typoid,
											 var->datatype->atttypmod,
											 attr->atttypid,
											 attr->atttypmod);

					tuplestore_putvalues(estate->tuple_store, tupdesc,
										 &retval, &isNull);
				}
				break;

			case PLPGSQL_DTYPE_REC:
				{
					PLpgSQL_rec *rec = (PLpgSQL_rec *) retvar;
					TupleDesc	rec_tupdesc;
					TupleConversionMap *tupmap;

					/* If rec is null, try to convert it to a row of nulls */
					if (rec->erh == NULL)
						instantiate_empty_record_variable(estate, rec);
					if (ExpandedRecordIsEmpty(rec->erh))
						deconstruct_expanded_record(rec->erh);

					/* Use eval_mcontext for tuple conversion work */
					oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
					rec_tupdesc = expanded_record_get_tupdesc(rec->erh);
					tupmap = convert_tuples_by_position(rec_tupdesc,
														tupdesc,
														gettext_noop("wrong record type supplied in RETURN NEXT"));
					tuple = expanded_record_get_tuple(rec->erh);
					if (tupmap)
						tuple = execute_attr_map_tuple(tuple, tupmap, NULL);
					tuplestore_puttuple(estate->tuple_store, tuple);
					MemoryContextSwitchTo(oldcontext);
				}
				break;

			case PLPGSQL_DTYPE_ROW:
				{
					PLpgSQL_row *row = (PLpgSQL_row *) retvar;

					/* We get here if there are multiple OUT parameters */

					/* Use eval_mcontext for tuple conversion work */
					oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
					tuple = make_tuple_from_row(estate, row, tupdesc);
					if (tuple == NULL)	/* should not happen */
						ereport(ERROR,
								(errcode(ERRCODE_DATATYPE_MISMATCH),
								 errmsg("wrong record type supplied in RETURN NEXT")));
					tuplestore_puttuple(estate->tuple_store, tuple);
					MemoryContextSwitchTo(oldcontext);
				}
				break;

			default:
				elog(ERROR, "unrecognized dtype: %d", retvar->dtype);
				break;
		}
	}
	else if (stmt->expr)
	{
		Datum		retval;
		bool		isNull;
		Oid			rettype;
		int32		rettypmod;

		retval = exec_eval_expr(estate,
								stmt->expr,
								&isNull,
								&rettype,
								&rettypmod);

		if (estate->retistuple)
		{
			/* Expression should be of RECORD or composite type */
			if (!isNull)
			{
				HeapTupleData tmptup;
				TupleDesc	retvaldesc;
				TupleConversionMap *tupmap;

				if (!type_is_rowtype(rettype))
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("cannot return non-composite value from function returning composite type")));

				/* Use eval_mcontext for tuple conversion work */
				oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
				retvaldesc = deconstruct_composite_datum(retval, &tmptup);
				tuple = &tmptup;
				tupmap = convert_tuples_by_position(retvaldesc, tupdesc,
													gettext_noop("returned record type does not match expected record type"));
				if (tupmap)
					tuple = execute_attr_map_tuple(tuple, tupmap, NULL);
				tuplestore_puttuple(estate->tuple_store, tuple);
				ReleaseTupleDesc(retvaldesc);
				MemoryContextSwitchTo(oldcontext);
			}
			else
			{
				/* Composite NULL --- store a row of nulls */
				Datum	   *nulldatums;
				bool	   *nullflags;

				nulldatums = (Datum *)
					eval_mcontext_alloc0(estate, natts * sizeof(Datum));
				nullflags = (bool *)
					eval_mcontext_alloc(estate, natts * sizeof(bool));
				memset(nullflags, true, natts * sizeof(bool));
				tuplestore_putvalues(estate->tuple_store, tupdesc,
									 nulldatums, nullflags);
			}
		}
		else
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, 0);

			/* Simple scalar result */
			if (natts != 1)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("wrong result type supplied in RETURN NEXT")));

			/* coerce type if needed */
			retval = exec_cast_value(estate,
									 retval,
									 &isNull,
									 rettype,
									 rettypmod,
									 attr->atttypid,
									 attr->atttypmod);

			tuplestore_putvalues(estate->tuple_store, tupdesc,
								 &retval, &isNull);
		}
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("RETURN NEXT must have a parameter")));
	}

	exec_eval_cleanup(estate);

	return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt_return_query		Evaluate a query and add it to the
 *								list of tuples returned by the current
 *								SRF.
 * ----------
 */
static int
exec_stmt_return_query(PLpgSQL_execstate *estate,
					   PLpgSQL_stmt_return_query *stmt)
{
	int64		tcount;
	DestReceiver *treceiver;
	int			rc;
	uint64		processed;
	MemoryContext stmt_mcontext = get_stmt_mcontext(estate);
	MemoryContext oldcontext;

	if (!estate->retisset)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("cannot use RETURN QUERY in a non-SETOF function")));

	if (estate->tuple_store == NULL)
		exec_init_tuple_store(estate);
	/* There might be some tuples in the tuplestore already */
	tcount = tuplestore_tuple_count(estate->tuple_store);

	/*
	 * Set up DestReceiver to transfer results directly to tuplestore,
	 * converting rowtype if necessary.  DestReceiver lives in mcontext.
	 */
	oldcontext = MemoryContextSwitchTo(stmt_mcontext);
	treceiver = CreateDestReceiver(DestTuplestore);
	SetTuplestoreDestReceiverParams(treceiver,
									estate->tuple_store,
									estate->tuple_store_cxt,
									false,
									estate->tuple_store_desc,
									gettext_noop("structure of query does not match function result type"));
	MemoryContextSwitchTo(oldcontext);

	if (stmt->query != NULL)
	{
		/* static query */
		PLpgSQL_expr *expr = stmt->query;
		ParamListInfo paramLI;
		SPIExecuteOptions options;

		/*
		 * On the first call for this expression generate the plan.
		 */
		if (expr->plan == NULL)
			exec_prepare_plan(estate, expr, CURSOR_OPT_PARALLEL_OK);

		/*
		 * Set up ParamListInfo to pass to executor
		 */
		paramLI = setup_param_list(estate, expr);

		/*
		 * Execute the query
		 */
		memset(&options, 0, sizeof(options));
		options.params = paramLI;
		options.read_only = estate->readonly_func;
		options.dest = treceiver;

		rc = SPI_execute_plan_extended(expr->plan, &options);
		if (rc != SPI_OK_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("query \"%s\" is not a SELECT", expr->query)));
	}
	else
	{
		/* RETURN QUERY EXECUTE */
		Datum		query;
		bool		isnull;
		Oid			restype;
		int32		restypmod;
		char	   *querystr;

		/*
		 * Evaluate the string expression after the EXECUTE keyword. Its
		 * result is the querystring we have to execute.
		 */
		Assert(stmt->dynquery != NULL);
		query = exec_eval_expr(estate, stmt->dynquery,
							   &isnull, &restype, &restypmod);
		if (isnull)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("query string argument of EXECUTE is null")));

		/* Get the C-String representation */
		querystr = convert_value_to_string(estate, query, restype);

		/* copy it into the stmt_mcontext before we clean up */
		querystr = MemoryContextStrdup(stmt_mcontext, querystr);

		exec_eval_cleanup(estate);

		/* Execute query, passing params if necessary */
		rc = SPI_execute_with_receiver(querystr,
									   exec_eval_using_params(estate,
															  stmt->params),
									   estate->readonly_func,
									   0,
									   treceiver);
		if (rc < 0)
			elog(ERROR, "SPI_execute_with_receiver failed executing query \"%s\": %s",
				 querystr, SPI_result_code_string(rc));
	}

	/* Clean up */
	treceiver->rDestroy(treceiver);
	exec_eval_cleanup(estate);
	MemoryContextReset(stmt_mcontext);

	/* Count how many tuples we got */
	processed = tuplestore_tuple_count(estate->tuple_store) - tcount;

	estate->eval_processed = processed;
	exec_set_found(estate, processed != 0);

	return PLPGSQL_RC_OK;
}

static void
exec_init_tuple_store(PLpgSQL_execstate *estate)
{
	ReturnSetInfo *rsi = estate->rsi;
	MemoryContext oldcxt;
	ResourceOwner oldowner;

	/*
	 * Check caller can handle a set result in the way we want
	 */
	if (!rsi || !IsA(rsi, ReturnSetInfo) ||
		(rsi->allowedModes & SFRM_Materialize) == 0 ||
		rsi->expectedDesc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));

	/*
	 * Switch to the right memory context and resource owner for storing the
	 * tuplestore for return set. If we're within a subtransaction opened for
	 * an exception-block, for example, we must still create the tuplestore in
	 * the resource owner that was active when this function was entered, and
	 * not in the subtransaction resource owner.
	 */
	oldcxt = MemoryContextSwitchTo(estate->tuple_store_cxt);
	oldowner = CurrentResourceOwner;
	CurrentResourceOwner = estate->tuple_store_owner;

	estate->tuple_store =
		tuplestore_begin_heap(rsi->allowedModes & SFRM_Materialize_Random,
							  false, work_mem);

	CurrentResourceOwner = oldowner;
	MemoryContextSwitchTo(oldcxt);

	estate->tuple_store_desc = rsi->expectedDesc;
}

#define SET_RAISE_OPTION_TEXT(opt, name) \
do { \
	if (opt) \
		ereport(ERROR, \
				(errcode(ERRCODE_SYNTAX_ERROR), \
				 errmsg("RAISE option already specified: %s", \
						name))); \
	opt = MemoryContextStrdup(stmt_mcontext, extval); \
} while (0)

/* ----------
 * exec_stmt_raise			Build a message and throw it with elog()
 * ----------
 */
static int
exec_stmt_raise(PLpgSQL_execstate *estate, PLpgSQL_stmt_raise *stmt)
{
	int			err_code = 0;
	char	   *condname = NULL;
	char	   *err_message = NULL;
	char	   *err_detail = NULL;
	char	   *err_hint = NULL;
	char	   *err_column = NULL;
	char	   *err_constraint = NULL;
	char	   *err_datatype = NULL;
	char	   *err_table = NULL;
	char	   *err_schema = NULL;
	MemoryContext stmt_mcontext;
	ListCell   *lc;

	if (estate->check_pullup)
	{
		if (stmt->elog_level < ERROR)
			*estate->check_pullup = true;

		return PLPGSQL_RC_OK;
	}

	/* RAISE with no parameters: re-throw current exception */
	if (stmt->condname == NULL && stmt->message == NULL &&
		stmt->options == NIL)
	{
		if (estate->cur_error != NULL)
			ReThrowError(estate->cur_error);
		/* oops, we're not inside a handler */
		ereport(ERROR,
				(errcode(ERRCODE_STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER),
				 errmsg("RAISE without parameters cannot be used outside an exception handler")));
	}

	/* We'll need to accumulate the various strings in stmt_mcontext */
	stmt_mcontext = get_stmt_mcontext(estate);

	if (stmt->condname)
	{
		err_code = plpgsql_recognize_err_condition(stmt->condname, true);
		condname = MemoryContextStrdup(stmt_mcontext, stmt->condname);
	}

	if (stmt->message)
	{
		StringInfoData ds;
		ListCell   *current_param;
		char	   *cp;
		MemoryContext oldcontext;

		/* build string in stmt_mcontext */
		oldcontext = MemoryContextSwitchTo(stmt_mcontext);
		initStringInfo(&ds);
		MemoryContextSwitchTo(oldcontext);

		current_param = list_head(stmt->params);

		for (cp = stmt->message; *cp; cp++)
		{
			/*
			 * Occurrences of a single % are replaced by the next parameter's
			 * external representation. Double %'s are converted to one %.
			 */
			if (cp[0] == '%')
			{
				Oid			paramtypeid;
				int32		paramtypmod;
				Datum		paramvalue;
				bool		paramisnull;
				char	   *extval;

				if (cp[1] == '%')
				{
					appendStringInfoChar(&ds, '%');
					cp++;
					continue;
				}

				/* should have been checked at compile time */
				if (current_param == NULL)
					elog(ERROR, "unexpected RAISE parameter list length");

				paramvalue = exec_eval_expr(estate,
											(PLpgSQL_expr *) lfirst(current_param),
											&paramisnull,
											&paramtypeid,
											&paramtypmod);

				if (paramisnull)
					extval = "<NULL>";
				else
					extval = convert_value_to_string(estate,
													 paramvalue,
													 paramtypeid);
				appendStringInfoString(&ds, extval);
				current_param = lnext(current_param);
				exec_eval_cleanup(estate);
			}
			else
				appendStringInfoChar(&ds, cp[0]);
		}

		/* should have been checked at compile time */
		if (current_param != NULL)
			elog(ERROR, "unexpected RAISE parameter list length");

		err_message = ds.data;
	}

	foreach(lc, stmt->options)
	{
		PLpgSQL_raise_option *opt = (PLpgSQL_raise_option *) lfirst(lc);
		Datum		optionvalue;
		bool		optionisnull;
		Oid			optiontypeid;
		int32		optiontypmod;
		char	   *extval;

		optionvalue = exec_eval_expr(estate, opt->expr,
									 &optionisnull,
									 &optiontypeid,
									 &optiontypmod);
		if (optionisnull)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("RAISE statement option cannot be null")));

		extval = convert_value_to_string(estate, optionvalue, optiontypeid);

		switch (opt->opt_type)
		{
			case PLPGSQL_RAISEOPTION_ERRCODE:
				if (err_code)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("RAISE option already specified: %s",
									"ERRCODE")));
				err_code = plpgsql_recognize_err_condition(extval, true);
				condname = MemoryContextStrdup(stmt_mcontext, extval);
				break;
			case PLPGSQL_RAISEOPTION_MESSAGE:
				SET_RAISE_OPTION_TEXT(err_message, "MESSAGE");
				break;
			case PLPGSQL_RAISEOPTION_DETAIL:
				SET_RAISE_OPTION_TEXT(err_detail, "DETAIL");
				break;
			case PLPGSQL_RAISEOPTION_HINT:
				SET_RAISE_OPTION_TEXT(err_hint, "HINT");
				break;
			case PLPGSQL_RAISEOPTION_COLUMN:
				SET_RAISE_OPTION_TEXT(err_column, "COLUMN");
				break;
			case PLPGSQL_RAISEOPTION_CONSTRAINT:
				SET_RAISE_OPTION_TEXT(err_constraint, "CONSTRAINT");
				break;
			case PLPGSQL_RAISEOPTION_DATATYPE:
				SET_RAISE_OPTION_TEXT(err_datatype, "DATATYPE");
				break;
			case PLPGSQL_RAISEOPTION_TABLE:
				SET_RAISE_OPTION_TEXT(err_table, "TABLE");
				break;
			case PLPGSQL_RAISEOPTION_SCHEMA:
				SET_RAISE_OPTION_TEXT(err_schema, "SCHEMA");
				break;
			default:
				elog(ERROR, "unrecognized raise option: %d", opt->opt_type);
		}

		exec_eval_cleanup(estate);
	}

	/* Default code if nothing specified */
	if (err_code == 0 && stmt->elog_level >= ERROR)
		err_code = ERRCODE_RAISE_EXCEPTION;

	/* Default error message if nothing specified */
	if (err_message == NULL)
	{
		if (condname)
		{
			err_message = condname;
			condname = NULL;
		}
		else
			err_message = MemoryContextStrdup(stmt_mcontext,
											  unpack_sql_state(err_code));
	}

	/*
	 * Throw the error (may or may not come back)
	 */
	ereport(stmt->elog_level,
			(err_code ? errcode(err_code) : 0,
			 errmsg_internal("%s", err_message),
			 (err_detail != NULL) ? errdetail_internal("%s", err_detail) : 0,
			 (err_hint != NULL) ? errhint("%s", err_hint) : 0,
			 (err_column != NULL) ?
			 err_generic_string(PG_DIAG_COLUMN_NAME, err_column) : 0,
			 (err_constraint != NULL) ?
			 err_generic_string(PG_DIAG_CONSTRAINT_NAME, err_constraint) : 0,
			 (err_datatype != NULL) ?
			 err_generic_string(PG_DIAG_DATATYPE_NAME, err_datatype) : 0,
			 (err_table != NULL) ?
			 err_generic_string(PG_DIAG_TABLE_NAME, err_table) : 0,
			 (err_schema != NULL) ?
			 err_generic_string(PG_DIAG_SCHEMA_NAME, err_schema) : 0));

	/* Clean up transient strings */
	MemoryContextReset(stmt_mcontext);

	return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt_assert			Assert statement
 * ----------
 */
static int
exec_stmt_assert(PLpgSQL_execstate *estate, PLpgSQL_stmt_assert *stmt)
{
	bool		value;
	bool		isnull;

	/* do nothing when asserts are not enabled */
	if (!plpgsql_check_asserts)
		return PLPGSQL_RC_OK;

	value = exec_eval_boolean(estate, stmt->cond, &isnull);
	exec_eval_cleanup(estate);

	if (isnull || !value)
	{
		char	   *message = NULL;

		if (stmt->message != NULL)
		{
			Datum		val;
			Oid			typeid;
			int32		typmod;

			val = exec_eval_expr(estate, stmt->message,
								 &isnull, &typeid, &typmod);
			if (!isnull)
				message = convert_value_to_string(estate, val, typeid);
			/* we mustn't do exec_eval_cleanup here */
		}

		ereport(ERROR,
				(errcode(ERRCODE_ASSERT_FAILURE),
				 message ? errmsg_internal("%s", message) :
				 errmsg("assertion failed")));
	}

	return PLPGSQL_RC_OK;
}

/* ----------
 * Initialize a mostly empty execution state
 * ----------
 */
static void
plpgsql_estate_setup(PLpgSQL_execstate *estate,
					 PLpgSQL_function *func,
					 ReturnSetInfo *rsi,
					 EState *simple_eval_estate,
					 ResourceOwner simple_eval_resowner)
{
	HASHCTL		ctl;

	/* this link will be restored at exit from plpgsql_call_handler */
	func->cur_estate = estate;

	estate->func = func;

	estate->retval = (Datum) 0;
	estate->retisnull = true;
	estate->rettype = InvalidOid;

	estate->fn_rettype = func->fn_rettype;
	estate->retistuple = func->fn_retistuple;
	estate->retisset = func->fn_retset;

	estate->readonly_func = func->fn_readonly;
	estate->atomic = true;

	estate->exitlabel = NULL;
	estate->cur_error = NULL;

	estate->tuple_store = NULL;
    estate->handle_exceptions = false;

	estate->tuple_store_desc = NULL;
	if (rsi)
	{
		estate->tuple_store_cxt = rsi->econtext->ecxt_per_query_memory;
		estate->tuple_store_owner = CurrentResourceOwner;
	}
	else
	{
		estate->tuple_store_cxt = NULL;
		estate->tuple_store_owner = NULL;
	}
	estate->rsi = rsi;

	estate->found_varno = func->found_varno;
	estate->ndatums = func->ndatums;
	estate->datums = NULL;
	/* the datums array will be filled by copy_plpgsql_datums() */
	estate->datum_context = CurrentMemoryContext;

	/* initialize our ParamListInfo with appropriate hook functions */
	estate->paramLI = (ParamListInfo)
		palloc(offsetof(ParamListInfoData, params));
	estate->paramLI->paramFetch = plpgsql_param_fetch;
	estate->paramLI->paramFetchArg = (void *) estate;
	estate->paramLI->paramCompile = plpgsql_param_compile;
	estate->paramLI->paramCompileArg = NULL;	/* not needed */
	estate->paramLI->parserSetup = (ParserSetupHook) plpgsql_parser_setup;
	estate->paramLI->parserSetupArg = NULL; /* filled during use */
	estate->paramLI->param_context = 0;
	estate->paramLI->numParams = estate->ndatums;

	/* set up for use of appropriate simple-expression EState and cast hash */
	if (simple_eval_estate)
	{
		estate->simple_eval_estate = simple_eval_estate;
		/* Private cast hash just lives in function's main context */
		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(plpgsql_CastHashKey);
		ctl.entrysize = sizeof(plpgsql_CastHashEntry);
		ctl.hcxt = CurrentMemoryContext;
		estate->cast_hash = hash_create("PLpgSQL private cast cache",
										16, /* start small and extend */
										&ctl,
										HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		estate->cast_hash_context = CurrentMemoryContext;
	}
	else
	{
		estate->simple_eval_estate = shared_simple_eval_estate;
		/* Create the session-wide cast-info hash table if we didn't already */
		if (shared_cast_hash == NULL)
		{
			shared_cast_context = AllocSetContextCreate(TopMemoryContext,
														"PLpgSQL cast info",
														ALLOCSET_DEFAULT_SIZES);
			memset(&ctl, 0, sizeof(ctl));
			ctl.keysize = sizeof(plpgsql_CastHashKey);
			ctl.entrysize = sizeof(plpgsql_CastHashEntry);
			ctl.hcxt = shared_cast_context;
			shared_cast_hash = hash_create("PLpgSQL cast cache",
										   16,	/* start small and extend */
										   &ctl,
										   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		}
		estate->cast_hash = shared_cast_hash;
		estate->cast_hash_context = shared_cast_context;
	}
	/* likewise for the simple-expression resource owner */
	if (simple_eval_resowner)
		estate->simple_eval_resowner = simple_eval_resowner;
	else
		estate->simple_eval_resowner = shared_simple_eval_resowner;

	/* if there's a procedure resowner, it'll be filled in later */
	estate->procedure_resowner = NULL;

	/*
	 * We start with no stmt_mcontext; one will be created only if needed.
	 * That context will be a direct child of the function's main execution
	 * context.  Additional stmt_mcontexts might be created as children of it.
	 */
	estate->stmt_mcontext = NULL;
	estate->stmt_mcontext_parent = CurrentMemoryContext;

	estate->eval_tuptable = NULL;
	estate->eval_processed = 0;
	estate->eval_lastoid = InvalidOid;
	estate->eval_econtext = NULL;

	estate->err_stmt = NULL;
	estate->err_text = NULL;

	estate->plugin_info = NULL;

	estate->check_pullup = NULL;
	estate->check_target_oid = InvalidOid;
	estate->check_tgtype = 0;

	/*
	 * Create an EState and ExprContext for evaluation of simple expressions.
	 */
	plpgsql_create_econtext(estate);

	/*
	 * Let the plugin see this function before we initialize any local
	 * PL/pgSQL variables - note that we also give the plugin a few function
	 * pointers so it can call back into PL/pgSQL for doing things like
	 * variable assignments and stack traces
	 */
	if (*plpgsql_plugin_ptr)
	{
		(*plpgsql_plugin_ptr)->error_callback = plpgsql_exec_error_callback;
		(*plpgsql_plugin_ptr)->assign_expr = exec_assign_expr;

		if ((*plpgsql_plugin_ptr)->func_setup)
			((*plpgsql_plugin_ptr)->func_setup) (estate, func);
	}
}

/* ----------
 * Release temporary memory used by expression/subselect evaluation
 *
 * NB: the result of the evaluation is no longer valid after this is done,
 * unless it is a pass-by-value datatype.
 *
 * NB: if you change this code, see also the hacks in exec_assign_value's
 * PLPGSQL_DTYPE_ARRAYELEM case for partial cleanup after subscript evals.
 * ----------
 */
static void
exec_eval_cleanup(PLpgSQL_execstate *estate)
{
	/* Clear result of a full SPI_execute */
	if (estate->eval_tuptable != NULL)
		SPI_freetuptable(estate->eval_tuptable);
	estate->eval_tuptable = NULL;

	/*
	 * Clear result of exec_eval_simple_expr (but keep the econtext).  This
	 * also clears any short-lived allocations done via get_eval_mcontext.
	 */
	if (estate->eval_econtext != NULL)
		ResetExprContext(estate->eval_econtext);
}

static void
exec_check_query_pullup(PLpgSQL_function *func, Query *query, FuncCheckData *chkdata)
{
	if (query == NULL || chkdata->pullup)
	{
		return;
	}

	if (query->commandType == CMD_UTILITY)
	{
		/* only SET operation is allowed */
		if (query->utilityStmt && IsA(query->utilityStmt, VariableSetStmt))
			return;

		chkdata->pullup = true;
		return;
	}
	else if (contain_dml((Node *)query))
	{
		/* For DML and 'select ... for update/share' */
		chkdata->pullup = true;
		return;
	}

	/* check if have pullup function */
	if (contain_unshippable_functions((Node *)query) ||
		contain_opentenbaseLO_functions((Node *)query))
	{
		chkdata->pullup = true;
		return;
	}

	/* check for function */
	if (contain_pullup_range_table((Node *)query))
	{
		chkdata->pullup = true;
		return;
	}
}

/* ----------
 * Generate a prepared plan
 * ----------
 */
static void
exec_prepare_plan(PLpgSQL_execstate *estate,
				  PLpgSQL_expr *expr, int cursorOptions)
{
	SPIPlanPtr	plan;

	/*
	 * The grammar can't conveniently set expr->func while building the parse
	 * tree, so make sure it's set before parser hooks need it.
	 */
	expr->func = estate->func;

	/*
	 * Generate and save the plan
	 */
	plan = SPI_prepare_params(expr->query,
							  (ParserSetupHook) plpgsql_parser_setup,
							  (void *) expr,
							  cursorOptions);
	if (plan == NULL)
		elog(ERROR, "SPI_prepare_extended failed for \"%s\": %s",
			 expr->query, SPI_result_code_string(SPI_result));

	SPI_keepplan(plan);
	expr->plan = plan;
	/* Check to see if it's a simple expression */
	exec_simple_check_plan(estate, expr);

	/*
	 * Mark expression as not using a read-write param.  exec_assign_value has
	 * to take steps to override this if appropriate; that seems cleaner than
	 * adding parameters to all other callers.
	 */
	expr->rwparam = -1;
}


/* ----------
 * exec_stmt_execsql			Execute an SQL statement (possibly with INTO).
 *
 * Note: some callers rely on this not touching stmt_mcontext.  If it ever
 * needs to use that, fix those callers to push/pop stmt_mcontext.
 * ----------
 */
static int
exec_stmt_execsql(PLpgSQL_execstate *estate,
				  PLpgSQL_stmt_execsql *stmt)
{
	ParamListInfo paramLI;
	long		tcount;
	int			rc;
	PLpgSQL_expr *expr = stmt->sqlstmt;
	
	/*
	 * On the first call for this statement generate the plan, and detect
	 * whether the statement is INSERT/UPDATE/DELETE
	 */
	if (expr->plan == NULL || estate->check_pullup)
	{
		ListCell   *l;

		exec_prepare_plan(estate, expr, CURSOR_OPT_PARALLEL_OK);
		stmt->mod_stmt = false;
		foreach(l, SPI_plan_get_plan_sources(expr->plan))
		{
			CachedPlanSource *plansource = (CachedPlanSource *) lfirst(l);
			ListCell   *l2;

			foreach(l2, plansource->query_list)
			{
				Query	   *q = lfirst_node(Query, l2);

				if (q->canSetTag)
				{
					if (q->commandType == CMD_INSERT ||
						q->commandType == CMD_UPDATE ||
						q->commandType == CMD_DELETE ||
						q->commandType == CMD_MERGE)
						stmt->mod_stmt = true;
					/* PGXCTODO: Support a better parameter interface for XC with DMLs */
					if
#ifdef XCP
					   (IS_DISTRIBUTED_DATANODE && !enable_datanode_row_triggers &&
#endif
						(q->commandType == CMD_INSERT ||
						 q->commandType == CMD_UPDATE ||
						 q->commandType == CMD_DELETE ||
						 q->commandType == CMD_MERGE)
#ifdef XCP
					   )
#endif
					{
						ereport(LOG,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("Postgres-XL does not support DML queries in PL/pgSQL on Datanodes")));
					}
				}
			}
		}
	}

	if (estate->check_pullup)
		return PLPGSQL_RC_OK;

	/*
	 * Set up ParamListInfo to pass to executor
	 */
	paramLI = setup_param_list(estate, expr);

	/*
	 * If we have INTO, then we only need one row back ... but if we have INTO
	 * STRICT, ask for two rows, so that we can verify the statement returns
	 * only one.  INSERT/UPDATE/DELETE are always treated strictly. Without
	 * INTO, just run the statement to completion (tcount = 0).
	 *
	 * We could just ask for two rows always when using INTO, but there are
	 * some cases where demanding the extra row costs significant time, eg by
	 * forcing completion of a sequential scan.  So don't do it unless we need
	 * to enforce strictness.
	 * 
	 * In distributed mode, tcount will not be sent from cn to dn, so dn will 
	 * run the statement to completion. Therefore, cn cannot limit tcount, 
	 * in order to ensure the correctness of _SPI_checktuples.
	 */
	if (stmt->into)
	{
		if (IS_PGXC_COORDINATOR)
			tcount = 0;
		else if (stmt->strict || stmt->mod_stmt)
			tcount = 2;
		else
			tcount = 1;
	}
	else
		tcount = 0;

#ifdef __OPENTENBASE__		
	if (PlpgsqlDebugPrint)
	{
		char *params = format_expr_params(estate, expr);
		elog(LOG, "Query Text: %s", expr->query);
		elog(LOG, "Query Parameters: %s", params);
		pfree_ext(params);
	}
#endif

	/*
	 * Execute the plan
	 */
	rc = SPI_execute_plan_with_paramlist(expr->plan, paramLI,
										 estate->readonly_func, tcount);

	/*
	 * Check for error, and set FOUND if appropriate (for historical reasons
	 * we set FOUND only for certain query types).  Also Assert that we
	 * identified the statement type the same as SPI did.
	 */
	switch (rc)
	{
		case SPI_OK_SELECT:
			Assert(!stmt->mod_stmt);
			exec_set_found(estate, (SPI_processed != 0));
			break;

		case SPI_OK_INSERT:
		case SPI_OK_UPDATE:
		case SPI_OK_DELETE:
		case SPI_OK_INSERT_RETURNING:
		case SPI_OK_UPDATE_RETURNING:
		case SPI_OK_DELETE_RETURNING:
		case SPI_OK_MERGE:
			Assert(stmt->mod_stmt);
			exec_set_found(estate, (SPI_processed != 0));
			break;

		case SPI_OK_SELINTO:
		case SPI_OK_UTILITY:
			Assert(!stmt->mod_stmt);
			break;

		case SPI_OK_REWRITTEN:
			Assert(!stmt->mod_stmt);

			/*
			 * The command was rewritten into another kind of command. It's
			 * not clear what FOUND would mean in that case (and SPI doesn't
			 * return the row count either), so just set it to false.
			 */
			exec_set_found(estate, false);
			break;

			/* Some SPI errors deserve specific error messages */
		case SPI_ERROR_COPY:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot COPY to/from client in PL/pgSQL")));
		case SPI_ERROR_TRANSACTION:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("unsupported transaction command in PL/pgSQL")));

		default:
			elog(ERROR, "SPI_execute_plan_with_paramlist failed executing query \"%s\": %s",
				 expr->query, SPI_result_code_string(rc));
	}

	/* All variants should save result info for GET DIAGNOSTICS */
	estate->eval_processed = SPI_processed;
	estate->eval_lastoid = SPI_lastoid;

	/* Process INTO if present */
	if (stmt->into)
	{
		SPITupleTable *tuptab = SPI_tuptable;
		uint64		n = SPI_processed;
		PLpgSQL_variable *target;

		/* If the statement did not return a tuple table, complain */
		if (tuptab == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("INTO used with a command that cannot return data")));

		/* Fetch target's datum entry */
		target = (PLpgSQL_variable *) estate->datums[stmt->target->dno];

		/*
		 * If SELECT ... INTO specified STRICT, and the query didn't find
		 * exactly one row, throw an error.  If STRICT was not specified, then
		 * allow the query to find any number of rows.
		 */
		if (n == 0)
		{
			if (stmt->strict)
			{
				char	   *errdetail;

				if (estate->func->print_strict_params)
					errdetail = format_expr_params(estate, expr);
				else
					errdetail = NULL;

				ereport(ERROR,
						(errcode(ERRCODE_NO_DATA_FOUND),
						 errmsg("query returned no rows"),
						 errdetail ? errdetail_internal("parameters: %s", errdetail) : 0));
			}
			/* set the target to NULL(s) */
			exec_move_row(estate, target, NULL, tuptab->tupdesc);
		}
		else
		{
			if (n > 1 && (stmt->strict || stmt->mod_stmt))
			{
				char	   *errdetail;

				if (estate->func->print_strict_params)
					errdetail = format_expr_params(estate, expr);
				else
					errdetail = NULL;

				ereport(ERROR,
						(errcode(ERRCODE_TOO_MANY_ROWS),
						 errmsg("query returned more than one row"),
						 errdetail ? errdetail_internal("parameters: %s", errdetail) : 0));
			}
			/* Put the first result row into the target */
			exec_move_row(estate, target, tuptab->vals[0], tuptab->tupdesc);
		}

		/* Clean up */
		exec_eval_cleanup(estate);
		SPI_freetuptable(SPI_tuptable);
	}
	else
	{
		/* If the statement returned a tuple table, complain */
		if (SPI_tuptable != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("query has no destination for result data"),
					 (rc == SPI_OK_SELECT) ? errhint("If you want to discard the results of a SELECT, use PERFORM instead.") : 0));
	}

	return PLPGSQL_RC_OK;
}


/* ----------
 * exec_stmt_dynexecute			Execute a dynamic SQL query
 *					(possibly with INTO).
 * ----------
 */
static int
exec_stmt_dynexecute(PLpgSQL_execstate *estate,
					 PLpgSQL_stmt_dynexecute *stmt)
{
	Datum		query;
	bool		isnull;
	Oid			restype;
	int32		restypmod;
	char	   *querystr;
	int			exec_res;
	ParamListInfo paramLI;
	MemoryContext stmt_mcontext = get_stmt_mcontext(estate);

	if (estate->check_pullup)
	{
		*estate->check_pullup = true;
		return PLPGSQL_RC_OK;
	}

	/*
	 * First we evaluate the string expression after the EXECUTE keyword. Its
	 * result is the querystring we have to execute.
	 */
	query = exec_eval_expr(estate, stmt->query, &isnull, &restype, &restypmod);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("query string argument of EXECUTE is null")));

	/* Get the C-String representation */
	querystr = convert_value_to_string(estate, query, restype);

	/* copy it into the stmt_mcontext before we clean up */
	querystr = MemoryContextStrdup(stmt_mcontext, querystr);

	exec_eval_cleanup(estate);

	/*
	 * Execute the query without preparing a saved plan.
	 */
	paramLI = exec_eval_using_params(estate, stmt->params);

#ifdef __OPENTENBASE__		
	if (PlpgsqlDebugPrint)
	{
		char *params = format_preparedparamsdata(estate, paramLI);
		elog(LOG, "Query Text: %s", querystr);
		elog(LOG, "Query Parameters: %s", params);
		pfree_ext(params);
	}
#endif

	exec_res = SPI_execute_with_receiver(querystr, paramLI,
										 estate->readonly_func, 0, NULL);

	switch (exec_res)
	{
		case SPI_OK_SELECT:
		case SPI_OK_INSERT:
		case SPI_OK_UPDATE:
		case SPI_OK_DELETE:
		case SPI_OK_INSERT_RETURNING:
		case SPI_OK_UPDATE_RETURNING:
		case SPI_OK_DELETE_RETURNING:
		case SPI_OK_MERGE:
		case SPI_OK_UTILITY:
		case SPI_OK_REWRITTEN:
			break;

		case 0:

			/*
			 * Also allow a zero return, which implies the querystring
			 * contained no commands.
			 */
			break;

		case SPI_OK_SELINTO:

			/*
			 * We want to disallow SELECT INTO for now, because its behavior
			 * is not consistent with SELECT INTO in a normal plpgsql context.
			 * (We need to reimplement EXECUTE to parse the string as a
			 * plpgsql command, not just feed it to SPI_execute.)  This is not
			 * a functional limitation because CREATE TABLE AS is allowed.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("EXECUTE of SELECT ... INTO is not implemented"),
					 errhint("You might want to use EXECUTE ... INTO or EXECUTE CREATE TABLE ... AS instead.")));
			break;

			/* Some SPI errors deserve specific error messages */
		case SPI_ERROR_COPY:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot COPY to/from client in PL/pgSQL")));
		case SPI_ERROR_TRANSACTION:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("EXECUTE of transaction commands is not implemented")));

		default:
			elog(ERROR, "SPI_execute_with_receiver failed executing query \"%s\": %s",
				 querystr, SPI_result_code_string(exec_res));
			break;
	}

	/* Save result info for GET DIAGNOSTICS */
	estate->eval_processed = SPI_processed;
	estate->eval_lastoid = SPI_lastoid;

	/* Process INTO if present */
	if (stmt->into)
	{
		SPITupleTable *tuptab = SPI_tuptable;
		uint64		n = SPI_processed;
		PLpgSQL_variable *target;

		/* If the statement did not return a tuple table, complain */
		if (tuptab == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("INTO used with a command that cannot return data")));

		/* Fetch target's datum entry */
		target = (PLpgSQL_variable *) estate->datums[stmt->target->dno];

		/*
		 * If SELECT ... INTO specified STRICT, and the query didn't find
		 * exactly one row, throw an error.  If STRICT was not specified, then
		 * allow the query to find any number of rows.
		 */
		if (n == 0)
		{
			if (stmt->strict)
			{
				char	   *errdetail;

				if (estate->func->print_strict_params)
					errdetail = format_preparedparamsdata(estate, paramLI);
				else
					errdetail = NULL;

				ereport(ERROR,
						(errcode(ERRCODE_NO_DATA_FOUND),
						 errmsg("query returned no rows"),
						 errdetail ? errdetail_internal("parameters: %s", errdetail) : 0));
			}
			/* set the target to NULL(s) */
			exec_move_row(estate, target, NULL, tuptab->tupdesc);
		}
		else
		{
			if (n > 1 && stmt->strict)
			{
				char	   *errdetail;

				if (estate->func->print_strict_params)
					errdetail = format_preparedparamsdata(estate, paramLI);
				else
					errdetail = NULL;

				ereport(ERROR,
						(errcode(ERRCODE_TOO_MANY_ROWS),
						 errmsg("query returned more than one row"),
						 errdetail ? errdetail_internal("parameters: %s", errdetail) : 0));
			}

			/* Put the first result row into the target */
			exec_move_row(estate, target, tuptab->vals[0], tuptab->tupdesc);
		}
		/* clean up after exec_move_row() */
		exec_eval_cleanup(estate);
	}
	else
	{
		/*
		 * It might be a good idea to raise an error if the query returned
		 * tuples that are being ignored, but historically we have not done
		 * that.
		 */
	}

	/* Release any result from SPI_execute, as well as transient data */
	SPI_freetuptable(SPI_tuptable);
	MemoryContextReset(stmt_mcontext);

	return PLPGSQL_RC_OK;
}


/* ----------
 * exec_stmt_dynfors			Execute a dynamic query, assign each
 *					tuple to a record or row and
 *					execute a group of statements
 *					for it.
 * ----------
 */
static int
exec_stmt_dynfors(PLpgSQL_execstate *estate, PLpgSQL_stmt_dynfors *stmt)
{
	Portal		portal;
	int			rc;

	if (estate->check_pullup)
	{
		*estate->check_pullup = true;
		return PLPGSQL_RC_OK;
	}

	portal = exec_dynquery_with_params(estate, stmt->query, stmt->params,
									   NULL, CURSOR_OPT_NO_SCROLL);

	/*
	 * Execute the loop
	 */
	rc = exec_for_query(estate, (PLpgSQL_stmt_forq *) stmt, portal, true);

	/*
	 * Close the implicit cursor
	 */
	SPI_cursor_close(portal);

	return rc;
}


/* ----------
 * exec_stmt_open			Execute an OPEN cursor statement
 * ----------
 */
static int
exec_stmt_open(PLpgSQL_execstate *estate, PLpgSQL_stmt_open *stmt)
{
	PLpgSQL_var *curvar;
	MemoryContext stmt_mcontext = NULL;
	char	   *curname = NULL;
	PLpgSQL_expr *query;
	Portal		portal;
	ParamListInfo paramLI;

	/* ----------
	 * Get the cursor variable and if it has an assigned name, check
	 * that it's not in use currently.
	 * ----------
	 */
	curvar = (PLpgSQL_var *) (estate->datums[stmt->curvar]);
	if (!curvar->isnull)
	{
		MemoryContext oldcontext;

		/* We only need stmt_mcontext to hold the cursor name string */
		stmt_mcontext = get_stmt_mcontext(estate);
		oldcontext = MemoryContextSwitchTo(stmt_mcontext);
		curname = TextDatumGetCString(curvar->value);
		MemoryContextSwitchTo(oldcontext);

		if (SPI_cursor_find(curname) != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_CURSOR),
					 errmsg("cursor \"%s\" already in use", curname)));
	}

	/* ----------
	 * Process the OPEN according to it's type.
	 * ----------
	 */
	if (stmt->query != NULL)
	{
		/* ----------
		 * This is an OPEN refcursor FOR SELECT ...
		 *
		 * We just make sure the query is planned. The real work is
		 * done downstairs.
		 * ----------
		 */
		query = stmt->query;
		if (query->plan == NULL || estate->check_pullup)
			exec_prepare_plan(estate, query, stmt->cursor_options);
	}
	else if (stmt->dynquery != NULL)
	{
		/* ----------
		 * This is an OPEN refcursor FOR EXECUTE ...
		 * ----------
		 */
		portal = exec_dynquery_with_params(estate,
										   stmt->dynquery,
										   stmt->params,
										   curname,
										   stmt->cursor_options);

		/*
		 * If cursor variable was NULL, store the generated portal name in it.
		 * Note: exec_dynquery_with_params already reset the stmt_mcontext, so
		 * curname is a dangling pointer here; but testing it for nullness is
		 * OK.
		 */
		if (curname == NULL)
			assign_text_var(estate, curvar, portal->name);

		return PLPGSQL_RC_OK;
	}
	else
	{
		/* ----------
		 * This is an OPEN cursor
		 *
		 * Note: parser should already have checked that statement supplies
		 * args iff cursor needs them, but we check again to be safe.
		 * ----------
		 */
		if (stmt->argquery != NULL)
		{
			/* ----------
			 * OPEN CURSOR with args.  We fake a SELECT ... INTO ...
			 * statement to evaluate the args and put 'em into the
			 * internal row.
			 * ----------
			 */
			PLpgSQL_stmt_execsql set_args;

			if (curvar->cursor_explicit_argrow < 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("arguments given for cursor without arguments")));

			memset(&set_args, 0, sizeof(set_args));
			set_args.cmd_type = PLPGSQL_STMT_EXECSQL;
			set_args.lineno = stmt->lineno;
			set_args.sqlstmt = stmt->argquery;
			set_args.into = true;
			/* XXX historically this has not been STRICT */
			set_args.target = (PLpgSQL_variable *)
				(estate->datums[curvar->cursor_explicit_argrow]);

			if (exec_stmt_execsql(estate, &set_args) != PLPGSQL_RC_OK)
				elog(ERROR, "open cursor failed during argument processing");
		}
		else
		{
			if (curvar->cursor_explicit_argrow >= 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("arguments required for cursor")));
		}

		query = curvar->cursor_explicit_expr;
		if (query->plan == NULL || estate->check_pullup)
			exec_prepare_plan(estate, query, curvar->cursor_options);
	}

	if (estate->check_pullup)
		return PLPGSQL_RC_OK;

	/*
	 * Set up ParamListInfo for this query
	 */
	paramLI = setup_param_list(estate, query);

	/*
	 * Open the cursor (the paramlist will get copied into the portal)
	 */
	portal = SPI_cursor_open_with_paramlist(curname, query->plan,
											paramLI,
											estate->readonly_func);
	if (portal == NULL)
		elog(ERROR, "could not open cursor: %s",
			 SPI_result_code_string(SPI_result));

	/*
	 * If cursor variable was NULL, store the generated portal name in it
	 */
	if (curname == NULL)
		assign_text_var(estate, curvar, portal->name);

	/* If we had any transient data, clean it up */
	exec_eval_cleanup(estate);
	if (stmt_mcontext)
		MemoryContextReset(stmt_mcontext);

	return PLPGSQL_RC_OK;
}


/* ----------
 * exec_stmt_fetch			Fetch from a cursor into a target, or just
 *							move the current position of the cursor
 * ----------
 */
static int
exec_stmt_fetch(PLpgSQL_execstate *estate, PLpgSQL_stmt_fetch *stmt)
{
	PLpgSQL_var *curvar;
	long		how_many = stmt->how_many;
	SPITupleTable *tuptab;
	Portal		portal;
	char	   *curname;
	uint64		n;
	MemoryContext oldcontext;

	if (estate->check_pullup)
		return PLPGSQL_RC_OK;

	/* ----------
	 * Get the portal of the cursor by name
	 * ----------
	 */
	curvar = (PLpgSQL_var *) (estate->datums[stmt->curvar]);
	if (curvar->isnull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("cursor variable \"%s\" is null", curvar->refname)));

	/* Use eval_mcontext for short-lived string */
	oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
	curname = TextDatumGetCString(curvar->value);
	MemoryContextSwitchTo(oldcontext);

	portal = SPI_cursor_find(curname);
	if (portal == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CURSOR),
				 errmsg("cursor \"%s\" does not exist", curname)));

	/* Calculate position for FETCH_RELATIVE or FETCH_ABSOLUTE */
	if (stmt->expr)
	{
		bool		isnull;

		/* XXX should be doing this in LONG not INT width */
		how_many = exec_eval_integer(estate, stmt->expr, &isnull);

		if (isnull)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("relative or absolute cursor position is null")));

		exec_eval_cleanup(estate);
	}

	if (!stmt->is_move)
	{
		PLpgSQL_variable *target;

		/* ----------
		 * Fetch 1 tuple from the cursor
		 * ----------
		 */
		SPI_scroll_cursor_fetch(portal, stmt->direction, how_many);
		tuptab = SPI_tuptable;
		n = SPI_processed;

		/* ----------
		 * Set the target appropriately.
		 * ----------
		 */
		target = (PLpgSQL_variable *) estate->datums[stmt->target->dno];
		if (n == 0)
			exec_move_row(estate, target, NULL, tuptab->tupdesc);
		else
			exec_move_row(estate, target, tuptab->vals[0], tuptab->tupdesc);

		exec_eval_cleanup(estate);
		SPI_freetuptable(tuptab);
	}
	else
	{
		/* Move the cursor */
		SPI_scroll_cursor_move(portal, stmt->direction, how_many);
		n = SPI_processed;
	}

	/* Set the ROW_COUNT and the global FOUND variable appropriately. */
	estate->eval_processed = n;
	exec_set_found(estate, n != 0);

	return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt_close			Close a cursor
 * ----------
 */
static int
exec_stmt_close(PLpgSQL_execstate *estate, PLpgSQL_stmt_close *stmt)
{
	PLpgSQL_var *curvar;
	Portal		portal;
	char	   *curname;
	MemoryContext oldcontext;

	if (estate->check_pullup)
		return PLPGSQL_RC_OK;

	/* ----------
	 * Get the portal of the cursor by name
	 * ----------
	 */
	curvar = (PLpgSQL_var *) (estate->datums[stmt->curvar]);
	if (curvar->isnull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("cursor variable \"%s\" is null", curvar->refname)));

	/* Use eval_mcontext for short-lived string */
	oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
	curname = TextDatumGetCString(curvar->value);
	MemoryContextSwitchTo(oldcontext);

	portal = SPI_cursor_find(curname);
	if (portal == NULL)
    {
        if (estate->handle_exceptions)
            return PLPGSQL_RC_OK;

        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_CURSOR),
                        errmsg("cursor \"%s\" does not exist", curname)));
    }

	/* ----------
	 * And close it.
	 * ----------
	 */
	SPI_cursor_close(portal);

	return PLPGSQL_RC_OK;
}

/*
 * exec_stmt_commit
 *
 * Commit the transaction.
 */
static int
exec_stmt_commit(PLpgSQL_execstate *estate, PLpgSQL_stmt_commit *stmt)
{
	bool is_trans_blk = IsTransactionBlock();

	if (estate->check_pullup)
		return PLPGSQL_RC_OK;

	if (enable_distri_print)
		elog(LOG, "exec_stmt_commit %d %d", stmt->cmd_type, stmt->lineno);
	SPI_commit();

	SPI_start_transaction(is_trans_blk);

	/*
	 * We need to build new simple-expression infrastructure, since the old
	 * data structures are gone.
	 */
	estate->simple_eval_estate = NULL;
	estate->simple_eval_resowner = NULL;
	plpgsql_create_econtext(estate);

	return PLPGSQL_RC_OK;
}

/*
 * exec_stmt_rollback
 *
 * Abort the transaction.
 */
static int
exec_stmt_rollback(PLpgSQL_execstate *estate, PLpgSQL_stmt_rollback *stmt)
{
	bool is_trans_blk = IsTransactionBlock();

	if (estate->check_pullup)
		return PLPGSQL_RC_OK;

	SPI_rollback();

	SPI_start_transaction(is_trans_blk);

	/*
	 * We need to build new simple-expression infrastructure, since the old
	 * data structures are gone.
	 */
	estate->simple_eval_estate = NULL;
	estate->simple_eval_resowner = NULL;
	plpgsql_create_econtext(estate);

	return PLPGSQL_RC_OK;
}

/*
 * exec_stmt_set
 *
 * Execute SET/RESET statement.
 *
 * We just parse and execute the statement normally, but we have to do it
 * without setting a snapshot, for things like SET TRANSACTION.
 */
static int
exec_stmt_set(PLpgSQL_execstate *estate, PLpgSQL_stmt_set *stmt)
{
	PLpgSQL_expr *expr = stmt->expr;
	SPIExecuteOptions options;
	int			rc;

	if (expr->plan == NULL || estate->check_pullup)
		exec_prepare_plan(estate, expr, 0);

	if (estate->check_pullup)
		return PLPGSQL_RC_OK;

	memset(&options, 0, sizeof(options));
	options.read_only = estate->readonly_func;
	options.no_snapshots = true;	/* disable SPI's snapshot management */

	rc = SPI_execute_plan_extended(expr->plan, &options);

	if (rc != SPI_OK_UTILITY)
		elog(ERROR, "SPI_execute_plan_extended failed executing query \"%s\": %s",
			 expr->query, SPI_result_code_string(rc));

	return PLPGSQL_RC_OK;
}

/* ----------
 * exec_assign_expr			Put an expression's result into a variable.
 * ----------
 */
static void
exec_assign_expr(PLpgSQL_execstate *estate, PLpgSQL_datum *target,
				 PLpgSQL_expr *expr)
{
	Datum		value;
	bool		isnull;
	Oid			valtype;
	int32		valtypmod;

	/*
	 * If first time through, create a plan for this expression, and then see
	 * if we can pass the target variable as a read-write parameter to the
	 * expression.  (This is a bit messy, but it seems cleaner than modifying
	 * the API of exec_eval_expr for the purpose.)
	 */
	if (expr->plan == NULL || estate->check_pullup)
	{

		if (estate->check_pullup &&
		    target->dtype == PLPGSQL_DTYPE_RECFIELD)
		{
			/* if we are assigning :new.distkey or :old.distkey */
			if (exec_check_rec_distkey(estate,
			                           (PLpgSQL_recfield *) target,
			                           InvalidOid))
			{
				*estate->check_pullup = true;
				return;
			}
		}

		/*
		 * Mark the expression as being an assignment source, if target is a
		 * simple variable.  (This is a bit messy, but it seems cleaner than
		 * modifying the API of exec_prepare_plan for the purpose.  We need to
		 * stash the target dno into the expr anyway, so that it will be
		 * available if we have to replan.)
		 */

		exec_prepare_plan(estate, expr, 0);
	}

	if (estate->check_pullup)
		return;

	value = exec_eval_expr(estate, expr, &isnull, &valtype, &valtypmod);
	exec_assign_value(estate, target, value, isnull, valtype, valtypmod);
	exec_eval_cleanup(estate);
}


/* ----------
 * exec_assign_c_string		Put a C string into a text variable.
 *
 * We take a NULL pointer as signifying empty string, not SQL null.
 *
 * As with the underlying exec_assign_value, caller is expected to do
 * exec_eval_cleanup later.
 * ----------
 */
static void
exec_assign_c_string(PLpgSQL_execstate *estate, PLpgSQL_datum *target,
					 const char *str)
{
	text	   *value;
	MemoryContext oldcontext;

	/* Use eval_mcontext for short-lived text value */
	oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
	if (str != NULL)
		value = cstring_to_text(str);
	else
		value = cstring_to_text("");
	MemoryContextSwitchTo(oldcontext);

	exec_assign_value(estate, target, PointerGetDatum(value), false,
					  TEXTOID, -1);
}


/* ----------
 * exec_assign_value			Put a value into a target datum
 *
 * Note: in some code paths, this will leak memory in the eval_mcontext;
 * we assume that will be cleaned up later by exec_eval_cleanup.  We cannot
 * call exec_eval_cleanup here for fear of destroying the input Datum value.
 * ----------
 */
static void
exec_assign_value(PLpgSQL_execstate *estate,
				  PLpgSQL_datum *target,
				  Datum value, bool isNull,
				  Oid valtype, int32 valtypmod)
{
	switch (target->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
			{
				/*
				 * Target is a variable
				 */
				PLpgSQL_var *var = (PLpgSQL_var *) target;
				Datum		newvalue;

				newvalue = exec_cast_value(estate,
										   value,
										   &isNull,
										   valtype,
										   valtypmod,
										   var->datatype->typoid,
										   var->datatype->atttypmod);

				if (isNull && var->notnull)
					ereport(ERROR,
							(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
							 errmsg("null value cannot be assigned to variable \"%s\" declared NOT NULL",
									var->refname)));

				/*
				 * If type is by-reference, copy the new value (which is
				 * probably in the eval_mcontext) into the procedure's main
				 * memory context.  But if it's a read/write reference to an
				 * expanded object, no physical copy needs to happen; at most
				 * we need to reparent the object's memory context.
				 *
				 * If it's an array, we force the value to be stored in R/W
				 * expanded form.  This wins if the function later does, say,
				 * a lot of array subscripting operations on the variable, and
				 * otherwise might lose.  We might need to use a different
				 * heuristic, but it's too soon to tell.  Also, are there
				 * cases where it'd be useful to force non-array values into
				 * expanded form?
				 */
				if (!var->datatype->typbyval && !isNull)
				{
					if (var->datatype->typisarray &&
						!VARATT_IS_EXTERNAL_EXPANDED_RW(DatumGetPointer(newvalue)))
					{
						/* array and not already R/W, so apply expand_array */
						newvalue = expand_array(newvalue,
												estate->datum_context,
												NULL);
					}
					else
					{
						/* else transfer value if R/W, else just datumCopy */
						newvalue = datumTransfer(newvalue,
												 false,
												 var->datatype->typlen);
					}
				}

				/*
				 * Now free the old value, if any, and assign the new one. But
				 * skip the assignment if old and new values are the same.
				 * Note that for expanded objects, this test is necessary and
				 * cannot reliably be made any earlier; we have to be looking
				 * at the object's standard R/W pointer to be sure pointer
				 * equality is meaningful.
				 */
				if (var->value != newvalue || var->isnull || isNull)
					assign_simple_var(estate, var, newvalue, isNull,
									  (!var->datatype->typbyval && !isNull));
				break;
			}

		case PLPGSQL_DTYPE_ROW:
			{
				/*
				 * Target is a row variable
				 */
				PLpgSQL_row *row = (PLpgSQL_row *) target;

				if (isNull)
				{
					/* If source is null, just assign nulls to the row */
					exec_move_row(estate, (PLpgSQL_variable *) row,
								  NULL, NULL);
				}
				else
				{
					/* Source must be of RECORD or composite type */
					if (!type_is_rowtype(valtype))
						ereport(ERROR,
								(errcode(ERRCODE_DATATYPE_MISMATCH),
								 errmsg("cannot assign non-composite value to a row variable")));
					exec_move_row_from_datum(estate, (PLpgSQL_variable *) row,
											 value);
				}
				break;
			}

		case PLPGSQL_DTYPE_REC:
			{
				/*
				 * Target is a record variable
				 */
				PLpgSQL_rec *rec = (PLpgSQL_rec *) target;

				if (isNull)
				{
					/* If source is null, just assign nulls to the record */
					exec_move_row(estate, (PLpgSQL_variable *) rec,
								  NULL, NULL);
				}
				else
				{
					/* Source must be of RECORD or composite type */
					if (!type_is_rowtype(valtype))
						ereport(ERROR,
								(errcode(ERRCODE_DATATYPE_MISMATCH),
								 errmsg("cannot assign non-composite value to a record variable")));
					exec_move_row_from_datum(estate, (PLpgSQL_variable *) rec,
											 value);
				}
				break;
			}

		case PLPGSQL_DTYPE_RECFIELD:
			{
				/*
				 * Target is a field of a record
				 */
				PLpgSQL_recfield *recfield = (PLpgSQL_recfield *) target;
				PLpgSQL_rec *rec;
				ExpandedRecordHeader *erh;

				rec = (PLpgSQL_rec *) (estate->datums[recfield->recparentno]);
				erh = rec->erh;

				/*
				 * If record variable is NULL, instantiate it if it has a
				 * named composite type, else complain.  (This won't change
				 * the logical state of the record, but if we successfully
				 * assign below, the unassigned fields will all become NULLs.)
				 */
				if (erh == NULL)
				{
					instantiate_empty_record_variable(estate, rec);
					erh = rec->erh;
				}

				/*
				 * Look up the field's properties if we have not already, or
				 * if the tuple descriptor ID changed since last time.
				 */
				if (unlikely(recfield->rectupledescid != erh->er_tupdesc_id))
				{
					if (!expanded_record_lookup_field(erh,
													  recfield->fieldname,
													  &recfield->finfo))
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_COLUMN),
								 errmsg("record \"%s\" has no field \"%s\"",
										rec->refname, recfield->fieldname)));
					recfield->rectupledescid = erh->er_tupdesc_id;
				}

				/* We don't support assignments to system columns. */
				if (recfield->finfo.fnumber <= 0)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot assign to system column \"%s\"",
									recfield->fieldname)));

				/* Cast the new value to the right type, if needed. */
				value = exec_cast_value(estate,
										value,
										&isNull,
										valtype,
										valtypmod,
										recfield->finfo.ftypeid,
										recfield->finfo.ftypmod);

				/* And assign it. */
				expanded_record_set_field(erh, recfield->finfo.fnumber,
										  value, isNull);
				break;
			}

		case PLPGSQL_DTYPE_ARRAYELEM:
			{
				/*
				 * Target is an element of an array
				 */
				PLpgSQL_arrayelem *arrayelem;
				int			nsubscripts;
				int			i;
				PLpgSQL_expr *subscripts[MAXDIM];
				int			subscriptvals[MAXDIM];
				Datum		oldarraydatum,
							newarraydatum,
							coerced_value;
				bool		oldarrayisnull;
				Oid			parenttypoid;
				int32		parenttypmod;
				SPITupleTable *save_eval_tuptable;
				MemoryContext oldcontext;

				/*
				 * We need to do subscript evaluation, which might require
				 * evaluating general expressions; and the caller might have
				 * done that too in order to prepare the input Datum.  We have
				 * to save and restore the caller's SPI_execute result, if
				 * any.
				 */
				save_eval_tuptable = estate->eval_tuptable;
				estate->eval_tuptable = NULL;

				/*
				 * To handle constructs like x[1][2] := something, we have to
				 * be prepared to deal with a chain of arrayelem datums. Chase
				 * back to find the base array datum, and save the subscript
				 * expressions as we go.  (We are scanning right to left here,
				 * but want to evaluate the subscripts left-to-right to
				 * minimize surprises.)  Note that arrayelem is left pointing
				 * to the leftmost arrayelem datum, where we will cache the
				 * array element type data.
				 */
				nsubscripts = 0;
				do
				{
					arrayelem = (PLpgSQL_arrayelem *) target;
					if (nsubscripts >= MAXDIM)
						ereport(ERROR,
								(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
								 errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
										nsubscripts + 1, MAXDIM)));
					subscripts[nsubscripts++] = arrayelem->subscript;
					target = estate->datums[arrayelem->arrayparentno];
				} while (target->dtype == PLPGSQL_DTYPE_ARRAYELEM);

				/* Fetch current value of array datum */
				exec_eval_datum(estate, target,
								&parenttypoid, &parenttypmod,
								&oldarraydatum, &oldarrayisnull);

				/* Update cached type data if necessary */
				if (arrayelem->parenttypoid != parenttypoid ||
					arrayelem->parenttypmod != parenttypmod)
				{
					Oid			arraytypoid;
					int32		arraytypmod = parenttypmod;
					int16		arraytyplen;
					Oid			elemtypoid;
					int16		elemtyplen;
					bool		elemtypbyval;
					char		elemtypalign;

					/* If target is domain over array, reduce to base type */
					arraytypoid = getBaseTypeAndTypmod(parenttypoid,
													   &arraytypmod);

					/* ... and identify the element type */
					elemtypoid = get_element_type(arraytypoid);
					if (!OidIsValid(elemtypoid))
						ereport(ERROR,
								(errcode(ERRCODE_DATATYPE_MISMATCH),
								 errmsg("subscripted object is not an array")));

					/* Collect needed data about the types */
					arraytyplen = get_typlen(arraytypoid);

					get_typlenbyvalalign(elemtypoid,
										 &elemtyplen,
										 &elemtypbyval,
										 &elemtypalign);

					/* Now safe to update the cached data */
					arrayelem->parenttypoid = parenttypoid;
					arrayelem->parenttypmod = parenttypmod;
					arrayelem->arraytypoid = arraytypoid;
					arrayelem->arraytypmod = arraytypmod;
					arrayelem->arraytyplen = arraytyplen;
					arrayelem->elemtypoid = elemtypoid;
					arrayelem->elemtyplen = elemtyplen;
					arrayelem->elemtypbyval = elemtypbyval;
					arrayelem->elemtypalign = elemtypalign;
				}

				/*
				 * Evaluate the subscripts, switch into left-to-right order.
				 * Like the expression built by ExecInitArrayRef(), complain
				 * if any subscript is null.
				 */
				for (i = 0; i < nsubscripts; i++)
				{
					bool		subisnull;

					subscriptvals[i] =
						exec_eval_integer(estate,
										  subscripts[nsubscripts - 1 - i],
										  &subisnull);
					if (subisnull)
						ereport(ERROR,
								(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
								 errmsg("array subscript in assignment must not be null")));

					/*
					 * Clean up in case the subscript expression wasn't
					 * simple. We can't do exec_eval_cleanup, but we can do
					 * this much (which is safe because the integer subscript
					 * value is surely pass-by-value), and we must do it in
					 * case the next subscript expression isn't simple either.
					 */
					if (estate->eval_tuptable != NULL)
						SPI_freetuptable(estate->eval_tuptable);
					estate->eval_tuptable = NULL;
				}

				/* Now we can restore caller's SPI_execute result if any. */
				Assert(estate->eval_tuptable == NULL);
				estate->eval_tuptable = save_eval_tuptable;

				/* Coerce source value to match array element type. */
				coerced_value = exec_cast_value(estate,
												value,
												&isNull,
												valtype,
												valtypmod,
												arrayelem->elemtypoid,
												arrayelem->arraytypmod);

				/*
				 * If the original array is null, cons up an empty array so
				 * that the assignment can proceed; we'll end with a
				 * one-element array containing just the assigned-to
				 * subscript.  This only works for varlena arrays, though; for
				 * fixed-length array types we skip the assignment.  We can't
				 * support assignment of a null entry into a fixed-length
				 * array, either, so that's a no-op too.  This is all ugly but
				 * corresponds to the current behavior of execExpr*.c.
				 */
				if (arrayelem->arraytyplen > 0 &&	/* fixed-length array? */
					(oldarrayisnull || isNull))
					return;

				/* empty array, if any, and newarraydatum are short-lived */
				oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));

				if (oldarrayisnull)
					oldarraydatum = PointerGetDatum(construct_empty_array(arrayelem->elemtypoid));

				/*
				 * Build the modified array value.
				 */
				newarraydatum = array_set_element(oldarraydatum,
												  nsubscripts,
												  subscriptvals,
												  coerced_value,
												  isNull,
												  true,
												  arrayelem->arraytyplen,
												  arrayelem->elemtyplen,
												  arrayelem->elemtypbyval,
												  arrayelem->elemtypalign);

				MemoryContextSwitchTo(oldcontext);

				/*
				 * Assign the new array to the base variable.  It's never NULL
				 * at this point.  Note that if the target is a domain,
				 * coercing the base array type back up to the domain will
				 * happen within exec_assign_value.
				 */
				exec_assign_value(estate, target,
								  newarraydatum,
								  false,
								  arrayelem->arraytypoid,
								  arrayelem->arraytypmod);
				break;
			}

		default:
			elog(ERROR, "unrecognized dtype: %d", target->dtype);
	}
}

/*
 * exec_eval_datum				Get current value of a PLpgSQL_datum
 *
 * The type oid, typmod, value in Datum format, and null flag are returned.
 *
 * At present this doesn't handle PLpgSQL_expr or PLpgSQL_arrayelem datums;
 * that's not needed because we never pass references to such datums to SPI.
 *
 * NOTE: the returned Datum points right at the stored value in the case of
 * pass-by-reference datatypes.  Generally callers should take care not to
 * modify the stored value.  Some callers intentionally manipulate variables
 * referenced by R/W expanded pointers, though; it is those callers'
 * responsibility that the results are semantically OK.
 *
 * In some cases we have to palloc a return value, and in such cases we put
 * it into the estate's eval_mcontext.
 */
static void
exec_eval_datum(PLpgSQL_execstate *estate,
				PLpgSQL_datum *datum,
				Oid *typeid,
				int32 *typetypmod,
				Datum *value,
				bool *isnull)
{
	MemoryContext oldcontext;

	switch (datum->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
			{
				PLpgSQL_var *var = (PLpgSQL_var *) datum;

				*typeid = var->datatype->typoid;
				*typetypmod = var->datatype->atttypmod;
				*value = var->value;
				*isnull = var->isnull;
				break;
			}

		case PLPGSQL_DTYPE_ROW:
			{
				PLpgSQL_row *row = (PLpgSQL_row *) datum;
				HeapTuple	tup;

				/* We get here if there are multiple OUT parameters */
				if (!row->rowtupdesc)	/* should not happen */
					elog(ERROR, "row variable has no tupdesc");
				/* Make sure we have a valid type/typmod setting */
				BlessTupleDesc(row->rowtupdesc);
				oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
				tup = make_tuple_from_row(estate, row, row->rowtupdesc);
				if (tup == NULL)	/* should not happen */
					elog(ERROR, "row not compatible with its own tupdesc");
				*typeid = row->rowtupdesc->tdtypeid;
				*typetypmod = row->rowtupdesc->tdtypmod;
				*value = HeapTupleGetDatum(tup);
				*isnull = false;
				MemoryContextSwitchTo(oldcontext);
				break;
			}

		case PLPGSQL_DTYPE_REC:
			{
				PLpgSQL_rec *rec = (PLpgSQL_rec *) datum;

				if (rec->erh == NULL)
				{
					/* Treat uninstantiated record as a simple NULL */
					*value = (Datum) 0;
					*isnull = true;
					/* Report variable's declared type */
					*typeid = rec->rectypeid;
					*typetypmod = -1;
				}
				else
				{
					if (ExpandedRecordIsEmpty(rec->erh))
					{
						/* Empty record is also a NULL */
						*value = (Datum) 0;
						*isnull = true;
					}
					else
					{
						*value = ExpandedRecordGetDatum(rec->erh);
						*isnull = false;
					}
					if (rec->rectypeid != RECORDOID)
					{
						/* Report variable's declared type, if not RECORD */
						*typeid = rec->rectypeid;
						*typetypmod = -1;
					}
					else
					{
						/* Report record's actual type if declared RECORD */
						*typeid = rec->erh->er_typeid;
						*typetypmod = rec->erh->er_typmod;
					}
				}
				break;
			}

		case PLPGSQL_DTYPE_RECFIELD:
			{
				PLpgSQL_recfield *recfield = (PLpgSQL_recfield *) datum;
				PLpgSQL_rec *rec;
				ExpandedRecordHeader *erh;

				rec = (PLpgSQL_rec *) (estate->datums[recfield->recparentno]);
				erh = rec->erh;

				/*
				 * If record variable is NULL, instantiate it if it has a
				 * named composite type, else complain.  (This won't change
				 * the logical state of the record: it's still NULL.)
				 */
				if (erh == NULL)
				{
					instantiate_empty_record_variable(estate, rec);
					erh = rec->erh;
				}

				/*
				 * Look up the field's properties if we have not already, or
				 * if the tuple descriptor ID changed since last time.
				 */
				if (unlikely(recfield->rectupledescid != erh->er_tupdesc_id))
				{
					if (!expanded_record_lookup_field(erh,
													  recfield->fieldname,
													  &recfield->finfo))
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_COLUMN),
								 errmsg("record \"%s\" has no field \"%s\"",
										rec->refname, recfield->fieldname)));
					recfield->rectupledescid = erh->er_tupdesc_id;
				}

				/* Report type data. */
				*typeid = recfield->finfo.ftypeid;
				*typetypmod = recfield->finfo.ftypmod;

				/* And fetch the field value. */
				*value = expanded_record_get_field(erh,
												   recfield->finfo.fnumber,
												   isnull);
				break;
			}

		default:
			elog(ERROR, "unrecognized dtype: %d", datum->dtype);
	}
}

/*
 * plpgsql_exec_get_datum_type				Get datatype of a PLpgSQL_datum
 *
 * This is the same logic as in exec_eval_datum, but we skip acquiring
 * the actual value of the variable.  Also, needn't support DTYPE_ROW.
 */
Oid
plpgsql_exec_get_datum_type(PLpgSQL_execstate *estate,
							PLpgSQL_datum *datum)
{
	Oid			typeid;

	switch (datum->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
			{
				PLpgSQL_var *var = (PLpgSQL_var *) datum;

				typeid = var->datatype->typoid;
				break;
			}

		case PLPGSQL_DTYPE_REC:
			{
				PLpgSQL_rec *rec = (PLpgSQL_rec *) datum;

				if (rec->erh == NULL || rec->rectypeid != RECORDOID)
				{
					/* Report variable's declared type */
					typeid = rec->rectypeid;
				}
				else
				{
					/* Report record's actual type if declared RECORD */
					typeid = rec->erh->er_typeid;
				}
				break;
			}

		case PLPGSQL_DTYPE_RECFIELD:
			{
				PLpgSQL_recfield *recfield = (PLpgSQL_recfield *) datum;
				PLpgSQL_rec *rec;

				rec = (PLpgSQL_rec *) (estate->datums[recfield->recparentno]);

				/*
				 * If record variable is NULL, instantiate it if it has a
				 * named composite type, else complain.  (This won't change
				 * the logical state of the record: it's still NULL.)
				 */
				if (rec->erh == NULL)
					instantiate_empty_record_variable(estate, rec);

				/*
				 * Look up the field's properties if we have not already, or
				 * if the tuple descriptor ID changed since last time.
				 */
				if (unlikely(recfield->rectupledescid != rec->erh->er_tupdesc_id))
				{
					if (!expanded_record_lookup_field(rec->erh,
													  recfield->fieldname,
													  &recfield->finfo))
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_COLUMN),
								 errmsg("record \"%s\" has no field \"%s\"",
										rec->refname, recfield->fieldname)));
					recfield->rectupledescid = rec->erh->er_tupdesc_id;
				}

				typeid = recfield->finfo.ftypeid;
				break;
			}

		default:
			elog(ERROR, "unrecognized dtype: %d", datum->dtype);
			typeid = InvalidOid;	/* keep compiler quiet */
			break;
	}

	return typeid;
}

/*
 * plpgsql_exec_get_datum_type_info			Get datatype etc of a PLpgSQL_datum
 *
 * An extended version of plpgsql_exec_get_datum_type, which also retrieves the
 * typmod and collation of the datum.  Note however that we don't report the
 * possibly-mutable typmod of RECORD values, but say -1 always.
 */
void
plpgsql_exec_get_datum_type_info(PLpgSQL_execstate *estate,
								 PLpgSQL_datum *datum,
								 Oid *typeid, int32 *typmod, Oid *collation)
{
	switch (datum->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
			{
				PLpgSQL_var *var = (PLpgSQL_var *) datum;

				*typeid = var->datatype->typoid;
				*typmod = var->datatype->atttypmod;
				*collation = var->datatype->collation;
				break;
			}

		case PLPGSQL_DTYPE_REC:
			{
				PLpgSQL_rec *rec = (PLpgSQL_rec *) datum;

				if (rec->erh == NULL || rec->rectypeid != RECORDOID)
				{
					/* Report variable's declared type */
					*typeid = rec->rectypeid;
					*typmod = -1;
				}
				else
				{
					/* Report record's actual type if declared RECORD */
					*typeid = rec->erh->er_typeid;
					/* do NOT return the mutable typmod of a RECORD variable */
					*typmod = -1;
				}
				/* composite types are never collatable */
				*collation = InvalidOid;
				break;
			}

		case PLPGSQL_DTYPE_RECFIELD:
			{
				PLpgSQL_recfield *recfield = (PLpgSQL_recfield *) datum;
				PLpgSQL_rec *rec;

				rec = (PLpgSQL_rec *) (estate->datums[recfield->recparentno]);

				/*
				 * If record variable is NULL, instantiate it if it has a
				 * named composite type, else complain.  (This won't change
				 * the logical state of the record: it's still NULL.)
				 */
				if (rec->erh == NULL)
					instantiate_empty_record_variable(estate, rec);

				/*
				 * Look up the field's properties if we have not already, or
				 * if the tuple descriptor ID changed since last time.
				 */
				if (unlikely(recfield->rectupledescid != rec->erh->er_tupdesc_id))
				{
					if (!expanded_record_lookup_field(rec->erh,
													  recfield->fieldname,
													  &recfield->finfo))
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_COLUMN),
								 errmsg("record \"%s\" has no field \"%s\"",
										rec->refname, recfield->fieldname)));
					recfield->rectupledescid = rec->erh->er_tupdesc_id;
				}

				*typeid = recfield->finfo.ftypeid;
				*typmod = recfield->finfo.ftypmod;
				*collation = recfield->finfo.fcollation;
				break;
			}

		default:
			elog(ERROR, "unrecognized dtype: %d", datum->dtype);
			*typeid = InvalidOid;	/* keep compiler quiet */
			*typmod = -1;
			*collation = InvalidOid;
			break;
	}
}

/* ----------
 * exec_eval_integer		Evaluate an expression, coerce result to int4
 *
 * Note we do not do exec_eval_cleanup here; the caller must do it at
 * some later point.  (We do this because the caller may be holding the
 * results of other, pass-by-reference, expression evaluations, such as
 * an array value to be subscripted.)
 * ----------
 */
static int
exec_eval_integer(PLpgSQL_execstate *estate,
				  PLpgSQL_expr *expr,
				  bool *isNull)
{
	Datum		exprdatum;
	Oid			exprtypeid;
	int32		exprtypmod;

	exprdatum = exec_eval_expr(estate, expr, isNull, &exprtypeid, &exprtypmod);
	exprdatum = exec_cast_value(estate, exprdatum, isNull,
								exprtypeid, exprtypmod,
								INT4OID, -1);
	return DatumGetInt32(exprdatum);
}

/* ----------
 * exec_eval_boolean		Evaluate an expression, coerce result to bool
 *
 * Note we do not do exec_eval_cleanup here; the caller must do it at
 * some later point.
 * ----------
 */
static bool
exec_eval_boolean(PLpgSQL_execstate *estate,
				  PLpgSQL_expr *expr,
				  bool *isNull)
{
	Datum		exprdatum;
	Oid			exprtypeid;
	int32		exprtypmod;

	exprdatum = exec_eval_expr(estate, expr, isNull, &exprtypeid, &exprtypmod);
	exprdatum = exec_cast_value(estate, exprdatum, isNull,
								exprtypeid, exprtypmod,
								BOOLOID, -1);
	return DatumGetBool(exprdatum);
}

/* ----------
 * exec_eval_expr			Evaluate an expression and return
 *					the result Datum, along with data type/typmod.
 *
 * NOTE: caller must do exec_eval_cleanup when done with the Datum.
 * ----------
 */
static Datum
exec_eval_expr(PLpgSQL_execstate *estate,
			   PLpgSQL_expr *expr,
			   bool *isNull,
			   Oid *rettype,
			   int32 *rettypmod)
{
	Datum		result = 0;
	int			rc;
	Form_pg_attribute attr;

	/*
	 * If first time through, create a plan for this expression.
	 */
	if (expr->plan == NULL || estate->check_pullup)
		exec_prepare_plan(estate, expr, CURSOR_OPT_PARALLEL_OK);

	if (estate->check_pullup)
	{
		*isNull = true;
		return (Datum) 0;
	}

	/*
	 * If this is a simple expression, bypass SPI and use the executor
	 * directly
	 */
	if (exec_eval_simple_expr(estate, expr,
							  &result, isNull, rettype, rettypmod))
		return result;

	/*
	 * Else do it the hard way via exec_run_select
	 */
	rc = exec_run_select(estate, expr, 2, NULL);
	if (rc != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("query \"%s\" did not return data", expr->query)));

	/*
	 * Check that the expression returns exactly one column...
	 */
	if (estate->eval_tuptable->tupdesc->natts != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg_plural("query \"%s\" returned %d column",
							   "query \"%s\" returned %d columns",
							   estate->eval_tuptable->tupdesc->natts,
							   expr->query,
							   estate->eval_tuptable->tupdesc->natts)));

	/*
	 * ... and get the column's datatype.
	 */
	attr = TupleDescAttr(estate->eval_tuptable->tupdesc, 0);
	*rettype = attr->atttypid;
	*rettypmod = attr->atttypmod;

	/*
	 * If there are no rows selected, the result is a NULL of that type.
	 */
	if (estate->eval_processed == 0)
	{
		*isNull = true;
		return (Datum) 0;
	}

	/*
	 * Check that the expression returned no more than one row.
	 */
	if (estate->eval_processed != 1)
		ereport(ERROR,
				(errcode(ERRCODE_CARDINALITY_VIOLATION),
				 errmsg("query \"%s\" returned more than one row",
						expr->query)));

	/*
	 * Return the single result Datum.
	 */
	return SPI_getbinval(estate->eval_tuptable->vals[0],
						 estate->eval_tuptable->tupdesc, 1, isNull);
}


/* ----------
 * exec_run_select			Execute a select query
 * ----------
 */
static int
exec_run_select(PLpgSQL_execstate *estate,
				PLpgSQL_expr *expr, long maxtuples, Portal *portalP)
{
	ParamListInfo paramLI;
	int			rc;

	/*
	 * On the first call for this expression generate the plan.
	 *
	 * If we don't need to return a portal, then we're just going to execute
	 * the query immediately, which means it's OK to use a parallel plan, even
	 * if the number of rows being fetched is limited.  If we do need to
	 * return a portal (i.e., this is for a FOR loop), the user's code might
	 * invoke additional operations inside the FOR loop, making parallel query
	 * unsafe.  In any case, we don't expect any cursor operations to be done,
	 * so specify NO_SCROLL for efficiency and semantic safety.
	 */
	if (expr->plan == NULL)
	{
		int			cursorOptions = CURSOR_OPT_NO_SCROLL;

		if (portalP == NULL)
			cursorOptions |= CURSOR_OPT_PARALLEL_OK;
		exec_prepare_plan(estate, expr, cursorOptions);
	}

	if (estate->check_pullup)
		return SPI_OK_CONNECT;

	/*
	 * Set up ParamListInfo to pass to executor
	 */
	paramLI = setup_param_list(estate, expr);

	/*
	 * If a portal was requested, put the query and paramlist into the portal
	 */
	if (portalP != NULL)
	{
		*portalP = SPI_cursor_open_with_paramlist(NULL, expr->plan,
												  paramLI,
												  estate->readonly_func);
		if (*portalP == NULL)
			elog(ERROR, "could not open implicit cursor for query \"%s\": %s",
				 expr->query, SPI_result_code_string(SPI_result));
		exec_eval_cleanup(estate);
		return SPI_OK_CURSOR;
	}

	/*
	 * Execute the query
	 */
	rc = SPI_execute_plan_with_paramlist(expr->plan, paramLI,
										 estate->readonly_func, maxtuples);
	if (rc != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("query \"%s\" is not a SELECT", expr->query)));

	/* Save query results for eventual cleanup */
	Assert(estate->eval_tuptable == NULL);
	estate->eval_tuptable = SPI_tuptable;
	estate->eval_processed = SPI_processed;
	estate->eval_lastoid = SPI_lastoid;

	return rc;
}


/*
 * exec_for_query --- execute body of FOR loop for each row from a portal
 *
 * Used by exec_stmt_fors, exec_stmt_forc and exec_stmt_dynfors
 */
static int
exec_for_query(PLpgSQL_execstate *estate, PLpgSQL_stmt_forq *stmt,
			   Portal portal, bool prefetch_ok)
{
	PLpgSQL_variable *var;
	SPITupleTable *tuptab;
	bool		found = false;
	int			rc = PLPGSQL_RC_OK;
	uint64		previous_id = INVALID_TUPLEDESC_IDENTIFIER;
	bool		tupdescs_match = true;
	uint64		n;
	long		count;

    /* Fetch loop variable's datum entry */
	var = (PLpgSQL_variable *) estate->datums[stmt->var->dno];

    /*
     * Make sure the portal doesn't get closed by the user statements we
     * execute.
     */
	 PinPortal(portal);

	/*
	 * Fetch the initial tuple(s).  If prefetching is allowed then we grab a
	 * few more rows to avoid multiple trips through executor startup
	 * overhead.
	 */
	if (IsA(linitial(portal->stmts), PlannedStmt) &&
		(((PlannedStmt *) linitial(portal->stmts))->commandType != CMD_UTILITY) &&
		IsA(((PlannedStmt *) linitial(portal->stmts))->planTree, RemoteQuery))
		count = max_remote_query_fetch;
	else
		count = prefetch_ok ? 10 : 1;

	SPI_cursor_fetch(portal, true, count);
	tuptab = SPI_tuptable;
	n = SPI_processed;

	if (n == max_remote_query_fetch &&
		(IsA(linitial(portal->stmts), PlannedStmt) &&
		(((PlannedStmt *) linitial(portal->stmts))->commandType != CMD_UTILITY) &&
		IsA(((PlannedStmt *) linitial(portal->stmts))->planTree, RemoteQuery)))
		elog(ERROR, "Can fetch only %d tuples via RemoteQuery execution",
				max_remote_query_fetch);

	/*
	 * If the query didn't return any rows, set the target to NULL and fall
	 * through with found = false.
	 */
	if (n == 0)
	{
		exec_move_row(estate, var, NULL, tuptab->tupdesc);
		exec_eval_cleanup(estate);
	}
	else
		found = true;			/* processed at least one tuple */

	/*
	 * Now do the loop
	 */
	while (n > 0)
	{
		uint64		i;

		for (i = 0; i < n; i++)
		{
			/*
			 * Assign the tuple to the target.  Here, because we know that all
			 * loop iterations should be assigning the same tupdesc, we can
			 * optimize away repeated creations of expanded records with
			 * identical tupdescs.  Testing for changes of er_tupdesc_id is
			 * reliable even if the loop body contains assignments that
			 * replace the target's value entirely, because it's assigned from
			 * a process-global counter.  The case where the tupdescs don't
			 * match could possibly be handled more efficiently than this
			 * coding does, but it's not clear extra effort is worthwhile.
			 */
			if (var->dtype == PLPGSQL_DTYPE_REC)
			{
				PLpgSQL_rec *rec = (PLpgSQL_rec *) var;

				if (rec->erh &&
					rec->erh->er_tupdesc_id == previous_id &&
					tupdescs_match)
				{
					/* Only need to assign a new tuple value */
					expanded_record_set_tuple(rec->erh, tuptab->vals[i], true);
				}
				else
				{
					/*
					 * First time through, or var's tupdesc changed in loop,
					 * or we have to do it the hard way because type coercion
					 * is needed.
					 */
					exec_move_row(estate, var,
								  tuptab->vals[i], tuptab->tupdesc);

					/*
					 * Check to see if physical assignment is OK next time.
					 * Once the tupdesc comparison has failed once, we don't
					 * bother rechecking in subsequent loop iterations.
					 */
					if (tupdescs_match)
					{
						tupdescs_match =
							(rec->rectypeid == RECORDOID ||
							 rec->rectypeid == tuptab->tupdesc->tdtypeid ||
							 compatible_tupdescs(tuptab->tupdesc,
												 expanded_record_get_tupdesc(rec->erh)));
					}
					previous_id = rec->erh->er_tupdesc_id;
				}
			}
			else
				exec_move_row(estate, var, tuptab->vals[i], tuptab->tupdesc);

			exec_eval_cleanup(estate);

			/*
			 * Execute the statements
			 */
			rc = exec_stmts(estate, stmt->body);

			if (rc != PLPGSQL_RC_OK)
			{
				if (rc == PLPGSQL_RC_EXIT)
				{
					if (estate->exitlabel == NULL)
					{
						/* unlabelled exit, so exit the current loop */
						rc = PLPGSQL_RC_OK;
					}
					else if (stmt->label != NULL &&
							 strcmp(stmt->label, estate->exitlabel) == 0)
					{
						/* label matches this loop, so exit loop */
						estate->exitlabel = NULL;
						rc = PLPGSQL_RC_OK;
					}

					/*
					 * otherwise, we processed a labelled exit that does not
					 * match the current statement's label, if any; return
					 * RC_EXIT so that the EXIT continues to recurse upward.
					 */
				}
				else if (rc == PLPGSQL_RC_CONTINUE)
				{
					if (estate->exitlabel == NULL)
					{
						/* unlabelled continue, so re-run the current loop */
						rc = PLPGSQL_RC_OK;
						continue;
					}
					else if (stmt->label != NULL &&
							 strcmp(stmt->label, estate->exitlabel) == 0)
					{
						/* label matches this loop, so re-run loop */
						estate->exitlabel = NULL;
						rc = PLPGSQL_RC_OK;
						continue;
					}

					/*
					 * otherwise, we process a labelled continue that does not
					 * match the current statement's label, if any; return
					 * RC_CONTINUE so that the CONTINUE will propagate up the
					 * stack.
					 */
				}

				/*
				 * We're aborting the loop.  Need a goto to get out of two
				 * levels of loop...
				 */
				goto loop_exit;
			}
		}

		SPI_freetuptable(tuptab);

		/*
		 * Fetch more tuples.  If prefetching is allowed, grab 50 at a time.
		 */
		SPI_cursor_fetch(portal, true, prefetch_ok ? 50 : 1);
		tuptab = SPI_tuptable;
		n = SPI_processed;
	}

loop_exit:

	/*
	 * Release last group of tuples (if any)
	 */
	SPI_freetuptable(tuptab);

	UnpinPortal(portal);

	/*
	 * Set the FOUND variable to indicate the result of executing the loop
	 * (namely, whether we looped one or more times). This must be set last so
	 * that it does not interfere with the value of the FOUND variable inside
	 * the loop processing itself.
	 */
	exec_set_found(estate, found);

	return rc;
}


/* ----------
 * exec_eval_simple_expr -		Evaluate a simple expression returning
 *								a Datum by directly calling ExecEvalExpr().
 *
 * If successful, store results into *result, *isNull, *rettype, *rettypmod
 * and return TRUE.  If the expression cannot be handled by simple evaluation,
 * return FALSE.
 *
 * Because we only store one execution tree for a simple expression, we
 * can't handle recursion cases.  So, if we see the tree is already busy
 * with an evaluation in the current xact, we just return FALSE and let the
 * caller run the expression the hard way.  (Other alternatives such as
 * creating a new tree for a recursive call either introduce memory leaks,
 * or add enough bookkeeping to be doubtful wins anyway.)  Another case that
 * is covered by the expr_simple_in_use test is where a previous execution
 * of the tree was aborted by an error: the tree may contain bogus state
 * so we dare not re-use it.
 *
 * It is possible that we'd need to replan a simple expression; for example,
 * someone might redefine a SQL function that had been inlined into the simple
 * expression.  That cannot cause a simple expression to become non-simple (or
 * vice versa), but we do have to handle replacing the expression tree.
 *
 * Note: if pass-by-reference, the result is in the eval_mcontext.
 * It will be freed when exec_eval_cleanup is done.
 * ----------
 */
static bool
exec_eval_simple_expr(PLpgSQL_execstate *estate,
					  PLpgSQL_expr *expr,
					  Datum *result,
					  bool *isNull,
					  Oid *rettype,
					  int32 *rettypmod)
{
	ExprContext *econtext = estate->eval_econtext;
	LocalTransactionId curlxid = MyProc->lxid;
	ParamListInfo paramLI;
	void	   *save_setup_arg;
	bool		need_snapshot;
	MemoryContext oldcontext;

	if (!OidIsValid(expr->expr_simple_type))
		estate->paramLI->param_context = 0;

	/*
	 * Forget it if expression wasn't simple before.
	 */
	if (expr->expr_simple_expr == NULL)
		return false;

	/*
	 * If expression is in use in current xact, don't touch it.
	 */
	if (unlikely(expr->expr_simple_in_use) &&
		expr->expr_simple_lxid == curlxid)
		return false;

	/*
	 * Check to see if the cached plan has been invalidated.  If not, and this
	 * is the first use in the current transaction, save a plan refcount in
	 * the simple-expression resowner.
	 */
	if (likely(CachedPlanIsSimplyValid(expr->expr_simple_plansource,
									   expr->expr_simple_plan,
									   (expr->expr_simple_plan_lxid != curlxid ?
										estate->simple_eval_resowner : NULL))))
	{
		/*
		 * It's still good, so just remember that we have a refcount on the
		 * plan in the current transaction.  (If we already had one, this
		 * assignment is a no-op.)
		 */
		expr->expr_simple_plan_lxid = curlxid;
	}
	else
	{
		/* Need to replan */
		CachedPlan *cplan;

		/*
		 * If we have a valid refcount on some previous version of the plan,
		 * release it, so we don't leak plans intra-transaction.
		 */
		if (expr->expr_simple_plan_lxid == curlxid)
		{
			ReleaseCachedPlan(expr->expr_simple_plan,
							  estate->simple_eval_resowner);
			expr->expr_simple_plan = NULL;
			expr->expr_simple_plan_lxid = InvalidLocalTransactionId;
		}

		/* Do the replanning work in the eval_mcontext */
		oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
		cplan = SPI_plan_get_cached_plan(expr->plan);
		MemoryContextSwitchTo(oldcontext);

		/*
		 * We can't get a failure here, because the number of
		 * CachedPlanSources in the SPI plan can't change from what
		 * exec_simple_check_plan saw; it's a property of the raw parsetree
		 * generated from the query text.
		 */
		Assert(cplan != NULL);

		/*
		 * These tests probably can't fail either, but if they do, cope by
		 * declaring the plan to be non-simple.  On success, we'll acquire a
		 * refcount on the new plan, stored in simple_eval_resowner.
		 */
		if (CachedPlanAllowsSimpleValidityCheck(expr->expr_simple_plansource,
												cplan) &&
			CachedPlanIsSimplyValid(expr->expr_simple_plansource, cplan,
									estate->simple_eval_resowner))
		{
			/* Remember that we have the refcount */
			expr->expr_simple_plan = cplan;
			expr->expr_simple_plan_lxid = curlxid;
		}
		else
		{
			/* Release SPI_plan_get_cached_plan's refcount */
			ReleaseCachedPlan(cplan, CurrentResourceOwner);
			/* Mark expression as non-simple, and fail */
			expr->expr_simple_expr = NULL;
			return false;
		}

		/*
		 * SPI_plan_get_cached_plan acquired a plan refcount stored in the
		 * active resowner.  We don't need that anymore, so release it.
		 */
		ReleaseCachedPlan(cplan, CurrentResourceOwner);

		/* Extract desired scalar expression from cached plan */
		exec_save_simple_expr(expr, cplan);

		/* better recheck r/w safety, as it could change due to inlining */
		if (expr->rwparam >= 0)
			exec_check_rw_parameter(expr, expr->rwparam);
	}

	/*
	 * Pass back previously-determined result type.
	 */
	*rettype = expr->expr_simple_type;
	*rettypmod = expr->expr_simple_typmod;

	/*
	 * Set up ParamListInfo to pass to executor.  For safety, save and restore
	 * estate->paramLI->parserSetupArg around our use of the param list.
	 */
	paramLI = estate->paramLI;
	save_setup_arg = paramLI->parserSetupArg;

	/*
	 * We can skip using setup_param_list() in favor of just doing this
	 * unconditionally, because there's no need for the optimization of
	 * possibly setting ecxt_param_list_info to NULL; we've already forced use
	 * of a generic plan.
	 */
	paramLI->parserSetupArg = (void *) expr;
	econtext->ecxt_param_list_info = paramLI;

	/*
	 * Prepare the expression for execution, if it's not been done already in
	 * the current transaction.  (This will be forced to happen if we called
	 * exec_save_simple_expr above.)
	 */
	if (unlikely(expr->expr_simple_lxid != curlxid))
	{
		PlanState *planstate = NULL;

		oldcontext = MemoryContextSwitchTo(estate->simple_eval_estate->es_query_cxt);
		if (enable_scalar_subquery_cache)
		{
			planstate = (PlanState *) makeNode(PlanState);
			planstate->state = estate->simple_eval_estate;
		}

		expr->expr_simple_state =
			ExecInitExprWithParams(expr->expr_simple_expr,
								   econtext->ecxt_param_list_info);
		expr->expr_simple_in_use = false;
		expr->expr_simple_lxid = curlxid;
		MemoryContextSwitchTo(oldcontext);
	}

	/*
	 * We have to do some of the things SPI_execute_plan would do, in
	 * particular push a new snapshot so that stable functions within the
	 * expression can see updates made so far by our own function.  However,
	 * we can skip doing that (and just invoke the expression with the same
	 * snapshot passed to our function) in some cases, which is useful because
	 * it's quite expensive relative to the cost of a simple expression.  We
	 * can skip it if the expression contains no stable or volatile functions;
	 * immutable functions shouldn't need to see our updates.  Also, if this
	 * is a read-only function, we haven't made any updates so again it's okay
	 * to skip.
	 */
	oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
	need_snapshot = (expr->expr_simple_mutable && !estate->readonly_func);
	if (need_snapshot)
	{
		CommandCounterIncrement();
		PushActiveSnapshot(GetTransactionSnapshot());
	}

	/*
	 * Mark expression as busy for the duration of the ExecEvalExpr call.
	 */
	expr->expr_simple_in_use = true;

	/*
	 * Finally we can call the executor to evaluate the expression
	 */
	*result = ExecEvalExpr(expr->expr_simple_state,
						   econtext,
						   isNull);

	/* Assorted cleanup */
	expr->expr_simple_in_use = false;

	econtext->ecxt_param_list_info = NULL;

	paramLI->parserSetupArg = save_setup_arg;

	if (need_snapshot)
		PopActiveSnapshot();

	MemoryContextSwitchTo(oldcontext);

	/*
	 * That's it.
	 */
	return true;
}


/*
 * Create a ParamListInfo to pass to SPI
 *
 * We use a single ParamListInfo struct for all SPI calls made to evaluate
 * PLpgSQL_exprs in this estate.  It contains no per-param data, just hook
 * functions, so it's effectively read-only for SPI.
 *
 * An exception from pure read-only-ness is that the parserSetupArg points
 * to the specific PLpgSQL_expr being evaluated.  This is not an issue for
 * statement-level callers, but lower-level callers must save and restore
 * estate->paramLI->parserSetupArg just in case there's an active evaluation
 * at an outer call level.  (A plausible alternative design would be to
 * create a ParamListInfo struct for each PLpgSQL_expr, but for the moment
 * that seems like a waste of memory.)
 */
static ParamListInfo
setup_param_list(PLpgSQL_execstate *estate, PLpgSQL_expr *expr)
{
	ParamListInfo paramLI;

	/*
	 * We must have created the SPIPlan already (hence, query text has been
	 * parsed/analyzed at least once); else we cannot rely on expr->paramnos.
	 */
	Assert(expr->plan != NULL);

	/*
	 * We only need a ParamListInfo if the expression has parameters.  In
	 * principle we should test with bms_is_empty(), but we use a not-null
	 * test because it's faster.  In current usage bits are never removed from
	 * expr->paramnos, only added, so this test is correct anyway.
	 */
	if (expr->paramnos)
	{
		/* Use the common ParamListInfo */
		paramLI = estate->paramLI;

		/*
		 * Set up link to active expr where the hook functions can find it.
		 * Callers must save and restore parserSetupArg if there is any chance
		 * that they are interrupting an active use of parameters.
		 */
		paramLI->parserSetupArg = (void *) expr;

		/*
		 * Also make sure this is set before parser hooks need it.  There is
		 * no need to save and restore, since the value is always correct once
		 * set.  (Should be set already, but let's be sure.)
		 */
		expr->func = estate->func;
	}
	else
	{
		/*
		 * Expression requires no parameters.  Be sure we represent this case
		 * as a NULL ParamListInfo, so that plancache.c knows there is no
		 * point in a custom plan.
		 */
		paramLI = NULL;
	}
	return paramLI;
}

/*
 * plpgsql_param_fetch		paramFetch callback for dynamic parameter fetch
 *
 * We always use the caller's workspace to construct the returned struct.
 *
 * Note: this is no longer used during query execution.  It is used during
 * planning (with speculative == true) and when the ParamListInfo we supply
 * to the executor is copied into a cursor portal or transferred to a
 * parallel child process.
 */
static ParamExternData *
plpgsql_param_fetch(ParamListInfo params,
					int paramid, bool speculative,
					ParamExternData *prm)
{
	int			dno;
	PLpgSQL_execstate *estate;
	PLpgSQL_expr *expr;
	PLpgSQL_datum *datum;
	bool		ok = true;
	int32		prmtypmod;

	/* paramid's are 1-based, but dnos are 0-based */
	dno = paramid - 1;
	Assert(dno >= 0 && dno < params->numParams);

	/* fetch back the hook data */
	estate = (PLpgSQL_execstate *) params->paramFetchArg;
	expr = (PLpgSQL_expr *) params->parserSetupArg;
	Assert(params->numParams == estate->ndatums);

	/* now we can access the target datum */
	datum = estate->datums[dno];

	/*
	 * Since copyParamList() or SerializeParamList() will try to materialize
	 * every single parameter slot, it's important to return a dummy param
	 * when asked for a datum that's not supposed to be used by this SQL
	 * expression.  Otherwise we risk failures in exec_eval_datum(), or
	 * copying a lot more data than necessary.
	 */
	if (!bms_is_member(dno, expr->paramnos))
		ok = false;

	/*
	 * If the access is speculative, we prefer to return no data rather than
	 * to fail in exec_eval_datum().  Check the likely failure cases.
	 */
	else if (speculative)
	{
		switch (datum->dtype)
		{
			case PLPGSQL_DTYPE_VAR:
				/* always safe */
				break;

			case PLPGSQL_DTYPE_ROW:
				/* should be safe in all interesting cases */
				break;

			case PLPGSQL_DTYPE_REC:
				/* always safe (might return NULL, that's fine) */
				break;

			case PLPGSQL_DTYPE_RECFIELD:
				{
					PLpgSQL_recfield *recfield = (PLpgSQL_recfield *) datum;
					PLpgSQL_rec *rec;

					rec = (PLpgSQL_rec *) (estate->datums[recfield->recparentno]);

					/*
					 * If record variable is NULL, don't risk anything.
					 */
					if (rec->erh == NULL)
						ok = false;

					/*
					 * Look up the field's properties if we have not already,
					 * or if the tuple descriptor ID changed since last time.
					 */
					else if (unlikely(recfield->rectupledescid != rec->erh->er_tupdesc_id))
					{
						if (expanded_record_lookup_field(rec->erh,
														 recfield->fieldname,
														 &recfield->finfo))
							recfield->rectupledescid = rec->erh->er_tupdesc_id;
						else
							ok = false;
					}
					break;
				}

			default:
				ok = false;
				break;
		}
	}

	/* Return "no such parameter" if not ok */
	if (!ok)
	{
		prm->value = (Datum) 0;
		prm->isnull = true;
		prm->pflags = 0;
		prm->ptype = InvalidOid;
		return prm;
	}

	/* OK, evaluate the value and store into the return struct */
	exec_eval_datum(estate, datum,
					&prm->ptype, &prmtypmod,
					&prm->value, &prm->isnull);
	/* We can always mark params as "const" for executor's purposes */
	prm->pflags = PARAM_FLAG_CONST;

	/*
	 * If it's a read/write expanded datum, convert reference to read-only,
	 * unless it's safe to pass as read-write.
	 */
	if (dno != expr->rwparam)
	{
		if (datum->dtype == PLPGSQL_DTYPE_VAR)
			prm->value = MakeExpandedObjectReadOnly(prm->value,
													prm->isnull,
													((PLpgSQL_var *) datum)->datatype->typlen);
		else if (datum->dtype == PLPGSQL_DTYPE_REC)
			prm->value = MakeExpandedObjectReadOnly(prm->value,
													prm->isnull,
													-1);
	}

	return prm;
}

/*
 * plpgsql_param_compile		paramCompile callback for plpgsql parameters
 */
static void
plpgsql_param_compile(ParamListInfo params, Param *param,
					  ExprState *state,
					  Datum *resv, bool *resnull)
{
	PLpgSQL_execstate *estate;
	PLpgSQL_expr *expr;
	int			dno;
	PLpgSQL_datum *datum;
	ExprEvalStep scratch;

	/* fetch back the hook data */
	estate = (PLpgSQL_execstate *) params->paramFetchArg;
	expr = (PLpgSQL_expr *) params->parserSetupArg;

	/* paramid's are 1-based, but dnos are 0-based */
	dno = param->paramid - 1;
	Assert(dno >= 0 && dno < estate->ndatums);

	/* now we can access the target datum */
	datum = estate->datums[dno];

	scratch.opcode = EEOP_PARAM_CALLBACK;
	scratch.resvalue = resv;
	scratch.resnull = resnull;

	/*
	 * Select appropriate eval function.  It seems worth special-casing
	 * DTYPE_VAR and DTYPE_RECFIELD for performance.  Also, we can determine
	 * in advance whether MakeExpandedObjectReadOnly() will be required.
	 * Currently, only VAR and REC datums could contain read/write expanded
	 * objects.
	 */
	if (datum->dtype == PLPGSQL_DTYPE_VAR)
	{
		if (dno != expr->rwparam &&
			((PLpgSQL_var *) datum)->datatype->typlen == -1)
			scratch.d.cparam.paramfunc = plpgsql_param_eval_var_ro;
		else
			scratch.d.cparam.paramfunc = plpgsql_param_eval_var;
	}
	else if (datum->dtype == PLPGSQL_DTYPE_RECFIELD)
		scratch.d.cparam.paramfunc = plpgsql_param_eval_recfield;
	else if (datum->dtype == PLPGSQL_DTYPE_REC &&
			 dno != expr->rwparam)
		scratch.d.cparam.paramfunc = plpgsql_param_eval_generic_ro;
	else
		scratch.d.cparam.paramfunc = plpgsql_param_eval_generic;

	/*
	 * Note: it's tempting to use paramarg to store the estate pointer and
	 * thereby save an indirection or two in the eval functions.  But that
	 * doesn't work because the compiled expression might be used with
	 * different estates for the same PL/pgSQL function.
	 */
	scratch.d.cparam.paramarg = NULL;
	scratch.d.cparam.paramid = param->paramid;
	scratch.d.cparam.paramtype = param->paramtype;
	ExprEvalPushStep(state, &scratch);
}

/*
 * plpgsql_param_eval_var		evaluation of EEOP_PARAM_CALLBACK step
 *
 * This is specialized to the case of DTYPE_VAR variables for which
 * we do not need to invoke MakeExpandedObjectReadOnly.
 */
static void
plpgsql_param_eval_var(ExprState *state, ExprEvalStep *op,
					   ExprContext *econtext)
{
	ParamListInfo params;
	PLpgSQL_execstate *estate;
	int			dno = op->d.cparam.paramid - 1;
	PLpgSQL_var *var;

	/* fetch back the hook data */
	params = econtext->ecxt_param_list_info;
	estate = (PLpgSQL_execstate *) params->paramFetchArg;
	Assert(dno >= 0 && dno < estate->ndatums);

	/* now we can access the target datum */
	var = (PLpgSQL_var *) estate->datums[dno];
	Assert(var->dtype == PLPGSQL_DTYPE_VAR);

	/* inlined version of exec_eval_datum() */
	*op->resvalue = var->value;
	*op->resnull = var->isnull;

	/* safety check -- an assertion should be sufficient */
	Assert(var->datatype->typoid == op->d.cparam.paramtype);
}

/*
 * plpgsql_param_eval_var_ro		evaluation of EEOP_PARAM_CALLBACK step
 *
 * This is specialized to the case of DTYPE_VAR variables for which
 * we need to invoke MakeExpandedObjectReadOnly.
 */
static void
plpgsql_param_eval_var_ro(ExprState *state, ExprEvalStep *op,
						  ExprContext *econtext)
{
	ParamListInfo params;
	PLpgSQL_execstate *estate;
	int			dno = op->d.cparam.paramid - 1;
	PLpgSQL_var *var;

	/* fetch back the hook data */
	params = econtext->ecxt_param_list_info;
	estate = (PLpgSQL_execstate *) params->paramFetchArg;
	Assert(dno >= 0 && dno < estate->ndatums);

	/* now we can access the target datum */
	var = (PLpgSQL_var *) estate->datums[dno];
	Assert(var->dtype == PLPGSQL_DTYPE_VAR);

	/*
	 * Inlined version of exec_eval_datum() ... and while we're at it, force
	 * expanded datums to read-only.
	 */
	*op->resvalue = MakeExpandedObjectReadOnly(var->value,
											   var->isnull,
											   -1);
	*op->resnull = var->isnull;

	/* safety check -- an assertion should be sufficient */
	Assert(var->datatype->typoid == op->d.cparam.paramtype);
}

/*
 * plpgsql_param_eval_recfield		evaluation of EEOP_PARAM_CALLBACK step
 *
 * This is specialized to the case of DTYPE_RECFIELD variables, for which
 * we never need to invoke MakeExpandedObjectReadOnly.
 */
static void
plpgsql_param_eval_recfield(ExprState *state, ExprEvalStep *op,
							ExprContext *econtext)
{
	ParamListInfo params;
	PLpgSQL_execstate *estate;
	int			dno = op->d.cparam.paramid - 1;
	PLpgSQL_recfield *recfield;
	PLpgSQL_rec *rec;
	ExpandedRecordHeader *erh;

	/* fetch back the hook data */
	params = econtext->ecxt_param_list_info;
	estate = (PLpgSQL_execstate *) params->paramFetchArg;
	Assert(dno >= 0 && dno < estate->ndatums);

	/* now we can access the target datum */
	recfield = (PLpgSQL_recfield *) estate->datums[dno];
	Assert(recfield->dtype == PLPGSQL_DTYPE_RECFIELD);

	/* inline the relevant part of exec_eval_datum */
	rec = (PLpgSQL_rec *) (estate->datums[recfield->recparentno]);
	erh = rec->erh;

	/*
	 * If record variable is NULL, instantiate it if it has a named composite
	 * type, else complain.  (This won't change the logical state of the
	 * record: it's still NULL.)
	 */
	if (erh == NULL)
	{
		instantiate_empty_record_variable(estate, rec);
		erh = rec->erh;
	}

	/*
	 * Look up the field's properties if we have not already, or if the tuple
	 * descriptor ID changed since last time.
	 */
	if (unlikely(recfield->rectupledescid != erh->er_tupdesc_id))
	{
		if (!expanded_record_lookup_field(erh,
										  recfield->fieldname,
										  &recfield->finfo))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("record \"%s\" has no field \"%s\"",
							rec->refname, recfield->fieldname)));
		recfield->rectupledescid = erh->er_tupdesc_id;
	}

	/* OK to fetch the field value. */
	*op->resvalue = expanded_record_get_field(erh,
											  recfield->finfo.fnumber,
											  op->resnull);

	/* safety check -- needed for, eg, record fields */
	if (unlikely(recfield->finfo.ftypeid != op->d.cparam.paramtype))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("type of parameter %d (%s) does not match that when preparing the plan (%s)",
						op->d.cparam.paramid,
						format_type_be(recfield->finfo.ftypeid),
						format_type_be(op->d.cparam.paramtype))));
}

/*
 * plpgsql_param_eval_generic		evaluation of EEOP_PARAM_CALLBACK step
 *
 * This handles all variable types, but assumes we do not need to invoke
 * MakeExpandedObjectReadOnly.
 */
static void
plpgsql_param_eval_generic(ExprState *state, ExprEvalStep *op,
						   ExprContext *econtext)
{
	ParamListInfo params;
	PLpgSQL_execstate *estate;
	int			dno = op->d.cparam.paramid - 1;
	PLpgSQL_datum *datum;
	Oid			datumtype;
	int32		datumtypmod;

	/* fetch back the hook data */
	params = econtext->ecxt_param_list_info;
	estate = (PLpgSQL_execstate *) params->paramFetchArg;
	Assert(dno >= 0 && dno < estate->ndatums);

	/* now we can access the target datum */
	datum = estate->datums[dno];

	/* fetch datum's value */
	exec_eval_datum(estate, datum,
					&datumtype, &datumtypmod,
					op->resvalue, op->resnull);

	/* safety check -- needed for, eg, record fields */
	if (unlikely(datumtype != op->d.cparam.paramtype))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("type of parameter %d (%s) does not match that when preparing the plan (%s)",
						op->d.cparam.paramid,
						format_type_be(datumtype),
						format_type_be(op->d.cparam.paramtype))));
}

/*
 * plpgsql_param_eval_generic_ro	evaluation of EEOP_PARAM_CALLBACK step
 *
 * This handles all variable types, but assumes we need to invoke
 * MakeExpandedObjectReadOnly (hence, variable must be of a varlena type).
 */
static void
plpgsql_param_eval_generic_ro(ExprState *state, ExprEvalStep *op,
							  ExprContext *econtext)
{
	ParamListInfo params;
	PLpgSQL_execstate *estate;
	int			dno = op->d.cparam.paramid - 1;
	PLpgSQL_datum *datum;
	Oid			datumtype;
	int32		datumtypmod;

	/* fetch back the hook data */
	params = econtext->ecxt_param_list_info;
	estate = (PLpgSQL_execstate *) params->paramFetchArg;
	Assert(dno >= 0 && dno < estate->ndatums);

	/* now we can access the target datum */
	datum = estate->datums[dno];

	/* fetch datum's value */
	exec_eval_datum(estate, datum,
					&datumtype, &datumtypmod,
					op->resvalue, op->resnull);

	/* safety check -- needed for, eg, record fields */
	if (unlikely(datumtype != op->d.cparam.paramtype))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("type of parameter %d (%s) does not match that when preparing the plan (%s)",
						op->d.cparam.paramid,
						format_type_be(datumtype),
						format_type_be(op->d.cparam.paramtype))));

	/* force the value to read-only */
	*op->resvalue = MakeExpandedObjectReadOnly(*op->resvalue,
											   *op->resnull,
											   -1);
}


/*
 * exec_move_row			Move one tuple's values into a record or row
 *
 * tup and tupdesc may both be NULL if we're just assigning an indeterminate
 * composite NULL to the target.  Alternatively, can have tup be NULL and
 * tupdesc not NULL, in which case we assign a row of NULLs to the target.
 *
 * Since this uses the mcontext for workspace, caller should eventually call
 * exec_eval_cleanup to prevent long-term memory leaks.
 */
static void
exec_move_row(PLpgSQL_execstate *estate,
			  PLpgSQL_variable *target,
			  HeapTuple tup, TupleDesc tupdesc)
{
	ExpandedRecordHeader *newerh = NULL;

	/*
	 * If target is RECORD, we may be able to avoid field-by-field processing.
	 */
	if (target->dtype == PLPGSQL_DTYPE_REC)
	{
		PLpgSQL_rec *rec = (PLpgSQL_rec *) target;

		/*
		 * If we have no source tupdesc, just set the record variable to NULL.
		 * (If we have a source tupdesc but not a tuple, we'll set the
		 * variable to a row of nulls, instead.  This is odd perhaps, but
		 * backwards compatible.)
		 */
		if (tupdesc == NULL)
		{
			if (rec->erh)
				DeleteExpandedObject(ExpandedRecordGetDatum(rec->erh));
			rec->erh = NULL;
			return;
		}

		/*
		 * Build a new expanded record with appropriate tupdesc.
		 */
		newerh = make_expanded_record_for_rec(estate, rec, tupdesc, NULL);

		/*
		 * If the rowtypes match, or if we have no tuple anyway, we can
		 * complete the assignment without field-by-field processing.
		 *
		 * The tests here are ordered more or less in order of cheapness.  We
		 * can easily detect it will work if the target is declared RECORD or
		 * has the same typeid as the source.  But when assigning from a query
		 * result, it's common to have a source tupdesc that's labeled RECORD
		 * but is actually physically compatible with a named-composite-type
		 * target, so it's worth spending extra cycles to check for that.
		 */
		if (rec->rectypeid == RECORDOID ||
			rec->rectypeid == tupdesc->tdtypeid ||
			!HeapTupleIsValid(tup) ||
			compatible_tupdescs(tupdesc, expanded_record_get_tupdesc(newerh)))
		{
			if (!HeapTupleIsValid(tup))
			{
				/* No data, so force the record into all-nulls state */
				deconstruct_expanded_record(newerh);
			}
			else
			{
				/* No coercion is needed, so just assign the row value */
				expanded_record_set_tuple(newerh, tup, true);
			}

			/* Complete the assignment */
			assign_record_var(estate, rec, newerh);

			return;
		}
	}

	/*
	 * Otherwise, deconstruct the tuple and do field-by-field assignment,
	 * using exec_move_row_from_fields.
	 */
	if (tupdesc && HeapTupleIsValid(tup))
	{
		int			td_natts = tupdesc->natts;
		Datum	   *values;
		bool	   *nulls;
		Datum		values_local[64];
		bool		nulls_local[64];

		/*
		 * Need workspace arrays.  If td_natts is small enough, use local
		 * arrays to save doing a palloc.  Even if it's not small, we can
		 * allocate both the Datum and isnull arrays in one palloc chunk.
		 */
		if (td_natts <= lengthof(values_local))
		{
			values = values_local;
			nulls = nulls_local;
		}
		else
		{
			char	   *chunk;

			chunk = eval_mcontext_alloc(estate,
										td_natts * (sizeof(Datum) + sizeof(bool)));
			values = (Datum *) chunk;
			nulls = (bool *) (chunk + td_natts * sizeof(Datum));
		}

		heap_deform_tuple(tup, tupdesc, values, nulls);

		exec_move_row_from_fields(estate, target, newerh,
								  values, nulls, tupdesc);
	}
	else
	{
		/*
		 * Assign all-nulls.
		 */
		exec_move_row_from_fields(estate, target, newerh,
								  NULL, NULL, NULL);
	}
}

/*
 * Build an expanded record object suitable for assignment to "rec".
 *
 * Caller must supply either a source tuple descriptor or a source expanded
 * record (not both).  If the record variable has declared type RECORD,
 * it'll adopt the source's rowtype.  Even if it doesn't, we may be able to
 * piggyback on a source expanded record to save a typcache lookup.
 *
 * Caller must fill the object with data, then do assign_record_var().
 *
 * The new record is initially put into the mcontext, so it will be cleaned up
 * if we fail before reaching assign_record_var().
 */
static ExpandedRecordHeader *
make_expanded_record_for_rec(PLpgSQL_execstate *estate,
							 PLpgSQL_rec *rec,
							 TupleDesc srctupdesc,
							 ExpandedRecordHeader *srcerh)
{
	ExpandedRecordHeader *newerh;
	MemoryContext mcontext = get_eval_mcontext(estate);

	if (rec->rectypeid != RECORDOID)
	{
		/*
		 * New record must be of desired type, but maybe srcerh has already
		 * done all the same lookups.
		 */
		if (srcerh && rec->rectypeid == srcerh->er_decltypeid)
			newerh = make_expanded_record_from_exprecord(srcerh,
														 mcontext);
		else
			newerh = make_expanded_record_from_typeid(rec->rectypeid, -1,
													  mcontext);
	}
	else
	{
		/*
		 * We'll adopt the input tupdesc.  We can still use
		 * make_expanded_record_from_exprecord, if srcerh isn't a composite
		 * domain.  (If it is, we effectively adopt its base type.)
		 */
		if (srcerh && !ExpandedRecordIsDomain(srcerh))
			newerh = make_expanded_record_from_exprecord(srcerh,
														 mcontext);
		else
		{
			if (!srctupdesc)
				srctupdesc = expanded_record_get_tupdesc(srcerh);
			newerh = make_expanded_record_from_tupdesc(srctupdesc,
													   mcontext);
		}
	}

	return newerh;
}

/*
 * exec_move_row_from_fields	Move arrays of field values into a record or row
 *
 * When assigning to a record, the caller must have already created a suitable
 * new expanded record object, newerh.  Pass NULL when assigning to a row.
 *
 * tupdesc describes the input row, which might have different column
 * types and/or different dropped-column positions than the target.
 * values/nulls/tupdesc can all be NULL if we just want to assign nulls to
 * all fields of the record or row.
 *
 * Since this uses the mcontext for workspace, caller should eventually call
 * exec_eval_cleanup to prevent long-term memory leaks.
 */
static void
exec_move_row_from_fields(PLpgSQL_execstate *estate,
						  PLpgSQL_variable *target,
						  ExpandedRecordHeader *newerh,
						  Datum *values, bool *nulls,
						  TupleDesc tupdesc)
{
	int			td_natts = tupdesc ? tupdesc->natts : 0;
	int			fnum;
	int			anum;

	/* Handle RECORD-target case */
	if (target->dtype == PLPGSQL_DTYPE_REC)
	{
		PLpgSQL_rec *rec = (PLpgSQL_rec *) target;
		TupleDesc	var_tupdesc;
		Datum		newvalues_local[64];
		bool		newnulls_local[64];

		Assert(newerh != NULL); /* caller must have built new object */

		var_tupdesc = expanded_record_get_tupdesc(newerh);

		/*
		 * Coerce field values if needed.  This might involve dealing with
		 * different sets of dropped columns and/or coercing individual column
		 * types.  That's sort of a pain, but historically plpgsql has allowed
		 * it, so we preserve the behavior.  However, it's worth a quick check
		 * to see if the tupdescs are identical.  (Since expandedrecord.c
		 * prefers to use refcounted tupdescs from the typcache, expanded
		 * records with the same rowtype will have pointer-equal tupdescs.)
		 */
		if (var_tupdesc != tupdesc)
		{
			int			vtd_natts = var_tupdesc->natts;
			Datum	   *newvalues;
			bool	   *newnulls;

			/*
			 * Need workspace arrays.  If vtd_natts is small enough, use local
			 * arrays to save doing a palloc.  Even if it's not small, we can
			 * allocate both the Datum and isnull arrays in one palloc chunk.
			 */
			if (vtd_natts <= lengthof(newvalues_local))
			{
				newvalues = newvalues_local;
				newnulls = newnulls_local;
			}
			else
			{
				char	   *chunk;

				chunk = eval_mcontext_alloc(estate,
											vtd_natts * (sizeof(Datum) + sizeof(bool)));
				newvalues = (Datum *) chunk;
				newnulls = (bool *) (chunk + vtd_natts * sizeof(Datum));
			}

			/* Walk over destination columns */
			anum = 0;
			for (fnum = 0; fnum < vtd_natts; fnum++)
			{
				Form_pg_attribute attr = TupleDescAttr(var_tupdesc, fnum);
				Datum		value;
				bool		isnull;
				Oid			valtype;
				int32		valtypmod;

				if (attr->attisdropped)
				{
					/* expanded_record_set_fields should ignore this column */
					continue;	/* skip dropped column in record */
				}

				while (anum < td_natts &&
					   TupleDescAttr(tupdesc, anum)->attisdropped)
					anum++;		/* skip dropped column in tuple */

				if (anum < td_natts)
				{
					value = values[anum];
					isnull = nulls[anum];
					valtype = TupleDescAttr(tupdesc, anum)->atttypid;
					valtypmod = TupleDescAttr(tupdesc, anum)->atttypmod;
					anum++;
				}
				else
				{
					value = (Datum) 0;
					isnull = true;
					valtype = UNKNOWNOID;
					valtypmod = -1;
				}

				/* Cast the new value to the right type, if needed. */
				newvalues[fnum] = exec_cast_value(estate,
												  value,
												  &isnull,
												  valtype,
												  valtypmod,
												  attr->atttypid,
												  attr->atttypmod);
				newnulls[fnum] = isnull;
			}

			values = newvalues;
			nulls = newnulls;
		}

		/* Insert the coerced field values into the new expanded record */
		expanded_record_set_fields(newerh, values, nulls);

		/* Complete the assignment */
		assign_record_var(estate, rec, newerh);

		return;
	}

	/* newerh should not have been passed in non-RECORD cases */
	Assert(newerh == NULL);

	/*
	 * For a row, we assign the individual field values to the variables the
	 * row points to.
	 *
	 * NOTE: both this code and the record code above silently ignore extra
	 * columns in the source and assume NULL for missing columns.  This is
	 * pretty dubious but it's the historical behavior.
	 *
	 * If we have no input data at all, we'll assign NULL to all columns of
	 * the row variable.
	 */
	if (target->dtype == PLPGSQL_DTYPE_ROW)
	{
		PLpgSQL_row *row = (PLpgSQL_row *) target;

		anum = 0;
		for (fnum = 0; fnum < row->nfields; fnum++)
		{
			PLpgSQL_var *var;
			Datum		value;
			bool		isnull;
			Oid			valtype;
			int32		valtypmod;

			var = (PLpgSQL_var *) (estate->datums[row->varnos[fnum]]);

			while (anum < td_natts &&
				   TupleDescAttr(tupdesc, anum)->attisdropped)
				anum++;			/* skip dropped column in tuple */

			if (anum < td_natts)
			{
				value = values[anum];
				isnull = nulls[anum];
				valtype = TupleDescAttr(tupdesc, anum)->atttypid;
				valtypmod = TupleDescAttr(tupdesc, anum)->atttypmod;
				anum++;
			}
			else
			{
				value = (Datum) 0;
				isnull = true;
				valtype = UNKNOWNOID;
				valtypmod = -1;
			}

			exec_assign_value(estate, (PLpgSQL_datum *) var,
							  value, isnull, valtype, valtypmod);
		}

		return;
	}

	elog(ERROR, "unsupported target");
}

/*
 * compatible_tupdescs: detect whether two tupdescs are physically compatible
 *
 * TRUE indicates that a tuple satisfying src_tupdesc can be used directly as
 * a value for a composite variable using dst_tupdesc.
 */
static bool
compatible_tupdescs(TupleDesc src_tupdesc, TupleDesc dst_tupdesc)
{
	int			i;

	/* Possibly we could allow src_tupdesc to have extra columns? */
	if (dst_tupdesc->natts != src_tupdesc->natts)
		return false;

	for (i = 0; i < dst_tupdesc->natts; i++)
	{
		Form_pg_attribute dattr = TupleDescAttr(dst_tupdesc, i);
		Form_pg_attribute sattr = TupleDescAttr(src_tupdesc, i);

		if (dattr->attisdropped != sattr->attisdropped)
			return false;
		if (!dattr->attisdropped)
		{
			/* Normal columns must match by type and typmod */
			if (dattr->atttypid != sattr->atttypid ||
				(dattr->atttypmod >= 0 &&
				 dattr->atttypmod != sattr->atttypmod))
				return false;
		}
		else
		{
			/* Dropped columns are OK as long as length/alignment match */
			if (dattr->attlen != sattr->attlen ||
				dattr->attalign != sattr->attalign)
				return false;
		}
	}
	return true;
}

/* ----------
 * make_tuple_from_row		Make a tuple from the values of a row object
 *
 * A NULL return indicates rowtype mismatch; caller must raise suitable error
 *
 * The result tuple is freshly palloc'd in caller's context.  Some junk
 * may be left behind in eval_mcontext, too.
 * ----------
 */
static HeapTuple
make_tuple_from_row(PLpgSQL_execstate *estate,
					PLpgSQL_row *row,
					TupleDesc tupdesc)
{
	int			natts = tupdesc->natts;
	HeapTuple	tuple;
	Datum	   *dvalues;
	bool	   *nulls;
	int			i;

	if (natts != row->nfields)
		return NULL;

	dvalues = (Datum *) eval_mcontext_alloc0(estate, natts * sizeof(Datum));
	nulls = (bool *) eval_mcontext_alloc(estate, natts * sizeof(bool));

	for (i = 0; i < natts; i++)
	{
		Oid			fieldtypeid;
		int32		fieldtypmod;

		if (TupleDescAttr(tupdesc, i)->attisdropped)
		{
			nulls[i] = true;	/* leave the column as null */
			continue;
		}

		exec_eval_datum(estate, estate->datums[row->varnos[i]],
						&fieldtypeid, &fieldtypmod,
						&dvalues[i], &nulls[i]);
		if (fieldtypeid != TupleDescAttr(tupdesc, i)->atttypid)
			return NULL;
		/* XXX should we insist on typmod match, too? */
	}

	tuple = heap_form_tuple(tupdesc, dvalues, nulls);

	return tuple;
}

/*
 * deconstruct_composite_datum		extract tuple+tupdesc from composite Datum
 *
 * The caller must supply a HeapTupleData variable, in which we set up a
 * tuple header pointing to the composite datum's body.  To make the tuple
 * value outlive that variable, caller would need to apply heap_copytuple...
 * but current callers only need a short-lived tuple value anyway.
 *
 * Returns a pointer to the TupleDesc of the datum's rowtype.
 * Caller is responsible for calling ReleaseTupleDesc when done with it.
 *
 * Note: it's caller's responsibility to be sure value is of composite type.
 * Also, best to call this in a short-lived context, as it might leak memory.
 */
static TupleDesc
deconstruct_composite_datum(Datum value, HeapTupleData *tmptup)
{
	HeapTupleHeader td;
	Oid			tupType;
	int32		tupTypmod;

	/* Get tuple body (note this could involve detoasting) */
	td = DatumGetHeapTupleHeader(value);

	/* Build a temporary HeapTuple control structure */
	tmptup->t_len = HeapTupleHeaderGetDatumLength(td);
	ItemPointerSetInvalid(&(tmptup->t_self));
	tmptup->t_tableOid = InvalidOid;
	tmptup->t_data = td;

	/* Extract rowtype info and find a tupdesc */
	tupType = HeapTupleHeaderGetTypeId(td);
	tupTypmod = HeapTupleHeaderGetTypMod(td);
	return lookup_rowtype_tupdesc(tupType, tupTypmod);
}

/*
 * exec_move_row_from_datum		Move a composite Datum into a record or row
 *
 * This is equivalent to deconstruct_composite_datum() followed by
 * exec_move_row(), but we can optimize things if the Datum is an
 * expanded-record reference.
 *
 * Note: it's caller's responsibility to be sure value is of composite type.
 */
static void
exec_move_row_from_datum(PLpgSQL_execstate *estate,
						 PLpgSQL_variable *target,
						 Datum value)
{
	/* Check to see if source is an expanded record */
	if (VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(value)))
	{
		ExpandedRecordHeader *erh = (ExpandedRecordHeader *) DatumGetEOHP(value);
		ExpandedRecordHeader *newerh = NULL;

		Assert(erh->er_magic == ER_MAGIC);

		/* These cases apply if the target is record not row... */
		if (target->dtype == PLPGSQL_DTYPE_REC)
		{
			PLpgSQL_rec *rec = (PLpgSQL_rec *) target;

			/*
			 * If it's the same record already stored in the variable, do
			 * nothing.  This would happen only in silly cases like "r := r",
			 * but we need some check to avoid possibly freeing the variable's
			 * live value below.  Note that this applies even if what we have
			 * is a R/O pointer.
			 */
			if (erh == rec->erh)
				return;

			/*
			 * If we have a R/W pointer, we're allowed to just commandeer
			 * ownership of the expanded record.  If it's of the right type to
			 * put into the record variable, do that.  (Note we don't accept
			 * an expanded record of a composite-domain type as a RECORD
			 * value.  We'll treat it as the base composite type instead;
			 * compare logic in make_expanded_record_for_rec.)
			 */
			if (VARATT_IS_EXTERNAL_EXPANDED_RW(DatumGetPointer(value)) &&
				(rec->rectypeid == erh->er_decltypeid ||
				 (rec->rectypeid == RECORDOID &&
				  !ExpandedRecordIsDomain(erh))))
			{
				assign_record_var(estate, rec, erh);
				return;
			}

			/*
			 * If we already have an expanded record object in the target
			 * variable, and the source record contains a valid tuple
			 * representation with the right rowtype, then we can skip making
			 * a new expanded record and just assign the tuple with
			 * expanded_record_set_tuple.  (We can't do the equivalent if we
			 * have to do field-by-field assignment, since that wouldn't be
			 * atomic if there's an error.)  We consider that there's a
			 * rowtype match only if it's the same named composite type or
			 * same registered rowtype; checking for matches of anonymous
			 * rowtypes would be more expensive than this is worth.
			 */
			if (rec->erh &&
				(erh->flags & ER_FLAG_FVALUE_VALID) &&
				erh->er_typeid == rec->erh->er_typeid &&
				(erh->er_typeid != RECORDOID ||
				 (erh->er_typmod == rec->erh->er_typmod &&
				  erh->er_typmod >= 0)))
			{
				expanded_record_set_tuple(rec->erh, erh->fvalue, true);
				return;
			}

			/*
			 * Otherwise we're gonna need a new expanded record object.  Make
			 * it here in hopes of piggybacking on the source object's
			 * previous typcache lookup.
			 */
			newerh = make_expanded_record_for_rec(estate, rec, NULL, erh);

			/*
			 * If the expanded record contains a valid tuple representation,
			 * and we don't need rowtype conversion, then just copying the
			 * tuple is probably faster than field-by-field processing.  (This
			 * isn't duplicative of the previous check, since here we will
			 * catch the case where the record variable was previously empty.)
			 */
			if ((erh->flags & ER_FLAG_FVALUE_VALID) &&
				(rec->rectypeid == RECORDOID ||
				 rec->rectypeid == erh->er_typeid))
			{
				expanded_record_set_tuple(newerh, erh->fvalue, true);
				assign_record_var(estate, rec, newerh);
				return;
			}

			/*
			 * Need to special-case empty source record, else code below would
			 * leak newerh.
			 */
			if (ExpandedRecordIsEmpty(erh))
			{
				/* Set newerh to a row of NULLs */
				deconstruct_expanded_record(newerh);
				assign_record_var(estate, rec, newerh);
				return;
			}
		}						/* end of record-target-only cases */

		/*
		 * If the source expanded record is empty, we should treat that like a
		 * NULL tuple value.  (We're unlikely to see such a case, but we must
		 * check this; deconstruct_expanded_record would cause a change of
		 * logical state, which is not OK.)
		 */
		if (ExpandedRecordIsEmpty(erh))
		{
			exec_move_row(estate, target, NULL,
						  expanded_record_get_tupdesc(erh));
			return;
		}

		/*
		 * Otherwise, ensure that the source record is deconstructed, and
		 * assign from its field values.
		 */
		deconstruct_expanded_record(erh);
		exec_move_row_from_fields(estate, target, newerh,
								  erh->dvalues, erh->dnulls,
								  expanded_record_get_tupdesc(erh));
	}
	else
	{
		/*
		 * Nope, we've got a plain composite Datum.  Deconstruct it; but we
		 * don't use deconstruct_composite_datum(), because we may be able to
		 * skip calling lookup_rowtype_tupdesc().
		 */
		HeapTupleHeader td;
		HeapTupleData tmptup;
		Oid			tupType;
		int32		tupTypmod;
		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		/* Ensure that any detoasted data winds up in the eval_mcontext */
		oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
		/* Get tuple body (note this could involve detoasting) */
		td = DatumGetHeapTupleHeader(value);
		MemoryContextSwitchTo(oldcontext);

		/* Build a temporary HeapTuple control structure */
		tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
		ItemPointerSetInvalid(&(tmptup.t_self));
		tmptup.t_tableOid = InvalidOid;
		tmptup.t_data = td;

#ifdef PGXC
		tmptup.t_xc_node_id = 0;
#endif

		/* Extract rowtype info */
		tupType = HeapTupleHeaderGetTypeId(td);
		tupTypmod = HeapTupleHeaderGetTypMod(td);

		/* Now, if the target is record not row, maybe we can optimize ... */
		if (target->dtype == PLPGSQL_DTYPE_REC)
		{
			PLpgSQL_rec *rec = (PLpgSQL_rec *) target;

			/*
			 * If we already have an expanded record object in the target
			 * variable, and the source datum has a matching rowtype, then we
			 * can skip making a new expanded record and just assign the tuple
			 * with expanded_record_set_tuple.  We consider that there's a
			 * rowtype match only if it's the same named composite type or
			 * same registered rowtype.  (Checking to reject an anonymous
			 * rowtype here should be redundant, but let's be safe.)
			 */
			if (rec->erh &&
				tupType == rec->erh->er_typeid &&
				(tupType != RECORDOID ||
				 (tupTypmod == rec->erh->er_typmod &&
				  tupTypmod >= 0)))
			{
				expanded_record_set_tuple(rec->erh, &tmptup, true);
				return;
			}

			/*
			 * If the source datum has a rowtype compatible with the target
			 * variable, just build a new expanded record and assign the tuple
			 * into it.  Using make_expanded_record_from_typeid() here saves
			 * one typcache lookup compared to the code below.
			 */
			if (rec->rectypeid == RECORDOID || rec->rectypeid == tupType)
			{
				ExpandedRecordHeader *newerh;
				MemoryContext mcontext = get_eval_mcontext(estate);

				newerh = make_expanded_record_from_typeid(tupType, tupTypmod,
														  mcontext);
				expanded_record_set_tuple(newerh, &tmptup, true);
				assign_record_var(estate, rec, newerh);
				return;
			}

			/*
			 * Otherwise, we're going to need conversion, so fall through to
			 * do it the hard way.
			 */
		}

		/*
		 * ROW target, or unoptimizable RECORD target, so we have to expend a
		 * lookup to obtain the source datum's tupdesc.
		 */
		tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

		/* Do the move */
		exec_move_row(estate, target, &tmptup, tupdesc);

		/* Release tupdesc usage count */
		ReleaseTupleDesc(tupdesc);
	}
}

/*
 * If we have not created an expanded record to hold the record variable's
 * value, do so.  The expanded record will be "empty", so this does not
 * change the logical state of the record variable: it's still NULL.
 * However, now we'll have a tupdesc with which we can e.g. look up fields.
 */
static void
instantiate_empty_record_variable(PLpgSQL_execstate *estate, PLpgSQL_rec *rec)
{
	Assert(rec->erh == NULL);	/* else caller error */

	/* If declared type is RECORD, we can't instantiate */
	if (rec->rectypeid == RECORDOID)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("record \"%s\" is not assigned yet", rec->refname),
				 errdetail("The tuple structure of a not-yet-assigned record is indeterminate.")));

	/* OK, do it */
	rec->erh = make_expanded_record_from_typeid(rec->rectypeid, -1,
												estate->datum_context);
}

/* ----------
 * convert_value_to_string			Convert a non-null Datum to C string
 *
 * Note: the result is in the estate's eval_mcontext, and will be cleared
 * by the next exec_eval_cleanup() call.  The invoked output function might
 * leave additional cruft there as well, so just pfree'ing the result string
 * would not be enough to avoid memory leaks if we did not do it like this.
 * In most usages the Datum being passed in is also in that context (if
 * pass-by-reference) and so an exec_eval_cleanup() call is needed anyway.
 *
 * Note: not caching the conversion function lookup is bad for performance.
 * However, this function isn't currently used in any places where an extra
 * catalog lookup or two seems like a big deal.
 * ----------
 */
static char *
convert_value_to_string(PLpgSQL_execstate *estate, Datum value, Oid valtype)
{
	char	   *result;
	MemoryContext oldcontext;
	Oid			typoutput;
	bool		typIsVarlena;

	oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
	getTypeOutputInfo(valtype, &typoutput, &typIsVarlena);
	result = OidOutputFunctionCall(typoutput, value);
	MemoryContextSwitchTo(oldcontext);

	return result;
}

/* ----------
 * exec_cast_value			Cast a value if required
 *
 * Note that *isnull is an input and also an output parameter.  While it's
 * unlikely that a cast operation would produce null from non-null or vice
 * versa, that could happen in principle.
 *
 * Note: the estate's eval_mcontext is used for temporary storage, and may
 * also contain the result Datum if we have to do a conversion to a pass-
 * by-reference data type.  Be sure to do an exec_eval_cleanup() call when
 * done with the result.
 * ----------
 */
static inline Datum
exec_cast_value(PLpgSQL_execstate *estate,
				Datum value, bool *isnull,
				Oid valtype, int32 valtypmod,
				Oid reqtype, int32 reqtypmod)
{
	if (estate->check_pullup)
		return Int32GetDatum(0);
	
	/*
	 * If the type of the given value isn't what's requested, convert it.
	 */
	if (valtype != reqtype ||
		(valtypmod != reqtypmod && reqtypmod != -1))
	{
		/* We keep the slow path out-of-line. */
		value = do_cast_value(estate, value, isnull, valtype, valtypmod,
							  reqtype, reqtypmod);
	}

	return value;
}

/* ----------
 * do_cast_value			Slow path for exec_cast_value.
 * ----------
 */
static Datum
do_cast_value(PLpgSQL_execstate *estate,
			  Datum value, bool *isnull,
			  Oid valtype, int32 valtypmod,
			  Oid reqtype, int32 reqtypmod)
{
	plpgsql_CastHashEntry *cast_entry;

	cast_entry = get_cast_hashentry(estate,
									valtype, valtypmod,
									reqtype, reqtypmod);
	if (cast_entry)
	{
		ExprContext *econtext = estate->eval_econtext;
		MemoryContext oldcontext;

		oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));

		econtext->caseValue_datum = value;
		econtext->caseValue_isNull = *isnull;

		cast_entry->cast_in_use = true;

		value = ExecEvalExpr(cast_entry->cast_exprstate, econtext,
							 isnull);

		cast_entry->cast_in_use = false;

		MemoryContextSwitchTo(oldcontext);
	}

	return value;
}

/* ----------
 * get_cast_hashentry			Look up how to perform a type cast
 *
 * Returns a plpgsql_CastHashEntry if an expression has to be evaluated,
 * or NULL if the cast is a mere no-op relabeling.  If there's work to be
 * done, the cast_exprstate field contains an expression evaluation tree
 * based on a CaseTestExpr input, and the cast_in_use field should be set
 * TRUE while executing it.
 * ----------
 */
static plpgsql_CastHashEntry *
get_cast_hashentry(PLpgSQL_execstate *estate,
				   Oid srctype, int32 srctypmod,
				   Oid dsttype, int32 dsttypmod)
{
	plpgsql_CastHashKey cast_key;
	plpgsql_CastHashEntry *cast_entry;
	bool		found;
	LocalTransactionId curlxid;
	MemoryContext oldcontext;

	/* Look for existing entry */
	cast_key.srctype = srctype;
	cast_key.dsttype = dsttype;
	cast_key.srctypmod = srctypmod;
	cast_key.dsttypmod = dsttypmod;
	cast_entry = (plpgsql_CastHashEntry *) hash_search(estate->cast_hash,
													   (void *) &cast_key,
													   HASH_FIND, NULL);

	if (cast_entry == NULL)
	{
		/* We've not looked up this coercion before */
		Node	   *cast_expr;
		CaseTestExpr *placeholder;

		/*
		 * Since we could easily fail (no such coercion), construct a
		 * temporary coercion expression tree in the short-lived
		 * eval_mcontext, then if successful copy it to cast_hash_context.
		 */
		oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));

		/*
		 * We use a CaseTestExpr as the base of the coercion tree, since it's
		 * very cheap to insert the source value for that.
		 */
		placeholder = makeNode(CaseTestExpr);
		placeholder->typeId = srctype;
		placeholder->typeMod = srctypmod;
		placeholder->collation = get_typcollation(srctype);

		/*
		 * Apply coercion.  We use ASSIGNMENT coercion because that's the
		 * closest match to plpgsql's historical behavior; in particular,
		 * EXPLICIT coercion would allow silent truncation to a destination
		 * varchar/bpchar's length, which we do not want.
		 *
		 * If source type is UNKNOWN, coerce_to_target_type will fail (it only
		 * expects to see that for Const input nodes), so don't call it; we'll
		 * apply CoerceViaIO instead.  Likewise, it doesn't currently work for
		 * coercing RECORD to some other type, so skip for that too.
		 */
		if (srctype == UNKNOWNOID || srctype == RECORDOID)
			cast_expr = NULL;
		else
			cast_expr = coerce_to_target_type(NULL,
											  (Node *) placeholder, srctype,
											  dsttype, dsttypmod,
											  COERCION_ASSIGNMENT,
											  COERCE_IMPLICIT_CAST,
											  -1);

		/*
		 * If there's no cast path according to the parser, fall back to using
		 * an I/O coercion; this is semantically dubious but matches plpgsql's
		 * historical behavior.  We would need something of the sort for
		 * UNKNOWN literals in any case.
		 */
		if (cast_expr == NULL)
		{
			CoerceViaIO *iocoerce = makeNode(CoerceViaIO);

			iocoerce->arg = (Expr *) placeholder;
			iocoerce->resulttype = dsttype;
			iocoerce->resulttypmod = dsttypmod;
			iocoerce->resultcollid = InvalidOid;
			iocoerce->coerceformat = COERCE_IMPLICIT_CAST;
			iocoerce->location = -1;
			cast_expr = (Node *) iocoerce;
			if (dsttypmod != -1)
				cast_expr = coerce_to_target_type(NULL,
												  cast_expr, dsttype,
												  dsttype, dsttypmod,
												  COERCION_ASSIGNMENT,
												  COERCE_IMPLICIT_CAST,
												  -1);
		}

		/* Note: we don't bother labeling the expression tree with collation */

		/* Detect whether we have a no-op (RelabelType) coercion */
		if (IsA(cast_expr, RelabelType) &&
			((RelabelType *) cast_expr)->arg == (Expr *) placeholder)
			cast_expr = NULL;

		if (cast_expr)
		{
			/* ExecInitExpr assumes we've planned the expression */
			cast_expr = (Node *) expression_planner((Expr *) cast_expr);

			/* Now copy the tree into cast_hash_context */
			MemoryContextSwitchTo(estate->cast_hash_context);

			cast_expr = copyObject(cast_expr);
		}

		MemoryContextSwitchTo(oldcontext);

		/* Now we can fill in a hashtable entry. */
		cast_entry = (plpgsql_CastHashEntry *) hash_search(estate->cast_hash,
														   (void *) &cast_key,
														   HASH_ENTER, &found);
		Assert(!found);			/* wasn't there a moment ago */
		cast_entry->cast_expr = (Expr *) cast_expr;
		cast_entry->cast_exprstate = NULL;
		cast_entry->cast_in_use = false;
		cast_entry->cast_lxid = InvalidLocalTransactionId;
		cast_entry->eval_expr_ctx = AllocSetContextCreate(estate->cast_hash_context,
												"PLpgSQL cast info",
												ALLOCSET_DEFAULT_SIZES);
	}

	/* Done if we have determined that this is a no-op cast. */
	if (cast_entry->cast_expr == NULL)
		return NULL;

	/*
	 * Prepare the expression for execution, if it's not been done already in
	 * the current transaction; also, if it's marked busy in the current
	 * transaction, abandon that expression tree and build a new one, so as to
	 * avoid potential problems with recursive cast expressions and failed
	 * executions.  (We will leak some memory intra-transaction if that
	 * happens a lot, but we don't expect it to.)  It's okay to update the
	 * hash table with the new tree because all plpgsql functions within a
	 * given transaction share the same simple_eval_estate.  (Well, regular
	 * functions do; DO blocks have private simple_eval_estates, and private
	 * cast hash tables to go with them.)
	 */
	curlxid = MyProc->lxid;
	if (cast_entry->cast_lxid != curlxid || cast_entry->cast_in_use)
	{
		Assert(cast_entry->eval_expr_ctx != NULL);
		MemoryContextReset(cast_entry->eval_expr_ctx);
		oldcontext = MemoryContextSwitchTo(cast_entry->eval_expr_ctx);
		cast_entry->cast_exprstate = ExecInitExpr(cast_entry->cast_expr, NULL);
		cast_entry->cast_in_use = false;
		cast_entry->cast_lxid = curlxid;
		MemoryContextSwitchTo(oldcontext);
	}

	return cast_entry;
}

/*
 * Check if a record field is the distribution key of target relation.
 *
 * Used to check trigger pull up or push down.
 */
static bool
exec_check_rec_distkey(PLpgSQL_execstate *estate,
					   PLpgSQL_recfield *recfield,
					   Oid diskey_type)
{
	PLpgSQL_rec *rec;
	int64        tgtype = estate->check_tgtype;

	/* parent means record like NEW or OLD */
	rec = (PLpgSQL_rec *) estate->datums[recfield->recparentno];
	if (rec->dtype == PLPGSQL_DTYPE_REC && rec->refname)
	{
		/* if true, it points to a target relation record */
		if ((downcase_ora_ident_strcmp(rec->refname, "new") == 0 && !TRIGGER_FOR_DELETE(tgtype)) ||
			(downcase_ora_ident_strcmp(rec->refname, "old") == 0 && !TRIGGER_FOR_INSERT(tgtype)))
		{
			Relation         target = relation_open(estate->check_target_oid, NoLock);
			RelationLocInfo *target_loc_info = RelationGetLocInfo(target);
			Oid              target_diskey_type;
			AttrNumber		firstAttrNum;
			char           *firstAttrName;

			if (target_loc_info->nDisAttrs == 1)
			{
				/* must protected this earlier */
				Assert((target_loc_info) && (target_loc_info->locatorType == LOCATOR_TYPE_SHARD));
				firstAttrNum = target_loc_info->disAttrNums[0];
				firstAttrName = get_attname(RelationGetRelid(target), firstAttrNum);
				target_diskey_type =
						RelationGetDescr(target)->attrs[firstAttrNum - 1].atttypid;

				if ((target_diskey_type == diskey_type || !OidIsValid(diskey_type)) &&
					strcmp(recfield->fieldname, firstAttrName) == 0)
				{
					/* same type, same name, it is distribution key of target */
					relation_close(target, NoLock);
					return true;
				}
			}

			relation_close(target, NoLock);
		}
	}

	return false;
}

/*
 * Check if a param expression field is the distribution key of target relation.
 *
 * Used to check trigger pull up or push down.
 */
static bool
exec_check_expr_pushdown(PLpgSQL_execstate *estate, Node *expr, Oid diskey_type)
{
	Param       *param;
	PLpgSQL_recfield *recfield;

	if (IsA(expr, FuncExpr))
	{
		FuncExpr    *fexpr = castNode(FuncExpr, expr);
		if (fexpr->funcid == 4564 ||    /* varchar2 */
			fexpr->funcid == 669)       /* varchar */
		{
			Node  *first_arg = linitial(fexpr->args);
			Const *explicit_c = (Const *) list_nth(fexpr->args, 2);
			bool   explicit = DatumGetBool(explicit_c->constvalue);

			if (IsA(first_arg, Param) && !explicit)
			{
				expr = first_arg;
			}
		}
	}

	if (!IsA(expr, Param))
		return false;

	param = castNode(Param, expr);

	recfield = (PLpgSQL_recfield *) estate->datums[param->paramid-1];

	if (recfield->dtype != PLPGSQL_DTYPE_RECFIELD)
		return false;

	return exec_check_rec_distkey(estate, recfield, diskey_type);
}


/* ----------
 * exec_simple_check_plan -		Check if a plan is simple enough to
 *								be evaluated by ExecEvalExpr() instead
 *								of SPI.
 * ----------
 */
static void
exec_simple_check_plan(PLpgSQL_execstate *estate, PLpgSQL_expr *expr)
{
	List	   *plansources;
	CachedPlanSource *plansource;
	Query	   *query;
	CachedPlan *cplan;
	MemoryContext oldcontext;

	/*
	 * Initialize to "not simple".
	 */
	expr->expr_simple_expr = NULL;

	/*
	 * Check the analyzed-and-rewritten form of the query to see if we will be
	 * able to treat it as a simple expression.  Since this function is only
	 * called immediately after creating the CachedPlanSource, we need not
	 * worry about the query being stale.
	 */

	/*
	 * We can only test queries that resulted in exactly one CachedPlanSource
	 */
	plansources = SPI_plan_get_plan_sources(expr->plan);
	if (list_length(plansources) != 1)
		return;
	plansource = (CachedPlanSource *) linitial(plansources);

	/*
	 * 1. There must be one single querytree.
	 */
	if (list_length(plansource->query_list) != 1)
		return;
	query = (Query *) linitial(plansource->query_list);

	/*
	 * 2. It must be a plain SELECT query without any input tables
	 */
	if (!IsA(query, Query))
		return;

	if (estate->check_pullup)
	{
		ListCell    *l;

		if (*estate->check_pullup)
			return;

		if (query->commandType == CMD_UTILITY)
		{
			/* only SET operation is allowed */
			if (query->utilityStmt && IsA(query->utilityStmt, VariableSetStmt))
				return;

			*estate->check_pullup = true;
			return;
		}

		/* traverse targetlist to find if there are un-pushable functions */
		foreach(l, query->targetList)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, l);
			if (IsA(tle->expr, FuncExpr))
			{
				FuncExpr *fe = castNode(FuncExpr, tle->expr);

				*estate->check_pullup =
						func_is_pullup(fe->funcid, NULL) ||
						contain_opentenbaseLO_functions_checker(fe->funcid, NULL, NULL);

				if (*estate->check_pullup)
					return;
			}
		}

		if (list_length(query->rtable) > 1 ||
			query->hasSubLinks)
		{
			*estate->check_pullup = true;
			return;
		}
		else if (list_length(query->rtable) == 1)
		{
			RangeTblEntry   *rte = linitial_node(RangeTblEntry, query->rtable);
			Relation         rel;
			RelationLocInfo *rel_loc_info;
			AttrNumber       diskey;
			char             loctype;
			Oid              diskey_type;
			int              nDisAttrs;

			if (rte->rtekind != RTE_RELATION)
			{
				/* trigger on a view must be pulled up */
				*estate->check_pullup = true;
				return;
			}

			rel = relation_open(rte->relid, NoLock);

			/* table has foreign key */
			if (RelationGetFKeyList(rel) != NIL &&
				(query->commandType == CMD_INSERT ||
				 query->commandType == CMD_UPDATE ||
				 query->commandType == CMD_MERGE))
			{
				relation_close(rel, NoLock);
				*estate->check_pullup = true;
				return;
			}

			/* table has pullup trigger for current command */
			if (rel->trigdesc && query->commandType != CMD_SELECT &&
				(rel->trigdesc->trig_pullup & pgxc_get_trigevent(query->commandType)) != 0)
			{
				relation_close(rel, NoLock);
				*estate->check_pullup = true;
				return;
			}

			rel_loc_info = RelationGetLocInfo(rel);
			if (rel_loc_info == NULL)
			{
				/* system table or cold-hot table */
				relation_close(rel, NoLock);
				*estate->check_pullup = true;
				return;
			}

			if (IsRelationReplicated(rel_loc_info))
			{
				relation_close(rel, NoLock);

				*estate->check_pullup = (query->commandType != CMD_SELECT);
				return;
			}

			nDisAttrs = rel_loc_info->nDisAttrs;
			diskey = rel_loc_info->disAttrNums[0];
			loctype = rel_loc_info->locatorType;
			diskey_type = RelationGetDescr(rel)->attrs[diskey-1].atttypid;
			relation_close(rel, NoLock);

			if (nDisAttrs > 1)
			{
				*estate->check_pullup = true;
			}
			else if (IsLocatorDistributedByValue(loctype))
			{
				/*
				 * if it's an INSERT command, make sure it's been like
				 * "INSERT INTO tbl(tbl_distkey, ...) values(new.tg_target_distkey, ...)" or
				 * "INSERT INTO tbl(tbl_distkey, ...) values(old.tg_target_distkey, ...)".
				 * And they are same type.
				 */
				if (query->commandType == CMD_INSERT)
				{
					/* traverse target list to find if there is a distkey */
					foreach(l, query->targetList)
					{
						TargetEntry *tle = lfirst_node(TargetEntry, l);
						if (tle->resno == diskey) /* we got one */
						{
							/*
							 * make sure it's a param of only new/old.dist_key,
							 * not a const or expression like "dist_key + 1"
							 */
							if (!exec_check_expr_pushdown(estate, (Node *) tle->expr, diskey_type))
							{
								*estate->check_pullup = true;
							}

							break;
						}
					}
				}
				else if (query->jointree)
				{
					Expr *distcol_expr = pgxc_find_distcol_expr(1, diskey,
																query->jointree->quals, InvalidOid);
					if (distcol_expr == NULL ||
						!exec_check_expr_pushdown(estate, (Node *) distcol_expr, diskey_type))
					{
						*estate->check_pullup = true;
					}
				}
				else
				{
					*estate->check_pullup = true;
				}
			}
			else if (query->commandType != CMD_SELECT)
			{
				*estate->check_pullup = true;
			}
		}

		return;
	}

	if (query->commandType != CMD_SELECT)
		return;
	if (query->rtable != NIL)
		return;

	/*
	 * 3. Can't have any subplans, aggregates, qual clauses either.  (These
	 * tests should generally match what inline_function() checks before
	 * inlining a SQL function; otherwise, inlining could change our
	 * conclusion about whether an expression is simple, which we don't want.)
	 */
	if (query->hasAggs ||
		query->hasWindowFuncs ||
		query->hasTargetSRFs ||
		query->hasSubLinks ||
		query->cteList ||
		query->jointree->fromlist ||
		query->jointree->quals ||
		query->groupClause ||
		query->groupingSets ||
		query->havingQual ||
		query->windowClause ||
		query->distinctClause ||
		query->sortClause ||
		query->limitOffset ||
		query->limitCount ||
		query->setOperations)
		return;

	/*
	 * 4. The query must have a single attribute as result
	 */
	if (list_length(query->targetList) != 1)
		return;

	/*
	 * OK, we can treat it as a simple plan.
	 *
	 * Get the generic plan for the query.  If replanning is needed, do that
	 * work in the eval_mcontext.
	 */
	oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));

	/* Add for pg_hint_plan instead of debug_query_string. */
	if (expr->query != NULL)
		spi_query_string = pstrdup(expr->query);
	else
		spi_query_string = NULL;

	PG_TRY();
	{
		cplan = SPI_plan_get_cached_plan(expr->plan);
	}
	PG_CATCH();
	{
		spi_query_string = NULL;
		PG_RE_THROW();
	}
	PG_END_TRY();

	spi_query_string = NULL;
	MemoryContextSwitchTo(oldcontext);

	/* Can't fail, because we checked for a single CachedPlanSource above */
	Assert(cplan != NULL);

	/*
	 * Verify that plancache.c thinks the plan is simple enough to use
	 * CachedPlanIsSimplyValid.  Given the restrictions above, it's unlikely
	 * that this could fail, but if it does, just treat plan as not simple.
	 */
	if (CachedPlanAllowsSimpleValidityCheck(plansource, cplan))
	{
		/*
		 * OK, use CachedPlanIsSimplyValid to save a refcount on the plan in
		 * the simple-expression resowner.  This shouldn't fail either, but if
		 * somehow it does, again we can cope by treating plan as not simple.
		 */
		if (CachedPlanIsSimplyValid(plansource, cplan,
									estate->simple_eval_resowner))
		{
			/* Remember that we have the refcount */
			expr->expr_simple_plansource = plansource;
			expr->expr_simple_plan = cplan;
			expr->expr_simple_plan_lxid = MyProc->lxid;

			/* Share the remaining work with the replan code path */
			exec_save_simple_expr(expr, cplan);
		}
	}

	/*
	 * Release the plan refcount obtained by SPI_plan_get_cached_plan.  (This
	 * refcount is held by the wrong resowner, so we can't just repurpose it.)
	 */
	ReleaseCachedPlan(cplan, CurrentResourceOwner);
}

/*
 * exec_save_simple_expr --- extract simple expression from CachedPlan
 */
static void
exec_save_simple_expr(PLpgSQL_expr *expr, CachedPlan *cplan)
{
	PlannedStmt *stmt;
	Plan	   *plan;
	Expr	   *tle_expr;

	/*
	 * Given the checks that exec_simple_check_plan did, none of the Asserts
	 * here should ever fail.
	 */

	/* Extract the single PlannedStmt */
	Assert(list_length(cplan->stmt_list) == 1);
	stmt = linitial_node(PlannedStmt, cplan->stmt_list);
	Assert(stmt->commandType == CMD_SELECT);

	/*
	 * Ordinarily, the plan node should be a simple Result.  However, if
	 * force_parallel_mode is on, the planner might've stuck a Gather node
	 * atop that.  The simplest way to deal with this is to look through the
	 * Gather node.  The Gather node's tlist would normally contain a Var
	 * referencing the child node's output, but it could also be a Param, or
	 * it could be a Const that setrefs.c copied as-is.
	 */
	plan = stmt->planTree;
	for (;;)
	{
		/* Extract the single tlist expression */
		Assert(list_length(plan->targetlist) == 1);
		tle_expr = castNode(TargetEntry, linitial(plan->targetlist))->expr;

		if (IsA(plan, Result))
		{
			Assert(plan->lefttree == NULL &&
				   plan->righttree == NULL &&
				   plan->initPlan == NULL &&
				   plan->qual == NULL &&
				   ((Result *) plan)->resconstantqual == NULL);
			break;
		}
		else if (IsA(plan, Gather))
		{
			Assert(plan->lefttree != NULL &&
				   plan->righttree == NULL &&
				   plan->initPlan == NULL &&
				   plan->qual == NULL);
			/* If setrefs.c copied up a Const, no need to look further */
			if (IsA(tle_expr, Const))
				break;
			/* Otherwise, it had better be a Param or an outer Var */
			Assert(IsA(tle_expr, Param) || (IsA(tle_expr, Var) &&
					((Var *) tle_expr)->varno == OUTER_VAR));
			/* Descend to the child node */
			plan = plan->lefttree;
		}
		else
			elog(ERROR, "unexpected plan node type: %d",
				 (int) nodeTag(plan));
	}

	/*
	 * Save the simple expression, and initialize state to "not valid in
	 * current transaction".
	 */
	expr->expr_simple_expr = tle_expr;
	expr->expr_simple_state = NULL;
	expr->expr_simple_in_use = false;
	expr->expr_simple_lxid = InvalidLocalTransactionId;
	/* Also stash away the expression result type */
	expr->expr_simple_type = exprType((Node *) tle_expr);
	expr->expr_simple_typmod = exprTypmod((Node *) tle_expr);
	/* We also want to remember if it is immutable or not */
	expr->expr_simple_mutable = contain_mutable_functions((Node *) tle_expr);
}

/*
 * exec_check_rw_parameter --- can we pass expanded object as read/write param?
 *
 * If we have an assignment like "x := array_append(x, foo)" in which the
 * top-level function is trusted not to corrupt its argument in case of an
 * error, then when x has an expanded object as value, it is safe to pass the
 * value as a read/write pointer and let the function modify the value
 * in-place.
 *
 * This function checks for a safe expression, and sets expr->rwparam to the
 * dno of the target variable (x) if safe, or -1 if not safe.
 */
static void
exec_check_rw_parameter(PLpgSQL_expr *expr, int target_dno)
{
	Oid			funcid;
	List	   *fargs;
	ListCell   *lc;

	/* Assume unsafe */
	expr->rwparam = -1;

	/*
	 * If the expression isn't simple, there's no point in trying to optimize
	 * (because the exec_run_select code path will flatten any expanded result
	 * anyway).  Even without that, this seems like a good safety restriction.
	 */
	if (expr->expr_simple_expr == NULL)
		return;

	/*
	 * If target variable isn't referenced by expression, no need to look
	 * further.
	 */
	if (!bms_is_member(target_dno, expr->paramnos))
		return;

	/*
	 * Top level of expression must be a simple FuncExpr or OpExpr.
	 */
	if (IsA(expr->expr_simple_expr, FuncExpr))
	{
		FuncExpr   *fexpr = (FuncExpr *) expr->expr_simple_expr;

		funcid = fexpr->funcid;
		fargs = fexpr->args;
	}
	else if (IsA(expr->expr_simple_expr, OpExpr))
	{
		OpExpr	   *opexpr = (OpExpr *) expr->expr_simple_expr;

		funcid = opexpr->opfuncid;
		fargs = opexpr->args;
	}
	else
		return;

	/*
	 * The top-level function must be one that we trust to be "safe".
	 * Currently we hard-wire the list, but it would be very desirable to
	 * allow extensions to mark their functions as safe ...
	 */
	if (!(funcid == F_ARRAY_APPEND ||
		  funcid == F_ARRAY_PREPEND))
		return;

	/*
	 * The target variable (in the form of a Param) must only appear as a
	 * direct argument of the top-level function.
	 */
	foreach(lc, fargs)
	{
		Node	   *arg = (Node *) lfirst(lc);

		/* A Param is OK, whether it's the target variable or not */
		if (arg && IsA(arg, Param))
			continue;
		/* Otherwise, argument expression must not reference target */
		if (contains_target_param(arg, &target_dno))
			return;
	}

	/* OK, we can pass target as a read-write parameter */
	expr->rwparam = target_dno;
}

/*
 * Recursively check for a Param referencing the target variable
 */
static bool
contains_target_param(Node *node, int *target_dno)
{
	if (node == NULL)
		return false;
	if (IsA(node, Param))
	{
		Param	   *param = (Param *) node;

		if (param->paramkind == PARAM_EXTERN &&
			param->paramid == *target_dno + 1)
			return true;
		return false;
	}
	return expression_tree_walker(node, contains_target_param,
								  (void *) target_dno);
}

/* ----------
 * exec_set_found			Set the global found variable to true/false
 * ----------
 */
static void
exec_set_found(PLpgSQL_execstate *estate, bool state)
{
	PLpgSQL_var *var;

	var = (PLpgSQL_var *) (estate->datums[estate->found_varno]);
	assign_simple_var(estate, var, BoolGetDatum(state), false, false);
}

/*
 * plpgsql_create_econtext --- create an eval_econtext for the current function
 *
 * We may need to create a new shared_simple_eval_estate too, if there's not
 * one already for the current transaction.  The EState will be cleaned up at
 * transaction end.  Ditto for shared_simple_eval_resowner.
 */
static void
plpgsql_create_econtext(PLpgSQL_execstate *estate)
{
	SimpleEcontextStackEntry *entry;

	/*
	 * Create an EState for evaluation of simple expressions, if there's not
	 * one already in the current transaction.  The EState is made a child of
	 * TopTransactionContext so it will have the right lifespan.
	 *
	 * Note that this path is never taken when beginning a DO block; the
	 * required EState was already made by plpgsql_inline_handler.  However,
	 * if the DO block executes COMMIT or ROLLBACK, then we'll come here and
	 * make a shared EState to use for the rest of the DO block.  That's OK;
	 * see the comments for shared_simple_eval_estate.  (Note also that a DO
	 * block will continue to use its private cast hash table for the rest of
	 * the block.  That's okay for now, but it might cause problems someday.)
	 */
	if (estate->simple_eval_estate == NULL)
	{
		MemoryContext oldcontext;

		if (shared_simple_eval_estate == NULL)
		{
			oldcontext = MemoryContextSwitchTo(TopTransactionContext);
			shared_simple_eval_estate = CreateExecutorState();
			MemoryContextSwitchTo(oldcontext);
		}
		estate->simple_eval_estate = shared_simple_eval_estate;
	}

	/*
	 * Likewise for the simple-expression resource owner.
	 */
	if (estate->simple_eval_resowner == NULL)
	{
		if (shared_simple_eval_resowner == NULL)
			shared_simple_eval_resowner =
				ResourceOwnerCreate(TopTransactionResourceOwner,
									"PL/pgSQL simple expressions");
		estate->simple_eval_resowner = shared_simple_eval_resowner;
	}

	/*
	 * Create a child econtext for the current function.
	 */
	estate->eval_econtext = CreateExprContext(estate->simple_eval_estate);

	/*
	 * Make a stack entry so we can clean up the econtext at subxact end.
	 * Stack entries are kept in TopTransactionContext for simplicity.
	 */
	entry = (SimpleEcontextStackEntry *)
		MemoryContextAlloc(TopTransactionContext,
						   sizeof(SimpleEcontextStackEntry));

	entry->stack_econtext = estate->eval_econtext;
	entry->xact_subxid = GetCurrentSubTransactionId();

	entry->next = simple_econtext_stack;
	simple_econtext_stack = entry;
}

/*
 * plpgsql_destroy_econtext --- destroy function's econtext
 *
 * We check that it matches the top stack entry, and destroy the stack
 * entry along with the context.
 */
static void
plpgsql_destroy_econtext(PLpgSQL_execstate *estate)
{
	SimpleEcontextStackEntry *next;

	Assert(simple_econtext_stack != NULL);
	Assert(simple_econtext_stack->stack_econtext == estate->eval_econtext);

	next = simple_econtext_stack->next;
	pfree(simple_econtext_stack);
	simple_econtext_stack = next;

	FreeExprContext(estate->eval_econtext, true);
	estate->eval_econtext = NULL;
}

/*
 * plpgsql_xact_cb --- post-transaction-commit-or-abort cleanup
 *
 * If a simple-expression EState was created in the current transaction,
 * it has to be cleaned up.  The same for the simple-expression resowner.
 */
void
plpgsql_xact_cb(XactEvent event, void *arg)
{
	/*
	 * If we are doing a clean transaction shutdown, free the EState and tell
	 * the resowner to release whatever plancache references it has, so that
	 * all remaining resources will be released correctly.  (We don't need to
	 * actually delete the resowner here; deletion of the
	 * TopTransactionResourceOwner will take care of that.)
	 *
	 * In an abort, we expect the regular abort recovery procedures to release
	 * everything of interest, so just clear our pointers.
	 */
	if (event == XACT_EVENT_COMMIT ||
		event == XACT_EVENT_PARALLEL_COMMIT ||
		event == XACT_EVENT_PREPARE)
	{
		simple_econtext_stack = NULL;

		if (shared_simple_eval_estate)
			FreeExecutorState(shared_simple_eval_estate);
		shared_simple_eval_estate = NULL;
		if (shared_simple_eval_resowner)
			ResourceOwnerReleaseAllPlanCacheRefs(shared_simple_eval_resowner);
		shared_simple_eval_resowner = NULL;
	}
	else if (event == XACT_EVENT_ABORT ||
			 event == XACT_EVENT_PARALLEL_ABORT)
	{
		simple_econtext_stack = NULL;
		shared_simple_eval_estate = NULL;
		shared_simple_eval_resowner = NULL;
	}
}

/*
 * plpgsql_subxact_cb --- post-subtransaction-commit-or-abort cleanup
 *
 * Make sure any simple-expression econtexts created in the current
 * subtransaction get cleaned up.  We have to do this explicitly because
 * no other code knows which econtexts belong to which level of subxact.
 */
void
plpgsql_subxact_cb(SubXactEvent event, SubTransactionId mySubid,
				   SubTransactionId parentSubid, void *arg)
{
	if (event == SUBXACT_EVENT_COMMIT_SUB || event == SUBXACT_EVENT_ABORT_SUB)
	{
		while (simple_econtext_stack != NULL &&
			   simple_econtext_stack->xact_subxid == mySubid)
		{
			SimpleEcontextStackEntry *next;

			FreeExprContext(simple_econtext_stack->stack_econtext,
							(event == SUBXACT_EVENT_COMMIT_SUB));
			next = simple_econtext_stack->next;
			pfree(simple_econtext_stack);
			simple_econtext_stack = next;
		}
	}
}

/*
 * assign_simple_var --- assign a new value to any VAR datum.
 *
 * This should be the only mechanism for assignment to simple variables,
 * lest we do the release of the old value incorrectly.
 */
static void
assign_simple_var(PLpgSQL_execstate *estate, PLpgSQL_var *var,
				  Datum newvalue, bool isnull, bool freeable)
{
	Assert(var->dtype == PLPGSQL_DTYPE_VAR);
	/* Free the old value if needed */
	if (var->freeval)
	{
		if (DatumIsReadWriteExpandedObject(var->value,
										   var->isnull,
										   var->datatype->typlen))
			DeleteExpandedObject(var->value);
		else
			pfree(DatumGetPointer(var->value));
	}
	/* Assign new value to datum */
	var->value = newvalue;
	var->isnull = isnull;
	var->freeval = freeable;
}

/*
 * free old value of a text variable and assign new value from C string
 */
static void
assign_text_var(PLpgSQL_execstate *estate, PLpgSQL_var *var, const char *str)
{
	assign_simple_var(estate, var, CStringGetTextDatum(str), false, true);
}

/*
 * assign_record_var --- assign a new value to any REC datum.
 */
static void
assign_record_var(PLpgSQL_execstate *estate, PLpgSQL_rec *rec,
				  ExpandedRecordHeader *erh)
{
	Assert(rec->dtype == PLPGSQL_DTYPE_REC);

	/* Transfer new record object into datum_context */
	TransferExpandedRecord(erh, estate->datum_context);

	/* Free the old value ... */
	if (rec->erh)
		DeleteExpandedObject(ExpandedRecordGetDatum(rec->erh));

	/* ... and install the new */
	rec->erh = erh;
}

/*
 * exec_eval_using_params --- evaluate params of USING clause
 *
 * The result data structure is created in the stmt_mcontext, and should
 * be freed by resetting that context.
 */
static ParamListInfo
exec_eval_using_params(PLpgSQL_execstate *estate, List *params)
{
	ParamListInfo paramLI;
	int			nargs;
	MemoryContext stmt_mcontext;
	MemoryContext oldcontext;
	int			i;
	ListCell   *lc;

	/* Fast path for no parameters: we can just return NULL */
	if (params == NIL)
		return NULL;

	nargs = list_length(params);
	stmt_mcontext = get_stmt_mcontext(estate);
	oldcontext = MemoryContextSwitchTo(stmt_mcontext);
	paramLI = makeParamList(nargs);
	MemoryContextSwitchTo(oldcontext);

	i = 0;
	foreach(lc, params)
	{
		PLpgSQL_expr *param = (PLpgSQL_expr *) lfirst(lc);
		ParamExternData *prm = &paramLI->params[i];
		int32		ppdtypmod;

		/*
		 * Always mark params as const, since we only use the result with
		 * one-shot plans.
		 */
		prm->pflags = PARAM_FLAG_CONST;

		prm->value = exec_eval_expr(estate, param,
									&prm->isnull,
									&prm->ptype,
									&ppdtypmod);

		oldcontext = MemoryContextSwitchTo(stmt_mcontext);

		if (prm->ptype == UNKNOWNOID)
		{
			/*
			 * Treat 'unknown' parameters as text, since that's what most
			 * people would expect.  The SPI functions can coerce unknown
			 * constants in a more intelligent way, but not unknown Params.
			 * This code also takes care of copying into the right context.
			 * Note we assume 'unknown' has the representation of C-string.
			 */
			prm->ptype = TEXTOID;
			if (!prm->isnull)
				prm->value = CStringGetTextDatum(DatumGetCString(prm->value));
		}
		/* pass-by-ref non null values must be copied into stmt_mcontext */
		else if (!prm->isnull)
		{
			int16		typLen;
			bool		typByVal;

			get_typlenbyval(prm->ptype, &typLen, &typByVal);
			if (!typByVal)
				prm->value = datumCopy(prm->value, typByVal, typLen);
		}

		MemoryContextSwitchTo(oldcontext);

		exec_eval_cleanup(estate);

		i++;
	}

	return paramLI;
}

/*
 * Open portal for dynamic query
 *
 * Caution: this resets the stmt_mcontext at exit.  We might eventually need
 * to move that responsibility to the callers, but currently no caller needs
 * to have statement-lifetime temp data that survives past this, so it's
 * simpler to do it here.
 */
static Portal
exec_dynquery_with_params(PLpgSQL_execstate *estate,
						  PLpgSQL_expr *dynquery,
						  List *params,
						  const char *portalname,
						  int cursorOptions)
{
	Portal		portal;
	Datum		query;
	bool		isnull;
	Oid			restype;
	int32		restypmod;
	char	   *querystr;
	MemoryContext stmt_mcontext = get_stmt_mcontext(estate);

	/*
	 * Evaluate the string expression after the EXECUTE keyword. Its result is
	 * the querystring we have to execute.
	 */
	query = exec_eval_expr(estate, dynquery, &isnull, &restype, &restypmod);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("query string argument of EXECUTE is null")));

	/* Get the C-String representation */
	querystr = convert_value_to_string(estate, query, restype);

	/* copy it into the stmt_mcontext before we clean up */
	querystr = MemoryContextStrdup(stmt_mcontext, querystr);

	exec_eval_cleanup(estate);

	/*
	 * Open an implicit cursor for the query.  We use
	 * SPI_cursor_parse_open_with_paramlist even when there are no params,
	 * because this avoids making and freeing one copy of the plan.
	 */
	portal = SPI_cursor_parse_open_with_paramlist(portalname,
												  querystr,
												  exec_eval_using_params(estate,
																		 params),
												  estate->readonly_func,
												  cursorOptions);

	if (portal == NULL)
		elog(ERROR, "could not open implicit cursor for query \"%s\": %s",
			 querystr, SPI_result_code_string(SPI_result));

	/* Release transient data */
	MemoryContextReset(stmt_mcontext);

	return portal;
}

/*
 * Return a formatted string with information about an expression's parameters,
 * or NULL if the expression does not take any parameters.
 * The result is in the eval_mcontext.
 */
static char *
format_expr_params(PLpgSQL_execstate *estate,
				   const PLpgSQL_expr *expr)
{
	int			paramno;
	int			dno;
	StringInfoData paramstr;
	MemoryContext oldcontext;

	if (!expr->paramnos)
		return NULL;

	oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));

	initStringInfo(&paramstr);
	paramno = 0;
	dno = -1;
	while ((dno = bms_next_member(expr->paramnos, dno)) >= 0)
	{
		Datum		paramdatum;
		Oid			paramtypeid;
		bool		paramisnull;
		int32		paramtypmod;
		PLpgSQL_var *curvar;

		curvar = (PLpgSQL_var *) estate->datums[dno];

		exec_eval_datum(estate, (PLpgSQL_datum *) curvar,
						&paramtypeid, &paramtypmod,
						&paramdatum, &paramisnull);

		appendStringInfo(&paramstr, "%s%s = ",
						 paramno > 0 ? ", " : "",
						 curvar->refname);

		if (paramisnull)
			appendStringInfoString(&paramstr, "NULL");
		else
		{
			char	   *value = convert_value_to_string(estate, paramdatum, paramtypeid);
			char	   *p;

			appendStringInfoCharMacro(&paramstr, '\'');
			for (p = value; *p; p++)
			{
				if (*p == '\'') /* double single quotes */
					appendStringInfoCharMacro(&paramstr, *p);
				appendStringInfoCharMacro(&paramstr, *p);
			}
			appendStringInfoCharMacro(&paramstr, '\'');
		}

		paramno++;
	}

	MemoryContextSwitchTo(oldcontext);

	return paramstr.data;
}

/*
 * Return a formatted string with information about the parameter values,
 * or NULL if there are no parameters.
 * The result is in the eval_mcontext.
 */
static char *
format_preparedparamsdata(PLpgSQL_execstate *estate,
						  ParamListInfo paramLI)
{
	int			paramno;
	StringInfoData paramstr;
	MemoryContext oldcontext;

	if (!paramLI)
		return NULL;

	oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));

	initStringInfo(&paramstr);
	for (paramno = 0; paramno < paramLI->numParams; paramno++)
	{
		ParamExternData *prm = &paramLI->params[paramno];

		/*
		 * Note: for now, this is only used on ParamListInfos produced by
		 * exec_eval_using_params(), so we don't worry about invoking the
		 * paramFetch hook or skipping unused parameters.
		 */
		appendStringInfo(&paramstr, "%s$%d = ",
						 paramno > 0 ? ", " : "",
						 paramno + 1);

		if (prm->isnull)
			appendStringInfoString(&paramstr, "NULL");
		else
		{
			char	   *value = convert_value_to_string(estate, prm->value, prm->ptype);
			char	   *p;

			appendStringInfoCharMacro(&paramstr, '\'');
			for (p = value; *p; p++)
			{
				if (*p == '\'') /* double single quotes */
					appendStringInfoCharMacro(&paramstr, *p);
				appendStringInfoCharMacro(&paramstr, *p);
			}
			appendStringInfoCharMacro(&paramstr, '\'');
		}
#if 0
			appendStringInfoStringQuoted(&paramstr,
										 convert_value_to_string(estate,
																 prm->value,
																 prm->ptype),
										 -1);
#endif
	}

	MemoryContextSwitchTo(oldcontext);

	return paramstr.data;
}

Datum
plpgsql_check_function(PLpgSQL_function *func, FunctionCallInfo fcinfo,
							EState *simple_eval_estate,
							ResourceOwner simple_eval_resowner,
							ResourceOwner procedure_resowner,
							bool atomic, FuncCheckData *chkdata)
{
	PLpgSQL_execstate estate;
	int plpgsql_level = SetEnterPlpgsqlFunc();

	/*
	 * Setup the execution state
	 */
	plpgsql_estate_setup(&estate, func, (ReturnSetInfo *) fcinfo->resultinfo,
						 simple_eval_estate, simple_eval_resowner);
	estate.procedure_resowner = procedure_resowner;
	estate.atomic = atomic;

	copy_plpgsql_datums(&estate, func);

	PG_TRY();
	{
		exec_check_stmt_block(func, func->action, chkdata);
	}
	PG_CATCH();
	{
		/* Clean up any leftover temporary memory */
		plpgsql_destroy_econtext(&estate);
		exec_eval_cleanup(&estate);
		/* stmt_mcontext will be destroyed when function's main context is */

		SetExitPlpgsqlFunc(plpgsql_level);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Clean up any leftover temporary memory */
	plpgsql_destroy_econtext(&estate);
	exec_eval_cleanup(&estate);
	/* stmt_mcontext will be destroyed when function's main context is */

	SetExitPlpgsqlFunc(plpgsql_level);

	return (Datum) 0;
}

static void
exec_check_stmts(PLpgSQL_function *func, List *stmts, FuncCheckData *chkdata)
{
	ListCell *s;

	if (stmts == NIL || chkdata->pullup)
	{
		return;
	}

	foreach (s, stmts)
	{
		PLpgSQL_stmt *stmt = (PLpgSQL_stmt *)lfirst(s);
		ListCell *lc;
		PLpgSQL_var *curvar;

		CHECK_FOR_INTERRUPTS();

		switch (stmt->cmd_type)
		{
			case PLPGSQL_STMT_BLOCK:
				exec_check_stmt_block(func, (PLpgSQL_stmt_block *)stmt, chkdata);
				break;
			case PLPGSQL_STMT_ASSIGN:
				exec_check_expr(func, ((PLpgSQL_stmt_assign *)stmt)->expr, chkdata);
				break;
			case PLPGSQL_STMT_IF:
				exec_check_expr(func, ((PLpgSQL_stmt_if *)stmt)->cond, chkdata);
				exec_check_stmts(func, ((PLpgSQL_stmt_if *)stmt)->then_body, chkdata);
				foreach(lc, ((PLpgSQL_stmt_if *)stmt)->elsif_list)
				{
					exec_check_stmts(func, ((PLpgSQL_if_elsif *) lfirst(lc))->stmts, chkdata);
				}
				exec_check_stmts(func, ((PLpgSQL_stmt_if *)stmt)->else_body, chkdata);
				break;
			case PLPGSQL_STMT_CASE:
				exec_check_expr(func, ((PLpgSQL_stmt_case *)stmt)->t_expr, chkdata);
				foreach(lc, ((PLpgSQL_stmt_case *)stmt)->case_when_list)
				{
					PLpgSQL_case_when *cwt = (PLpgSQL_case_when *) lfirst(lc);
					exec_check_stmts(func, cwt->stmts, chkdata);
				}
				exec_check_stmts(func, ((PLpgSQL_stmt_case *)stmt)->else_stmts, chkdata);
				break;
			case PLPGSQL_STMT_LOOP:
				exec_check_stmts(func, ((PLpgSQL_stmt_loop *)stmt)->body, chkdata);
				break;
			case PLPGSQL_STMT_WHILE:
				exec_check_stmts(func, ((PLpgSQL_stmt_while *)stmt)->body, chkdata);
				break;
			case PLPGSQL_STMT_FORI:
				exec_check_stmts(func, ((PLpgSQL_stmt_fori *)stmt)->body, chkdata);
				break;
			case PLPGSQL_STMT_FORS:
				exec_check_expr(func, ((PLpgSQL_stmt_fors *)stmt)->query, chkdata);
				exec_check_stmts(func, ((PLpgSQL_stmt_fors *)stmt)->body, chkdata);
				break;
			case PLPGSQL_STMT_FORC:
				curvar = (PLpgSQL_var *)(func->datums[((PLpgSQL_stmt_forc *)stmt)->curvar]);
				exec_check_expr(func, ((PLpgSQL_stmt_forc *)stmt)->argquery, chkdata);
				exec_check_expr(func, curvar->cursor_explicit_expr, chkdata);
				exec_check_stmts(func, ((PLpgSQL_stmt_forc *)stmt)->body, chkdata);
				break;
			case PLPGSQL_STMT_FOREACH_A:
				exec_check_expr(func, ((PLpgSQL_stmt_foreach_a *)stmt)->expr, chkdata);
				exec_check_stmts(func, ((PLpgSQL_stmt_foreach_a *)stmt)->body, chkdata);
				break;
			case PLPGSQL_STMT_RETURN:
				exec_check_expr(func, ((PLpgSQL_stmt_return *)stmt)->expr, chkdata);
				break;
			case PLPGSQL_STMT_RETURN_NEXT:
				exec_check_expr(func, ((PLpgSQL_stmt_return_next *)stmt)->expr, chkdata);
				break;
			case PLPGSQL_STMT_RETURN_QUERY:
				exec_check_expr(func, ((PLpgSQL_stmt_return_query *)stmt)->query, chkdata);
				exec_check_expr(func, ((PLpgSQL_stmt_return_query *)stmt)->dynquery, chkdata);
				break;
			case PLPGSQL_STMT_ASSERT:
				exec_check_expr(func, ((PLpgSQL_stmt_assert *)stmt)->cond, chkdata);
				exec_check_expr(func, ((PLpgSQL_stmt_assert *)stmt)->message, chkdata);
				break;
			case PLPGSQL_STMT_EXECSQL:
				exec_check_expr(func, ((PLpgSQL_stmt_execsql *)stmt)->sqlstmt, chkdata);
				break;
			case PLPGSQL_STMT_OPEN:
				if (((PLpgSQL_stmt_open *)stmt)->dynquery != NULL)
				{
					chkdata->pullup = true;
					break;
				}
				curvar = (PLpgSQL_var *)(func->datums[ ((PLpgSQL_stmt_open *)stmt)->curvar]);
				exec_check_expr(func,  ((PLpgSQL_stmt_open *)stmt)->query, chkdata);
				exec_check_expr(func,  ((PLpgSQL_stmt_open *)stmt)->argquery, chkdata);
				exec_check_expr(func, curvar->cursor_explicit_expr, chkdata);
				break;
			case PLPGSQL_STMT_SET:
				exec_check_expr(func, ((PLpgSQL_stmt_set *)stmt)->expr, chkdata);
				break;
			case PLPGSQL_STMT_PERFORM:
			case PLPGSQL_STMT_CALL:
			case PLPGSQL_STMT_GETDIAG:
			case PLPGSQL_STMT_RAISE:
			case PLPGSQL_STMT_DYNEXECUTE:
			case PLPGSQL_STMT_DYNFORS:
				chkdata->pullup = true;
				break;
			case PLPGSQL_STMT_EXIT:
			case PLPGSQL_STMT_FETCH:
			case PLPGSQL_STMT_CLOSE:
			case PLPGSQL_STMT_COMMIT:
			case PLPGSQL_STMT_ROLLBACK:
				break;
			default:
				elog(ERROR, "unrecognized cmd_type: %d", stmt->cmd_type);
		}
	}
}

static void
exec_check_stmt_block(PLpgSQL_function *func, PLpgSQL_stmt_block *block, FuncCheckData *chkdata)
{
	if (block == NULL || chkdata->pullup)
	{
		return;
	}

	exec_check_stmts(func, block->body, chkdata);

	if (block->exceptions)
	{
		ListCell *e;
		foreach (e, block->exceptions->exc_list)
		{
			PLpgSQL_exception *exception = (PLpgSQL_exception *)lfirst(e);
			exec_check_stmts(func, exception->action, chkdata);
		}
	}
}

static void
exec_check_expr(PLpgSQL_function *func, PLpgSQL_expr *expr, FuncCheckData *chkdata)
{
	List	   *raw_parsetree_list;
	ListCell   *list_item;

	if (expr == NULL || chkdata->pullup)
	{
		return;
	}
	
	expr->func = func;
	raw_parsetree_list = pg_parse_query(expr->query);
	foreach (list_item, raw_parsetree_list)
	{
		RawStmt *parsetree = lfirst_node(RawStmt, list_item);
		List *querytree_list;
		ListCell *cell;
		querytree_list = pg_analyze_and_rewrite_params(parsetree, expr->query,
													   (ParserSetupHook)plpgsql_parser_setup,
													   (void *)expr, NULL, false, true);
		foreach (cell, querytree_list)
		{
			Query *query = lfirst_node(Query, cell);
			exec_check_query_pullup(func, query, chkdata);
		}
	}
}
