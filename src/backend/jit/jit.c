/*-------------------------------------------------------------------------
 *
 * jit.c
 *	  Provider independent JIT infrastructure.
 *
 * Code related to loading JIT providers, redirecting calls into JIT providers
 * and error handling.  No code specific to a specific JIT implementation
 * should end up here.
 *
 *
 * Copyright (c) 2016-2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/jit/jit.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"


#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>


#include "fmgr.h"
#include "executor/execExpr.h"
#include "jit/jit.h"
#include "miscadmin.h"
#include "utils/resowner_private.h"
#include "utils/fmgrprotos.h"


/* GUCs */
bool		jit_enabled = false;
char	   *jit_provider = NULL;
bool		jit_debugging_support = false;
bool		jit_dump_bitcode = false;
bool		jit_expressions = true;
bool		jit_profiling_support = false;
bool		jit_tuple_deforming = true;
double		jit_above_cost = 100000;
double		jit_inline_above_cost = 500000;
double		jit_optimize_above_cost = 500000;

static JitProviderCallbacks provider;
static bool provider_successfully_loaded = false;
static bool provider_failed_loading = false;


static bool provider_init(void);
static bool file_exists(const char *name);


/*
 * SQL level function returning whether JIT is available in the current
 * backend. Will attempt to load JIT provider if necessary.
 */
Datum
pg_jit_available(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(provider_init());
}


/*
 * Return whether a JIT provider has successfully been loaded, caching the
 * result.
 */
static bool
provider_init(void)
{
	char		path[MAXPGPATH];
	JitProviderInit init;

	/* don't even try to load if not enabled */
	if (!jit_enabled)
		return false;

	/*
	 * Don't retry loading after failing - attempting to load JIT provider
	 * isn't cheap.
	 */
	if (provider_failed_loading)
		return false;
	if (provider_successfully_loaded)
		return true;

	/*
	 * Check whether shared library exists. We do that check before actually
	 * attempting to load the shared library (via load_external_function()),
	 * because that'd error out in case the shlib isn't available.
	 */
	snprintf(path, MAXPGPATH, "%s/%s%s", pkglib_path, jit_provider, DLSUFFIX);
	elog(DEBUG1, "probing availability of JIT provider at %s", path);
	if (!file_exists(path))
	{
		elog(DEBUG1,
			 "provider not available, disabling JIT for current session");
		provider_failed_loading = true;
		return false;
	}

	/*
	 * If loading functions fails, signal failure. We do so because
	 * load_external_function() might error out despite the above check if
	 * e.g. the library's dependencies aren't installed. We want to signal
	 * ERROR in that case, so the user is notified, but we don't want to
	 * continually retry.
	 */
	provider_failed_loading = true;

	/* and initialize */
	init = (JitProviderInit)
		load_external_function(path, "_PG_jit_provider_init", true, NULL);
	init(&provider);

	provider_successfully_loaded = true;
	provider_failed_loading = false;

	elog(DEBUG1, "successfully loaded JIT provider in current session");

	return true;
}

/*
 * Reset JIT provider's error handling. This'll be called after an error has
 * been thrown and the main-loop has re-established control.
 */
void
jit_reset_after_error(void)
{
	if (provider_successfully_loaded)
		provider.reset_after_error();
}

/*
 * Release resources required by one JIT context.
 */
void
jit_release_context(JitContext *context)
{
	if (provider_successfully_loaded)
		provider.release_context(context);

	ResourceOwnerForgetJIT(context->resowner, PointerGetDatum(context));
	pfree(context);
}

/*
 * Ask provider to JIT compile an expression.
 *
 * Returns true if successful, false if not.
 */
bool
jit_compile_expr(struct ExprState *state)
{
	/*
	 * We can easily create a one-off context for functions without an
	 * associated PlanState (and thus EState). But because there's no executor
	 * shutdown callback that could deallocate the created function, they'd
	 * live to the end of the transactions, where they'd be cleaned up by the
	 * resowner machinery. That can lead to a noticeable amount of memory
	 * usage, and worse, trigger some quadratic behaviour in gdb. Therefore,
	 * at least for now, don't create a JITed function in those circumstances.
	 */
	if (!state->parent)
		return false;

	/* if no jitting should be performed at all */
	if (!(state->parent->state->es_jit_flags & PGJIT_PERFORM))
		return false;

	/* or if expressions aren't JITed */
	if (!(state->parent->state->es_jit_flags & PGJIT_EXPR))
		return false;

	/* this also takes !jit_enabled into account */
	if (provider_init())
		return provider.compile_expr(state);

	return false;
}

/* Aggregate JIT instrumentation information */
void
InstrJitAgg(JitInstrumentation *dst, JitInstrumentation *add)
{
	dst->created_functions += add->created_functions;
	INSTR_TIME_ADD(dst->generation_counter, add->generation_counter);
	INSTR_TIME_ADD(dst->inlining_counter, add->inlining_counter);
	INSTR_TIME_ADD(dst->optimization_counter, add->optimization_counter);
	INSTR_TIME_ADD(dst->emission_counter, add->emission_counter);
}

static bool
file_exists(const char *name)
{
	struct stat st;

	AssertArg(name != NULL);

	if (stat(name, &st) == 0)
		return S_ISDIR(st.st_mode) ? false : true;
	else if (!(errno == ENOENT || errno == ENOTDIR))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not access file \"%s\": %m", name)));

	return false;
}

#ifdef __OPENTENBASE_C__
/*
 * Check if var is supported by jit.
 *
 * Only support when var is both notnull and byval.
 */
static bool
jit_supported_var(Var *var, List *es_range_table)
{
	RangeTblEntry *rte;
	int attno = var->varoattno;
	int varno = var->varnoold;

	if (attno <= 0 || varno <= 0 || varno > list_length(es_range_table))
		return false;

	rte = (RangeTblEntry *) list_nth(es_range_table, varno - 1);
	if (rte && OidIsValid(rte->relid))
	{
		Relation baserel = RelationIdGetRelation(rte->relid);
		if (baserel)
		{
			bool result = false;
			if (attno <= baserel->rd_att->natts)
			{
				Form_pg_attribute attr = TupleDescAttr(baserel->rd_att, attno - 1);
				result = attr && attr->attbyval && attr->attnotnull;
			}
			RelationClose(baserel);
			return result;
		}
	}

	return false;
}

/*
 * Ask provider to JIT compile a hashjoin.
 */
void
jit_compile_hashjoin(struct HashJoinState *hjstate)
{
	ListCell *lc_inner;
	ListCell *lc_outer;
	EState *estate = hjstate->js.ps.state;
	Plan *plan = hjstate->js.ps.plan;
	HashState *hstate = (HashState*)hjstate->js.ps.righttree;

	if (!(estate->es_jit_flags & PGJIT_PERFORM))
		return;

	if (hjstate->js.jointype != JOIN_INNER ||
		hjstate->js.joinqual || hjstate->js.ps.qual)
		return;

	if (((HashJoin*)plan)->num_batches > 1 || ((HashJoin*)plan)->nonequijoin)
		return;

	if (plan->extParam)
	{
		int typeoid;
		int idx = -1;

		while((idx = bms_next_member(plan->extParam, idx)) >= 0)
		{
			typeoid = list_nth_int(estate->es_plannedstmt->paramExecTypes, idx);
			if (OidIsValid(typeoid))
				return;
		}
	}

	forboth (lc_inner, hstate->hashkeys, lc_outer, hjstate->hj_OuterHashKeys)
	{
		Expr *expr1 = ((ExprState *)lfirst(lc_inner))->expr;
		Expr *expr2 = ((ExprState *)lfirst(lc_outer))->expr;

		if (IsA(expr1, Var) && IsA(expr2, Var))
		{
			if (jit_supported_var((Var *)expr1, estate->es_range_table) &&
				jit_supported_var((Var *)expr2, estate->es_range_table))
				continue;
			else
				return;
		}
		else
			return;
	}

	if (provider_init())
		provider.compile_hashjoin(hjstate);
}
#endif
