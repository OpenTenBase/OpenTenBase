/*-------------------------------------------------------------------------
 *
 * params.h
 *	  Support for finding the values associated with Param nodes.
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/params.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARAMS_H
#define PARAMS_H

/* Forward declarations, to avoid including other headers */
struct Bitmapset;
struct ExprState;
struct Param;
struct ParseState;


/*
 *	  ParamListInfo
 *
 *	  ParamListInfo structures are used to pass parameters into the executor
 *	  for parameterized plans.  We support two basic approaches to supplying
 *	  parameter values, the "static" way and the "dynamic" way.
 *
 *	  In the static approach, per-parameter data is stored in an array of
 *	  ParamExternData structs appended to the ParamListInfo struct.
 *	  Each entry in the array defines the value to be substituted for a
 *	  PARAM_EXTERN parameter.  The "paramid" of a PARAM_EXTERN Param
 *	  can range from 1 to numParams.
 *
 *	  Although parameter numbers are normally consecutive, we allow
 *	  ptype == InvalidOid to signal an unused array entry.
 *
 *	  pflags is a flags field.  Currently the only used bit is:
 *	  PARAM_FLAG_CONST signals the planner that it may treat this parameter
 *	  as a constant (i.e., generate a plan that works only for this value
 *	  of the parameter). PARAM_FLAG_EMPTY_STRING tells the NULL is from empty
 *	  string, it is only used to pass value to SQL from PLSQL.
 *
 *	  In the dynamic approach, all access to parameter values is done through
 *	  hook functions found in the ParamListInfo struct.  In this case,
 *	  the ParamExternData array is typically unused and not allocated;
 *	  but the legal range of paramid is still 1 to numParams.
 *
 *	  Although the data structure is really an array, not a list, we keep
 *	  the old typedef name to avoid unnecessary code changes.
 *
 *	  There are 3 hook functions that can be associated with a ParamListInfo
 *	  structure:
 *
 *	  If paramFetch isn't null, it is called to fetch the ParamExternData
 *	  for a particular param ID, rather than accessing the relevant element
 *	  of the ParamExternData array.  This supports the case where the array
 *	  isn't there at all, as well as cases where the data in the array
 *	  might be obsolete or lazily evaluated.  paramFetch must return the
 *	  address of a ParamExternData struct describing the specified param ID;
 *	  the convention above about ptype == InvalidOid signaling an invalid
 *	  param ID still applies.  The returned struct can either be placed in
 *	  the "workspace" supplied by the caller, or it can be in storage
 *	  controlled by the paramFetch hook if that's more convenient.
 *	  (In either case, the struct is not expected to be long-lived.)
 *	  If "speculative" is true, the paramFetch hook should not risk errors
 *	  in trying to fetch the parameter value, and should report an invalid
 *	  parameter instead.
 *
 *	  If paramCompile isn't null, then it controls what execExpr.c compiles
 *	  for PARAM_EXTERN Param nodes --- typically, this hook would emit a
 *	  EEOP_PARAM_CALLBACK step.  This allows unnecessary work to be
 *	  optimized away in compiled expressions.
 *
 *	  If parserSetup isn't null, then it is called to re-instantiate the
 *	  original parsing hooks when a query needs to be re-parsed/planned.
 *	  This is especially useful if the types of parameters might change
 *	  from time to time, since it can replace the need to supply a fixed
 *	  list of parameter types to the parser.
 *
 *	  Notice that the paramFetch and paramCompile hooks are actually passed
 *	  the ParamListInfo struct's address; they can therefore access all
 *	  three of the "arg" fields, and the distinction between paramFetchArg
 *	  and paramCompileArg is rather arbitrary.
 */

#define PARAM_FLAG_CONST	0x0001	/* parameter is constant */
#define PARAM_FLAG_EMPTY_STRING	0x0002	/* parameter is null from empty string. */

typedef struct ParamExternData
{
	Datum		value;			/* parameter value */
	bool		isnull;			/* is it NULL? */
	uint16		pflags;			/* flag bits, see above */
	Oid			ptype;			/* parameter's datatype, or 0 */
} ParamExternData;

typedef struct ParamListInfoData *ParamListInfo;

#if 1
typedef ParamExternData *(*ParamFetchHook) (ParamListInfo params,
											int paramid, bool speculative,
											ParamExternData *workspace);

typedef void (*ParamCompileHook) (ParamListInfo params, struct Param *param,
								  struct ExprState *state,
								  Datum *resv, bool *resnull);
#else
#ifdef _PG_ORCL_
typedef ParamExternData *(*ParamFetchHook) (ParamListInfo params, int paramid);
#else
typedef void (*ParamFetchHook) (ParamListInfo params, int paramid);
#endif
#endif

typedef void (*ParserSetupHook) (struct ParseState *pstate, void *arg);

/*
 * Parameter may be evaluated at different cases, use 'param_context' to identify
 * it. Currently, only has one bit: PARAM_CONTEXT_PLASSIGN. It indicates if
 * executing an assignment statement in PLSQL.
 */
#define PARAM_CONTEXT_PLASSIGN 0x0001

typedef struct ParamListInfoData
{
	ParamFetchHook paramFetch;	/* parameter fetch hook */
	void	   *paramFetchArg;
	ParamCompileHook paramCompile;	/* parameter compile hook */
	void	   *paramCompileArg;
	ParserSetupHook parserSetup;	/* parser setup hook */
	void	   *parserSetupArg;
	int			numParams;		/* nominal/maximum # of Params represented */
	int			param_context; /* At which context to evaluate the params */

	/*
	 * params[] may be of length zero if paramFetch is supplied; otherwise it
	 * must be of length numParams.
	 */
	ParamExternData params[FLEXIBLE_ARRAY_MEMBER];
}			ParamListInfoData;


/* ----------------
 *	  ParamExecData
 *
 *	  ParamExecData entries are used for executor internal parameters
 *	  (that is, values being passed into or out of a sub-query).  The
 *	  paramid of a PARAM_EXEC Param is a (zero-based) index into an
 *	  array of ParamExecData records, which is referenced through
 *	  es_param_exec_vals or ecxt_param_exec_vals.
 *
 *	  If execPlan is not NULL, it points to a SubPlanState node that needs
 *	  to be executed to produce the value.  (This is done so that we can have
 *	  lazy evaluation of InitPlans: they aren't executed until/unless a
 *	  result value is needed.)	Otherwise the value is assumed to be valid
 *	  when needed.
 * ----------------
 */

typedef struct ParamExecData
{
	void	   *execPlan;		/* should be "SubPlanState *" */
	Datum		value;
	bool		isnull;
#ifdef XCP
	Oid			ptype;
	bool		done;
#endif
} ParamExecData;


/* Functions found in src/backend/nodes/params.c */
extern ParamListInfo makeParamList(int numParams);
extern ParamListInfo copyParamList(ParamListInfo from);
extern Size EstimateParamListSpace(ParamListInfo paramLI);
extern void SerializeParamList(ParamListInfo paramLI, char **start_address);
extern ParamListInfo RestoreParamList(char **start_address);
extern char *BuildParamLogString(ParamListInfo params, char **knownTextValues,
								 int maxlen);
#endif							/* PARAMS_H */
