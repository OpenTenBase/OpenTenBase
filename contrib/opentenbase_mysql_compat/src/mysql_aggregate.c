/*-------------------------------------------------------------------------
 *
 * mysql_aggregate.c
 *	  MySQL-compatible aggregate functions.
 *
 * Functions: GROUP_CONCAT
 *
 * Copyright (c) 2026, OpenTenBase Contributors
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/varlena.h"
#include "utils/memutils.h"
#include "nodes/pg_list.h"

Datum mysql_group_concat_transfn(PG_FUNCTION_ARGS);
Datum mysql_group_concat_finalfn(PG_FUNCTION_ARGS);


/*
 * GROUP_CONCAT transition state
 */
typedef struct GroupConcatState
{
	StringInfo	str;
	bool		first;
	char	   *separator;
} GroupConcatState;


/*
 * GROUP_CONCAT transition function
 * Accumulates values with a comma separator (default) or custom separator.
 */
PG_FUNCTION_INFO_V1(mysql_group_concat_transfn);
Datum
mysql_group_concat_transfn(PG_FUNCTION_ARGS)
{
	GroupConcatState *state;
	text	   *sep;
	bool		is_first_call = PG_ARGISNULL(0);

	if (is_first_call)
	{
		MemoryContext aggcontext;

		if (!AggCheckCallContext(fcinfo, &aggcontext))
			elog(ERROR, "group_concat_transfn called in non-aggregate context");

		state = (GroupConcatState *) MemoryContextAllocZero(aggcontext,
															sizeof(GroupConcatState));
		state->str = makeStringInfo();
		state->first = true;

		/* Custom separator from second argument */
		if (PG_NARGS() > 1 && !PG_ARGISNULL(1))
		{
			sep = PG_GETARG_TEXT_P(1);
			state->separator = text_to_cstring(sep);
		}
		else
			state->separator = ",";
	}
	else
	{
		state = (GroupConcatState *) PG_GETARG_POINTER(0);
	}

	/* Add value if not NULL (MySQL GROUP_CONCAT skips NULLs by default) */
	if (!PG_ARGISNULL(is_first_call ? 0 : 2))
	{
		text   *val;
		int		arg_idx = is_first_call ? 0 : 2;

		val = PG_GETARG_TEXT_P(arg_idx);

		if (!state->first)
			appendStringInfoString(state->str, state->separator);

		appendBinaryStringInfo(state->str, VARDATA_ANY(val),
							   VARSIZE_ANY_EXHDR(val));
		state->first = false;
	}

	PG_RETURN_POINTER(state);
}


/*
 * GROUP_CONCAT final function
 * Returns the concatenated string.
 */
PG_FUNCTION_INFO_V1(mysql_group_concat_finalfn);
Datum
mysql_group_concat_finalfn(PG_FUNCTION_ARGS)
{
	GroupConcatState *state;
	text	   *result;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (GroupConcatState *) PG_GETARG_POINTER(0);

	if (state->str->len == 0)
		PG_RETURN_NULL();

	result = cstring_to_text_with_len(state->str->data, state->str->len);
	PG_RETURN_TEXT_P(result);
}
