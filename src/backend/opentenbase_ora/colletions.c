/*-------------------------------------------------------------------------
 *
 * colletions.c
 *	  To compatile with opentenbase_ora, it supports opentenbase_ora's collects such as: 
 *		VARRAY
 *		NESTED TABLE
 *		Associative Arrays
 *
 *	It only supports opentenbase_ora's some function for collects in plpgsql, but it needs some function
 * 	to call by function/procedure.
 *
 * Portions Copyright (c) 2021, OpenTenBase
 *
 *
 * IDENTIFICATION
 *	  src/backend/opentenbase_ora/colletions.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xact.h"
#include "fmgr.h"
#include "utils/varlena.h"
#include "utils/builtins.h"
#include "opentenbase_ora/opentenbase_ora.h"

static Ora_Collect_Func_Set g_ora_collect_funcs = {0};

Ora_Collect_Func_Set* 
ora_get_collect_func_sets(void)
{
	return &g_ora_collect_funcs;
}

Datum
opentenbase_ora_collect_func(PG_FUNCTION_ARGS)
{
	char *sz_func = NULL;
	void *pl_func = NULL;
	int32 var_no = 0;
	int32 param1= 0;
	bool isnull = false;
	int32 ret = 0;

	if (!InPlpgsqlFunc())
	{
		PG_RETURN_NULL();
	}

	if (NULL == g_ora_collect_funcs.com_func)
	{
		elog(ERROR, "Collect type error: No common function to handle collect's function");
	}

	if (PG_ARGISNULL(0))
	{
		elog(ERROR, "Collect type error: The function's name is null");
	}

	if (PG_ARGISNULL(1))
	{
		elog(ERROR, "Collect type error: The function addr in plpgsql is null");
	}

	if (PG_ARGISNULL(2))
	{
		elog(ERROR, "Collect type error: The parameter's NO is null");
	}

	sz_func = text_to_cstring(PG_GETARG_TEXT_PP(0));
	pl_func = (void*)PG_GETARG_INT64(1);
	var_no = PG_GETARG_INT32(2);
	param1 = PG_GETARG_INT32_0_IF_NULL(3);

	ret = g_ora_collect_funcs.com_func(sz_func, pl_func, var_no, param1, &isnull);
	if (isnull)
	{
		PG_RETURN_NULL();
	}

	PG_RETURN_INT32(ret);
}

Datum
opentenbase_ora_collect_exists(PG_FUNCTION_ARGS)
{
	char *sz_func = NULL;
	void *pl_func = NULL;
	int32 var_no = 0;
	int32 param1= 0;
	bool ret = 0;

	if (!InPlpgsqlFunc())
	{
		PG_RETURN_NULL();
	}

	if (NULL == g_ora_collect_funcs.exists)
	{
		elog(ERROR, "Collect type error: No exists function to handle collect's function");
	}

	if (PG_ARGISNULL(0))
	{
		elog(ERROR, "Collect type error: The function's name is null");
	}

	if (PG_ARGISNULL(1))
	{
		elog(ERROR, "Collect type error: The function addr in plpgsql is null");
	}

	if (PG_ARGISNULL(2))
	{
		elog(ERROR, "Collect type error: The parameter's NO is null");
	}

	if (PG_ARGISNULL(3))
	{
		elog(ERROR, "Collect type error: The subscripe is need");
	}

	sz_func = text_to_cstring(PG_GETARG_TEXT_PP(0));
	pl_func = (void*)PG_GETARG_INT64(1);
	var_no = PG_GETARG_INT32(2);
	param1 = PG_GETARG_INT32(3);

	ret = g_ora_collect_funcs.exists(sz_func, pl_func, var_no, param1);
	elog(DEBUG3, "Collect type %s: sz_func=%s,pl_func=%p, var_no=%d,  param1=%d, ret=%d", 
		__FUNCTION__ , sz_func,  pl_func, var_no, param1, ret);

	PG_RETURN_BOOL(ret);
}


Datum
opentenbase_ora_collect_proc(PG_FUNCTION_ARGS)
{
	char *sz_func = NULL;
	void *pl_func = NULL;
	int32 var_no = 0;
	int32 param1= 0;
	int32 param2 = 0;
	int32 param3 = 0;

	if (!InPlpgsqlFunc())
	{
		PG_RETURN_NULL();
	}

	if (NULL == g_ora_collect_funcs.com_proc)
	{
		elog(ERROR, "Error: No common procedure to handle collect's procedure");
	}

	if (PG_ARGISNULL(0))
	{
		elog(ERROR, "Collect type error: The function's name is null");
	}

	if (PG_ARGISNULL(1))
	{
		elog(ERROR, "Collect type error: The function addr in plpgsql is null");
	}

	if (PG_ARGISNULL(2))
	{
		elog(ERROR, "Collect type error: The parameter's NO is null");
	}

	sz_func = text_to_cstring(PG_GETARG_TEXT_PP(0));
	pl_func = (void*)PG_GETARG_INT64(1);
	var_no = PG_GETARG_INT32(2);
	
	param1 = PG_GETARG_INT32_0_IF_NULL(3);
	param2 = PG_GETARG_INT32_0_IF_NULL(4);
	param3 = PG_GETARG_INT32_0_IF_NULL(5);

	g_ora_collect_funcs.com_proc(sz_func, pl_func, var_no, param1, param2, param3);
	elog(DEBUG3, "Collect type %s: sz_func=%s, pl_func=%p var_no=%d,  param1=%d, param2=%d, param3=%d", 
		__FUNCTION__ , sz_func, pl_func, var_no, param1, param2, param3);
	
	PG_RETURN_NULL();
}

Datum
opentenbase_ora_collect_func_text(PG_FUNCTION_ARGS)
{
	char *sz_func = NULL;
	void *pl_func = NULL;
	int32 var_no = 0;
	text *param1 = NULL;
	text *ret = NULL;
	bool isnull = false;

	if (!InPlpgsqlFunc())
	{
		PG_RETURN_NULL();
	}

	if (NULL == g_ora_collect_funcs.assa_func)
	{
		elog(ERROR, "Collect type error: No common function to handle collect's function");
	}

	if (PG_ARGISNULL(0))
	{
		elog(ERROR, "Collect type error: The function's name is null");
	}

	if (PG_ARGISNULL(1))
	{
		elog(ERROR, "Collect type error: The function addr in plpgsql is null");
	}

	if (PG_ARGISNULL(2))
	{
		elog(ERROR, "Collect type error: The parameter's NO is null");
	}

	sz_func = text_to_cstring(PG_GETARG_TEXT_PP(0));
	pl_func = (void*)PG_GETARG_INT64(1);
	var_no = PG_GETARG_INT32(2);

	param1 = PG_GETARG_TEXT_P_IF_NULL(3);

	ret = g_ora_collect_funcs.assa_func(sz_func, pl_func, var_no, param1, &isnull);

	if (isnull || NULL == ret)
	{
		PG_RETURN_NULL();
	}

	PG_RETURN_TEXT_P(ret);
}

Datum
opentenbase_ora_collect_exists_text(PG_FUNCTION_ARGS)
{
	char *sz_func = NULL;
	void *pl_func = NULL;
	int32 var_no = 0;
	text *param1= NULL;
	bool ret = 0;

	if (!InPlpgsqlFunc())
	{
		PG_RETURN_NULL();
	}

	if (NULL == g_ora_collect_funcs.assa_exists)
	{
		elog(ERROR, "Collect type error: No exists function to handle collect's function");
	}

	if (PG_ARGISNULL(0))
	{
		elog(ERROR, "Collect type error: The function's name is null");
	}

	if (PG_ARGISNULL(1))
	{
		elog(ERROR, "Collect type error: The function addr in plpgsql is null");
	}

	if (PG_ARGISNULL(2))
	{
		elog(ERROR, "Collect type error: The parameter's NO is null");
	}

	if (PG_ARGISNULL(3))
	{
		elog(ERROR, "Collect type error: The subscripe is need");
	}

	sz_func = text_to_cstring(PG_GETARG_TEXT_PP(0));
	pl_func = (void*)PG_GETARG_INT64(1);
	var_no = PG_GETARG_INT32(2);
	param1 = PG_GETARG_TEXT_P(3);

	ret = g_ora_collect_funcs.assa_exists(sz_func, pl_func, var_no, param1);
	PG_RETURN_BOOL(ret);
}


Datum
opentenbase_ora_collect_proc_text(PG_FUNCTION_ARGS)
{
	char *sz_func = NULL;
	void *pl_func = NULL;
	int32 var_no = 0;
	text *param1= NULL;
	text *param2 = NULL;
	int32 param3 = 0;

	if (!InPlpgsqlFunc())
	{
		PG_RETURN_NULL();
	}

	if (NULL == g_ora_collect_funcs.assa_proc)
	{
		elog(ERROR, "Error: No common procedure to handle collect's procedure");
	}

	if (PG_ARGISNULL(0))
	{
		elog(ERROR, "Collect type error: The function's name is null");
	}

	if (PG_ARGISNULL(1))
	{
		elog(ERROR, "Collect type error: The function addr in plpgsql is null");
	}

	if (PG_ARGISNULL(2))
	{
		elog(ERROR, "Collect type error: The parameter's NO is null");
	}

	sz_func = text_to_cstring(PG_GETARG_TEXT_PP(0));
	pl_func = (void*)PG_GETARG_INT64(1);
	var_no = PG_GETARG_INT32(2);
	
	param1 = PG_GETARG_TEXT_PP_IF_NULL(3);
	param2 = PG_GETARG_TEXT_PP_IF_NULL(4);
	param3 = PG_GETARG_INT32_0_IF_NULL(5);

	g_ora_collect_funcs.assa_proc(sz_func, pl_func, var_no, param1, param2, param3);	
	PG_RETURN_NULL();
}

Datum 
opentenbase_ora_collect_fetch_sidx(PG_FUNCTION_ARGS)
{
	void *pl_func = NULL;
	int32 var_no = 0;
	text *param1;
	bool isnull = false;
	int32 ret = 0;

	if (!InPlpgsqlFunc())
	{
		PG_RETURN_NULL();
	}

	if (NULL == g_ora_collect_funcs.assa_sidx)
	{
		elog(ERROR, "Error: No common procedure to handle collect's fetch value");
	}

	if (PG_ARGISNULL(0))
	{
		elog(ERROR, "Collect type error: The function addr in plpgsql is null");
	}

	if (PG_ARGISNULL(1))
	{
		elog(ERROR, "Collect type error: The parameter's NO is null");
	}

	if (PG_ARGISNULL(2))
	{
		elog(ERROR, "Collect type error: The hash key is null");
	}

	pl_func = (void*)PG_GETARG_INT64(0);
	var_no = PG_GETARG_INT32(1);	
	param1 = PG_GETARG_TEXT_P(2);

	ret = g_ora_collect_funcs.assa_sidx(pl_func, var_no, param1, &isnull);
	if (isnull)
	{
		PG_RETURN_NULL();
	}

	PG_RETURN_INT32(ret);
}

Datum 
opentenbase_ora_collect_fetch_iidx(PG_FUNCTION_ARGS)
{
	void *pl_func = NULL;
	int32 var_no = 0;
	int32 param1= 0;
	bool isnull = false;
	int32 ret = 0;

	if (!InPlpgsqlFunc())
	{
		PG_RETURN_NULL();
	}

	if (NULL == g_ora_collect_funcs.assa_iidx)
	{
		elog(ERROR, "Error: No common procedure to handle collect's fetch value");
	}

	if (PG_ARGISNULL(0))
	{
		elog(ERROR, "Collect type error: The function addr in plpgsql is null");
	}

	if (PG_ARGISNULL(1))
	{
		elog(ERROR, "Collect type error: The parameter's NO is null");
	}

	if (PG_ARGISNULL(2))
	{
		elog(ERROR, "Collect type error: The hash key is null");
	}

	pl_func = (void*)PG_GETARG_INT64(0);
	var_no = PG_GETARG_INT32(1);	
	param1 = PG_GETARG_INT32(2);

	ret = g_ora_collect_funcs.assa_iidx(pl_func, var_no, param1, &isnull);
	if (isnull)
	{
		PG_RETURN_NULL();
	}

	PG_RETURN_INT32(ret);
}

/**
 * for plpgsql, fetch variable's subscirpt in varray or nested table, and check the subscipt valid
 */
Datum
opentenbase_ora_collect_fetch_array_iidx(PG_FUNCTION_ARGS)
{
	char *sz_var = NULL;
	void *pl_func = NULL;
	int32 var_no = 0;
	int32 param1= 0;
	bool isnull = false;
	int32 ret = 0;

	if (!InPlpgsqlFunc())
	{
		PG_RETURN_NULL();
	}

	if (NULL == g_ora_collect_funcs.array_iidx)
	{
		elog(ERROR, "Collect type error: No fetch array subscript function to handle collect's function");
	}

	if (PG_ARGISNULL(0))
	{
		elog(ERROR, "Collect type error: The var name is null");
	}

	if (PG_ARGISNULL(1))
	{
		elog(ERROR, "Collect type error: The function addr in plpgsql is null");
	}

	if (PG_ARGISNULL(2))
	{
		elog(ERROR, "Collect type error: The parameter's NO is null");
	}

	if (PG_ARGISNULL(3))
	{
		elog(ERROR, "Collect type error: The subscripe is need");
	}

	sz_var = text_to_cstring(PG_GETARG_TEXT_PP(0));
	pl_func = (void*)PG_GETARG_INT64(1);
	var_no = PG_GETARG_INT32(2);
	param1 = PG_GETARG_INT32(3);

	ret = g_ora_collect_funcs.array_iidx(sz_var, pl_func, var_no, param1, &isnull);
	elog(DEBUG3, "Collect type %s: sz_var=%s,pl_func=%p, var_no=%d,  param1=%d", 
		__FUNCTION__ , sz_var,  pl_func, var_no, param1);

	if (isnull)
	{
		PG_RETURN_NULL();
	}

	PG_RETURN_INT32(ret);
}
