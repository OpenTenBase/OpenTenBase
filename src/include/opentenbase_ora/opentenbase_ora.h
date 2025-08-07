#ifndef __OPENTENBASE_ORA__
#define __OPENTENBASE_ORA__

#include "postgres.h"
#include "catalog/catversion.h"
#include "catalog/pg_type.h"
#include "nodes/pg_list.h"
#include <sys/time.h>
#include "utils/datetime.h"
#include "utils/datum.h"
#include "utils/guc.h"

#define PG_GETARG_IF_EXISTS(n, type, defval) \
	((PG_NARGS() > (n) && !PG_ARGISNULL(n)) ? PG_GETARG_##type(n) : (defval))

#define PG_GETARG_TEXT_P_IF_EXISTS(_n) \
	(PG_NARGS() > (_n) ? PG_GETARG_TEXT_P(_n) : NULL)
#define PG_GETARG_TEXT_P_IF_NULL(_n)\
	(PG_ARGISNULL(_n) ? NULL : PG_GETARG_TEXT_P_IF_EXISTS(_n))

#define PG_GETARG_TEXT_PP_IF_EXISTS(_n) \
	(PG_NARGS() > (_n) ? PG_GETARG_TEXT_PP(_n) : NULL)
#define PG_GETARG_TEXT_PP_IF_NULL(_n)\
	(PG_ARGISNULL(_n) ? NULL : PG_GETARG_TEXT_PP_IF_EXISTS(_n))

#define PG_GETARG_INT32_0_IF_EXISTS(_n) \
	(PG_NARGS() > (_n) ? PG_GETARG_INT32(_n) : 0)
#define PG_GETARG_INT32_0_IF_NULL(_n) \
	(PG_ARGISNULL(_n) ? 0 : PG_GETARG_INT32_0_IF_EXISTS(_n))

#define PG_GETARG_INT32_1_IF_EXISTS(_n) \
	(PG_NARGS() > (_n) ? PG_GETARG_INT32(_n) : 1)
#define PG_GETARG_INT32_1_IF_NULL(_n) \
	(PG_ARGISNULL(_n) ? 1 : PG_GETARG_INT32_1_IF_EXISTS(_n))

#define PG_RETURN_NULL_IF_EMPTY_TEXT(_in) \
	do { \
		if (_in != NULL) \
		{ \
				int len = pg_encoding_max_length(GetDatabaseEncoding()); \
				char *_instr = palloc0(len + 1); \
				text_to_cstring_buffer(_in, _instr, len + 1); \
				if (_instr[0] == '\0') \
				{ \
						pfree(_instr); \
						PG_RETURN_NULL(); \
				} \
				pfree(_instr); \
		} \
	} while (0)

#define GETSTRCMPFUNC (ORA_MODE ? strcasecmp : strcmp)

extern Datum orcl_lpad(PG_FUNCTION_ARGS);
extern Datum orcl_rpad(PG_FUNCTION_ARGS);

/* Data Type Precedence
 * opentenbase_ora uses data type precedence to determine implicit data type conversion,
 * which is discussed in the section that follows. opentenbase_ora data types take the
 * following precedence:
 *
 *		Datetime and interval data types
 *		BINARY_DOUBLE
 *		BINARY_FLOAT
 *		NUMBER
 *		Character data types
 *		All other built-in data types
 */
typedef enum OraTypePrecedence
{
    LV_LEAST,
    LV_CHARACTER,
    LV_NUMBER,
    LV_FLOAT,
    LV_DOUBLE,
    LV_DATETIME_INTERVAL
} OraTypePrecedence;
#define ORA_PRECEDENCE(a, b) 			{(a), (b)},
typedef struct OraPrecedence
{
    Oid	typoid;
    int	level;
} OraPrecedence;

static const OraPrecedence OraPrecedenceMap[] =
        {
                ORA_PRECEDENCE(INTERVALOID, LV_DATETIME_INTERVAL)
                ORA_PRECEDENCE(FLOAT8OID, LV_DOUBLE)
                ORA_PRECEDENCE(FLOAT4OID, LV_FLOAT)
                ORA_PRECEDENCE(NUMERICOID, LV_NUMBER)
                ORA_PRECEDENCE(TEXTOID, LV_CHARACTER)
                ORA_PRECEDENCE(VARCHAROID, LV_CHARACTER)
                ORA_PRECEDENCE(VARCHAR2OID, LV_CHARACTER)
                ORA_PRECEDENCE(NVARCHAR2OID, LV_CHARACTER)
                ORA_PRECEDENCE(BPCHAROID, LV_CHARACTER)
                ORA_PRECEDENCE(CHAROID, LV_CHARACTER)
        };
static const int NumOraPrecedenceMap = lengthof(OraPrecedenceMap);

extern RowId OpenTenBaseOraRowIdGetIdent(Datum d);
extern Datum orcl_userenv(PG_FUNCTION_ARGS);

/* opentenbase_ora collect type's function */
typedef int32 (*Ora_Collect_CFunc_Int)(const char* sz_func, void *pl_func, int32 var_no, int32 param1, bool *isnull);
typedef bool (*Ora_Collect_Exists_Int)(const char* sz_func, void *pl_func, int32 var_no, int32 param1);
typedef void (*Ora_Collect_CProc_Int)(const char* sz_func, void *pl_func, int32 var_no, int32 param1, int32 param2, int32 num_param);

typedef text* (*Ora_Collect_CFunc_Text)(const char* sz_func, void *pl_func, int32 var_no, text *param1, bool *isnull);
typedef bool (*Ora_Collect_Exists_Text)(const char* sz_func, void *pl_func, int32 var_no, text *param1);
typedef void (*Ora_Collect_CProc_Text)(const char* sz_func, void *pl_func, int32 var_no, text *param1, text *param2, int32 num_param);

typedef int32 (*Ora_Collect_Fetch_sidx)(void *pl_func, int32 var_no, text *key, bool *isnull);
typedef int32 (*Ora_Collect_Fetch_iidx)(void *pl_func, int32 var_no, int32 key, bool *isnull);
typedef int32 (*Ora_Collect_Fetch_array_iidx)(const char* sz_var, void *pl_func, int32 var_no, int32 sidx, bool *isnull);

typedef struct Ora_Collect_Func_Set 
{
	Ora_Collect_CFunc_Int com_func;
	Ora_Collect_Exists_Int exists;
	Ora_Collect_CProc_Int com_proc;
	Ora_Collect_Fetch_array_iidx array_iidx;

	Ora_Collect_CFunc_Text assa_func;
	Ora_Collect_Exists_Text assa_exists;
	Ora_Collect_CProc_Text assa_proc;

	Ora_Collect_Fetch_sidx assa_sidx;
	Ora_Collect_Fetch_iidx assa_iidx;
} Ora_Collect_Func_Set;

extern Ora_Collect_Func_Set* ora_get_collect_func_sets(void);
extern Datum opentenbase_ora_collect_func(PG_FUNCTION_ARGS);
extern Datum opentenbase_ora_collect_exists(PG_FUNCTION_ARGS);
extern Datum opentenbase_ora_collect_proc(PG_FUNCTION_ARGS);
extern Datum opentenbase_ora_collect_func_text(PG_FUNCTION_ARGS);
extern Datum opentenbase_ora_collect_exists_text(PG_FUNCTION_ARGS);
extern Datum opentenbase_ora_collect_proc_text(PG_FUNCTION_ARGS);
extern Datum opentenbase_ora_collect_fetch_iidx(PG_FUNCTION_ARGS);
extern Datum opentenbase_ora_collect_fetch_sidx(PG_FUNCTION_ARGS);
extern Datum opentenbase_ora_collect_fetch_array_iidx(PG_FUNCTION_ARGS);

#endif
