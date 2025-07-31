/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#ifndef __PG_ORACLE__
#define __PG_ORACLE__

#include "postgres.h"
#include "catalog/catversion.h"
#include "nodes/pg_list.h"
#include <sys/time.h>
#include "utils/datetime.h"
#include "utils/datum.h"
/* 
*function macro 
*/
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
            char _instr[2] = {0, 0}; \
            text_to_cstring_buffer(_in, _instr, 2); \
            if (_instr[0] == '\0') \
                PG_RETURN_NULL(); \
        } \
    } while (0)

/* 
* related function declaration
*/
extern Datum orcl_lpad(PG_FUNCTION_ARGS);
extern Datum orcl_rpad(PG_FUNCTION_ARGS);

#endif  /* __PG_ORACLE__ */
