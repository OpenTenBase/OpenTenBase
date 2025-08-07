/*-------------------------------------------------------------------------
 *
 * orcl_datetime.h
 *    declarations for datetime types in opentenbase_ora 19c
 *
 *    src/include/utils/orcl_datetime.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ORCL_DATETIME_H
#define ORCL_DATETIME_H


#include <wctype.h>

#include "utils/date.h"
#include "utils/datetime.h"

#define IS_ORA_TIMESTAMPS(x)	    (false)
#define IS_ORA_DATETIME(x)	    (false)
extern void IntervalForSQLStandard(Interval *span);
#endif /* ORCL_DATETIME_H */
