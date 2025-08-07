#include "postgres.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "commands/variable.h"

#include "opentenbase_ora.h"
#include "builtins.h"
#include "pipe.h"

PG_FUNCTION_INFO_V1(get_empty_clob);

/* adapt opentenbase_ora empty_clob ()*/
Datum get_empty_clob(PG_FUNCTION_ARGS)
{
    text *result = (text *) palloc(VARHDRSZ);
    SET_VARSIZE(result, VARHDRSZ);
    PG_RETURN_TEXT_P(result);
}
