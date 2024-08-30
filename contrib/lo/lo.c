/*
 *    PostgreSQL definitions for managed Large Objects.
 *
 *    contrib/lo/lo.c
 *
 */

#include "postgres.h"

#include "commands/trigger.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

/* Utility function to check errors */ /* 检查错误的辅助函数 */
static void ensure_trigger_fired_by_row(TriggerData* trigdata) {
    if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
        elog(ERROR, "%s: must be fired for row", trigdata->tg_trigger->tgname);
}
/*
 * This is the trigger that protects us from orphaned large objects
 */
 /* 通过触发器管理大型对象的生命周期 */
PG_FUNCTION_INFO_V1(lo_manage);

Datum
manage_large_objects(PG_FUNCTION_ARGS)
{
    TriggerData* trigdata = (TriggerData*)fcinfo->context;
    // Descriptive variable names for better readability /* 更具描述性的变量名以增强可读性 */
    int         attributeNumber;
    char** arguments;
    TupleDesc   tupleDescriptor;
    HeapTuple   returnValue;
    bool        isDeleteAction;
    HeapTuple   newTuple;
    HeapTuple   triggerTuple;

    if (!CALLED_AS_TRIGGER(fcinfo)) /* internal error */ /* 内部错误 */
        elog(ERROR, "manage_large_objects: not fired by trigger manager");

    ensure_trigger_fired_by_row(trigdata);

    if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event)) /* internal error */
        elog(ERROR, "%s: must be fired for row",
             trigdata->tg_trigger->tgname);

    /*
     * Fetch some values from trigdata
     */
    newtuple = trigdata->tg_newtuple;
    trigtuple = trigdata->tg_trigtuple;
    tupdesc = trigdata->tg_relation->rd_att;
    args = trigdata->tg_trigger->tgargs;

    if (args == NULL)            /* internal error */
        elog(ERROR, "%s: no column name provided in the trigger definition",
             trigdata->tg_trigger->tgname);

    /* tuple to return to Executor */
    if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
        rettuple = newtuple;
    else
        rettuple = trigtuple;

    /* Are we deleting the row? */
    isdelete = TRIGGER_FIRED_BY_DELETE(trigdata->tg_event);

    /* Get the column we're interested in */
    attnum = SPI_fnumber(tupdesc, args[0]);

    if (attnum <= 0)
        elog(ERROR, "%s: column \"%s\" does not exist",
             trigdata->tg_trigger->tgname, args[0]);

    /*
    * Handle updates
    * If the value of the monitored attribute changes, unlink the associated large object.
    * 处理更新：如果被监测属性的值发生变化，那么取消关联的大对象。
    */
    if (newTuple != NULL)
    {
        char* originalValue = SPI_getvalue(triggerTuple, tupleDescriptor, attributeNumber);
        char* newValue = SPI_getvalue(newTuple, tupleDescriptor, attributeNumber);

        if (originalValue != NULL && (newValue == NULL || strcmp(originalValue, newValue) != 0))
            DirectFunctionCall1(lo_unlink,
                ObjectIdGetDatum(atooid(originalValue)));

        if (newValue)
            pfree(newValue);
        if (originalValue)
            pfree(originalValue);
    }


    /*
     * Handle deleting of rows
     *
     * Here, we unlink the large object associated with the managed attribute
     */
    if (isdelete)
    {
        char       *orig = SPI_getvalue(trigtuple, tupdesc, attnum);

        if (orig != NULL)
        {
            DirectFunctionCall1(be_lo_unlink,
                                ObjectIdGetDatum(atooid(orig)));

            pfree(orig);
        }
    }

    return PointerGetDatum(rettuple);
}
