#include "eds.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(show_node);

char *
CurrentUserName(void)
{
    Oid userId = GetUserId();
    return GetUserNameFromId(userId,false);   
}

char *
escape_param_str(const char *str)
{
    const char *cp;
    StringInfoData buf;

    initStringInfo(&buf);

    for (cp = str; *cp; cp++)
    {
        if (*cp == '\\' || *cp == '\'')
            appendStringInfoChar(&buf, '\\');
        appendStringInfoChar(&buf, *cp);
    }

    return buf.data;
}

bool
is_valid_dblink_option(const PQconninfoOption *options, const char *option,
                       Oid context)
{
    const PQconninfoOption *opt;

    /*在libpq结果中查找选项*/
    for (opt = options; opt->keyword; opt++)
    {
        if (strcmp(opt->keyword, option) == 0)
            break;
    }
    if (opt->keyword == NULL)
        return false;

    /* 不允许调试选项（特别是“复制”） */
    if (strchr(opt->dispchar, 'D'))
        return false;

    /* Disallow "client_encoding" */
    if (strcmp(opt->keyword, "client_encoding") == 0)
        return false;

    /*
    *如果选项为“用户”或标记为安全，则应仅指定该选项
    *在用户映射中。其他应仅在SERVER中指定。
    */
    if (strcmp(opt->keyword, "user") == 0 || strchr(opt->dispchar, '*'))
    {
        if (context != UserMappingRelationId)
            return false;
    }
    else
    {
        if (context != ForeignServerRelationId)
            return false;
    }

    return true;
}


char *
get_connect_string(const char *servername)
{
    ForeignServer *foreign_server = NULL;
    UserMapping *user_mapping;
    ListCell   *cell;
    StringInfoData buf;
    ForeignDataWrapper *fdw;
    AclResult   aclresult;
    char       *srvname;

    static const PQconninfoOption *options = NULL;

    initStringInfo(&buf);

    /*
    *获取有效libpq选项的列表。
    *
    *为了避免不必要的工作，我们获取一次列表并将其贯穿始终
    *   此后端进程的生存期。我们不需要关心
    *内存上下文问题，因为PQconnfefaults使用malloc进行分配。
    */
    if (!options)
    {   
        options = PQconndefaults();
        if (!options)           /* assume reason for failure is OOM */
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_OUT_OF_MEMORY),
                     errmsg("out of memory"),
                     errdetail("Could not get libpq's default connection options.")));
    }

    /*首先收集服务器连接选项*/
    srvname = pstrdup(servername);
    truncate_identifier(srvname, strlen(srvname), false);
    foreign_server = GetForeignServerByName(srvname, true);

    if (foreign_server)
    {
        Oid         serverid = foreign_server->serverid;
        Oid         fdwid = foreign_server->fdwid;
        Oid         userid = GetUserId();

        user_mapping = GetUserMapping(userid, serverid);
        fdw = GetForeignDataWrapper(fdwid);

        /*检查权限，用户必须在服务器上有使用权限*/
        aclresult = pg_foreign_server_aclcheck(serverid, userid, ACL_USAGE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, OBJECT_FOREIGN_SERVER, foreign_server->servername);

        foreach(cell, fdw->options)
        {
            DefElem    *def = lfirst(cell);

            if (is_valid_dblink_option(options, def->defname, ForeignDataWrapperRelationId))
                appendStringInfo(&buf, "%s='%s' ", def->defname,
                                 escape_param_str(strVal(def->arg)));
        }

        foreach(cell, foreign_server->options)
        {
            DefElem    *def = lfirst(cell);

            if (is_valid_dblink_option(options, def->defname, ForeignServerRelationId))
                appendStringInfo(&buf, "%s='%s' ", def->defname,
                                 escape_param_str(strVal(def->arg)));
        }

        foreach(cell, user_mapping->options)
        {

            DefElem    *def = lfirst(cell);

            if (is_valid_dblink_option(options, def->defname, UserMappingRelationId))
                appendStringInfo(&buf, "%s='%s' ", def->defname,
                                 escape_param_str(strVal(def->arg)));
        }

        return buf.data;
    }
    else
        return NULL;
}

void
dblink_connstr_check(const char *connstr)
{
    if (!superuser())
    {
        PQconninfoOption *options;
        PQconninfoOption *option;
        bool        connstr_gives_password = false;

        options = PQconninfoParse(connstr, NULL);
        if (options)
        {
            for (option = options; option->keyword != NULL; option++)
            {
                if (strcmp(option->keyword, "password") == 0)
                {
                    if (option->val != NULL && option->val[0] != '\0')
                    {
                        connstr_gives_password = true;
                        break;
                    }
                }
            }
            PQconninfoFree(options);
        }

        if (!connstr_gives_password)
            ereport(ERROR,
                    (errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
                     errmsg("password is required"),
                     errdetail("Non-superusers must provide a password in the connection string.")));
    }
}

Datum
show_node(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    int call_cntr;
    int max_calls;
    TupleDesc tupdesc;
    AttInMetadata *attinmeta;
    int i,j,tupleCount,ret;
    char ***values;
    PGconn *conn = NULL;
    PGresult *result = NULL;
    StringInfo data = makeStringInfo();
/*
    char *connstr = NULL;
    char *conname_or_str = NULL;
    connstr = get_connect_string(conname_or_str);
    if (connstr == NULL)
        connstr = conname_or_str;
    dblink_connstr_check(connstr);
*/
    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;
        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("function returning record called in context "
                         "that cannot accept type record")));
        appendStringInfo(data,PG_CONNECT_PARAMS,PostPortNumber,CurrentUserName(),get_database_name(MyDatabaseId));
        elog(DEBUG1,"%s",data->data);

//        conn = PQconnectdb(connstr);
        conn = PQconnectdb(data->data);

        ret = PQstatus(conn);
        if (ret != CONNECTION_OK)
            ereport(ERROR,(errmsg("%s",conn->errorMessage.data)));
        result = PQexec(conn,"BEGIN;");
        ret = PQresultStatus(result);
        if (ret != PGRES_COMMAND_OK)
            ereport(ERROR,(errmsg("begin transaction failed")));

        resetStringInfo(data);
        appendStringInfo(data,SELECT_QUERY);
        result = PQexec(conn,data->data);
        ret = PQresultStatus(result);
        if (ret != PGRES_TUPLES_OK)
        {
            result = PQexec(conn,"ROLLBACK;");
            ret = PQresultStatus(result);
            if (ret == PGRES_COMMAND_OK)
                ereport(ERROR,(errmsg("select failed")));
        }
        tupleCount = PQntuples(result);
        if(tupleCount < 0)
        {
            result = PQexec(conn,"ROLLBACK;");
            ret = PQresultStatus(result);
            if (ret == PGRES_COMMAND_OK)
                ereport(ERROR,(errmsg("select failed")));
        }
        funcctx->max_calls = tupleCount;
        max_calls = funcctx->max_calls;
        values = (char ***) calloc(max_calls,sizeof(char **));
        for( i = 0;i < max_calls;i++)
        {
            values[i] = (char **) calloc(12,sizeof(char *));
            for(j = 0;j < 12;j++)
            {
                values[i][j] = (char *) calloc(128,sizeof(char));
                if(PQgetvalue(result,i,j) == NULL || !strcmp(PQgetvalue(result,i,j), ""))
                    values[i][j] = NULL;
                else
                    strcpy(values[i][j],PQgetvalue(result,i,j));
            }
        }

        result = PQexec(conn,"COMMIT;");
        ret = PQresultStatus(result);
        if (ret != PGRES_COMMAND_OK)
        {
            result = PQexec(conn,"ROLLBACK;");
            ret = PQresultStatus(result);
            if (ret == PGRES_COMMAND_OK)
                ereport(ERROR,(errmsg("commit transaction failed")));
        }
        PQclear(result);
        PQfinish(conn);

        attinmeta = TupleDescGetAttInMetadata(tupdesc);
        funcctx->attinmeta = attinmeta;
        funcctx->user_fctx = values;
        MemoryContextSwitchTo(oldcontext);
    }
    funcctx = SRF_PERCALL_SETUP();
    call_cntr = funcctx->call_cntr;
    max_calls = funcctx->max_calls;
    attinmeta = funcctx->attinmeta;
    values = funcctx->user_fctx;

    if (call_cntr < max_calls)
    {
        Datum result2;
        HeapTuple tuple;

        tuple = BuildTupleFromCStrings(attinmeta, values[call_cntr]);
        result2 = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result2);
    }
    else
    {
        SRF_RETURN_DONE(funcctx);
    }
}
