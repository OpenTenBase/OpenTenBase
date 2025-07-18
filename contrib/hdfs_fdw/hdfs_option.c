#include "postgres.h"
#include "hdfs_fdw.h"
#include "hdfs_option.h"
#include "foreign/foreign.h"
#include "utils/builtins.h"
#include "commands/defrem.h"
#include "miscadmin.h"

/*
 * Valid options list
 */
const char *hdfs_valid_options[] = {
    "host", "port", "dbname", "username", "password", "table_name",
    "connect_timeout", "receive_timeout", "fetch_size",
    "log_remote_sql", "client_type", "auth_type",
    NULL
};

/*
 * Get the options for a foreign table
 */
hdfs_opt *hdfs_get_options(Oid foreigntableid)
{
    hdfs_opt   *opt;
    ForeignTable *table;
    ForeignServer *server;
    UserMapping *user;
    List       *options;
    ListCell   *lc;

    /* Get foreign table, server, and user mapping */
    table = GetForeignTable(foreigntableid);
    server = GetForeignServer(table->serverid);
    user = GetUserMapping(GetUserId(), table->serverid);

    /* Allocate options structure */
    opt = (hdfs_opt *) palloc0(sizeof(hdfs_opt));

    /* Set default options */
    opt->host = pstrdup("localhost");
    opt->port = 10000;
    opt->dbname = pstrdup("default");
    opt->username = pstrdup("anonymous");
    opt->password = pstrdup("");
    opt->table_name = NULL;
    opt->connect_timeout = 300;
    opt->receive_timeout = 300;
    opt->fetch_size = 1000;
    opt->log_remote_sql = false;
    opt->client_type = 1; /* HIVESERVER */
    opt->auth_type = pstrdup("NOSASL");

    /* Merge all options */
    options = NIL;
    options = list_concat(options, table->options);
    options = list_concat(options, server->options);
    if (user)
        options = list_concat(options, user->options);

    /* Parse options */
    foreach(lc, options)
    {
        DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "host") == 0)
            opt->host = defGetString(def);
        else if (strcmp(def->defname, "port") == 0)
            opt->port = atoi(defGetString(def));
        else if (strcmp(def->defname, "dbname") == 0)
            opt->dbname = defGetString(def);
        else if (strcmp(def->defname, "username") == 0)
            opt->username = defGetString(def);
        else if (strcmp(def->defname, "password") == 0)
            opt->password = defGetString(def);
        else if (strcmp(def->defname, "fetch_size") == 0)
            opt->fetch_size = atoi(defGetString(def));
        else if (strcmp(def->defname, "log_remote_sql") == 0)
            opt->log_remote_sql = defGetBoolean(def);
        else if (strcmp(def->defname, "table_name") == 0)
            opt->table_name = defGetString(def);
        /* Parse other options... */
    }

    return opt;
}
