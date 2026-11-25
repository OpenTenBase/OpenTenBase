#ifndef __EDS_H__
#define __EDS_H__
#endif

#include "postgres.h"

#include <limits.h>
#include <math.h>

#include "access/htup_details.h"
#include "access/relation.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_type.h"
#include "catalog/pg_user_mapping.h"
#include "commands/dbcommands.h"
#include "common/connect.h"
#include "common/logging.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "parser/scansup.h"
#include "postmaster/postmaster.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/varlena.h"

#define PG_CONNECT_PARAMS "hostaddr=127.0.0.1 port=%d user=%s dbname=%s"
#define SELECT_QUERY "select ins_id,ins_loc,ins_name,hostname,port,username,dbname,params,isactive from eds.eds_instance_table;"

extern char * CurrentUserName(void);
extern char * get_connect_string(const char *servername);
extern bool is_valid_dblink_option(const PQconninfoOption *options, const char *option, Oid context);
extern char * escape_param_str(const char *str);
extern void dblink_connstr_check(const char *connstr);
