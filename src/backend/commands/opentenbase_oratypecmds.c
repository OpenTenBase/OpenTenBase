/*-------------------------------------------------------------------------
 *
 * opentenbase_oratypecmds.c
 *	 
 * IDENTIFICATION
 *	  src/backend/commands/opentenbase_oratypecmds.c
 *
 * DESCRIPTION
 *	  Adapting opentenbase_ora syntax
 *       "create TYPE person AS OBJECT(id integer, name text,age integer);"
 *	  "create TYPE CourseList IS TABLE OF VARCHAR2(16);"
 *
 * NOTES
 *	  
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/binary_upgrade.h"
#include "catalog/catalog.h"
#include "catalog/heap.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_am.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_constraint_fn.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/pg_range.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_fn.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_synonym.h"
#include "commands/tablespace.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/typecmds.h"
#include "executor/executor.h"
#include "access/multixact.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/var.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "parser/parser.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "catalog/pg_type_object.h"
#include "utils/guc.h"
#include "commands/extension.h"
