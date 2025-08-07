#include "postgres.h"
#include "postgres_ext.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/xlogreader.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_audit.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_mls.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/partition.h"
#include "catalog/pg_inherits_fn.h"
#include "contrib/pgcrypto/pgp.h"
#include "commands/schemacmds.h"
#include "commands/tablespace.h"
#include "executor/tuptable.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/execnodes.h"
#include "parser/parsetree.h"
#include "parser/parse_relation.h"
#include "pgxc/squeue.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/lockdefs.h"
#include "storage/lwlock.h"
#include "storage/sinval.h"
#include "license/license.h"
#include "storage/shmem.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/mls.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/memutils.h"
#include "utils/inval.h"
#include "utils/snapshot.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/ruleutils.h"
#include "utils/resowner_private.h"
#include "utils/datamask.h"
#include "utils/guc.h"

#include "pgxc/pgxcnode.h"
#include "pgxc/nodemgr.h"
#include "pgstat.h"




#ifdef _PG_REGRESS_
bool g_enable_cls               = true;
bool g_enable_data_mask         = true;
#else
bool g_enable_cls               = false;
bool g_enable_data_mask         = false;
#endif



#define MLS_QUERY_STRING_PRUNE_DELIMETER '('


Datum pg_execute_query_on_all_nodes(PG_FUNCTION_ARGS);
Datum pg_get_table_oid_by_name(PG_FUNCTION_ARGS);
Datum pg_get_role_oid_by_name(PG_FUNCTION_ARGS);
Datum pg_get_function_oid_by_name(PG_FUNCTION_ARGS);
Datum pg_get_schema_oid_by_name(PG_FUNCTION_ARGS);
Datum pg_get_tablespace_oid_by_tablename(PG_FUNCTION_ARGS);
Datum pg_get_tablespace_name_by_tablename(PG_FUNCTION_ARGS);
Datum pg_get_current_database_oid(PG_FUNCTION_ARGS);
Datum pg_transparent_crypt_support_datatype(PG_FUNCTION_ARGS);

extern List * FunctionGetOidsByNameString(List * func_name);


#if MARK("sys function")
/*
 * this is a special function, called by cls user or audit user, to modify cls or audit system tables directly,
 * and would keep the transaction consistence and relcache fresh.
 */
Datum pg_execute_query_on_all_nodes(PG_FUNCTION_ARGS)
{
    char    *query_str;
    Oid     *co_oids;
    Oid     *dn_oids;
    int     num_dnodes;
    int     num_coords;
    Oid     authenticate_userid;

    authenticate_userid = GetAuthenticatedUserId();

    if ((DEFAULT_ROLE_MLS_SYS_USERID != authenticate_userid)
        && (DEFAULT_ROLE_AUDIT_SYS_USERID != authenticate_userid))
    {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("permission denied, function only could be called by cls user or audit user")));  
    }

    query_str = text_to_cstring(PG_GETARG_TEXT_PP(0));
	if (NULL == query_str)
	{
		elog(ERROR, "input query is NULL");
	}

	PgxcNodeGetOids(&co_oids, &dn_oids, &num_coords, &num_dnodes, false);

	if(!IS_CENTRALIZED_MODE)
	{
		pgxc_execute_on_nodes(num_coords, co_oids, query_str);
		pgxc_execute_on_nodes(num_dnodes, dn_oids, query_str);
	}
	else
	{
		SPI_connect();
		SPI_execute(query_str, false, 1);
		SPI_finish();
	}

    return BoolGetDatum(true);
}

/*
 * there are basic functions, get object id with name, 
 * cause oid on nodes maybe different, so get the oid locally.
 */
Datum pg_get_table_oid_by_name(PG_FUNCTION_ARGS)
{
	text	   *tablename = PG_GETARG_TEXT_PP(0);
	Oid			tableoid;

	tableoid = convert_table_name(tablename);

    PG_RETURN_OID(tableoid);
}

Datum pg_get_role_oid_by_name(PG_FUNCTION_ARGS)
{
    Name		rolename = PG_GETARG_NAME(0);
	Oid			roleoid;

	roleoid = get_role_oid(NameStr(*rolename), false);

    PG_RETURN_OID(roleoid);
}

Datum pg_get_function_oid_by_name(PG_FUNCTION_ARGS)
{
    Name		funcname = PG_GETARG_NAME(0);
	Oid			funcoid;
    List *      funcnamelist;
    List *      funcoidlist;

    funcnamelist = list_make1(NameStr(*funcname));

    funcoidlist  = FunctionGetOidsByNameString(funcnamelist);
    
    funcoid      = list_nth_oid(funcoidlist, 0);

    PG_RETURN_OID(funcoid);
}

Datum pg_get_schema_oid_by_name(PG_FUNCTION_ARGS)
{
	Oid			schemaoid;
    Name		nspname = PG_GETARG_NAME(0);
    
	schemaoid  = get_namespace_oid(NameStr(*nspname), false);

    PG_RETURN_OID(schemaoid);
}

Datum pg_get_tablespace_oid_by_tablename(PG_FUNCTION_ARGS)
{
    Oid			  relid;
	Oid			  spcoid;
    HeapTuple	  reltup;
	Form_pg_class relform;
    text	   *  tablename;
	
    tablename = PG_GETARG_TEXT_PP(0);

    relid     = convert_table_name(tablename);
    
	reltup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(reltup))
	{
		elog(ERROR, "cache lookup failed for relation %u", relid);
	}
	relform = (Form_pg_class) GETSTRUCT(reltup);

    if (InvalidOid == relform->reltablespace)
    {
        spcoid = DEFAULTTABLESPACE_OID;
    }
    else
    {
        spcoid = relform->reltablespace;
    }

    ReleaseSysCache(reltup);

    PG_RETURN_OID(spcoid);
}

Datum pg_get_tablespace_name_by_tablename(PG_FUNCTION_ARGS)
{
    Oid			  relid;
	Oid			  spcoid;
    Name          spcname;
    HeapTuple	  reltup;
	Form_pg_class relform;
    text	   *  tablename;
	Form_pg_tablespace spcform;
    
    tablename = PG_GETARG_TEXT_PP(0);

    /* get relid of table string line */
    relid     = convert_table_name(tablename);

    /* get spaceoid of relid */
	reltup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(reltup))
	{
		elog(ERROR, "cache lookup failed for relation %u", relid);
	}
	relform = (Form_pg_class) GETSTRUCT(reltup);
    if (InvalidOid == relform->reltablespace)
    {
        spcoid = DEFAULTTABLESPACE_OID;
    }
    else
    {
        spcoid = relform->reltablespace;
    }
    ReleaseSysCache(reltup);

    /* alloc result set */
    spcname = (Name) palloc(NAMEDATALEN);
    memset(NameStr(*spcname), 0, NAMEDATALEN);

    /* get spcname of spaceoid */
    reltup = SearchSysCache1(TABLESPACEOID, ObjectIdGetDatum(spcoid));
	if (!HeapTupleIsValid(reltup))
	{
		elog(ERROR, "cache lookup failed for tablespace %u", spcoid);
	}
	spcform = (Form_pg_tablespace) GETSTRUCT(reltup);
    namestrcpy(spcname, NameStr(spcform->spcname));
    ReleaseSysCache(reltup);
    
    /* here we go */
    PG_RETURN_NAME(spcname);
}

Datum pg_get_current_database_oid(PG_FUNCTION_ARGS)
{
    PG_RETURN_OID(MyDatabaseId);
}

Datum pg_transparent_crypt_support_datatype(PG_FUNCTION_ARGS)
{
    Oid datatype;
    bool support;

    datatype = PG_GETARG_OID(0);

    support = mls_support_data_type(datatype);
    
    PG_RETURN_BOOL(support);
}
#endif

#if MARK("mls permission control")
/*
 * check relid having mls policy bound. 
 * at the same time, schema_bound means wether the schema where relid belongs is bound mls policy,
 * under this condition, the output arg is usefull, one relation could be dropped directly whose schema is crypted,
 * cause, the relation would be crypted when created again, 
 * so there is no corner for somebody who wants to escape relfile crypto by a 'fake' with the same relname.
 */
bool mls_check_relation_permission(Oid relid, bool * schema_bound)
{
    Oid  parent_oid;

    if (!IS_SYSTEM_REL(relid))
    {
        if (schema_bound)
        {
            *schema_bound = false;
        }
        
        parent_oid = mls_get_parent_oid_by_relid(relid);
        
        if (datamask_check_table_has_datamask(parent_oid) ||
		        datamask_check_table_has_datamask(relid))
        {
            return true;
        }

//        if (transparent_crypt_check_table_has_crypto(parent_oid,  true, schema_bound) ||
//		        transparent_crypt_check_table_has_crypto(relid,  true, schema_bound))
//        {
//            return true;
//        }

        if (cls_check_table_has_policy(parent_oid) ||
                cls_check_table_has_policy(relid))
        {
        	return true;
        }

    }

    return false;
}

bool mls_check_schema_permission(Oid schemaoid)
{
    bool        found;
    List       *relid_list;
    ListCell   *cell;
    Oid         relOid;

    found      = false;
    relid_list = NIL;

    relid_list = mls_get_relid_list_in_schema(schemaoid);
    if (NIL != relid_list)
    {
    	foreach(cell, relid_list)
    	{
    		relOid = lfirst_oid(cell);

            found = mls_check_relation_permission(relOid, NULL);
            if (true == found)
            {
                return found;
            }
    	}
    }

    return found;
}

bool mls_check_column_permission(Oid relid, int attnum)
{
    Oid  parent_oid;

    if (!IS_SYSTEM_REL(relid))
    {
        parent_oid = mls_get_parent_oid_by_relid(relid);

        if (datamask_check_table_col_has_datamask(parent_oid, attnum) ||
		        datamask_check_table_col_has_datamask(relid, attnum))
        {
            return true;
        }

//        if (transparent_crypt_check_table_col_has_crypto(parent_oid, attnum) ||
//		        transparent_crypt_check_table_col_has_crypto(relid, attnum))
//        {
//            return true;
//        }

        if (cls_check_table_col_has_policy(parent_oid, attnum) ||
		        cls_check_table_col_has_policy(relid, attnum))
        {
            return true;
        }        
    }

    return false;

}

bool mls_check_role_permission(Oid roleid)
{
    bool found;

    found = datamask_check_user_in_white_list(roleid);

    return found;
}

bool mls_user(void)
{
    if (DEFAULT_ROLE_MLS_SYS_USERID == GetAuthenticatedUserId())
    {
        return true;
    }
    return false;
}

/*
 * pooler connection option contains MLS_CONN_OPTION, so, if exists, skip md5 authentication.
 */
bool mls_check_inner_conn(const char * cmd_option)
{
    if (cmd_option)
    {
        if(strstr(cmd_option, MLS_CONN_OPTION) != NULL)
        {
            return true;
        }
    }
    return false;
}
#endif

#if MARK("datamask")
void mls_check_datamask_need_passby(ScanState * scanstate, Oid relid)
{
    Oid parent_oid;

    if (!g_enable_data_mask)
        return;
    
    if (InvalidOid == relid)
    {
        scanstate->ps.skip_data_mask_check = DATA_MASK_SKIP_ALL_TRUE;
        return;
    }
    
    parent_oid = mls_get_parent_oid_by_relid(relid);

    if (datamask_check_table_has_datamask(parent_oid))
    {
        if (false == datamask_check_user_and_column_in_white_list(parent_oid, GetUserId(), -1))
        {
            scanstate->ps.skip_data_mask_check = DATA_MASK_SKIP_ALL_FALSE;
            return;
        }
    }

    scanstate->ps.skip_data_mask_check = DATA_MASK_SKIP_ALL_TRUE;
    
    return;
}
#endif

/*
 * cls and audit add several system tables, we manage them in this sample way.
 */
int transfer_rel_kind_ext(Oid relid)
{    
    switch (relid)
    {
        case ClsCompartmentRelationId:
        case ClsGroupRelationId:
        case ClsLabelRelationId:
        case ClsLevelRelationId:
        case ClsPolicyRelationId:
        case ClsTableRelationId:
        case ClsUserRelationId:
        case DataMaskMapRelationId:
        case DataMaskUserRelationId:
            return RELKIND_MLS_SYS_TABLE;
            break;
        case PgAuditObjDefOptsRelationId:
        case PgAuditObjConfRelationId:
        case PgAuditStmtConfRelationId:
        case PgAuditUserConfRelationId:
        case PgAuditFgaConfRelationId:
            return RELKIND_AUDIT_SYS_TABLE;
            break;
        default:
            break;
    }

    if (relid < FirstNormalObjectId)
    {
        return RELKIND_SYS_TABLE;
    }

    return RELKIND_NORMAL_TABLE;
}

/*
 * while, we define three role kind to extend orginal role kind, 
 * cls user in charge of security, audit user in charge audit event.
 */
int transfer_rol_kind_ext(Oid rolid)
{
    if (DEFAULT_ROLE_MLS_SYS_USERID == rolid)
    {
        return ROLE_MLS_USER;
    }
    else if (DEFAULT_ROLE_AUDIT_SYS_USERID == rolid)
    {
        return ROLE_AUDIT_USER;
    }

    return ROLE_NORMAL_USER;
}

bool is_mls_or_audit_user(void)
{
    if ((DEFAULT_ROLE_MLS_SYS_USERID == GetAuthenticatedUserId()) 
        || (DEFAULT_ROLE_AUDIT_SYS_USERID == GetAuthenticatedUserId()))
    {
        return true;
    }
    return false;
}

Oid mls_get_parent_oid(Relation rel)
{
    return mls_get_parent_oid_by_relid(rel->rd_id);
}

/*
 * return parent oid if exists, or return itself
 */
Oid mls_get_parent_oid_by_relid(Oid relid)
{
	Oid parent_oid = InvalidOid;
	Oid tbl_oid;
	HeapTuple tp;
	HeapTuple tbl_tp  = NULL;

	if (!IS_SYSTEM_REL(relid))
	{
		tbl_oid = relid;

		tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
		if (HeapTupleIsValid(tp))
		{
			Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);

			/* convert index oid to relation oid */
			if (RELKIND_INDEX == reltup->relkind)
			{
				tbl_oid = IndexGetRelation(relid, false);
				tbl_tp  = SearchSysCache1(RELOID, ObjectIdGetDatum(tbl_oid));
				reltup  = (Form_pg_class)GETSTRUCT(tbl_tp);
			}

			if (reltup->relispartition)
			{
				parent_oid = get_partition_parent(tbl_oid);
			}
			else
			{
				parent_oid = tbl_oid;
			}

			if (HeapTupleIsValid(tbl_tp))
			{
				ReleaseSysCache(tbl_tp);
			}
			ReleaseSysCache(tp);
		}
	}

	return parent_oid;
}

/*
 * focus on datatype supported currently, if new datatype added, this is the enterance function on critical path
 */
bool mls_support_data_type(Oid typid)
{
    switch(typid)
    {
        case TIMESTAMPOID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
            
        case BPCHAROID:     /* char */
        case VARCHAR2OID:        
        case VARCHAROID:
        case TEXTOID:
        case BYTEAOID:
        
            return true;
            break;
        default:
            break;
    }
    return false;
}



/*
 * all relative row level control feature enterance, such as datamask, cls, tranparent crypt.
 */
void MlsExecCheck(ScanState *node, TupleTableSlot *slot)
{
    Oid parent_oid = InvalidOid;
    
    if (g_enable_cls || g_enable_data_mask)
    {
        if (node)
        {
            if (node->ss_currentRelation)
            {
                if (node->ss_currentRelation->rd_att)
                {
                    /* 
                     * entrance: datamask 
                     */
                    if (g_enable_data_mask)
                    {
                        if (node->ss_currentRelation->rd_att->tdatamask)
                        {
                            /* 
                             * skip_data_mask_check is assigned in execinitnode, 
                             * so concurrent changing to datamask has not effect on current select 
                             */
                            if (DATA_MASK_SKIP_ALL_TRUE != node->ps.skip_data_mask_check)
                            {
                                parent_oid = mls_get_parent_oid(node->ss_currentRelation);
                                datamask_exchange_all_cols_value((Node*)node, slot, parent_oid);
                            }
                        }
                    }
                }
            }
        }
    }
    return;
}

/*
 * return children part list if exist, including interval and original partitions
 * NOTE, the parent relation is not included in returning list, so if this is a normal relation, NIL is returned.
 */
List * FetchAllParitionList(Oid relid)
{
    char     relkind;
    Relation rel;
    List    *children = NIL;
    /* in case the relation has already been eliminated */
    rel = try_relation_open(relid, NoLock);
    if (NULL == rel)
    {
        return NIL;
    }

    relkind = rel->rd_rel->relkind ;

    relation_close(rel, NoLock);

    /* treat partition tables */
    if (NIL == children && RELKIND_PARTITIONED_TABLE == relkind )
    {
        children = find_all_inheritors(relid, AccessShareLock, NULL);
    }

    return children;
}

/*
 * this function cover two kinds of partition, interval and original partition
 */
void CacheInvalidateRelcacheAllPartitions(Oid databaseid, Oid relid)
{
    List    *children = NIL;

    children = FetchAllParitionList(relid);

    if (NIL != children)
    {
		ListCell *	lc;
		Oid 		partoid;

		foreach(lc, children)
		{
			partoid = lfirst_oid(lc);
            RegisterRelcacheInvalidMsg(databaseid, partoid);
		}
    }

    return;
}

/*
 * hide sensitive info from query string.
 *
 * we cut off from the first '(', cause, infos like password must appear in (clause_stmt).
 *
 * new querystring is allocateing from MessageContext, which would be reset for every query,
 * so need to worry about memory leak.
 */
const char * mls_query_string_prune(const char * querystring)
{
    char *        string_prune = NULL;
    char *        string_delimeter;
    int           string_len;
    
    if (querystring)
    {
        string_delimeter = strchr(querystring, MLS_QUERY_STRING_PRUNE_DELIMETER);

        if (NULL == string_delimeter)
        {
            return querystring;
        }

        string_len = string_delimeter - querystring;
        
        string_prune = palloc(string_len + 1);
        
        memcpy(string_prune, querystring, string_len);

        string_prune[string_len] = '\0';
    }
    
    return string_prune;
}

bool is_mls_user(void)
{
    if (DEFAULT_ROLE_MLS_SYS_USERID == GetAuthenticatedUserId())
    {
        return true;
    }
    return false;
}

