#include "postgres.h"
#include "postgres_ext.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/xlogreader.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_audit.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_type.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_mls.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "contrib/pgcrypto/pgp.h"
#include "executor/tuptable.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "parser/parse_relation.h"
#include "storage/bufmgr.h"
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
#include "pgxc/pgxcnode.h"
#include "pgxc/nodemgr.h"
#include "pgstat.h"



#if MARK("datamask")

/*just test stub */
#define DATAMASK_TEST_TEXT_STUB     "XXXX"
#define DATAMASK_TEST_INT8_STUB     PG_INT8_MAX
#define DATAMASK_TEST_INT16_STUB    PG_INT16_MAX
#define DATAMASK_TEST_INT32_STUB    PG_INT32_MAX
#define DATAMASK_TEST_INT64_STUB    PG_INT64_MAX

/*
 * value of option in pg_data_mask_map, indentify kinds of data mask.
 */
enum
{
    DATAMASK_KIND_INVALID       = 0,
    DATAMASK_KIND_VALUE         = 1,    /* exchange value with datamask */
    DATAMASK_KIND_STR_PREFIX    = 2,    /* to mask several characters in string, the 'several' is indendified by datamask */
    DATAMASK_KIND_DEFAULT_VAL   = 3,    /* datamask use default value kept in pg_data_mask_map.defaultval */
    DATAMASK_KIND_STR_POSTFIX   = 4,    /* same as DATAMASK_KIND_STR_PREFIX, except treating string from end to begin */
    DATAMASK_KIND_FUNC_PROCESS  = 5,    /* value treate by identified function */
    DATAMASK_KIND_BUTT
};

static Datum datamask_exchange_one_col_value(Oid relid, Form_pg_attribute attr, Datum inputval, bool isnull, bool *datumvalid);
static bool datamask_attr_mask_is_valid(Datamask   *datamask, int attnum);
static char * transfer_str_prefix(text * text_str, int mask_bit_count);
static char * transfer_str_postfix(text * text_str, int mask_bit_count);
static int calculate_character_cnt(char * input_str, int input_str_len);

static int calculate_character_cnt(char * input_str, int input_str_len)
{
    int loop;
    int character_cnt = 0;
    
    for(loop = 0; loop < input_str_len; )
    {
        if (IS_HIGHBIT_SET(input_str[loop]) && PG_SQL_ASCII == GetDatabaseEncoding())
        {
            loop          += 3;
            character_cnt += 1;
        }
        else
        {
            loop          += 1;
            character_cnt += 1;
        }
    }    

    return character_cnt;
}


/*
 * relative to input_str, to alloc a new memory, and exchange serveral chars from end to begin
 * and returns up to max('mask_bit_count', strlen(input_str) characters
 * if input_str is null, a string of 'X' with 'mask_bit_count' length would be returned
 */
 
static char * transfer_str_postfix(text * text_str, int mask_bit_count)
{
    char *  str;
    char *  input_str = NULL;
    int     bit_count;
    int     loop;
    int     input_str_len     = 0;
    int     character_cnt     = 0;
    int     begin_to_mask_idx = 0;
    int     dst_loop;
    int     input_loop;
    int     character_idx;
    
    /* string mask must be valid */
    Assert(mask_bit_count);

    if (text_str)
    {
        input_str     = VARDATA_ANY(text_str);
        input_str_len = VARSIZE_ANY_EXHDR(text_str);
    }

    if (input_str)
    {
        character_cnt = calculate_character_cnt(input_str, input_str_len);
        begin_to_mask_idx = 0;
        if (character_cnt > mask_bit_count)
        {
            begin_to_mask_idx = character_cnt - mask_bit_count;
        }
    }

    if ((character_cnt > mask_bit_count) && (0 != input_str_len))
    {
        input_str     = VARDATA_ANY(text_str);
        
        str = palloc0(input_str_len + 1);

        for (dst_loop = 0, input_loop = 0, character_idx = 0; 
            input_loop < input_str_len && character_idx < begin_to_mask_idx && character_idx < character_cnt;)
        {
            if (IS_HIGHBIT_SET(input_str[input_loop]) && PG_SQL_ASCII == GetDatabaseEncoding())
            {
                for(loop = 0; loop < 3; loop++)
                {
                    str[dst_loop] = input_str[input_loop];
                    input_loop    += 1;
                    dst_loop      += 1;
                }
                character_idx += 1;
            }
            else
            {
                str[dst_loop] = input_str[input_loop];
                input_loop    += 1;
                dst_loop      += 1;
                character_idx += 1;
            }
        }

        for (bit_count = 0;
            input_loop < input_str_len && begin_to_mask_idx < character_cnt && bit_count < mask_bit_count;
            input_loop++, dst_loop++, begin_to_mask_idx++)
        {
            str[dst_loop] = 'X';
        }

        str[dst_loop] = '\0';

#if 0        
        input_str_len = strlen(input_str);

        str = palloc(input_str_len + 1);
        strncpy(str, input_str, input_str_len);
        str[input_str_len] = '\0';

        for (loop = input_str_len - 1, bit_count = 0; 
            loop >=0 && bit_count < mask_bit_count;
            loop--,bit_count++)
        {
            str[loop] = 'X';
        }
#endif
    }
    else
    {
        str = palloc(mask_bit_count + 1);
        for(loop = 0; loop < mask_bit_count; loop++)
        {
            str[loop] = 'X';
        }
        str[mask_bit_count] = '\0';
    }

    return str;
}

/*
 * exactly same as transfer_str_postfix, except the direction of exchanging string
 */
static char * transfer_str_prefix(text * text_str, int mask_bit_count)
{
    char *  input_str = NULL;
    char *  str;
    int     input_str_len = 0;
    int     bit_count;
    int     loop;
    int     character_cnt = 0;
    int     dst_loop;
    int     input_loop;
    int     character_idx;

    /* string mask must be valid */
    Assert(mask_bit_count);

    if (text_str)
    {
        input_str     = VARDATA_ANY(text_str);
        input_str_len = VARSIZE_ANY_EXHDR(text_str);
        character_cnt = calculate_character_cnt(input_str, input_str_len);
    }
    
    if (character_cnt > mask_bit_count && 0 != input_str_len)
    {
        input_str     = VARDATA_ANY(text_str);
        
        str = palloc0(input_str_len + 1);

        for (dst_loop = 0, input_loop = 0, bit_count = 0, character_idx = 0; 
            input_loop < input_str_len && bit_count < mask_bit_count && character_idx < character_cnt;)
        {
            if (IS_HIGHBIT_SET(input_str[input_loop]) && PG_SQL_ASCII == GetDatabaseEncoding())
            {
                str[dst_loop] = 'X';
                input_loop    += 3;
                dst_loop      += 1;
                character_idx += 1;
                bit_count     += 1;
            }
            else
            {
                str[dst_loop] = 'X';
                input_loop    += 1;
                dst_loop      += 1;
                character_idx += 1;
                bit_count     += 1;
            }
        }

        for(;input_loop < input_str_len; input_loop++, dst_loop++)
        {
            str[dst_loop] = input_str[input_loop];
        }

        str[dst_loop] = '\0';
#if 0        
        strncpy(str, input_str, input_str_len);
        str[input_str_len] = '\0';

        for (loop = 0, bit_count = 0; 
            loop < input_str_len && bit_count < mask_bit_count;
            loop++, bit_count++)
        {
            str[loop] = 'X';
        }
#endif            
    }
    else
    {
        str = palloc0(mask_bit_count + 1);
        for(loop = 0; loop < mask_bit_count; loop++)
        {
            str[loop] = 'X';
        }
        str[mask_bit_count] = '\0';
    }

    return str;
}

bool datamask_check_user_and_column_in_white_list(Oid relid, Oid userid, int16 attnum)
{
    SysScanDesc scan;
    ScanKeyData skey[3];
    HeapTuple   htup;
    Relation    rel;
    bool        found;

    found = false;
    
    ScanKeyInit(&skey[0],
                    Anum_pg_data_mask_user_relid,
                    BTEqualStrategyNumber, 
                    F_OIDEQ,
                    ObjectIdGetDatum(relid));
    ScanKeyInit(&skey[1],
                    Anum_pg_data_mask_user_userid,
                    BTEqualStrategyNumber, 
                    F_OIDEQ,
                    ObjectIdGetDatum(userid));
    ScanKeyInit(&skey[2],
                    Anum_pg_data_mask_user_attnum,
                    BTEqualStrategyNumber, 
                    F_INT2EQ,
                    Int16GetDatum(attnum));
    
    rel = heap_open(DataMaskUserRelationId, AccessShareLock);
    scan = systable_beginscan(rel, 
                              PgDataMaskUserIndexId, 
                              true,
                              NULL, 
                              3, 
                              skey);

    if (HeapTupleIsValid(htup = systable_getnext(scan)))
    {
        Form_pg_data_mask_user form_pg_datamask_user = (Form_pg_data_mask_user) GETSTRUCT(htup);
        if (true == form_pg_datamask_user->enable)
        {
            found = true;
        }
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return found;
}

/*
 * return list of user name in white list
 * 
 * allocating in current memory context, so remember to free it.
 */
List * datamask_get_user_in_white_list(void)
{
    List * retlist = NIL;
    SysScanDesc scan;
    HeapTuple   htup;
    Relation    rel;
    HeapTuple   rtup;
    
    rel = heap_open(DataMaskUserRelationId, AccessShareLock);
    scan = systable_beginscan(rel, 
                              InvalidOid, 
                              false,
                              NULL, 
                              0, 
                              NULL);

    while (HeapTupleIsValid(htup = systable_getnext(scan)))
    {
        Form_pg_data_mask_user form_pg_datamask_user = (Form_pg_data_mask_user) GETSTRUCT(htup);
        if (true == form_pg_datamask_user->enable)
        {
            rtup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(form_pg_datamask_user->userid));
        	if (HeapTupleIsValid(rtup))
        	{
                Name username = palloc(sizeof(NameData));

                memcpy(username->data, NameStr(form_pg_datamask_user->username), sizeof(NameData));

                retlist = lappend(retlist, username);
            }
            ReleaseSysCache(rtup);
        }
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return retlist;
}

bool datamask_check_user_in_white_list(Oid userid)
{
    SysScanDesc scan;
    ScanKeyData skey[1];
    HeapTuple   htup;
    Relation    rel;
    bool        found;

    if (false == g_enable_data_mask)
    {
        return false;
    }

    found = false;
    
    ScanKeyInit(&skey[0],
                    Anum_pg_data_mask_user_userid,
                    BTEqualStrategyNumber, 
                    F_OIDEQ,
                    ObjectIdGetDatum(userid));
   
    rel = heap_open(DataMaskUserRelationId, AccessShareLock);
    scan = systable_beginscan(rel, 
                              PgDataMaskUserIndexId, 
                              true,
                              NULL, 
                              1, 
                              skey);

    while (HeapTupleIsValid(htup = systable_getnext(scan)))
    {
        Form_pg_data_mask_user form_pg_datamask_user = (Form_pg_data_mask_user) GETSTRUCT(htup);
        if (true == form_pg_datamask_user->enable)
        {
            found = true;
            break;
        }
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return found;
}


/*
 *  exchange one column once, while, only several basic type supported, 
 *      such as integer(int2\int4\int8),varchar,text 
 *  the col 'datamask' of pg_data_mask_map, that would be more flexible.
 */
static Datum datamask_exchange_one_col_value(Oid relid, 
                                                        Form_pg_attribute attr, 
                                                        Datum inputval, 
                                                        bool isnull,
                                                        bool *datumvalid)
{
    SysScanDesc scan;
    ScanKeyData skey[2];
    HeapTuple   htup;
    Relation    rel;
    bool        unknown_option_kind;
    bool        unsupport_data_type;
    Datum       value;
    char       *ret_str;
    int         option;
    int         typmod;
    int         string_len;

    value               = Int32GetDatum(0);
    option              = DATAMASK_KIND_INVALID;
    unknown_option_kind = false;
    unsupport_data_type = false;
    
    if (mls_support_data_type(attr->atttypid))
    {
        if ( true == datamask_check_user_and_column_in_white_list(relid, GetUserId(), attr->attnum))
        {
            *datumvalid = false;
            return value;
        }
        
        ScanKeyInit(&skey[0],
                        Anum_pg_data_mask_map_relid,
                        BTEqualStrategyNumber, 
                        F_OIDEQ,
                        ObjectIdGetDatum(relid));
        ScanKeyInit(&skey[1],
                        Anum_pg_data_mask_map_attnum,
                        BTEqualStrategyNumber, 
                        F_OIDEQ,
                        Int32GetDatum(attr->attnum));
        
        rel = heap_open(DataMaskMapRelationId, AccessShareLock);
        scan = systable_beginscan(rel, 
                                  PgDataMaskMapIndexId, 
                                  true,
                                  NULL, 
                                  2, 
                                  skey);

        if (HeapTupleIsValid(htup = systable_getnext(scan)))
        {
            Form_pg_data_mask_map form_pg_datamask = (Form_pg_data_mask_map) GETSTRUCT(htup);
            if (true == form_pg_datamask->enable)
            {        
                *datumvalid = true;
                option = form_pg_datamask->option;
                switch(option)
                {
                    case DATAMASK_KIND_VALUE:
                        if (INT4OID == attr->atttypid || INT2OID == attr->atttypid || INT8OID == attr->atttypid)
                        {
                            value = Int64GetDatum(form_pg_datamask->datamask);
                        }
                        else
                        {
                            unsupport_data_type = true;
                        }
                        break;
                    case DATAMASK_KIND_STR_PREFIX:
                        {
                            if (VARCHAROID == attr->atttypid 
                                || TEXTOID == attr->atttypid 
                                || VARCHAR2OID == attr->atttypid
                                || BPCHAROID == attr->atttypid)
                            {
                                ret_str = transfer_str_prefix(isnull?NULL:DatumGetTextP(inputval), 
                                                            form_pg_datamask->datamask);

                                value = CStringGetTextDatum(ret_str);
                            }
                            else 
                            {
                                unsupport_data_type = true;
                            }
                            break;
                        }
                    case DATAMASK_KIND_STR_POSTFIX:
                        {
                            if (VARCHAROID == attr->atttypid 
                                || TEXTOID == attr->atttypid 
                                || VARCHAR2OID == attr->atttypid
                                || BPCHAROID == attr->atttypid )
                            {
                                ret_str = transfer_str_postfix(isnull?NULL:DatumGetTextP(inputval), 
                                                            form_pg_datamask->datamask);

                                value = CStringGetTextDatum(ret_str);
                            }
                            else 
                            {
                                unsupport_data_type = true;
                            }
                            break;
                        }
                    case DATAMASK_KIND_DEFAULT_VAL:
                        {
                            if (TIMESTAMPOID == attr->atttypid)
                            {
                                value =  OidInputFunctionCall(TIMESTAMP_IN_OID, 
                                                              TextDatumGetCString(&(form_pg_datamask->defaultval)),
                                                              TIMESTAMPOID,
                                                              -1); /* the typmod of timestamp is -1 */
                            }
                            else if (FLOAT4OID ==  attr->atttypid)
                            {
                                value =  OidInputFunctionCall(FLOAT4_IN_OID, 
                                                              TextDatumGetCString(&(form_pg_datamask->defaultval)),
                                                              TIMESTAMPOID,
                                                              -1); /* the typmod of float4 is -1 */
                            }
                            else if (FLOAT8OID ==  attr->atttypid)
                            {
                                value =  OidInputFunctionCall(FLOAT8_IN_OID, 
                                                              TextDatumGetCString(&(form_pg_datamask->defaultval)),
                                                              TIMESTAMPOID,
                                                              -1); /* the typmod of float8 is -1 */
                            }
                            else if (BPCHAROID ==  attr->atttypid)
                            {
                                string_len = strlen(TextDatumGetCString(&(form_pg_datamask->defaultval)));
                                if (string_len > (attr->atttypmod - VARHDRSZ))
                                {
                                    typmod = -1;
                                }
                                else
                                {
                                    typmod = attr->atttypmod;
                                }
                                value =  OidInputFunctionCall(BPCHAR_IN_OID, 
                                                              TextDatumGetCString(&(form_pg_datamask->defaultval)),
                                                              BPCHAROID,
                                                              typmod);
                            }
                            else if (VARCHAR2OID ==  attr->atttypid)
                            {
                                value =  OidInputFunctionCall(VARCHAR2_IN_OID, 
                                                              TextDatumGetCString(&(form_pg_datamask->defaultval)),
                                                              VARCHAR2OID,
                                                              -1); /* the typmod of varchar2 is -1 */
                            }
                            else if (NUMERICOID ==  attr->atttypid)
                            {
                                value =  OidInputFunctionCall(NUMERIC_IN_OID, 
                                                              TextDatumGetCString(&(form_pg_datamask->defaultval)),
                                                              NUMERICOID,
                                                              -1); /* the typmod of numeric is -1 */
                            }
                            else if (VARCHAROID ==  attr->atttypid)
                            {
                                value =  OidInputFunctionCall(VARCHAR_IN_OID, 
                                                              TextDatumGetCString(&(form_pg_datamask->defaultval)),
                                                              VARCHAROID,
                                                              -1); /* the typmod of varchar is -1 */
                            }
                            else if (TEXTOID ==  attr->atttypid)
                            {
                                value =  OidInputFunctionCall(TEXT_IN_OID, 
                                                              TextDatumGetCString(&(form_pg_datamask->defaultval)),
                                                              TEXTOID,
                                                              -1); /* the text of varchar is -1 */
                            }
                            else
                            {
                                unsupport_data_type = true;
                            }
                            break;
                        }
                    default:
                        unknown_option_kind = true;
                        break;
                }
            }
        }

        systable_endscan(scan);
        heap_close(rel, AccessShareLock);
    }
    else
    {
        unsupport_data_type = true;
    }

    if (unknown_option_kind)
    {
        elog(ERROR, "datamask:unsupport type, typid:%d for option:%d", attr->atttypid, option);
    }

    if (unsupport_data_type)
    {
        elog(ERROR, "datamask:unsupport type, typid:%d for option:%d", attr->atttypid, option);
    }
    
    return value;
}

/* 
 *  one col needs data masking when the corresponding mask_array is valid
 */
static bool datamask_attr_mask_is_valid(Datamask *datamask, int attnum)
{
    if (datamask)
    {
        return datamask->mask_array[attnum];
    }
    return false;
}

/* 
 * after tuple deform to slot, exchange the col values with those defined by user or defaults.
 */
void datamask_exchange_all_cols_value(Node *node, TupleTableSlot *slot, Oid relid)
{
    bool        need_exchange_slot_tts_tuple;
    bool        datumvalid;
    int         attnum;
    int         natts;
    TupleDesc   tupleDesc;
    Datum      *tuple_values;
    bool       *tuple_isnull;
    Datum      *slot_values;
    bool       *slot_isnull;
    Datamask   *datamask;
    HeapTuple   new_tuple;
    MemoryContext      old_memctx;
    ScanState * scanstate;

    scanstate   = (ScanState *)node;
    tupleDesc   = slot->tts_tupleDescriptor;
    datamask    = tupleDesc->tdatamask;
    natts       = tupleDesc->natts;
    
    slot_values = slot->tts_values;
    slot_isnull = slot->tts_isnull;
    
    need_exchange_slot_tts_tuple = false;
    
    if (datamask)
    {
        if (slot->tts_tuple)
        {
            need_exchange_slot_tts_tuple = true;
        }

        old_memctx = MemoryContextSwitchTo(slot->tts_mls_mcxt);

        if (need_exchange_slot_tts_tuple)
        {
            tuple_values = (Datum *) palloc(natts * sizeof(Datum));
            tuple_isnull = (bool *) palloc(natts * sizeof(bool));

            heap_deform_tuple(slot->tts_tuple, tupleDesc, tuple_values, tuple_isnull);
            
        }
        
        for (attnum = 0; attnum < natts; attnum++)
        {
            if (datamask_attr_mask_is_valid(datamask, attnum))
            {
                Form_pg_attribute thisatt = TupleDescAttr(tupleDesc, attnum);

                datumvalid = false;
                if (need_exchange_slot_tts_tuple)
                {
                    slot_values[attnum]  = datamask_exchange_one_col_value(relid, 
                                                                            thisatt, 
                                                                            tuple_values[attnum], 
                                                                            tuple_isnull[attnum],
                                                                            &datumvalid);
                }
                else
                {
                    /* tuple_values are null, so try slot_values */
                    slot_values[attnum]  = datamask_exchange_one_col_value(relid, 
                                                                            thisatt, 
                                                                            slot_values[attnum], 
                                                                            slot_isnull[attnum],
                                                                            &datumvalid);
                }
                slot_isnull[attnum]  = false;

                /* 
                 * if datum is invalid, slot_values is invalid either, keep orginal value in tuple_value
                 * it seems a little bored
                 */
                if (need_exchange_slot_tts_tuple && datumvalid)
                {
                    tuple_values[attnum] = slot_values[attnum];
                    tuple_isnull[attnum] = slot_isnull[attnum];
                }
            }
        }

        if (need_exchange_slot_tts_tuple)
        {
            /* do not forget to set shardid */
            if (RelationIsSharded(scanstate->ss_currentRelation))
            {
                new_tuple = heap_form_tuple_plain(tupleDesc, tuple_values, tuple_isnull, RelationGetDisKeys(scanstate->ss_currentRelation),
					                              RelationGetNumDisKeys(scanstate->ss_currentRelation), RelationGetRelid(scanstate->ss_currentRelation));
            }
            else
            {
                new_tuple = heap_form_tuple(tupleDesc, tuple_values, tuple_isnull);
            }

            /* remember to do this copy manually */
            new_tuple->t_self       = slot->tts_tuple->t_self;
            new_tuple->t_tableOid   = slot->tts_tuple->t_tableOid;
            new_tuple->t_xc_node_id = slot->tts_tuple->t_xc_node_id;

            if (TTS_SHOULDFREE(slot))
            {
                heap_freetuple(slot->tts_tuple);
            }
            
            slot->tts_tuple      = new_tuple;
            slot->tts_flags |= TTS_FLAG_SHOULDFREE;

            /* fresh tts_values in slot */
            slot_deform_tuple_extern((void*)slot, natts);        
            
            pfree(tuple_values);
            pfree(tuple_isnull);
        }

        MemoryContextSwitchTo(old_memctx);
    }
    
    return;
}

/*
 * a little quick check whether this table binding a datamask.
 */
bool datamask_check_table_has_datamask(Oid relid)
{
    SysScanDesc scan;
    ScanKeyData skey[1];
    HeapTuple   htup;
    Relation    rel;
    bool        hasdatamask;

	if (!g_enable_data_mask)
		return false;

    ScanKeyInit(&skey[0],
                    Anum_pg_data_mask_map_relid,
                    BTEqualStrategyNumber, 
                    F_OIDEQ,
                    ObjectIdGetDatum(relid));

    rel = heap_open(DataMaskMapRelationId, AccessShareLock);
    scan = systable_beginscan(rel, 
                              PgDataMaskMapIndexId, 
                              true,
                              NULL, 
                              1, 
                              skey);

    hasdatamask = false;
    while (HeapTupleIsValid(htup = systable_getnext(scan)))
    {
        Form_pg_data_mask_map form_data_mask_map = (Form_pg_data_mask_map) GETSTRUCT(htup);
        if (true == form_data_mask_map->enable)
        {
            hasdatamask = true;
            break;
        }
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return hasdatamask;
}

/*
 * here, we already known this table coupled with datamask, so check attributes one by one to mark.
 */
bool datamask_check_table_col_has_datamask(Oid relid, int attnum)
{
    SysScanDesc scan;
    ScanKeyData skey[2];
    HeapTuple   htup;
    Relation    rel;
    bool        hasdatamask;

    if (false == g_enable_data_mask)
    {
        return false;
    }

    ScanKeyInit(&skey[0],
                    Anum_pg_data_mask_map_relid,
                    BTEqualStrategyNumber, 
                    F_OIDEQ,
                    ObjectIdGetDatum(relid));
    ScanKeyInit(&skey[1],
                    Anum_pg_data_mask_map_attnum,
                    BTEqualStrategyNumber, 
                    F_OIDEQ,
                    Int32GetDatum(attnum));

    rel = heap_open(DataMaskMapRelationId, AccessShareLock);
    scan = systable_beginscan(rel, 
                              PgDataMaskMapIndexId, 
                              true,
                              NULL, 
                              2, 
                              skey);

    hasdatamask = false;
    if (HeapTupleIsValid(htup = systable_getnext(scan)))
    {
        Form_pg_data_mask_map form_pg_datamask = (Form_pg_data_mask_map) GETSTRUCT(htup);
        if (true == form_pg_datamask->enable)
        {
            hasdatamask = true;
        }
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return hasdatamask;
}

static Datamask * datamask_alloc_datamask_struct(int attmasknum)
{
    Datamask * datamask;

    datamask = (Datamask * )palloc(sizeof(Datamask));
    datamask->attmasknum = attmasknum;
    datamask->mask_array = (bool*)palloc0(sizeof(bool)*attmasknum);

    return datamask;
}

/*
 * to make up tupdescfree.
 */ 
void datamask_free_datamask_struct(Datamask *datamask)
{
    if (datamask)
    {
        if (datamask->mask_array)
        {
            pfree(datamask->mask_array);
        }

        pfree(datamask);
    }
    
    return ;
}

/*
 * we place datamask in tupledesc
 */
void datamask_assign_relation_tupledesc_field(Relation relation)
{
    int         attnum;
    int         natts;
    Datamask*   datamask;
    Oid         parent_oid;

    if (false == g_enable_data_mask)
    {
        return;
    }
    
    if (!IS_SYSTEM_REL(RelationGetRelid(relation)))
    {
        parent_oid = mls_get_parent_oid(relation);
        
        /* quick check wether this table has col with datamask */
        if (false == datamask_check_table_has_datamask(parent_oid))
        {
            if (relation->rd_att->tdatamask)
            {
                datamask_free_datamask_struct(relation->rd_att->tdatamask);
            }
            relation->rd_att->tdatamask = NULL;
            return;
        }

        natts = relation->rd_att->natts;

        datamask = (Datamask * )MemoryContextAllocZero(CacheMemoryContext, sizeof(Datamask));
        datamask->attmasknum = natts;
        datamask->mask_array = (bool*)MemoryContextAllocZero(CacheMemoryContext, sizeof(bool)*natts);

        for (attnum = 0; attnum < natts; attnum++)
        {
            /* skip the droped column */
            if (0 == TupleDescAttr(relation->rd_att, attnum)->attisdropped)
            {
                datamask->mask_array[attnum] = datamask_check_table_col_has_datamask(parent_oid, attnum+1);
            }
        }
        
        relation->rd_att->tdatamask = datamask;
    }
    else
    {
        relation->rd_att->tdatamask = NULL;
    }
    
    return;
}

/*
 * to make up tupledesccopy.
 */ 
void datamask_alloc_and_copy(TupleDesc dst, TupleDesc src)
{
    if (false == g_enable_data_mask)
    {
        return;
    }
    
    /*skip system table*/
    if (src->natts > 0)
    {
        if (!IS_SYSTEM_REL(TupleDescAttr(src, 0)->attrelid))
        {
            if (dst->tdatamask)
            {
                datamask_free_datamask_struct(dst->tdatamask);
                dst->tdatamask = NULL;
            }
            if (src->tdatamask)
            {
                if (src->tdatamask->attmasknum)
                {
                    dst->tdatamask = datamask_alloc_datamask_struct(src->tdatamask->attmasknum);

                    memcpy(dst->tdatamask->mask_array, 
                            src->tdatamask->mask_array, 
                            sizeof(bool) * src->tdatamask->attmasknum);
                }
            }
        }
    }
    return;
}

/*
 * to make up the equal judgement of tupledesc.
 */ 
bool datamask_check_datamask_equal(Datamask * dm1, Datamask * dm2)
{
    if (false == g_enable_data_mask)
    {
        return true;
    }

    if ((dm1 && !dm2) || (!dm1 && dm2))
    {
        return false;
    }
    
    if (dm1 == dm2)
    {
        return true;
    }

    if (dm1->attmasknum != dm2->attmasknum)
    {
        return false;
    }

    if (0 != memcpy(dm1->mask_array, dm2->mask_array, dm1->attmasknum))
    {
        return false;
    }

    return true;
}

void datamask_exchange_all_cols_value_copy(TupleDesc tupleDesc, Datum   *tuple_values, bool*tuple_isnull, Oid relid)
{
    int         attnum;
    int         natts;
    Datamask   *datamask;
    Datum       datum_ret;
    bool        datumvalid;
    
    natts    = tupleDesc->natts;
    datamask = tupleDesc->tdatamask;

    for (attnum = 0; attnum < natts; attnum++)
    {
        if (datamask_attr_mask_is_valid(datamask, attnum))
        {
            Form_pg_attribute thisatt = TupleDescAttr(tupleDesc, attnum);

            datumvalid = false;

            datum_ret  = datamask_exchange_one_col_value(relid, 
                                                        thisatt, 
                                                        tuple_values[attnum], 
                                                        tuple_isnull[attnum],
                                                        &datumvalid);

            if (datumvalid)
            {
                tuple_values[attnum] = datum_ret;
            }

            tuple_isnull[attnum]  = false;
        }
    }
    return;
}

bool datamask_check_column_in_expr(Node * node, void * context)
{
    ParseState   * parsestate;
    Var          * var;
    RangeTblEntry* rte;
    bool           found;
    bool           is_legaluser;
    char         * attrname;        
    Oid            parent_oid;
    Oid            relid;
    //ListCell     * cell;
    TargetEntry  * targetentry;
    
    if (node == NULL)
    {
        return false;
    }

    if (IsA(node, Var))
    {   
        parsestate = (ParseState *)context;
        var        = (Var*)node;
        
        if (parsestate->p_rtable)
        {
            rte = GetRTEByRangeTablePosn(parsestate, var->varno, var->varlevelsup);
            if (NULL != rte)
            {
                if (RTE_SUBQUERY == rte->rtekind)
                {
                    targetentry = (TargetEntry*)list_nth(rte->subquery->targetList, var->varattno - 1);

                    if ((InvalidOid != targetentry->resorigtbl) && (InvalidAttrNumber != targetentry->resorigcol))
                    {   
                        parent_oid  = mls_get_parent_oid_by_relid(targetentry->resorigtbl);
                        found       = datamask_check_table_col_has_datamask(parent_oid, targetentry->resorigcol);
                        if (found)
                        {
                            is_legaluser = datamask_check_user_and_column_in_white_list(parent_oid, GetAuthenticatedUserId(), var->varattno);
                            if (!is_legaluser)
                            {
                                attrname = get_relid_attribute_name(parent_oid, targetentry->resorigcol);
                                if (NULL != attrname)
                                {
                                    elog(ERROR, "column(attrname:%s) with datamask policy is forbidden in expression", attrname);
                                }
                                else
                                {
                                    elog(ERROR, "column(attrnum:%d) with datamask policy is forbidden in expression", targetentry->resorigcol);
                                }
                            }
                        }
                    }         
                }
                else if (RTE_RELATION == rte->rtekind)
                {
                    relid = rte->relid;
                    
                    parent_oid = mls_get_parent_oid_by_relid(relid);
                    found      = datamask_check_table_col_has_datamask(parent_oid, var->varattno);
                    if (found)
                    {
                        is_legaluser = datamask_check_user_and_column_in_white_list(parent_oid, GetAuthenticatedUserId(), var->varattno);
                        if (!is_legaluser)
                        {
                            attrname = get_rte_attribute_name(rte, var->varattno);
                            if (NULL != attrname)
                            {
                                elog(ERROR, "column(attrname:%s) with datamask policy is forbidden in expression", attrname);
                            }
                            else
                            {
                                elog(ERROR, "column(attrnum:%d) with datamask policy is forbidden in expression", var->varattno);
                            }
                        }
                    }
                }
            }
        }
    }
    else if (IsA(node, TargetEntry))
    {
        TargetEntry* te;
        
        te = (TargetEntry*)node;
        
        parsestate = (ParseState *)context;
        if (parsestate->p_target_relation)
        {
            relid      = parsestate->p_target_relation->rd_id;
            
            parent_oid = mls_get_parent_oid_by_relid(relid);
            found      = datamask_check_table_col_has_datamask(parent_oid, te->resno);
            if (found)
            {
                is_legaluser = datamask_check_user_and_column_in_white_list(parent_oid, GetAuthenticatedUserId(), te->resno);
                if (!is_legaluser)
                {
                    if (NULL != te->resname)
                    {
                        elog(ERROR, "column(attrname:%s) with datamask policy is forbidden in expression", te->resname);
                    }
                    else
                    {
                        elog(ERROR, "column(attrnum:%d) with datamask policy is forbidden in expression", te->resno);
                    }
                }
            }
        }
    }

    return expression_tree_walker(node, datamask_check_column_in_expr, context);
}


#endif

