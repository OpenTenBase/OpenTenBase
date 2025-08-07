/*-------------------------------------------------------------------------
 *
 * dbms_md_itf.h
 *
 *	Public interface to dbms_metadata.
 *
 *-------------------------------------------------------------------------
 */

#ifndef DBMS_MD_ITF_H
#define DBMS_MD_ITF_H

typedef enum _DBMS_MD_DUMPOBJ_BI
{
	DBMS_MD_DBOJ_TI_MIN = 0,
	DBMS_MD_DOBJ_TABLE = 0,
	DBMS_MD_DOBJ_INDEX = 1,
	DBMS_MD_DOBJ_TYPE = 2,
	DBMS_MD_DOBJ_FUNC = 3,

	DBMS_MD_DBOJ_TI_MAX = 63,
	DBMS_MD_DOBJ_BUTT = 64 /*now dump object type id must less 64*/
}DBMS_MD_DUMPOBJ_BI;

extern void
dump_main(int dobj_type, const char *schema_name, const char *object_name, StringInfo out_buf);


#endif							/* DBMS_MD_ITF_H*/
