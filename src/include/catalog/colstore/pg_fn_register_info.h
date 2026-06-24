#ifndef OPENTENBASE_C_PG_FN_REGISTER_INFO_H__
#define OPENTENBASE_C_PG_FN_REGISTER_INFO_H__

#include "catalog/genbki.h"

#define PgFnRegisterInfoRelationId  8198
#define PgFnRegisterInfoRelation_Rowtype_Id 8199

CATALOG(pg_fn_register_info,8198) BKI_SHARED_RELATION BKI_ROWTYPE_OID(8199) BKI_WITHOUT_OIDS BKI_SCHEMA_MACRO	 
{
	NameData	f_node_name;
	char		f_node_type;
	int32		f_node_port;
	int32		f_max_backends;
	int32		f_window_size;
	int32		f_postmaster_pid;
	int64		f_forwardmgr_ts;
}FormData_pg_fn_register_info;

typedef FormData_pg_fn_register_info *Form_pg_fn_register_info;

#define Natts_pg_fn_register_info					7

#define Anum_pg_fn_register_info_f_node_name		1
#define Anum_pg_fn_register_info_f_node_type		2
#define Anum_pg_fn_register_info_f_node_port		3
#define Anum_pg_fn_register_info_f_max_backends		4
#define Anum_pg_fn_register_info_f_window_size		5
#define Anum_pg_fn_register_info_f_postmaster_pid	6
#define Anum_pg_fn_register_info_f_forwardmgr_ts	7

#endif	 /* OPENTENBASE_C_PG_FN_REGISTER_INFO_H__ */
