#ifndef PG_DBMS_JOB_H
#define PG_DBMS_JOB_H

#include "catalog/genbki.h"

#define timestamp int64
 
#define PgDbmsJobRelationId	9415
 
CATALOG(pg_dbms_job,9415) BKI_ROWTYPE_OID(9423) BKI_SHARED_RELATION BKI_SCHEMA_MACRO
{
	int32 job_id;
	int64 current_postgres_pid;
	NameData log_user;
	NameData priv_user;
	NameData dbname;
	NameData node_name;
	NameData nspname;
	char job_status;
	timestamp start_date;
	timestamp last_start_date;
	timestamp last_end_date;
	timestamp last_suc_date;
	timestamp this_run_date;
	int16 failure_count;
	timestamp next_run_date;
#ifdef CATALOG_VARLEN
	text what;
	text interval;
	text failure_msg;
#endif
} FormData_pg_dbms_job;
 
#define Natts_pg_dbms_job 18
#define Anum_pg_dbms_job_job_id 1
#define Anum_pg_dbms_job_current_postgres_pid 2
#define Anum_pg_dbms_job_log_user 3
#define Anum_pg_dbms_job_priv_user 4
#define Anum_pg_dbms_job_dbname 5
#define Anum_pg_dbms_job_node_name 6
#define Anum_pg_dbms_job_nspname 7
#define Anum_pg_dbms_job_job_status 8
#define Anum_pg_dbms_job_start_date 9
#define Anum_pg_dbms_job_last_start_date 10
#define Anum_pg_dbms_job_last_end_date 11
#define Anum_pg_dbms_job_last_suc_date 12
#define Anum_pg_dbms_job_this_run_date 13
#define Anum_pg_dbms_job_failure_count 14
#define Anum_pg_dbms_job_next_run_date 15
#define Anum_pg_dbms_job_what 16
#define Anum_pg_dbms_job_interval 17
#define Anum_pg_dbms_job_failure_msg 18
 
typedef FormData_pg_dbms_job *Form_pg_dbms_job;

/* Define job status. */
#define PGJOB_RUN_STATUS    'r'
#define PGJOB_SUCC_STATUS   's'
#define PGJOB_FAIL_STATUS   'f'
#define PGJOB_ABORT_STATUS  'd'

/* Status of pg job. */
typedef enum {
	Pgjob_Run, 
	Pgjob_Succ, 
	Pgjob_Fail,   /* Execute what fail */
} Update_Pgjob_Status;

/* Oid of related for delete pg_job. */
typedef enum {
	DbOid,
	UserOid,
	RelOid
} Delete_Pgjob_Oid;


#define JOBID_ALLOC_OK         0                /* alloc jobid ok */
#define JOBID_ALLOC_ERROR      1                /* alloc jobid error */

#define JOBID_MAX_NUMBER  ((uint16)(32767))

extern void update_run_job_to_fail(void);
extern int jobid_alloc(uint16* pusJobId, int64 job_max_number);
extern void check_job_permission(HeapTuple tuple, bool check_running);
extern void	get_job_values(int32 job_id, HeapTuple tup, Relation relation, Datum *values, bool *visnull);
extern Datum job_submit(PG_FUNCTION_ARGS);
extern Datum job_cancel(PG_FUNCTION_ARGS);
extern Datum job_finish(PG_FUNCTION_ARGS);
extern Datum job_update(PG_FUNCTION_ARGS);
extern Datum job_isubmit(PG_FUNCTION_ARGS);
extern void execute_job(int32 job_id);
extern void update_pg_job_dbname(Oid jobid, const char* dbname);
extern void check_interval_valid(int32 job_id, Relation rel, Datum interval);
extern void remove_job_by_oid(Oid oid, Delete_Pgjob_Oid oidFlag, bool local);

#endif

