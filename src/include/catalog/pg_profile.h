/*-------------------------------------------------------------------------
 *
 * pg_profile.h
 *	  definition of the system "resource limit" relation (pg_profile)
 *	  along with the relation's initial contents.
 *
 *
 * src/include/catalog/pg_profile.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PROFILE_H
#define PG_PROFILE_H

#include "catalog/genbki.h"
#include "catalog/pg_profile_user_password.h"
#include "nodes/parsenodes.h"

#define PgProfileRelationId  9038

CATALOG(pg_profile,9038)   BKI_SHARED_RELATION
{
	NameData  profname;           /* profile name */
    float4    profsessions;       /* integer, the number of concurrent sessions to which you want to limit the user */
    float4    profsessioncpu;     /* integer, the CPU time limit for a session, expressed in hundredth of seconds */
    float4    profquerycpu;       /* integer, the CPU time limit for a query, expressed in hundredths of seconds*/
    float4    profcontime;        /* integer, the total elapsed time limit for a session, expressed in minutes */
    float4    profidletime;       /* integer, the permitted periods of continuous inactive time during a session, expressed in minutes */
    float4    profsessionblks;    /* integer, the permitted number of data blocks read in a session, including blocks read from memory and disk */
    float4    profqueryblks;      /* integer, the permitted number of data blocks read in a query, including blocks read from memory and disk */
    float4    profprivatesga;     /* integer, the amount of space a session can allocate in the shared buffers, in MB */
    float4    profcompositelimit; /* integer, the total resource cost for a session */
    float4    proffailedlogtimes; /* integer, the number of consecutive failed attempts to log in to the user account before the account is locked */
	float4    profpasslocktime;   /* the number of days an account will be locked after the specified number of consecutive failed login attempts */
	float4    profpasslifetime;   /* the number of days the same password can be used for authentication */
	float4    profpassgracetime;  /* the number of days after the grace period begins during which a warning is issued and login is allowed */
	float4    profpassreusetime;  /* the number of days before which a password cannot be reused */
    float4    profpassreusemax;   /* integer, the number of password changes required before the current password can be reused */
    float4    profcontainer;      /* integer, not used now */
#ifdef CATALOG_VARLEN			   /* variable-length fields start here */
	text		profpassverifyfunc;	      /* the oid of the password complexity verification function */
#endif
} FormData_pg_profile;

typedef FormData_pg_profile *Form_pg_profile;

#define Natts_pg_profile					      18

#define Anum_pg_profile_name  		              1
#define Anum_pg_profile_sessions		          2
#define Anum_pg_profile_session_cpu       		  3
#define Anum_pg_profile_query_cpu                 4
#define Anum_pg_profile_connect_time			  5
#define Anum_pg_profile_idle_time        		  6
#define Anum_pg_profile_session_blks              7
#define Anum_pg_profile_query_blks          	  8
#define Anum_pg_profile_private_sga      		  9
#define Anum_pg_profile_composite_limit			  10
#define Anum_pg_profile_failed_log_times		  11
#define Anum_pg_profile_passwd_lock_time          12
#define Anum_pg_profile_passwd_life_time          13
#define Anum_pg_profile_passwd_grace_time         14
#define Anum_pg_profile_passwd_reuse_time         15
#define Anum_pg_profile_passwd_reuse_max          16
#define Anum_pg_profile_container                 17
#define Anum_pg_profile_verify_function           18



/*
 * Do not support container now, Reserved
 */
typedef enum ContainerType
{
	ContainerType_None,
	ContainerType_Current,
	ContainerType_All,
	ContainerType_Reserve
} ContainerType;


typedef struct ProfileResourceLimit
{
	char      *name;
    float4	  sessions;//integer
    float4	  cpu_per_session;//integer
    float4	  cpu_per_query;//integer
    float4	  connection_time;//integer
    float4	  idle_time;//integer
    float4	  blks_per_session;//integer
    float4	  blks_per_query;//integer
    float4	  private_sga;//integer
    float4	  composite_limit;//integer
    float4	  failed_login_times;//integer
	float4	  passwd_lock_time;
	float4	  passwd_life_time;
	float4	  passwd_grace_time;
	float4	  passwd_reuse_time;
    float4	  passwd_reuse_max;//integer
	char 	  *passwd_verify_func;
    float4	  container;//integer
} ProfileResourceLimit;

typedef struct ProfileCostWeight
{
    int32 cpuweight;
    int32 connectweight;
    int32 blksweight;
    int32 sgaweight;
}  ProfileCostWeight;

typedef struct ProfileSessionInfo
{
    int64	  cpu_now;
    int64	  conn_time_now;
    int64	  idle_time_now;
    int64	  blks_now;
    int64	  private_sga_now;
} ProfileSessionInfo;

extern ProfileResourceLimit *MyProfile;
extern ProfileCostWeight *profcostweight;
extern bool profile_resource_limit;
extern bool profile_password_limit;
extern ProfileUserPasswd *MyProfUserPasswd;

typedef struct ProfileParameter
{
	NodeTag type;
	char *name;
	Value *value;
} ProfileParameter;

extern void PgProfileCreate(ProfileResourceLimit *profile);
extern void PgProfileRemove(char *pname, DropBehavior behavior);
extern void CreateProfile(CreateProfileStmt *stmt);
extern void AlterProfile(AlterProfileStmt *stmt);
extern void DropProfile(DropProfileStmt *stmt);
extern void AlterResourceCost(AlterResourceCostStmt *stmt);
extern Oid GetProfileIdByName(char *pname);
extern void profile_passwd_comlexity_check(char *pname, char *user, char *password);
extern void InsertProfileUserPasswd(char *user, char *passwd, bool update);
extern void GetMyProfile(char *user);
extern void GetMyProfCostWeight(void);
extern ProfileUserPasswd *GetMyProfUserPassword(char *user);
extern void ProfileAuthentication(int status, char *user);
extern void ProfileReusePassword(char *user, char *passwd);
extern void ProfileAccountUnlock(char *user);
extern void DropProfileUserPasswd(char *user);
extern void RenameProfileUserPasswd(const char *olduser, const char *newuser);
extern void ProfileSessionResourceLimitAuthDN(int64 query_cpu, int64 query_blks, int64 shared_blks);
extern void ProfileSessionResourceLimitAuthCN(int64 conn_time_now, int64 idle_time_now);
#endif                               /* PG_PROFILE_H */