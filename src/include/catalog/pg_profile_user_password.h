/*-------------------------------------------------------------------------
 *
 * pg_profile_user_password.h
 *	  definition of the system "profile user password" relation (pg_profile_user_password)
 *	  along with the relation's initial contents.
 *
 *
 * src/include/catalog/pg_profile_user_password.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PROFILE_USER_PASSWORD_H
#define PG_PROFILE_USER_PASSWORD_H

#include "catalog/genbki.h"

#define timestamptz int64

#define PgProfileUserPasswdRelationId  9042

CATALOG(pg_profile_user_password,9042)   BKI_WITHOUT_OIDS BKI_SHARED_RELATION
{
	NameData    username;          /* user name */
	bool        iscurrent;         /* is this current user and password? */
	int32       failedlogattempts; /* failed login attempts */
	timestamptz failedlogtime;     /* time of last failed login */
	timestamptz passwdstarttime;   /* time of password start to be used */
	timestamptz passwddiscardtime; /* time of password discarded */
	int32       passwdchangetimes; /* password change times since this password discarded */
#ifdef CATALOG_VARLEN			   /* variable-length fields start here */
	text		password;	       /* password, if any */
#endif
} FormData_pg_profile_user_password;

#undef timestamptz

typedef FormData_pg_profile_user_password *Form_pg_profile_user_password;

#define Natts_pg_profile_user_passwd	          8

#define Anum_pg_profile_user_passwd_name  		          1
#define Anum_pg_profile_user_passwd_curruser		      2
#define Anum_pg_profile_user_passwd_failed_times       	  3
#define Anum_pg_profile_user_passwd_failed_time           4
#define Anum_pg_profile_user_passwd_start_time			  5
#define Anum_pg_profile_user_passwd_discard_time          6
#define Anum_pg_profile_user_passwd_change_times          7
#define Anum_pg_profile_user_passwd_password          	  8

typedef struct ProfileUserPasswd
{
	char *username;
	bool iscurrent;
	int32 failedlogattempts;
	int64 failedlogtime;
	int64 passwdstarttime;
	int64 Passwddiscardtime;
	int32 passwdchangetimes;
	char *password;
}  ProfileUserPasswd;
#endif                    /* PG_PROFILE_USER_PASSWORD_H */
