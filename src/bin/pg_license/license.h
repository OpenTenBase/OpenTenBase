#ifndef _LICENSE_
#define _LICENSE_

#include "license_timestamp.h"

#define LICENSE_MAGIC 0x5555
#define LICENSE_TEXT_LEN 64

typedef struct LicenseInfo
{
	uint16	magic_code;
	uint8 	ver_main;
	uint8 	ver_sub;
	int32	licenseid;
	/* 8 bytes */
	TimestampTz		creation_time;
	/* 8 bytes */
	TimestampTz 	expireddate;
	/* 8 bytes */
	int16   max_cn;
	int16   max_dn;
	int32   max_cpu_cores;
	/* 8 bytes */
	int16   max_conn_per_cn;
	int16   max_cores_per_cn;
	int16   max_cores_per_dn;
	int16	reserved_field2;
	/* 8 bytes */
	int32	max_volumn;		/*G*/
	int32	reserved_field3;
	/* 8 bytes */
	char	userid[LICENSE_TEXT_LEN];
	char	applicant_name[LICENSE_TEXT_LEN];
	char	group_name[LICENSE_TEXT_LEN];
	char	company_name[LICENSE_TEXT_LEN];
	char	product_name[LICENSE_TEXT_LEN];
	char	type[LICENSE_TEXT_LEN];
	/* 384 bytes */
	char	reserved_field4[592];
}LicenseInfo;

extern bytea * toBytea(const char *str);

extern int decrypt_license_file(LicenseInfo *lic, const char *license_file, const char *pubkey_file);
extern int decrypt_license_use_defaultkey(LicenseInfo *lic, const char *license_file);
extern int decrypt_license(LicenseInfo *lic, bytea *license, bytea *pubkey);
extern int pg_decrypt_license(LicenseInfo *lic, const char *license, const char *pubkey);

extern int create_license_file(const char *filepath,
							const char *private_keyfile,
							int licenseid,
							TimestampTz expired_date,
							int max_cn,
							int max_dn,
							int max_cpu_cores,
							int max_conn_per_cn,
							int max_cores_per_cn,
							int max_cores_per_dn,
							int max_volumn,
							char *userid,
							char *applicant_name,
							char *group_name,
							char *company_name,
							char *product_name,
							char *type);

extern int create_license_use_defaultkey(const char *filepath,
							int licenseid,
							TimestampTz expired_date,
							int max_cn,
							int max_dn,
							int max_cpu_cores,
							int max_conn_per_cn,
							int max_cores_per_cn,
							int max_cores_per_dn,
							int max_volumn,
							char *userid,
							char *applicant_name,
							char *group_name,
							char *company_name,
							char *product_name,
							char *type);

extern int create_license_info(LicenseInfo **licinfo,
							int licenseid,
							TimestampTz expired_date,
							int max_cn,
							int max_dn,
							int max_cpu_cores,
							int max_conn_per_cn,
							int max_cores_per_cn,
							int max_cores_per_dn,
							int max_volumn,
							char *userid,
							char *applicant_name,
							char *group_name,
							char *company_name,
							char *product_name,
							char *type);
extern int encrypt_license(LicenseInfo *lic, bytea *private_key, bytea **license_text);


#endif
