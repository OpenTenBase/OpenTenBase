#include "postgres.h"

#include <stdlib.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>

#include "miscadmin.h"
#include "contrib/pgcrypto/pgp.h"
#include "storage/spin.h"
#include "storage/shmem.h"
#include "license/license.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "utils/guc.h"
#include "pgtime.h"
#include "pgxc/pgxc.h"

 
char *license_file 	= "pgxz_license/license.info";
char *pubkey_file 	= "pgxz_license/public.key";

char *PUBLIC_KEY = "-----BEGIN PGP PRIVATE KEY BLOCK-----\n"
"Version: GnuPG v2.0.22 (GNU/Linux)\n"
"\n"
"lQHYBFknnR8BBADB6hBi/AsXTMAkDKq43rvFNGer8TObaMnsoYFKbwxyYbrA5YxB\n"
"oa8AYBPKJ7PmXD0KQ7LUssk5965NgZJm3baElf0b5t7kA9Ui1nVMxPsN51DTd6EV\n"
"fQUe2XQbnivGBMItb6761Wjt4jidCa3WnqRdOpkCV2bDsbCFm12u79fGLQARAQAB\n"
"AAP6AjGV4Kv9q1Rovz+n1fjD4qKjVZrTiaHxPtl1ry2u1CoIEdL6KwS1CFBBIqiG\n"
"UxUFP8HgtDuwv3rubUQJyVrsWKVjcSfpjvPHs8r5QyF5MYqpe1hS3MseU0DQs8ph\n"
"3oM9pi8B4V2mk4sb0Ba5ZoLJMJ5Dz8P7hagTFoXHgEtP2+UCANTKUVFZVvtOfL79\n"
"z+sQpclkJe3ttggBnK7n4WiN0xjYcB+9oWdDzLofiwafLaJXk82unqDOP4M1KbL2\n"
"8HH134MCAOlKf2UkJ/PtfH3/6Uucte066zSMhYPSANcSRJuqNWMNXYIx/1UqcvYv\n"
"CCiGduw+Wic5qH9jQjtYjfO4AClwpI8CAKglYK5jrlQYek0EbF1pEYCVlJmGo9p2\n"
"rSKKZj4dpcTsSEnMz/H5p0hszJOdOr1MqQ9lvly4HiR9dfHNo+Fxo/ymKbQFUEct\n"
"WFqIuQQTAQIAIwUCWSedHwIbAwcLCQgHAwIBBhUIAgkKCwQWAgMBAh4BAheAAAoJ\n"
"EHxW2iMSd0PL99YEAJa1l9SVQ75jekJJHjhh6EiNPTgZC4amJNPOfzxzwsbEvaC+\n"
"GCV5qOcLhQthGu2uYe7ZgIQ42zEwQMMhBv6cDvkQm1j/5xG0JfY+rvo3oE2EJSPH\n"
"rB3EepqAnrmjtfpZgoVeO4OOfYWuEiyRZ9zyvF3Xcy9kJBUqP8NnRGq5fZh6nQHY\n"
"BFknnR8BBADA3U0lHbbxVqavcQ+uYd3wnQep6XHb/TYfnWlDm9/cLZN1D3RoucXb\n"
"KCZfZooeayaC9mFdGG00pIDdgBQOCqyqNlnnCxunfik6zPvJnmlbAqvQzd0HFsvT\n"
"QWLTGhq01UAfs5iS2ppLU6HcLxlB/CrGGzNunVjsPBk9wcYnGR8R3wARAQABAAP/\n"
"V8LcHe2dl8h0ZRUvq4yBL8JlAF5oH1Dj1hMNGWTOwyRCG1yC/jy62fU1MOg6JYlg\n"
"putydqhs2S2aLjDf71vQIYEdABFNCcMx84Wrv/q5cXVxDo8DL2cCli1YB/R2n3ci\n"
"dxHWAVHj3gjlSwBBWqaKvMSSel7NzB4hIzuC64Fol3ECAMnTfJhmX8nu/tgg4yxJ\n"
"h90J1HLB4S+SLFE3gsvT/vMhR0ihQgKEUQ3InZcHFi9i+Eau8JfVfqL4UZgCNRAl\n"
"L5MCAPSiAz7VHlw09RIiOKUIsnwflzuNb3f9iO2b+sj5id2AtErrnTXrOocypFaw\n"
"ri28h7yo5g1FqjzwogYa5TUUzAUB/jRa9bXPdD2fbef7j3LxUczQ0EaiOCDD7NDA\n"
"Mi9ZvBD9PRjHFs5ewxRR1I8+gndcH2IiWzxD88xprDHn65y1YVOdyoifBBgBAgAJ\n"
"BQJZJ50fAhsMAAoJEHxW2iMSd0PLat8D/16RYD91IreV5J/wtP5X5/Xteyb2kH2s\n"
"lbitue1nZhAiqAE8eKPTWGe2jFj2gPJCvXpZ4Nr0gSCwMe93pHNVLHRn3962e1s6\n"
"72pWS2TXsH7sRoEEj704Ens+ts+1g4jXoEFmStb2pUokAdjEiokSg7U0SnS/TksC\n"
"riCCIPGs7bkF\n"
"=vscQ\n"
"-----END PGP PRIVATE KEY BLOCK-----";

#ifdef _MLS_
char *PRIVATE_KEY = "-----BEGIN PGP PUBLIC KEY BLOCK-----\n"
"Version: GnuPG v2.0.22 (GNU/Linux)\n"
"\n"
"mI0EWSedHwEEAMHqEGL8CxdMwCQMqrjeu8U0Z6vxM5toyeyhgUpvDHJhusDljEGh\n"
"rwBgE8ons+ZcPQpDstSyyTn3rk2BkmbdtoSV/Rvm3uQD1SLWdUzE+w3nUNN3oRV9\n"
"BR7ZdBueK8YEwi1vrvrVaO3iOJ0JrdaepF06mQJXZsOxsIWbXa7v18YtABEBAAG0\n"
"BVBHLVhaiLkEEwECACMFAlknnR8CGwMHCwkIBwMCAQYVCAIJCgsEFgIDAQIeAQIX\n"
"gAAKCRB8VtojEndDy/fWBACWtZfUlUO+Y3pCSR44YehIjT04GQuGpiTTzn88c8LG\n"
"xL2gvhgleajnC4ULYRrtrmHu2YCEONsxMEDDIQb+nA75EJtY/+cRtCX2Pq76N6BN\n"
"hCUjx6wdxHqagJ65o7X6WYKFXjuDjn2FrhIskWfc8rxd13MvZCQVKj/DZ0RquX2Y\n"
"eriNBFknnR8BBADA3U0lHbbxVqavcQ+uYd3wnQep6XHb/TYfnWlDm9/cLZN1D3Ro\n"
"ucXbKCZfZooeayaC9mFdGG00pIDdgBQOCqyqNlnnCxunfik6zPvJnmlbAqvQzd0H\n"
"FsvTQWLTGhq01UAfs5iS2ppLU6HcLxlB/CrGGzNunVjsPBk9wcYnGR8R3wARAQAB\n"
"iJ8EGAECAAkFAlknnR8CGwwACgkQfFbaIxJ3Q8tq3wP/XpFgP3Uit5Xkn/C0/lfn\n"
"9e17JvaQfayVuK257WdmECKoATx4o9NYZ7aMWPaA8kK9elng2vSBILAx73ekc1Us\n"
"dGff3rZ7WzrvalZLZNewfuxGgQSPvTgSez62z7WDiNegQWZK1valSiQB2MSKiRKD\n"
"tTRKdL9OSwKuIIIg8aztuQU=\n"
"=PhbK\n"
"-----END PGP PUBLIC KEY BLOCK-----";
#endif

int DefaultMaxConnections = 20;
int DefaultMaxCN = 2;
int DefaultMaxDN = 4;
int DefaultMaxCpuCores = 128;


Datum pg_license_test(PG_FUNCTION_ARGS);
Datum pgxz_license(PG_FUNCTION_ARGS);
Datum pgxz_license_info(PG_FUNCTION_ARGS);
Datum pgxz_reload_license(PG_FUNCTION_ARGS);

static int write_file(const char *filepath, bytea *filecontent);
static int read_file(const char *filepath, bytea **filecontent);

typedef struct ShardLicenseAggrement
{
	bool is_loaded;
	slock_t  lock;
	LicenseInfo aggrement;
}ShardLicenseAggrement;


static ShardLicenseAggrement *license = NULL;

Size LicenseShmemSize(void)
{
	return sizeof(ShardLicenseAggrement);
}

void LicenseShmemInit(void)
{
	bool found;
	license = (ShardLicenseAggrement *)ShmemInitStruct("Licence Aggrement", 
														sizeof(ShardLicenseAggrement), 
														&found);
	if(found)
	{
		return;
	}

	MemSet((void *)license, 0x00, sizeof(ShardLicenseAggrement));
	license->is_loaded = false;
	SpinLockInit(&license->lock);	

	if(IS_PGXC_COORDINATOR)
	{
		load_license();
	}
}


bool load_license()
{
	int ret = 0;

	if(!license)
	{
		elog(WARNING, "license's shared memory has not initialized yet.");
		return false;
	}
	
	SpinLockAcquire(&license->lock);
	//ret = decrypt_license_file(&license->aggrement, license_file, pubkey_file);
	ret = decrypt_license_use_defaultkey(&license->aggrement, license_file);
	if(ret == 0)
		license->is_loaded = true;
	SpinLockRelease(&license->lock);
	
	if(ret < 0)
	{
		elog(WARNING, "load license failed. cluster can only be read.");
		return false;
	}	
	
	return true;
}

int decrypt_license_file(LicenseInfo *lic, const char *license_file, const char *pubkey_file)
{
	bytea *license = NULL;
	bytea *pubkey = NULL;

	int ret;
	if((ret = read_file(pubkey_file, &pubkey)) < 0)
	{
		return ret;
	}

	if((ret = read_file(license_file, &license)) < 0)
	{
		return ret;
	}

	ret = decrypt_license(lic, license, pubkey);

	if(license)
		pfree(license);
	if(pubkey)
		pfree(pubkey);

	return ret;
}

int decrypt_license_use_defaultkey(LicenseInfo *lic, const char *license_file)
{
	bytea *license = NULL;
	bytea *pubkey = NULL;
	int ret = 0;
	
	if((ret = read_file(license_file, &license)) < 0)
	{
		return ret;
	}
	
	pubkey = palloc(VARHDRSZ + strlen(PUBLIC_KEY));
	SET_VARSIZE(pubkey, VARHDRSZ + strlen(PUBLIC_KEY));
	memcpy(VARDATA(pubkey), PUBLIC_KEY, strlen(PUBLIC_KEY));

	ret = decrypt_license(lic, license, pubkey);

	if(license)
		pfree(license);
	if(pubkey)
		pfree(pubkey);

	return ret;
}


int decrypt_license(LicenseInfo *lic, bytea *license, bytea *pubkey)
{
	int ret = 0;
	StringInfoData license_dearmor;
	text	*license_text = NULL;
	StringInfoData pubkey_dearmor;
	text	*pubkey_text = NULL;
	text	*license_decrypt = NULL;

	initStringInfo(&license_dearmor);
	initStringInfo(&pubkey_dearmor);
	
	/*check arg*/
	if(!lic || !license || !pubkey)
	{
		return -1;
	}
	
	/*dearmor license*/
	
	ret = pgp_armor_decode((uint8 *)VARDATA(license), VARSIZE(license) - VARHDRSZ, &license_dearmor);
	if(ret < 0)
	{
		printf("license is invalid.");
		ret = -2;
		goto decrypt_error;
	}
	license_text = palloc(VARHDRSZ + license_dearmor.len);
	SET_VARSIZE(license_text, VARHDRSZ + license_dearmor.len);
	memcpy(VARDATA(license_text), license_dearmor.data, license_dearmor.len);

	/*dearmor pubkey*/
	ret = pgp_armor_decode((uint8 *)VARDATA(pubkey), VARSIZE(pubkey) - VARHDRSZ, &pubkey_dearmor);
	if(ret < 0)
	{
		printf("public key is invalid.");
		ret = -3;
		goto decrypt_error;
	}
	pubkey_text = palloc(VARHDRSZ + pubkey_dearmor.len);
	SET_VARSIZE(pubkey_text, VARHDRSZ + pubkey_dearmor.len);
	memcpy(VARDATA(pubkey_text), pubkey_dearmor.data, pubkey_dearmor.len);

	/*decrypt_license*/
	license_decrypt = decrypt_internal(1, 0, license_text, pubkey_text, NULL, NULL);

	/*contruct Location Info*/
	if(VARSIZE(license_decrypt) - VARHDRSZ != sizeof(LicenseInfo))
	{
		printf("license is invalid.");
		ret = -4;
		goto decrypt_error;
	}

	memcpy(lic, VARDATA(license_decrypt), sizeof(LicenseInfo));
	ret = 0;

	/* handle error and release resource */
decrypt_error:
	if(license_dearmor.data)
		pfree(license_dearmor.data);
	if(license_text)
		pfree(license_text);
	if(pubkey_dearmor.data)
		pfree(pubkey_dearmor.data);
	if(pubkey_text)
		pfree(pubkey_text);
#ifdef _MLS_    
	if(license_decrypt)
		crypt_free(license_decrypt);
#endif
	return ret;
}

int create_license_file(const char *filepath,
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
							char *type)
{
	int ret = 0;
	bytea *private_key_src = NULL;
	bytea *license_encryped = NULL;
	LicenseInfo *licinfo = NULL;
	/*read priveate key*/
	ret = read_file(private_keyfile, &private_key_src);

	if(ret < 0)
		return ret-1000;
	
	/*create license info*/	
	ret = create_license_info(&licinfo, licenseid,
						expired_date,
						max_cn, max_dn, max_cpu_cores, max_conn_per_cn, 
						max_cores_per_cn, max_cores_per_dn,
						max_volumn,
						userid, applicant_name,
						group_name, company_name,
						product_name,type);
	if(ret < 0)
	{
		ret = ret-2000;
		goto create_failed;
	}
	
	/*encrypt_license*/
	ret = encrypt_license(licinfo, private_key_src, &license_encryped);
	if(ret < 0)
	{
		ret = ret-3000;
		goto create_failed;
	}

	/*write license to file*/
	ret = write_file(filepath, license_encryped);
	if(ret < 0)
	{
		ret = ret-4000;
		goto create_failed;
	}

create_failed:
	if(private_key_src)
		pfree(private_key_src);
	if(license_encryped)
		pfree(license_encryped);
	if(licinfo)
		pfree(licinfo);
	
	return ret;
}

int create_license_info(LicenseInfo **licinfo,
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
							char *type)
{
	LicenseInfo *license;

	struct pg_tm	expired_date_tm;
	TimestampTz ts_trunc;
	fsec_t fsec;
	
	if(!licinfo)
		return -1;

	if(0 != timestamp2tm(expired_date, NULL, &expired_date_tm, &fsec, NULL, NULL))
	{
		return -2;
	}

	expired_date_tm.tm_hour = 0;
	expired_date_tm.tm_min = 0;
	expired_date_tm.tm_sec = 0;

	if(0 != tm2timestamp(&expired_date_tm, 0, NULL, &ts_trunc))
	{
		return -3;
	}

	if(!userid)
	{
		return -4;
	}

	if(!applicant_name)
	{
		return -5;
	}

	if(!group_name)
	{
		return -6;
	}

	if(!company_name)
	{
		return -7;
	}

	if(!product_name)
	{
		return -8;
	}

	if(!userid || strlen(userid) > LICENSE_TEXT_LEN - 1)
	{
		return -9;
	}

	if(!applicant_name ||strlen(applicant_name) > LICENSE_TEXT_LEN - 1)
	{
		return -10;
	}

	if(!group_name || strlen(group_name) > LICENSE_TEXT_LEN - 1)
	{
		return -11;
	}

	if(!company_name || strlen(company_name) > LICENSE_TEXT_LEN - 1)
	{
		return -12;
	}

	if(!product_name || strlen(product_name) > LICENSE_TEXT_LEN - 1)
	{
		return -13;
	}

	if(!type)
	{
		return -14;
	}

	if(!type || strlen(type) > LICENSE_TEXT_LEN - 1)
	{
		return -15;
	}

	*licinfo = (LicenseInfo *)palloc(sizeof(LicenseInfo));
	memset((void *)*licinfo, 0, sizeof(LicenseInfo));

	license = *licinfo;

	license->magic_code = LICENSE_MAGIC;
	license->ver_main = 1;
	license->ver_sub = 1;
	license->licenseid = licenseid;
	license->creation_time = GetCurrentTimestamp();
	license->expireddate = ts_trunc;
	license->max_cn = max_cn & 0xFFFF;
	license->max_dn = max_dn & 0xFFFF;
	license->max_cpu_cores = max_cpu_cores & 0xFFFFFFFF;
	license->max_conn_per_cn = max_conn_per_cn & 0xFFFF;
	license->max_cores_per_cn = max_cores_per_cn & 0xFFFF;
	license->max_cores_per_dn = max_cores_per_dn & 0xFFFF;
	license->max_volumn = max_volumn;
	memcpy(license->userid, userid, strlen(userid)+1);
	memcpy(license->applicant_name, applicant_name, strlen(applicant_name)+1);
	memcpy(license->group_name, group_name, strlen(group_name)+1);
	memcpy(license->company_name, company_name, strlen(company_name)+1);
	memcpy(license->product_name, product_name, strlen(product_name)+1);
	memcpy(license->type, type, strlen(type)+1);
	
	return 0;
}

int encrypt_license(LicenseInfo *lic, bytea *private_key, bytea **license_text)
{
	int ret = 0;
	StringInfoData private_dearmor;
	text	*private_text = NULL;
	text	*lic_text = NULL;
	bytea	*lic_encryped = NULL;
	StringInfoData	lic_armored;


	initStringInfo(&private_dearmor);
	initStringInfo(&lic_armored);

	if(!lic || !private_key || !license_text)
	{
		return -1;
	}
	
	/*dearmor private_key*/
	
	ret = pgp_armor_decode((uint8 *)VARDATA(private_key), VARSIZE(private_key) - VARHDRSZ, &private_dearmor);
	if(ret < 0)
	{
		printf("private key is invalid.");
		ret = -2;
		goto encrypt_error;
	}
	private_text = palloc(VARHDRSZ + private_dearmor.len);
	SET_VARSIZE(private_text, VARHDRSZ + private_dearmor.len);
	memcpy(VARDATA(private_text), private_dearmor.data, private_dearmor.len);

	/*construct bytea for license*/
	lic_text = palloc(VARHDRSZ + sizeof(LicenseInfo));
	SET_VARSIZE(lic_text, VARHDRSZ + sizeof(LicenseInfo));
	memcpy(VARDATA(lic_text), (void *)lic, sizeof(LicenseInfo));

	/*encrypt license*/
	lic_encryped = encrypt_internal(1, 0, lic_text, private_text, NULL);

	/*enarmor license*/		
	pgp_armor_encode((uint8 *)VARDATA(lic_encryped), 
						(unsigned)(VARSIZE(lic_encryped) - VARHDRSZ), 
						&lic_armored,
						0, NULL, NULL);

	*license_text = (bytea *)palloc(lic_armored.len + VARHDRSZ);
	SET_VARSIZE(*license_text, lic_armored.len + VARHDRSZ);
	memcpy(VARDATA(*license_text), lic_armored.data, lic_armored.len);
	ret = 0;
	
encrypt_error:
	if(private_dearmor.data)
		pfree(private_dearmor.data);
	if(private_text)
		pfree(private_text);
	if(lic_text)
		pfree(lic_text);
	if(lic_encryped)
		pfree(lic_encryped);
	if(lic_armored.data)
		pfree(lic_armored.data);

	return ret;
}

static int write_file(const char *filepath, bytea *filecontent)
{
	int ret = 0;
	int fd = -1;
	int bytes_writed = -1;
	
	if(!filepath || !filecontent)
		return -1;
	
	fd = open(filepath, O_WRONLY | O_CREAT, S_IRUSR | S_IRGRP | S_IROTH);

	if(fd < 0)
	{
		if (errno == EMFILE || errno == ENFILE)
		{	
			puts("out of file descriptors");
		}
		else
		{
			printf("open/create file %s failed.\n", filepath);
		}
		ret = -2;
		goto write_error;
	}

	bytes_writed = write(fd, VARDATA(filecontent), VARSIZE(filecontent) - VARHDRSZ);

	if(bytes_writed != VARSIZE(filecontent) - VARHDRSZ)
	{
		ret = -3;
		goto write_error;
	}

write_error:
	if(fd > 0)
		close(fd);
	return ret;
}

/*
  *return value:
  * -1: arg is invalid.
  * -2: open file failed.
  * -3: get filesize failed.
  * -4: memory is not enough
  * -5: file content has been modified
  * -6: read file failed.
  */
static int read_file(const char *filepath, bytea **filecontent)
{
	int ret = 0;
	int fd = -1;
	int bytes_read = 0;
	int bytes_reading = 0;
	int retry = 0;
	long filesize = -1;
	struct stat statbuff;  

	if(!filepath || !filecontent)
		return -1;
	
	*filecontent = NULL;
	
	fd = open(filepath, O_RDONLY);

	if(fd < 0)
	{
		if (errno == EMFILE || errno == ENFILE)
		{
			puts("out of file descriptors");
		}
		else
		{
			printf("read file %s failed, please ensure license file has been imported.\n", filepath);
		}
		ret = -2;
		goto read_error;
	}

	while(retry<3 && fstat(fd, &statbuff) < 0)
		retry++;

	if(retry >= 3)
	{
		ret = -3;
		goto read_error;
	}

	filesize = (long)statbuff.st_size;

	*filecontent = (bytea *)palloc(filesize + VARHDRSZ);

	if(!*filecontent)
	{
		puts("alloc memory field.");
		ret = -4;
		goto read_error;
	}
	
	while(1)
	{
		if(filesize - bytes_read < 0)
		{
			printf("file %s content has been modified.\n", filepath);
			ret = -5;
			goto read_error;
		}
		bytes_reading = read(fd, VARDATA(*filecontent) + bytes_read, filesize - bytes_read);
		if(bytes_reading == 0)
			break;
		if(bytes_read < 0)
		{
			printf("read file %s failed.\n", filepath);
			ret = -6;
			goto read_error;
		}

		bytes_read += bytes_reading;

		if(bytes_read >= filesize)
			break;
	}

	SET_VARSIZE(*filecontent, bytes_read + VARHDRSZ);

read_error:
	if(ret != 0 && *filecontent)
	{
		pfree(*filecontent);
		*filecontent = NULL;
	}
	
	if(fd > 0)
		close(fd);
	
	return ret;
}

Datum
pg_license_test(PG_FUNCTION_ARGS)
{
	int ret = 0;
	ret = create_license_file(license_file,
								"license/private.key",
								10101,
								GetCurrentTimestamp(),
								6,
								8,
								128,
								100,
								-1,
								-1,
								2048,
								"user01",
								"xuzhongqing",
								"Tencent Group",
								"Tencent",
								"WeiXin",
								"Standard");

	if(ret >= 0)
	{
		LicenseInfo licinfo;
		ret = decrypt_license_file(&licinfo, license_file, pubkey_file);
		if(ret >= 0)
		{
			elog(WARNING, "licenseid: %d", licinfo.licenseid);
			elog(WARNING, "expired date: %s", timestamptz_to_str(licinfo.expireddate));
			elog(WARNING, "create time: %s", timestamptz_to_str(licinfo.creation_time));
			elog(WARNING, "max cn: %d", licinfo.max_cn);
			elog(WARNING, "max dn: %d", licinfo.max_dn);
			elog(WARNING, "max cpu_cores: %d", licinfo.max_cpu_cores);
			elog(WARNING, "max_conn_per_cn: %d", licinfo.max_conn_per_cn);
			elog(WARNING, "max_cores_per_cn: %d",licinfo.max_cores_per_cn);
			elog(WARNING, "max_cores_per_dn: %d",licinfo.max_cores_per_dn);
			elog(WARNING, "max_volumn: %d",licinfo.max_volumn);
			elog(WARNING, "userid: %s", licinfo.userid);
			elog(WARNING, "applicant_name: %s", licinfo.applicant_name);
			elog(WARNING, "group_name: %s", licinfo.group_name);
			elog(WARNING, "company_name: %s", licinfo.company_name);
			elog(WARNING, "product_name: %s", licinfo.product_name);
			elog(WARNING, "type: %s", licinfo.type);

			load_license();
			ret = 0;
		}
		else
		{
			ret -= 2000000;
		}
	}
	else
	{
		ret -= 1000000;
	}
						
	PG_RETURN_INT32(ret);
}

Datum
pgxz_reload_license(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(load_license());
}


Datum
pgxz_license(PG_FUNCTION_ARGS)
{
	if(license && license->is_loaded)
	{
		StringInfoData	lic_buf;
		initStringInfo(&lic_buf);
		appendStringInfoString(&lic_buf, "License Agreement:\n");
		appendStringInfo(&lic_buf, "\tVersion                     :  %03d.%03d\n", license->aggrement.ver_main, license->aggrement.ver_sub);
		appendStringInfo(&lic_buf, "\tType                        :  %s\n", license->aggrement.type);
		appendStringInfo(&lic_buf, "\tLicense ID                  :  %010x\n", license->aggrement.licenseid);
		appendStringInfo(&lic_buf, "\tCreate Time                 :  %s\n", timestamptz_to_str(license->aggrement.creation_time));
		appendStringInfo(&lic_buf, "\tExpired Date                :  %s\n", timestamptz_to_str(license->aggrement.expireddate));
		appendStringInfo(&lic_buf, "\tUser ID                     :  %s\n", license->aggrement.userid);
		appendStringInfo(&lic_buf, "\tApplicant Name              :  %s\n", license->aggrement.applicant_name);
		appendStringInfo(&lic_buf, "\tGroup Name                  :  %s\n", license->aggrement.group_name);
		appendStringInfo(&lic_buf, "\tCompany Name                :  %s\n", license->aggrement.company_name);
		appendStringInfo(&lic_buf, "\tProduct Name                :  %s\n", license->aggrement.product_name);
		appendStringInfo(&lic_buf, "\tMax Coorinators             :  %d\n", license->aggrement.max_cn);
		appendStringInfo(&lic_buf, "\tMax DataNodes               :  %d\n", license->aggrement.max_dn);
		appendStringInfo(&lic_buf, "\tMax Total Cpu cores        :  %d\n", license->aggrement.max_cpu_cores);
		appendStringInfo(&lic_buf, "\tConnections Per Coordinator :  %d\n", license->aggrement.max_conn_per_cn);
		//appendStringInfo(&lic_buf, "\tCapcity(G)                  :  %d\n", license->aggrement.max_volumn);

		PG_RETURN_TEXT_P(cstring_to_text(lic_buf.data));
	}
	else
	{
		PG_RETURN_TEXT_P(cstring_to_text("No License."));
	}
}

Datum
pgxz_license_info(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}

bool IsLicenseValid()
{
	if(license && license->is_loaded)
		return true;
	else
		return false;
}

bool IsLicenseExpired()
{
	TimestampTz now;
	if(!license || !license->is_loaded)
		return false;

	now = GetCurrentTimestamp();
	if(now > license->aggrement.expireddate)
		return true;
	else
		return false;	
}


int GetMaxCN()
{
	if(!license->is_loaded)
	{
		return DefaultMaxCN;
	}

	return license->aggrement.max_cn;
}

int GetMaxDN()
{
	if(!license->is_loaded)
	{
		return DefaultMaxDN;
	}

	return license->aggrement.max_dn;
}

int GetMaxVolumn()
{
	if(!license->is_loaded)
	{
		return -1;
	}

	return license->aggrement.max_volumn;
}

int GetLimitsFromLicenseFile(int *max_conn, int *max_dn, int *max_cn, TimestampTz *expired_date)
{
	int ret = 0;
	LicenseInfo tmp_lic;
	MemSet((void *)&tmp_lic, 0x00, sizeof(LicenseInfo));
	//ret = decrypt_license_file(&tmp_lic, license_file, pubkey_file);
	ret = decrypt_license_use_defaultkey(&tmp_lic, license_file);

	if(ret < 0)
	{
		elog(WARNING, "read license failed. errcode:%d", ret);
		return ret;
	}
	else
	{
		if(max_conn)
			*max_conn = tmp_lic.max_conn_per_cn;
		if(max_dn)
			*max_dn = tmp_lic.max_dn;
		if(max_cn)
			*max_cn = tmp_lic.max_cn;
		if(expired_date)
			*expired_date = tmp_lic.expireddate;
		return 0;
	}
}


void
LicenseRestrictMaxConn()
{
	if(license->is_loaded)
	{
		if(license->aggrement.max_conn_per_cn <= 0)
			return;
		else if(license->aggrement.max_conn_per_cn < MaxConnections)
		{
			elog(WARNING, "max_connection(%d) cannot greater than max connection(%d) from license aggrement.",
					MaxConnections, license->aggrement.max_conn_per_cn);
			MaxConnections = license->aggrement.max_conn_per_cn;
		}
	}
	else
	{
		//have no license, read only. connections is unlimited.
	}
	
}

char * transparent_crypt_get_pub_key(void)
{
    return PRIVATE_KEY;
}
char * transparent_crypt_get_private_key(void)
{
    return PUBLIC_KEY;
}

