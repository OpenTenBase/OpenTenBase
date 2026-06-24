#include <limits.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>
#include "license_c.h"
#include "common.h"
#include "license.h"
#include "port.h"

#include "getaddrinfo.h"
#include "getopt_long.h"

#define MAX_INT_UNLIMITED 0
#define MAX_LIMITED_CN  256
#define MAX_LIMITED_DN  8192
#define MAX_LIMITED_CONN  8192
#define MAX_LIMITED_CPU_CORES 65536

static int DAYS_OF_MONTH[] = {0,31,29,31,30,31,30,31,31,30,31,20,31};

void usage(const char * progname);

char 	*progname = NULL;
int
main(int argc, char *argv[])
{
	
	static struct option long_options[] = {
		{"gen-license", no_argument, NULL, 'g'},
		{"read-license", no_argument, NULL, 'r'},
		{"private-key-path", required_argument, NULL, 'm'},
		{"public-key-path", required_argument, NULL, 'p'},
		{"license-path", required_argument, NULL, 'l'},
		{"license-id", required_argument, NULL, 'i'},
		{"license-type",required_argument,NULL,'t'},
		{"user-id", required_argument, NULL, 'u'},
		{"expired-year", required_argument, NULL, 'Y'},
		{"expired-month", required_argument, NULL, 'M'},
		{"expired-day", required_argument, NULL, 'T'},
		{"applicant-name", required_argument, NULL, 'A'},
		{"group-name", required_argument, NULL, 'G'},
		{"company-name", required_argument, NULL, 'C'},
		{"product-name", required_argument, NULL, 'P'},
		{"max-cn", required_argument, NULL, 1},
		{"max-dn", required_argument, NULL, 2},
		{"max-conn", required_argument, NULL, 3},
		{"max-cpu-cores", required_argument, NULL, 5},
		/*{"max-volumn", required_argument, NULL, 4},*/				
		{"data-dir", required_argument, NULL, 'D'},
		{"version", no_argument, NULL, 'v'},
		{"show", no_argument, NULL, 's'},

		{NULL, 0, NULL, 0}
	};

	int			option_index;
		
	bool	is_genlic = false;
	bool	is_readlic = false;
	char	*prikey_path = NULL;
	char	*pubkey_path = NULL;
	char	*datadir = NULL;
	char	*lic_path = NULL;
	int 	lic_id = 0;
	char	*userid = NULL;
	int		ex_year = MAX_INT_UNLIMITED;
	int		ex_month = MAX_INT_UNLIMITED;
	int 	ex_day = MAX_INT_UNLIMITED;
	char    *license_type = NULL;
	char	*applicant_name = NULL;
	char	*group_name = NULL;
	char	*com_name = NULL;
	char 	*product_name = NULL;
	int		max_cn = MAX_INT_UNLIMITED;
	int		max_dn = MAX_INT_UNLIMITED;
	int 	max_conn = MAX_INT_UNLIMITED;
	//int		max_volumn = MAX_INT_UNLIMITED;
	int   max_cpu_cores = MAX_INT_UNLIMITED;

	int		maxlen = LICENSE_TEXT_LEN - 1;

	int64 	ts = 0;
	const char	*progname = NULL;
	int		c = 0;
	int 	i = 0;
	char    lic_newpath[4096 + 64]; /* to avoid compiler warning */
	
	switch_elog(true);
	
	progname = get_progname(argv[0]);
	//set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_license"));

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage(progname);
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("initdb (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	/* process command-line options */

	while ((c = getopt_long(argc, argv, "grm:p:l:i:u:Y:M:T:A:G:C:P:D:t:vs", long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'A':
				applicant_name = strdup(optarg);
				if(strlen(applicant_name) > LICENSE_TEXT_LEN - 1)
				{
					printf("length of applicant name cannot be greater than %d\n", maxlen);
					exit(1);
				}
				break;
			case 'C':
				com_name = strdup(optarg);
				if(strlen(com_name) > LICENSE_TEXT_LEN - 1)
				{
					printf("length of company name cannot be greater than %d\n", maxlen);
					exit(1);
				}
				break;
			case 'D':
				datadir = strdup(optarg);
				break;
			case 'T':
				ex_day = atoi(optarg);
				if(ex_day <= 0 || ex_day > 31)
				{
					printf("expired day is invalid.\n");
					exit(1);
				}
				break;
			case 'g':
				is_genlic = true;
				break;
			case 'G':
				group_name= strdup(optarg);
				if(strlen(group_name) > LICENSE_TEXT_LEN - 1)
				{
					printf("length of group name cannot be greater than %d\n", maxlen);
					exit(1);
				}
				break;
			case 'i':
				lic_id = atoi(optarg);
				if(lic_id <= 0)
				{
					printf("license id is invalid\n");
					exit(1);
				}
				break;
			case 'l':
				lic_path = strdup(optarg);
				break;
			case 'm':
				prikey_path= strdup(optarg);
				break;
			case 'M':
				ex_month = atoi(optarg);
				if(ex_month <= 0 || ex_month > 12)
				{
					printf("expired month is invalid.\n");
					exit(1);
				}
				break;
			case 'p':
				pubkey_path = strdup(optarg);
				break;
			case 'P':
				product_name= strdup(optarg);
				if(strlen(product_name) > LICENSE_TEXT_LEN - 1)
				{
					printf("length of product name cannot be greater than %d\n", maxlen);
					exit(1);
				}
				break;
			case 't':
				license_type= strdup(optarg);
				for(i=0;license_type[i];i++)
					license_type[i]=tolower(license_type[i]);
				if(license_type[0])
					license_type[0]=toupper(license_type[0]);
				
				if(strcmp(license_type,"Standard")!=0 &&
				       strcmp(license_type,"Lite")!=0 &&
				       strcmp(license_type,"Professional")!=0 )
				{
					printf("License type must be Standard,Lite or Professional\n");
					exit(1);
				}
				break;
			case 'r':
				is_readlic = true;
				break;
			case 'u':
				userid= strdup(optarg);
				if(strlen(userid) > LICENSE_TEXT_LEN - 1)
				{
					printf("length of userid cannot be greater than %d\n", maxlen);
					exit(1);
				}
				break;
			case 'Y':
				ex_year = atoi(optarg);
				if(ex_year <= 2000 || ex_year > 10000)
				{
					printf("expired year is invalid.\n");
					exit(1);
				}
				break;
			case 1:
				max_cn = atoi(optarg);
				if(max_cn < 0 || max_cn > MAX_LIMITED_CN)
				{
					printf("max number of coordinators(0~256) is invalid.\n");
					exit(1);
				}
				break;
			case 2:
				max_dn = atoi(optarg);
				if(max_dn < 0 || max_dn > MAX_LIMITED_DN)
				{
					printf("max number of datanodes(0~8192) is invalid.\n");
					exit(1);
				}
				break;
			case 3:
				max_conn = atoi(optarg);
				if(max_conn < 0 || max_conn > MAX_LIMITED_CONN)
				{
					printf("max number of connection(0~8192) from one cooridnator is invalid.\n");
					exit(1);
				}
				break;
			/*
			case 4:
				max_volumn = atoi(optarg);
				if(max_volumn < 10 || max_volumn > MAX_LIMITED_VOLUMN)
				{
					printf("max data size is invalid.\n");
					exit(1);
				}
				break;
			*/
			case 5:
				max_cpu_cores = atoi(optarg);
				if(max_cpu_cores < 0 || max_cpu_cores > MAX_LIMITED_CPU_CORES)
				{
					printf("max number of cpu cores(0~65536) from total cpu cores is invalid.\n");
					exit(1);
				}
				break;
			default:
				/* getopt_long already emitted a complaint */
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
						progname);
				exit(1);
		}
	}

	if (optind < argc)
	{
		fprintf(stderr, _("%s: too many command-line arguments (first is \"%s\")\n"),
				progname, argv[optind]);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(1);
	}

	if(is_genlic && is_readlic)
	{
		fprintf(stderr, _("What do you want to do? to generate or read one license?"));
		exit(1);
	}

	/* default to read one license */
	if(!is_genlic && !is_readlic)
		is_readlic = true; 

	if(is_readlic)
	{
		int ret = 0;
		LicenseInfo licinfo;
		
		/*pubkey_file, license_file, datadir*/
		if(pubkey_path)
		{
			char *newpath = realpath(pubkey_path, NULL);

			if(!newpath)
			{
				fprintf(stderr, _("public key path %s is invalid.\n"), pubkey_path);
				exit(1);
			}
			else
			{
				free(pubkey_path);
				pubkey_path = newpath;
			}
		}

		if(lic_path)
		{
			char *newpath = realpath(lic_path, NULL);

			if(!newpath)
			{
				fprintf(stderr, _("license path %s is invalid.\n"), lic_path);
				exit(1);
			}
			else
			{
				free(lic_path);
				lic_path = newpath;
			}
		}
		
		if(datadir && (pubkey_path || lic_path))
		{
			fprintf(stderr, _("Don't to give the path of public key or license when data directory has been given."));
			exit(1);
		}
			
		if(datadir)
		{
			//sprintf(pubkey_newpath, "%s/pgxz_license/public.key", datadir);
			//pubkey_path = pubkey_newpath;
			sprintf(lic_newpath, "%s/pgxz_license/license.info", datadir);
			lic_path = lic_newpath;
		}
		else if(!lic_path)
		{
			char currentdir[4096];
			
			if(!getcwd(currentdir, 4095))
			{
				fprintf(stderr, _("Get current work directory failed.\n"));
				exit(1);
			}

			//sprintf(pubkey_newpath, "%s/pgxz_license/public.key", currentdir);
			//pubkey_path = pubkey_newpath;
			sprintf(lic_newpath, "%s/pgxz_license/license.info", currentdir);
			lic_path = lic_newpath;
		}
		
		if(pubkey_path)
		{
			ret = decrypt_license_file(&licinfo, lic_path, pubkey_path);
		}
		else
		{
			ret = decrypt_license_use_defaultkey(&licinfo, lic_path);
		}
		
		if(ret >= 0)
		{
			int ey,em,ed,cy,cm,cd;
			printf("License Aggrement:\n");
			printf("    version:  %03d.%03d\n", licinfo.ver_main, licinfo.ver_sub);
			printf("    licenseid: %d\n", licinfo.licenseid);
			printf("    license_type: %s\n", licinfo.type);
			timestamp2date(licinfo.expireddate, &ey, &em, &ed);
			printf("    expired date: %d/%d/%d\n", ey,em,ed);
			timestamp2date(licinfo.creation_time, &cy, &cm, &cd);
			printf("    creation date: %d/%d/%d\n", cy,cm,cd);
			printf("    max cn: %d\n", licinfo.max_cn);
			printf("    max dn: %d\n", licinfo.max_dn);
			printf("    max_cpu_cores: %d\n", licinfo.max_cpu_cores);
			printf("    max_conn_per_cn: %d\n", licinfo.max_conn_per_cn);			
			//printf("    max_cores_per_cn: %d\n",licinfo.max_cores_per_cn);
			//printf("    max_cores_per_dn: %d\n",licinfo.max_cores_per_dn);
			//printf("    max_volumn: %d\n",licinfo.max_volumn);
			printf("    userid: %s\n", licinfo.userid);
			printf("    applicant_name: %s\n", licinfo.applicant_name);
			printf("    group_name: %s\n", licinfo.group_name);
			printf("    company_name: %s\n", licinfo.company_name);
			printf("    product_name: %s\n", licinfo.product_name);
			ret = 0;
		}
		else
		{
			printf("decrypt license failed. errcode:%d\n", ret);
			return 2;
		}
	}
	
	if(is_genlic)
	{
		
		int ret = 0;

		if(prikey_path)
		{
			char *newpath = realpath(prikey_path, NULL);

			if(!newpath)
			{
				fprintf(stderr, _("private key path %s is invalid.\n"), prikey_path);
				exit(1);
			}
			else
			{
				free(prikey_path);
				prikey_path = newpath;
			}
		}
		else
		{
			if(max_dn > 4)
			{
				fprintf(stderr, _("You are limted to apply for max 4 Dn without private key.\n"));
				exit(1);
			}
			if(max_cn > 2)
			{
				fprintf(stderr, _("You are limited to apply for max 2 Cn without private key\n"));
				exit(1);
			}
			if(max_conn > 100)
			{
				fprintf(stderr, _("You are limited to apply for max 100 connections without private key.\n"));
				exit(1);
			}

			if(max_cpu_cores > 128)
			{
				fprintf(stderr, _("You are limited to apply for max 128 cpu core totally without private key.\n"));
				exit(1);
			}
		}

		if(!lic_path)
		{
			fprintf(stderr, _("You must give the path of license if you want to generate license.\n"));
			exit(1);
		}

		if(!lic_id)
		{
			fprintf(stderr, _("You must give the licenseid if you want to generate license.\n"));
			exit(1);
		}

		if(ex_year ==  MAX_INT_UNLIMITED)
		{
			ex_year = 3000;
			ex_month = 12;
			ex_day = 31;
		}
		else
		{
			if(ex_month == MAX_INT_UNLIMITED)
			{
				ex_month = 12;
			}

			if(ex_day == MAX_INT_UNLIMITED)
			{
				ex_day = DAYS_OF_MONTH[ex_month];
			}
		}

		if(!license_type)
		{
			fprintf(stderr, _("License type can't be empty.\n"));
			exit(1);
		}

		ts = get_timestamp(ex_year, ex_month, ex_day);

		if(prikey_path != NULL)
		{

			ret = create_license_file(lic_path,
									prikey_path,
									lic_id,
									ts,
									max_cn,
									max_dn,
									max_cpu_cores,
									max_conn,
									MAX_INT_UNLIMITED,
									MAX_INT_UNLIMITED,
									MAX_INT_UNLIMITED,
									userid,
									applicant_name,
									group_name,
									com_name,
									product_name,
									license_type);
		}
		else 
		{
			if(strcmp(license_type,"Lite") != 0)
			{
				fprintf(stderr, _("You are limted to Lite version without private key.\n"));
				exit(1);
			}
			
			ret = create_license_use_defaultkey(lic_path,
									lic_id,
									ts,
									max_cn,
									max_dn,
									max_cpu_cores,
									max_conn,
									MAX_INT_UNLIMITED,
									MAX_INT_UNLIMITED,
									MAX_INT_UNLIMITED,
									userid,
									applicant_name,
									group_name,
									com_name,
									product_name,
									license_type);
		}
		
		if(ret != 0)
		{
			fprintf(stderr, _("create license file failed, errno:%d\n"), ret);
			return 3;
		}
		else
		{
			fprintf(stdout, _("create license file succeed, license path:%s.\n"), lic_path);
		}
	}

	return 0;
}


void usage(const char * progname)
{
	static const char usage_str[] =
		"Usage: %s [OPTION] \n"
		"generate license:\n"
		"  usage 1:\n"
		"      pg_license -g -l LICENSE_FILE -m PRIVATE_KEY_FILE -i LICENSE_ID  -t LICENSE_TYPE -u USER_ID\n"
		"                 -Y EXPIRED_YEAR -M EXPIRED_MONTH -T EXPIRED_DAY -A zhangshan -G GROUP -C COMPANY\n"
		"                 -P PRODUCT_NAME --max-cn=MAX_COORDINATORS --max-dn=MAX_DATANODES \n"
		"                 --max-conn=MAX_CONNECTIONS_PER_COORDINATOR --max-cpu-cores=MAX_CPU_CORES\n"
		"read license:\n"
		"  usage 2:\n"
		"      pg_license\n"
		"  usage 3:\n"
		"      pg_license -D PG_DATA_DIR\n"
		"  usage 4:\n"
		"      pg_license -l LICENSE_FILE -p PUBLIC_KEY_FILE\n"
		"  usage 5:(use default public key)\n"
		"      pg_license -l LICENSE_FILE"
		"DESCRIPTIONS:"
		"  -g, --gen-license                            Generate one new license file\n"
		"  -r, --read-license                           Read a existing license file\n"
		"  -m, --private-key-path=<private key path>    Path of private key, which is used from create license\n"
		"  -p, --public-key-path=<public key path>      Path of publick key, which is userd for read license\n"
		"  -l, --license-path=<license path>            Path of license path\n"
		"  -i, --license-id=<licenseid>                 unique license id\n"
		"  -u, --user-id=<userid>                       id of authorized user\n"
		"  -t, --license-type=<type>                    License type must be Lite,Standard or Professional\n"
		"  -Y, --expired-year=<expired-year>            expired year(0~9999)\n"
		"  -M, --expired-month=<expired-month>          expired month(1~12)\n"
		"  -T, --expired-day=<expired-day>              expired day(1~31)\n"
		"  -A, --applicant-name=<applicant-name>        applicant name\n"
		"  -G, --group-name=<group-name>                Group name which to be authorized\n"
		"  -C, --company-name=<company-name>            Company name which to be authorized\n"
		"  -P, --product-name=<product-name>            Product name which to be authorized\n"
		"      --max-cn=<max-cn>                        Maximum number of coordinators\n"
		"      --max-dn=<max-dn>                        Maximum number of datanodes\n"
		"      --max-conn=<max-conn>                    Maximum number of connections to one cooridnator\n"
		"      --max-cpu-cores=<max-cpu-cores>     Maximum number of total cpu cores\n"
		"  -V, --version                                Show version\n"
		"  -h, --help                                   Show this help screen and exit\n";

	printf(usage_str, progname);
}
