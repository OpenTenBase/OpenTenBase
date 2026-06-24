/*-------------------------------------------------------------------------
 *
 * url.h
 *    routines for external table access to urls.
 *    to the qExec processes.
 *
 * src/include/access/url.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef URL_H
#define URL_H

#include "access/extprotocol.h"

#include "commands/copy.h"
#include "pgxc/shardmap.h"

/*
 * Different "flavors", or implementations, of external tables.
 */
enum fcurl_type_e
{
	CFTYPE_NONE = 0,
	CFTYPE_FILE = 1,
	CFTYPE_CURL = 2,
	CFTYPE_EXEC = 3,
	CFTYPE_CUSTOM = 4
};

/*
 * Common state of all different implementations external tables. The
 * implementation-specific url_*_fopen() function returns a palloc'd
 * struct that begins with this common part.
 */
typedef struct URL_FILE
{
	enum fcurl_type_e type;     /* type of handle */
	char *url;

	/* implementation-specific fields follow. */

} URL_FILE;

typedef struct extvar_t
{
	char	DATE[9];			/* YYYYMMDD */
	char	TIME[7];			/* HHMMSS */
	char	XID[21];			/* global transaction id */
	char	CID[10];			/* command id */
	char	SN[10];				/* scan number */
	char	NODE_ID[11];  		/* segments content id */
	char	NODE_COUNT[11];		/* total number of nodes in the system */
	char	CSVOPT[15]; 		/* "m.x...q...n.h."
 								 * CSV escape/quote/eol_type/header option */
	char	FORMATOPT[15]; 		/* not used now */

 	/* EOL vars */
 	char	*LINE_DELIM_STR;
	char	LINE_DELIM_LENGTH[11];

	/* SHARDING */
	char   *COLUMN_DELIM_STR;
	char	COLUMN_DELIM_LENGTH[11];
	char    NEED_ROUTE[2];
	char    NODE_IN_SHARD[SHARD_MAP_GROUP_NUM + 1];
	char    SHARD_NUM[5];
	char    FIELDS_NUM[5];
	char    DIS_COLUMN_NUM[5];
	char   *DIS_COLUMN;
	char   *DIS_COLUMN_TYPE;
	char   *COLUMN_TYPE;
	char   *COLUMN_NAME;
	char   *FORCE_NOTNULL_FLAGS; /* list of column names */
	char   *FORCE_NULL_FLAGS;
	char   *CONVERT_SELECT_FLAGS; /* per-column CSV/TEXT CS flags */
	char   *FORCE_QUOTE_FLAGS;	/* per-column CSV FQ flags */
	char   *NULL_PRINT;			/* NULL marker string (server encoding!) */
	char    NULL_PRINT_LEN[11];			/* length of same */
	char    FILE_ENCODING[11];			/* encoding of external file */
	char    EXTRA_ERROR_OPTS[9];  /* "m.i.c.e." fill_missing/ignore_extra_data/compatible_illegal_chars option */

	/* Support positions of columns */
	char   *COL_POS;		/* position(s) of the column(s) if not NULL */

	char    TIME_FORMAT_LENGTH[11];
	char   *TIME_FORMAT;
	char    TIME_STAMP_FORMAT_LENGTH[11];
	char   *TIME_STAMP_FORMAT;
	char    DATE_FORMAT_LENGTH[11];
	char   *DATE_FORMAT;

	char    TIME_STAMP_TZ_FORMAT_LENGTH[11];
	char   *TIME_STAMP_TZ_FORMAT;
	
	char    OPENTENBASE_ORA_COMPATIBLE[2];

	char   *TIMEZONE_STRING;

	char   *QUERY_STRING;
} extvar_t;


/* an EXECUTE string will always be prefixed like this */
#define EXEC_URL_PREFIX "execute:"

extern void external_set_env_vars_ext(extvar_t *extvar, CopyState cstate, uint32 scancounter,
									  List *params, RelationLocInfo *rd_locator_info);

/* exported functions */
extern URL_FILE *url_fopen(char *url, bool forwrite, extvar_t *ev, CopyState pstate, ExternalSelectDesc desc);
extern void url_fclose(URL_FILE *file, bool failOnError, const char *relname);
extern bool url_feof(URL_FILE *file, int bytesread);
extern bool url_ferror(URL_FILE *file, int bytesread, char *ebuf, int ebuflen);
extern size_t url_fread(void *ptr, size_t size, URL_FILE *file, CopyState pstate);
extern size_t url_fwrite(void *ptr, size_t size, URL_FILE *file, CopyState pstate);
extern void url_fflush(URL_FILE *file, CopyState pstate);

/* prototypes for functions in url_execute.c */
extern char *make_command(const char *cmd, extvar_t *ev);

/* implementation-specific functions. */
extern URL_FILE *url_curl_fopen(char *url, bool forwrite, extvar_t *ev, CopyState pstate);
extern void url_curl_fclose(URL_FILE *file, bool failOnError, const char *relname);
extern bool url_curl_feof(URL_FILE *file, int bytesread);
extern bool url_curl_ferror(URL_FILE *file, int bytesread, char *ebuf, int ebuflen);
extern size_t url_curl_fread(void *ptr, size_t size, URL_FILE *file, CopyState pstate);
extern size_t url_curl_fwrite(void *ptr, size_t size, URL_FILE *file, CopyState pstate);
extern void url_curl_fflush(URL_FILE *file, CopyState pstate);

extern URL_FILE *url_file_fopen(char *url, bool forwrite, extvar_t *ev, CopyState pstate);
extern void url_file_fclose(URL_FILE *file, bool failOnError, const char *relname);
extern bool url_file_feof(URL_FILE *file, int bytesread);
extern bool url_file_ferror(URL_FILE *file, int bytesread, char *ebuf, int ebuflen);
extern size_t url_file_fread(void *ptr, size_t size, URL_FILE *file, CopyState pstate);

extern URL_FILE *url_execute_fopen(char *url, bool forwrite, extvar_t *ev, CopyState pstate);
extern void url_execute_fclose(URL_FILE *file, bool failOnError, const char *relname);
extern bool url_execute_feof(URL_FILE *file, int bytesread);
extern bool url_execute_ferror(URL_FILE *file, int bytesread, char *ebuf, int ebuflen);
extern size_t url_execute_fread(void *ptr, size_t size, URL_FILE *file, CopyState pstate);
extern size_t url_execute_fwrite(void *ptr, size_t size, URL_FILE *file, CopyState pstate);

extern URL_FILE *url_custom_fopen(char *url, bool forwrite, extvar_t *ev, CopyState pstate, ExternalSelectDesc desc);
extern void url_custom_fclose(URL_FILE *file, bool failOnError, const char *relname);
extern bool url_custom_feof(URL_FILE *file, int bytesread);
extern bool url_custom_ferror(URL_FILE *file, int bytesread, char *ebuf, int ebuflen);
extern size_t url_custom_fread(void *ptr, size_t size, URL_FILE *file, CopyState pstate);
extern size_t url_custom_fwrite(void *ptr, size_t size, URL_FILE *file, CopyState pstate);

/* GUC */
extern int readable_external_table_timeout;
extern int tdx_retry_timeout;

#endif
