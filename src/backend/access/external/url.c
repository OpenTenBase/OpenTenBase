/*-------------------------------------------------------------------------
 *
 * url.c
 *	  Core support for opening external relations via a URL
 *
 * src/backend/access/external/url.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "access/url.h"
#include "commands/copy.h"
#include "commands/dbcommands.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "pgxc/nodemgr.h"
#include "pgxc/shardmap.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/uri.h"

/* GUC */
int readable_external_table_timeout = 0;
int tdx_retry_timeout = 300;

static char *get_eol_delimiter(List *params);
static int URLEncode(const char *str, const int strSize, char *result, const int resultSize);

void
external_set_env_vars_ext(extvar_t *extvar, CopyState cstate,
		uint32 scancounter, List *params, RelationLocInfo *rd_locator_info)
{
	char		*encoded_delim;
	char        *encoded_null_print;
	char        *format_string;
	int			i = 0;
	int 		nodeid;
	int			line_delim_len;
	int			column_delim_len;
	int         null_print_len;
	int         format_len = 0;
	Snapshot	snapshot = GetActiveSnapshot();
	int         natts = cstate->rel->rd_att->natts;
	StringInfo  buf = makeStringInfo();

	snprintf(extvar->CSVOPT, sizeof(extvar->CSVOPT),
			"m%1dx%3dq%3dn%1dh%1d",
			cstate->csv_mode ? 1 : 0,
			cstate->escape ? 255 & *cstate->escape : 0,
			cstate->quote ? 255 & *cstate->quote : 0,
			cstate->eol_type,
			cstate->header_line ? 1 : 0);

	sprintf(extvar->XID, "%lu-%s", snapshot->start_ts, PGXCQueryId);
	sprintf(extvar->CID, "%x", 0);
	sprintf(extvar->SN, "%x", scancounter);
	sprintf(extvar->NODE_ID, "%d", PGXCNodeId - 1);
	sprintf(extvar->NODE_COUNT, "%d", NumDataNodes);
	
	if (rd_locator_info)
	{
		int dis_attr_num = rd_locator_info->nDisAttrs;
		sprintf(extvar->NEED_ROUTE, "%d", 1);

		/* following parameters used when need TDX to evaluating distribution. */
		/* column delimiter */
		column_delim_len = (int) strlen(cstate->delim);
		if (column_delim_len > 0)
		{
			encoded_delim = (char *) palloc0(column_delim_len * 3 + 1); /* %xx%xx */
			URLEncode(cstate->delim, column_delim_len, encoded_delim, column_delim_len * 3 + 1);
		}
		else
		{
			column_delim_len = 0;
			encoded_delim = "";
		}
		extvar->COLUMN_DELIM_STR = pstrdup(encoded_delim);
		sprintf(extvar->COLUMN_DELIM_LENGTH, "%d", column_delim_len);
		
		/* shard number, default 4096 */
		sprintf(extvar->SHARD_NUM, "%d", SHARD_MAP_GROUP_NUM);
		
		/* shardmap */
		for (i = 0; i < SHARD_MAP_GROUP_NUM; i++)
		{
			nodeid = GetNodeid(i);
			if (nodeid == PGXCNodeId)
			{
				extvar->NODE_IN_SHARD[i] = 'y';
			}
			else
			{
				extvar->NODE_IN_SHARD[i] = 'n';
			}
		}
		
		/* total column number. */
		sprintf(extvar->FIELDS_NUM, "%d", natts);

		if (cstate->fixed_position)
		{
			List *attr_info_list = cstate->attinfolist;
			ListCell *cell = NULL;
			StringInfo str = makeStringInfo();

			/*
			 * (id text position(1:3), c1 text position(*:40))
			 *
			 * for positions like this, record it as "1,1:3|0,0:40"
			 */
			Assert(attr_info_list && (attr_info_list->length == natts));
			foreach (cell, attr_info_list)
			{
				AttInfo *attr_info = (AttInfo *) lfirst(cell);
				Position *pos;

				/* all columns should contain position info */
				Assert(attr_info->attrpos);
				pos = attr_info->attrpos;
				if (cell != list_head(attr_info_list))
					appendStringInfoChar(str, '|');
				appendStringInfo(str,
								 "%d,%d:%d",
								 (int) pos->flag,
								 pos->start_offset,
								 pos->end_offset);
			}

			extvar->COL_POS = (char *) palloc0(str->len + 1);
			sprintf(extvar->COL_POS, "%s", str->data);
		}

		/* distribution column number */
		sprintf(extvar->DIS_COLUMN_NUM, "%d", dis_attr_num);
		
		/*
		 * which are distribution columns and their order
		 *
		 * If the distribution columns are [c1, c39, c1411] and their attribute
		 * number index are [0, 38, 1410] (1600 columns in total allowed), they
		 * will be recorded as "0,38,1410". Given the distribution columns are
		 * up to 10 in usual, then the max string length is
		 *		(4 * 10) for attribute numbers +
		 *		9		 for commas +
		 *		1		 for '\0'.
		 */
		extvar->DIS_COLUMN = (char *) palloc0(50);
		for (i = 0; i < dis_attr_num; i++)
		{
			if (i == 0)
				sprintf(extvar->DIS_COLUMN, "%d", rd_locator_info->disAttrNums[i] - 1);
			else
				sprintf(extvar->DIS_COLUMN + strlen(extvar->DIS_COLUMN), ",%d", rd_locator_info->disAttrNums[i] - 1);
		}

		/*
		 * distribution type
		 *
		 * If the distribution columns are [c1, c3, c2] and their type oids are
		 * [1042, 23, 1114] (notice that the distribution columns might be in a
		 * different order from they are in the table), they will be recorded
		 * as "1042,23,1114". Since the digit length of type oids are up to 4
		 * bytes, then the max string length is
		 * 		(dis_attr_num * 4) for type oids +
		 * 		(dis_attr_num - 1) for commas +
		 * 		1				   for '\0'.
		 */
		extvar->DIS_COLUMN_TYPE = (char *) palloc0(dis_attr_num * 5);
		for (i = 0; i < dis_attr_num; i++)
		{
			if (i == 0)
				sprintf(extvar->DIS_COLUMN_TYPE, "%d", rd_locator_info->disAttrTypes[i]);
			else
				sprintf(extvar->DIS_COLUMN_TYPE + strlen(extvar->DIS_COLUMN_TYPE), ",%d", rd_locator_info->disAttrTypes[i]);
		}
		
		extvar->COLUMN_TYPE = (char *) palloc0(natts * (sizeof(Oid) + 1));
		for (i = 0; i < natts; i++)
		{
			if (i == 0)
				sprintf(extvar->COLUMN_TYPE, "%d", TupleDescAttr(cstate->rel->rd_att, i)->atttypid);
			else
				sprintf(extvar->COLUMN_TYPE + strlen(extvar->COLUMN_TYPE), "-%d", TupleDescAttr(cstate->rel->rd_att, i)->atttypid);
		}
		
		for (i = 0; i < natts; i++)
		{
			if (buf->len > 0)
				appendStringInfoCharMacro(buf, '-');
			appendStringInfoString(buf, quote_identifier(NameStr(TupleDescAttr(cstate->rel->rd_att, i)->attname)));
		}
		extvar->COLUMN_NAME = pstrdup(buf->data);
		pfree(buf->data);
		pfree(buf);
		
		/* copy options */
		extvar->FORCE_NOTNULL_FLAGS = (char *) palloc0(natts + 1);
		for (i = 0; i < natts; ++i)
		{
			if (cstate->force_notnull_flags[i])
				extvar->FORCE_NOTNULL_FLAGS[i] = '1';
			else
				extvar->FORCE_NOTNULL_FLAGS[i] = '0';
		}
		extvar->FORCE_NULL_FLAGS = (char *) palloc0(natts + 1);
		for (i = 0; i < natts; ++i)
		{
			if (cstate->force_null_flags[i])
				extvar->FORCE_NOTNULL_FLAGS[i] = '1';
			else
				extvar->FORCE_NOTNULL_FLAGS[i] = '0';
		}
		
		extvar->CONVERT_SELECT_FLAGS = NULL;
		if (cstate->convert_selectively)
		{
			extvar->CONVERT_SELECT_FLAGS = (char *) palloc0(natts + 1);
			for (i = 0; i < natts; i++)
			{
				if (cstate->convert_select_flags[i])
					extvar->CONVERT_SELECT_FLAGS[i] = '1';
				else
					extvar->CONVERT_SELECT_FLAGS[i] = '0';
			}
		}
		
		extvar->FORCE_QUOTE_FLAGS = (char *)palloc0(natts + 1);
		if (cstate->force_quote_all)
			for (i = 0; i < natts; i++)
				extvar->FORCE_QUOTE_FLAGS[i] = '1';
		else
		{
			for (i = 0; i < natts; i++)
			{
				if (cstate->force_quote_flags[i])
					extvar->FORCE_QUOTE_FLAGS[i] = '1';
				else
					extvar->FORCE_QUOTE_FLAGS[i] = '0';
			}
		}
		
		null_print_len = (int) strlen(cstate->null_print);
		if (null_print_len > 0)
		{
			encoded_null_print = (char *) palloc0(null_print_len * 3 + 1); /* %xx%xx%xx%xx%xx%xx */
			URLEncode(cstate->null_print, null_print_len, encoded_null_print, null_print_len * 3 + 1);
		}
		else
		{
			null_print_len = 0;
			encoded_null_print = "";
		}
		
		extvar->NULL_PRINT = pstrdup(encoded_null_print);
		sprintf(extvar->NULL_PRINT_LEN, "%d", null_print_len);
		
		sprintf(extvar->FILE_ENCODING, "%d", cstate->file_encoding);
		snprintf(extvar->EXTRA_ERROR_OPTS, sizeof(extvar->EXTRA_ERROR_OPTS),
		         "m%1di%1dc%1de%1d",
		         cstate->fill_missing ? 1 : 0,
		         cstate->ignore_extra_data ? 1 : 0,
		         cstate->compatible_illegal_chars ? 1: 0,
				 cstate->escape_off ? 1 : 0);
		
		/* user-defined date format string */
		if (cstate->time_format)
		{
			format_len = (int) strlen(cstate->time_format);
			format_string = cstate->time_format;
		}
		else
		{
			format_len = 0;
			format_string = "";
		}
		extvar->TIME_FORMAT = pstrdup(format_string);
		sprintf(extvar->TIME_FORMAT_LENGTH, "%d", format_len);
		
		if (cstate->date_format)
		{
			format_len = (int) strlen(cstate->date_format);
			format_string = cstate->date_format;
		}
		else
		{
			if (ORA_MODE)
			{
				format_len = (int) strlen(nls_date_format);
				format_string = nls_date_format;
			}
			else
			{
				format_len = 0;
				format_string = "";
			}
		}
		sprintf(extvar->DATE_FORMAT_LENGTH, "%d", format_len);
		extvar->DATE_FORMAT = pstrdup(format_string);
		
		if (cstate->timestamp_format)
		{
			format_len = (int) strlen(cstate->timestamp_format);
			format_string = cstate->timestamp_format;
		}
		else
		{
			if (ORA_MODE)
			{
				format_len = (int) strlen(nls_timestamp_format);
				format_string = nls_timestamp_format;
			}
			else
			{
				format_len = 0;
				format_string = "";
			}

		}
		sprintf(extvar->TIME_STAMP_FORMAT_LENGTH, "%d", format_len);
		extvar->TIME_STAMP_FORMAT = pstrdup(format_string);

		if (cstate->timestamp_tz_format)
		{
			format_len = (int) strlen(cstate->timestamp_tz_format);
			format_string = cstate->timestamp_tz_format;
		}
		else
		{
			if (ORA_MODE)
			{
				format_len = (int) strlen(nls_timestamp_tz_format);
				format_string = nls_timestamp_tz_format;
			}
			else
			{
				format_len = 0;
				format_string = "";
			}
		}
		sprintf(extvar->TIME_STAMP_TZ_FORMAT_LENGTH, "%d", format_len);
		extvar->TIME_STAMP_TZ_FORMAT = pstrdup(format_string);
        //TODO
		sprintf(extvar->OPENTENBASE_ORA_COMPATIBLE, "%d", sql_mode);
		
		if (session_timezone)
			extvar->TIMEZONE_STRING = pstrdup(pg_get_timezone_name(session_timezone));
	}

	extvar->QUERY_STRING = (char *)debug_query_string;

	if (NULL != params)
	{
		char	   *line_delim_str = get_eol_delimiter(params);

		line_delim_len = (int) strlen(line_delim_str);
		if (line_delim_len > 0)
		{
			encoded_delim = (char *) palloc0(line_delim_len * 3 + 1); /* %xx%xx */
			URLEncode(line_delim_str, line_delim_len, encoded_delim, line_delim_len * 3 + 1);
		}
		else
		{
			line_delim_len = 0;
			encoded_delim = "";
		}
	}
	else
	{
		switch(cstate->eol_type)
		{
			case EOL_CR:
				encoded_delim = "0D";
				line_delim_len = 1;
				break;
			case EOL_NL:
				encoded_delim = "0A";
				line_delim_len = 1;
				break;
			case EOL_CRNL:
				encoded_delim = "0D0A";
				line_delim_len = 2;
				break;
			case EOL_UD:
				line_delim_len = (int) strlen(cstate->eol);
				if (line_delim_len > 0)
				{
					encoded_delim = (char *) palloc0(line_delim_len * 3 + 1); /* %xx%xx */
					URLEncode(cstate->eol, line_delim_len, encoded_delim, line_delim_len * 3 + 1);
				}
				else
				{
					line_delim_len = 0;
					encoded_delim = "";
				}
				break;
			default:
				encoded_delim = "";
				line_delim_len = 0;
				break;
		}
	}
	extvar->LINE_DELIM_STR = pstrdup(encoded_delim);
	sprintf(extvar->LINE_DELIM_LENGTH, "%d", line_delim_len);
}

/**
 * @brief URLEncode : encode the base64 string "str"
 *
 * @param str:  the base64 encoded string
 * @param strsz:  the str length (exclude the last \0)
 * @param result:  the result buffer
 * @param resultsz: the result buffer size(exclude the last \0)
 *
 * @return: >=0 represent the encoded result length
 *              <0 encode failure
 *
 * Note:
 * 1) to ensure the result buffer has enough space to contain the encoded string, we'd better
 *     to set resultsz to 3*strsz
 *
 * 2) we don't check whether str has really been base64 encoded
 */
static int URLEncode(const char *str, const int strSize, char *result, const int resultSize)
{
	int i;
	int j = 0;//for result index
	char ch;
	
	if ((str == NULL) || (result == NULL) || (strSize <= 0) || (resultSize <= 0))
	{
		return 0;
	}
	
	for (i = 0; (i < strSize) && (j < resultSize); ++i)
	{
		ch = str[i];
		if (((ch >= 'A') && (ch < 'Z')) ||
		    ((ch >= 'a') && (ch < 'z')) ||
		    ((ch >= '0') && (ch < '9')))
		{
			result[j++] = ch;
		}
		else if (ch == ' ')
		{
			result[j++] = '+';
		}
		else if (ch == '.' || ch == '-' || ch == '_' || ch == '*')
		{
			result[j++] = ch;
		}
		else
		{
			if (j + 3 < resultSize)
			{
				sprintf(result + j, "%%%02X", (unsigned char) ch);
				j += 3;
			}
			else
			{
				return 0;
			}
		}
	}
	
	result[j] = '\0';
	return j;
}

static char *
get_eol_delimiter(List *params)
{
	ListCell   *lc = params->head;

	while (lc)
	{
		if (pg_strcasecmp(((DefElem *) lc->data.ptr_value)->defname, "line_delim") == 0)
			return pstrdup(((Value *) ((DefElem *) lc->data.ptr_value)->arg)->val.str);
		lc = lc->next;
	}
	return pstrdup("");
}

/*
 * url_fopen
 *
 * checks for URLs or types in the 'url' and basically use the real fopen() for
 * standard files, or if the url happens to be a command to execute it uses
 * popen to execute it.
 *
 * On error, ereport()s
 */
URL_FILE *
url_fopen(char *url, bool forwrite, extvar_t *ev, CopyState pstate, ExternalSelectDesc desc)
{
	/*
	 * if 'url' starts with "execute:" then it's a command to execute and
	 * not a url (the command specified in CREATE EXTERNAL TABLE .. EXECUTE)
	 */
	if (pg_strncasecmp(url, EXEC_URL_PREFIX, strlen(EXEC_URL_PREFIX)) == 0)
		return url_execute_fopen(url, forwrite, ev, pstate);	/* NULL */
	else if (IS_FILE_URI(url))
		return url_file_fopen(url, forwrite, ev, pstate);		/* NULL */
	else if (IS_HTTP_URI(url) || IS_TDX_URI(url) || IS_TDXS_URI(url) || IS_COS_URI(url) || IS_KAFKA_URI(url))
		return url_curl_fopen(url, forwrite, ev, pstate);
	else
		elog(ERROR, "unsupported external table protocol: %s", url);
}

/*
 * url_fclose: Disposes of resources associated with this external web table.
 *
 * If failOnError is true, errors encountered while closing the resource results
 * in raising an ERROR.  This is particularly true for "execute:" resources where
 * command termination is not reflected until close is called.  If failOnClose is
 * false, close errors are just logged.  failOnClose should be false when closure
 * is due to LIMIT clause satisfaction.
 *
 * relname is passed in for being available in data messages only.
 */
void
url_fclose(URL_FILE *file, bool failOnError, const char *relname)
{
	if (file == NULL)
	{
		elog(WARNING, "internal error: call url_fclose with bad parameter");
		return;
	}

	switch (file->type)
	{
		case CFTYPE_FILE:
			url_file_fclose(file, failOnError, relname);
			break;

		case CFTYPE_EXEC:
			url_execute_fclose(file, failOnError, relname);
			break;

		case CFTYPE_CURL:
			url_curl_fclose(file, failOnError, relname);
			break;

		case CFTYPE_CUSTOM:
			url_custom_fclose(file, failOnError, relname);
			break;

		default: /* unknown or unsupported type - oh dear */
			elog(ERROR, "unrecognized external table type: %d", file->type);
			break;
    }
}

bool
url_feof(URL_FILE *file, int bytesread)
{
    switch (file->type)
    {
		case CFTYPE_FILE:
			return url_file_feof(file, bytesread);

		case CFTYPE_EXEC:
			return url_execute_feof(file, bytesread);

		case CFTYPE_CURL:
			return url_curl_feof(file, bytesread);

		case CFTYPE_CUSTOM:
			return url_custom_feof(file, bytesread);

		default: /* unknown or supported type - oh dear */
			elog(ERROR, "unrecognized external table type: %d", file->type);
    }
}


bool
url_ferror(URL_FILE *file, int bytesread, char *ebuf, int ebuflen)
{
	switch (file->type)
	{
		case CFTYPE_FILE:
			return url_file_ferror(file, bytesread, ebuf, ebuflen);

		case CFTYPE_EXEC:
			return url_execute_ferror(file, bytesread, ebuf, ebuflen);

		case CFTYPE_CURL:
			return url_curl_ferror(file, bytesread, ebuf, ebuflen);

		case CFTYPE_CUSTOM:
			return url_custom_ferror(file, bytesread, ebuf, ebuflen);

		default: /* unknown or supported type - oh dear */
			elog(ERROR, "unrecognized external table type: %d", file->type);
	}
}

size_t
url_fread(void *ptr,
          size_t size,
          URL_FILE *file,
          CopyState pstate)
{
    switch (file->type)
    {
		case CFTYPE_FILE:
			return url_file_fread(ptr, size, file, pstate);

		case CFTYPE_EXEC:
			return url_execute_fread(ptr, size, file, pstate);

		case CFTYPE_CURL:
			return url_curl_fread(ptr, size, file, pstate);

		case CFTYPE_CUSTOM:
			return url_custom_fread(ptr, size, file, pstate);

		default: /* unknown or supported type */
			elog(ERROR, "unrecognized external table type: %d", file->type);
    }
}

size_t
url_fwrite(void *ptr, size_t size, URL_FILE *file, CopyState pstate)
{
    switch (file->type)
    {
		case CFTYPE_FILE:
			elog(ERROR, "CFTYPE_FILE not yet supported in url.c");
			return 0;		/* keep compiler quiet */

		case CFTYPE_EXEC:
			return url_execute_fwrite(ptr, size, file, pstate);

		case CFTYPE_CURL:
			return url_curl_fwrite(ptr, size, file, pstate);

		case CFTYPE_CUSTOM:
			return url_custom_fwrite(ptr, size, file, pstate);

		default: /* unknown or unsupported type */
			elog(ERROR, "unrecognized external table type: %d", file->type);
    }
}

/*
 * flush all remaining buffered data waiting to be written out to external source
 */
void
url_fflush(URL_FILE *file, CopyState pstate)
{
    switch (file->type)
    {
		case CFTYPE_FILE:
			elog(ERROR, "CFTYPE_FILE not yet supported in url.c");
			break;

		case CFTYPE_EXEC:
		case CFTYPE_CUSTOM:
			/* data isn't buffered on app level. no op */
			break;

		case CFTYPE_CURL:
			url_curl_fflush(file, pstate);
			break;

		default: /* unknown or unsupported type */
			elog(ERROR, "unrecognized external table type: %d", file->type);
    }
}
