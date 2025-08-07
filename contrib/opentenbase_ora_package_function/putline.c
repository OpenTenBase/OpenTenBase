#include "postgres.h"
#include "funcapi.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"

#undef USE_SSL
#undef ENABLE_GSS
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "utils/memutils.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "opentenbase_ora.h"
#include "builtins.h"

#if defined(WIN32) && !defined(_MSC_VER)
extern PGDLLIMPORT ProtocolVersion FrontendProtocol;	/* for mingw */
#endif

/*
 * TODO: BUFSIZE_UNLIMITED to be truely unlimited (or INT_MAX),
 * and allocate buffers on-demand.
 */
#define BUFSIZE_DEFAULT		20000
#define BUFSIZE_MIN			2000
#define BUFSIZE_MAX			1000000
#define BUFSIZE_UNLIMITED	BUFSIZE_MAX
#define LEN_MAX_INT_STR       (22)

static bool is_server_output = false;
static char *buffer = NULL;
static int   buffer_size = 0;	/* allocated bytes in buffer */
static int   buffer_len = 0;	/* used bytes in buffer */
static int   buffer_get = 0;	/* retrieved bytes in buffer */

static void add_str(const char *str, int len);
static void add_text(text *str);
static void add_newline(void);
static void send_buffer(void);

/*
 * Aux. buffer functionality
 */
static void
add_str(const char *str, int len)
{
	/* Discard all buffers if get_line was called. */
	if (buffer_get > 0)
	{
		buffer_get = 0;
		buffer_len = 0;
	}

	if (buffer_len + len > buffer_size)
		ereport(ERROR,
			(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
			 errmsg("buffer overflow"),
			 errdetail("Buffer overflow, limit of %d bytes", buffer_size),
			 errhint("Increase buffer size in dbms_output.enable() next time")));

	memcpy(buffer + buffer_len, str, len);
	buffer_len += len;
	buffer[buffer_len] = '\0';
}

static void
add_text(text *str)
{
	add_str(VARDATA_ANY(str), VARSIZE_ANY_EXHDR(str));
}

static void 
add_int(int64 num)	
{
	char str[LEN_MAX_INT_STR]  = {0};
	pg_lltoa(num, str);
	add_str(str, strnlen(str, LEN_MAX_INT_STR));
}

static void
add_newline(void)
{
	add_str("", 1);	/* add \0 */
	if (is_server_output)
		send_buffer();
}


static void
send_buffer()
{
	if (buffer_len > 0)
	{
		StringInfoData msgbuf;
		char *cursor = buffer;

		while (--buffer_len > 0)
		{
			if (*cursor == '\0')
				*cursor = '\n';
			cursor++;
		}

		if (*cursor != '\0')
			ereport(ERROR,
				    (errcode(ERRCODE_INTERNAL_ERROR),
				     errmsg("internal error"),
				     errdetail("Wrong message format detected")));

		pq_beginmessage(&msgbuf, 'N');

		/*
		 * FrontendProtocol is not avalilable in MSVC because it is not
		 * PGDLLEXPORT'ed. So, we assume always the protocol >= 3.
		 */

#ifndef _MSC_VER

		if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
		{

#endif

			pq_sendbyte(&msgbuf, PG_DIAG_MESSAGE_PRIMARY);
			pq_sendstring(&msgbuf, buffer);
			pq_sendbyte(&msgbuf, '\0');

#ifndef _MSC_VER

		}
		else
		{
			*cursor++ = '\n';
			*cursor = '\0';
			pq_sendstring(&msgbuf, buffer);
		}

#endif

		pq_endmessage(&msgbuf);
		pq_flush();
	}
}


/*
 * Aux db functions
 *
 */

static void
dbms_output_enable_internal(int32 n_buf_size)
{
	/* We allocate +2 bytes for an end-of-line and a string terminator. */
	if (buffer == NULL)
	{
		buffer = MemoryContextAlloc(TopMemoryContext, n_buf_size + 2);
		buffer_size = n_buf_size;
		buffer_len = 0;
		buffer_get = 0;
	}
	else if (n_buf_size > buffer_len)
	{
		/* We cannot shrink buffer less than current length. */
		buffer = repalloc(buffer, n_buf_size + 2);
		buffer_size = n_buf_size;
	}
}

PG_FUNCTION_INFO_V1(dbms_output_enable_default);

Datum
dbms_output_enable_default(PG_FUNCTION_ARGS)
{
	dbms_output_enable_internal(BUFSIZE_DEFAULT);
    PG_RETURN_VOID();
}


PG_FUNCTION_INFO_V1(dbms_output_enable);

Datum
dbms_output_enable(PG_FUNCTION_ARGS)
{
	int32 n_buf_size;

	if (PG_ARGISNULL(0))
		n_buf_size = BUFSIZE_UNLIMITED;
	else
	{
		n_buf_size = PG_GETARG_INT32(0);

		if (n_buf_size > BUFSIZE_MAX)
		{
			n_buf_size = BUFSIZE_MAX;
			elog(WARNING, "Limit decreased to %d bytes.", BUFSIZE_MAX);
		}
		else if (n_buf_size < BUFSIZE_MIN)
		{
			n_buf_size = BUFSIZE_MIN;
			elog(WARNING, "Limit increased to %d bytes.", BUFSIZE_MIN);
		}
	}

	dbms_output_enable_internal(n_buf_size);
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_disable);

Datum
dbms_output_disable(PG_FUNCTION_ARGS)
{
	if (buffer)
		pfree(buffer);

	buffer = NULL;
	buffer_size = 0;
	buffer_len = 0;
	buffer_get = 0;
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_serveroutput);

Datum
dbms_output_serveroutput(PG_FUNCTION_ARGS)
{
	is_server_output = PG_GETARG_BOOL(0);
	if (is_server_output && !buffer)
		dbms_output_enable_internal(BUFSIZE_DEFAULT);
	PG_RETURN_VOID();
}

/*
 * main functions
 */

PG_FUNCTION_INFO_V1(dbms_output_put);

Datum
dbms_output_put(PG_FUNCTION_ARGS)
{
	if (buffer && !PG_ARGISNULL(0))
		add_text(PG_GETARG_TEXT_PP(0));
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_rowid);

Datum
dbms_output_put_rowid(PG_FUNCTION_ARGS)
{
	if (buffer)
	{
		if (!PG_ARGISNULL(0))
		{
			Datum arg = PG_GETARG_DATUM(0);
			Datum result = DirectFunctionCall1(rowid_to_text, arg);
			add_text(DatumGetTextPP(result));
		}
	}
	
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_raw);

Datum
dbms_output_put_raw(PG_FUNCTION_ARGS)
{
	if (buffer)
	{
		if (!PG_ARGISNULL(0))
		{
			Datum arg = PG_GETARG_DATUM(0);
			Datum result = DirectFunctionCall1(rawtotext, arg);
			add_text(DatumGetTextPP(result));
		}
	}
	
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_long);

Datum
dbms_output_put_long(PG_FUNCTION_ARGS)
{
	if (buffer)
	{
		if (!PG_ARGISNULL(0))
			add_text(PG_GETARG_TEXT_PP(0));
	}
	
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_int);

Datum
dbms_output_put_int(PG_FUNCTION_ARGS)
{
	if (buffer)
	{
		 if (!PG_ARGISNULL(0))
			add_int(PG_GETARG_INT32(0));
	}
	
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_pls_int);

Datum
dbms_output_put_pls_int(PG_FUNCTION_ARGS)
{
	if (buffer)
	{
		if (!PG_ARGISNULL(0))
			add_int(PG_GETARG_INT32(0));
	}
	
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_simple_int);

Datum
dbms_output_put_simple_int(PG_FUNCTION_ARGS)
{
	if (buffer)
	{
		if (!PG_ARGISNULL(0))
			add_int(PG_GETARG_INT32(0));
	}
	
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_binary_int);

Datum
dbms_output_put_binary_int(PG_FUNCTION_ARGS)
{
	if (buffer)
	{
		if (!PG_ARGISNULL(0))
			add_int(PG_GETARG_INT32(0));
	}
	
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_numeric);

Datum
dbms_output_put_numeric(PG_FUNCTION_ARGS)
{
	if (buffer && !PG_ARGISNULL(0))
	{
		char * str = DatumGetCString(DirectFunctionCall1(numeric_out, PG_GETARG_DATUM(0)));
		add_str(str, strlen(str));
	}
	
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_float8);

Datum
dbms_output_put_float8(PG_FUNCTION_ARGS)
{
	if (buffer && !PG_ARGISNULL(0))
	{
		Datum val = DirectFunctionCall1(float8out, PG_GETARG_DATUM(0));
		text *valtxt = cstring_to_text(DatumGetCString(val));
		add_text(valtxt);
	}
	
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_bytea);

Datum
dbms_output_put_bytea(PG_FUNCTION_ARGS)
{
	if (buffer && !PG_ARGISNULL(0))
	{
		bytea	*line_bytea = PG_GETARG_BYTEA_P(0);
		Datum	line_datum = PointerGetDatum(line_bytea);
		char	*str = DatumGetCString(DirectFunctionCall1(byteaout, line_datum));
		add_str(str, strlen(str));
	}
	
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_blob);

Datum
dbms_output_put_blob(PG_FUNCTION_ARGS)
{
	if (buffer && !PG_ARGISNULL(0))
	{
		bytea	*line_bytea = PG_GETARG_BLOB_P(0);
		Datum	line_datum = PointerGetDatum(line_bytea);
		char	*str = DatumGetCString(DirectFunctionCall1(byteaout, line_datum));
		add_str(str, strlen(str));
	}
	
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_clob);

Datum
dbms_output_put_clob(PG_FUNCTION_ARGS)
{
	if (buffer)
	{
		if (!PG_ARGISNULL(0))
			add_text(PG_GETARG_CLOB_P(0));
	}
	
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_line);

Datum
dbms_output_put_line(PG_FUNCTION_ARGS)
{
	if (buffer)
	{
		if (!PG_ARGISNULL(0))
			add_text(PG_GETARG_TEXT_PP(0));
		add_newline();
	}
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_line_rowid);

Datum
dbms_output_put_line_rowid(PG_FUNCTION_ARGS)
{
	if (buffer)
	{
		if (!PG_ARGISNULL(0))
		{
			Datum arg = PG_GETARG_DATUM(0);
			Datum result = DirectFunctionCall1(rowid_to_text, arg);
			add_text(DatumGetTextPP(result));
		}
		add_newline();
	}
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_line_raw);

Datum
dbms_output_put_line_raw(PG_FUNCTION_ARGS)
{
	if (buffer)
	{
		if (!PG_ARGISNULL(0))
		{
			Datum arg = PG_GETARG_DATUM(0);
			Datum result = DirectFunctionCall1(rawtotext, arg);
			add_text(DatumGetTextPP(result));
		}
		add_newline();
	}
	PG_RETURN_VOID();
}



PG_FUNCTION_INFO_V1(dbms_output_put_line_long);

Datum
dbms_output_put_line_long(PG_FUNCTION_ARGS)
{
	if (buffer)
	{
		if (!PG_ARGISNULL(0))
			add_text(PG_GETARG_TEXT_PP(0));
		add_newline();
	}
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_line_int);

Datum
dbms_output_put_line_int(PG_FUNCTION_ARGS)
{
	if (buffer)
	{
		if (!PG_ARGISNULL(0))
			add_int(PG_GETARG_INT32(0));
		add_newline();
	}
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_line_pls_int);

Datum
dbms_output_put_line_pls_int(PG_FUNCTION_ARGS)
{
	if (buffer)
	{
		if (!PG_ARGISNULL(0))
			add_int(PG_GETARG_INT32(0));
		add_newline();
	}
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_line_simple_int);

Datum
dbms_output_put_line_simple_int(PG_FUNCTION_ARGS)
{
	if (buffer)
	{
		if (!PG_ARGISNULL(0))
			add_int(PG_GETARG_INT32(0));
		add_newline();
	}
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_line_binary_int);

Datum
dbms_output_put_line_binary_int(PG_FUNCTION_ARGS)
{
	if (buffer)
	{
		if (!PG_ARGISNULL(0))
			add_int(PG_GETARG_INT32(0));
		add_newline();
	}
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_line_numeric);

Datum
dbms_output_put_line_numeric(PG_FUNCTION_ARGS)
{
	if (buffer)
	{
		if (!PG_ARGISNULL(0))
		{
			char * str = DatumGetCString(DirectFunctionCall1(numeric_out, PG_GETARG_DATUM(0)));

			add_str(str, strlen(str));
		}
		add_newline();
	}
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_line_float8);

Datum
dbms_output_put_line_float8(PG_FUNCTION_ARGS)
{
	if (buffer)
	{
		if (!PG_ARGISNULL(0))
		{
			Datum val = DirectFunctionCall1(float8out, PG_GETARG_DATUM(0));
			text *valtxt = cstring_to_text(DatumGetCString(val));

			add_text(valtxt);
		}
		add_newline();
	}
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_line_bytea);

Datum
dbms_output_put_line_bytea(PG_FUNCTION_ARGS)
{
	if (buffer)
	{
		if (!PG_ARGISNULL(0))
		{
			bytea	*line_bytea = PG_GETARG_BYTEA_P(0);
			Datum	line_datum = PointerGetDatum(line_bytea);
			char	*str = DatumGetCString(DirectFunctionCall1(byteaout, line_datum));

			add_str(str, strlen(str));
		}
		add_newline();
	}
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_line_blob);

Datum
dbms_output_put_line_blob(PG_FUNCTION_ARGS)
{
	if (buffer)
	{
		if (!PG_ARGISNULL(0))
		{
			bytea   *line_bytea = PG_GETARG_BLOB_P(0);
			Datum   line_datum = PointerGetDatum(line_bytea);
			char    *str = DatumGetCString(DirectFunctionCall1(byteaout, line_datum));

			add_str(str, strlen(str));
		}
		add_newline();
	}
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_put_line_clob);

Datum
dbms_output_put_line_clob(PG_FUNCTION_ARGS)
{
	if (buffer)
	{
		if (!PG_ARGISNULL(0))
			add_text(PG_GETARG_CLOB_P(0));
		add_newline();
	}
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_byte2text);

static char*
bytea2char(char * bytea_str)
{
	int		i = 0;
	int		k = 0;
	char	*result;
	int		len = strlen(bytea_str);
	int		result_len = len / 2 + 1;

	result = (char *)palloc(sizeof(char) * result_len);
	if (len < 2 || (len % 2 != 0) ||
		(!(bytea_str[0] == '\\' && bytea_str[1] =='x')))
	{
		elog(ERROR, "%s in wrong format", bytea_str);
	}

	for (i = 2; i < len; i++)
	{
		int num;
		if (bytea_str[i] >= '0' && bytea_str[i] <= '9')
			num = bytea_str[i] - '0';
		else if (bytea_str[i] >= 'a' && bytea_str[i] <= 'f')
			num = bytea_str[i] - 'a' + 10;
		else if (bytea_str[i] >= 'A' && bytea_str[i] <= 'F')
			num = bytea_str[i] - 'A' + 10;
		else
			elog(ERROR, "%s in wrong format", bytea_str);

		if (i % 2 == 0)
		{
			result[k] = num;
		}
		else
		{
			result[k] = result[k] * 16 + num;
			k++;
		}
	}

	result[k] = '\0';
	return result;
}

Datum
dbms_output_byte2text(PG_FUNCTION_ARGS)
{
	bytea	*line_bytea = PG_GETARG_BYTEA_P(0);
	Datum	line_datum = PointerGetDatum(line_bytea);
	char	*bytea_str = DatumGetCString(DirectFunctionCall1(byteaout, line_datum));
	char	*char_str = bytea2char(bytea_str);
	text	*result = cstring_to_text(char_str);
	PG_RETURN_TEXT_P(result);
}

PG_FUNCTION_INFO_V1(dbms_output_new_line);

Datum
dbms_output_new_line(PG_FUNCTION_ARGS)
{
	if (buffer)
		add_newline();
	PG_RETURN_VOID();
}

static text *
dbms_output_next(void)
{
	if (buffer_get < buffer_len)
	{
		text *line = cstring_to_text(buffer + buffer_get);
		buffer_get += VARSIZE_ANY_EXHDR(line) + 1;
		return line;
	}
	else
		return NULL;
}

PG_FUNCTION_INFO_V1(dbms_output_get_line);

Datum
dbms_output_get_line(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		result;
	HeapTuple	tuple;
	Datum		values[2];
	bool		nulls[2] = { false, false };
	text	   *line;

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	if ((line = dbms_output_next()) != NULL)
	{
		values[0] = PointerGetDatum(line);
		values[1] = Int32GetDatum(0);	/* 0: succeeded */
	}
	else
	{
		nulls[0] = true;
		values[1] = Int32GetDatum(1);	/* 1: failed */
	}

	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}


PG_FUNCTION_INFO_V1(dbms_output_get_lines);

Datum
dbms_output_get_lines(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		result;
	HeapTuple	tuple;
	Datum		values[2];
	bool		nulls[2] = { false, false };
	text	   *line;

	int32		max_lines = PG_GETARG_INT32(0);
	int32		n;
	ArrayBuildState *astate = NULL;

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	for (n = 0; n < max_lines && (line = dbms_output_next()) != NULL; n++)
	{
		astate = accumArrayResult(astate, PointerGetDatum(line), false,
					TEXTOID, CurrentMemoryContext);
	}

	/* 0: lines as text array */
	if (n > 0)
		values[0] = makeArrayResult(astate, CurrentMemoryContext);
	else
	{
		int16		typlen;
		bool		typbyval;
		char		typalign;
		ArrayType  *arr;

		get_typlenbyvalalign(TEXTOID, &typlen, &typbyval, &typalign);
		arr = construct_md_array(
			NULL,
			NULL,
			0, NULL, NULL, TEXTOID, typlen, typbyval, typalign);
		values[0] = PointerGetDatum(arr);
	}

	/* 1: # of lines as integer */
	values[1] = Int32GetDatum(n);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}
