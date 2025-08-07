#include "tdx_tool.h"
#include "utils/hsearch.h"

struct optional opt = {8080, 8080, 0, 0, 0, ".", 0, 0, -1, 0, 0, 32768, 0, 256, 0, 0, 0, 0, 0, 6, MAX_READ_THREAD_DEFAULT, THREAD_POOL_SIZE_DEFAULT};

void gprint(const request_t *r, const char *fmt, ...)
pg_attribute_printf(2, 3);

void gprintln(const request_t *r, const char *fmt, ...)
pg_attribute_printf(2, 3);

void gprintlnif(const request_t *r, const char *fmt, ...)
pg_attribute_printf(2, 3);

void gfatal(const request_t *r, const char *fmt, ...)
pg_attribute_printf(2, 3);

void gwarning(const request_t *r, const char *fmt, ...)
pg_attribute_printf(2, 3);

void gdebug(const request_t *r, const char *fmt, ...)
pg_attribute_printf(2, 3);

char *datetime(apr_time_t t)
{
	static char buf[100];
	apr_time_exp_t texp;
	
	apr_time_exp_lt(&texp, t);
	
	sprintf(buf,
	        "%04d-%02d-%02d %02d:%02d:%02d.%05d",
	        1900 + texp.tm_year,
	        1
	        + texp.tm_mon,
	        texp.tm_mday,
	        texp.tm_hour,
	        texp.tm_min,
	        texp.tm_sec,
			texp.tm_usec);
	
	return buf;
}

long datetime_diff(apr_time_t t1, apr_time_t t2)
{
	long ret;
	apr_time_exp_t texp1, texp2;
	
	apr_time_exp_lt(&texp1, t1);
	apr_time_exp_lt(&texp2, t2);
	
	ret = (texp2.tm_usec - texp1.tm_usec) / 1000 +
	           (texp2.tm_sec - texp1.tm_sec) * 1000 +
	           (texp2.tm_min - texp1.tm_min) * 60 * 1000 +
	           (texp2.tm_hour - texp1.tm_hour) * 24 * 60 * 1000;
	
	return ret;
}

char *datetime_now(void)
{
	return datetime(apr_time_now());
}

/* get process id */
static int ggetpid()
{
	static int pid = 0;
	if (pid == 0)
	{
#ifdef WIN32
		pid = GetCurrentProcessId();
#else
		pid = getpid();
#endif
	}
	
	return pid;
}

void _gprint(const request_t *r, const char *level, const char *fmt, va_list args)
pg_attribute_printf(3, 0);

void _gprint(const request_t *r, const char *level, const char *fmt, va_list args)
{
	printf("%s %d %s ", datetime_now(), ggetpid(), level);
	if (r != NULL)
		printf("[%ld:%ld:%d:%d] ", r->sid, r->id, r->segid, r->sock);
	vprintf(fmt, args);
}

void gprint(const request_t *r, const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	_gprint(r, "INFO", fmt, args);
	va_end(args);
}

void gprintln(const request_t *r, const char *fmt, ...)
{
	va_list args;

	if (opt.s)
		return;

	va_start(args, fmt);
	_gprint(r, "INFO", fmt, args);
	va_end(args);
	printf("\n");
}

/* Print for GET, or POST if Verbose */
void gprintlnif(const request_t *r, const char *fmt, ...)
{
	va_list args;

	if (r != NULL && !r->is_get && !opt.V)
		return;
	
	if (opt.s)
		return;

	va_start(args, fmt);
	_gprint(r, "INFO", fmt, args);
	va_end(args);
	printf("\n");
}

void gfatal(const request_t *r, const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	_gprint(r, "FATAL", fmt, args);
	va_end(args);
	
	printf("\n          ... exiting\n");
	exit(1);
}

void gwarning(const request_t *r, const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	_gprint(r, "WARN", fmt, args);
	va_end(args);
	printf("\n");
}

void gdebug(const request_t *r, const char *fmt, ...)
{
	va_list args;

	if (!opt.V)
		return;

	va_start(args, fmt);
	_gprint(r, "DEBUG", fmt, args);
	va_end(args);
	printf("\n");
}

bool is_valid_timeout(int timeout_val)
{
	if (timeout_val == 0)
		return true;

	if (timeout_val <= 1 || timeout_val > 7200)
		return false;
	else
		return true;
}

bool is_valid_session_timeout(int timeout_val)
{
	if (timeout_val == 0)
		return true;
	
	if (timeout_val < 0 || timeout_val > 7200)
		return false;
	else
		return true;
	
}

bool is_valid_listen_queue_size(int listen_queue_size)
{
	if (listen_queue_size < 16 || listen_queue_size > 512)
		return false;
	else
		return true;
}

#define SECONDS_IN_HOUR (60 * 60)
#define SECONDS_IN_MINUTE (60)
#define MILLISECONDS_IN_SECOND (1000)
#define MILLISECONDS_IN_SECOND_LOG10 (3)
#define MICROSECONDS_IN_SECOND (1000000L)
#define MICROSECONDS_IN_SECOND_LOG10 (6)
#define MICROSECONDS_IN_MILLISECOND (1000)

/*
 * Print the time represented by 'fracseconds' since midnight,
 * in PostgreSQL 'TIME' format.
 *
 * @param fracseconds time of day, expressed in fractions of second. A concrete
 * fraction is specified by 'frac_multiplier'
 * @param frac_multiplier specifier of the fraction. A value, such that
 * 'fracseconds / frac_multipler = seconds'
 * @param frac_width specifier of the fraction: log10(frac_multiplier); the
 * number of leading zeros.
 */
static bool
fracseconds_to_time_string(int64_t fracseconds, int64_t frac_multiplier, int frac_width, StringInfo buff)
{
	int32_t hours = fracseconds / (frac_multiplier * SECONDS_IN_HOUR);
	int32_t minutes;
	int32_t seconds;
	int64_t fracs;

	fracseconds = fracseconds % (frac_multiplier * SECONDS_IN_HOUR);
	minutes = fracseconds / (frac_multiplier * SECONDS_IN_MINUTE);
	
	fracseconds = fracseconds % (frac_multiplier * SECONDS_IN_MINUTE);
	seconds = fracseconds / frac_multiplier;
	fracs = fracseconds % frac_multiplier;
	
	appendStringInfo(buff, "%02d:%02d:%02d.%0*ld", hours, minutes, seconds, frac_width, fracs);
	return true;
}

/*
 * Print the date represented by 'days' since the Epoch into 'buff', in the
 * ISO 8601 date format (%Y-%m-%d).
 */
static bool
epoch_days_to_date_string(int32_t days, StringInfo buff)
{
	int			year;
	int			month;
	int			day;
	
	j2date(days + UNIX_EPOCH_JDATE, &year, &month, &day);
	
	appendStringInfo(buff, "%04d-%02d-%02d", year, month, day);
	return true;
}

/*
 * Print the timestamp represented by 'useconds' (microseconds) since the Epoch
 * into 'buff', in the ISO 8601 date format combined with PostgreSQL TIME
 * format (%Y-%m-%d %H:%M:%S.<microseconds> ).
 */
static bool
epoch_useconds_to_timestamp_string(int64_t useconds, StringInfo buff)
{
	struct pg_tm tm;			/* Timestamp split into parts */
	fsec_t		tm_fsec;		/* Fraction of a second */
	
	memset(&tm, 0, sizeof(struct pg_tm));
	
	/*
	 * timestamp2tm() is a PostgreSQL function. To represent time, PostgreSQL
	 * uses its own epoch which starts at 2000-01-01 00:00:00. In order to
	 * convert the result to UNIX epoch, subtract 30 years.
	 *
	 * This is NOT correct. There are leap seconds between UNIX and PostgreSQL
	 * epoch dates; thus timestamp2tm() should produce results which differ
	 * from mktime() ones (which counts dates from UNIX epoch) not just by 30
	 * years, but by 30 years and 22 seconds (there were 22 leap seconds
	 * between years 1970 and 2000).
	 *
	 * However, the results produced by both methods ACTUALLY differ only by
	 * 30 years (and zero seconds); thus we rely on the match of
	 * implementation results of mktime() and timestamp2tm().
	 *
	 * The reason why PostgreSQL implementation is incorrect is that
	 * timestamp2tm() uses TMODULO() macros internally, which assumes the
	 * length of a day (in seconds) does not depend on the date. This is an
	 * invalid assumption. An interesting question is why Linux / glibc
	 * implementation of mktime() does not account for leap seconds...
	 */
	
	useconds -= USECS_PER_DAY * (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE);
	
	if (timestamp2tm(useconds, NULL, &tm, &tm_fsec, NULL, NULL) < 0)
	{
		gwarning(NULL, "Failed to convert value '%" PRId64 "' to a PostgreSQL timestamp", useconds);
		return false;
	}
	
	appendStringInfo(buff, "%04d-%02d-%02d %02d:%02d:%02d.%06d", tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, (int) tm_fsec);
	return true;
}

/* 
 * time.precision.mode=connect
 * interval.handling.mode=numeric
 * */
static bool
convertDateTimeData(const char *value, Oid typeOid, StringInfo buff)
{
	bool err = true;
	
	switch (typeOid)
	{
		case DATEOID:
			err = epoch_days_to_date_string((int32)atol(value), buff);
			break;
		case TIMEOID: /* MUST BE MILLISECONDS */
			err = fracseconds_to_time_string(atoll(value), MILLISECONDS_IN_SECOND, MILLISECONDS_IN_SECOND_LOG10, buff);
			break;
		case TIMESTAMPOID: /* MUST BE MILLISECONDS*/
			err = epoch_useconds_to_timestamp_string(atoll(value) * MILLISECONDS_IN_SECOND, buff);
			break;
		case BITOID:
		case VARBITOID:
			// TODO
		case INTERVALOID:
		default:
			/* 
			 * other data type all treated as string. 
			 * decimal.handling.mode=string
			 */
			appendStringInfo(buff, "%s", value);
	}
	return err;
}

/* 
 * deserialize a message 
 * return: ret 
 */
bool 
json_deserialize(json_c_object *jobject, session_t *session, line_item* ret)
{
	json_c_object *tmp_object, *op_object, *change_object;
	int col_idx = 0;
	ListCell *lc;
	int i = 0;
	char *key;
	const char *op;
	pthread_mutex_t *err_mutex = &session->err_mutex;
	
	op_object = jobject;
	ret->has_before = false;
	ret->has_after = false;
	foreach(lc, session->op_pos)
	{
		key = (char *) lfirst(lc);
		op_object = json_object_object_get(op_object, key);
	}
	
	if (op_object)
	{
		op = json_object_get_string(op_object);
		gdebug(NULL, "op - %s", op);
		ret->op = op[0]; /* I D U */
	}
	else
	{
		pthread_mutex_lock(err_mutex);
		if (!session->is_error)
		{
			session->errmsg = apr_psprintf(session->pool, "no operator specified in msg.");
			session->is_error = 1;
			gwarning(NULL, "%s", session->errmsg);
		}
		pthread_mutex_unlock(err_mutex);
		return false;
	}
	
	for (i = 0; i < session->fields_num; ++i)
	{
		ret->raw_fields_before[i] = NULL;
		ret->isnull_before[i] = true;
		ret->raw_fields_after[i] = NULL;
		ret->isnull_after[i] = true;
	}
	
	/* BEFORE json */
	change_object = jobject;
	foreach(lc, session->before_pos)
	{
		key = (char *) lfirst(lc);
		change_object = json_object_object_get(change_object, key);
	}
	/* "before": {
	 *      "w_id": 2,
	 *      "w_ytd": "AcnDgA==",
	 *      "w_tax": "BU4=",
	 *      "w_name": "FhbucbOHx",
	 *      "w_street_1": "MYh2qeXL8jaOuGddFHV",
	 *      "w_street_2": "QPehNNzibGpHZe",
	 *      "w_city": "bAWJvXpslzktIKCaGA",
	 *      "w_state": "AO",
	 *      "w_zip": "373611111"
	 *      }
	 */
	if (json_object_is_type(change_object, json_type_object))
	{
		col_idx = 0;
		ret->has_before = true;
		
		/* get value for each column */
		foreach(lc, session->column_name)
		{
			key = (char *) lfirst(lc);
			tmp_object = change_object;
			
			tmp_object = json_object_object_get(tmp_object, key);
			if (json_object_is_type(tmp_object, json_type_null))
			{
				ret->raw_fields_before[col_idx] = NULL;
				ret->isnull_before[col_idx] = true;
			}
			else
			{
				StringInfoData buff;
				const char *value = json_object_get_string(tmp_object);
				gdebug(NULL, "before[%d] %s - %s", col_idx, key, value);
				
				initStringInfo(&buff);
				if (session->need_convert_datetime)
				{
					Oid typeOid = (Oid) atoi((char *) list_nth(session->column_type, col_idx));
					bool err = convertDateTimeData(value, typeOid, &buff);
					
					if (!err)
					{
						pthread_mutex_lock(err_mutex);
						if (!session->is_error)
						{
							session->errmsg = apr_psprintf(session->pool, "Failed to convert value to a PostgreSQL timestamp");
							session->is_error = 1;
							gwarning(NULL, "%s", session->errmsg);
						}
						pthread_mutex_unlock(err_mutex);
						return false;
					}
				}
				else
				{
					appendStringInfo(&buff, "%s", value);
				}
				
				ret->raw_fields_before[col_idx] = buff.data;
				ret->isnull_before[col_idx] = false;
			}
			col_idx ++;
		}
	}
	else if (!json_object_is_type(change_object, json_type_null))
	{
		pthread_mutex_lock(err_mutex);
		if (!session->is_error)
		{
			session->errmsg = apr_psprintf(session->pool, "no before json data specified in msg");
			session->is_error = 1;
			gwarning(NULL, "%s", session->errmsg);
		}
		pthread_mutex_unlock(err_mutex);
		return false;
	}
	
	/* AFTER json */
	change_object = jobject;
	foreach(lc, session->after_pos)
	{
		key = (char *) lfirst(lc);
		change_object = json_object_object_get(change_object, key);
	}
	/* "after": {
	 *      "w_id": 2,
	 *      "w_ytd": "AcnDgA==",
	 *      "w_tax": "BU4=",
	 *      "w_name": "FhbucbOHx",
	 *      "w_street_1": "MYh2qeXL8jaOuGddFHV",
	 *      "w_street_2": "QPehNNzibGpHZe",
	 *      "w_city": "bAWJvXpslzktIKCaGA",
	 *      "w_state": "AO",
	 *      "w_zip": "373611111"
	 *      }
	 */
	if (json_object_is_type(change_object, json_type_object))
	{
		col_idx = 0;
		ret->has_after = true;
		
		/* get value for each column */
		foreach(lc, session->column_name)
		{
			key = (char *) lfirst(lc);
			tmp_object = change_object;
			
			tmp_object = json_object_object_get(tmp_object, key);
			if (json_object_is_type(tmp_object, json_type_null))
			{
				ret->raw_fields_after[col_idx] = NULL;
				ret->isnull_after[col_idx] = true;
			}
			else
			{
				StringInfoData buff;
				const char *value = json_object_get_string(tmp_object);
				gdebug(NULL, "after[%d] %s - %s", col_idx, key, value);
				
				initStringInfo(&buff);
				if (session->need_convert_datetime)
				{
					Oid typeOid = (Oid) atoi((char *) list_nth(session->column_type, col_idx));
					
					bool err = convertDateTimeData(value, typeOid, &buff);
					if (!err)
					{
						pthread_mutex_lock(err_mutex);
						if (!session->is_error)
						{
							session->errmsg = apr_psprintf(session->pool,
							                               "Failed to convert value to a PostgreSQL timestamp");
							session->is_error = 1;
							gwarning(NULL, "%s", session->errmsg);
						}
						pthread_mutex_unlock(err_mutex);
						return false;
					}
				}
				else
				{
					appendStringInfo(&buff, "%s", value);
				}
				ret->raw_fields_after[col_idx] = buff.data;
				ret->isnull_after[col_idx] = false;
			}
			col_idx ++;
		}
	}
	else if (!json_object_is_type(change_object, json_type_null))
	{
		pthread_mutex_lock(err_mutex);
		if (!session->is_error)
		{
			session->errmsg = apr_psprintf(session->pool, "no after json data specified in msg");
			session->is_error = 1;
			gwarning(NULL, "%s", session->errmsg);
		}
		pthread_mutex_unlock(err_mutex);
		return false;
	}
	return true;
}

