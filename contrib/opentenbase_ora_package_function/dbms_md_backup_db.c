/*-------------------------------------------------------------------------
 *
 * pg_backup_db.c
 *
 *	Implements the basic DB functions used by the archiver.
 *
 * IDENTIFICATION
 *	  src/bin/pg_dump/pg_backup_db.c
 *
 *-------------------------------------------------------------------------
 */

#include <unistd.h>
#include <ctype.h>
#include "postgres.h"
#include "lib/stringinfo.h"
#include "utils/memutils.h"
#include "utils/formatting.h"
#include "catalog/pg_collation.h"

#include "dbms_md_dumputils.h"
#include "dbms_md_backup_archiver.h"
#include "dbms_md_backup_db.h"
#include "executor/spi.h"

#ifdef HAVE_TERMIOS_H
#include <termios.h>
#endif

static int g_spi_connect = 0;
static int g_spi_close = 0;

static bool dbms_md_check_tup_params(SPITupleTable *tups, int tup_idx, int field_idx)
{
	if (NULL == tups)
	{
		elog(ERROR, "SPITupleTable is null");
		return false;
	}

	if (NULL == tups->tupdesc)
	{
		elog(ERROR, "Invalid tupdesc for TupleTable");
		return false;
	}
	
	if (tups->tupdesc->natts < field_idx || field_idx < 0)
	{
		elog(ERROR, "Invalid field subscript for tupdesc field_idx=%d, tupdesc->natts=%d", 
					field_idx, tups->tupdesc->natts);
		return false;
	}

	return true;
}

void dbms_md_free_tuples(SPITupleTable *tups) 
{
	int ret = 0;
	SPI_freetuptable(tups);

	ret = SPI_finish();

	g_spi_close++;
	elog(DEBUG3, "[dbms_md_free_tuples]g_spi_close=%d, g_spi_connect=%d", g_spi_close, g_spi_connect);

	if (SPI_OK_FINISH != ret)
	{
		elog(DEBUG3, "[dbms_md_free_tuples]ret=%d, g_spi_close=%d, g_spi_connect=%d",
			ret, g_spi_close, g_spi_connect);
	}
}

const char* dbms_md_get_field_name(SPITupleTable *tups, int field_idx) 
{
	if (!dbms_md_check_tup_params(tups,0, field_idx))
		return NULL;
	
	return SPI_fname(tups->tupdesc, field_idx);
}

int
dbms_md_get_field_val_len(SPITupleTable *tups, int tup_idx, int field_idx)
{
	return strlen(dbms_md_get_field_value(tups, tup_idx, field_idx));
}


char* 
dbms_md_get_field_value(SPITupleTable *tups, int tup_idx, int field_idx)
{
	char	*val, *new;

	if (!dbms_md_check_tup_params(tups,tup_idx, field_idx))
		return NULL;
	
	val = SPI_getvalue(tups->vals[tup_idx], tups->tupdesc, field_idx);
	if (val == NULL)
		new = pstrdup("");
	else
	{
		new = pstrdup(val);
		pfree(val);
	}

	return new;
}

char* 
dbms_md_get_field_strval(SPITupleTable *tups, int tup_idx, int field_idx)
{
	char	*val, *new;

	if (!dbms_md_check_tup_params(tups,tup_idx, field_idx))
		return NULL;
	
	val = SPI_getvalue(tups->vals[tup_idx], tups->tupdesc, field_idx);
	if (val == NULL)
		new = pstrdup("");
	else
	{
		new = pstrdup(val);
		pfree(val);
	}

	return new;
}

int dbms_md_get_tuple_num(SPITupleTable *tups)
{
	return SPI_processed;
}

int dbms_md_get_field_subscript(SPITupleTable *tups, const char* field_name)
{
	char *upname = pstrdup(field_name);

	if (NULL == tups)
		return -1;

	if (ORA_MODE)
		upname = str_toupper(field_name, strlen(field_name), DEFAULT_COLLATION_OID);

	return SPI_fnumber(tups->tupdesc, upname);
}



/* dbms_md_is_tuple_field_null:
 *	returns the null status of a field value.
 */
int
dbms_md_is_tuple_field_null(SPITupleTable *tups, int tup_idx, int field_idx)
{
	bool isnull = false;
	
	if (!dbms_md_check_tup_params(tups,tup_idx, field_idx))
		return 1;
	
	SPI_getbinval(tups->vals[tup_idx], tups->tupdesc, field_idx, &isnull);

	return isnull ? 1:0;
}


/*
 * Make a database connection with the given parameters.  The
 * connection handle is returned, the parameters are stored in AHX.
 * An interactive password prompt is automatically issued if required.
 *
 * Note: it's not really all that sensible to use a single-entry password
 * cache if the username keeps changing.  In current usage, however, the
 * username never does change, so one savedPassword is sufficient.
 */
void
ConnectDatabase(Archive *AHX,
				const char *dbname,
				const char *pghost,
				const char *pgport,
				const char *username,
				trivalue prompt_password)
{
	/*ArchiveHandle *AH = (ArchiveHandle *) AHX;
	int ret = SPI_connect();
	if (ret < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SPI connect failure on node '%s', returned %d",
						PGXCNodeName, ret)));
	}
	AH->connection = ret;*/
}

void
ReconnectToServer(ArchiveHandle *AH, const char *dbname)
{
	return;
}

/*
 * Close the connection to the database and also cancel off the query if we
 * have one running.
 */
void
DisconnectDatabase(Archive *AHX)
{
#if 0
	ArchiveHandle *AH = (ArchiveHandle *) AHX;
	char		errbuf[1];

	if (!AH->connection)
		return;

	if (AH->connCancel)
	{
		/*
		 * If we have an active query, send a cancel before closing, ignoring
		 * any errors.  This is of no use for a normal exit, but might be
		 * helpful during exit_horribly().
		 */
		if (PQtransactionStatus(AH->connection) == PQTRANS_ACTIVE)
			(void) PQcancel(AH->connCancel, errbuf, sizeof(errbuf));

		/*
		 * Prevent signal handler from sending a cancel after this.
		 */
		set_archive_cancel_info(AH, NULL);
	}

	PQfinish(AH->connection);
	AH->connection = NULL;
#endif
}


void
ExecuteSqlStatement(Archive *AHX, const char *query)
{
	/*int ret;
	//ArchiveHandle *AH = (ArchiveHandle *) AHX;

	if ((ret = SPI_connect()) < 0)	
	{
		ereport(ERROR, (errmsg("SPI_connect failed: error code %d", ret)));	
		return ;
	}

	ret = SPI_execute(query, true, 0);
	if (ret != SPI_OK_SELECT)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to execute '%s' on node '%s', ret=%d",
						query, PGXCNodeName, ret)));
	}

	SPI_finish();*/
}

void dbms_md_exe_sql_no_result(Archive *AHX, const char* query)
{
	int ret;
	//ArchiveHandle *AH = (ArchiveHandle *) AHX;

	if ((ret = SPI_connect()) < 0)	
	{
		ereport(ERROR, (errmsg("SPI_connect failed: error code %d", ret)));	
		return ;
	}
	SPI_set_and_check_pl_conflict(ORA_PLSQL);

	ret = SPI_execute(query, false, 0);
	if (ret != SPI_OK_UTILITY)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to execute '%s' on node '%s', ret=%d",
						query, PGXCNodeName, ret)));
	}
	elog(DEBUG3, "query[%s]", query);
	SPI_finish();
}

SPITupleTable *ExecuteSqlQuery(Archive *AHX, const char *query)
{
	//ArchiveHandle *AH = (ArchiveHandle *) AHX;
	int   ret = 0;

	if ((ret = SPI_connect()) < 0)	
	{
		ereport(ERROR, (errmsg("SPI_connect failed: error code %d", ret)));	
		return NULL;
	}
	SPI_set_and_check_pl_conflict(ORA_PLSQL);

	g_spi_connect++;
	elog(DEBUG3, "[ExecuteSqlQuery]g_spi_close=%d, g_spi_connect=%d",
		g_spi_close, g_spi_connect);

	ret = SPI_execute(query, true, DBMS_MD_MAX_TUPS);
	if (ret != SPI_OK_SELECT)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to execute '%s' on node '%s', ret=%d",
						query, PGXCNodeName, ret)));
	}
#if 0
	if (SPI_processed <= 0)
	{
		/*elog(INFO, "query(%s) return null", query);		*/
		return NULL;
	}
#endif

	return SPI_tuptable;
}

/*
 * Execute an SQL query and verify that we got exactly one row back.
 */
SPITupleTable *ExecuteSqlQueryForSingleRow(Archive *fout, char *query)
{
	return ExecuteSqlQuery(fout, query);
}

void
DropBlobIfExists(ArchiveHandle *AH, Oid oid)
{
	/*
	 * If we are not restoring to a direct database connection, we have to
	 * guess about how to detect whether the blob exists.  Assume new-style.
	 */
	if (AH->version >= 90000)
	{
		ahprintf(AH,
				 "SELECT pg_catalog.lo_unlink(oid) "
				 "FROM pg_catalog.pg_largeobject_metadata "
				 "WHERE oid = '%u';\n",
				 oid);
	}
	else
	{
		/* Restoring to pre-9.0 server, so do it the old way */
		ahprintf(AH,
				 "SELECT CASE WHEN EXISTS("
				 "SELECT 1 FROM pg_catalog.pg_largeobject WHERE loid = '%u'"
				 ") THEN pg_catalog.lo_unlink('%u') END;\n",
				 oid, oid);
	}
}


