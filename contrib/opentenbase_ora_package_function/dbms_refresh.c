#include "postgres.h"
#include "access/hash.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "executor/spi.h"

#include "stdlib.h"
#include <errno.h>

#include "opentenbase_ora.h"
#include "builtins.h"

PG_FUNCTION_INFO_V1(dbms_refresh_refresh);

Datum
dbms_refresh_refresh(PG_FUNCTION_ARGS)
{
	VarChar    *source = PG_GETARG_VARCHAR_PP(0);
	int32		n_len;
	char	   *n_data;
	char		*mview;
	bool snapshot_set = false;
	int ret;
	StringInfoData cmd;

	n_len = VARSIZE_ANY_EXHDR(source);
	n_data = VARDATA_ANY(source);

	if (n_len <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Invalid name of materialized view.")));

	PG_TRY();
	{
		if (!ActiveSnapshotSet())
		{
			PushActiveSnapshot(GetTransactionSnapshot());
			snapshot_set = true;
		}

		SPI_connect();

		mview = pnstrdup(n_data, n_len);
		initStringInfo(&cmd);
		appendStringInfo(&cmd, "REFRESH MATERIALIZED VIEW %s", mview);
		pfree(mview);

		ret = SPI_execute(cmd.data, false, 0);
		if (ret < 0)
			elog(ERROR, "Failed: %s", cmd.data);

		pfree(cmd.data);
		SPI_finish();
		
		if (snapshot_set)
			PopActiveSnapshot();
	}
	PG_CATCH();
	{
		if (snapshot_set)
			PopActiveSnapshot();

		PG_RE_THROW();
	}
	PG_END_TRY();

	PG_RETURN_VOID();
}
