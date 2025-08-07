#include "postgres.h"
#include "utils/gpfxdist.h"
#include "libpq/pqformat.h"
#include "logical_proto.h"

/* Write a tuple to the outputstream */
static void
logical_write_values(StringInfo out, char **values, bool *isnull, int nfields)
{
	int			i;
	pq_sendint16(out, (int16)nfields);
	
	/* Write the values */
	for (i = 0; i < nfields; i++)
	{
		if (isnull[i])
		{
			enlargeStringInfo(out, 1);
			pq_sendbyte(out, 'n');	/* null column */
			continue;
		}
		
		/* maybe too frequently?? */
		enlargeStringInfo(out, strlen(values[i]) + 1 + 4);
		/* text */
		pq_sendbyte(out, 't');	/* 'text' data follows */
		
		pq_sendcountedtext(out, values[i], strlen(values[i]), false);
	}
}

/* Write DELETE to the output stream. */
void
logical_write_delete_op(StringInfo out, char **values, bool *isnull, int nfields, bool only_key)
{
	pq_sendbyte(out, 'D');		/* action DELETE */
	pq_sendbyte(out, 'd');	/* default replident d */
	
	if (!only_key)
		pq_sendbyte(out, 'O');	/* old tuple follows */
	else
		pq_sendbyte(out, 'K');	/* old key follows */
	logical_write_values(out, values, isnull, nfields);
}

/* Write INSERT to the output stream. */
void
logical_write_insert_op(StringInfo out, char **values, bool *isnull, int nfields)
{
	pq_sendbyte(out, 'I');		/* action INSERT */
	pq_sendbyte(out, 'N');		/* new tuple follows */
	logical_write_values(out, values, isnull, nfields);
}

/* Write UPDATE to the output stream. */
// TODO: update distribution key is prohibited.
void
logical_write_update_op(StringInfo out, char **old_values, bool *old_isnull,
                     char **new_values, bool *new_isnull, int nfields, bool only_key)
{
	pq_sendbyte(out, 'U');		/* action UPDATE */
	
	pq_sendbyte(out, 'd');	/* default replident d */
	if (old_values != NULL)
	{
		if (!only_key)
			pq_sendbyte(out, 'O');	/* old tuple follows */
		else
			pq_sendbyte(out, 'K');	/* old key follows */
		logical_write_values(out, old_values, old_isnull, nfields);
	}
	
	pq_sendbyte(out, 'N');	/* New tuple follows */
	logical_write_values(out, new_values, new_isnull, nfields);
}
