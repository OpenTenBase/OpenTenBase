#ifndef PG_ORA_SEQUENCE_H
#define PG_ORA_SEQUENCE_H

#include "catalog/genbki.h"
#include "utils/numeric.h"

#define OraSequenceRelationId	9805

#define SeqOrderOption 0x0001

/* opentenbase_ora max sequence value 28 nums : 9999999999999999999999999999 */
/* #define OPENTENBASE_ORA_MAX_SEQUENCE (((int128) 1000000000000000LL) * 10000000000000LL - 1) */
#define OPENTENBASE_ORA_MAX_SEQUENCE (IS_CENTRALIZED_MODE ? ((((int128) 1000000000000000LL) * 10000000000000LL - 1)) : (INT64_MAX))
/* opentenbase_ora min sequence value 28 nums : -999999999999999999999999999 */
/* #define OPENTENBASE_ORA_MIN_SEQUENCE (((int128) -1000000000000000LL) * 1000000000000LL + 1) */
#define OPENTENBASE_ORA_MIN_SEQUENCE (IS_CENTRALIZED_MODE ? ((((int128) -1000000000000000LL) * 1000000000000LL + 1)) : (INT64_MIN))
/* Int128 abs function */
#define INT128_ABS(x) (x > 0 ? x : -x)

CATALOG(pg_ora_sequence,9805) BKI_WITHOUT_OIDS
{
	Oid			seqrelid;
	Oid			seqtypid;
	bool		seqcycle;
	bool		seqlocal;
	int64		seqoptions;
	NumericData	seqcache;
	NumericData	seqstart;
	NumericData	seqincrement;
	NumericData	seqmax;
	NumericData	seqmin;
} FormData_pg_ora_sequence;

typedef FormData_pg_ora_sequence *Form_pg_ora_sequence;

#define Natts_pg_ora_sequence				10
#define Anum_pg_ora_sequence_seqrelid		1
#define Anum_pg_ora_sequence_seqtypid		2
#define Anum_pg_ora_sequence_seqcycle		3
#define Anum_pg_ora_sequence_seqlocal		4
#define Anum_pg_ora_sequence_seqoptions		5
#define Anum_pg_ora_sequence_seqcache		6
#define Anum_pg_ora_sequence_seqstart		7
#define Anum_pg_ora_sequence_seqincrement	8
#define Anum_pg_ora_sequence_seqmax			9
#define Anum_pg_ora_sequence_seqmin			10


#endif							/* PG_ORA_SEQUENCE_H */
