/*-------------------------------------------------------------------------
 *
 * pg_cast.h
 *	  definition of the system "type casts" relation (pg_cast)
 *	  along with the relation's initial contents.
 *
 * As of Postgres 8.0, pg_cast describes not only type coercion functions
 * but also length coercion functions.
 *
 *
 * Copyright (c) 2002-2017, PostgreSQL Global Development Group
 *
 * src/include/catalog/pg_cast.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CAST_H
#define PG_CAST_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_cast definition.  cpp turns this into
 *		typedef struct FormData_pg_cast
 * ----------------
 */
#define CastRelationId	2605

CATALOG(pg_cast,2605)
{
	Oid			castsource;		/* source datatype for cast */
	Oid			casttarget;		/* destination datatype for cast */
	Oid			castfunc;		/* cast function; 0 = binary coercible */
	char		castcontext;	/* contexts in which cast can be used */
	char		castmethod;		/* cast method */
} FormData_pg_cast;

typedef FormData_pg_cast *Form_pg_cast;

/*
 * The allowable values for pg_cast.castcontext are specified by this enum.
 * Since castcontext is stored as a "char", we use ASCII codes for human
 * convenience in reading the table.  Note that internally to the backend,
 * these values are converted to the CoercionContext enum (see primnodes.h),
 * which is defined to sort in a convenient order; the ASCII codes don't
 * have to sort in any special order.
 */

typedef enum CoercionCodes
{
	COERCION_CODE_IMPLICIT = 'i',	/* coercion in context of expression */
	COERCION_CODE_ASSIGNMENT = 'a', /* coercion in context of assignment */
	COERCION_CODE_EXPLICIT = 'e',	/* explicit cast operation */
	COERCION_CODE_LIGHTWEIGHT_ORA_IMPLICIT = 'x', /* lightweight_ora mode: coercion in context of expression */
	COERCION_CODE_LIGHTWEIGHT_ORA_ASSIGNMENT = 'z', /* lightweight_ora mode: coercion in context of assignment */
	COERCION_CODE_ORA_IMPLICIT = 'o',	/* coercion in context of expression for opentenbase_ora */
	COERCION_CODE_ORA_SPECIAL = 's'	/* coercion in context of expression for special case, like assignment but have higher priority */
} CoercionCodes;

/*
 * The allowable values for pg_cast.castmethod are specified by this enum.
 * Since castmethod is stored as a "char", we use ASCII codes for human
 * convenience in reading the table.
 */
typedef enum CoercionMethod
{
	COERCION_METHOD_FUNCTION = 'f', /* use a function */
	COERCION_METHOD_BINARY = 'b',	/* types are binary-compatible */
	COERCION_METHOD_INOUT = 'i' /* use input/output functions */
} CoercionMethod;


/* ----------------
 *		compiler constants for pg_cast
 * ----------------
 */
#define Natts_pg_cast				5
#define Anum_pg_cast_castsource		1
#define Anum_pg_cast_casttarget		2
#define Anum_pg_cast_castfunc		3
#define Anum_pg_cast_castcontext	4
#define Anum_pg_cast_castmethod		5

/* ----------------
 *		initial contents of pg_cast
 *
 * Note: this table has OIDs, but we don't bother to assign them manually,
 * since nothing needs to know the specific OID of any built-in cast.
 * ----------------
 */

/*
 * Numeric category: implicit casts are allowed in the direction
 * int2->int4->int8->numeric->float4->float8, while casts in the
 * reverse direction are assignment-only.
 */
DATA(insert (	20	 21  714 a f ));
DATA(insert (	20	 23  480 a f ));
DATA(insert (	20	700  652 i f ));
DATA(insert (	20	701  482 i f ));
DATA(insert (	20 1700 1781 i f ));
DATA(insert (	21	 20  754 i f ));
DATA(insert (	21	 23  313 i f ));
DATA(insert (	21	700  236 i f ));
DATA(insert (	21	701  235 i f ));
DATA(insert (	21 1700 1782 i f ));
DATA(insert (	23	 20  481 i f ));
DATA(insert (	23	 21  314 a f ));
DATA(insert (	23	700  318 i f ));
DATA(insert (	23	701  316 i f ));
DATA(insert (	23 1700 1740 i f ));
DATA(insert (  700	 20  653 a f ));
DATA(insert (  700	 21  238 a f ));
DATA(insert (  700	 23  319 a f ));
DATA(insert (  700	701  311 i f ));
DATA(insert (  700 1700 1742 a f ));
DATA(insert (  701	 20  483 a f ));
DATA(insert (  701	 21  237 a f ));
DATA(insert (  701	 23  317 a f ));
DATA(insert (  701	700  312 a f ));
PGDATA(insert (  701 1700 1743 a f ));
ORADATA(insert (  701 1700 8143 a f ));
DATA(insert ( 1700	 20 1779 a f ));
DATA(insert ( 1700	 21 1783 a f ));
DATA(insert ( 1700	 23 1744 a f ));
DATA(insert ( 1700	700 1745 i f ));
DATA(insert ( 1700	701 1746 i f ));
DATA(insert (  790 1700 3823 a f ));
DATA(insert ( 1700	790 3824 a f ));
DATA(insert ( 23	790 3811 a f ));
DATA(insert ( 20	790 3812 a f ));

/* numericd */
DATA(insert ( 9301	 20 9601 a f ));
DATA(insert ( 9301	 21 9602 a f ));
DATA(insert ( 9301	 23 9603 a f ));
DATA(insert ( 9301	700 9604 i f ));
DATA(insert ( 9301	701 9605 i f ));
DATA(insert ( 9301 1700 9606 a f ));
DATA(insert (	20 9301 9607 i f ));
DATA(insert (	21 9301 9608 i f ));
DATA(insert (	23 9301 9609 i f ));
DATA(insert (  700 9301 9610 a f ));
DATA(insert (  701 9301 9611 a f ));
DATA(insert ( 1700 9301 9612 i f ));

/* numericf */
DATA(insert ( 9351	 20 9651 a f ));
DATA(insert ( 9351	 21 9652 a f ));
DATA(insert ( 9351	 23 9653 a f ));
DATA(insert ( 9351	700 9654 i f ));
DATA(insert ( 9351	701 9655 i f ));
DATA(insert ( 9351 1700 9656 a f ));
DATA(insert ( 9351 9301 9663 i f ));
DATA(insert (	20 9351 9657 i f ));
DATA(insert (	21 9351 9658 i f ));
DATA(insert (	23 9351 9659 i f ));
DATA(insert (  700 9351 9660 a f ));
DATA(insert (  701 9351 9661 a f ));
DATA(insert ( 1700 9351 9662 i f ));

/* Allow explicit coercions between int4 and bool */
DATA(insert (	23	16	2557 e f ));
DATA(insert (	16	23	2558 e f ));

/*
 * OID category: allow implicit conversion from any integral type (including
 * int8, to support OID literals > 2G) to OID, as well as assignment coercion
 * from OID to int4 or int8.  Similarly for each OID-alias type.  Also allow
 * implicit coercions between OID and each OID-alias type, as well as
 * regproc<->regprocedure and regoper<->regoperator.  (Other coercions
 * between alias types must pass through OID.)	Lastly, there are implicit
 * casts from text and varchar to regclass, which exist mainly to support
 * legacy forms of nextval() and related functions.
 */
DATA(insert (	20	 26 1287 i f ));
DATA(insert (	21	 26  313 i f ));
DATA(insert (	23	 26    0 i b ));
DATA(insert (	26	 20 1288 a f ));
DATA(insert (	26	 23    0 a b ));
DATA(insert (	26	 24    0 i b ));
DATA(insert (	24	 26    0 i b ));
DATA(insert (	20	 24 1287 i f ));
DATA(insert (	21	 24  313 i f ));
DATA(insert (	23	 24    0 i b ));
DATA(insert (	24	 20 1288 a f ));
DATA(insert (	24	 23    0 a b ));
DATA(insert (	24 2202    0 i b ));
DATA(insert ( 2202	 24    0 i b ));
DATA(insert (	26 2202    0 i b ));
DATA(insert ( 2202	 26    0 i b ));
DATA(insert (	20 2202 1287 i f ));
DATA(insert (	21 2202  313 i f ));
DATA(insert (	23 2202    0 i b ));
DATA(insert ( 2202	 20 1288 a f ));
DATA(insert ( 2202	 23    0 a b ));
DATA(insert (	26 2203    0 i b ));
DATA(insert ( 2203	 26    0 i b ));
DATA(insert (	20 2203 1287 i f ));
DATA(insert (	21 2203  313 i f ));
DATA(insert (	23 2203    0 i b ));
DATA(insert ( 2203	 20 1288 a f ));
DATA(insert ( 2203	 23    0 a b ));
DATA(insert ( 2203 2204    0 i b ));
DATA(insert ( 2204 2203    0 i b ));
DATA(insert (	26 2204    0 i b ));
DATA(insert ( 2204	 26    0 i b ));
DATA(insert (	20 2204 1287 i f ));
DATA(insert (	21 2204  313 i f ));
DATA(insert (	23 2204    0 i b ));
DATA(insert ( 2204	 20 1288 a f ));
DATA(insert ( 2204	 23    0 a b ));
DATA(insert (	26 2205    0 i b ));
DATA(insert ( 2205	 26    0 i b ));
DATA(insert (	20 2205 1287 i f ));
DATA(insert (	21 2205  313 i f ));
DATA(insert (	23 2205    0 i b ));
DATA(insert ( 2205	 20 1288 a f ));
DATA(insert ( 2205	 23    0 a b ));
DATA(insert (	26 2206    0 i b ));
DATA(insert ( 2206	 26    0 i b ));
DATA(insert (	20 2206 1287 i f ));
DATA(insert (	21 2206  313 i f ));
DATA(insert (	23 2206    0 i b ));
DATA(insert ( 2206	 20 1288 a f ));
DATA(insert ( 2206	 23    0 a b ));
DATA(insert (	26 3734    0 i b ));
DATA(insert ( 3734	 26    0 i b ));
DATA(insert (	20 3734 1287 i f ));
DATA(insert (	21 3734  313 i f ));
DATA(insert (	23 3734    0 i b ));
DATA(insert ( 3734	 20 1288 a f ));
DATA(insert ( 3734	 23    0 a b ));
DATA(insert (	26 3769    0 i b ));
DATA(insert ( 3769	 26    0 i b ));
DATA(insert (	20 3769 1287 i f ));
DATA(insert (	21 3769  313 i f ));
DATA(insert (	23 3769    0 i b ));
DATA(insert ( 3769	 20 1288 a f ));
DATA(insert ( 3769	 23    0 a b ));
DATA(insert (	25 2205 1079 i f ));
DATA(insert ( 1043 2205 1079 i f ));
DATA(insert (	26 4096    0 i b ));
DATA(insert ( 4096	 26    0 i b ));
DATA(insert (	20 4096 1287 i f ));
DATA(insert (	21 4096  313 i f ));
DATA(insert (	23 4096    0 i b ));
DATA(insert ( 4096	 20 1288 a f ));
DATA(insert ( 4096	 23    0 a b ));
DATA(insert (	26 4089    0 i b ));
DATA(insert ( 4089	 26    0 i b ));
DATA(insert (	20 4089 1287 i f ));
DATA(insert (	21 4089  313 i f ));
DATA(insert (	23 4089    0 i b ));
DATA(insert ( 4089	 20 1288 a f ));
DATA(insert ( 4089	 23    0 a b ));

/*
 * String category
 */
DATA(insert (	25 1042    0 i b ));
DATA(insert (	25 1043    0 i b ));
DATA(insert ( 1042	 25  401 i f ));
DATA(insert ( 1042 1043  401 i f ));
DATA(insert ( 1043	 25    0 i b ));
DATA(insert ( 1043 1042    0 i b ));
DATA(insert (	18	 25  946 i f ));
DATA(insert (	18 1042  860 a f ));
DATA(insert (	18 1043  946 a f ));
DATA(insert (	19	 25  406 i f ));
DATA(insert (	19 1042  408 a f ));
DATA(insert (	19 1043 1401 a f ));
DATA(insert (	25	 18  944 a f ));
DATA(insert ( 1042	 18  944 a f ));
DATA(insert ( 1043	 18  944 a f ));
DATA(insert (	25	 19  407 i f ));
DATA(insert ( 1042	 19  409 i f ));
DATA(insert ( 1043	 19 1400 i f ));

/* Allow explicit coercions between int4 and "char" */
DATA(insert (	18	 23   77 e f ));
DATA(insert (	23	 18   78 e f ));

/* pg_node_tree can be coerced to, but not from, text */
DATA(insert (  194	 25    0 i b ));

/* pg_ndistinct can be coerced to, but not from, bytea and text */
DATA(insert (  3361  17    0 i b ));
DATA(insert (  3361  25    0 i i ));

/* pg_dependencies can be coerced to, but not from, bytea and text */
DATA(insert (  3402  17    0 i b ));
DATA(insert (  3402  25    0 i i ));

/*
 * Datetime category
 */
DATA(insert (  702 1082 1179 a f ));
DATA(insert (  702 1083 1364 a f ));
DATA(insert (  702 1114 2023 i f ));
DATA(insert (  702 1184 1173 i f ));
DATA(insert (  703 1186 1177 i f ));
DATA(insert ( 1082 1114 2024 i f ));
DATA(insert ( 1082 1184 1174 i f ));
DATA(insert ( 1083 1186 1370 i f ));
DATA(insert ( 1083 1266 2047 i f ));
DATA(insert ( 1114	702 2030 a f ));
DATA(insert ( 1114 1082 2029 a f ));
DATA(insert ( 1114 1083 1316 a f ));
DATA(insert ( 1114 1184 2028 i f ));
DATA(insert ( 1184	702 1180 a f ));
DATA(insert ( 1184 1082 1178 a f ));
DATA(insert ( 1184 1083 2019 a f ));
DATA(insert ( 1184 1114 2027 a f ));
DATA(insert ( 1184 1266 1388 a f ));
DATA(insert ( 1186	703 1194 a f ));
DATA(insert ( 1186 1083 1419 a f ));
DATA(insert ( 1266 1083 2046 a f ));
/* Cross-category casts between int4 and abstime, reltime */
DATA(insert (	23	702    0 e b ));
DATA(insert (  702	 23    0 e b ));
DATA(insert (	23	703    0 e b ));
DATA(insert (  703	 23    0 e b ));

/*
 * Geometric category
 */
DATA(insert (  600	603 4091 a f ));
DATA(insert (  601	600 1532 e f ));
DATA(insert (  602	600 1533 e f ));
DATA(insert (  602	604 1449 a f ));
DATA(insert (  603	600 1534 e f ));
DATA(insert (  603	601 1541 e f ));
DATA(insert (  603	604 1448 a f ));
DATA(insert (  603	718 1479 e f ));
DATA(insert (  604	600 1540 e f ));
DATA(insert (  604	602 1447 a f ));
DATA(insert (  604	603 1446 e f ));
DATA(insert (  604	718 1474 e f ));
DATA(insert (  718	600 1416 e f ));
DATA(insert (  718	603 1480 e f ));
DATA(insert (  718	604 1544 e f ));

/*
 * MAC address category
 */
DATA(insert (  829	774    4123 i f ));
DATA(insert (  774	829    4124 i f ));

/*
 * INET category
 */
DATA(insert (  650	869    0 i b ));
DATA(insert (  869	650 1715 a f ));

/*
 * BitString category
 */
DATA(insert ( 1560 1562    0 i b ));
DATA(insert ( 1562 1560    0 i b ));
/* Cross-category casts between bit and int4, int8 */
DATA(insert (	20 1560 2075 e f ));
DATA(insert (	23 1560 1683 e f ));
DATA(insert ( 1560	 20 2076 e f ));
DATA(insert ( 1560	 23 1684 e f ));

/*
 * Cross-category casts to and from TEXT
 *
 * We need entries here only for a few specialized cases where the behavior
 * of the cast function differs from the datatype's I/O functions.  Otherwise,
 * parse_coerce.c will generate CoerceViaIO operations without any prompting.
 *
 * Note that the castcontext values specified here should be no stronger than
 * parse_coerce.c's automatic casts ('a' to text, 'e' from text) else odd
 * behavior will ensue when the automatic cast is applied instead of the
 * pg_cast entry!
 */
DATA(insert (  650	 25  730 a f ));
DATA(insert (  869	 25  730 a f ));
DATA(insert (	16	 25 2971 a f ));
DATA(insert (  142	 25    0 a b ));
DATA(insert (	25	142 2896 e f ));

/*
 * Cross-category casts to and from VARCHAR
 *
 * We support all the same casts as for TEXT.
 */
DATA(insert (  650 1043  730 a f ));
DATA(insert (  869 1043  730 a f ));
DATA(insert (	16 1043 2971 a f ));
DATA(insert (  142 1043    0 a b ));
DATA(insert ( 1043	142 2896 e f ));

/*
 * Cross-category casts to and from BPCHAR
 *
 * We support all the same casts as for TEXT.
 */
DATA(insert (  650 1042  730 a f ));
DATA(insert (  869 1042  730 a f ));
DATA(insert (	16 1042 2971 a f ));
DATA(insert (  142 1042    0 a b ));
DATA(insert ( 1042	142 2896 e f ));

/*
 * Length-coercion functions
 */
DATA(insert ( 1042 1042  668 i f ));
DATA(insert ( 1043 1043  669 i f ));
DATA(insert ( 1083 1083 1968 i f ));
DATA(insert ( 1114 1114 1961 i f ));
DATA(insert ( 1184 1184 1967 i f ));
DATA(insert ( 1186 1186 1200 i f ));
DATA(insert ( 1266 1266 1969 i f ));
DATA(insert ( 1560 1560 1685 i f ));
DATA(insert ( 1562 1562 1687 i f ));
DATA(insert ( 1700 1700 1703 i f ));
DATA(insert ( 9301 9301 9314 i f ));
DATA(insert ( 9351 9351 9364 i f ));


/* json to/from jsonb */
DATA(insert (  114 3802    0 a i ));
DATA(insert ( 3802	114    0 a i ));

/* cast nvarchar2 */
ORADATA(insert ( 8027   25    0 i b ));
ORADATA(insert (   25 8027    0 i b ));
ORADATA(insert ( 8027 1042    0 i b ));
ORADATA(insert ( 1042 8027    0 i b ));
ORADATA(insert ( 8027 1043    0 i b ));
ORADATA(insert ( 1043 8027    0 i b ));
ORADATA(insert ( 8027  700    0 i i ));
ORADATA(insert (  700 8027    0 i i ));
ORADATA(insert ( 8027  701    0 i i ));
ORADATA(insert (  701 8027    0 i i ));
ORADATA(insert ( 8027   23    0 i i ));
ORADATA(insert (   23 8027    0 i i ));
ORADATA(insert ( 8027   21    0 i i ));
ORADATA(insert (   21 8027    0 i i ));
ORADATA(insert ( 8027   20    0 i i ));
ORADATA(insert (   20 8027    0 i i ));
ORADATA(insert ( 8027 1700    0 i i ));
ORADATA(insert ( 1700 8027    0 i i ));
ORADATA(insert ( 8027 1186    0 i i ));
ORADATA(insert ( 1186 8027    0 i i ));
ORADATA(insert ( 8027 8027 8114 i f ));

/* cast varchar2 */
ORADATA(insert ( 8026   25    0 i b ));
ORADATA(insert (   25 8026    0 i b ));
ORADATA(insert ( 8026 1042    0 i b ));
ORADATA(insert ( 1042 8026    0 i b ));
ORADATA(insert ( 8026 1043    0 i b ));
ORADATA(insert ( 1043 8026    0 i b ));
ORADATA(insert ( 8026  700    0 i i ));
ORADATA(insert (  700 8026    0 i i ));
ORADATA(insert ( 8026  701    0 i i ));
ORADATA(insert (  701 8026    0 i i ));
ORADATA(insert ( 8026   23    0 i i ));
ORADATA(insert (   23 8026    0 i i ));
ORADATA(insert ( 8026   21    0 i i ));
ORADATA(insert (   21 8026    0 i i ));
ORADATA(insert ( 8026   20    0 i i ));
ORADATA(insert (   20 8026    0 i i ));
ORADATA(insert ( 8026 1700    0 i i ));
ORADATA(insert ( 1700 8026    0 i i ));
ORADATA(insert ( 8026 1186    0 i i ));
ORADATA(insert ( 1186 8026    0 i i ));
ORADATA(insert ( 8026 8026 8107 i f ));

/* cast to text */
ORADATA(insert ( 1186   25 8346 i f ));
ORADATA(insert ( 1114   25    0 i i ));
ORADATA(insert ( 1184   25    0 i i ));
ORADATA(insert ( 1082   25    0 i i ));

/* cast xmltype to xml */
ORADATA(insert ( 8024  142    0 i b ));
ORADATA(insert (  142 8024    0 a b ));

/* cast xmltype to text */
ORADATA(insert ( 8024   25   0 i b ));

/* cast raw to text */
ORADATA(insert ( 4831   4831  669  i f ));
ORADATA(insert ( 25     4831  4846 i f ));
ORADATA(insert ( 4831   25    4847 i f ));
ORADATA(insert ( 5504   25    4847 i f ));

/* cast long raw to raw */
ORADATA(insert ( 4831 5504  0 i b ));
ORADATA(insert ( 17 5504  0 i b ));

/* cast long to text */
ORADATA(insert ( 8186 25  5083 e f ));
ORADATA(insert ( 25 8186  0 i b ));

/* cast blob and clob */
DATA(insert ( 8762	17		0 i b ));
DATA(insert ( 8764	25		0 i b ));
DATA(insert ( 8764	1043	0 i b ));
ORADATA(insert ( 8764	8026    0 i b ));
ORADATA(insert ( 8291	25		0 i i ));
ORADATA(insert (   17	4831    0 a b ));
ORADATA(insert (   25	8291	0 a i ));

/* BEING LIGHTWEIGHT_ORA */
/* cherry-picked from v3.16.4.4 */
PGDATA(insert (   18 8764  946 i f ));
PGDATA(insert ( 8764   18  944 i f ));
PGDATA(insert ( 1082   25    0 a i ));
PGDATA(insert ( 23 1186 9648 i f ));
PGDATA(insert ( 21 1186 9750 i f ));
PGDATA(insert ( 700 1186 9751 i f ));
PGDATA(insert ( 701 1186 9752 i f ));
PGDATA(insert ( 1700 1186 9753 i f ));
PGDATA(insert (   20   25    0 a i ));
PGDATA(insert (   25   20    0 i i ));
PGDATA(insert ( 1700   25    0 a i ));
PGDATA(insert (   25 1700    0 i i ));
PGDATA(insert (   20 1043    0 a i ));
PGDATA(insert ( 1043   20    0 i i ));
PGDATA(insert ( 1700 1043    0 a i ));
PGDATA(insert ( 1043 1700    0 i i ));
PGDATA(insert ( 1184   25 8348 a f ));
PGDATA(insert (   25 1184    0 i i ));
PGDATA(insert ( 1043 1114 9287 i f ));
PGDATA(insert ( 1114   25    0 a i ));

/* added in v5.21.7.1 */
PGDATA(insert ( 23   25   0    z i ));
PGDATA(insert ( 25   23   0    x i ));
PGDATA(insert ( 25   1114 0    x i ));
PGDATA(insert ( 701  25   0    z i ));
PGDATA(insert ( 701  1043 0    z i ));
PGDATA(insert ( 1042 1700 0    x i ));
PGDATA(insert ( 1043 23   0    x i ));
PGDATA(insert ( 1043 701  0    x i ));
PGDATA(insert ( 1186 1042 0    z i ));
PGDATA(insert ( 1266 25   0    z i ));
PGDATA(insert ( 1700 1042 0    z i ));
/* END LIGHTWEIGHT_ORA */

/* cast to blob */
DATA(insert ( 17	8762	0 i b ));

/* cast to rowid */
ORADATA(insert ( 25  8022 8131 i f ));
ORADATA(insert ( 8026  8022 8131 i f ));
ORADATA(insert ( 8027  8022 8131 i f ));
ORADATA(insert ( 1043  8022 8131 i f ));


/* cast to urowid */
ORADATA(insert ( 25  8004 0 i b ));
ORADATA(insert ( 8026  8004 0 i b ));
ORADATA(insert ( 8027  8004 0 i b ));
ORADATA(insert ( 1043  8004 0 i b ));
ORADATA(insert ( 8022  8004 8130 i f ));
ORADATA(insert ( 8004  8004 9009 e f ));

/* cast from char */
ORADATA(insert ( 1042  8774 0 o i ));
ORADATA(insert ( 1042  8776 0 o i ));
ORADATA(insert ( 1042  8778 0 o i ));
ORADATA(insert ( 1042  8780 0 o i ));
ORADATA(insert ( 1042  1186 0 o i ));
ORADATA(insert ( 1042  1700 0 o i ));
ORADATA(insert ( 1042  8186 0 o i ));
ORADATA(insert ( 1042  4831 0 o i ));
ORADATA(insert ( 1042  8022 0 o i ));
ORADATA(insert ( 1042  8764 0 o i ));
ORADATA(insert ( 1042  8762 0 o i ));
ORADATA(insert ( 1042  8291 0 o i ));

/* cast from varchar2 */
ORADATA(insert ( 8026  8027 0 o i ));
ORADATA(insert ( 8026  8186 0 o i ));
ORADATA(insert ( 8026  4831 0 o i ));
ORADATA(insert ( 8026  8764 0 o i ));
ORADATA(insert ( 8026  8291 0 o i ));

/* cast from nvarchar2 */
ORADATA(insert ( 8027  8026 0 o i ));
ORADATA(insert ( 8027  8186 0 o i ));
ORADATA(insert ( 8027  4831 0 o i ));
ORADATA(insert ( 8027  8764 0 o i ));
ORADATA(insert ( 8027  8291 0 o i ));

/* cast from date */
ORADATA(insert ( 8774  1042 0 o i ));

/* cast from TIMESTAMP WITHOUT TIME ZONE */
ORADATA(insert ( 8776  1042 0 o i ));
ORADATA(insert ( 8776  8186 0 o i ));

/* cast from TIMESTAMP WITH LOCAL TIME ZONE */
ORADATA(insert ( 8778  1042 0 o i ));
ORADATA(insert ( 8778  8186 0 o i ));

/* cast from TIMESTAMP WITH TIME ZONE */
ORADATA(insert ( 8780  1042 0 o i ));
ORADATA(insert ( 8780  8186 0 o i ));

/* cast from INTERVAL */
ORADATA(insert ( 1186  1042 0 o i ));
ORADATA(insert ( 1186  8186 0 o i ));

/* cast from numeric (number, binary_float, binary_double) */
ORADATA(insert ( 1700  1042 0 o i ));
ORADATA(insert ( 1700  8186 0 o i ));

/* cast from long */
ORADATA(insert ( 8186  1042 0 o i ));
ORADATA(insert ( 8186  8026 0 o i ));
ORADATA(insert ( 8186  8027 0 o i ));
ORADATA(insert ( 8186  4831 0 o i ));
ORADATA(insert ( 8186  8764 0 o i ));
ORADATA(insert ( 8186  8291 0 o i ));

/* cast from raw */
ORADATA(insert ( 4831  1042 0 o i ));
ORADATA(insert ( 4831  8026 0 o i ));
ORADATA(insert ( 4831  8027 0 o i ));
ORADATA(insert ( 4831  8186 0 o i ));
ORADATA(insert ( 4831  8762 0 o i ));

/* cast from ROWID */
ORADATA(insert ( 8022  1042 0 o i ));
ORADATA(insert ( 8022  8026 0 o i ));
ORADATA(insert ( 8022  8027 0 o i ));

/* cast from CLOB */
ORADATA(insert ( 8764  1042 9008 o f ));
ORADATA(insert ( 8764  8027 0 o i ));
ORADATA(insert ( 8764  8186 0 o i ));
ORADATA(insert ( 8764  8291 0 o i ));

/* cast from BLOB */
ORADATA(insert ( 8762  4831 9122 o f ));
ORADATA(insert ( 8762  5504 0 o b ));

/* cast from NCLOB */
ORADATA(insert ( 8291  1042 0 o i ));
ORADATA(insert ( 8291  8026 0 o i ));
ORADATA(insert ( 8291  8027 0 o i ));
ORADATA(insert ( 8291  8186 0 o i ));
ORADATA(insert ( 8291  8764 0 o i ));

/* cast from JSON */
ORADATA(insert ( 114  8026 0 o i ));
ORADATA(insert ( 114  8762 0 o i ));
ORADATA(insert ( 114  8764 0 o i ));

/* jsonb to numeric and bool types */
DATA(insert ( 3802  16   6142 e f ));
DATA(insert ( 3802  1700 6143 e f ));
DATA(insert ( 3802  21   6144 e f ));
DATA(insert ( 3802  23   6145 e f ));
DATA(insert ( 3802  20   6146 e f ));
DATA(insert ( 3802  700  6147 e f ));
DATA(insert ( 3802  701  6148 e f ));

#endif							/* PG_CAST_H */
