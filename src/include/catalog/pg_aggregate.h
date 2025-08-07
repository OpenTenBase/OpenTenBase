/*-------------------------------------------------------------------------
 *
 * pg_aggregate.h
 *	  definition of the system "aggregate" relation (pg_aggregate)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_aggregate.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_AGGREGATE_H
#define PG_AGGREGATE_H

#include "catalog/genbki.h"

/* ----------------------------------------------------------------
 *		pg_aggregate definition.
 *
 *		cpp turns this into typedef struct FormData_pg_aggregate
 *
 *	aggfnoid			pg_proc OID of the aggregate itself
 *	aggkind				aggregate kind, see AGGKIND_ categories below
 *	aggnumdirectargs	number of arguments that are "direct" arguments
 *	aggtransfn			transition function
 *	aggfinalfn			final function (0 if none)
 *	aggcombinefn		combine function (0 if none)
 *	aggserialfn			function to convert transtype to bytea (0 if none)
 *	aggdeserialfn		function to convert bytea to transtype (0 if none)
 *	aggmtransfn			forward function for moving-aggregate mode (0 if none)
 *	aggminvtransfn		inverse function for moving-aggregate mode (0 if none)
 *	aggmfinalfn			final function for moving-aggregate mode (0 if none)
 *	aggfinalextra		true to pass extra dummy arguments to aggfinalfn
 *	aggmfinalextra		true to pass extra dummy arguments to aggmfinalfn
 *	aggfinalmodify		tells whether aggfinalfn modifies transition state
 *	aggmfinalmodify		tells whether aggmfinalfn modifies transition state
 *	aggsortop			associated sort operator (0 if none)
 *	aggtranstype		type of aggregate's transition (state) data
 *	aggtransspace		estimated size of state data (0 for default estimate)
 *	aggmtranstype		type of moving-aggregate state data (0 if none)
 *	aggmtransspace		estimated size of moving-agg state (0 for default est)
 *	agginitval			initial value for transition state (can be NULL)
 *	aggminitval			initial value for moving-agg state (can be NULL)
 * ----------------------------------------------------------------
 */
#define AggregateRelationId  2600

CATALOG(pg_aggregate,2600) BKI_WITHOUT_OIDS
{
	regproc		aggfnoid;
	char		aggkind;
	int16		aggnumdirectargs;
	regproc		aggtransfn;
	regproc		aggfinalfn;
	regproc		aggcombinefn;
	regproc		aggserialfn;
	regproc		aggdeserialfn;
	regproc		aggmtransfn;
	regproc		aggminvtransfn;
	regproc		aggmfinalfn;
	bool		aggfinalextra;
	bool		aggmfinalextra;
	char		aggfinalmodify;
	char		aggmfinalmodify;
	Oid			aggsortop;
	Oid			aggtranstype;
	int32		aggtransspace;
	Oid			aggmtranstype;
	int32		aggmtransspace;

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		agginitval;
	text		aggminitval;
#endif
} FormData_pg_aggregate;

/* ----------------
 *		Form_pg_aggregate corresponds to a pointer to a tuple with
 *		the format of pg_aggregate relation.
 * ----------------
 */
typedef FormData_pg_aggregate *Form_pg_aggregate;

/* ----------------
 *		compiler constants for pg_aggregate
 * ----------------
 */

#define Natts_pg_aggregate					22
#define Anum_pg_aggregate_aggfnoid			1
#define Anum_pg_aggregate_aggkind			2
#define Anum_pg_aggregate_aggnumdirectargs	3
#define Anum_pg_aggregate_aggtransfn		4
#define Anum_pg_aggregate_aggfinalfn		5
#define Anum_pg_aggregate_aggcombinefn		6
#define Anum_pg_aggregate_aggserialfn		7
#define Anum_pg_aggregate_aggdeserialfn		8
#define Anum_pg_aggregate_aggmtransfn		9
#define Anum_pg_aggregate_aggminvtransfn	10
#define Anum_pg_aggregate_aggmfinalfn		11
#define Anum_pg_aggregate_aggfinalextra		12
#define Anum_pg_aggregate_aggmfinalextra	13
#define Anum_pg_aggregate_aggfinalmodify	14
#define Anum_pg_aggregate_aggmfinalmodify	15
#define Anum_pg_aggregate_aggsortop			16
#define Anum_pg_aggregate_aggtranstype		17
#define Anum_pg_aggregate_aggtransspace		18
#define Anum_pg_aggregate_aggmtranstype		19
#define Anum_pg_aggregate_aggmtransspace	20
#define Anum_pg_aggregate_agginitval		21
#define Anum_pg_aggregate_aggminitval		22

/*
 * Symbolic values for aggkind column.  We distinguish normal aggregates
 * from ordered-set aggregates (which have two sets of arguments, namely
 * direct and aggregated arguments) and from hypothetical-set aggregates
 * (which are a subclass of ordered-set aggregates in which the last
 * direct arguments have to match up in number and datatypes with the
 * aggregated arguments).
 */
#define AGGKIND_NORMAL			'n'
#define AGGKIND_ORDERED_SET		'o'
#define AGGKIND_HYPOTHETICAL	'h'

/* Use this macro to test for "ordered-set agg including hypothetical case" */
#define AGGKIND_IS_ORDERED_SET(kind)  ((kind) != AGGKIND_NORMAL)

/*
 * Symbolic values for aggfinalmodify and aggmfinalmodify columns.
 * Preferably, finalfns do not modify the transition state value at all,
 * but in some cases that would cost too much performance.  We distinguish
 * "pure read only" and "trashes it arbitrarily" cases, as well as the
 * intermediate case where multiple finalfn calls are allowed but the
 * transfn cannot be applied anymore after the first finalfn call.
 */
#define AGGMODIFY_READ_ONLY			'r'
#define AGGMODIFY_SHAREABLE			's'
#define AGGMODIFY_READ_WRITE		'w'

/*
 * Item Order of ODCIAggregateInterface and ODCIFunctionNames must
 * match each other.
 */
typedef enum
{
	ODCI_AGGREGATE_INITIALIZE = 0,
	ODCI_AGGREGATE_ITERATE,
	ODCI_AGGREGATE_MERGE,
	ODCI_AGGREGATE_TERMINATE,

	/* Keep it at the bottom */
	NUM_OF_ODCI_FUNCTIONS
} ODCIAggregateInterface;

extern const char* ODCIFunctionNames[];

#define INITVAL_MAGIC_HEADER "_opentenbase_"


/* ----------------
 * initial contents of pg_aggregate
 * ---------------
 */

/* avg */
DATA(insert ( 2100	n 0 int8_avg_accum		numeric_poly_avg	int8_avg_combine	int8_avg_serialize		int8_avg_deserialize	int8_avg_accum	int8_avg_accum_inv	numeric_poly_avg	f f r r 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2101	n 0 int4_avg_accum		int8_avg			int4_avg_combine	-						-						int4_avg_accum	int4_avg_accum_inv	int8_avg			f f r r 0	1016	0	1016	0	"{0,0}" "{0,0}" ));
DATA(insert ( 2102	n 0 int2_avg_accum		int8_avg			int4_avg_combine	-						-						int2_avg_accum	int2_avg_accum_inv	int8_avg			f f r r 0	1016	0	1016	0	"{0,0}" "{0,0}" ));
DATA(insert ( 2103	n 0 numeric_avg_accum	numeric_avg			numeric_avg_combine numeric_avg_serialize	numeric_avg_deserialize numeric_avg_accum numeric_accum_inv numeric_avg			f f r r 0	2281	128 2281	128 _null_ _null_ ));
DATA(insert ( 2104	n 0 float4_accum		float8_avg			float8_combine		-						-						-				-				-						f f r r 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2105	n 0 float8_accum		float8_avg			float8_combine		-						-						-				-				-						f f r r 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2106	n 0 interval_accum		interval_avg		interval_combine	-						-						interval_accum	interval_accum_inv	interval_avg		f f r r 0	1187	0	1187	0	"{0 second,0 second}" "{0 second,0 second}" ));
DATA(insert ( 9305	n 0 numericd_avg_accum	numericd_avg		numericd_avg_combine -						-						-				-				-						f f r r 0	9302	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 9355	n 0 numericf_avg_accum	numericd_avg		numericd_avg_combine -						-						-				-				-						f f r r 0	9302	0	0		0	"{0,0,0}" _null_ ));

ORADATA(insert ( 6061  n 0 text_nu_avg_accum   numeric_avg	numeric_avg_combine	numeric_avg_serialize	numeric_avg_deserialize	text_nu_avg_accum	text_nu_accum_inv	numeric_avg	f f r r 0   2281    128 2281    128 _null_ _null_ ));
ORADATA(insert ( 6062  n 0 text_nu_avg_accum   numeric_avg	numeric_avg_combine	numeric_avg_serialize	numeric_avg_deserialize	text_nu_avg_accum	text_nu_accum_inv	numeric_avg	f f r r 0	2281	128 2281	128 _null_ _null_ ));

/* sum */
DATA(insert ( 2107	n 0 int8_avg_accum		numeric_poly_sum	int8_avg_combine	int8_avg_serialize		int8_avg_deserialize	int8_avg_accum	int8_avg_accum_inv	numeric_poly_sum	f f r r 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2108	n 0 int4_sum			-					int8pl				-						-						int4_avg_accum	int4_avg_accum_inv	int2int4_sum		f f r r 0	20		0	1016	0	_null_ "{0,0}" ));
DATA(insert ( 2109	n 0 int2_sum			-					int8pl				-						-						int2_avg_accum	int2_avg_accum_inv	int2int4_sum		f f r r 0	20		0	1016	0	_null_ "{0,0}" ));
DATA(insert ( 2110	n 0 float4pl			-					float4pl			-						-						-				-					-					f f r r 0	700		0	0		0	_null_ _null_ ));
DATA(insert ( 2111	n 0 float8pl			-					float8pl			-						-						-				-					-					f f r r 0	701		0	0		0	_null_ _null_ ));
DATA(insert ( 2112	n 0 cash_pl				-					cash_pl				-						-						cash_pl			cash_mi				-					f f r r 0	790		0	790		0	_null_ _null_ ));
DATA(insert ( 2113	n 0 interval_pl			-					interval_pl			-						-						interval_pl		interval_mi			-					f f r r 0	1186	0	1186	0	_null_ _null_ ));
DATA(insert ( 2114	n 0 numeric_avg_accum	numeric_sum			numeric_avg_combine numeric_avg_serialize	numeric_avg_deserialize numeric_avg_accum numeric_accum_inv numeric_sum			f f r r 0	2281	128 2281	128 _null_ _null_ ));
DATA(insert ( 9306	n 0 numericd_pl			-					numericd_pl			-						-						-				-					-					f f r r 0	9301	0	0		0	_null_ _null_ ));
DATA(insert ( 9356	n 0 numericf_pl			-					numericf_pl			-						-						-				-					-					f f r r 0	9351	0	0		0	_null_ _null_ ));

/* max */
DATA(insert ( 2115	n 0 int8larger		-				int8larger			-	-	-				-				-				f f r r 413		20		0	0		0	_null_ _null_ ));
DATA(insert ( 2116	n 0 int4larger		-				int4larger			-	-	-				-				-				f f r r 521		23		0	0		0	_null_ _null_ ));
DATA(insert ( 2117	n 0 int2larger		-				int2larger			-	-	-				-				-				f f r r 520		21		0	0		0	_null_ _null_ ));
DATA(insert ( 2118	n 0 oidlarger		-				oidlarger			-	-	-				-				-				f f r r 610		26		0	0		0	_null_ _null_ ));
DATA(insert ( 2119	n 0 float4larger	-				float4larger		-	-	-				-				-				f f r r 623		700		0	0		0	_null_ _null_ ));
DATA(insert ( 2120	n 0 float8larger	-				float8larger		-	-	-				-				-				f f r r 674		701		0	0		0	_null_ _null_ ));
DATA(insert ( 2121	n 0 int4larger		-				int4larger			-	-	-				-				-				f f r r 563		702		0	0		0	_null_ _null_ ));
DATA(insert ( 2122	n 0 date_larger		-				date_larger			-	-	-				-				-				f f r r 1097	1082	0	0		0	_null_ _null_ ));
DATA(insert ( 2123	n 0 time_larger		-				time_larger			-	-	-				-				-				f f r r 1112	1083	0	0		0	_null_ _null_ ));
DATA(insert ( 2124	n 0 timetz_larger	-				timetz_larger		-	-	-				-				-				f f r r 1554	1266	0	0		0	_null_ _null_ ));
DATA(insert ( 2125	n 0 cashlarger		-				cashlarger			-	-	-				-				-				f f r r 903		790		0	0		0	_null_ _null_ ));
DATA(insert ( 2126	n 0 timestamp_larger	-			timestamp_larger	-	-	-				-				-				f f r r 2064	1114	0	0		0	_null_ _null_ ));
DATA(insert ( 2127	n 0 timestamptz_larger	-			timestamptz_larger	-	-	-				-				-				f f r r 1324	1184	0	0		0	_null_ _null_ ));
DATA(insert ( 2128	n 0 interval_larger -				interval_larger		-	-	-				-				-				f f r r 1334	1186	0	0		0	_null_ _null_ ));
DATA(insert ( 2129	n 0 text_larger		-				text_larger			-	-	-				-				-				f f r r 666		25		0	0		0	_null_ _null_ ));
ORADATA(insert ( 5073	n 0 raw_larger		-			raw_larger			-	-	-				-				-				f f r r 4837	4831		0	0		0	_null_ _null_ ));
DATA(insert ( 2130	n 0 numeric_larger	-				numeric_larger		-	-	-				-				-				f f r r 1756	1700	0	0		0	_null_ _null_ ));
DATA(insert ( 2050	n 0 array_larger	-				array_larger		-	-	-				-				-				f f r r 1073	2277	0	0		0	_null_ _null_ ));
DATA(insert ( 2244	n 0 bpchar_larger	-				bpchar_larger		-	-	-				-				-				f f r r 1060	1042	0	0		0	_null_ _null_ ));
DATA(insert ( 2797	n 0 tidlarger		-				tidlarger			-	-	-				-				-				f f r r 2800	27		0	0		0	_null_ _null_ ));
DATA(insert ( 3526	n 0 enum_larger		-				enum_larger			-	-	-				-				-				f f r r 3519	3500	0	0		0	_null_ _null_ ));
DATA(insert ( 3564	n 0 network_larger	-				network_larger		-	-	-				-				-				f f r r 1205	869		0	0		0	_null_ _null_ ));
DATA(insert ( 9307	n 0 numericd_larger	-				numericd_larger		-	-	-				-				-				f f r r 9404	9301	0	0		0	_null_ _null_ ));
DATA(insert ( 9357	n 0 numericf_larger	-				numericf_larger		-	-	-				-				-				f f r r 9454	9351	0	0		0	_null_ _null_ ));

/* min */
DATA(insert ( 2131	n 0 int8smaller		-				int8smaller			-	-	-				-				-				f f r r 412		20		0	0		0	_null_ _null_ ));
DATA(insert ( 2132	n 0 int4smaller		-				int4smaller			-	-	-				-				-				f f r r 97		23		0	0		0	_null_ _null_ ));
DATA(insert ( 2133	n 0 int2smaller		-				int2smaller			-	-	-				-				-				f f r r 95		21		0	0		0	_null_ _null_ ));
DATA(insert ( 2134	n 0 oidsmaller		-				oidsmaller			-	-	-				-				-				f f r r 609		26		0	0		0	_null_ _null_ ));
DATA(insert ( 2135	n 0 float4smaller	-				float4smaller		-	-	-				-				-				f f r r 622		700		0	0		0	_null_ _null_ ));
DATA(insert ( 2136	n 0 float8smaller	-				float8smaller		-	-	-				-				-				f f r r 672		701		0	0		0	_null_ _null_ ));
DATA(insert ( 2137	n 0 int4smaller		-				int4smaller			-	-	-				-				-				f f r r 562		702		0	0		0	_null_ _null_ ));
DATA(insert ( 2138	n 0 date_smaller	-				date_smaller		-	-	-				-				-				f f r r 1095	1082	0	0		0	_null_ _null_ ));
DATA(insert ( 2139	n 0 time_smaller	-				time_smaller		-	-	-				-				-				f f r r 1110	1083	0	0		0	_null_ _null_ ));
DATA(insert ( 2140	n 0 timetz_smaller	-				timetz_smaller		-	-	-				-				-				f f r r 1552	1266	0	0		0	_null_ _null_ ));
DATA(insert ( 2141	n 0 cashsmaller		-				cashsmaller			-	-	-				-				-				f f r r 902		790		0	0		0	_null_ _null_ ));
DATA(insert ( 2142	n 0 timestamp_smaller	-			timestamp_smaller	-	-	-				-				-				f f r r 2062	1114	0	0		0	_null_ _null_ ));
DATA(insert ( 2143	n 0 timestamptz_smaller -			timestamptz_smaller -	-	-				-				-				f f r r 1322	1184	0	0		0	_null_ _null_ ));
DATA(insert ( 2144	n 0 interval_smaller	-			interval_smaller	-	-	-				-				-				f f r r 1332	1186	0	0		0	_null_ _null_ ));
DATA(insert ( 2145	n 0 text_smaller	-				text_smaller		-	-	-				-				-				f f r r 664		25		0	0		0	_null_ _null_ ));
ORADATA(insert ( 5074	n 0 raw_smaller	-				raw_smaller			-	-	-				-				-				f f r r 4835	4831	0	0		0	_null_ _null_ ));
DATA(insert ( 2146	n 0 numeric_smaller -				numeric_smaller		-	-	-				-				-				f f r r 1754	1700	0	0		0	_null_ _null_ ));
DATA(insert ( 2051	n 0 array_smaller	-				array_smaller		-	-	-				-				-				f f r r 1072	2277	0	0		0	_null_ _null_ ));
DATA(insert ( 2245	n 0 bpchar_smaller	-				bpchar_smaller		-	-	-				-				-				f f r r 1058	1042	0	0		0	_null_ _null_ ));
DATA(insert ( 2798	n 0 tidsmaller		-				tidsmaller			-	-	-				-				-				f f r r 2799	27		0	0		0	_null_ _null_ ));
DATA(insert ( 3527	n 0 enum_smaller	-				enum_smaller		-	-	-				-				-				f f r r 3518	3500	0	0		0	_null_ _null_ ));
DATA(insert ( 3565	n 0 network_smaller -				network_smaller		-	-	-				-				-				f f r r 1203	869		0	0		0	_null_ _null_ ));
DATA(insert ( 9308	n 0 numericd_smaller	-			numericd_smaller	-	-	-				-				-				f f r r 9402	9301	0	0		0	_null_ _null_ ));
DATA(insert ( 9358	n 0 numericf_smaller	-			numericf_smaller	-	-	-				-				-				f f r r 9452	9351	0	0		0	_null_ _null_ ));

/* count */
PGDATA(insert ( 2147	n 0 int8inc_any		-				int8pl	-	-	int8inc_any		int8dec_any		-				f f r r 0		20		0	20		0	"0" "0" ));
PGDATA(insert ( 2803	n 0 int8inc			-				int8pl	-	-	int8inc			int8dec			-				f f r r 0		20		0	20		0	"0" "0" ));
ORADATA(insert ( 2698	n 0 int8inc_any		numeric_fn				int8pl	-	-	int8inc_any		int8dec_any		numeric_fn			f f r r 0		20	0	20	0	"0" "0" ));
ORADATA(insert ( 3409	n 0 int8inc			numeric_fn				int8pl	-	-	int8inc		int8dec		numeric_fn			f f r r 0		20	0	20	0	"0" "0" ));

/* var_pop */
DATA(insert ( 2718	n 0 int8_accum		numeric_var_pop			numeric_combine			numeric_serialize		numeric_deserialize			int8_accum		int8_accum_inv	numeric_var_pop			f f r r 0	2281	128 2281	128 _null_ _null_ ));
DATA(insert ( 2719	n 0 int4_accum		numeric_poly_var_pop	numeric_poly_combine	numeric_poly_serialize	numeric_poly_deserialize	int4_accum		int4_accum_inv	numeric_poly_var_pop	f f r r 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2720	n 0 int2_accum		numeric_poly_var_pop	numeric_poly_combine	numeric_poly_serialize	numeric_poly_deserialize	int2_accum		int2_accum_inv	numeric_poly_var_pop	f f r r 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2721	n 0 float4_accum	float8_var_pop			float8_combine			-						-							-				-				-						f f r r 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2722	n 0 float8_accum	float8_var_pop			float8_combine			-						-							-				-				-						f f r r 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2723	n 0 numeric_accum	numeric_var_pop			numeric_combine			numeric_serialize		numeric_deserialize			numeric_accum	numeric_accum_inv numeric_var_pop		f f r r 0	2281	128 2281	128 _null_ _null_ ));

/* var_samp */
DATA(insert ( 2641	n 0 int8_accum		numeric_var_samp		numeric_combine			numeric_serialize		numeric_deserialize			int8_accum		int8_accum_inv	numeric_var_samp		f f r r 0	2281	128 2281	128 _null_ _null_ ));
DATA(insert ( 2642	n 0 int4_accum		numeric_poly_var_samp	numeric_poly_combine	numeric_poly_serialize	numeric_poly_deserialize	int4_accum		int4_accum_inv	numeric_poly_var_samp	f f r r 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2643	n 0 int2_accum		numeric_poly_var_samp	numeric_poly_combine	numeric_poly_serialize	numeric_poly_deserialize	int2_accum		int2_accum_inv	numeric_poly_var_samp	f f r r 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2644	n 0 float4_accum	float8_var_samp			float8_combine			-						-							-				-				-						f f r r 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2645	n 0 float8_accum	float8_var_samp			float8_combine			-						-							-				-				-						f f r r 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2646	n 0 numeric_accum	numeric_var_samp		numeric_combine			numeric_serialize		numeric_deserialize			numeric_accum	numeric_accum_inv numeric_var_samp		f f r r 0	2281	128 2281	128 _null_ _null_ ));

/* variance: historical Postgres syntax for var_samp */
DATA(insert ( 2148	n 0 int8_accum		numeric_var_samp		numeric_combine			numeric_serialize		numeric_deserialize			int8_accum		int8_accum_inv	numeric_var_samp		f f r r 0	2281	128 2281	128 _null_ _null_ ));
DATA(insert ( 2149	n 0 int4_accum		numeric_poly_var_samp	numeric_poly_combine	numeric_poly_serialize	numeric_poly_deserialize	int4_accum		int4_accum_inv	numeric_poly_var_samp	f f r r 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2150	n 0 int2_accum		numeric_poly_var_samp	numeric_poly_combine	numeric_poly_serialize	numeric_poly_deserialize	int2_accum		int2_accum_inv	numeric_poly_var_samp	f f r r 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2151	n 0 float4_accum	float8_var_samp			float8_combine			-						-							-				-				-						f f r r 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2152	n 0 float8_accum	float8_var_samp			float8_combine			-						-							-				-				-						f f r r 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2153	n 0 numeric_accum	numeric_var_samp		numeric_combine			numeric_serialize		numeric_deserialize			numeric_accum	numeric_accum_inv numeric_var_samp		f f r r 0	2281	128 2281	128 _null_ _null_ ));

/* stddev_pop */
DATA(insert ( 2724	n 0 int8_accum		numeric_stddev_pop		numeric_combine			numeric_serialize		numeric_deserialize			int8_accum		int8_accum_inv	numeric_stddev_pop		f f r r 0	2281	128 2281	128 _null_ _null_ ));
DATA(insert ( 2725	n 0 int4_accum		numeric_poly_stddev_pop numeric_poly_combine	numeric_poly_serialize	numeric_poly_deserialize	int4_accum		int4_accum_inv	numeric_poly_stddev_pop f f r r 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2726	n 0 int2_accum		numeric_poly_stddev_pop numeric_poly_combine	numeric_poly_serialize	numeric_poly_deserialize	int2_accum		int2_accum_inv	numeric_poly_stddev_pop f f r r 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2727	n 0 float4_accum	float8_stddev_pop		float8_combine			-						-							-				-				-						f f r r 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2728	n 0 float8_accum	float8_stddev_pop		float8_combine			-						-							-				-				-						f f r r 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2729	n 0 numeric_accum	numeric_stddev_pop		numeric_combine			numeric_serialize		numeric_deserialize			numeric_accum	numeric_accum_inv numeric_stddev_pop	f f r r 0	2281	128 2281	128 _null_ _null_ ));

/* stddev_samp */
DATA(insert ( 2712	n 0 int8_accum		numeric_stddev_samp			numeric_combine			numeric_serialize		numeric_deserialize			int8_accum	int8_accum_inv	numeric_stddev_samp			f f r r 0	2281	128 2281	128 _null_ _null_ ));
DATA(insert ( 2713	n 0 int4_accum		numeric_poly_stddev_samp	numeric_poly_combine	numeric_poly_serialize	numeric_poly_deserialize	int4_accum	int4_accum_inv	numeric_poly_stddev_samp	f f r r 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2714	n 0 int2_accum		numeric_poly_stddev_samp	numeric_poly_combine	numeric_poly_serialize	numeric_poly_deserialize	int2_accum	int2_accum_inv	numeric_poly_stddev_samp	f f r r 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2715	n 0 float4_accum	float8_stddev_samp			float8_combine			-						-							-			-				-							f f r r 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2716	n 0 float8_accum	float8_stddev_samp			float8_combine			-						-							-			-				-							f f r r 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2717	n 0 numeric_accum	numeric_stddev_samp			numeric_combine			numeric_serialize		numeric_deserialize			numeric_accum numeric_accum_inv numeric_stddev_samp		f f r r 0	2281	128 2281	128 _null_ _null_ ));

/* stddev: historical Postgres syntax for stddev_samp */
DATA(insert ( 2154	n 0 int8_accum		numeric_stddev_samp			numeric_combine			numeric_serialize		numeric_deserialize			int8_accum		int8_accum_inv	numeric_stddev_samp			f f r r 0	2281	128 2281	128 _null_ _null_ ));
DATA(insert ( 2155	n 0 int4_accum		numeric_poly_stddev_samp	numeric_poly_combine	numeric_poly_serialize	numeric_poly_deserialize	int4_accum		int4_accum_inv	numeric_poly_stddev_samp	f f r r 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2156	n 0 int2_accum		numeric_poly_stddev_samp	numeric_poly_combine	numeric_poly_serialize	numeric_poly_deserialize	int2_accum		int2_accum_inv	numeric_poly_stddev_samp	f f r r 0	2281	48	2281	48	_null_ _null_ ));
DATA(insert ( 2157	n 0 float4_accum	float8_stddev_samp			float8_combine			-						-							-				-				-							f f r r 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2158	n 0 float8_accum	float8_stddev_samp			float8_combine			-						-							-				-				-							f f r r 0	1022	0	0		0	"{0,0,0}" _null_ ));
DATA(insert ( 2159	n 0 numeric_accum	numeric_stddev_samp			numeric_combine			numeric_serialize		numeric_deserialize			numeric_accum	numeric_accum_inv numeric_stddev_samp		f f r r 0	2281	128 2281	128 _null_ _null_ ));

/* SQL2003 binary regression aggregates */
DATA(insert ( 2818	n 0 int8inc_float8_float8	-					int8pl				-	-	-				-				-			f f r r 0	20		0	0		0	"0" _null_ ));
DATA(insert ( 2819	n 0 float8_regr_accum	float8_regr_sxx			float8_regr_combine -	-	-				-				-			f f r r 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));
DATA(insert ( 2820	n 0 float8_regr_accum	float8_regr_syy			float8_regr_combine -	-	-				-				-			f f r r 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));
DATA(insert ( 2821	n 0 float8_regr_accum	float8_regr_sxy			float8_regr_combine -	-	-				-				-			f f r r 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));
DATA(insert ( 2822	n 0 float8_regr_accum	float8_regr_avgx		float8_regr_combine -	-	-				-				-			f f r r 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));
DATA(insert ( 2823	n 0 float8_regr_accum	float8_regr_avgy		float8_regr_combine -	-	-				-				-			f f r r 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));
DATA(insert ( 2824	n 0 float8_regr_accum	float8_regr_r2			float8_regr_combine -	-	-				-				-			f f r r 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));
DATA(insert ( 2825	n 0 float8_regr_accum	float8_regr_slope		float8_regr_combine -	-	-				-				-			f f r r 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));
DATA(insert ( 2826	n 0 float8_regr_accum	float8_regr_intercept	float8_regr_combine -	-	-				-				-			f f r r 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));
DATA(insert ( 2827	n 0 float8_regr_accum	float8_covar_pop		float8_regr_combine -	-	-				-				-			f f r r 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));
DATA(insert ( 2828	n 0 float8_regr_accum	float8_covar_samp		float8_regr_combine -	-	-				-				-			f f r r 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));
DATA(insert ( 2829	n 0 float8_regr_accum	float8_corr				float8_regr_combine -	-	-				-				-			f f r r 0	1022	0	0		0	"{0,0,0,0,0,0}" _null_ ));

/* boolean-and and boolean-or */
DATA(insert ( 2517	n 0 booland_statefunc	-	booland_statefunc	-	-	bool_accum	bool_accum_inv	bool_alltrue	f f r r 58	16	0	2281	16	_null_ _null_ ));
DATA(insert ( 2518	n 0 boolor_statefunc	-	boolor_statefunc	-	-	bool_accum	bool_accum_inv	bool_anytrue	f f r r 59	16	0	2281	16	_null_ _null_ ));
DATA(insert ( 2519	n 0 booland_statefunc	-	booland_statefunc	-	-	bool_accum	bool_accum_inv	bool_alltrue	f f r r 58	16	0	2281	16	_null_ _null_ ));

/* bitwise integer */
DATA(insert ( 2236	n 0 int2and		-				int2and -	-	-				-				-				f f r r 0	21		0	0		0	_null_ _null_ ));
DATA(insert ( 2237	n 0 int2or		-				int2or	-	-	-				-				-				f f r r 0	21		0	0		0	_null_ _null_ ));
DATA(insert ( 2238	n 0 int4and		-				int4and -	-	-				-				-				f f r r 0	23		0	0		0	_null_ _null_ ));
DATA(insert ( 2239	n 0 int4or		-				int4or	-	-	-				-				-				f f r r 0	23		0	0		0	_null_ _null_ ));
DATA(insert ( 2240	n 0 int8and		-				int8and -	-	-				-				-				f f r r 0	20		0	0		0	_null_ _null_ ));
DATA(insert ( 2241	n 0 int8or		-				int8or	-	-	-				-				-				f f r r 0	20		0	0		0	_null_ _null_ ));
DATA(insert ( 2242	n 0 bitand		-				bitand	-	-	-				-				-				f f r r 0	1560	0	0		0	_null_ _null_ ));
DATA(insert ( 2243	n 0 bitor		-				bitor	-	-	-				-				-				f f r r 0	1560	0	0		0	_null_ _null_ ));

/* xml */
DATA(insert ( 2901	n 0 xmlconcat2	-				-		-	-	-				-				-				f f r r 0	142		0	0		0	_null_ _null_ ));

/* array */
DATA(insert ( 2335	n 0 array_agg_transfn		array_agg_finalfn		-	-	-	-		-				-				t f r r 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 4053	n 0 array_agg_array_transfn array_agg_array_finalfn -	-	-	-		-				-				t f r r 0	2281	0	0		0	_null_ _null_ ));

/* text */
DATA(insert ( 3538	n 0 string_agg_transfn	string_agg_finalfn	-	-	-	-				-				-				f f r r 0	2281	0	0		0	_null_ _null_ ));

/* bytea */
DATA(insert ( 3545	n 0 bytea_string_agg_transfn	bytea_string_agg_finalfn	-	-	-	-				-				-		f f r r 0	2281	0	0		0	_null_ _null_ ));

/* BEGIN LIGHTWEIGHT_ORA */
/* text */
PGDATA(insert ( 9649	n 0 integer_string_agg_transfn	string_agg_finalfn	-	-	-	-				-				-				f f r r 0	2281	0	0		0	_null_ _null_ ));
/* sum (text) */
PGDATA(insert ( 9640	n 0 text_nu_avg_accum 		numeric_sum numeric_avg_combine numeric_avg_serialize numeric_avg_deserialize text_nu_avg_accum text_nu_accum_inv numeric_sum		f f r r 0	2281	128 2281	128 _null_ _null_ ));
/* END LIGHTWEIGHT_ORA */
/* json */
DATA(insert ( 3175	n 0 json_agg_transfn	json_agg_finalfn			-	-	-	-				-				-				f f r r 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3197	n 0 json_object_agg_transfn json_object_agg_finalfn -	-	-	-				-				-				f f r r 0	2281	0	0		0	_null_ _null_ ));

/* jsonb */
DATA(insert ( 3267	n 0 jsonb_agg_transfn	jsonb_agg_finalfn				-	-	-	-				-				-			f f r r 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3270	n 0 jsonb_object_agg_transfn jsonb_object_agg_finalfn	-	-	-	-				-				-			f f r r 0	2281	0	0		0	_null_ _null_ ));

/* ordered-set and hypothetical-set aggregates */
DATA(insert ( 3972	o 1 ordered_set_transition			percentile_disc_final					-	-	-	-		-		-		t f s s 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3974	o 1 ordered_set_transition			percentile_cont_float8_final			-	-	-	-		-		-		f f s s 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3976	o 1 ordered_set_transition			percentile_cont_interval_final			-	-	-	-		-		-		f f s s 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3978	o 1 ordered_set_transition			percentile_disc_multi_final				-	-	-	-		-		-		t f s s 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3980	o 1 ordered_set_transition			percentile_cont_float8_multi_final		-	-	-	-		-		-		f f s s 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3982	o 1 ordered_set_transition			percentile_cont_interval_multi_final	-	-	-	-		-		-		f f s s 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3984	o 0 ordered_set_transition			mode_final								-	-	-	-		-		-		t f s s 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3986	h 1 ordered_set_transition_multi	rank_final								-	-	-	-		-		-		t f w w 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3988	h 1 ordered_set_transition_multi	percent_rank_final						-	-	-	-		-		-		t f w w 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3990	h 1 ordered_set_transition_multi	cume_dist_final							-	-	-	-		-		-		t f w w 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 3992	h 1 ordered_set_transition_multi	dense_rank_final						-	-	-	-		-		-		t f w w 0	2281	0	0		0	_null_ _null_ ));

#ifdef _PG_ORCL_
ORADATA(insert ( 8268	n 0 text_nu_avg_accum 		text_nu_sum text_nu_avg_combine text_nu_avg_serialize text_nu_avg_deserialize text_nu_avg_accum text_nu_accum_inv text_nu_sum		f f r r 0	2281	128 2281	128 _null_ _null_ ));
ORADATA(insert ( 8269	n 0 varchar2_nu_avg_accum	text_nu_sum text_nu_avg_combine text_nu_avg_serialize text_nu_avg_deserialize text_nu_avg_accum varchar2_nu_accum_inv text_nu_sum	f f r r 0	2281	128 2281	128 _null_ _null_ ));

ORADATA(insert ( 8213	n 0 rowid_larger	-				rowid_larger		-	-	-				-				-				f f r r 8153	8022    0	0		0	_null_ _null_ ));
ORADATA(insert ( 8214	n 0 rowid_smaller	-				rowid_smaller		-	-	-				-				-				f f r r 8152	8022    0	0		0	_null_ _null_ ));

/* listagg urowid */
ORADATA(insert ( 8410	n 0 urowid_larger	-				urowid_larger		-	-	-				-				-				f f r r 8190	8004    0	0		0	_null_ _null_ ));
ORADATA(insert ( 8411	n 0 urowid_smaller	-				urowid_smaller		-	-	-				-				-				f f r r 8189	8004    0	0		0	_null_ _null_ ));

/* listagg text */
DATA(insert ( 8234	n 0 list_agg_p1_transfn		list_agg_finalfn	-	-	-	-				-				-				f f r r 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 8235	n 0 list_agg_p2i_transfn	list_agg_finalfn	-	-	-	-				-				-				f f r r 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 8236	n 0 list_agg_p2u_transfn	list_agg_finalfn	-	-	-	-				-				-				f f r r 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 8237	n 0 list_agg_p3_transfn		list_agg_finalfn	-	-	-	-				-				-				f f r r 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 8238	n 0 list_agg_p5_transfn		list_agg_finalfn	-	-	-	-				-				-				f f r r 0	2281	0	0		0	_null_ _null_ ));
/* listagg integer */
DATA(insert ( 8239	n 0 list_agg_i_p1_transfn	list_agg_finalfn	-	-	-	-				-				-				f f r r 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 8240	n 0 list_agg_i_p2i_transfn	list_agg_finalfn	-	-	-	-				-				-				f f r r 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 8241	n 0 list_agg_i_p2u_transfn	list_agg_finalfn	-	-	-	-				-				-				f f r r 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 8242	n 0 list_agg_i_p3_transfn	list_agg_finalfn	-	-	-	-				-				-				f f r r 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 8243	n 0 list_agg_i_p5_transfn	list_agg_finalfn	-	-	-	-				-				-				f f r r 0	2281	0	0		0	_null_ _null_ ));
/* listagg numeric */
DATA(insert ( 8249	n 0 list_agg_numeric_p1_transfn		list_agg_finalfn	-	-	-	-				-				-				f f r r 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 8250	n 0 list_agg_numeric_p2i_transfn	list_agg_finalfn	-	-	-	-				-				-				f f r r 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 8251	n 0 list_agg_numeric_p2u_transfn	list_agg_finalfn	-	-	-	-				-				-				f f r r 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 8252	n 0 list_agg_numeric_p3_transfn		list_agg_finalfn	-	-	-	-				-				-				f f r r 0	2281	0	0		0	_null_ _null_ ));
DATA(insert ( 8253	n 0 list_agg_numeric_p5_transfn		list_agg_finalfn	-	-	-	-				-				-				f f r r 0	2281	0	0		0	_null_ _null_ ));

#endif

#ifdef __OPENTENBASE_C__
DATA(insert ( 8099	n 0 distinct_accum  distinct_final  distinct_combine  distinct_serialize  distinct_deserialize	-	-	-	f f r r 0	2281	0	0	0	_null_ _null_ ));
#define DISTINCT_FUNC_OID 8099
#endif

#endif							/* PG_AGGREGATE_H */
