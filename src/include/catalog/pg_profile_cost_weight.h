/*-------------------------------------------------------------------------
 *
 * pg_profile_cost_weight.h
 *	  definition of the system "cost weight" relation (pg_profile_cost_weight)
 *	  along with the relation's initial contents.
 *
 *
 * src/include/catalog/pg_profile_cost_weight.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PROFILE_COST_WEIGHT_H
#define PG_PROFILE_COST_WEIGHT_H

#include "catalog/genbki.h"

#define PgProfileCostWeightRelationId  9041

CATALOG(pg_profile_cost_weight,9041)   BKI_WITHOUT_OIDS BKI_SHARED_RELATION
{
	int32 cpuweight;
	int32 connectweight;
	int32 blksweight;
	int32 sgaweight;
} FormData_pg_profile_cost_weight;

typedef FormData_pg_profile_cost_weight *Form_pg_profile_cost_weight;

#define Natts_pg_profile_cost_weight			  4

#define Anum_pg_profile_weight_cpu  		      1
#define Anum_pg_profile_weight_connect		      2
#define Anum_pg_profile_weight_blks       		  3
#define Anum_pg_profile_weight_sga                4

#endif                   /* PG_PROFILE_COST_WEIGHT_H */
