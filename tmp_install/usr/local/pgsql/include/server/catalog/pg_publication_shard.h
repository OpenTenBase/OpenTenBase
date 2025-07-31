/*-------------------------------------------------------------------------
 *
 * pg_publication_shard.h
 *      definition of the publication to shard map (pg_publication_shard)
  *
 * Portions Copyright (c) 2018, Tencent OpenTenBase Group
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 *
 * src/include/catalog/pg_publication_shard.h
 *
 * NOTES
 *      the genbki.pl script reads this file and generates .bki
 *      information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PUBLICATION_SHARD_H
#define PG_PUBLICATION_SHARD_H

#include "catalog/genbki.h"

/* ----------------
 *        pg_publication_shard definition.  cpp turns this into
 *        typedef struct FormData_pg_publication_shard
 *
 * ----------------
 */
#define PublicationShardRelationId                9026

CATALOG(pg_publication_shard,9026)
{
    Oid            prpubid;        /* Oid of the publication */
    int32        prshardid;        /* Id of the shard */
} FormData_pg_publication_shard;

/* ----------------
 *        Form_pg_publication_shard corresponds to a pointer to a tuple with
 *        the format of pg_publication_shard relation.
 * ----------------
 */
typedef FormData_pg_publication_shard *Form_pg_publication_shard;

/* ----------------
 *        compiler constants for pg_publication_shard
 * ----------------
 */

#define Natts_pg_publication_shard                2
#define Anum_pg_publication_shard_prpubid        1
#define Anum_pg_publication_shard_prshardid        2

#endif                            /* PG_PUBLICATION_SHARD_H */

