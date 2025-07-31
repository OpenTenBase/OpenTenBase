/*-------------------------------------------------------------------------
 *
 * relcryptaccess.h
 *      POSTGRES tuple descriptor definitions.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/relcryptaccess.h
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELCRYPT_ACCESS_H
#define RELCRYPT_ACCESS_H

extern HeapTuple transparent_crypt_encrypt_columns(Relation relation, HeapTuple tup, MemoryContext memctx);
extern void trsprt_crypt_free_strut_in_tupdesc(TupleDesc tupledesc, int natts);
extern void transparent_crypt_copy_attrs(Form_pg_attribute * dst_attrs, Form_pg_attribute * src_attrs, int natts);
#endif                            /* RELCRYPT_ACCESS_H */
