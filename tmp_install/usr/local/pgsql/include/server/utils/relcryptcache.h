/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#ifndef RELCRYPT_CACHE_H
#define RELCRYPT_CACHE_H
/*related function declaration*/
extern void trsprt_crypt_asign_rela_tuldsc_fld(Relation relation);
extern void crypt_key_info_load_by_tuple(HeapTuple heaptuple, TupleDesc tupledesc);
extern void rel_crypt_create_direct(Oid relid, int16 algo_id);
extern void rel_crypt_relation_check_policy(Oid relid);
extern void rel_crypt_index_check_policy(Oid heap_oid, Relation indexrel);
extern void rel_crypt_relat_chk_plcy_wth_rnode(HeapTuple tuple, Oid databaseid);
#endif /*RELCRYPT_CACHE_H*/
