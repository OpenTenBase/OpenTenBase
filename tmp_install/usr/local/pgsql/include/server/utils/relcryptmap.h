/*-------------------------------------------------------------------------
 *
 * relcryptmap.h
 *    relation file crypt map
 *
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 *
 * src/include/utils/relcryptmap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELCRYPT_MAP_H
#define RELCRYPT_MAP_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "storage/relfilenode.h"
#include "utils/relcrypt.h"

/* ----------------
 *      relcrypt-related XLOG entries
 * ----------------
 */

#define XLOG_CRYPT_KEY_INSERT               0x10
#define XLOG_CRYPT_KEY_DELETE               0x20
#define XLOG_REL_CRYPT_INSERT               0x40
#define XLOG_REL_CRYPT_DELETE               0x80

typedef struct xl_rel_crypt_insert
{
    RelFileNode rnode;
    int         algo_id;
} xl_rel_crypt_insert;

typedef xl_rel_crypt_insert xl_rel_crypt_delete;

extern void rel_crypt_redo(XLogReaderState *record);
extern void rel_crypt_desc(StringInfo buf, XLogReaderState *record);
extern const char * rel_crypt_identify(uint8 info);
extern void cyprt_key_info_hash_init(void);
extern void crypt_key_info_load_mapfile(void);
extern void rel_cyprt_hash_init(void);
extern void rel_crypt_load_mapfile(void);
extern void CheckPointRelCrypt(void);
extern void StartupReachConsistentState(void);
extern Size crypt_key_info_hash_shmem_size(void);
extern Size rel_crypt_hash_shmem_size(void);
extern bool crypt_key_info_hash_lookup(AlgoId algo_id, CryptKeyInfo * relcrypt_ret);
extern void cryt_ky_inf_fil_stru_for_deflt_alg(AlgoId algo_id, CryptKeyInfo cryptkey_out);
extern void crypt_key_info_load_default_key(void);
extern void crypt_key_info_free(CryptKeyInfo cryptkey);
extern CryptKeyInfo crypt_key_info_alloc(int option);
extern void rel_crypt_hash_insert(RelFileNode * rnode, AlgoId algo_id, bool write_wal, bool in_building_procedure);
extern void remove_rel_crypt_hash_elem(RelCrypt relCrypt, bool write_wal);
extern void rel_crypt_hash_delete(RelFileNode * rnode, bool write_wal);
extern void crypt_key_info_hash_insert(CryptKeyInfo cryptkey_input, bool write_wal, bool in_building_procedure);
extern int crypt_key_info_cal_key_size(CryptKeyInfo cryptkey);
extern bool CheckRelFileNodeExists(RelFileNode *rnode);
extern List* MarkRelCryptInvalid(void);
extern void rel_crypt_write_mapfile(bool is_backup);
#endif                          /* RELCRYPT_MAP_H */
