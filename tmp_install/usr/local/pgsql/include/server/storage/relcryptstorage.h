/*-------------------------------------------------------------------------
 *
 * relcryptstorage.h

 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 *
 * src/include/storage/relcryptstorage.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELCRYPT_STORAGE_H
#define RELCRYPT_STORAGE_H

extern void rel_crypt_struct_init(RelCrypt relcrypt);
extern void rel_crypt_page_decrypt(RelCrypt relcrypt, Page page);
extern Page rel_crypt_page_encrypt(RelCrypt relcrypt, Page page);
extern bool rel_crypt_hash_lookup(RelFileNode * rnode, RelCrypt relcrypt_ret);

#endif                            /* RELCRYPT_STORAGE_H */
