/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#ifndef RELCRYPT_MISC_H
#define RELCRYPT_MISC_H
/*Function declaration*/
extern void trsprt_crypt_dcrpt_all_col_vale(ScanState *node, TupleTableSlot *slot, Oid relid);
extern bool trsprt_crypt_check_table_has_crypt(Oid relid, bool mix, bool * schema_bound);
extern bool trsprt_crypt_chk_tbl_col_has_crypt(Oid relid, int attnum);
extern text * encrypt_procedure(AlgoId algo_id, text * text_src, char * page_new_output);
extern text * decrypt_procedure(AlgoId algo_id, text * text_src, int context_length);
extern int rel_crypt_page_encrypting_parellel(int16 algo_id, char * page, char * buf_need_encrypt, char * page_new, CryptKeyInfo cryptkey, int workerid);
extern void rel_crypt_init(void);
extern Datum trsprt_crypt_decrypt_one_col_value(TranspCrypt*transp_crypt, Form_pg_attribute attr, Datum inputval);
extern bool trsprt_crypt_chk_tbl_has_col_crypt(Oid relid);

#endif /*RELCRYPT_MISC_H*/
