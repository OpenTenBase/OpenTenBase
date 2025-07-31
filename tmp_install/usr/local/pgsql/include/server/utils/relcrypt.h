/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#ifndef RELCRYPT_H
#define RELCRYPT_H

#include "storage/bufmgr.h"
#include "contrib/sm/sm4.h"

typedef int16 AlgoId;


extern bool g_enable_cls;
extern bool g_enable_transparent_crypt;
extern bool g_enable_crypt_debug;
extern int g_rel_crypt_hash_size;

extern int g_checkpoint_crypt_worker;
extern int g_checkpoint_crypt_queue_length;


typedef enum
{
    CRYPT_KEY_INFO_OPTION_SYMKEY = 1,    /* kind of encrypted with a symmetric key */
    CRYPT_KEY_INFO_OPTION_ANYKEY = 2,    /* kind of encrypted with a public-key    */
    CRYPT_KEY_INFO_OPTION_UDF    = 3,    /* kind of UDF encrypted */
    CRYPT_KEY_INFO_OPTION_SM4    = 4,    /* kind of guomi, sm4 */
    CRYPT_KEY_INFO_OPTION_BUTT
}CRYPT_KEY_INFO_OPTION_ENUM;

/* crypt error code */
enum
{
    CRYPT_RET_SUCCESS       = 0,
    CRYPT_RET_OVERSIZE      = 1,
    CRYPT_RET_ASSERT_ERR    = 2,
    CRYPT_RET_BUTT
};

/* default invalid slot and workid for parellel crypt */
#define INVALID_SLOT_ID   -1
#define INVALID_WORKER_ID -1

#define CRYPT_KEY_INFO_MAX_OPT_ARGS_LEN 96
#define CRYPT_KEY_INFO_MAX_PASSWORD_LEN 72
#define CRYPT_KEY_INFO_MAX_PUBKEY_LEN   3000
#define CRYPT_KEY_INFO_MAX_PRIKEY_LEN   3000
#define CRYPT_KEY_INFO_PROC_SRC_LEN     128
#define CRYPT_KEY_INFO_PROC_BIN_LEN     128

#define INVALID_CONTEXT_LENGTH          0

/* if attnum of pg_transparent_crypt_policy_map is REL_FILE_CRYPT_ATTR_NUM, it means rel file crypt */
#define REL_FILE_CRYPT_ATTR_NUM             -32767
/* this algo_id is only used for the crypting storage of TransparentCryptPolicyAlgorithmId  */
#define CRYPT_DEFAULT_INNER_ALGO_ID         32767  

#define TRANSP_CRYPT_ATTRS_EXT_ENABLE(X)     \
    do{\
        if (g_enable_transparent_crypt)\
        {\
            (X)->use_attrs_ext = true;\
        }\
    }while(0)
#define TRANSP_CRYPT_ATTRS_EXT_DISABLE(X)    \
    do{\
        if (g_enable_transparent_crypt)\
        {\
            (X)->use_attrs_ext = false;\
        }\
    }while(0)
#define TRANSP_CRYPT_ATTRS_EXT_IS_ENABLED(X) (g_enable_transparent_crypt && (true == ((X)->use_attrs_ext)) && (NULL != (X)->attrs_ext))

#define TRANSP_CRYPT_INVALID_ALGORITHM_ID   0
#define TRANSP_CRYPT_ALGO_ID_IS_VALID(_algo_id)  (TRANSP_CRYPT_INVALID_ALGORITHM_ID != (_algo_id))
#define REL_CRYPT_ENTRY_IS_VALID(_relcrypt)      (TRANSP_CRYPT_INVALID_ALGORITHM_ID != ((_relcrypt)->algo_id))


typedef struct tagCryptKeyInfoUDF
{
    Oid     encrypt_oid;    
    Oid     decrypt_oid;
    PGFunction encrypt_func;
    PGFunction decrypt_func;
    char    encrypt_prosrc[CRYPT_KEY_INFO_PROC_SRC_LEN];
    char    encrypt_probin[CRYPT_KEY_INFO_PROC_BIN_LEN];
    char    decrypt_prosrc[CRYPT_KEY_INFO_PROC_SRC_LEN];
    char    decrypt_probin[CRYPT_KEY_INFO_PROC_BIN_LEN];
}CryptKeyInfoUDF;

typedef struct tagCryptKeyInfoNonSym
{
    char    publickey  [CRYPT_KEY_INFO_MAX_PUBKEY_LEN]; 
    char    privatekey [CRYPT_KEY_INFO_MAX_PRIKEY_LEN]; 
}CryptKeyInfoKeyPair;

typedef struct tagCryptKeyInfoEntry
{
    AlgoId  algo_id;
    int16   option;         /* see CRYPT_KEY_INFO_OPTION_ENUM */
    int     keysize;
    
    /* use for sym crypt */
    char    password   [CRYPT_KEY_INFO_MAX_PASSWORD_LEN];  
    /* do as args passing to pgp_sym_encrypt, the arg #2 */
    char    option_args[CRYPT_KEY_INFO_MAX_OPT_ARGS_LEN];    

    /* above is common, and the whole context of sym crypt */

    sm4_context sm4_ctx_encrypt;
    sm4_context sm4_ctx_decrypt;
    
    /* for user define crypt algorithm */
    CryptKeyInfoUDF * udf;

    /* use for non sym crypt, also keep udf pubkey and privatekey */
    CryptKeyInfoKeyPair * keypair;
    
}CryptKeyInfoEntry;
typedef CryptKeyInfoEntry * CryptKeyInfo;

/* free mem return from crypt api */
extern void crypt_free(void * ptr);
extern void RenameCryptRelation(Oid myrelid, const char *newrelname);
extern void DeleteCryptPolicy(Oid relid);
extern void AlterCryptdTableNamespace(Oid myrelid, Oid newschemaoid, const char *newschemaname);

#endif /*RELCRYPT_H*/
