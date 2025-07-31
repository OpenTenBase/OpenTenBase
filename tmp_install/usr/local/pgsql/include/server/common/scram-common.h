/*-------------------------------------------------------------------------
 *
 * scram-common.h
 *        Declarations for helper functions used for SCRAM authentication
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/common/scram-common.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SCRAM_COMMON_H
#define SCRAM_COMMON_H

#include "common/sha2.h"

/* Length of SCRAM keys (client and server) */
#define SCRAM_KEY_LEN                PG_SHA256_DIGEST_LENGTH

/* length of HMAC */
#define SHA256_HMAC_B                PG_SHA256_BLOCK_LENGTH

/*
 * Size of random nonce generated in the authentication exchange.  This
 * is in "raw" number of bytes, the actual nonces sent over the wire are
 * encoded using only ASCII-printable characters.
 */
#define SCRAM_RAW_NONCE_LEN            18

/* length of salt when generating new verifiers */
#define SCRAM_DEFAULT_SALT_LEN        12

/* default number of iterations when generating verifier */
#define SCRAM_DEFAULT_ITERATIONS    4096

/*
 * Context data for HMAC used in SCRAM authentication.
 */
typedef struct
{
    pg_sha256_ctx sha256ctx;
    uint8        k_opad[SHA256_HMAC_B];
} scram_HMAC_ctx;

extern void scram_HMAC_init(scram_HMAC_ctx *ctx, const uint8 *key, int keylen);
extern void scram_HMAC_update(scram_HMAC_ctx *ctx, const char *str, int slen);
extern void scram_HMAC_final(uint8 *result, scram_HMAC_ctx *ctx);

extern void scram_SaltedPassword(const char *password, const char *salt,
                     int saltlen, int iterations, uint8 *result);
extern void scram_H(const uint8 *str, int len, uint8 *result);
extern void scram_ClientKey(const uint8 *salted_password, uint8 *result);
extern void scram_ServerKey(const uint8 *salted_password, uint8 *result);

extern char *scram_build_verifier(const char *salt, int saltlen, int iterations,
                     const char *password);

#endif                            /* SCRAM_COMMON_H */
