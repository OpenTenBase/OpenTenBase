/* ----------
 * zstd_compress.h -
 *
 *	Definitions for the builtin zstd compressor
 *
 * ----------
 */

#ifndef _PG_ZSTD_COMPRESS_H_
#define _PG_ZSTD_COMPRESS_H_

#include "postgres.h"
#include <zstd.h> 

typedef struct _CompressResouce 
{
    size_t in_size;
    size_t out_size;
    size_t compressed_size;
    int compress_level;
    ZSTD_CCtx *ctx;
    
    void *in_buf_ptr; /* for eliminating compilation warnings */
    ZSTD_inBuffer zstd_in_buf;
    ZSTD_outBuffer zstd_out_buf;
	char *errormsg_buf;
} CompressResouce;

typedef struct _DecompressResouce 
{
    size_t in_size;
    size_t out_size;
    ZSTD_DCtx *ctx;
    
    void *in_buf_ptr;
    ZSTD_inBuffer zstd_in_buf;
    ZSTD_outBuffer zstd_out_buf;
	char *errormsg_buf;
} DecompressResouce;

extern CompressResouce *init_compress_resouce(int compress_level, size_t in_size);
extern CompressResouce *simple_init_compress_resouce(void);
extern int compress_file(CompressResouce *resouce, const char *src_path, const char *dst_path);
extern void free_compress_resouce(CompressResouce *resouce);

extern DecompressResouce *init_decompress_resouce(size_t out_size);
extern DecompressResouce *simple_init_decompress_resouce(void);
extern int decompress_file(DecompressResouce *resouce, const char *src_path, const char *dst_path);
extern void free_decompress_resouce(DecompressResouce *res);
#endif