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

typedef struct _CompressResource 
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
} CompressResource;

typedef struct _DecompressResource 
{
    size_t in_size;
    size_t out_size;
    ZSTD_DCtx *ctx;
    
    void *in_buf_ptr;
    ZSTD_inBuffer zstd_in_buf;
    ZSTD_outBuffer zstd_out_buf;
	char *errormsg_buf;
} DecompressResource;

extern CompressResource *init_compress_resource(int compress_level, size_t in_size);
extern CompressResource *simple_init_compress_resource(void);
extern int compress_file(CompressResource *resource, const char *src_path, const char *dst_path);
extern void free_compress_resource(CompressResource *resource);

extern DecompressResource *init_decompress_resource(size_t out_size);
extern DecompressResource *simple_init_decompress_resource(void);
extern int decompress_file(DecompressResource *resource, const char *src_path, const char *dst_path);
extern void free_decompress_resource(DecompressResource *resource);
#endif