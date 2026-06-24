/*-------------------------------------------------------------------------
 *
 * zstd_compress.c
 *		Encapsulate the functionality of file compression using zstd.
 *-------------------------------------------------------------------------
 */

#include <fcntl.h>
#include <unistd.h>

#include "common/zstd_compress.h"

static const int ZSTD_ERRORMSG_LEN = 1000;

static void report_invalid_msg(char *errormsg_buf, const char *fmt, ...) pg_attribute_printf(2, 3);

static void 
report_invalid_msg(char *errormsg_buf, const char *fmt, ...)
{
    va_list args;

    fmt = _(fmt);

    va_start(args, fmt);
    vsnprintf(errormsg_buf, ZSTD_ERRORMSG_LEN, fmt, args);
    va_end(args);
}

void 
free_compress_resouce(CompressResouce *resouce)
{
    if (resouce == NULL)
        return;
    if (resouce->ctx != NULL)
        ZSTD_freeCCtx(resouce->ctx);
    if (resouce->errormsg_buf != NULL)
        pfree(resouce->errormsg_buf);
    if (resouce->zstd_out_buf.dst != NULL)
        pfree(resouce->zstd_out_buf.dst);
    if (resouce->in_buf_ptr != NULL)
        pfree(resouce->in_buf_ptr);
    pfree(resouce);
}


/*
  compress_level: zstd paremter
  in_size: max input buffer size
*/
CompressResouce *
init_compress_resouce(int compress_level, size_t in_size)
{
    size_t zerr;
    CompressResouce *resouce = (CompressResouce *)palloc_extended(sizeof(CompressResouce),
						MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
    if (resouce == NULL)
        return NULL;
    
    resouce->compress_level = compress_level;
    resouce->in_size = in_size;
    resouce->in_buf_ptr = palloc_extended(resouce->in_size, MCXT_ALLOC_NO_OOM);
    resouce->zstd_in_buf.src = resouce->in_buf_ptr;

    if (resouce->zstd_in_buf.src == NULL) 
    {
        pfree(resouce);
        return NULL;
    }
 
    resouce->out_size = ZSTD_compressBound(resouce->in_size) + 16;
    resouce->zstd_out_buf.dst = palloc_extended(resouce->out_size, MCXT_ALLOC_NO_OOM);
    if (resouce->zstd_out_buf.dst == NULL) 
    {
        pfree(resouce->in_buf_ptr);
        pfree(resouce);
        return NULL;
    }

    resouce->errormsg_buf = palloc_extended(ZSTD_ERRORMSG_LEN + 1, MCXT_ALLOC_NO_OOM);
    if (resouce->errormsg_buf == NULL) 
    {
        pfree(resouce->zstd_out_buf.dst);
        pfree(resouce->in_buf_ptr);
        pfree(resouce);
        return NULL;
    }

    resouce->errormsg_buf[0] = '\0';

    resouce->ctx = ZSTD_createCCtx();  /* memory is not under our control */

    if (resouce->ctx == NULL)
    {
        pfree(resouce->errormsg_buf);
        pfree(resouce->zstd_out_buf.dst);
        pfree(resouce->in_buf_ptr);
        pfree(resouce);
        return NULL;
    }

    zerr = ZSTD_CCtx_setParameter(resouce->ctx, ZSTD_c_compressionLevel, resouce->compress_level);
    if (ZSTD_isError(zerr))
    {
        ZSTD_freeCCtx(resouce->ctx);
        pfree(resouce->errormsg_buf);
        pfree(resouce->zstd_out_buf.dst);
        pfree(resouce->in_buf_ptr);
        pfree(resouce);
        return NULL;
    }

    zerr = ZSTD_CCtx_setParameter(resouce->ctx, ZSTD_c_checksumFlag, 1);
    if (ZSTD_isError(zerr))
    {
        ZSTD_freeCCtx(resouce->ctx);
        pfree(resouce->errormsg_buf);
        pfree(resouce->zstd_out_buf.dst);
        pfree(resouce->in_buf_ptr);
        pfree(resouce);
        return NULL;
    }
    return resouce;
}

CompressResouce *
simple_init_compress_resouce(void)
{
    return init_compress_resouce(3, ZSTD_BLOCKSIZE_MAX);
}

static ssize_t 
zstd_file_read(int fd, const char *buffer, size_t amount)
{
    ssize_t ret;

    Assert(fd > 0);
retry:
    ret = read(fd, (char *)buffer, amount);

    if (ret < 0)
    {
        if (errno == EINTR)
            goto retry;
    }

    return ret;
}

static ssize_t 
zstd_file_write(int fd, const char *buffer, size_t amount)
{
    ssize_t ret;

    Assert(fd > 0);
retry:
    ret = write(fd, buffer, amount);

    if (ret < 0)
    {
        if (errno == EINTR)
            goto retry;
    }

    return ret;
}

int 
compress_file(CompressResouce *resouce, const char *src_path, const char *dst_path)
{
    int src_fd;
    int dst_fd;
    bool read_end = false; /* true means no more data can be read from src file */
    
    Assert(src_path != NULL);
    Assert(dst_path != NULL);
    Assert(resouce != NULL);
    ZSTD_CCtx_reset(resouce->ctx, ZSTD_reset_session_only);
    resouce->compressed_size = 0;

    src_fd = open(src_path, O_RDONLY, (S_IRUSR | S_IWUSR));
    if (src_fd < 0)
    {
        report_invalid_msg(resouce->errormsg_buf, "compress:open file %s failed:%s", src_path, strerror(errno));
        return 1;
    }

    dst_fd = open(dst_path, O_RDWR | O_CREAT, (S_IRUSR | S_IWUSR));
    if (dst_fd < 0)
    {
        report_invalid_msg(resouce->errormsg_buf, "compress:open file %s failed:%s", dst_path, strerror(errno));
        close(src_fd);
        return 1;
    }

    do
    {
        bool finished = false;
        ZSTD_EndDirective mode = ZSTD_e_continue;
        ssize_t read_len = zstd_file_read(src_fd, resouce->zstd_in_buf.src, resouce->in_size);
        if (read_len < 0)
        {
            close(src_fd);
            close(dst_fd);
            report_invalid_msg(resouce->errormsg_buf, "compress:read file %s content failed:%s", src_path, strerror(errno));
            return 1;
        }

        if (read_len < resouce->in_size)
        {
            read_end = true;
            mode = ZSTD_e_end;
        }
            
        resouce->zstd_in_buf.size = (size_t)read_len;
        resouce->zstd_in_buf.pos = 0;
        resouce->compressed_size += (size_t)read_len;

        while (!finished)
        {
            size_t remaining;
            ssize_t write_len;

            resouce->zstd_out_buf.pos = 0;
            resouce->zstd_out_buf.size = resouce->out_size;
            remaining = ZSTD_compressStream2(resouce->ctx, &resouce->zstd_out_buf, &resouce->zstd_in_buf, mode);
            if (ZSTD_isError(remaining))
            {
                close(src_fd);
                close(dst_fd);
                report_invalid_msg(resouce->errormsg_buf, "compress file %s failed:%s", src_path, ZSTD_getErrorName(remaining));
                return 1;
            }

            write_len = zstd_file_write(dst_fd, resouce->zstd_out_buf.dst, resouce->zstd_out_buf.pos);
            if (write_len < 0)
            {
                close(src_fd);
                close(dst_fd);
                report_invalid_msg(resouce->errormsg_buf, "compress:write file %s content failed:%s", dst_path, strerror(errno));
                return 1;
            }
            finished = read_end ? (remaining == 0) : (resouce->zstd_in_buf.pos == resouce->zstd_in_buf.size);
        }
      
    }while (!read_end);
    close(src_fd);
    close(dst_fd); 
    
    return 0;
}

void 
free_decompress_resouce(DecompressResouce *res)
{
    if (res == NULL)
        return;

    if (res->ctx != NULL)
        ZSTD_freeDCtx(res->ctx);
    if (res->errormsg_buf != NULL)
        pfree(res->errormsg_buf);
    if (res->zstd_out_buf.dst != NULL)
        pfree(res->zstd_out_buf.dst);
    if (res->in_buf_ptr != NULL)
        pfree(res->in_buf_ptr);
    pfree(res);
}

DecompressResouce *
init_decompress_resouce(size_t out_size)
{
    DecompressResouce *resouce = (DecompressResouce *)palloc_extended(sizeof(DecompressResouce),
                            MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
    if (resouce == NULL)
        return NULL;
    
    resouce->out_size = out_size;
    resouce->in_size = ZSTD_compressBound(resouce->out_size) + 16;


    resouce->in_buf_ptr = palloc_extended(resouce->in_size, MCXT_ALLOC_NO_OOM);
    resouce->zstd_in_buf.src = resouce->in_buf_ptr;

    if (resouce->zstd_in_buf.src == NULL) 
    {
        pfree(resouce);
        return NULL;
    }
    resouce->zstd_in_buf.size = resouce->in_size;
    
    resouce->zstd_out_buf.dst = palloc_extended(resouce->out_size, MCXT_ALLOC_NO_OOM);
    if (resouce->zstd_out_buf.dst == NULL) 
    {
        pfree(resouce->in_buf_ptr);
        pfree(resouce);
        return NULL;
    }
    resouce->zstd_out_buf.size = resouce->out_size;

    resouce->errormsg_buf = palloc_extended(ZSTD_ERRORMSG_LEN + 1, MCXT_ALLOC_NO_OOM);
    if (resouce->errormsg_buf == NULL) 
    {
        pfree(resouce->zstd_out_buf.dst);
        pfree(resouce->in_buf_ptr);
        pfree(resouce);
        return NULL;
    }

    resouce->errormsg_buf[0] = '\0';

    resouce->ctx = ZSTD_createDCtx();  /* memory is not under our control */

    if (resouce->ctx == NULL)
    {
        pfree(resouce->errormsg_buf);
        pfree(resouce->zstd_out_buf.dst);
        pfree(resouce->in_buf_ptr);
        pfree(resouce);
        return NULL;
    }

    return resouce;
}

DecompressResouce *
simple_init_decompress_resouce(void)
{
    return init_decompress_resouce(ZSTD_BLOCKSIZE_MAX);
}

int decompress_file(DecompressResouce *resouce, const char *src_path, const char *dst_path)
{
    int src_fd;
    int dst_fd;
    bool last_block = false;

    Assert(src_path != NULL);
    Assert(dst_path != NULL);
    Assert(resouce != NULL);

    ZSTD_DCtx_reset(resouce->ctx, ZSTD_reset_session_only);

    src_fd = open(src_path, O_RDONLY, (S_IRUSR | S_IWUSR));
    if (src_fd < 0)
    {
        report_invalid_msg(resouce->errormsg_buf, "decompress:open file %s failed:%s", src_path, strerror(errno));
        return 1;
    }

    dst_fd = open(dst_path, O_RDWR | O_CREAT, (S_IRUSR | S_IWUSR));
    if (dst_fd < 0)
    {
        report_invalid_msg(resouce->errormsg_buf, "decompress:open file %s failed:%s", dst_path, strerror(errno));
        close(src_fd);
        return 1;
    }

    do
    {
        bool finished = false;
        ssize_t read_len = zstd_file_read(src_fd, resouce->zstd_in_buf.src, resouce->in_size);

        if (read_len < 0)
        {
            report_invalid_msg(resouce->errormsg_buf, "decompress:read file %s content failed:%s", src_path, strerror(errno));
            close(src_fd);
            close(dst_fd);
            return 1;
        }
        resouce->zstd_in_buf.size = (size_t)read_len;
        resouce->zstd_in_buf.pos = 0;

        if (read_len < resouce->in_size)
            last_block = true;

        while (!finished)
        {
            size_t remaining;
            ssize_t write_len;

            resouce->zstd_out_buf.pos = 0;
            resouce->zstd_out_buf.size = resouce->out_size;
            remaining = ZSTD_decompressStream(resouce->ctx, &resouce->zstd_out_buf , &resouce->zstd_in_buf);
            if (ZSTD_isError(remaining))
            {
                close(src_fd);
                close(dst_fd);
                report_invalid_msg(resouce->errormsg_buf, "compress file %s failed:%s", src_path, ZSTD_getErrorName(remaining));
                return 1;
            }

            write_len = zstd_file_write(dst_fd, resouce->zstd_out_buf.dst, resouce->zstd_out_buf.pos);
            if (write_len < 0)
            {
                report_invalid_msg(resouce->errormsg_buf, "compress:write file %s content failed:%s", dst_path, strerror(errno));
            }
            finished = last_block ? (remaining == 0) : (resouce->zstd_in_buf.pos == resouce->zstd_in_buf.size);
        }

    } while(!last_block);

    close(src_fd);
    close(dst_fd);
    return 0;
}

