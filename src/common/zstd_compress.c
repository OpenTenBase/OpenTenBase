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
free_compress_resource(CompressResource *resource)
{
    if (resource == NULL)
        return;
    if (resource->ctx != NULL)
        ZSTD_freeCCtx(resource->ctx);
    if (resource->errormsg_buf != NULL)
        pfree(resource->errormsg_buf);
    if (resource->zstd_out_buf.dst != NULL)
        pfree(resource->zstd_out_buf.dst);
    if (resource->in_buf_ptr != NULL)
        pfree(resource->in_buf_ptr);
    pfree(resource);
}


/*
  compress_level: zstd paremter
  in_size: max input buffer size
*/
CompressResource *
init_compress_resource(int compress_level, size_t in_size)
{
    size_t zerr;
    CompressResource *resource = (CompressResource *)palloc_extended(sizeof(CompressResource),
						MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
    if (resource == NULL)
        return NULL;
    
    resource->compress_level = compress_level;
    resource->in_size = in_size;
    resource->in_buf_ptr = palloc_extended(resource->in_size, MCXT_ALLOC_NO_OOM);
    resource->zstd_in_buf.src = resource->in_buf_ptr;

    if (resource->zstd_in_buf.src == NULL) 
    {
        pfree(resource);
        return NULL;
    }
 
    resource->out_size = ZSTD_compressBound(resource->in_size) + 16;
    resource->zstd_out_buf.dst = palloc_extended(resource->out_size, MCXT_ALLOC_NO_OOM);
    if (resource->zstd_out_buf.dst == NULL) 
    {
        pfree(resource->in_buf_ptr);
        pfree(resource);
        return NULL;
    }

    resource->errormsg_buf = palloc_extended(ZSTD_ERRORMSG_LEN + 1, MCXT_ALLOC_NO_OOM);
    if (resource->errormsg_buf == NULL) 
    {
        pfree(resource->zstd_out_buf.dst);
        pfree(resource->in_buf_ptr);
        pfree(resource);
        return NULL;
    }

    resource->errormsg_buf[0] = '\0';

    resource->ctx = ZSTD_createCCtx();  /* memory is not under our control */

    if (resource->ctx == NULL)
    {
        pfree(resource->errormsg_buf);
        pfree(resource->zstd_out_buf.dst);
        pfree(resource->in_buf_ptr);
        pfree(resource);
        return NULL;
    }

    zerr = ZSTD_CCtx_setParameter(resource->ctx, ZSTD_c_compressionLevel, resource->compress_level);
    if (ZSTD_isError(zerr))
    {
        ZSTD_freeCCtx(resource->ctx);
        pfree(resource->errormsg_buf);
        pfree(resource->zstd_out_buf.dst);
        pfree(resource->in_buf_ptr);
        pfree(resource);
        return NULL;
    }

    zerr = ZSTD_CCtx_setParameter(resource->ctx, ZSTD_c_checksumFlag, 1);
    if (ZSTD_isError(zerr))
    {
        ZSTD_freeCCtx(resource->ctx);
        pfree(resource->errormsg_buf);
        pfree(resource->zstd_out_buf.dst);
        pfree(resource->in_buf_ptr);
        pfree(resource);
        return NULL;
    }
    return resource;
}

CompressResource *
simple_init_compress_resource(void)
{
    return init_compress_resource(3, ZSTD_BLOCKSIZE_MAX);
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
compress_file(CompressResource *resource, const char *src_path, const char *dst_path)
{
    int src_fd;
    int dst_fd;
    bool read_end = false; /* true means no more data can be read from src file */
    
    Assert(src_path != NULL);
    Assert(dst_path != NULL);
    Assert(resource != NULL);
    ZSTD_CCtx_reset(resource->ctx, ZSTD_reset_session_only);
    resource->compressed_size = 0;

    src_fd = open(src_path, O_RDONLY, (S_IRUSR | S_IWUSR));
    if (src_fd < 0)
    {
        report_invalid_msg(resource->errormsg_buf, "compress:open file %s failed:%s", src_path, strerror(errno));
        return 1;
    }

    dst_fd = open(dst_path, O_RDWR | O_CREAT, (S_IRUSR | S_IWUSR));
    if (dst_fd < 0)
    {
        report_invalid_msg(resource->errormsg_buf, "compress:open file %s failed:%s", dst_path, strerror(errno));
        close(src_fd);
        return 1;
    }

    do
    {
        bool finished = false;
        ZSTD_EndDirective mode = ZSTD_e_continue;
        ssize_t read_len = zstd_file_read(src_fd, resource->zstd_in_buf.src, resource->in_size);
        if (read_len < 0)
        {
            close(src_fd);
            close(dst_fd);
            report_invalid_msg(resource->errormsg_buf, "compress:read file %s content failed:%s", src_path, strerror(errno));
            return 1;
        }

        if (read_len < resource->in_size)
        {
            read_end = true;
            mode = ZSTD_e_end;
        }
            
        resource->zstd_in_buf.size = (size_t)read_len;
        resource->zstd_in_buf.pos = 0;
        resource->compressed_size += (size_t)read_len;

        while (!finished)
        {
            size_t remaining;
            ssize_t write_len;

            resource->zstd_out_buf.pos = 0;
            resource->zstd_out_buf.size = resource->out_size;
            remaining = ZSTD_compressStream2(resource->ctx, &resource->zstd_out_buf, &resource->zstd_in_buf, mode);
            if (ZSTD_isError(remaining))
            {
                close(src_fd);
                close(dst_fd);
                report_invalid_msg(resource->errormsg_buf, "compress file %s failed:%s", src_path, ZSTD_getErrorName(remaining));
                return 1;
            }

            write_len = zstd_file_write(dst_fd, resource->zstd_out_buf.dst, resource->zstd_out_buf.pos);
            if (write_len < 0)
            {
                close(src_fd);
                close(dst_fd);
                report_invalid_msg(resource->errormsg_buf, "compress:write file %s content failed:%s", dst_path, strerror(errno));
                return 1;
            }
            finished = read_end ? (remaining == 0) : (resource->zstd_in_buf.pos == resource->zstd_in_buf.size);
        }
      
    }while (!read_end);
    close(src_fd);
    close(dst_fd); 
    
    return 0;
}

void 
free_decompress_resource(DecompressResource *res)
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

DecompressResource *
init_decompress_resource(size_t out_size)
{
    DecompressResource *resource = (DecompressResource *)palloc_extended(sizeof(DecompressResource),
                            MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
    if (resource == NULL)
        return NULL;
    
    resource->out_size = out_size;
    resource->in_size = ZSTD_compressBound(resource->out_size) + 16;


    resource->in_buf_ptr = palloc_extended(resource->in_size, MCXT_ALLOC_NO_OOM);
    resource->zstd_in_buf.src = resource->in_buf_ptr;

    if (resource->zstd_in_buf.src == NULL) 
    {
        pfree(resource);
        return NULL;
    }
    resource->zstd_in_buf.size = resource->in_size;
    
    resource->zstd_out_buf.dst = palloc_extended(resource->out_size, MCXT_ALLOC_NO_OOM);
    if (resource->zstd_out_buf.dst == NULL) 
    {
        pfree(resource->in_buf_ptr);
        pfree(resource);
        return NULL;
    }
    resource->zstd_out_buf.size = resource->out_size;

    resource->errormsg_buf = palloc_extended(ZSTD_ERRORMSG_LEN + 1, MCXT_ALLOC_NO_OOM);
    if (resource->errormsg_buf == NULL) 
    {
        pfree(resource->zstd_out_buf.dst);
        pfree(resource->in_buf_ptr);
        pfree(resource);
        return NULL;
    }

    resource->errormsg_buf[0] = '\0';

    resource->ctx = ZSTD_createDCtx();  /* memory is not under our control */

    if (resource->ctx == NULL)
    {
        pfree(resource->errormsg_buf);
        pfree(resource->zstd_out_buf.dst);
        pfree(resource->in_buf_ptr);
        pfree(resource);
        return NULL;
    }

    return resource;
}

DecompressResource *
simple_init_decompress_resource(void)
{
    return init_decompress_resource(ZSTD_BLOCKSIZE_MAX);
}

int decompress_file(DecompressResource *resource, const char *src_path, const char *dst_path)
{
    int src_fd;
    int dst_fd;
    bool last_block = false;

    Assert(src_path != NULL);
    Assert(dst_path != NULL);
    Assert(resource != NULL);

    ZSTD_DCtx_reset(resource->ctx, ZSTD_reset_session_only);

    src_fd = open(src_path, O_RDONLY, (S_IRUSR | S_IWUSR));
    if (src_fd < 0)
    {
        report_invalid_msg(resource->errormsg_buf, "decompress:open file %s failed:%s", src_path, strerror(errno));
        return 1;
    }

    dst_fd = open(dst_path, O_RDWR | O_CREAT, (S_IRUSR | S_IWUSR));
    if (dst_fd < 0)
    {
        report_invalid_msg(resource->errormsg_buf, "decompress:open file %s failed:%s", dst_path, strerror(errno));
        close(src_fd);
        return 1;
    }

    do
    {
        bool finished = false;
        ssize_t read_len = zstd_file_read(src_fd, resource->zstd_in_buf.src, resource->in_size);

        if (read_len < 0)
        {
            report_invalid_msg(resource->errormsg_buf, "decompress:read file %s content failed:%s", src_path, strerror(errno));
            close(src_fd);
            close(dst_fd);
            return 1;
        }
        resource->zstd_in_buf.size = (size_t)read_len;
        resource->zstd_in_buf.pos = 0;

        if (read_len < resource->in_size)
            last_block = true;

        while (!finished)
        {
            size_t remaining;
            ssize_t write_len;

            resource->zstd_out_buf.pos = 0;
            resource->zstd_out_buf.size = resource->out_size;
            remaining = ZSTD_decompressStream(resource->ctx, &resource->zstd_out_buf , &resource->zstd_in_buf);
            if (ZSTD_isError(remaining))
            {
                close(src_fd);
                close(dst_fd);
                report_invalid_msg(resource->errormsg_buf, "decompress file %s failed:%s", src_path, ZSTD_getErrorName(remaining));
                return 1;
            }

            write_len = zstd_file_write(dst_fd, resource->zstd_out_buf.dst, resource->zstd_out_buf.pos);
            if (write_len < 0)
            {
                report_invalid_msg(resource->errormsg_buf, "decompress:write file %s content failed:%s", dst_path, strerror(errno));
            }
            finished = last_block ? (remaining == 0) : (resource->zstd_in_buf.pos == resource->zstd_in_buf.size);
        }

    } while(!last_block);

    close(src_fd);
    close(dst_fd);
    return 0;
}

