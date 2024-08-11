/*-------------------------------------------------------------------------
 *
 * rda_dsm_impl.c
 *
 *	  Functions to re-distrubution data to remote Datanodes
 *
 *
 * Copyright (c) 2019-2024 TeleDB Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/pgxc/pool/rda_dsm_impl.c
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h> 
#include <sys/stat.h>
#ifdef HAVE_SYS_IPC_H
#include <sys/ipc.h>
#endif
#ifdef HAVE_SYS_SHM_H
#include <sys/shm.h>
#endif
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/spin.h"
#include "portability/mem.h"
#include "pgxc/rda_frame_impl.h"
#include "storage/fd.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "postmaster/postmaster.h"

/* Size of buffer to be used for zero-filling. */
#define ZBUFFER_SIZE                4096


FILE *
debug_out_file_open(int64 rda_id, int nodeid, int owner)
{
    FILE *fp;
    char name[128];

   
    snprintf(name, 128, PG_RDA_DIR "/%ld-%d-%d", rda_id, nodeid, owner);

    fp = fopen(name, "wb");
    if (fp == NULL)
    {
        elog(LOG, "open out buffer %s failed: %d", name, errno);
        return NULL;
    }

    return fp;
}

void
debug_out_file_close(FILE *fp)
{
    if (fp == NULL)
        return;

    fclose(fp);
}


bool
debug_out_file_write_buffer(FILE *fp, uint32_t len, uint8_t *data)
{
    if (NULL == fp || NULL == data)
    {
        return true;
    }

    if (0 == len)
    {
        return true;
    }

    if (fwrite(data, len, 1, fp) != 1)
    {
        return false;
    }

    fflush(fp);
    return true;
}

static bool
debug_out_file_write_tup(FILE *fp, rda_dsm_tup *tup,
                                     uint32_t len, uint8_t *data)
{
    size_t ret;

    if (fp == NULL)
        return true;

    ret = fwrite(tup, sizeof(rda_dsm_tup), 1, fp);
    if (ret != 1)
        goto file_err;

    ret = fwrite(data, len, 1, fp);
    if (ret != 1)
        goto file_err;

    fflush(fp);
    return true;

file_err:
    elog(LOG, "write out buffer failed: %d", errno);
    fclose(fp);
    return false;
}


/*
 *  dms_rda_create_and_attach
 * Create shared-memory for RDA in backend.
 * Create file and resize to request-size, mmap this file
 */
bool
dms_rda_create_and_attach(int64 rda_id, uint32_t request_size,
                          void **mapped_addr, uint32_t *mapped_size)
{
    char    name[128];
    int     fd;
    int     flags;
    char    *addr;
    struct stat st;

    snprintf(name, 128, PG_RDA_DIR "/" PG_RDA_MMAP_FILE_PREFIX "%ld" , rda_id);

    /* make sure we create a new shmem-file */
    flags = O_RDWR | O_CREAT | O_TRUNC;
    if ((fd = open(name, flags, 0600)) == -1)
    {
        return false;
    }

    /* get file size */
    if (fstat(fd, &st) != 0)
    {
        int     save_errno;

        /* Back out what's already been done. */
        save_errno = errno;
        close(fd);
        errno = save_errno;

        return false;
    }

    /* make sure the request size */
    if (st.st_size > request_size && ftruncate(fd, request_size))
    {
        int         save_errno;

        /* Back out what's already been done. */
        save_errno = errno;
        close(fd);
        unlink(name);
        errno = save_errno;

        return false;
    }
    else if (st.st_size < request_size)
    {
		char	   *zbuffer = (char *) palloc0(ZBUFFER_SIZE);
		uint32		remaining = request_size;
		bool		success = true;

		while (success && remaining > 0)
		{
			Size		goal = remaining;

			if (goal > ZBUFFER_SIZE)
				goal = ZBUFFER_SIZE;

			if (write(fd, zbuffer, goal) == goal)
				remaining -= goal;
			else
				success = false;
		}
        pfree(zbuffer);

        if (!success)
        {
            int         save_errno;

            save_errno = errno;
            close(fd);
            unlink(name);
            errno = save_errno;

            return false;
        }
    }

	/* Map it. */
    addr = mmap(NULL, request_size, PROT_READ | PROT_WRITE,
                MAP_SHARED | MAP_NOSYNC, fd, 0);
    if (addr == MAP_FAILED)
    {
        int			save_errno;

        /* Back out what's already been done. */
        save_errno = errno;
        close(fd);
        unlink(name);
        errno = save_errno;

        return false;
    }

    *mapped_addr = addr;
    *mapped_size = request_size;
    close(fd);

    return true;
}

/*
 * dms_rda_attach
 * Attach shared-memory of RDA in Forwarder
 * RDA shared-memory file is created by backend, and Forwarder-Process
 *  just attach it
 */
bool
dms_rda_attach(int64 rda_id, void **mapped_addr, uint32_t *mapped_size)
{
    char    name[128];
    int     fd;
    char    *addr;
    struct stat st;

    snprintf(name, 128, PG_RDA_DIR "/" PG_RDA_MMAP_FILE_PREFIX "%ld" , rda_id);

    /* open file */
	if ((fd = open(name, O_RDWR, 0600)) == -1)
    {
        return false;
    }

    /* get file size */
    if (fstat(fd, &st) != 0)
    {
        int     save_errno;

        /* Back out what's already been done. */
        save_errno = errno;
        close(fd);
        errno = save_errno;

        return false;
    }

	/* Map it. */
    addr = mmap(NULL, st.st_size, PROT_READ | PROT_WRITE,
                MAP_SHARED | MAP_HASSEMAPHORE | MAP_NOSYNC, fd, 0);
    if (addr == MAP_FAILED)
    {
        int			save_errno;

        /* Back out what's already been done. */
        save_errno = errno;
        close(fd);
        errno = save_errno;

        return false;
    }

    *mapped_addr = addr;
    *mapped_size = st.st_size;
    close(fd);

    return true;
}


static void
dsm_rda_frame_init(rda_dsm_frame *frame)
{
    frame->magic = PG_RDA_FRAME_MAGIC;
    frame->head = 0;
    frame->tail = 0;
    frame->status      = RDA_FRAME_STATUS_OK;
    frame->src_nodeid  = 0;
    frame->dest_nodeid = 0;
    frame->_pending_1  = 'X';
    frame->_pending_2  = 'X';
    frame->length = RDA_FRAME_SIZE - sizeof(rda_dsm_frame);
	SpinLockInit(&(frame->lock));
}

/*
 * Split rda shared-memory to rda-frames
 */
void
dsm_map_rda_frames(void *mapped_addr, uint32_t mapped_size, int in_nodes,
                   int out_nodes, rda_dsm_frame **in_frames,
                   rda_dsm_frame **out_frames, bool init)
{
    int i;
    uint32_t offset_idx = 0;
    uint8_t *data = (uint8_t *)mapped_addr;

    Assert(mapped_size >= (in_nodes + out_nodes) * RDA_FRAME_SIZE);

    for (i = 0; i < in_nodes; i++)
    {
        rda_dsm_frame *frame = (rda_dsm_frame *)(data +
                                (RDA_FRAME_SIZE * (offset_idx++)));
        if (init)
        {
            dsm_rda_frame_init(frame);
        }

        Assert(frame->magic == PG_RDA_FRAME_MAGIC);
        in_frames[i] = frame;
    }

    for (i = 0; i < out_nodes; i++)
    {
        rda_dsm_frame *frame = (rda_dsm_frame *)(data +
                                (RDA_FRAME_SIZE * (offset_idx++)));
        if (init)
        {
            dsm_rda_frame_init(frame);
        }

        Assert(frame->magic == PG_RDA_FRAME_MAGIC);
        out_frames[i] = frame;
    }
}

/*
 * dsm_rda_detach
 * Detach RDA shared-memory in backend
 * Backend-Process just detach shared-memory, and Forwarder-Process will
 *  unlink it later
 */
bool
dsm_rda_detach(int64 rda_id, void **mapped_addr, uint32_t *mapped_size)
{
    char    name[128];
    snprintf(name, 128, PG_RDA_DIR "/" PG_RDA_MMAP_FILE_PREFIX "%ld" , rda_id);
    if (*mapped_addr != NULL
        && munmap(*mapped_addr, *mapped_size) != 0)
    {
        return false;
    }

    *mapped_addr = NULL;
    *mapped_size = 0;
    return true;
}

/*
 * dsm_rda_detach_and_unlink
 * Detach RDA shared-memory and remove the file.
 * Forwarder-Process should clean RDA shared-memory, backend just detach it.
 */
bool
dsm_rda_detach_and_unlink(int64 rda_id, void **mapped_addr, uint32_t *mapped_size)
{
    bool    ret;
    char    name[128];

    ret = dsm_rda_detach(rda_id, mapped_addr, mapped_size);
    if (!ret)
        return ret;

    snprintf(name, 128, PG_RDA_DIR "/" PG_RDA_MMAP_FILE_PREFIX "%ld" , rda_id);
    if (unlink(name) != 0)
    {
        return false;
    }

    return true;
}

/*
 * Used to increase the write pointer after write some data.
 */
void
rda_frame_inc_write_off(rda_dsm_frame *frame, uint32_t uiLen)
{
    uint32_t free_size = rda_frame_free_space(frame);

    if (frame == NULL)
        return;

    Assert(free_size >= uiLen);
	
	SpinLockAcquire(&(frame->lock));
    frame->head += uiLen;
    frame->head = frame->head % frame->length;
	SpinLockRelease(&(frame->lock));
}

/*
 * Increate data offset, used after finishing read data from queue.
 */
void
rda_frame_inc_data_off(rda_dsm_frame *frame, uint32_t uiLen)
{
    uint32_t data_size = rda_frame_data_size(frame);
    if (frame == NULL)
        return;

    Assert(data_size >= uiLen);
	
	SpinLockAcquire(&(frame->lock));
    frame->tail = (frame->tail + uiLen) % frame->length;
	SpinLockRelease(&(frame->lock));
}

/* 
 * Return free space of the buffer.
 */
uint32_t
rda_frame_free_space(rda_dsm_frame *frame)
{
    uint32_t len = 0;
	if (frame == NULL)
    {
        return 0;
    }
	
	SpinLockAcquire(&(frame->lock));
    if (frame->tail <= frame->head)
    {
        len = frame->tail + frame->length - frame->head - 1;
    }
	else
	{
        len = frame->tail - frame->head - 1;
	}
	SpinLockRelease(&(frame->lock));
	
    return len;
}

/*
 * Return total data size in buffer
 */
uint32_t
rda_frame_data_size(rda_dsm_frame *frame)
{
    uint32_t size = 0;

    if (frame == NULL)
        return 0;

	SpinLockAcquire(&(frame->lock));
    if (frame->tail <= frame->head)
        size = frame->head - frame->tail;
    else
        size = frame->length - frame->tail + frame->head;
	SpinLockRelease(&(frame->lock));

    return size;
}

/*
 * Get a free space in frame to write, return the buffer
 *  pointer and length could be written.
 * If the free space is around and splits, just return the first part
 *  which is at the end of frame.
 */
uint8_t *
rda_frame_get_write_off(rda_dsm_frame *frame, uint32_t *uiLen)
{
    uint32_t head = 0;
    uint32_t tail = 0;
	uint8_t  *data;

    *uiLen = 0;
    if (frame == NULL)
        return NULL;

	SpinLockAcquire(&(frame->lock));
	
    head = frame->head;
    tail = frame->tail;
    if (head >= tail)
    {
        if (tail != 0)
            *uiLen = frame->length - head;
        else
            *uiLen = frame->length - head - 1;
    }
    else
        *uiLen = tail - head - 1;
	data = frame->data + head;
	SpinLockRelease(&(frame->lock));

    return data;
}

/*
 * Get a package(include data) from frame to read, return the package
 *  buffer.
 * If the package is around, just return the end part in this time,
 *  and you could get the other part in next reading.
 */
uint8_t *
rda_frame_get_data_off(rda_dsm_frame *frame, uint32_t *uiLen)
{
    uint32_t head = 0;
    uint32_t tail = 0;
	uint8_t  *data;

    *uiLen = 0;
    if (frame == NULL || rda_frame_data_size(frame) <= 0)
    {
        return NULL;
    }

	SpinLockAcquire(&(frame->lock));
    head = frame->head;
    tail = frame->tail;
    if (tail >= head)
        *uiLen = frame->length - tail;
    else
        *uiLen = head - tail;

	data = frame->data + tail;
	SpinLockRelease(&(frame->lock));
    return data;
}

/* 
 * Return status of the frame.
 */
int32_t
rda_get_frame_status(rda_dsm_frame *frame)
{
	if (frame == NULL)
    {
        return RDA_FRAME_STATUS_OK;
    }
	
    return frame->status;
}

/* 
 * Return status of the frame.
 */
void
rda_set_frame_status(rda_dsm_frame *frame, int32_t status)
{
	if (frame == NULL)
    {
        return;
    }
	
    frame->status = status;
}
/* 
 * Reset frame status.
 */
void
rda_reset_frame(rda_dsm_frame *frame)
{
    SpinLockAcquire(&(frame->lock));
	frame->magic  = PG_RDA_FRAME_MAGIC;
    frame->head   = 0;
    frame->tail   = 0;
    frame->status = RDA_FRAME_STATUS_OK;
    SpinLockRelease(&(frame->lock));
}


/*
 * Enqueue tup(data) to frame
 * Failed if frame space is not enough, OK on the other side.
 * The tup data may be splited to two parts, the first one fills
 * the end of frame, and the other on the head.
 */
bool
rda_frame_enqueue_data(FILE **fpp, uint32_t seq_num, rda_dsm_frame *frame,
                       uint16_t type, uint32_t len, uint32_t node, uint8_t *data)
{
    uint8_t     *ptr;
    uint32_t    bufferLen;
    uint32_t    needLen = RDA_TUP_HEAD_SIZE + len;
    uint32_t    leftLen = needLen;
    uint32_t    headLeftLen = 0;
#ifdef DATAROW_MD5_CHECK
    char        cal_md5[MD5_CHECK_LEN + 1];
#endif

    rda_dsm_tup tup = {
                        .type = type,
                        .magic = PG_RDA_TUP_MAGIC,
                        .seq_num = seq_num,
                        .checksum = ((type | (PG_RDA_TUP_MAGIC << 16)) ^ node ^ len ^ seq_num),
                        .msgnode = node,
                        .msglen = len
                      };
#ifdef DATAROW_MD5_CHECK
    /* validate md5sum */
    memset(cal_md5, 0, MD5_CHECK_LEN + 1);

    if (!pg_md5_hash(data, len, cal_md5))
        elog(ERROR, "Fail to pg_md5_hash in rda_frame_enqueue_data!");

    memcpy(tup.md5, cal_md5, MD5_CHECK_LEN);

#endif

    /* free space is not enough for new tup */
    if (frame == NULL || rda_frame_free_space(frame) < needLen)
        return false;

    ptr = rda_frame_get_write_off(frame, &bufferLen);
    Assert((ptr != NULL) && (bufferLen > 0));
    if (bufferLen > RDA_TUP_HEAD_SIZE)
    {
        uint32_t copyLen = (bufferLen > needLen)
                                ? len : (bufferLen - RDA_TUP_HEAD_SIZE);

        /* copy struct body */
        memcpy(ptr, &tup, RDA_TUP_HEAD_SIZE);
        leftLen = leftLen - RDA_TUP_HEAD_SIZE;

        memcpy(ptr + RDA_TUP_HEAD_SIZE, data, copyLen);
        leftLen = leftLen - copyLen;
    }
    else
    {
        /* free space is too small to put tup header */
        memcpy(ptr, &tup, bufferLen);

        headLeftLen = RDA_TUP_HEAD_SIZE - bufferLen;
        leftLen = leftLen - bufferLen;
    }
    rda_frame_inc_write_off(frame, needLen - leftLen);

    /* copy completed */
    if (leftLen == 0)
    {
        if (!debug_out_file_write_tup(*fpp, &tup, len, data))
            *fpp = NULL;

        return true;
    }

    ptr = rda_frame_get_write_off(frame, &bufferLen);
    Assert((ptr != NULL) && (bufferLen >= leftLen));

    if (headLeftLen > 0)
    {
        /* copy the left part of tup struct */
        memcpy(ptr, ((uint8_t *)&tup) + RDA_TUP_HEAD_SIZE - headLeftLen, headLeftLen);

        ptr = ptr + headLeftLen;
        leftLen = leftLen - headLeftLen;
    }
    /* copy the left data */
    memcpy(ptr, data + (len - leftLen), leftLen);
    rda_frame_inc_write_off(frame, leftLen + headLeftLen);

    if (!debug_out_file_write_tup(*fpp, &tup, len, data))
        *fpp = NULL;

    return true;
}

/*
 * Get a special size buffer from frame.
 * Return false if frame size is not enough.
 * We will copy out buffer from frame to 'dest', pls make memory ready
 */
static bool
rda_frame_internal_get_data(FILE **fpp, rda_dsm_frame *frame, uint8_t *dest, uint32_t size)
{
    uint8_t         *ptr;
    uint32_t        offset = 0;
    uint32_t        bufferLen;
    uint32_t        needLen = size;

    if (frame == NULL || rda_frame_data_size(frame) < size)
    {
        return false;
    }

    ptr = rda_frame_get_data_off(frame, &bufferLen);
    Assert((ptr != NULL) && (bufferLen > 0));
    if (bufferLen < needLen)
    {
        memcpy(dest, ptr, bufferLen);
        rda_frame_inc_data_off(frame, bufferLen);

        offset = bufferLen;
        needLen = needLen - bufferLen;

        /* get buffer again */
        ptr = rda_frame_get_data_off(frame, &bufferLen);
    }

    Assert((ptr != NULL) && (bufferLen >= needLen));

    memcpy(dest + offset, ptr, needLen);
    rda_frame_inc_data_off(frame, needLen);

    if (!debug_out_file_write_buffer(*fpp, size, dest))
    {
        /* reset owner pointer when write failed */
        *fpp = NULL;
    }

    return true;
}

/*
 * Get a Tup Header from frame
 */
bool
rda_frame_get_tup_head(FILE **fpp, rda_dsm_frame *frame, rda_dsm_tup *tup)
{
    bool ret;
    if (tup == NULL)
        return true;

    ret = rda_frame_internal_get_data(fpp, frame, (uint8_t *)tup,
                                       RDA_TUP_HEAD_SIZE);
    if (ret)
    {
        uint32 checksum = (tup->type | (PG_RDA_TUP_MAGIC << 16))
                             ^ tup->msgnode ^ tup->msglen ^ tup->seq_num;
        Assert(tup->checksum == checksum);
    }
    return ret;
}

/*
 * Get Tup Msg Data from frame.
 * Make sure get Tup Data after Tup Header
 */
bool
rda_frame_get_tup_data(FILE **fpp, rda_dsm_frame *frame, rda_dsm_tup_datarow *datarow, uint8_t *md5)
{
    bool ret;
#ifdef DATAROW_MD5_CHECK
    char cal_md5[MD5_CHECK_LEN + 1];
#endif

    if (datarow == NULL || datarow->msglen == 0)
        return true;
   
    ret = rda_frame_internal_get_data(fpp, frame, datarow->msg, datarow->msglen);
#ifdef DATAROW_MD5_CHECK
    if(ret && md5 != NULL)
    {
        memset(cal_md5, 0, MD5_CHECK_LEN + 1);
        if (!pg_md5_hash(datarow->msg, datarow->msglen, cal_md5))
            elog(ERROR, "Fail to pg_md5_hash in recevice_rescan_from_in_frame!");
    
        Assert(memcmp(md5, cal_md5, MD5_CHECK_LEN) == 0);
    }
#endif  
    return ret;
}
#if 0
/*
 * Get a Tup Header from frame
 */
bool
rda_frame_test_dequeue(FILE **fpp, rda_dsm_frame *frame, uint8_t *data, int size)
{
    return rda_frame_internal_get_data(fpp, frame, (uint8_t *)data, size);
}
#endif
