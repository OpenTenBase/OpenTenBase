/*-------------------------------------------------------------------------
 *
 * memutils.h
 *	  This file contains declarations for memory allocation utility
 *	  functions.  These are functions that are not quite widely used
 *	  enough to justify going in utils/palloc.h, but are still part
 *	  of the API of the memory management subsystem.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/memutils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MEMUTILS_H
#define MEMUTILS_H

#include "postgres.h"
#include "nodes/memnodes.h"
#include "lib/stringinfo.h"
#include <pthread.h>

/*
 * MaxAllocSize, MaxAllocHugeSize
 *		Quasi-arbitrary limits on size of allocations.
 *
 * Note:
 *		There is no guarantee that smaller allocations will succeed, but
 *		larger requests will be summarily denied.
 *
 * palloc() enforces MaxAllocSize, chosen to correspond to the limiting size
 * of varlena objects under TOAST.  See VARSIZE_4B() and related macros in
 * postgres.h.  Many datatypes assume that any allocatable size can be
 * represented in a varlena header.  This limit also permits a caller to use
 * an "int" variable for an index into or length of an allocation.  Callers
 * careful to avoid these hazards can access the higher limit with
 * MemoryContextAllocHuge().  Both limits permit code to assume that it may
 * compute twice an allocation's size without overflow.
 */
#define MaxAllocSize	((Size) 0x3fffffff) /* 1 gigabyte - 1 */

#define AllocSizeIsValid(size)	((Size) (size) <= MaxAllocSize)

#define MaxAllocHugeSize	((Size) -1 >> 1)	/* SIZE_MAX / 2 */

#define AllocHugeSizeIsValid(size)	((Size) (size) <= MaxAllocHugeSize)


/*
 * Standard top-level memory contexts.
 *
 * Only TopMemoryContext and ErrorContext are initialized by
 * MemoryContextInit() itself.
 */
extern PGDLLIMPORT MemoryContext TopMemoryContext;
#ifndef __TDX__
extern PGDLLIMPORT __thread MemoryContext ErrorContext;
#else
extern PGDLLIMPORT MemoryContext ErrorContext;
#endif
extern PGDLLIMPORT MemoryContext PostmasterContext;
extern PGDLLIMPORT MemoryContext CacheMemoryContext;
extern PGDLLIMPORT MemoryContext MessageContext;
extern PGDLLIMPORT MemoryContext TopTransactionContext;
extern PGDLLIMPORT MemoryContext CurTransactionContext;
extern PGDLLIMPORT __thread MemoryContext SubThreadLocalContext;

/* This is a transient link to the active portal's memory context: */
extern PGDLLIMPORT MemoryContext PortalContext;

#ifdef __AUDIT__
extern PGDLLIMPORT MemoryContext AuditContext;
#endif

/* Backwards compatibility macro */
#define MemoryContextResetAndDeleteChildren(ctx) MemoryContextReset(ctx)


/*
 * Memory-context-type-independent functions in mcxt.c
 */
extern void MemoryContextInit(void);
extern void MemoryContextReset(MemoryContext context);
extern void MemoryContextDelete(MemoryContext context);
extern void MemoryContextResetOnly(MemoryContext context);
extern void MemoryContextResetChildren(MemoryContext context);
extern void MemoryContextDeleteChildren(MemoryContext context);
extern void MemoryContextSetIdentifier(MemoryContext context, const char *id);
extern void MemoryContextSetParent(MemoryContext context,
					   MemoryContext new_parent);
extern Size GetMemoryChunkSpace(void *pointer);
extern MemoryContext MemoryContextGetParent(MemoryContext context);
extern bool MemoryContextIsEmpty(MemoryContext context);
extern Size MemoryContextMemAllocated(MemoryContext context, bool recurse);
extern void MemoryContextStats(MemoryContext context);
extern void MemoryContextStatsAll(MemoryContext context);
extern void MemoryContextStatsDetail(MemoryContext context, int max_children,
									 bool print_to_stderr);
extern void MemoryContextAllowInCriticalSection(MemoryContext context,
									bool allow);

#ifdef MEMORY_CONTEXT_CHECKING
extern void MemoryContextCheck(MemoryContext context);
#endif
extern bool MemoryContextContains(MemoryContext context, void *pointer);

/* Handy macro for copying and assigning context ID ... but note double eval */
#define MemoryContextCopyAndSetIdentifier(cxt, id) \
	MemoryContextSetIdentifier(cxt, MemoryContextStrdup(cxt, id))

/*
 * GetMemoryChunkContext
 *		Given a currently-allocated chunk, determine the context
 *		it belongs to.
 *
 * All chunks allocated by any memory context manager are required to be
 * preceded by the corresponding MemoryContext stored, without padding, in the
 * preceding sizeof(void*) bytes.  A currently-allocated chunk must contain a
 * backpointer to its owning context.  The backpointer is used by pfree() and
 * repalloc() to find the context to call.
 */
#ifndef FRONTEND
static inline MemoryContext
GetMemoryChunkContext(void *pointer)
{
	MemoryContext context;

	/*
	 * Try to detect bogus pointers handed to us, poorly though we can.
	 * Presumably, a pointer that isn't MAXALIGNED isn't pointing at an
	 * allocated chunk.
	 */
	Assert(pointer != NULL);
	Assert(pointer == (void *) MAXALIGN(pointer));

	/*
	 * OK, it's probably safe to look at the context.
	 */
	context = *(MemoryContext *) (((char *) pointer) - sizeof(void *));

	AssertArg(MemoryContextIsValid(context));

	return context;
}
#endif

/*
 * This routine handles the context-type-independent part of memory
 * context creation.  It's intended to be called from context-type-
 * specific creation routines, and noplace else.
 */
extern void MemoryContextCreate(MemoryContext node,
					NodeTag tag,
					const MemoryContextMethods *methods,
					MemoryContext parent,
					const char *name);

extern void HandleLogMemoryContextInterrupt(void);
extern void ProcessLogMemoryContextInterrupt(void);
extern void HandleGetMemoryContextDetailInterrupt(void);
extern void HandleGetMemoryDetailInterrupt(void);
extern void ProcessGetMemoryContextDetailInterrupt(void);
extern void ProcessGetMemoryDetailInterrupt(void);

/*
 * Memory-context-type-specific functions
 */

/* aset.c */
extern MemoryContext AllocSetContextCreateExtended(MemoryContext parent,
							  const char *name,
							  Size minContextSize,
							  Size initBlockSize,
							  Size maxBlockSize);
extern Size AllocSetContextTotalSize(MemoryContext context);

/*
 * This wrapper macro exists to check for non-constant strings used as context
 * names; that's no longer supported.  (Use MemoryContextSetIdentifier if you
 * want to provide a variable identifier.)  Note you must specify block sizes
 * with one of the abstraction macros below.
 */
#ifdef HAVE__BUILTIN_CONSTANT_P
#define AllocSetContextCreate(parent, name, allocparams) \
	(StaticAssertExpr(__builtin_constant_p(name), \
					  "memory context names must be constant strings"), \
	 AllocSetContextCreateExtended(parent, name, allocparams))
#else
#define AllocSetContextCreate(parent, name, allocparams) \
	AllocSetContextCreateExtended(parent, name, allocparams)
#endif

/* slab.c */
extern MemoryContext SlabContextCreate(MemoryContext parent,
				  const char *name,
				  Size blockSize,
				  Size chunkSize);

/* generation.c */
extern MemoryContext GenerationContextCreate(MemoryContext parent,
						const char *name,
						Size blockSize);

#ifdef __OPENTENBASE__
extern int32 get_total_memory_size(void);
extern void AllocSetStats_Output(MemoryContext context, long *total_space, long *free_space);
#endif

/*
 * Recommended default alloc parameters, suitable for "ordinary" contexts
 * that might hold quite a lot of data.
 */
#define ALLOCSET_DEFAULT_MINSIZE   0
#define ALLOCSET_DEFAULT_INITSIZE  (8 * 1024)
#define ALLOCSET_DEFAULT_MAXSIZE   (8 * 1024 * 1024)
#define ALLOCSET_DEFAULT_SIZES \
	ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE

/*
 * Recommended alloc parameters for "small" contexts that are never expected
 * to contain much data (for example, a context to contain a query plan).
 */
#define ALLOCSET_SMALL_MINSIZE	 0
#define ALLOCSET_SMALL_INITSIZE  (1 * 1024)
#define ALLOCSET_SMALL_MAXSIZE	 (8 * 1024)
#define ALLOCSET_SMALL_SIZES \
	ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE

/*
 * Recommended alloc parameters for contexts that should start out small,
 * but might sometimes grow big.
 */
#define ALLOCSET_START_SMALL_SIZES \
	ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE


/*
 * Threshold above which a request in an AllocSet context is certain to be
 * allocated separately (and thereby have constant allocation overhead).
 * Few callers should be interested in this, but tuplesort/tuplestore need
 * to know it.
 */
#define ALLOCSET_SEPARATE_THRESHOLD  8192

#define SLAB_DEFAULT_BLOCK_SIZE		(8 * 1024)
#define SLAB_LARGE_BLOCK_SIZE		(8 * 1024 * 1024)

#define TOP50_MEMDETAIL_LEN 50

#define NO_TRACK_MEMINFOS()                                                                        \
	do                                                                                             \
	{                                                                                              \
		no_track_meminfos = true;                                                                  \
	} while (0)

#define RESUME_TRACK_MEMINFOS()                                                                    \
	do                                                                                             \
	{                                                                                              \
		no_track_meminfos = false;                                                                 \
	} while (0)

typedef enum
{
	TRACK_MEM_OFF,     /* do not track memory */
	TRACK_MEM_SESSION, /* session, support tracking the total memory usage of the context at the session level */
	TRACK_MEM_CONTEXT  /* context, support tracking the memory allocation of specific file:line in the context code. */
} TrackMemMode;

/* for PgBackendMemStat */
typedef struct MemCxtDetail
{
	char cxt_name[NAMEDATALEN];
	Size total_size;
} MemCxtDetail;

typedef struct MemoryDetail
{
	char cxt_name[NAMEDATALEN];
	Size total_size;
} MemoryDetail;

typedef struct MemCtxDetail
{
	char file[NAMEDATALEN];
	int         line;
	Size total_size;
} MemCtxDetail;

typedef struct MemAllocDetailKey
{
	const char *file;
	int         line;
	char        cxt_name[NAMEDATALEN];
} MemAllocDetailKey;

typedef struct MemAllocDetail
{
	MemAllocDetailKey key;
	Size              total_size;
} MemAllocDetail;

/* The elements of the first-level hash store context information and the second-level hash. */
typedef struct TrackMemCxtEle
{
	MemoryContext key;
	void *        mem_alloc_stats_hash;
} TrackMemCxtEle;

/* The elements of the second-level hash store memory allocation information. */
typedef struct MemAllocEleKey
{
	const char *file;
	int         line;
} MemAllocEleKey;

typedef struct MemAllocEle
{
	MemAllocEleKey key;
	Size           total_size;
} MemAllocEle;

/* for pg_stat_context_memory_detail */
typedef struct SessionMemAllocDetail
{
	MemAllocDetail entry;
	int            pid;
	char           sessionid[NAMEDATALEN];
} SessionMemAllocDetail;

typedef struct SessionMemAllocDetailList
{
	SessionMemAllocDetail             entry;
	struct SessionMemAllocDetailList *next;
} SessionMemAllocDetailList;

/* for pg_stat_session_memory_detail */
typedef struct SessionMemCxtDetail
{
	char cxt_name[NAMEDATALEN];
	int  pid;
	char sessionid[NAMEDATALEN];
	Size total_size;
} SessionMemCxtDetail;

typedef struct SessionMemCxtDetailList
{
	SessionMemCxtDetail             entry;
	struct SessionMemCxtDetailList *next;
} SessionMemCxtDetailList;

extern int           track_memory_mode;
extern char         *track_context_name;
extern MemoryContext track_context;
extern bool          no_track_meminfos;
extern bool          TrackMemInit(void);
extern bool          track_all_memcxt;
extern __thread bool track_memory_inited;

extern void          ProcessMemTrackAchieveInterrupt(void);
extern void          HandleMemTrackInfoAchieve(void);
extern bool          ParseTrackCxtlist(const char *cxt_name);
extern void InsertMemoryAllocInfo(const void *pointer, MemoryContext context, const char *file,
                                  int line, Size size);
extern void RemoveMemoryAllocInfo(const void *pointer, const MemoryContext context,
                                  const char *file, int line, Size size);
extern void RemoveTrackMemAllocInfo(const void *pointer, const MemoryContext context,
                                    const char *file, int line, Size size);
extern bool MemCxtShouldTrack(const MemoryContext context);
extern void InsertTrackMemAllocInfo(const void *pointer, const MemoryContext context,
                                    const char *file, int line, Size size);
extern void RemoveTrackMemCxt(const MemoryContext context);
extern void DoMemTrackInfoAchieve(void);
extern void DoMemTrackAllocInfoPrint(void);
extern SessionMemCxtDetailList *  GetSessionMemCxtDetail(void);
extern SessionMemAllocDetailList *GetSessionMemAllocDetail(void);
extern void GetTopnMemcxtEles(MemCxtDetail *elem, MemCxtDetail *arr, int *arr_len);
extern void GetTopnMemAllocEles(MemAllocDetail *elem, MemAllocDetail *arr, int *arr_len);

#define SHMEM_MEMCONTEXT_SIZE		 128

typedef struct mcxtdumpEntry
{
    pid_t	dst_pid;
    pid_t	src_pid;
    char	cxt_name[NAMEDATALEN];
} mcxtdumpEntry;

extern void McxtDumpShmemInit(void);
extern void GetMemoryContextDetail(MemCtxDetail *arr, int *arr_len, const char* cxtname);
extern void GetMemoryDetail(MemoryDetail *arr, int *arr_len);
extern void DoMemoryDetailBooking(void);
extern void DoMemCtxDetailBooking(const char*ctxname);
extern void GetAllocBlockInfo(MemoryContext context, StringInfoData* buf);
extern void CollateMemctxInfoInternal(StringInfo mem_info, int* res_len, MemCtxDetail* arr);

#endif							/* MEMUTILS_H */
