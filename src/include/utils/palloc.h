/*-------------------------------------------------------------------------
 *
 * palloc.h
 *	  POSTGRES memory allocator definitions.
 *
 * This file contains the basic memory allocation interface that is
 * needed by almost every backend module.  It is included directly by
 * postgres.h, so the definitions here are automatically available
 * everywhere.  Keep it lean!
 *
 * Memory allocation occurs within "contexts".  Every chunk obtained from
 * palloc()/MemoryContextAlloc() is allocated within a specific context.
 * The entire contents of a context can be freed easily and quickly by
 * resetting or deleting the context --- this is both faster and less
 * prone to memory-leakage bugs than releasing chunks individually.
 * We organize contexts into context trees to allow fine-grain control
 * over chunk lifetime while preserving the certainty that we will free
 * everything that should be freed.  See utils/mmgr/README for more info.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/palloc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PALLOC_H
#define PALLOC_H

extern __thread bool am_sub_thread;		/* Am I a sub thread? */

#define DISABLED_IN_SUB_THREAD() \
do	\
{ \
	if (unlikely(am_sub_thread)) \
		elog(ERROR, "can not call %s in sub thread", __FUNCTION__);	\
} while(0)

/*
 * Type MemoryContextData is declared in nodes/memnodes.h.  Most users
 * of memory allocation should just treat it as an abstract type, so we
 * do not provide the struct contents here.
 */
typedef struct MemoryContextData *MemoryContext;

/*
 * A memory context can have callback functions registered on it.  Any such
 * function will be called once just before the context is next reset or
 * deleted.  The MemoryContextCallback struct describing such a callback
 * typically would be allocated within the context itself, thereby avoiding
 * any need to manage it explicitly (the reset/delete action will free it).
 */
typedef void (*MemoryContextCallbackFunction) (void *arg);

typedef struct MemoryContextCallback
{
	MemoryContextCallbackFunction func; /* function to call */
	void	   *arg;			/* argument to pass it */
	struct MemoryContextCallback *next; /* next in list of callbacks */
} MemoryContextCallback;

/*
 * CurrentMemoryContext is the default allocation context for palloc().
 * Avoid accessing it directly!  Instead, use MemoryContextSwitchTo()
 * to change the setting.
 */
extern PGDLLIMPORT __thread MemoryContext CurrentMemoryContext;

/*
 * Flags for MemoryContextAllocExtended.
 */
#define MCXT_ALLOC_HUGE			0x01	/* allow huge allocation (> 1 GB) */
#define MCXT_ALLOC_NO_OOM		0x02	/* no failure if out-of-memory */
#define MCXT_ALLOC_ZERO			0x04	/* zero allocated memory */

/*
 * Fundamental memory-allocation operations (more are in utils/memutils.h)
 */
extern void *MemoryContextAllocInternal(MemoryContext context, Size size, const char* file, int line);
#define MemoryContextAlloc(context, size) MemoryContextAllocInternal(context, size, __FILE__, __LINE__)

extern void *MemoryContextAllocZeroInternal(MemoryContext context, Size size, const char* file, int line);
#define MemoryContextAllocZero(context, size) MemoryContextAllocZeroInternal(context, size, __FILE__, __LINE__)

extern void *MemoryContextAllocZeroAlignedIternal(MemoryContext context, Size size, const char* file, int line);
#define MemoryContextAllocZeroAligned(context, size) MemoryContextAllocZeroAlignedIternal(context, size, __FILE__, __LINE__)
extern void *MemoryContextAllocExtendedIternal(MemoryContext context, Size size, int flags, const char* file, int line);
#define MemoryContextAllocExtended(context, size, flags) MemoryContextAllocExtendedIternal(context, size, flags, __FILE__, __LINE__)

extern void *palloc_internal(Size size, const char* file, int line);
extern void *palloc0_internal(Size size, const char* file, int line);
extern void *palloc_extended_internal(Size size, int flags, const char* file, int line);
extern void *repallocInternal(void *pointer, Size size, const char* file, int line);
extern void pfree(void *pointer);

#ifdef palloc
#undef palloc
#endif
#ifdef palloc0
#undef palloc0
#endif
#ifdef palloc_extended
#undef palloc_extended
#endif
#define palloc(sz) palloc_internal((sz), __FILE__, __LINE__)
#define palloc0(sz) palloc0_internal((sz), __FILE__, __LINE__)
#define palloc_extended(size, flags) palloc_extended_internal(size, flags, __FILE__, __LINE__)
#define repalloc(pointer, size) repallocInternal(pointer, size, __FILE__, __LINE__)

#define pfree_ext(ptr)        \
do {                          \
	if (ptr) {                \
		pfree((void *)(ptr)); \
		(ptr) = NULL; 		  \
	}						  \
} while(0)

/* Note: ptr is not set NULL after free */
#define pfree_ext_not_set_null(ptr)     \
do {                        			\
	if (ptr)                			\
		pfree((void *)(ptr)); 			\
} while(0)

/*
 * The result of palloc() is always word-aligned, so we can skip testing
 * alignment of the pointer when deciding which MemSet variant to use.
 * Note that this variant does not offer any advantage, and should not be
 * used, unless its "sz" argument is a compile-time constant; therefore, the
 * issue that it evaluates the argument multiple times isn't a problem in
 * practice.
 */
#define palloc0fast(sz) \
	( MemSetTest(0, sz) ? \
		MemoryContextAllocZeroAligned(CurrentMemoryContext, sz) : \
		MemoryContextAllocZero(CurrentMemoryContext, sz) )

/* Higher-limit allocators. */
extern void *MemoryContextAllocHugeInternal(MemoryContext context, Size size, const char* file, int line);
#define MemoryContextAllocHuge(context, size) MemoryContextAllocHugeInternal(context, size, __FILE__, __LINE__)
extern void *MemoryContextAllocHugeDebugInternal(MemoryContext context, Size size, const char* file, int line);
#define MemoryContextAllocHugeDebug(context, size) MemoryContextAllocHugeDebugInternal(context, size, __FILE__, __LINE__)

extern void *repallocHugeInternal(void *pointer, Size size, const char* file, int line);
#define repalloc_huge(pointer, size) repallocHugeInternal(pointer, size, __FILE__, __LINE__);

extern void CheckSubThreadContextInternal(MemoryContext context, const char *func);

#define CheckSubThreadContext(context) \
do	\
{ \
	if (unlikely(am_sub_thread)) \
		CheckSubThreadContextInternal(context, __FUNCTION__);	\
} while(0)

/*
 * Although this header file is nominally backend-only, certain frontend
 * programs like pg_controldata include it via postgres.h.  For some compilers
 * it's necessary to hide the inline definition of MemoryContextSwitchTo in
 * this scenario; hence the #ifndef FRONTEND.
 */

#ifndef FRONTEND
static inline MemoryContext
MemoryContextSwitchTo(MemoryContext context)
{
	MemoryContext old = CurrentMemoryContext;

	CheckSubThreadContext(context);

	CurrentMemoryContext = context;
	return old;
}
#endif							/* FRONTEND */

/* Registration of memory context reset/delete callbacks */
extern void MemoryContextRegisterResetCallback(MemoryContext context,
								   MemoryContextCallback *cb);

/*
 * These are like standard strdup() except the copied string is
 * allocated in a context, not with malloc().
 */
extern char *MemoryContextStrdup(MemoryContext context, const char *string);
extern char *pstrdup(const char *in);
extern char *pnstrdup(const char *in, Size len);

extern char *pchomp(const char *in);

/* sprintf into a palloc'd buffer --- these are in psprintf.c */
extern char *psprintf(const char *fmt,...) pg_attribute_printf(1, 2);
extern size_t pvsnprintf(char *buf, size_t len, const char *fmt, va_list args) pg_attribute_printf(3, 0);

#endif							/* PALLOC_H */
