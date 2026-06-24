/*
 *	fe_memutils.h
 *		memory management support for frontend code
 *
 *	Copyright (c) 2003-2017, PostgreSQL Global Development Group
 *
 *	src/include/common/fe_memutils.h
 */
#ifndef FE_MEMUTILS_H
#define FE_MEMUTILS_H

/*
 * Flags for pg_malloc_extended and palloc_extended, deliberately named
 * the same as the backend flags.
 */
#define MCXT_ALLOC_HUGE			0x01	/* allow huge allocation (> 1 GB) not
										 * actually used for frontends */
#define MCXT_ALLOC_NO_OOM		0x02	/* no failure if out-of-memory */
#define MCXT_ALLOC_ZERO			0x04	/* zero allocated memory */

/*
 * "Safe" memory allocation functions --- these exit(1) on failure
 * (except pg_malloc_extended with MCXT_ALLOC_NO_OOM)
 */
extern char *pg_strdup(const char *in);
extern void *pg_malloc(size_t size);
extern void *pg_malloc0(size_t size);
extern void *pg_malloc_extended(size_t size, int flags);
extern void *pg_realloc(void *pointer, size_t size);
extern void pg_free(void *pointer);

/* Equivalent functions, deliberately named the same as backend functions */
extern char *pstrdup(const char *in);
extern void *palloc_internal(Size size, const char* file, int line);
extern void *palloc0_internal(Size size, const char* file, int line);
extern void *palloc_extended_internal(Size size, int flags, const char* file, int line);
extern void *repallocInternal(void *pointer, Size size, const char* file, int line);

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
#define palloc_extended(size, flags) palloc_extended_internal((size), flags, __FILE__, __LINE__)
extern void pfree(void *pointer);
#define repalloc(pointer, size) repallocInternal(pointer, size, __FILE__, __LINE__)

/* sprintf into a palloc'd buffer --- these are in psprintf.c */
extern char *psprintf(const char *fmt,...) pg_attribute_printf(1, 2);
extern size_t pvsnprintf(char *buf, size_t len, const char *fmt, va_list args) pg_attribute_printf(3, 0);

#endif							/* FE_MEMUTILS_H */
