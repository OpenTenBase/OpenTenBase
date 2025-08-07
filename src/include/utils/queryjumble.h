/*-------------------------------------------------------------------------
 *
 * queryjumble.h
 *	  Query normalization and fingerprinting.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/include/utils/queryjumble.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef QUERYJUBLE_H
#define QUERYJUBLE_H

#include "nodes/parsenodes.h"

#define JUMBLE_SIZE				1024	/* query serialization buffer size */

/*
 * Struct for tracking locations/lengths of constants during normalization
 */
typedef struct LocationLen
{
	int			location;		/* start offset in query text */
	int			length;			/* length in bytes, or -1 to ignore */
} LocationLen;

/*
 * Working state for computing a query jumble and producing a normalized
 * query string
 */
typedef struct JumbleState
{
	/* Jumble of current query tree */
	unsigned char *jumble;

	/* Number of bytes used in jumble[] */
	Size		jumble_len;

	/* Array of locations of constants that should be removed */
	LocationLen *clocations;

	/* Allocated length of clocations array */
	int			clocations_buf_size;

	/* Current number of valid entries in clocations array */
	int			clocations_count;

	/* highest Param id we've seen, in order to start normalization correctly */
	int            highest_extern_param_id;

	List          *param_list;
	Bitmapset     *paramids;
} JumbleState;

extern const char *CleanQuerytext(const char *query, int *location, int *len);
extern JumbleState *JumbleQuery(Query *query, const char *querytext);
extern char *generate_normalized_query(JumbleState *jstate, const char *query,
						  int query_loc, int *query_len_p, bool use_param_no);
extern void         AppendJumble(JumbleState *jstate, const unsigned char *item, Size size);

#endif							/* QUERYJUMBLE_H */
