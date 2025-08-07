/*-------------------------------------------------------------------------
 *
 * fsmfuncs.c
 *	  Functions to investigate FSM pages
 *
 * These functions are restricted to superusers for the fear of introducing
 * security holes if the input checking isn't as water-tight as it should.
 * You'd need to be superuser to obtain a raw page image anyway, so
 * there's hardly any use case for using these without superuser-rights
 * anyway.
 *
 * Copyright (c) 2007-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pageinspect/fsmfuncs.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pageinspect.h"

#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/fsm_internals.h"
#include "utils/builtins.h"

/*
 * Dumps the contents of a FSM page.
 */
PG_FUNCTION_INFO_V1(fsm_page_contents);

Datum
fsm_page_contents(PG_FUNCTION_ARGS)
{
	bytea	   *raw_page = PG_GETARG_BYTEA_P(0);
	StringInfoData sinfo;
	Page		page;
	FSMPage		fsmpage;
	int			i;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use raw page functions"))));
	
	/*
 	 * pageinspect: Fix handling of page sizes and AM types
 	 *
 	 * The page size check was fuzzy in a couple of places, sometimes
 	 * looking after only a sub-range, but what we are looking for is an exact
 	 * match on BLCKSZ.  After considering a few options here, I have settled
 	 * down to do a generalization of get_page_from_raw().  Most of the SQL
 	 * functions already used that, and this is not strictly required if not
 	 * accessing an 8-byte-wide value from a raw page, but this feels safer in
 	 * the long run for alignment-picky environment, particularly if a code
 	 * path begins to access such values.  This also reduces the number of
 	 * strings that need to be translated.
	 */
	page = get_page_from_raw(raw_page);

	/*
 	 * pageinspect: Fix handling of all-zero pages
 	 *
 	 * Getting from get_raw_page() an all-zero page is considered as a valid
 	 * case by the buffer manager and it can happen for example when finding a
 	 * corrupted page with zero_damaged_pages enabled (using zero_damaged_pages
 	 * to look at corrupted pages happens), or after a crash when a relation
 	 * file is extended before any WAL for its new data is generated (before a
 	 * vacuum or autovacuum job comes in to do some cleanup).
 	 *
 	 * However, all the functions of pageinspect, as of the index AMs (except
 	 * hash that has its own idea of new pages), heap, the FSM or the page
 	 * header have never worked with all-zero pages, causing various crashes
 	 * when going through the page internals.
 	 *
 	 * This commit changes all the pageinspect functions to be compliant with
 	 * all-zero pages, where the choice is made to return NULL or no rows for
 	 * SRFs when finding a new page.  get_raw_page() still works the same way,
 	 * returning a batch of zeros in the bytea of the page retrieved.  A hard
 	 * error could be used but NULL, while more invasive, is useful when
 	 * scanning relation files in full to get a batch of results for a single
 	 * relation in one query.  Tests are added for all the code paths
 	 * impacted.
 	 */
	if (PageIsNew(page))
		PG_RETURN_NULL();

	fsmpage = (FSMPage) PageGetContents(page);

	initStringInfo(&sinfo);

	for (i = 0; i < NodesPerPage; i++)
	{
		if (fsmpage->fp_nodes[i] != 0)
			appendStringInfo(&sinfo, "%d: %d\n", i, fsmpage->fp_nodes[i]);
	}
	appendStringInfo(&sinfo, "fp_next_slot: %d\n", fsmpage->fp_next_slot);

	PG_RETURN_TEXT_P(cstring_to_text_with_len(sinfo.data, sinfo.len));
}
