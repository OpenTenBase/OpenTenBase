/*-------------------------------------------------------------------------
 *
 * remotecopy.h
 *		Routines for extension of COPY command for cluster management
 *
 *
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *		src/include/pgxc/remotecopy.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REMOTECOPY_H
#define REMOTECOPY_H

#include "nodes/parsenodes.h"
#include "pgxc/locator.h"

/*
 * This contains the set of data necessary for remote COPY control.
 */
typedef struct RemoteCopyData {
	/* COPY FROM/TO? */
	bool		is_from;

	/*
	 * On Coordinator we need to rewrite query.
	 * While client may submit a copy command dealing with file, Datanodes
	 * always send/receive data to/from the Coordinator. So we can not use
	 * original statement and should rewrite statement, specifing STDIN/STDOUT
	 * as copy source or destination
	 */
	StringInfoData query_buf;
	Locator			*locator;		/* the locator object */
	int             n_dist_types;
	Oid            *dist_types;		/* data type of the distribution column */

	/* Locator information */
	RelationLocInfo *rel_loc;		/* the locator key */
} RemoteCopyData;

/*
 * List of all the options used for query deparse step
 * As CopyStateData stays private in copy.c and in order not to
 * make Postgres-XC code too much intrusive in PostgreSQL code,
 * this intermediate structure is used primarily to generate remote
 * COPY queries based on deparsed options.
 */
typedef struct RemoteCopyOptions {
	bool		rco_binary;			/* binary format? */
	bool		rco_oids;			/* include OIDs? */
#ifdef _PG_ORCL_
	bool		rco_rowids;
#endif
	bool		rco_csv_mode;		/* Comma Separated Value format? */
	bool		rco_fixed_mode;
	int		    rco_eol_type;
	char	   *rco_eol;		/* user defined EOL string */
#ifdef __OPENTENBASE__
	bool        rco_insert_into;
#endif
	char	   *rco_delim;			/* column delimiter (must be 1 byte) */
	char	   *rco_null_print;		/* NULL marker string (server encoding!) */
	char	   *rco_quote;			/* CSV quote char (must be 1 byte) */
	char	   *rco_escape;			/* CSV escape char (must be 1 byte) */
	List	   *rco_force_quote;	/* list of column names */
	bool        rco_force_quote_all; /* FORCE_QUOTE *? */
	List	   *rco_force_notnull;	/* list of column names */
	List	   *rco_force_null;     /* list of column names */
	int        file_encoding;       /* file or remote side's character encoding */
	bool        fill_missing;        /* missing attrs at end of line are NULL */
	bool        ignore_extra_data;   /* ignore extra column at end of line_buf */
	bool        compatible_illegal_chars;
	char       *illegal_conv_chars;
	List       *formatter;
	bool        formatter_with_cut;
} RemoteCopyOptions;

extern void RemoteCopy_BuildStatement(RemoteCopyData *state,
									  Relation rel,
									  RemoteCopyOptions *options,
									  List *attnamelist,
									  List *attinfolist,
									  List *attnums,
#ifdef _SHARDING_
									  const Bitmapset *shards
#endif
);
extern void RemoteCopy_GetRelationLoc(RemoteCopyData *state,
									  Relation rel,
									  List *attnums);
extern RemoteCopyOptions *makeRemoteCopyOptions(void);
extern void FreeRemoteCopyData(RemoteCopyData *state);
extern void FreeRemoteCopyOptions(RemoteCopyOptions *options);
#endif
