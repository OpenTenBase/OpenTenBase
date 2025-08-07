/*-------------------------------------------------------------------------
 *
 * copy.h
 *	  Definitions for using the POSTGRES copy command.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/copy.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPY_H
#define COPY_H

#include "commands/trigger.h"
#include "executor/execPartition.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "parser/parse_node.h"
#include "tcop/dest.h"
#include "foreign/foreign.h"

#ifdef PGXC
#include "pgxc/remotecopy.h"
#endif

#ifdef __OPENTENBASE_C__
enum SegmentMessageType
{
	ENUM_SEGMENT_MESSAGE_FILE = 0,
	ENUM_SEGMENT_MESSAGE_ATTNO_INFO   = 1,
	ENUM_SEGMENT_MESSAGE_VERSION_INFO = 2
};
#endif

#define MAX_LEN 64
#define DELIM_MAX_LEN 10
#define INVALID_DELIM_CHAR "\\.abcdefghijklmnopqrstuvwxyz0123456789"

typedef int (*copy_data_source_cb) (void *outbuf, int minread, int maxread, void *extra);
typedef bool (*copy_read_line_cb) (void *copystate);

/*
 * Represents the different source/dest cases we need to worry about at
 * the bottom level
 */
typedef enum CopyDest
{
	COPY_FILE,					/* to/from file (or a piped program) */
	COPY_OLD_FE,				/* to/from frontend (2.0 protocol) */
	COPY_NEW_FE,					/* to/from frontend (3.0 protocol) */
#ifdef PGXC
	COPY_BUFFER,				/* Do not send, just prepare */
#endif
	COPY_CALLBACK				/* to/from callback function */
} CopyDest;


/*
 *	Represents the end-of-line terminator type of the input
 */
typedef enum EolType
{
	EOL_UNKNOWN,
	EOL_NL,
	EOL_CR,
	EOL_CRNL,
	EOL_UD
} EolType;

/*
 * The error handling mode for this data load.
 */
typedef enum CopyErrMode
{
    ALL_OR_NOTHING,             /* Either all rows or no rows get loaded (the default) */
    SREH_IGNORE,                /* Sreh - ignore errors (REJECT, but don't log errors) */
    SREH_LOG                    /* Sreh - log errors */
} CopyErrMode;

/*
 * COPY FROM modes (from file/client to table)
 */
typedef enum
{
    COPY_DEFAULT,       /* NORMAL COPY ON COORDINATOR AND DATANODE. */
    COPY_TDX            /* COPY PROCESS FOR TDX. */
} CopyMode;

/*
 * This struct contains all the state variables used throughout a COPY
 * operation. For simplicity, we use the same struct for all variants of COPY,
 * even though some fields are used in only some cases.
 *
 * Multi-byte encodings: all supported client-side encodings encode multi-byte
 * characters by having the first byte's high bit set. Subsequent bytes of the
 * character can have the high bit not set. When scanning data in such an
 * encoding to look for a match to a single-byte (ie ASCII) character, we must
 * use the full pg_encoding_mblen() machinery to skip over multibyte
 * characters, else we might find a false match to a trailing byte. In
 * supported server encodings, there is no possibility of a false match, and
 * it's faster to make useless comparisons to trailing bytes than it is to
 * invoke pg_encoding_mblen() to skip over them. encoding_embeds_ascii is TRUE
 * when we have to do it the hard way.
 */
typedef struct CopyStateData
{
	/* low-level state data */
	CopyDest copy_dest;			/* type of copy source/destination */
	FILE *copy_file;			/* used if copy_dest == COPY_FILE */
	StringInfo fe_msgbuf;		/* used for all dests during COPY TO, only for
								 * dest == COPY_NEW_FE in COPY FROM */
	bool fe_eof;				/* true if detected end of copy data */
	EolType eol_type;			/* EOL type of input */
	int file_encoding;			/* file or remote side's character encoding */
	bool need_transcoding;		/* file encoding diff from server? */
	bool encoding_embeds_ascii; /* ASCII can be non-first byte? */

	/* parameters from the COPY command */
	Relation rel;						/* relation to copy to or from */
	QueryDesc *queryDesc;				/* executable query to copy from */
	List *attnumlist;					/* integer list of attnums to copy */
	List *attinfolist;                  /* name, pos (if exists) list and expr (transformed) list of attr to copy */
	List *attinfolist_sorted;           /* sorted name, pos (if exists) list and expr (transformed) list of attr to copy */
	bool  fixed_position;               /* is pos_spec given to every field? */
	char *filename;						/* filename, or NULL for STDIN/STDOUT */
	bool is_program;					/* is 'filename' a program to popen? */
	copy_data_source_cb data_source_cb; /* function for reading data */
	void *data_source_cb_extra;
	bool binary;						/* binary format? */
	bool oids;							/* include OIDs? */
	bool freeze;						/* freeze rows on loading? */
	bool csv_mode;						/* Comma Separated Value format? */
	bool fixed_mode;
#ifdef __OPENTENBASE__
	bool internal_mode; /* internal format? */
#endif
	bool header_line;			/* CSV header line? */
	char *null_print;			/* NULL marker string (server encoding!) */
	int null_print_len;			/* length of same */
	char *null_print_client;	/* same converted to file encoding */
	char *delim;				/* column delimiter */
	int   delim_len;				/* length of delimiter */
	char *quote;				/* CSV quote char (must be 1 byte) */
	char *escape;				/* CSV escape char (must be 1 byte) */
	char *eol;                  		/* user defined EOL string */
	List *force_quote;			/* list of column names */
	bool force_quote_all;		/* FORCE_QUOTE *? */
	bool *force_quote_flags;	/* per-column CSV FQ flags */
	List *force_notnull;		/* list of column names */
	bool *force_notnull_flags;	/* per-column CSV FNN flags */
	List *force_null;			/* list of column names */
	bool *force_null_flags;		/* per-column CSV FN flags */
	bool convert_selectively;	/* do selective binary conversion? */
	List *convert_select;		/* list of column names (can be NIL) */
	bool *convert_select_flags; /* per-column CSV/TEXT CS flags */

	/* these are just for error messages, see CopyFromErrorCallback */
	const char *cur_relname; /* table name for error messages */
	int cur_lineno;			 /* line number for error messages */
	int cur_quote_lineno;	/* line number for error messages for quoted str */
	int64 cur_bytenum;			 /* bytenum for error messages */
	const char *cur_attname; /* current att for error messages */
	const char *cur_attval;	 /* current att value for error messages */

	/*
	 * Working state for COPY TO/FROM
	 */
	MemoryContext copycontext; /* per-copy execution context */

	/*
	 * Working state for COPY TO
	 */
	FmgrInfo *out_functions;  /* lookup info for output functions */
	MemoryContext rowcontext; /* per-row evaluation context */

	/*
	 * Working state for COPY FROM
	 */
	AttrNumber num_defaults;
	bool file_has_oids;
#ifdef _PG_ORCL_
	bool		file_has_rowids; /* If file has rowid columnt */
	bool		rowids; /* If WITH ROWIDs option */
	FmgrInfo	rid_in_function;
	Oid			rid_typioparam;
	Oid			rid_glob_seqId; /* If a replication with global sequence for RowId */
	StringInfoData	text_line_buf_rid; /* append rowid for text mode */
#endif
	FmgrInfo oid_in_function;
	Oid oid_typioparam;
	FmgrInfo *in_functions; /* array of input functions for each attrs */
	Oid *typioparams;		/* array of element types for in_functions */
	int *defmap;			/* array of default att numbers */
	ExprState **defexprs;	/* array of default att expressions */
	bool volatile_defexprs; /* is any of defexprs volatile? */
	List *range_table;

	/* Tuple-routing support info */
	PartitionTupleRouting *partition_tuple_routing;

	TransitionCaptureState *transition_capture;

	/*
	 * These variables are used to reduce overhead in textual COPY FROM.
	 *
	 * attribute_buf holds the separated, de-escaped text for each field of
	 * the current line.  The CopyReadAttributes functions return arrays of
	 * pointers into this buffer.  We avoid palloc/pfree overhead by re-using
	 * the buffer on each cycle.
	 */
	StringInfoData attribute_buf;

	/* opentenbase_ora compatible feature Copy Position is used to store raw data before encoding conversion */
	StringInfoData attribute_buf_before;

	/* field raw data pointers found by COPY FROM */

	int max_fields;
	char **raw_fields;

	/*
	 * Similarly, line_buf holds the whole input line being processed. The
	 * input cycle is first to read the whole line into line_buf, convert it
	 * to server encoding there, and then extract the individual attribute
	 * fields into attribute_buf.  line_buf is preserved unmodified so that we
	 * can display it in error messages if appropriate.
	 */
	StringInfoData line_buf;
	bool line_buf_converted; /* converted to server encoding? */
	bool line_buf_valid;	 /* contains the row being processed? */

	/*
	 * Finally, raw_buf holds raw data read from the data source (file or
	 * client connection).  CopyReadLine parses this data sufficiently to
	 * locate line boundaries, then transfers the data to line_buf and
	 * converts it.  Note: we guarantee that there is a \0 at
	 * raw_buf[raw_buf_len].
	 */
#define RAW_BUF_SIZE 65536 /* we palloc RAW_BUF_SIZE+1 bytes */
	char *raw_buf;
	int raw_buf_index; /* next byte to process */
	int raw_buf_len;   /* total # of bytes stored */
#ifdef PGXC
	/* Remote COPY state data */
	RemoteCopyData *remoteCopyState;
#endif

	bool	escape_off;		/* treat backslashes as non-special */

#ifdef _SHARDING_
	Bitmapset *shard_array;
#endif
#ifdef __OPENTENBASE__
	bool insert_into;
	int data_index;
	char ***data_list;
	int data_ncolumns;
	int ndatarows;
	bool whole_line;
#endif
	/* Error handling options */
	SingleRowErrorDesc *sred;
	CopyErrMode        errMode;
	struct Srehandler  *srehandler;         /* single row error handler */
	CopyMode copy_mode;

	char *date_format;          /* user-defined date format */
	char *time_format;          /* user-defined time format */
	char *timestamp_format;     /* user-defined timestamp format */
	char *timestamp_tz_format;  /* user-defined timestamp tz format */
	bool	fill_missing;		/* missing attrs at end of line are NULL */
	bool	ignore_extra_data;	/* ignore extra column at end of line_buf */
	bool	compatible_illegal_chars;
	
	/* kafka streaming via tdx */
	List    *l_partition_offset_pairs;  /* prt and off get from local exttable_fdw.offsets, which would be sent to tdx. */
	List    *n_partition_offset_pairs;  /* prt and off get from tdx server, which would be used to updated exttable_fdw.offsets.  */
	bool    is_logical_woker;           /* TRUE: message is DML log; FALSE: text/csv raw data(reserved). 
										 * LoadFromStmt can only be used to synchronize stream data of DML log type. */
	/* DN task info when import from hdfs_fdw */
	List *taskList;
	ListCell *curTaskPtr;	/* taskList listCell */
	void *curTaskFileHandle; /* file handle related to curTaskPtr, eg: hdfsFile */
	void *server_opt;		/* server option of read data, eg: hdfs_opt */
	void *server_connection;		/* server connection of read data, eg: hdfsFS | ossContext */
	copy_read_line_cb next_task_cb; /* function for moving to next task file */
	List *partitions_list;	/* hive partition column name list */
	char *hive_partition_dm[MAX_HIVE_PARTITION];
	bool hive_partition_default[MAX_HIVE_PARTITION];

	/* when exporting used by hdfs_fdw */
	StringInfo outBuffer;
	uint64 copyToTotalSize;
	StringInfo export_filename;
	int export_segment_num;
	char *illegal_conv_chars;
	List *formatter;
	size_t formatter_size;
	bool formatter_with_cut;
	ExprState **attexprs;
	ParamListInfo param_list;
} CopyStateData;

typedef struct PartitionOffsetPair
{
	int32_t partition;
	int64_t offset;
} PartitionOffsetPair;

/* CopyStateData is private in commands/copy.c */
typedef struct CopyStateData *CopyState;

extern void DoCopy(ParseState *state, const CopyStmt *stmt,
	   int stmt_location, int stmt_len,
	   uint64 *processed);

extern void ProcessCopyOptions(ParseState *pstate, CopyState cstate, bool is_from, List *options);
extern CopyState BeginCopyFrom(ParseState *pstate, Relation rel, const char *filename,
			  bool is_program, copy_data_source_cb data_source_cb,
			  void *data_source_cb_extra, List *attnamelist, List *attinfolist, List *options, bool fixed_position);
extern CopyState BeginCopyToForeignTable(Relation forrel, List *options);
extern void CopyFixedHeaderTo(CopyState cstate);
extern void CopyOneRowFixedTo(CopyState cstate, Oid tupleOid, Datum *values, bool *nulls
);
extern void CopyOneRowTo(CopyState cstate, Oid tupleOid, Datum *values, bool *nulls
);
extern void CopySendEndOfRow(CopyState cstate);
extern void EndCopyFrom(CopyState cstate);
extern void EndCopyTo(CopyState cstate);
extern bool NextCopyFrom(CopyState cstate, EState *estate,
			 Datum *values, bool *nulls, Oid *tupleOid
#ifdef _PG_ORCL_
			 , RowId *rowid
#endif
			 );
extern bool CopyLoadRawBuf(CopyState cstate);
extern bool NextCopyFromRawFields(CopyState cstate,
					  char ***fields, int *nfields);
extern void CopyFromErrorCallback(void *arg);

extern uint64 CopyFrom(CopyState cstate);

extern DestReceiver *CreateCopyDestReceiver(void);

extern char *limit_printout_length(const char *str);
extern void truncateEol(StringInfo buf, EolType	eol_type);
extern void truncateEolStr(char *str, EolType eol_type);
extern Datum InputFunctionCallForBulkload(CopyState cstate, FmgrInfo *flinfo, char *str, Oid typioparam, int32 typmod);
extern bool tdx_illegal_chars_conversion;
#endif							/* COPY_H */
