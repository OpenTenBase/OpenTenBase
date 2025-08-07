/*-------------------------------------------------------------------------
 *
 * copysreh.h
 *	  routines for single row error handling.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/copysreh.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef COPYSREH_H
#define COPYSREH_H

#include "fmgr.h"
#include "commands/copy.h"
#include "utils/memutils.h"
#include "utils/pg_crc.h"

#define ErrorLogDir "errlog"
#define PersistentErrorLogDir "errlogpersistent"

#define ErrorLogNormalFileName(fname, dbId, relId) \
        snprintf(fname, MAXPGPATH, "%s/%u_%u", ErrorLogDir, dbId, relId)

#define ErrorLogPersistentFileName(fname, dbId, namespaceId, relName) \
        snprintf(fname, MAXPGPATH, "%s/%u_%u_%s", PersistentErrorLogDir, dbId, namespaceId, relName)

#define NUM_ERRORTABLE_ATTR 9
#define errtable_cmdtime 1
#define errtable_relname 2
#define errtable_filename 3
#define errtable_linenum 4
#define errtable_bytenum 5
#define errtable_errmsg 6
#define errtable_rawdata 7
#define errtable_rawbyte 8
#define errtable_nodename 9


/*
 * All the Single Row Error Handling state is kept here.
 * When an error happens and we are in single row error handling
 * mode this struct is updated and handed to the single row
 * error handling manager.
 */
typedef struct Srehandler
{
    /* bad row information */
    char       *errmsg;                 /* the error message for this bad data row */
    StringInfo rawdata;                 /* the bad data row which may contain \0 inside */
    char       *relname;                /* target relation */
    int64      linenumber;              /* line number of error in original file */
    int64      bytenum;                 /* offset number of error in original file */
    uint64     processed;               /* num logical input rows processed so far */
    bool       is_server_enc;           /* was bad row converted to server encoding? */
    char       nodename[NAMEDATALEN];   /* node which the bad row was processed on */
    
    /* reject limit state */
    int  rejectlimit;                   /* SEGMENT REJECT LIMIT value */
    int64  rejectcount;                   /* how many were rejected so far */
    bool is_limit_in_rows;              /* ROWS = true, PERCENT = false */
    
    MemoryContext badrowcontext;        /* per-badrow evaluation context */
    char          filename[MAXPGPATH];  /* "uri [filename]" */
    
    bool log_to_file;                   /* if log into file? */
    bool error_log_persistent;          /* persistent error table, when drop the external table, the error log not get dropped */
    Oid  relid;                         /* parent relation id */
} Srehandler;


extern int copy_reject_percent_threshold;       /* Reject limit in percent starts calculating after this number of rows processed */
extern int copy_initial_bad_row_limit;          /* Stops processing when number of the first bad rows exceeding this value */

extern void ReportSrehResults(Srehandler *srehandler);

extern void destroyCopySrehandler(Srehandler *srehandler);

extern void HandleSingleRowError(Srehandler *srehandler);

extern bool ExceedSegmentRejectHardLimit(Srehandler *srehandler);

extern void ErrorIfRejectLimitReached(Srehandler *srehandler);

extern void VerifyRejectLimit(char rejectlimittype, int rejectlimit);

extern bool ErrorLogDelete(Oid databaseId, Oid relationId);

extern bool PersistentErrorLogDelete(Oid databaseId, Oid namespaceId, const char *fname);

extern bool RetrievePersistentErrorLogFromRangeVar(RangeVar *relrv, AclMode mode, char *fname /*out*/);

extern bool TruncateErrorLog(text *relname, bool persistent);

extern HeapTuple ReadValidErrorLogDatum(FILE *fp, const char *fname);

extern Srehandler *makeSrehandler(int rejectlimit, bool is_limit_in_rows,
                                  char *filename, char *relname,
                                  char log_type);

extern void ErrorLogFileName(Oid dbid, Oid relid, bool persistent, char *fname /* out */);

extern TupleDesc GetErrorTupleDesc(void);
#endif                          /* COPYSREH_H */


