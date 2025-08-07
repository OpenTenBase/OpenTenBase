/*-------------------------------------------------------------------------
 *
 * external.h
 *	  routines for getting external info from external table foreign table.
 *
 * src/include/access/external.h
*
*-------------------------------------------------------------------------
*/
#ifndef EXTERNAL_H
#define EXTERNAL_H

#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "access/heapam.h"
#include "access/url.h"
#include "utils/uri.h"

#define fmttype_is_custom(c) (c == 'b')
#define fmttype_is_text(c)   (c == 't')
#define fmttype_is_csv(c)    (c == 'c')

/*
 * Descriptor of a single external relation.
 * For now very similar to the catalog row itself but may change in time.
 */
typedef struct ExtTableEntry
{
	List*	urilocations;
	List*	execlocations;
	char	fmtcode;
	List*	options;
	char*	command;
	int		rejectlimit;
	char	rejectlimittype;
	char	logerrors;
    int		encoding;
    bool	iswritable;
    bool	isweb;		/* extra state, not cataloged */
} ExtTableEntry;

/*
 * used for scan of external relations with the file protocol
 */
typedef struct FileScanDescData
{
	/* scan parameters */
	Relation	fs_rd;			/* target relation descriptor */
	URL_FILE   **fs_file;		/* the file pointer to our URI */
	char	   **fs_uri;		/* the URI string */
	bool		fs_noop;		/* no op. this segdb has no file to scan */
	uint32      fs_scancounter;	/* copied from struct ExternalScan in plan */
	int         fs_num;         /* the file number of scan */
	int         fs_current;     /* the file index of scan */
	Bitmapset   *fs_bitmap;
	
	/* current file parse state */
	struct CopyStateData *fs_pstate;
	
	AttrNumber	num_phys_attrs;
	Datum	   *values;
	bool	   *nulls;
	FmgrInfo   *in_functions;
	Oid		   *typioparams;
	Oid			in_func_oid;
	
	/* current file scan state */
	TupleDesc	fs_tupDesc;
	HeapTupleData fs_ctup;		/* current tuple in scan, if any */
	
	/* custom data formatter */
	FmgrInfo   *fs_custom_formatter_func; /* function to convert to custom format */
	List	   *fs_custom_formatter_params; /* list of defelems that hold user's format parameters */
	
	/* CHECK constraints and partition check quals, if any */
	bool		fs_hasConstraints;
	ExprState **fs_constraintExprs;
	bool		fs_isPartition;
	ExprState  *fs_partitionCheckExpr;
	
	/* target relation locator info if needed. */
	RelationLocInfo *locator_info;
}	FileScanDescData;

typedef FileScanDescData *FileScanDesc;

typedef struct TdxSyncDescData
{
	List *options;
	Oid ext_relid;
	Relation rel;
	char *nspname;
	char *relname;
	FileScanDesc scan;
	MemoryContext syncContext;
	bool updatable;
} TdxSyncDescData;

typedef TdxSyncDescData *TdxSyncDesc;

extern List * TokenizeLocationUris(char *locations);

extern ExtTableEntry *GetExtTableEntry(Oid relid);
extern ExtTableEntry *GetExtTableEntryIfExists(Oid relid);
extern ExtTableEntry *GetExtFromForeignTableOptions(List *ftoptons, Oid relid);

extern int external_getdata_callback(void *outbuf, int minread, int maxread, void *extra);
extern void open_external_readable_source(FileScanDesc scan, ExternalSelectDesc desc);

extern void exttableGetOptions(Oid foreigntableid, FileScanDesc scan, List **extoptions, List **attinfolist);
extern void ApplyMsgFromStreamOnDN(LoadFromStmt *stmt);

extern int tdx_external_max_segs;
#endif   /* EXTERNAL_H */