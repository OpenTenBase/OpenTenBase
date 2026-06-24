/*----------------------------------------------------------------------------
 *
 *     dbms_lob.c
 *     Adapt opentenbase_ora's dbms_lob package
 *
 *----------------------------------------------------------------------------
 */

#include "postgres.h"
#include <ctype.h>
#include <limits.h>
#include "access/hash.h"
#include "utils/bytea.h"
#include "utils/varlena.h"
#include "utils/syscache.h"
#include "utils/memutils.h"
#include "fmgr.h"
#include "builtins.h"
#include "utils/builtins.h"
#include "catalog/directory.h"
#include "access/htup_details.h"
#include "mb/pg_wchar.h"
#include <sys/stat.h>

#define DBMS_LOB_LOBMAXSIZE (0x7fffffff)
#define DBMS_LOB_LOBSTARTOFFSET (1)
#define DBMS_LOB_SUBSTR_MAX_LEN (32767)

PG_FUNCTION_INFO_V1(dbms_lob_compare_blob);
PG_FUNCTION_INFO_V1(dbms_lob_compare_clob);
PG_FUNCTION_INFO_V1(dbms_lob_open_bfile);
PG_FUNCTION_INFO_V1(dbms_lob_getlength);
PG_FUNCTION_INFO_V1(dbms_lob_read);
PG_FUNCTION_INFO_V1(dbms_lob_close);
PG_FUNCTION_INFO_V1(dbms_lob_closeall);
PG_FUNCTION_INFO_V1(dbms_lob_substr_blob);
PG_FUNCTION_INFO_V1(dbms_lob_substr_clob);
PG_FUNCTION_INFO_V1(dbms_lob_substr_bfile);
PG_FUNCTION_INFO_V1(dbms_lob_blob_substr);
PG_FUNCTION_INFO_V1(dbms_lob_clob_substr);

#define DBMS_LOB_READONLY "dbms_lob.lob_readonly"
#define BFILE_MAX_LEN (0xffffffff)

const int BFILE_DIR_INDEX = 1;
const int BFILE_FILE_INDEX = 3;
const int MAX_LINESIZE = 32767;

/* File does not exist, or you do not have access privileges on the file. */
#define INVALID_OPERATION		"INVALID_OPERATION"
/* Directory does not exist. */
#define NOEXIST_DIRECTORY		"NOEXIST_DIRECTORY"
/* You do not have privileges for the directory. */
#define NOPRIV_DIRECTORY		"NOPRIV_DIRECTORY"
/* Directory has been invalidated after the file was opened. */
#define INVALID_DIRECTORY		"INVALID_DIRECTORY"
/* Directory and file path too long. */
#define DIRLENTH_TOO_LONG		"INVALID_DIRECTORY"
/* File is not opened using the input locator. */
#define UNOPENED_FILE			"UNOPENED_FILE"
/* Open mode is invalid*/
#define INVALID_OPEN_MODE		"INVALID_OPEN_MODE"

#define MAX_SLOTS		50			/* OpenTenBase_Ora 10g supports 50 files */
#define INVALID_SLOTID	-1			/* invalid slot id */

#define CUSTOM_EXCEPTION(msg, detail) \
	ereport(ERROR, \
		(errcode(ERRCODE_RAISE_EXCEPTION), \
		 errmsg("%s", msg), \
		 errdetail("%s", detail)))

#define STRERROR_EXCEPTION(msg) \
	do { char *strerr = strerror(errno); CUSTOM_EXCEPTION(msg, strerr); } while(0);

typedef struct FileSlot
{
	FILE	*file;
	char	*lobname;
	int		lobname_len;
} FileSlot;

static FileSlot	*slots = NULL;	/* initilaized with zeros */

static int open_bfile(char *lobname, char *modetype);
static int get_dbms_lob_slot(char *lobname);
static void bfile_close(int slotidx, char *lobname);

static void
IO_EXCEPTION(void)
{
	switch (errno)
	{
		case EACCES:
			STRERROR_EXCEPTION(INVALID_DIRECTORY);
			break;
		case ENAMETOOLONG:
			STRERROR_EXCEPTION(DIRLENTH_TOO_LONG);
		case ENOENT:
		case ENOTDIR:
			STRERROR_EXCEPTION(INVALID_DIRECTORY);
			break;

		default:
			STRERROR_EXCEPTION(INVALID_OPERATION);
	}
}

/*
 * get_descriptor(FILE *file) find any free slot for FILE pointer.
 * If isn't realloc array slots and add 32 new free slots.
 *
 */
static int
get_descriptor(FILE *file, const char *lobname)
{
	int i;

	if (slots == NULL)
		slots = (FileSlot *) MemoryContextAlloc(TopMemoryContext,
												sizeof(FileSlot) * MAX_SLOTS);

	for (i = 0; i < MAX_SLOTS; i++)
	{
		if (slots[i].file == NULL)
		{
			int len = strlen(lobname) + 1;
			slots[i].file = file;
			slots[i].lobname_len = len;
			slots[i].lobname = (char *)MemoryContextAlloc(TopMemoryContext, len);
			snprintf(slots[i].lobname, len, "%s", lobname);
			return i;
		}
	}

	return INVALID_SLOTID;
}

Datum
dbms_lob_blob_substr(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	return DirectFunctionCall3(bytea_substr,
								PG_GETARG_DATUM(0),
								PG_GETARG_DATUM(2),
								PG_GETARG_DATUM(1));
}

Datum
dbms_lob_clob_substr(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	return DirectFunctionCall3(text_substr,
								PG_GETARG_DATUM(0),
								PG_GETARG_DATUM(2),
								PG_GETARG_DATUM(1));
}

/*
*INTEGER: 0 if the comparison succeeds, nonzero if not.
*NULL, if any of amount, offset_1 or offset_2 is not a valid LOB offset value. 
*		A valid offset is within the range of 1 to LOBMAXSIZE inclusive.
*/
Datum
dbms_lob_compare_blob(PG_FUNCTION_ARGS)
{
	bytea	*lob_1 = NULL;
	bytea	*lob_2 = NULL;
	int32 	amount = DBMS_LOB_LOBMAXSIZE;
	int32	offset_1 = DBMS_LOB_LOBSTARTOFFSET;
	int32	offset_2 = DBMS_LOB_LOBSTARTOFFSET;
	int32	lob_1_len = 0;
	int32	lob_2_len = 0;
	int32	len_1 = 0;
	int32	len_2 = 0;
	int		cmp;
	char		*lob_1_data = NULL;
	char 	*lob_2_data = NULL;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("invalid input parameter lob_1 or lob_2")));

	lob_1 = PG_GETARG_BYTEA_P(0);
	lob_2 = PG_GETARG_BYTEA_P(1);

	if  (!PG_ARGISNULL(2))
		amount = PG_GETARG_INT32(2);

	if  (!PG_ARGISNULL(3))
		offset_1 = PG_GETARG_INT32(3);

	if  (!PG_ARGISNULL(4))
		offset_2 = PG_GETARG_INT32(4);

	if ((amount > DBMS_LOB_LOBMAXSIZE) 
		|| (amount < DBMS_LOB_LOBSTARTOFFSET))
	{
		elog(DEBUG3, "The length of data compared is invalid, length=%d", amount);
		PG_RETURN_NULL();			
	}

	lob_1_len = VARSIZE_ANY_EXHDR(lob_1);
	lob_2_len = VARSIZE_ANY_EXHDR(lob_2);

	if ((offset_1 > DBMS_LOB_LOBMAXSIZE) 
		|| (offset_1 < DBMS_LOB_LOBSTARTOFFSET))
	{
		elog(DEBUG3, "Lob1's offset is invalid, offset_1=%d", offset_1);
		PG_RETURN_NULL();			
	}

	if ((offset_2 > DBMS_LOB_LOBMAXSIZE) 
		|| (offset_2 < DBMS_LOB_LOBSTARTOFFSET))
	{
		elog(DEBUG3, "Lob2's offset is invalid, offset_2=%d", offset_2);
		PG_RETURN_NULL();			
	}

	offset_1 -= 1;
	offset_2 -= 1;
	len_1 = lob_1_len - offset_1;
	len_2 = lob_2_len - offset_2;

	/*elog(INFO, "len_1=%d, len_2=%d, lob_1_len=%d, lob_2_len=%d, offset_1=%d, offset_2=%d", 
		len_1, len_2, lob_1_len, lob_2_len, offset_1, offset_2);*/

	if ((len_1 <= 0) && (len_2 <= 0))	
	{
		if (lob_1_len == 0 && lob_2_len == 0)
			PG_RETURN_INT32(0);	
		else
			PG_RETURN_NULL();
	}
	
	if ((len_1 <= 0) && (len_2 > 0))
		PG_RETURN_INT32(-1);

	if ((len_1 > 0) && (len_2 <= 0))
		PG_RETURN_INT32(1);

	lob_1_data = VARDATA_ANY(lob_1);
	lob_2_data = VARDATA_ANY(lob_2);
	lob_1_data += offset_1;
	lob_2_data += offset_2;
	
	cmp = memcmp(lob_1_data, lob_2_data, Min(amount, Min(len_1, len_2)));
	if (cmp == 0) 
	{
		if ((amount >= Max(len_1, len_2)) && (len_1 != len_2))
			cmp = (len_1 < len_2) ? -1 : 1;
	}
	
	PG_FREE_IF_COPY(lob_1, 0);
	PG_FREE_IF_COPY(lob_2, 1);

	PG_RETURN_INT32(cmp);
}

Datum
dbms_lob_compare_clob(PG_FUNCTION_ARGS)
{
	text		*lob_1 = NULL;
	text		*lob_2 = NULL;
	int32 	amount = DBMS_LOB_LOBMAXSIZE;
	int32	offset_1 = DBMS_LOB_LOBSTARTOFFSET;
	int32	offset_2 = DBMS_LOB_LOBSTARTOFFSET;
	int32	lob_1_len = 0;
	int32	lob_2_len = 0;
	int32	len_1 = 0;
	int32	len_2 = 0;
	int		cmp;
	char		*lob_1_data = NULL;
	char 	*lob_2_data = NULL;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("invalid input parameter lob_1 or lob_2")));

	lob_1 = PG_GETARG_TEXT_P(0);
	lob_2 = PG_GETARG_TEXT_P(1);

	if  (!PG_ARGISNULL(2))
		amount = PG_GETARG_INT32(2);

	if  (!PG_ARGISNULL(3))
		offset_1 = PG_GETARG_INT32(3);

	if  (!PG_ARGISNULL(4))
		offset_2 = PG_GETARG_INT32(4);

	if ((amount > DBMS_LOB_LOBMAXSIZE) 
		|| (amount < DBMS_LOB_LOBSTARTOFFSET))
	{
		elog(DEBUG3, "The length of data compared is invalid, length=%d", amount);
		PG_RETURN_NULL();			
	}

	lob_1_len = VARSIZE_ANY_EXHDR(lob_1);
	lob_2_len = VARSIZE_ANY_EXHDR(lob_2);

	if ((offset_1 > DBMS_LOB_LOBMAXSIZE) 
		|| (offset_1 < DBMS_LOB_LOBSTARTOFFSET))
	{
		elog(DEBUG3, "Lob1's offset is invalid, offset_1=%d", offset_1);
		PG_RETURN_NULL();			
	}

	if ((offset_2 > DBMS_LOB_LOBMAXSIZE) 
		|| (offset_2 < DBMS_LOB_LOBSTARTOFFSET))
	{
		elog(DEBUG3, "Lob2's offset is invalid, offset_2=%d", offset_2);
		PG_RETURN_NULL();			
	}

	offset_1 -= 1;
	offset_2 -= 1;
	len_1 = lob_1_len - offset_1;
	len_2 = lob_2_len - offset_2;

	/*elog(INFO, "len_1=%d, len_2=%d, lob_1_len=%d, lob_2_len=%d, offset_1=%d, offset_2=%d", 
		len_1, len_2, lob_1_len, lob_2_len, offset_1, offset_2);*/

	if ((len_1 <= 0) && (len_2 <= 0))	
	{
		if (lob_1_len == 0 && lob_2_len == 0)
			PG_RETURN_INT32(0);	
		else
			PG_RETURN_NULL();
	}
	
	if ((len_1 <= 0) && (len_2 > 0))
		PG_RETURN_INT32(-1);

	if ((len_1 > 0) && (len_2 <= 0))
		PG_RETURN_INT32(1);

	lob_1_data = VARDATA_ANY(lob_1);
	lob_2_data = VARDATA_ANY(lob_2);
	lob_1_data += offset_1;
	lob_2_data += offset_2;
	
	cmp = memcmp(lob_1_data, lob_2_data, Min(amount, Min(len_1, len_2)));
	if (cmp == 0) 
	{
		if ((amount >= Max(len_1, len_2)) && (len_1 != len_2))
			cmp = (len_1 < len_2) ? -1 : 1;
	}
	
	PG_FREE_IF_COPY(lob_1, 0);
	PG_FREE_IF_COPY(lob_2, 1);

	PG_RETURN_INT32(cmp);
}

/*
 * split dirname and filename
 * lobname format: "bfilename('dirname', 'filename')"
 * elemlist store the split results, elemlist[1]:dirname, elemlist[3]:filename
 */
static void
split_bfilename(char *lobname, char **dirname, char **filename)
{
#define BFILE_NAME_LEN_BY_SEMICOLON 5

	List		*elemlist = NIL;
	ListCell	*l;
	int			i = 0;

	if (!SplitDirectoriesString(lobname, '\'', &elemlist))
		elog(ERROR, "bfile: %s in wrong fromat", lobname);
	if (list_length(elemlist) != BFILE_NAME_LEN_BY_SEMICOLON)
		elog(ERROR, "bfile: %s in wrong fromat", lobname);

	foreach(l, elemlist)
	{
		if (i == BFILE_DIR_INDEX)
			*dirname = (char *) lfirst(l);
		else if (i == BFILE_FILE_INDEX)
			*filename = (char *) lfirst(l);
		i++;
	}
}

static char*
get_bfile_fullname(char *lobname)
{
	char	*dirname = NULL;
	char	*upper_dirname = NULL;
	char	*filename = NULL;
	char	*pathname = NULL;
	char	*fullname = NULL;
	char	*nodename = NULL;
	int		fixlen = 1;
	int		fullname_len = 0;
	int		i;
	Datum   pathdatum;
	bool	isnull;
	HeapTuple	tuple;
	Form_pg_directory	pgdir;

	split_bfilename(pstrdup(lobname), &dirname, &filename);

	upper_dirname = (char *)palloc(strlen(dirname) + 1);
	/* convert dirname to upper letter */
	for (i = 0; i < strlen(dirname); i++)
	{
		upper_dirname[i] = toupper((dirname)[i]);
	}
	upper_dirname[i] = '\0';

	/* Check if dirname exists in pg_directory */
	tuple = SearchSysCache1(BFILEDIRECTORY, CStringGetDatum(upper_dirname));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "could not find tuple for directory %s in pg_directory", dirname);
	pfree(upper_dirname);
	upper_dirname = NULL;

	pathdatum = SysCacheGetAttr(BFILEDIRECTORY,
								tuple,
								Anum_directory_path,
								&isnull);
	pathname = TextDatumGetCString(pathdatum);
	pgdir = (Form_pg_directory) GETSTRUCT(tuple);
	nodename = (pgdir->nodename).data;

	if (strcmp(nodename, PGXCNodeName) != 0)
		elog(ERROR, "%s in %s, not in %s", dirname, nodename, PGXCNodeName);
	ReleaseSysCache(tuple);

	fullname_len = strlen(pathname) + strlen(filename) + fixlen + 1;
	fullname = (char*)palloc(fullname_len);
	snprintf(fullname, fullname_len, "%s/%s", pathname, filename);
	canonicalize_path(fullname);

	return fullname;
}

/* 
 * open file, store file descriptor in global slots,
 * and return slots index.
 */
static int
open_bfile(char *lobname, char *modetype)
{
	char	*fullname = NULL;
	FILE	*file;
	int		slotidx;

	fullname = get_bfile_fullname(lobname);
	file = fopen(fullname, modetype);
	if (!file)
		IO_EXCEPTION();

	slotidx = get_descriptor(file, lobname);
	if (slotidx == INVALID_SLOTID)
	{
		fclose(file);
		ereport(ERROR,
		    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
		     errmsg("program limit exceeded"),
		     errdetail("Too much concurent opened files"),
		     errhint("You can only open a maximum of ten files for each session")));
	}

	pfree(fullname);
	return slotidx;
}

Datum
dbms_lob_open_bfile(PG_FUNCTION_ARGS)
{
	char	*lobname;
	text	*mode;
	int		slotidx;

	lobname = PG_GETARG_BFILE(0);
	mode = PG_GETARG_TEXT_P(1);

	if (lobname == NULL)
		elog(ERROR, "bfile should not be empty");
	if (mode == NULL)
		elog(ERROR, "mode should not be empty");

	if (VARSIZE(mode) - VARHDRSZ != 1)
		CUSTOM_EXCEPTION(INVALID_OPEN_MODE, "open mode is different than [R,W,A]");

	switch (*((char*)VARDATA(mode)))
	{
		case 'r':
		case 'R':
			break;
		default:
			CUSTOM_EXCEPTION(INVALID_OPEN_MODE,
							"bfile open mode is different than [R,r]");
	}

	slotidx = get_dbms_lob_slot(lobname);
	if (slotidx == INVALID_SLOTID)
	{
		open_bfile(lobname, PG_BINARY_R);
	}
	else
		elog(DEBUG1, "bfile: %s already opend.", lobname);

	PG_RETURN_NULL();
}

/*
 * return slot index that store lobfile's descriptor.
 */
static int
get_dbms_lob_slot(char *lobname)
{
	int	i;
	if (slots == NULL)
	{
		slots = (FileSlot *)MemoryContextAllocZero(TopMemoryContext, sizeof(FileSlot) * MAX_SLOTS);
		return INVALID_SLOTID;
	}

	for (i = 0; i < MAX_SLOTS; i++)
	{
		if (slots[i].lobname &&	strcmp(slots[i].lobname, lobname) == 0 &&
			slots[i].file != NULL)
		{
			return i;
		}
	}
	return INVALID_SLOTID;
}

Datum
dbms_lob_getlength(PG_FUNCTION_ARGS)
{
	char	*fullname;
	int64 	filelen;
	struct stat fst;
	char	*lobname = PG_GETARG_BFILE(0);

	if (lobname == NULL)
		elog(ERROR, "bfile should not be empty");

	fullname = get_bfile_fullname(lobname);
	if (stat(fullname, &fst) < 0)
	{
		if (errno == ENOENT)
			PG_RETURN_NULL();
		else
			elog(ERROR, "could not stat bfile: %s", lobname);
	}
	filelen = (int64) fst.st_size;
	pfree(fullname);
	PG_RETURN_INT64(filelen);
}

static void
bfile_close(int slotidx, char *lobname)
{
	FILE *fp = slots[slotidx].file;

	if (fclose(fp) != 0)
	{
		elog(ERROR, "bfile: %s close failed", lobname);
	}
	slots[slotidx].file = NULL;
	pfree(slots[slotidx].lobname);
	slots[slotidx].lobname = NULL;
	slots[slotidx].lobname_len = 0;
}

Datum
dbms_lob_close(PG_FUNCTION_ARGS)
{
	int		slotidx = INVALID_SLOTID;
	char	*lobname = PG_GETARG_BFILE(0);

	if (lobname == NULL)
		elog(ERROR, "bfile should not be empty");

	slotidx = get_dbms_lob_slot(lobname);
	if (slotidx == INVALID_SLOTID)
	{
		elog(DEBUG1, "bfile: %s has not opend, no need close", lobname);
		PG_RETURN_NULL();
	}

	bfile_close(slotidx, lobname);

	PG_RETURN_NULL();
}

Datum
dbms_lob_closeall(PG_FUNCTION_ARGS)
{
	int	i;
	if (slots == NULL)
	{
		STRERROR_EXCEPTION(UNOPENED_FILE);
		PG_RETURN_NULL();
	}

	for (i = 0; i < MAX_SLOTS; i++)
	{
		if (slots[i].file)
			bfile_close(i, slots[i].lobname);
	}
	pfree(slots);
	slots = NULL;
	PG_RETURN_NULL();
}

static bytea *
read_binary_file(FILE *file, const char *filename, int64 seek_offset, int64 bytes_to_read)
{
	bytea	*buf;
	size_t	nbytes;

	if (bytes_to_read < 0)
	{
		if (seek_offset < 0)
			bytes_to_read = -seek_offset;
		else
		{
			struct stat fst;
			if (stat(filename, &fst) < 0)
			{
				ereport(ERROR,
						(errcode_for_file_access(),
							errmsg("could not stat file \"%s\": %m", filename)));
			}
			bytes_to_read = fst.st_size - seek_offset;
		}
	}

	/* not sure why anyone thought that int64 length was a good idea */
	if (bytes_to_read > (MaxAllocSize - VARHDRSZ))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("requested length too large")));


	if (fseeko(file, (off_t) seek_offset,
				(seek_offset >= 0) ? SEEK_SET : SEEK_END) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
					errmsg("could not seek in file \"%s\": %m", filename)));

	buf = (bytea *) palloc((Size) bytes_to_read + VARHDRSZ);

	nbytes = fread(VARDATA(buf), 1, (size_t) bytes_to_read, file);

	if (ferror(file))
		ereport(ERROR,
				(errcode_for_file_access(),
					errmsg("could not read file \"%s\": %m", filename)));

	SET_VARSIZE(buf, nbytes + VARHDRSZ);

	return buf;
}

Datum
dbms_lob_read(PG_FUNCTION_ARGS)
{
	bytea	*buf;
	char	*fullname;
	int		slotidx = INVALID_SLOTID;
	char	*lobname = PG_GETARG_BFILE(0);
	int64	bytes_to_read = PG_GETARG_INT64(1);
	int64	seek_offset = PG_GETARG_INT64(2);

	if (lobname == NULL)
		elog(ERROR, "bfile should not be empty");

	slotidx = get_dbms_lob_slot(lobname);
	if (slotidx == INVALID_SLOTID)
	{
		STRERROR_EXCEPTION(UNOPENED_FILE);
		PG_RETURN_NULL();
	}

	if (bytes_to_read > BFILE_MAX_LEN || seek_offset > BFILE_MAX_LEN)
		elog(ERROR, "Read len and  offset exceed %u", BFILE_MAX_LEN);
	if (bytes_to_read + seek_offset > BFILE_MAX_LEN)
		elog(ERROR, "Can't read bfile exceed %u", BFILE_MAX_LEN);

	fullname = get_bfile_fullname(lobname);
	buf = read_binary_file(slots[slotidx].file, fullname,
							seek_offset, bytes_to_read);
	PG_RETURN_BYTEA_P(buf);
}

static bool
handle_lob_substr_param(PG_FUNCTION_ARGS, int32 *amount, int32 *offset)
{
	/*
	 * return NULL if:
	 * - any input parameter is NULL
	 * - amount < 1
	 * - amount > 32767
	 * - offset < 1
	 * - offset > LOBMAXSIZE
	 */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		return true;

	*amount = PG_GETARG_INT32(1);
	*offset = PG_GETARG_INT32(2);

	if ((*amount > DBMS_LOB_SUBSTR_MAX_LEN || *amount < 1 || *offset < 1 || *offset > DBMS_LOB_LOBMAXSIZE) 
		|| (*amount < DBMS_LOB_LOBSTARTOFFSET))
		return true;

	return false;
}

Datum
dbms_lob_substr_blob(PG_FUNCTION_ARGS)
{
	int32 	amount = DBMS_LOB_SUBSTR_MAX_LEN;
	int32	offset = DBMS_LOB_LOBSTARTOFFSET;
	bytea	*blob;
	Datum	result;
	char	*buf;
	char	*blob_str;
	int		blob_len;
	int		nres;
	int		i;

	if (handle_lob_substr_param(fcinfo, &amount, &offset))
		PG_RETURN_NULL();

	blob = PG_GETARG_BYTEA_P(0);

	/*
	 * blob type is malformed data type, it should store as hex binary,
	 * instead it is implemented as string literal.
	 * eg: '31323334', blob should be stored as '1234', but it is '31323334'
	 * and if offset is 1, it should start from '2' of '123', and it's length
	 * is 3.
	 * we hacked here by treating 2 char as an character's ascii value,
	 * '31' -> '1', '32' -> 2, ...
	 */
	blob_str = VARDATA_ANY(blob);
	blob_len = VARSIZE_ANY_EXHDR(blob);

	/* check if valid hex encoded */
	for (i = 0; i < blob_len; ++i)
		if (!isxdigit(blob_str[i]))
			elog(ERROR, "numeric or value error: hex to raw conversion error");

	offset = offset - 1;
	offset = offset * 2;
	if (offset > blob_len || offset < 0)
		PG_RETURN_NULL();

	nres = amount * 2;
	if (nres > blob_len - offset)
		nres = blob_len - offset;

	buf = (char *) palloc(sizeof(char) * (nres + 1));
	StrNCpy(buf, blob_str + offset, nres + 1);

	result = DirectFunctionCall1(rawin, PointerGetDatum(buf));

	pfree(buf);

	return result;
}

Datum
dbms_lob_substr_clob(PG_FUNCTION_ARGS)
{
	int32 	amount = DBMS_LOB_SUBSTR_MAX_LEN;
	int32	offset = DBMS_LOB_LOBSTARTOFFSET;

	if (handle_lob_substr_param(fcinfo, &amount, &offset))
		PG_RETURN_NULL();

	return DirectFunctionCall3(text_substr,
								PG_GETARG_DATUM(0),
								PG_GETARG_DATUM(2),
								PG_GETARG_DATUM(1));
}
