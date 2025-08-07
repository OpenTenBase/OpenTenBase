/*-------------------------------------------------------------------------
 *
 * url_custom.c
 *	  Core support for opening external relations via a custom URL
 *
 * src/backend/access/external/url_custom.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/extprotocol.h"
#include "access/url.h"
#include "catalog/pg_extprotocol.h"
#include "commands/copy.h"
#include "utils/memutils.h"

/*
 * Private state for an external table that's backend by a user-defined
 * protocol handler function.
 */
typedef struct URL_CUSTOM_FILE
{
	URL_FILE	common;

	FmgrInfo   *protocol_udf;
	ExtProtocol extprotocol;
	MemoryContext protcxt;

} URL_CUSTOM_FILE;

static int32
InvokeExtProtocol(void *ptr,
                  size_t nbytes,
                  URL_CUSTOM_FILE *file,
                  CopyState pstate,
                  bool last_call);

URL_FILE *
url_custom_fopen(char *url,
                 bool forwrite,
                 extvar_t *ev,
                 CopyState pstate,
                 ExternalSelectDesc desc)
{
	/* we're using a custom protocol */
	URL_CUSTOM_FILE   *file;
	MemoryContext oldcontext;
	ExtPtcFuncType ftype;
	Oid			procOid;
	char	   *prot_name;
	int			url_len;
	int			i;

	file = palloc0(sizeof(URL_CUSTOM_FILE));
	file->common.type = CFTYPE_CUSTOM;
	file->common.url = pstrdup(url);
	ftype = (forwrite ? EXTPTC_FUNC_WRITER : EXTPTC_FUNC_READER);

	/* extract protocol name from url string */
	url_len = strlen(file->common.url);
	i = 0;
	while (file->common.url[i] != ':' && i < url_len - 1)
		i++;

	prot_name = pstrdup(file->common.url);
	prot_name[i] = '\0';
	procOid = LookupExtProtocolFunction(prot_name, ftype, true);

	/*
	 * Create a memory context to store all custom UDF private
	 * memory. We do this in order to allow resource cleanup in
	 * cases of query abort. We use TopTransactionContext as a
	 * parent context so that it lives longer than Portal context.
	 * Note that we always Delete our new context, in normal execution
	 * and in abort (see url_fclose()).
	 */
	file->protcxt = AllocSetContextCreate(TopTransactionContext,
										  "CustomProtocolMemCxt",
										  ALLOCSET_DEFAULT_SIZES);

	oldcontext = MemoryContextSwitchTo(file->protcxt);

	file->protocol_udf = palloc(sizeof(FmgrInfo));
	file->extprotocol = (ExtProtocolData *) palloc (sizeof(ExtProtocolData));

	/* we found our function. set it in custom file handler */
	fmgr_info(procOid, file->protocol_udf);

	MemoryContextSwitchTo(oldcontext);

	file->extprotocol->prot_user_ctx = NULL;
	file->extprotocol->prot_last_call = false;
	file->extprotocol->prot_url = NULL;
	file->extprotocol->prot_databuf = NULL;
	file->extprotocol->desc = desc;

	pfree(prot_name);

	return (URL_FILE *) file;
}

void
url_custom_fclose(URL_FILE *file, bool failOnError, const char *relname)
{
	URL_CUSTOM_FILE   *cfile = (URL_CUSTOM_FILE *) file;

	/* last call. let the user close custom resources */
	if (cfile->protocol_udf)
		(void) InvokeExtProtocol(NULL, 0, cfile, NULL, true);

	/* now clean up everything not cleaned by user */
	MemoryContextDelete(cfile->protcxt);

	pfree(cfile);
}

bool
url_custom_feof(URL_FILE *file, int bytesread)
{
	return bytesread == 0;
}

bool
url_custom_ferror(URL_FILE *file, int bytesread, char *ebuf, int ebuflen)
{
	return bytesread == -1;
}

size_t
url_custom_fread(void *ptr, size_t size, URL_FILE *file, CopyState pstate)
{
	URL_CUSTOM_FILE   *cfile = (URL_CUSTOM_FILE *) file;

	return (size_t) InvokeExtProtocol(ptr, size, cfile, pstate, false);
}

size_t
url_custom_fwrite(void *ptr, size_t size, URL_FILE *file, CopyState pstate)
{
	URL_CUSTOM_FILE   *cfile = (URL_CUSTOM_FILE *) file;

	return (size_t) InvokeExtProtocol(ptr, size, cfile, pstate, false);
}

static int32
InvokeExtProtocol(void *ptr,
                  size_t nbytes,
                  URL_CUSTOM_FILE *file,
                  CopyState pstate,
                  bool last_call)
{
	return 0;
}
