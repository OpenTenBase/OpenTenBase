/*-------------------------------------------------------------------------
 *
 * url_file.c
 *	  Core support for opening external relations via a file URL
 *
 * src/backend/access/external/url_file.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/url.h"
#include "commands/copy.h"
#include "utils/uri.h"
#include "utils/builtins.h"

/*
 * Private state for a file external table, backend by an fstream.
 */
typedef struct URL_FSTREAM_FILE
{
	URL_FILE	common;
} URL_FSTREAM_FILE;

URL_FILE *
url_file_fopen(char *url, bool forwrite, extvar_t *ev, CopyState pstate)
{
	return (URL_FILE *) NULL;
}

void
url_file_fclose(URL_FILE *file, bool failOnError, const char *relname)
{
}

size_t
url_file_fread(void *ptr, size_t size, URL_FILE *file, CopyState pstate)
{
	return 0;
}

bool
url_file_feof(URL_FILE *file, int bytesread)
{
	return true;
}

bool
url_file_ferror(URL_FILE *file, int bytesread, char *ebuf, int ebuflen)
{
	return false;
}
