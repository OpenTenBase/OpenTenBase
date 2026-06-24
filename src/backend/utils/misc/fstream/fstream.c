#ifdef WIN32
/* exclude transformation features on windows for now */
#undef GPFXDIST
#endif

/*#include "c.h"*/
#ifdef WIN32
#define _WINSOCKAPI_
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#endif
#include <postgres.h>
#include <commands/copy.h>
#include <fstream/fstream.h>
#include <assert.h>
#include <glob.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#ifdef GPFXDIST
#include "utils/gpfxdist.h"
#endif

#define FILE_ERROR_SZ 200
char* format_error(char* c1, char* c2);

/*
 * Returns a pointer to the end of the last delimiter occurrence,
 * or the start pointer if delimiter doesn't appear.
 *
 * delimiter_length MUST be > 0
 */
static char *find_last_eol_delim(const char *start, const int size, 
								 const char *delimiter, const int delimiter_length)
{
	char* p;

	if (size <= delimiter_length)
		return (char*)start - 1;

	for (p = (char*)start + size - delimiter_length; start <= p; p--)
	{
		if (memcmp(p, delimiter, delimiter_length) == 0)
		{
			return (char*)p + delimiter_length - 1;
		}
	}
	return (char*)start - 1;
}

/*
 * Returns a pointer to the end of the first delimiter occurrence, or the end 
 * pointer if delimiter doesn't appear. In case the first expected delimiter 
 * have not been found until the search_limit, an error flag will be returned 
 * if error is not NULL.
 *
 * delimiter_length MUST be > 0
 */
char *find_first_eol_delim(char *start, char *end, const char *delimiter, 
						   const int delimiter_length, const int max_line_len, 
						   int *error)
{
	int i;
	int err_flag;
	char *search_limit;
	
	/* 
	 * set search_limit according to max_line_len and err_flag, 
	 * supposing an error in advance but err_flag might not be used
	 */
	if (max_line_len > 0 && max_line_len < (end - start))
	{
		err_flag = DELIM_NOT_FOUND_MAXLEN;
		search_limit = (char*) start + max_line_len - delimiter_length;
	}
	else
	{
		/* max_line_len <= 0 means it is not designated */
		err_flag = DELIM_NOT_FOUND_END;
		search_limit = (char*) end - delimiter_length;
	}
	
	if (end - start <= delimiter_length)
		return end;
	
	for (i = 0; (start + i) <= search_limit; i++)
	{
		if (memcmp(start + i, delimiter, delimiter_length) == 0)
		{
			if (error != NULL)
				*error = DELIM_FOUND;
				
			return (char*) (start + i + delimiter_length - 1);
		}
	}

	/* if nothing is returned after the whole loop above, it must be an error */
	if (error != NULL)
		*error = err_flag;

	return end;
}

/*
 * Given a pattern, glob it and populate pglob with the results that match it.
 * For example, if 'pattern' equals '/somedir/<asterisk>.txt' and the filesystem has
 * 2 .txt files inside of somedir, namely 1.txt and 2.txt, then the result is:
 * g.gl_pathc = 2
 * g.gl_pathv[0] =  '/somedir/1.txt'
 * g.gl_pathv[1] =  '/somedir/2.txt'
 */
static int glob_and_copy(const char *pattern, int flags,
						 int(*errfunc)(const char *epath, int eerno),
						 glob_and_copy_t *pglob)
{
	glob_t 	g;
	int 	i = glob(pattern, flags, errfunc, &g), j;

	if (!i)
	{
		char **a = gfile_malloc(sizeof *a * (pglob->gl_pathc + g.gl_pathc));

		if (!a)
			i = GLOB_NOSPACE;
		else
		{
			for (j = 0; j < pglob->gl_pathc; j++)
				a[j] = pglob->gl_pathv[j];
			if (pglob->gl_pathv)
				gfile_free(pglob->gl_pathv);
			pglob->gl_pathv = a;
			a += pglob->gl_pathc;
			for (j = 0; j < g.gl_pathc; j++)
			{
				char*b = g.gl_pathv[j];
				if (!(*a = gfile_malloc(strlen(b) + 1)))
				{
					i = GLOB_NOSPACE;
					break;
				}
				strcpy(*a++, b);
				gfile_printf_then_putc_newline("new path %s", b);
				pglob->gl_pathc++;
			}
		}

		globfree(&g);
	}
	return i;
}

/* free memory allocated in pglob */
static void glob_and_copyfree(glob_and_copy_t *pglob)
{
	if (pglob->gl_pathv)
	{
		int i;

		for (i = 0; i < pglob->gl_pathc; i++)
			gfile_free(pglob->gl_pathv[i]);

		gfile_free(pglob->gl_pathv);
		pglob->gl_pathc = 0;
		pglob->gl_pathv = 0;
	}
}

const char*
fstream_get_error(fstream_t*fs)
{
	return fs->ferror;
}

/*
 * Scan the data in [p, q) according to the csv parsing rules. Return as many
 * complete csv records as possible. However, if 'one' was passed in, return the
 * first complete record only.
 *
 * We need this function for gpfdist because until 'text' format we can't just
 * peek at the line newline in the buffer and send the whole data chunk to the
 * server. That is because it may be inside a quote. We have to carefully parse
 * the data from the start in order to find the last unquoted newline.
 */
static char*
scan_csv_records_crlf(char *p, char *q, int one, fstream_t* fs)
{
	int 	in_quote = 0;
	int 	last_was_esc = 0;
	int 	qc = fs->options.quote;
	int 	xc = fs->options.escape;
	char*	last_record_loc = 0;
	int 	ch = 0;
	int 	lastch = 0;

	while (p < q)
	{
		lastch = ch;
		ch = *p++;

		if (in_quote)
		{
			if (!last_was_esc)
			{
				if (ch == qc)
					in_quote = 0;
				else if (ch == xc)
					last_was_esc = 1;
			}
			else
				last_was_esc = 0;
		}
		else if (ch == '\n' && lastch == '\r')
		{
			last_record_loc = p;
			fs->line_number++;
			if (one)
				break;
		}
		else if (ch == qc)
			in_quote = 1;
	}

	return last_record_loc;
}

static char*
scan_csv_records_cr_or_lf(char *p, char *q, int one, fstream_t *fs, int nc)
{
	int 	in_quote = 0;
	int 	last_was_esc = 0;
	int 	qc = fs->options.quote;
	int 	xc = fs->options.escape;
	char*	last_record_loc = 0;

	while (p < q)
	{
		int ch = *p++;

		if (in_quote)
		{
			if (!last_was_esc)
			{
				if (ch == qc)
					in_quote = 0;
				else if (ch == xc)
					last_was_esc = 1;
			}
			else
				last_was_esc = 0;
		}
		else if (ch == nc)
		{
			last_record_loc = p;
			fs->line_number++;
			if (one)
				break;
		}
		else if (ch == qc)
			in_quote = 1;
	}

	return last_record_loc;
}

static char*
scan_csv_records(char *p, char *q, int one, fstream_t *fs)
{
	switch(fs->options.eol_type)
	{
		case EOL_CRNL:
		   return scan_csv_records_crlf(p, q, one, fs);					/* \r\n */
		case EOL_CR:
		   return scan_csv_records_cr_or_lf(p, q, one, fs, '\r');	/* \r */
		case EOL_NL:
		default:
		   return scan_csv_records_cr_or_lf(p, q, one, fs, '\n');	/* \n */
	}
}

/* close the file stream */
void fstream_close(fstream_t* fs)
{
#ifdef GPFXDIST
	/*
	 * remove temporary file we created to hold the file paths
	 */
	if (fs->options.transform && fs->options.transform->tempfilename)
	{
		apr_file_remove(fs->options.transform->tempfilename, fs->options.transform->mp);
		fs->options.transform->tempfilename = NULL;
	}
#endif

	if(fs->buffer)
		gfile_free(fs->buffer);

	glob_and_copyfree(&fs->glob);
	gfile_close(&fs->fd);
	gfile_free(fs);
}

static int fpath_all_directories(const glob_and_copy_t *glob)
{
	int i;

	for (i = 0; i < glob->gl_pathc; i++)
	{
		const char	*a = glob->gl_pathv[i];

		if (!*a || a[strlen(a) - 1] != '/')
			return 0;
	}
	return 1;
}

static int expand_directories(fstream_t *fs)
{
	glob_and_copy_t g;
	int 			i, j;

	memset(&g, 0, sizeof g);

	for (i = 0; i < fs->glob.gl_pathc; i++)
	{
		const char* a = fs->glob.gl_pathv[i];
		char* b = gfile_malloc(2 * strlen(a) + 2), *c;

		if (!b)
		{
			gfile_printf_then_putc_newline("fstream out of memory");
			glob_and_copyfree(&g);
			return 1;
		}

		for (c = b ; *a ; a++)
		{
			if (*a == '?' || *a == '*' || *a == '[')
				*c++ = '\\';
			*c++ = *a;
		}

		*c++ = '*';
		*c++ = 0;
		j = glob_and_copy(b, 0, 0, &g);
		gfile_free(b);

		if (j == GLOB_NOMATCH)
		{
			gfile_printf_then_putc_newline("fstream %s is empty directory", fs->glob.gl_pathv[i]);
		}
		else if (j)
		{
			gfile_printf_then_putc_newline("fstream glob failed");
			glob_and_copyfree(&g);
			return 1;
		}
	}

	glob_and_copyfree(&fs->glob);
	fs->glob = g;
	return 0;
}

static int glob_path(fstream_t *fs, const char *path)
{
	char	*path2 = gfile_malloc(strlen(path) + 1);

	if (!path2)
	{
		gfile_printf_then_putc_newline("fstream out of memory");
		return 1;
	}

	path = strcpy(path2, path);

	do
	{
		char*	p;

		while (*path == ' ')
			path++;

		p = strchr(path, ' ');

		if (p)
			*p++ = 0;

		if (*path &&
			glob_and_copy(path, GLOB_MARK | GLOB_NOCHECK, 0, &fs->glob))
		{
			gfile_printf_then_putc_newline("fstream glob failed");
			gfile_free(path2);
			return 1;
		}
		path = p;
	} while (path);

	gfile_free(path2);

	return 0;
}


#ifdef GPFXDIST
/*
 * adjust fs->glob so that it contains a single item which is a
 * properly allocated copy of the specified filename.  assumes fs->glob
 * contains at least one name.  
 */

static int glob_adjust(fstream_t* fs, char* filename, int* response_code, const char** response_string)
{
	int i;
	int tlen    = strlen(filename) + 1;
	char* tname = gfile_malloc(tlen);
	if (!tname) 
	{
		*response_code = 500;
		*response_string = "fstream out of memory allocating copy of temporary file name";
		gfile_printf_then_putc_newline("%s", *response_string);
		return 1;
	}

	for (i = 0; i<fs->glob.gl_pathc; i++)
	{
		gfile_free(fs->glob.gl_pathv[i]);
		fs->glob.gl_pathv[i] = NULL;
	}
	strcpy(tname, filename);
	fs->glob.gl_pathv[0] = tname;
	fs->glob.gl_pathc = 1;
	return 0;
}
#endif

/*
 * Allocate a new file stream given a path (a url). in case of wildcards,
 * expand them here. we end up with a final list of files and include them
 * in our filestream that we return.
 *
 * In case of errors we set the proper http response code to send to the client.
 */
fstream_t*
fstream_open(const char *path, const fstream_options *options,
			 int *response_code, const char **response_string)
{
	int i;
	fstream_t* fs;

	*response_code = 500;
	*response_string = "Internal Server Error";

	if (0 == (fs = gfile_malloc(sizeof *fs)))
	{
		gfile_printf_then_putc_newline("fstream out of memory");
		return 0;
	}

	memset(fs, 0, sizeof *fs);
	fs->options = *options;
	fs->buffer = gfile_malloc(options->bufsize);
	
	/*
	 * get a list of all files that were requested to be read and include them
	 * in our fstream. This includes any wildcard pattern matching.
	 */
	if (glob_path(fs, path))
	{
		fstream_close(fs);
		return 0;
	}

	/*
	 * If the list of files in our filestrem includes a directory name, expand
	 * the directory and add all the files inside of it.
	 */
	if (fpath_all_directories(&fs->glob))
	{
		if (expand_directories(fs))
		{
			fstream_close(fs);
			return 0;
		}
	}

	/* check if we don't have any matching files */
	if (fs->glob.gl_pathc == 0)
	{
		gfile_printf_then_putc_newline("fstream bad path: %s", path);
		fstream_close(fs);
		*response_code = 404;
		*response_string = "No matching file(s) found";
		return 0;
	}

	if (fs->glob.gl_pathc != 1 && options->forwrite)
	{
		gfile_printf_then_putc_newline("fstream open for write found more than one file (%d)",
										fs->glob.gl_pathc);
		*response_code = 404;
		*response_string = "More than 1 file found for writing. Unsupported operation.";

		fstream_close(fs);
		return 0;
	}


#ifdef GPFXDIST
	/*
	 * when the subprocess transformation wants to handle iteration over the files
	 * we write the paths to a temporary file and replace the fs->glob items with
	 * just a single entry referencing the path to the temporary file.
	 */
	if (options->transform && options->transform->pass_paths)
	{
		apr_pool_t*  mp = options->transform->mp;
		apr_file_t*  f = NULL;
		const char*  tempdir = NULL;
		char*        tempfilename = NULL;
		apr_status_t rv;

		if ((rv = apr_temp_dir_get(&tempdir, mp)) != APR_SUCCESS)
		{
			*response_code = 500;
			*response_string = "failed to get temporary directory for paths file";
			gfile_printf_then_putc_newline("%s", *response_string);
			fstream_close(fs);
			return 0;
		}

	    tempfilename = apr_pstrcat(mp, tempdir, "/pathsXXXXXX", (char *) NULL);
		if ((rv = apr_file_mktemp(&f, tempfilename, APR_CREATE|APR_WRITE|APR_EXCL, mp)) != APR_SUCCESS)
		{
			*response_code = 500;
			*response_string = "failed to open temporary paths file";
			gfile_printf_then_putc_newline("%s", *response_string);
			fstream_close(fs);
			return 0;
		}

		options->transform->tempfilename = tempfilename;

		for (i = 0; i<fs->glob.gl_pathc; i++)
		{
			char* filename      = fs->glob.gl_pathv[i];
			apr_size_t expected = strlen(filename) + 1;
			
			if (apr_file_printf(f, "%s\n", filename) < expected)
			{
				apr_file_close(f);

				*response_code = 500;
				*response_string = "failed to fully write path to temporary paths file";
				gfile_printf_then_putc_newline("%s", *response_string);
				fstream_close(fs);
				return 0;
			}
		}

		apr_file_close(f);

		if (glob_adjust(fs, tempfilename, response_code, response_string)) 
		{
			fstream_close(fs);
			return 0;
		}
	}
#endif

	/*
	 * if writing - check write access rights for the one file.
	 * if reading - check read access right for all files, and 
	 * then close them, leaving the first file open.
	 */
	for (i = fs->glob.gl_pathc; --i >= 0;)
	{
		/*
		 * CR-2173 - the fstream code allows the upper level logic to treat a
		 * collection of input sources as a single stream.  One problem it has
		 * to handle is the possibility that some of the underlying sources may
		 * not be readable.  Here we're trying to detect potential problems in
		 * advance by checking that we can open and close each source in our
		 * list in reverse order.
		 *
		 * However in the case of subprocess transformations, we don't want to 
		 * start and stop each transformation in this manner.  The check that 
		 * each transformation's underlying input source can be read is still 
		 * useful so we do those until we get to the first source, at which 
		 * point we proceed to just setup the tranformation for it.
		 */
		struct gpfxdist_t* transform = (i == 0) ? options->transform : NULL;

		gfile_close(&fs->fd);

		if (gfile_open(&fs->fd, fs->glob.gl_pathv[i], gfile_open_flags(options->forwrite, options->usesync),
					   response_code, response_string, transform))
		{
			gfile_printf_then_putc_newline("fstream unable to open file %s",
					fs->glob.gl_pathv[i]);
			fstream_close(fs);
			return 0;
		}

		fs->compressed_size += gfile_get_compressed_size(&fs->fd);
	}

	fs->line_number = 1;
	fs->skip_header_line = options->header;

	return fs;
}

/*
 * Updates the currently used filename and line number and offset. Since we
 * may be reading from more than 1 file, we need to be up to date all the time.
 */
static void updateCurFileState(fstream_t* fs, fstream_filename_and_offset* fo)
{
	if (fo)
	{
		fo->foff = fs->foff;
		fo->line_number = fs->line_number;
		strncpy(fo->fname, fs->glob.gl_pathv[fs->fidx], sizeof fo->fname);
		fo->fname[sizeof fo->fname - 1] = 0;
	}
}

/*
 * open the next source file, if any.
 * return 1 if could not open the next file.
 * return 0 otherwise.
 */
static int nextFile(fstream_t*fs)
{
	int response_code;
	const char	*response_string;
	struct gpfxdist_t* transform = fs->options.transform;

	fs->compressed_position += gfile_get_compressed_size(&fs->fd);
	gfile_close(&fs->fd);
	fs->foff = 0;
	fs->line_number = 1;
	fs->fidx++;

	if (fs->fidx < fs->glob.gl_pathc)
	{
		fs->skip_header_line = fs->options.header;

		if (gfile_open(&fs->fd, fs->glob.gl_pathv[fs->fidx], GFILE_OPEN_FOR_READ, 
					   &response_code, &response_string, transform))
		{
			gfile_printf_then_putc_newline("fstream unable to open file %s",
											fs->glob.gl_pathv[fs->fidx]);
			fs->ferror = "unable to open file";
			return 1;
		}
	}

	return 0;
}

/*
 * enables addition of string parameters to the const char* error message in fstream_t
 * while enabling the calling functions not to worry about freeing memory - which is 
 * the present behaviour
 */
char* format_error(char* c1, char* c2)
{
	int         len1, len2;
	char       *targ;

	static char err_msg[FILE_ERROR_SZ];
	memset(err_msg, 0, FILE_ERROR_SZ);
	
	len1 = strlen(c1);
	len2 = strlen(c2);
	if ( (len1 + len2) > FILE_ERROR_SZ )
	{
		gfile_printf_then_putc_newline("cannot read file");
		return "cannot read file";
	}
	
	targ = err_msg;
	memcpy(targ, c1, len1);
	targ += len1;
	memcpy(targ, c2, len2);
	
	gfile_printf_then_putc_newline("%s", err_msg);
	
	return err_msg;
}

/*
 * Read 'size' bytes of data from the filestream into 'buffer'.
 * If 'read_whole_lines' is specified then read up to the last logical row
 * in the source buffer. 'fo' keeps the state (name, offset, etc) of the current
 * filestream file we are reading from.
 */
int fstream_read(fstream_t *fs,
				 void *dest,
				 int size,
				 fstream_filename_and_offset *fo,
				 const int read_whole_lines,
				 const char *line_delim_str,
				 const int line_delim_length)
{
	int delim_error;
	int buffer_capacity = fs->options.bufsize;
	static char err_buf[FILE_ERROR_SZ] = {0};
	
	if (fs->ferror)
		return -1;

	for (;;)
	{
		ssize_t bytesread; 		/* num bytes read from filestream */
		ssize_t bytesread2;		/* same, but when reading a second round */

		/* invalid buffer size, or all files are read */
		if (!size || fs->fidx == fs->glob.gl_pathc)
			return 0;

		/*
		 * If data source has a header, we consume it now and in order to
		 * move on to real data that follows it.
		 */
		if (fs->skip_header_line)
		{
			char* 	p = fs->buffer;
			char* 	q = p + fs->buffer_cur_size;
			size_t 	rest_len = 0;		/* the rest length of buffer to read */

		    assert(fs->buffer_cur_size < buffer_capacity);

			/* read data from the source file and fill up the file stream buffer */
			rest_len = buffer_capacity - fs->buffer_cur_size;
			bytesread = gfile_read(&fs->fd, q, rest_len);

			if (bytesread < 0)
			{
				fs->ferror = format_error("cannot read file - ", fs->glob.gl_pathv[fs->fidx]);
				return -1;
			}

			/* update the buffer size according to new byte count we just read */
			fs->buffer_cur_size += bytesread;
			q += bytesread;

			/* p will be the end of the first line of the file in the buffer */
			if (fs->options.is_csv)
			{
				/* csv header */
				p = scan_csv_records(p, q, 1, fs);
			}
			else
			{
				if (line_delim_length > 0)
				{
					/* text header with defined EOL */
					p = find_first_eol_delim(p, q, line_delim_str, 
											 line_delim_length, size, 
											 &delim_error);
					
					if (delim_error == DELIM_NOT_FOUND_MAXLEN)
					{
						fs->ferror = "max data length too short to find a delimiter";
						return -1;
					}
					else if (delim_error == DELIM_NOT_FOUND_END)
					{
						fs->ferror = "line too long to find a delimiter";
						return -1;
					}
				}
				else
				{
					/* text header with \n as delimiter (by default) */
					for (; p < q && *p != '\n'; p++)
						;
				}

				/*
				 * p equals q means the heard is too long
				 * that the whole buffer can not contain it
				 */
				p = (p < q) ? p + 1 : 0;
				fs->line_number++;
			}

			if (!p)
			{
				if (fs->buffer_cur_size == buffer_capacity)
				{
					gfile_printf_then_putc_newline(
							"fstream ERROR: header too long in file %s",
							fs->glob.gl_pathv[fs->fidx]);
					
					fs->ferror = "line too long in file";
					return -1;
				}
				p = q;
			}

			/*
			 * update the filestream buffer offset to past last line read and
			 * copy the end of the buffer (past header data) to the beginning.
			 * we now bypassed the header data and can continue to real data.
			 */
			fs->foff += p - fs->buffer;
			fs->buffer_cur_size = q - p;
			memmove(fs->buffer, p, fs->buffer_cur_size);
			fs->skip_header_line = 0;
		}

		/*
		 * If we need to read all the data up to the last *complete* logical
		 * line in the data buffer (like gpfdist for example) - we choose this
		 * path. We grab the bigger chunk we can get that includes whole lines.
		 * Otherwise, if we just want the whole buffer we skip.
		 */
		if (read_whole_lines)
		{
			char	*p;
			ssize_t total_bytes = fs->buffer_cur_size;
			
			assert(size >= buffer_capacity);

			if (total_bytes > 0)
			{
				/*
				 * source buffer is not empty. copy the data from the beginning
				 * up to the current length before moving on to reading more
				 */
				fs->buffer_cur_size = 0;
				updateCurFileState(fs, fo);
				memcpy(dest, fs->buffer, total_bytes);
			}

			/* read more data from source file into destination buffer */
			bytesread2 = gfile_read(&fs->fd, (char*) dest + total_bytes, size - total_bytes);

			if (bytesread2 < 0)
			{
				fs->ferror = format_error("cannot read file - ", fs->glob.gl_pathv[fs->fidx]);
				return -1;
			}

			/*
			 * If bytesread2 < size - total_bytes,
			 * the current file must have been read entirely.
			 * It is also possible bytesread2 = size - total_bytes.
			 */
			if (bytesread2 < size - total_bytes)
			{
				/*
				 * We didn't read as much as we asked for. Check why.
				 * We could be done reading data, we may need to move
				 * on the reading the next data file (if any).
				 */
				if (total_bytes == 0)
				{
					if (bytesread2 == 0)
					{
						/*
						 * buffer is empty and the entire file has been read
						 * try to open the next file
						 */
						if (nextFile(fs))
							return -1; /* found next file but failed to open */
						continue;
					}
					updateCurFileState(fs, fo);
				}

				/*
				 * try to open the next file if any, and return the number of
				 * bytes read to buffer earlier, if next file was found but
				 * could not open return -1
				 */
				return nextFile(fs) ? -1 : total_bytes + bytesread2;
			}

			updateCurFileState(fs, fo);

			/*
			 * Now that we have enough data in our filestream buffer, get a 
			 * chunk of whole rows and copy it into our dest buffer to be sent
			 * out later.
			 */
			if (fs->options.is_csv)
			{
				/* CSV: go slow, scan byte-by-byte for record boundary */
				p = scan_csv_records(dest, (char*)dest + size, 0, fs);
			}
			else
			{
				/*
				 * TEXT: go fast, scan for end of line delimiter (\n by default) for
				 * record boundary.
				 * find the last end of line delimiter from the back
				 */
				if (line_delim_length > 0)
				{
					p = find_last_eol_delim((char*)dest, size, line_delim_str, line_delim_length);
				}
				else
				{
					for (p = (char*)dest + size; (char*)dest <= --p && *p != '\n';)
						;
				}

				p = (char*)dest <= p ? p + 1 : 0;
				fs->line_number = 0;
			}

			/* what if we could not find even one complete row in the buffer */
			if (!p || (char*)dest + size >= p + buffer_capacity)
			{
#ifdef WIN32
				snprintf(err_buf, sizeof(err_buf)-1, "line too long in file %s near (%ld bytes)",
						 fs->glob.gl_pathv[fs->fidx], (long) fs->foff);
#else
				snprintf(err_buf, sizeof(err_buf)-1, "line too long in file %s near (%lld bytes)",
						 fs->glob.gl_pathv[fs->fidx], (long long) fs->foff);
#endif
				fs->ferror = err_buf;
				gfile_printf_then_putc_newline("%s", err_buf);
				return -1;
			}

			/* copy the result chunk of data into our buffer */
			fs->buffer_cur_size = (char*)dest + size - p;
			memcpy(fs->buffer, p, fs->buffer_cur_size);
			fs->foff += p - (char*)dest;

			return p - (char*)dest;
		}

		/*
		 * if we're here it means that we just want chunks of data and don't
		 * care if it includes whole rows or not (for example, backend url_read
		 * code - a segdb that reads all the data that gpfdist sent to it,
		 * buffer by buffer and then parses the lines internally).
		 */

		if (fs->buffer_cur_size)
		{
			ssize_t total_bytes = fs->buffer_cur_size;
			
			updateCurFileState(fs, fo);

			if (total_bytes > size)
				total_bytes = size;

			memcpy(dest, fs->buffer, total_bytes);
			fs->buffer_cur_size -= total_bytes;
			memmove(fs->buffer, fs->buffer + total_bytes, fs->buffer_cur_size);

			fs->foff += total_bytes;
			fs->line_number = 0;

			return total_bytes;
		}

		bytesread = gfile_read(&fs->fd, dest, size);

		if (bytesread < 0)
		{
			fs->ferror = format_error("cannot read file - ", fs->glob.gl_pathv[fs->fidx]);
			
			return -1;
		}

		if (bytesread)
		{
			updateCurFileState(fs, fo);
			fs->foff += bytesread;
			fs->line_number = 0;
			return bytesread;
		}

		if (nextFile(fs))
			return -1;
	}
}

int fstream_write(fstream_t *fs,
				  void *buf,
				  int size,
				  const int write_whole_lines,
				  const char* line_delim_str,
				  const int line_delim_length)
{
	int byteswritten = 0;
	
	if (fs->ferror)
		return -1;

	/*
	 * If we need to write all the data up to the last *complete* logical
	 * line in the data buffer (like gpfdist for example) - we choose this
	 * path (we don't want to write partial lines to the output file). 
	 * We grab the bigger chunk we can get that includes whole lines.
	 * Otherwise, if we just want the whole buffer we skip.
	 */	
	if (write_whole_lines)
	{
		char* last_delim;

	   /*
		* scan for EOL Delimiter (\n by default) for record boundary
		* find the last EOL Delimiter from the back
		*/
		if (line_delim_length > 0)
		{
			last_delim = find_last_eol_delim(buf, size, line_delim_str, line_delim_length);
		}
		else
		{
			for (last_delim = (char*)buf + size; (char*)buf <= --last_delim && *last_delim != '\n';)
				;
		}
		last_delim = (char*)buf <= last_delim ? last_delim + 1 : 0;

		if (last_delim == 0)
		{
			fs->ferror = "no complete data row found for writing";
			return -1;
		}
				
		/* TODO: need to do this more carefully for CSV, like in the read case */
		
                size = last_delim - (char *) buf;
		/* caller should move leftover data to start of buffer */
        }

	/* write data to destination file */
	byteswritten = (int)gfile_write(&fs->fd, (char*) buf, size);

	if (byteswritten < 0)
	{
                gfile_printf_then_putc_newline("cannot write into file, byteswritten=%d, size=%d, errno=%d, errmsg=%s", 
                        byteswritten, size, errno, strerror(errno));
		fs->ferror = "cannot write into file";
		return -1;
	}

	return byteswritten;
	
}

int cos_fstream_write(ossContext context, const char *bucket,  cos_fstream_t *cos_fs, 
					  void *buf, int size, const int write_whole_lines,
                      const char *line_delim_str, const int line_delim_length)
{
	int byteswritten = 0;
	
	if (cos_fs->ferror)
		return -1;
	
	/*
	 * If we need to write all the data up to the last *complete* logical
	 * line in the data buffer (like gpfdist for example) - we choose this
	 * path (we don't want to write partial lines to the output file). 
	 * We grab the bigger chunk we can get that includes whole lines.
	 * Otherwise, if we just want the whole buffer we skip.
	 */
	if (write_whole_lines)
	{
		char* last_delim;
		
		/*
		 * scan for EOL Delimiter (\n by default) for record boundary
		 * find the last EOL Delimiter from the back
		 */
		if (line_delim_length > 0)
		{
			last_delim = find_last_eol_delim(buf, size, line_delim_str, line_delim_length);
		}
		else
		{
			for (last_delim = (char*)buf + size; (char*)buf <= --last_delim && *last_delim != '\n';)
				;
		}
		last_delim = (char*)buf <= last_delim ? last_delim + 1 : 0;
		
		if (last_delim == 0)
		{
			cos_fs->ferror = "no complete data row found for writing";
			return -1;
		}
		
		/* TODO: need to do this more carefully for CSV, like in the read case */
		
		size = last_delim - (char *) buf;
		/* caller should move leftover data to start of buffer */
	}
	
	/* write data to destination file */
	byteswritten = (int) ossWrite(context, cos_fs->obj, (char *) buf, size);
	
	if (byteswritten < size)
	{
		gfile_printf_then_putc_newline("cannot write into cos file, byteswritten=%d, size=%d, cos_errmsg=%s",
		                               byteswritten, size, ossGetLastError());
		cos_fs->ferror = "cannot write into cos object";
		return -1;
	}
	
	return byteswritten;
}

int fstream_eof(fstream_t *fs)
{
	return fs->fidx == fs->glob.gl_pathc;
}

int64_t fstream_get_compressed_size(fstream_t *fs)
{
	return fs->compressed_size;
}

int64_t fstream_get_compressed_position(fstream_t *fs)
{
	int64_t p = fs->compressed_position;
	if (fs->fidx != fs->glob.gl_pathc)
		p += gfile_get_compressed_position(&fs->fd);
	return p;
}

bool_t fstream_is_win_pipe(fstream_t *fs)
{
	return fs->fd.is_win_pipe;
}

static char *
scan_csv_records_cos(char *p, char *q, int one, cos_fstream_t *cos_fs)
{
	char *ret;
	fstream_t *fs = gfile_malloc(sizeof(fstream_t));
	fs->options = cos_fs->options;
	fs->line_number = cos_fs->line_number;
	switch (fs->options.eol_type)
	{
		case EOL_CRNL:
			ret = scan_csv_records_crlf(p, q, one, fs);                    /* \r\n */
		case EOL_CR:
			ret = scan_csv_records_cr_or_lf(p, q, one, fs, '\r');    /* \r */
		case EOL_NL:
		default:
			ret = scan_csv_records_cr_or_lf(p, q, one, fs, '\n');    /* \n */
	}
	/* update cos_fs info */
	cos_fs->line_number = fs->line_number;
	gfile_free(fs);
	return ret;
}

/* close the cos stream */
void 
cos_fstream_close(ossContext context, cos_fstream_t *cos_fs)
{
#ifdef GPFXDIST
	/* remove temporary file we created to hold the file paths */
	if (cos_fs->options.transform && cos_fs->options.transform->tempfilename)
	{
		apr_file_remove(cos_fs->options.transform->tempfilename, cos_fs->options.transform->mp);
		cos_fs->options.transform->tempfilename = NULL;
	}
#endif
	if (cos_fs->obj)
	{
		ossCloseObject(context, cos_fs->obj);
		cos_fs->obj = 0;
	}
	
	if (cos_fs->buffer)
		gfile_free(cos_fs->buffer);
	/* cos_fs->obj has been closed in nextOssObject */
	ossFreeListObjectResult(cos_fs->object_result);
	gfile_free(cos_fs);
}

/*
 * open the next object, if any.
 * return 1 if could not open the next file.
 * return 0 otherwise.
 */
static int 
nextOssObject(ossContext context, const char *bucket, cos_fstream_t *cos_fs)
{
	if (cos_fs->obj)
	{
		ossCloseObject(context, cos_fs->obj);
		cos_fs->obj = 0;
	}
	cos_fs->obj_off = 0;
	cos_fs->line_number = 1;
	cos_fs->obj_idx++;
	
	while (cos_fs->obj_idx < cos_fs->object_result->nObjects)
	{
		ossObjectInfo obj_info = cos_fs->object_result->objects[cos_fs->obj_idx];
		
		if (obj_info.isDir || obj_info.size == 0)
		{
			/* ignore directory */
			cos_fs->obj_idx++;
			continue;
		}
		
		gprintln(NULL, "start reading object \"%s\"", obj_info.key);
		cos_fs->skip_header_line = cos_fs->options.header;
		cos_fs->obj = ossGetObject(context, bucket, obj_info.key, obj_info.range.start, obj_info.size - 1);
		if (!cos_fs->obj)
		{
			gfile_printf_then_putc_newline("Get object failed with error message %s", (char *) ossGetLastError());
			cos_fs->ferror = "unable to Get object";
			cos_fstream_close(context, cos_fs);
			return 1;
		}
		
		cos_fs->compressed_position += cos_fs->compressed_size;
		break;
	}
	
	return 0;
}

static int 
cos_get_all_objects(ossContext context, const char *bucket,
                               const char *prefix, const char *delimeter, int limit, cos_fstream_t *cos_fs)
{
	ossObjectResult *object_result = ossListObjects(context, bucket, prefix, delimeter, 0);
	if (object_result)
		cos_fs->object_result = object_result;
	else
	{
		gfile_printf_then_putc_newline(" list bucket %s objects failed with error message %s",
		                               bucket,
		                               (char *) ossGetLastError());
		return 1;
	}
	return 0;
}

/*
 * Allocate a new file stream given a path (a url). in case of wildcards,
 * expand them here. we end up with a final list of files and include them
 * in our filestream that we return.
 *
 * In case of errors we set the proper http response code to send to the client.
 */
cos_fstream_t *
cos_fstream_open(ossContext context, const char *bucket, const char *prefix,
                 const fstream_options *options,
                 int *response_code, const char **response_string)
{
	int i;
	cos_fstream_t *cos_fs;
	
	*response_code = 500;
	*response_string = "Internal Server Error";
	
	if (0 == (cos_fs = gfile_malloc(sizeof *cos_fs)))
	{
		gfile_printf_then_putc_newline("fstream out of memory");
		return NULL;
	}
	
	memset(cos_fs, 0, sizeof(cos_fstream_t));
	cos_fs->options = *options;
	cos_fs->buffer = gfile_malloc(options->bufsize);
	
	/*
	 * get a list of all objects that were requested to be read and include them
	 * in our cos_fstream.
	 */
	if (cos_get_all_objects(context, bucket, prefix, "", 0, cos_fs))
	{
		cos_fstream_close(context, cos_fs);
		return NULL;
	}
	
	/* check if we don't have any matching files */
	if (cos_fs->object_result->nObjects == 0 && !cos_fs->options.forwrite)
	{
		gfile_printf_then_putc_newline("fstream bad path: %s", cos_fs->object_result->name);
		cos_fstream_close(context, cos_fs);
		*response_code = 404;
		*response_string = "No matching cos object(s) found";
		return NULL;
	}
	
	if (cos_fs->object_result->nObjects > 1 && options->forwrite)
	{
		gfile_printf_then_putc_newline("cos fstream open for write found more than one file (%d)",
		                               cos_fs->object_result->nObjects);
		*response_code = 404;
		*response_string = "More than 1 cos object found for writing. Unsupported operation.";
		
		cos_fstream_close(context, cos_fs);
		return NULL;
	}
	
	/*
	 * if writing - check write access rights for the one file.
	 * if reading - check read access right for all ossObject, and 
	 * then close them, leaving the first ossObject open.
	 */
	for (i = cos_fs->object_result->nObjects; --i >= 0;)
	{
		ossObjectInfo obj_info = cos_fs->object_result->objects[i];
		
		/* ignore directory */
		if (obj_info.isDir || obj_info.size == 0)
			continue;
		
		printf("trying to open cos fstream object %d: (%s, %ld)\n", i, obj_info.key, obj_info.size);
		
		/* open read object */
		if (cos_fs->obj)
		{
			ossCloseObject(context, cos_fs->obj);
			cos_fs->obj = 0;
		}
		if (!cos_fs->options.forwrite)
		{
			cos_fs->obj = ossGetObject(context, bucket, obj_info.key, obj_info.range.start, obj_info.size - 1);
		}
		else
		{
			cos_fs->obj = ossPutObject(context, bucket, obj_info.key, true);
		}
		
		if (!cos_fs->obj)
		{
			gfile_printf_then_putc_newline("GET/PUT object failed with error message %s", (char *) ossGetLastError());
			cos_fstream_close(context, cos_fs);
			return NULL;
		}
		cos_fs->obj_idx = i;
		cos_fs->compressed_size += obj_info.size;
	}
	
	if (cos_fs->options.forwrite && !cos_fs->obj)
	{
		cos_fs->obj = ossPutObject(context, bucket, cos_fs->object_result->prefix, true);
		
		if (!cos_fs->obj)
		{
			gfile_printf_then_putc_newline("PUT object failed with error message %s", (char *) ossGetLastError());
			cos_fstream_close(context, cos_fs);
			return NULL;
		}
		cos_fs->obj_idx = i;
		cos_fs->compressed_size = 0;
	}
	
	cos_fs->line_number = 1;
	cos_fs->skip_header_line = options->header;
	
	return cos_fs;
}

/*
 * Updates the currently used filename and line number and offset. Since we
 * may be reading from more than 1 file, we need to be up to date all the time.
 */
static void 
updateCosCurFileState(cos_fstream_t *cos_fs, fstream_filename_and_offset *fo)
{
	if (fo)
	{
		fo->foff = cos_fs->obj_off;
		fo->line_number = cos_fs->line_number;
		strncpy(fo->fname,
		        cos_fs->object_result->objects[cos_fs->obj_idx].key,
		        strlen(cos_fs->object_result->objects[cos_fs->obj_idx].key));
		fo->fname[strlen(fo->fname)] = '\0';
	}
}

/* continue reading all objects in objects */
int 
cos_fstream_read(ossContext context, const char *bucket, cos_fstream_t *cos_fs, 
				 void *dest, int size, fstream_filename_and_offset *fo, const int read_whole_lines, 
				 const char *line_delim_str, const int line_delim_length)
{
	int delim_error;
	int buffer_capacity = cos_fs->options.bufsize;
	static char err_buf[FILE_ERROR_SZ] = {0};
	
	if (cos_fs->ferror)
		return -1;
	
	for (;;)
	{
		ssize_t bytesread;        /* num bytes read from filestream */
		ssize_t bytesread2;        /* same, but when reading a second round */
		
		/* invalid buffer size, or all files are read */
		if (!size || cos_fs->obj_idx == cos_fs->object_result->nObjects)
			return 0;
		
		
		/* If data source has a header, we consume it now and in order to
		 * move on to real data that follows it.
		 */
		if (cos_fs->skip_header_line)
		{
			char *p = cos_fs->buffer;
			char *q = p + cos_fs->buffer_cur_size;
			size_t rest_len = 0;        /* the rest length of buffer to read */
			
			assert(cos_fs->buffer_cur_size < buffer_capacity);
			
			/* read data from the source file and fill up the file stream buffer */
			rest_len = buffer_capacity - cos_fs->buffer_cur_size;
			bytesread = ossRead(context, cos_fs->obj, q, rest_len);
			
			if (bytesread < 0)
			{
				cos_fs->ferror = format_error("Read object with error message ", (char *) ossGetLastError());
				return -1;
			}
			
			/* update the buffer size according to new byte count we just read */
			cos_fs->buffer_cur_size += bytesread;
			q += bytesread;
			
			/* p will be the end of the first line of the file in the buffer */
			if (cos_fs->options.is_csv)
			{
				/* csv header */
				p = scan_csv_records_cos(p, q, 1, cos_fs);
			}
			else
			{
				if (line_delim_length > 0)
				{
					/* text header with defined EOL */
					p = find_first_eol_delim(p, q, line_delim_str, 
											 line_delim_length, size, 
											 &delim_error);

					if (delim_error == DELIM_NOT_FOUND_MAXLEN)
					{
						cos_fs->ferror = "max data length too short to find a delimiter";
						return -1;
					}
					else if (delim_error == DELIM_NOT_FOUND_END)
					{
						cos_fs->ferror = "line too long to find a delimiter";
						return -1;
					}
				}
				else
				{
					/* text header with \n as delimiter (by default) */
					for (; p < q && *p != '\n'; p++);
				}
				
				/*
				 * p equals q means the heard is too long
				 * that the whole buffer can not contain it
				 */
				p = (p < q) ? p + 1 : 0;
				cos_fs->line_number++;
			}
			
			if (!p)
			{
				if (cos_fs->buffer_cur_size == buffer_capacity)
				{
					gfile_printf_then_putc_newline(
							"cos fstream ERROR: header too long in file %s",
							cos_fs->object_result[cos_fs->obj_idx].name);
					
					cos_fs->ferror = "line too long in file";
					return -1;
				}
				p = q;
			}
			
			/*
			 * update the filestream buffer offset to past last line read and
			 * copy the end of the buffer (past header data) to the beginning.
			 * we now bypassed the header data and can continue to real data.
			 */
			cos_fs->obj_off += p - cos_fs->buffer;
			cos_fs->buffer_cur_size = q - p;
			memmove(cos_fs->buffer, p, cos_fs->buffer_cur_size);
			cos_fs->skip_header_line = 0;
		}
		
		/*
		 * If we need to read all the data up to the last *complete* logical
		 * line in the data buffer (like gpfdist for example) - we choose this
		 * path. We grab the bigger chunk we can get that includes whole lines.
		 * Otherwise, if we just want the whole buffer we skip.
		 */
		if (read_whole_lines)
		{
			char *p;
			ssize_t total_bytes = cos_fs->buffer_cur_size;
			
			assert(size >= buffer_capacity);
			
			if (total_bytes > 0)
			{
				/*
				 * source buffer is not empty. copy the data from the beginning
				 * up to the current length before moving on to reading more
				 */
				cos_fs->buffer_cur_size = 0;
				updateCosCurFileState(cos_fs, fo);
				memcpy(dest, cos_fs->buffer, total_bytes);
			}
			
			/* read more data from source file into destination buffer */
			bytesread2 = ossRead(context, cos_fs->obj, (char *) dest + total_bytes, size - total_bytes);
			if (bytesread2 < 0)
			{
				cos_fs->ferror = format_error("Read failed with error message %s\n", (char *) ossGetLastError());
				return -1;
			}
			
			/*
			 * If bytesread2 < size - total_bytes,
			 * the current file must have been read entirely.
			 * It is also possible bytesread2 = size - total_bytes.
			 */
			if (bytesread2 < size - total_bytes)
			{
				/*
				 * We didn't read as much as we asked for. Check why.
				 * We could be done reading data, we may need to move
				 * on the reading the next data file (if any).
				 */
				if (total_bytes == 0)
				{
					if (bytesread2 == 0)
					{
						/*
						 * buffer is empty and the entire file has been read
						 * try to open the next file
						 */
						if (nextOssObject(context, bucket, cos_fs))
							return -1; /* found next file but failed to open */
						continue;
					}
					updateCosCurFileState(cos_fs, fo);
				}
				
				/*
				 * try to open the next file if any, and return the number of
				 * bytes read to buffer earlier, if next file was found but
				 * could not open return -1
				 */
				return nextOssObject(context, bucket, cos_fs) ? -1 : total_bytes + bytesread2;
			}
			
			updateCosCurFileState(cos_fs, fo);
			
			/*
			 * Now that we have enough data in our filestream buffer, get a 
			 * chunk of whole rows and copy it into our dest buffer to be sent
			 * out later.
			 */
			if (cos_fs->options.is_csv)
			{
				/* CSV: go slow, scan byte-by-byte for record boundary */
				p = scan_csv_records_cos(dest, (char *) dest + size, 0, cos_fs);
			}
			else
			{
				/*
				 * TEXT: go fast, scan for end of line delimiter (\n by default) for
				 * record boundary.
				 * find the last end of line delimiter from the back
				 */
				if (line_delim_length > 0)
				{
					p = find_last_eol_delim((char *) dest, size, line_delim_str, line_delim_length);
				}
				else
				{
					for (p = (char *) dest + size; (char *) dest <= --p && *p != '\n';);
				}
				
				p = (char *) dest <= p ? p + 1 : 0;
				cos_fs->line_number = 0;
			}
			
			/* what if we could not find even one complete row in the buffer */
			if (!p || (char *) dest + size >= p + buffer_capacity)
			{
#ifdef WIN32
				snprintf(err_buf, sizeof(err_buf)-1, "line too long in file %s near (%ld bytes)",
						 cos_fs->object_result->name, (long) cos_fs->obj_off);
#else
				snprintf(err_buf, sizeof(err_buf) - 1, "line too long in file %s near (%lld bytes)",
				         cos_fs->object_result->name, (long long) cos_fs->obj_off);
#endif
				cos_fs->ferror = err_buf;
				gfile_printf_then_putc_newline("%s", err_buf);
				return -1;
			}
			
			/* copy the result chunk of data into our buffer */
			cos_fs->buffer_cur_size = (char *) dest + size - p;
			memcpy(cos_fs->buffer, p, cos_fs->buffer_cur_size);
			cos_fs->obj_off += p - (char *) dest;
			
			return p - (char *) dest;
		}
		
		/*
		 * if we're here it means that we just want chunks of data and don't
		 * care if it includes whole rows or not (for example, backend url_read
		 * code - a segdb that reads all the data that gpfdist sent to it,
		 * buffer by buffer and then parses the lines internally).
		 */
		
		if (cos_fs->buffer_cur_size)
		{
			ssize_t total_bytes = cos_fs->buffer_cur_size;
			
			updateCosCurFileState(cos_fs, fo);
			
			if (total_bytes > size)
				total_bytes = size;
			
			memcpy(dest, cos_fs->buffer, total_bytes);
			cos_fs->buffer_cur_size -= total_bytes;
			memmove(cos_fs->buffer, cos_fs->buffer + total_bytes, cos_fs->buffer_cur_size);
			
			cos_fs->obj_off += total_bytes;
			cos_fs->line_number = 0;
			
			return total_bytes;
		}
		
		bytesread = ossRead(context, cos_fs->obj, dest, size);
		
		if (bytesread < 0)
		{
			cos_fs->ferror = format_error("cannot read cos object, error message %s\n", (char *) ossGetLastError());
			
			return -1;
		}
		
		if (bytesread)
		{
			updateCosCurFileState(cos_fs, fo);
			cos_fs->obj_off += bytesread;
			cos_fs->line_number = 0;
			return bytesread;
		}
		
		if (nextOssObject(context, bucket, cos_fs))
			return -1;
	}
}

int64_t 
cos_fstream_get_compressed_size(cos_fstream_t *cos_fs)
{
	return cos_fs->compressed_size;
}

int64_t 
cos_fstream_get_compressed_position(cos_fstream_t *cos_fs)
{
	int64_t p = cos_fs->compressed_position;
	if (cos_fs->obj_idx != cos_fs->object_result->nObjects)
		p += cos_fs->compressed_position;
	return p;
}

const char *
cos_fstream_get_error(cos_fstream_t *cos_fs)
{
	return cos_fs->ferror;
}
