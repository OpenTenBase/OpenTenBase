#ifndef FSTREAM_H
#define FSTREAM_H

#include <fstream/gfile.h>
#include <sys/types.h>
/*#include "c.h"*/
#ifdef WIN32
typedef __int64 int64_t;
#endif
#include "liboss2/oss.h"

struct gpfxdist_t;

typedef struct fstream_options{
	int header;
	int is_csv;
	int verbose;
	char quote;		/* quote char */
	char escape;	/* escape char */
	int eol_type;
	int bufsize;
	int forwrite;   /* true for write, false for read */
	int usesync;    /* true if writes use O_SYNC */
	struct gpfxdist_t* transform;	/* for gpfxdist transformations */
} fstream_options;

typedef struct
{
	int 	gl_pathc;
	char**	gl_pathv;
} glob_and_copy_t;

struct fstream_t
{
	glob_and_copy_t glob;
	gfile_t 		fd;
	int 			fidx; /* current index in ffd[] */
	int64_t 		foff; /* current offset in ffd[fidx] */
	int64_t 		line_number;
	int64_t 		compressed_size;
	int64_t 		compressed_position;
	int 			skip_header_line;
	char* 			buffer;			 /* buffer to store data read from file */
	int 			buffer_cur_size; /* number of bytes in buffer currently */
	const char*		ferror; 		 /* error string */
	fstream_options options;
};

/* A file stream - data may come from several files */
typedef struct fstream_t fstream_t;

typedef struct cos_fstream_t
{
	char *buffer;                /* buffer to store data read from file */
	int buffer_cur_size;    /* number of bytes in buffer currently */
	
	ossObjectResult *object_result;
	
	/* Info for current reading object */
	ossObject obj;
	int obj_idx; /* current index in objects[] */
	int64_t obj_off; /* current offset in objects[obj_idx] */
	
	int64_t line_number;
	int64_t compressed_size;
	int64_t compressed_position;
	
	const char *ferror;            /* error string */
	fstream_options options;            /* copy_options from datanode */
	int skip_header_line;
} cos_fstream_t;

typedef struct fstream_filename_and_offset{
    char 	fname[256];
    int64_t line_number; /* Line number of first line in buffer.  Zero means fstream doesn't know the line number. */
    int64_t foff;
} fstream_filename_and_offset;

enum find_delim_error
{
	DELIM_FOUND = 0,
	DELIM_NOT_FOUND_MAXLEN,	/* delimiter not found until start + max_line_len */
	DELIM_NOT_FOUND_END		/* delimiter not found until end */
};

// If read_whole_lines, then size must be at least the value of -m (blocksize). */
int fstream_read(fstream_t* fs, void* buffer, int size,
				 struct fstream_filename_and_offset* fo,
				 const int read_whole_lines,
				 const char *line_delim_str,
				 const int line_delim_length);
int fstream_write(fstream_t *fs,
				  void *buf,
				  int size,
				  const int write_whole_lines,
				  const char *line_delim_str,
				  const int line_delim_length);
int fstream_eof(fstream_t* fs);
int64_t fstream_get_compressed_size(fstream_t* fs);
int64_t fstream_get_compressed_position(fstream_t* fs);
const char* fstream_get_error(fstream_t* fs);
fstream_t* fstream_open(const char* path, const fstream_options* options,
						int* response_code, const char** response_string);
void fstream_close(fstream_t* fs);
bool_t fstream_is_win_pipe(fstream_t *fs);

/* COS */
cos_fstream_t *cos_fstream_open(ossContext cosContext, const char *bucket, const char *prefix,
                                const fstream_options *options, int *response_code, const char **response_string);

void cos_fstream_close(ossContext cosContext, cos_fstream_t *cos_fs);

int cos_fstream_read(ossContext cosContext, const char *bucket, cos_fstream_t *cos_fs,
                     void *dest, int size, fstream_filename_and_offset *fo, const int read_whole_lines,
                     const char *line_delim_str, const int line_delim_length);

int cos_fstream_write(ossContext context, const char *bucket, cos_fstream_t *cos_fs,
                      void *buf, int size, const int write_whole_lines,
                      const char *line_delim_str, const int line_delim_length);
					  
int64_t cos_fstream_get_compressed_size(cos_fstream_t *cos_fs);

int64_t cos_fstream_get_compressed_position(cos_fstream_t *cos_fs);

const char *cos_fstream_get_error(cos_fstream_t *cos_fs);

#define DATA_STREAM_CLOSE( ) \
    do { \
            if (session->remote_protocol_type == REMOTE_COS_PROTOCOL) \
			{ \
				cos_fstream_close(cosContext, session->cos_fs);    \
				session->cos_fs = 0; \
			} \
            else if (session->remote_protocol_type == REMOTE_KAFKA_PROTOCOL) \
			{ \
				kafka_obj_close(session->kobj);    \
				session->kobj = NULL; \
			} \
            else             \
			{ \
				fstream_close(session->fstream);    \
				session->fstream = 0;   \
			} \
		} while (0)

#define IS_DATA_STREAM_NULL( ) \
        ((session->remote_protocol_type == REMOTE_COS_PROTOCOL) ? (0 == session->cos_fs) : \
        ((session->remote_protocol_type == REMOTE_KAFKA_PROTOCOL) ? (NULL == session->kobj) : (0 == session->fstream)))

#endif
