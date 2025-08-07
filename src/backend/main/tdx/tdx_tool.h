#ifndef TDX_HELPER_H
#define TDX_HELPER_H
#include <stdbool.h>
#include <json-c/json.h>
#include <iconv.h>
#include "transform.h"
#include "utils/gpfxdist.h"
#include "lib/stringinfo.h"
#include "utils/timestamp.h"
#include "catalog/pg_type.h"


/*	Struct of command line options */
struct optional
{
	int p; /* port */
	int last_port;
	int v; /* verbose */
	int V; /* very verbose */
	int s;
	const char *d; /* directory */
	const char *l; /* log filename */
	const char *f; /* forced filename */
	int g; /* tdx_proto (0 or 1) (internal, not documented) */
	int t; /* timeout in seconds */
	const char *b; /* IP address to bind (internal, not documented) */
	int m; /* max data line len */
	int S; /* use O_SYNC when opening files for write  */
	int z; /* listen queue size (hidden option currently for debugging) */
	const char *c; /* config file */
	struct transform *trlist; /* transforms from config file */
	datasource *dslist;         /* remote-data-source(cos/kakfa) configuration from config file */
	const char *ssl; /* path to certificates in case we use tdx with ssl */
	int w; /* The time used for session timeout in seconds */
	int e; /* The OpenTenBase server encoding set */
	int parallel; /* The max number thread tdx will launch. */
	int thread_pool_size;
};

typedef struct line_item
{
	char op;
	char **raw_fields_before;
	bool *isnull_before;
	bool has_before;
	char **raw_fields_after;
	bool *isnull_after;
	bool has_after;
} line_item;

struct optional opt;
bool is_valid_timeout(int timeout_val);
bool is_valid_session_timeout(int timeout_val);
bool is_valid_listen_queue_size(int listen_queue_size);
bool json_deserialize(json_c_object *jobject, session_t *session, line_item* ret);

extern char *datetime_now(void);
extern char *datetime(apr_time_t t);
extern void gprint(const request_t *r, const char *fmt, ...);
extern void gprintlnif(const request_t *r, const char *fmt, ...);
extern void gdebug(const request_t *r, const char *fmt, ...);
extern void gwarning(const request_t *r, const char *fmt, ...);
extern long datetime_diff(apr_time_t t1, apr_time_t t2);
#endif
