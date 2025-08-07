#ifndef TDX_TRANSFORM_H
#define TDX_TRANSFORM_H
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef NDEBUG
#undef NDEBUG
#endif
#include <assert.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <regex.h>

#include <apr.h>
#if APR_HAVE_UNISTD_H
#include <unistd.h>
#endif
#if APR_HAVE_IO_H
#include <io.h>
#endif

#include <apr_general.h>
#include <apr_thread_proc.h>
#include <apr_strings.h>

#include <yaml.h>
#include "utils/gpfxdist.h"
#include "liboss2/oss.h"
/*
 * YAML tdx configuration parser, stage 1.
 *
 * In this stage we build a simple structure from the YAML input.
 * For example the following YAML document
 *
 *  ---
 *      abc: 123
 *      xyz:
 *        def: 456
 *
 * results in the following memory structure:
 *
 *   streamstate        mapping
 *       document --->   nxt NULL
 *                       kvlist
 *                          |
 *                          v
 *                      keyvalue         keyvalue
 *                        nxt --------->   nxt -----> ...
 *                        type KV_SCALAR   type KV_MAPPING
 *                        key "abc"        key "xyz"
 *                        scalar "123"     scalar NULL
 *                        mapping NULL     mapping
 *                                           |
 *                                           v
 *                                       mapping
 *                                          nxt ---> ...
 *                                          kvlist
 *                                             |
 *                                             v
 *                                         keyvalue
 *                                           nxt NULL
 *                                           type KV_SCALAR
 *                                           key "def"
 *                                           scalar "456"
 *                                           mapping NULL
 *
 * At this stage we're only concerned about verifying the general
 * syntax and structure of the document.  Note that the tdx configuration
 * does not make use of sequences or aliases so we treat these as errors here.
 */


struct keyvalue
{
    struct keyvalue*  nxt;              /* next keyvalue in linked list or NULL */
#define KV_UNKNOWN 0
#define KV_SCALAR   1
#define KV_MAPPING  2
#define KV_SEQUENCE 3
    int               type;             /* type of keyvalue (KV_SCALAR or KV_MAPPING) */
#define MAX_KEYLEN 256
    char*             key;              /* this keyvalue's key */
    char*             scalar;           /* for KV_SCALAR, this keyvalue's scalar value */
    struct mapping*   map;              /* for KV_MAPPING, this keyvalue's mapping value */
    struct sequence*  seq;              /* for KV_SEQUENCE, this keyvalue's sequence value */
    yaml_mark_t       keymark;          /* index, line, column of key */
    yaml_mark_t       valuemark;        /* index, line, column of scalar or mapping */
};

struct mapping
{
    struct mapping*   nxt;              /* next mapping in linked list or NULL */
    struct keyvalue*  kvlist;           /* linked list of keyvalues in this mapping */
};

struct sequence
{
	struct sequence*    nxt;                /* next sequence in linked list or NULL */
	struct keyvalue*    kvlist;             /* linked list of keyvalues in this sequence */
};

struct streamstate
{
    const char*       filename;         /* name of YAML file (for error reporting) */
    yaml_parser_t     parser;           /* YAML parser */
    yaml_event_t      event;            /* current YAML event */
    int               stream_start;     /* 1 if between stream start and stream end */
    int               document_start;   /* 1 if between document start and document end */
    struct mapping*   document;         /* top level mapping "document" */
    struct mapping*   curmap;           /* current mapping being built */
	struct sequence*  cursequence;      /* current sequence being built */
    struct keyvalue*  curkv;            /* current keyvalue being built */
    char              errbuf[1024];     /* buffer for error message */
};

/*
 * debugging/dumping/error handling
 */

char* event_type(yaml_event_t* ep);
void debug_event_type(yaml_event_t* ep);

void debug_mapping(struct mapping* map, int indent);
void debug_keyvalue(struct keyvalue* kv, int indent);

/*
 * yaml structural parse errors
 */

int unexpected_event(struct streamstate* stp);
int error_parser_initialize_failed(struct streamstate* stp);
int error_parser_parse_failed(struct streamstate* stp);
int error_invalid_stream_start(struct streamstate* stp);
int error_stream_not_started(struct streamstate* stp, char* what);
int error_invalid_document_start(struct streamstate* stp);
int error_document_not_started(struct streamstate* stp, char* what);
int error_no_current_mapping(struct streamstate* stp, char* what);

/*
 * parse tree construction
 */

char* copy_scalar(struct streamstate* stp);
struct keyvalue* new_keyvalue(struct streamstate* stp, struct keyvalue* nxt);
struct mapping* new_mapping(struct streamstate* stp, struct mapping* nxt);
int handle_mapping_start(struct streamstate* stp);
int handle_mapping_end(struct streamstate* stp);
int handle_scalar(struct streamstate* stp);
int stage1_parse(struct streamstate* stp, FILE* file, int verbose);

/*
 * YAML tdx configuration parser, stage 2.
 *
 * In this stage we construct the list of transformations from the mappings
 * built in stage 1 and validate that the values are legal.  The final result
 * is fairly straightforward:
 *
 *   parsestate        transform         transform
 *       trlist ----->   nxt ---------->   nxt --------> ...
 *                       type              type
 *                       command           command
 *
 */

struct transform
{
    struct transform* nxt;              /* next transform in linked list or NULL */
    struct keyvalue*  kv;               /* keyvalue whose key is the name of this transform */
#define TR_UNKNOWN 0
#define TR_INPUT   1
#define TR_OUTPUT  2
    int               type;             /* type of transform (TR_INPUT or TR_OUTPUT) */
    char*             command;          /* command string to execute */
#define TR_FN_UNKNOWN 0
#define TR_FN_DATA    1
#define TR_FN_PATHS   2
	int               content;          /* what transform expects %filename% to contain (data or path names) */
	char*             safe;             /* if set, regex specifying safe file names */
	regex_t           saferegex;        /* compiled form of safe */
#define TR_ER_UNKNOWN 0
#define TR_ER_CONSOLE 1
#define TR_ER_SERVER  2
    int               errs;             /* where stderr output should go (console or server) */
};
#define JSON_FORMAT 1

typedef struct kafka_msg_info
{
	int format;
	char *op_pos;
	int nfields;
	char *before_pos;
	char *after_pos;
	bool datetime_need_convert;
	size_t k_msg_batch; /* Limit on the number of Kafka messages retrieved */
	int k_timeout_ms;   /* Kafka response timeout */
} kafka_msg_info;

typedef struct datasource
{
	struct datasource *nxt;             /* next transform in linked list or NULL */
	struct keyvalue *kv;                /* keyvalue whose key is the name of this transform */
	
	int proto_type;                     /* REMOTE_COS_PROTOCOL*/
	ossConf *oss_conf;
	rd_kafka_conf_t *kafka_conf;
	kafka_msg_info *kafka_msg_format;
} datasource;

struct parsestate
{
	const char *filename;               /* name of YAML file (for error reporting) */
	struct transform *trlist;           /* list of transformations in the YAML file */
	datasource *dslist;                 /* list of data protovol conf in the YAML file */
	char *version_str;                  /* version of TDX */
	char errbuf[1024];                  /* buffer for error message */
};

/*
 * debugging/dumping/error handling
 */

void dump_transform(struct transform* tr);
void dump_datasource(datasource *do_conf);
void debug_transforms(struct transform* trlist);
int format_onlyfilename(struct parsestate* psp, char* fmt, char *extra_str);
int format_key1(struct parsestate* psp, char* fmt, yaml_mark_t mark, char* key);
int format_key2(struct parsestate* psp, char* fmt, yaml_mark_t mark, char* key, char* value);
int format_key3(struct parsestate* psp, char* fmt, yaml_mark_t mark, char* key, char* value1, char *value2);

#define ERRFMT "%s:%d:%d:"
int error_failed_to_find_key_for_conf(struct parsestate* psp, struct keyvalue* kv, char *keyword, char *conf_name);
int error_not_proper_yaml_map(struct parsestate* psp, struct keyvalue* kv);
int error_not_supported_x(struct parsestate* psp, struct keyvalue* kv);
int error_failed_to_find_command_for_transformation(struct parsestate* psp, struct keyvalue* kv);
int error_invalid_command_for_transformation(struct parsestate* psp, struct keyvalue* trkv, struct keyvalue* kv);
int error_stderr_not_scalar(struct parsestate* psp, struct keyvalue* trkv, struct keyvalue* kv);
int error_content_not_scalar(struct parsestate* psp, struct keyvalue* trkv, struct keyvalue* kv);
int error_safe_not_scalar(struct parsestate* psp, struct keyvalue* trkv, struct keyvalue* kv);
int error_safe_not_valid_regex(struct parsestate* psp, struct keyvalue* trkv, struct keyvalue* kv, regex_t* r, int rc);


/*
 * transform validation and construction
 */

struct transform* new_transform(struct parsestate* psp, struct transform* nxt);
struct keyvalue* find_keyvalue(struct keyvalue* kvlist, char* name);
int validate_transform(struct parsestate* psp, struct transform* tr, struct mapping* map);
int validate_transformation(struct parsestate* psp, struct keyvalue* kv);
int validate_datasource(struct parsestate* psp, struct keyvalue* kv);


/*
 * configure transforms
 */

extern int transform_config(const char* filename, struct transform** trlistp, datasource **dolistp, int verbose);


/*
 * lookup transformation
 */

struct transform* transform_lookup(struct transform* trlist, const char* name, int for_write, int verbose);

datasource *datasource_lookup(datasource* dplist, const char* name, int verbose);

/*
 * transformation accessors
 */

char* transform_command(struct transform* tr);
int transform_stderr_server(struct transform* tr);
int transform_content_paths(struct transform* tr);
char* transform_safe(struct transform* tr);
regex_t* transform_saferegex(struct transform* tr);

#define TRANSFORM_CONF "TRANSFORMATIONS"
#define DATA_SOURCE_CONF "DATASOURCE"
#define VERSION_CONF "VERSION"

#define TX_TYPE "QCloud"

#define RD_KAFKA_CONF_SET_CONF_CHECK(kafka_conf, kafka_setting, kafka_setting_value, errstr) \
if (rd_kafka_conf_set(kafka_conf, kafka_setting, kafka_setting_value, errstr, sizeof(errstr))) \
    { \
    char errmsg[1024] = {0}; \
    sprintf(errmsg, "Failed to set '%s' Kafka configuration parameter: %s", kafka_setting, errstr);                                                                                         \
    gwarning(r, "%s", errmsg); \
    http_error(r, FDIST_BAD_REQUEST, errmsg); \
    request_end(r, 1, 0); \
    apr_pool_destroy(pool); \
    return -1; \
    }

#endif
