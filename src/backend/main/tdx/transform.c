#ifdef WIN32
/* don't build this for now */
#else
#include "transform.h"

/*
 * debugging/dumping/error handling
 */

char*
event_type(yaml_event_t* ep) 
{
    switch (ep->type) 
	{
        case YAML_NO_EVENT:             return("YAML_NO_EVENT\n");              break;
        case YAML_STREAM_START_EVENT:   return("YAML_STREAM_START_EVENT\n");    break;
        case YAML_STREAM_END_EVENT:     return("YAML_STREAM_END_EVENT\n");      break;
        case YAML_DOCUMENT_START_EVENT: return("YAML_DOCUMENT_START_EVENT\n");  break;
        case YAML_DOCUMENT_END_EVENT:   return("YAML_DOCUMENT_END_EVENT\n");    break;
        case YAML_ALIAS_EVENT:          return("YAML_ALIAS_EVENT\n");           break;
        case YAML_SCALAR_EVENT:         return("YAML_SCALAR_EVENT\n");          break;
        case YAML_SEQUENCE_START_EVENT: return("YAML_SEQUENCE_START_EVENT\n");  break;
        case YAML_SEQUENCE_END_EVENT:   return("YAML_SEQUENCE_END_EVENT\n");    break;
        case YAML_MAPPING_START_EVENT:  return("YAML_MAPPING_START_EVENT\n");   break;
        case YAML_MAPPING_END_EVENT:    return("YAML_MAPPING_END_EVENT\n");     break;
        default:                        return("unknown event\n");              break;
    }
}

void
debug_event_type(yaml_event_t* ep) 
{
	printf("%s", event_type(ep));
}


/* forward decl for debug_keyvalue */
void 
debug_mapping(struct mapping* map, int indent);

void
debug_keyvalue(struct keyvalue* kv, int indent)
{
    printf("%*s%s: ", indent, "", kv->key);
    if (kv->type == KV_SCALAR)
        printf("%s\n", kv->scalar);
    else 
    {
        printf("\n");
        if (kv->map)
            debug_mapping(kv->map, indent+1);
    }
}

void
debug_mapping(struct mapping* map, int indent)
{
    struct keyvalue* kv;
    for (kv = map->kvlist; kv; kv=kv->nxt)
        debug_keyvalue(kv, indent);
}


/*
 * yaml structural parse errors
 */

int
unexpected_event(struct streamstate* stp) 
{
    fprintf(stderr, "%s:%zu:%zu:unexpected event: %s\n",
			stp->filename, stp->event.start_mark.line+1, stp->event.start_mark.column+1, 
			event_type(&stp->event));
	return 1;
}

int
error_parser_initialize_failed(struct streamstate* stp) 
{
    fprintf(stderr, "%s:0:0:yaml_parser_initialize failed\n", stp->filename);
	return 1;
}

int
error_parser_parse_failed(struct streamstate* stp) 
{
    fprintf(stderr, "%s:%zu:%zu:yaml_parser_parse failed\n",
           stp->filename, stp->event.start_mark.line+1, stp->event.start_mark.column+1);
	return 1;
}

int
error_invalid_stream_start(struct streamstate* stp) 
{
    fprintf(stderr, "%s:%zu:%zu:stream_start_event but stream has already been started\n",
			stp->filename, stp->event.start_mark.line+1, stp->event.start_mark.column+1);
	return 1;
}

int
error_stream_not_started(struct streamstate* stp, char* what) 
{
    fprintf(stderr, "%s:%zu:%zu:%s but stream has not been started\n",
			stp->filename, stp->event.start_mark.line+1, stp->event.start_mark.column+1, what);
	return 1;
}

int
error_invalid_document_start(struct streamstate* stp) 
{
    fprintf(stderr, "%s:%zu:%zu:document_start but already within a document\n",
			stp->filename, stp->event.start_mark.line+1, stp->event.start_mark.column+1);
	return 1;
}

int
error_document_not_started(struct streamstate* stp, char* what) 
{
    fprintf(stderr, "%s:%zu:%zu:%s but not within a document\n",
			stp->filename, stp->event.start_mark.line+1, stp->event.start_mark.column+1, what);
	return 1;
}

int
error_no_current_mapping(struct streamstate* stp, char* what) 
{
    fprintf(stderr, "%s:%zu:%zu:%s but not within a mapping\n",
			stp->filename, stp->event.start_mark.line+1, stp->event.start_mark.column+1, what);
	return 1;
}


/*
 * parse tree construction
 */

char* 
copy_scalar(struct streamstate* stp)
{
    unsigned char* src  = stp->event.data.scalar.value;
    size_t         len  = stp->event.data.scalar.length;    
    char*          copy = malloc(len+1);
    if (copy) 
    {
        memcpy(copy, src, len);
        copy[len] = 0;
    } else
        snprintf(stp->errbuf, sizeof(stp->errbuf), 
				 "%s:%zu:%zu:allocation failed for malloc(%zu)", 
                 stp->filename, stp->event.start_mark.line+1, stp->event.start_mark.column+1, len+1);
    return copy;
}

struct keyvalue*
new_keyvalue(struct streamstate* stp, struct keyvalue* nxt)
{
    struct keyvalue* kv = calloc(1, sizeof(struct keyvalue));
    if (kv)
        kv->nxt = nxt;
    else
        snprintf(stp->errbuf, sizeof(stp->errbuf), 
				 "%s:%zu:%zu:allocation failed for calloc(1, sizeof(struct keyvalue))",
                 stp->filename, stp->event.start_mark.line+1, stp->event.start_mark.column+1);
    return kv;


}

struct mapping*
new_mapping(struct streamstate* stp, struct mapping* nxt)
{
    struct mapping* map = calloc(1, sizeof(struct mapping));
    if (map)
        map->nxt = nxt;
    else
        snprintf(stp->errbuf, sizeof(stp->errbuf), 
				 "%s:%zu:%zu:allocation failed for calloc(1, sizeof(struct mapping))",
                 stp->filename, stp->event.start_mark.line+1, stp->event.start_mark.column+1);
    return map;
}

static struct sequence *
new_sequence(struct streamstate *stp, struct sequence *nxt)
{
	struct sequence *seq = calloc(1, sizeof(struct sequence));
	if (seq)
		seq->nxt = nxt;
	else
		snprintf(stp->errbuf, sizeof(stp->errbuf),
		         "%s:%zu:%zu:allocation failed for calloc(1, sizeof(struct sequence))",
		         stp->filename, stp->event.start_mark.line + 1, stp->event.start_mark.column + 1);
	return seq;
}

int
handle_mapping_start(struct streamstate* stp)
{
	struct keyvalue* kv;

    stp->curmap = new_mapping(stp, stp->curmap);
    if (! stp->curmap)
        return 1;

    kv = stp->curkv;
    if (kv) 
    {
        /*
         * this mapping is the value for the current keyvalue
         */
        kv->type      = KV_MAPPING;
        kv->map       = stp->curmap;
        kv->valuemark = stp->event.start_mark;
        stp->curkv = NULL;
    } 
    else 
    {
        /*
         * this mapping is the top level document.
         */
        if (! stp->document)
            stp->document = stp->curmap;
    }
    return 0;
}

static int
handle_sequence_start(struct streamstate *stp)
{
	struct keyvalue *kv;

	stp->cursequence = new_sequence(stp, stp->cursequence);
	if (!stp->cursequence)
		return 1;
	
	kv = stp->curkv;
	if (kv)
	{
		/*
		 * this sequence is the value for the current keyvalue
		 */
		kv->type = KV_SEQUENCE;
		kv->seq = stp->cursequence;
		kv->valuemark = stp->event.start_mark;
		stp->curkv = NULL;
	}
	else
	{
		/*
		 * this mapping is the top level document.
		 */
		if (!stp->document)
			stp->document = stp->curmap;
	}
	return 0;
}

int
handle_mapping_end(struct streamstate* stp)
{
    stp->curmap = stp->curmap->nxt; /* pop mapping from stack */
    return 0;
}

static int
handle_sequence_end(struct streamstate *stp)
{
	stp->cursequence = stp->cursequence->nxt; /* pop sequence from stack */
	return 0;
}

int
handle_scalar(struct streamstate* stp)
{
    struct mapping*  map = stp->curmap;
    struct keyvalue* kv  = stp->curkv;
    if (kv) 
    {
        /*
         * this scalar is the value for current keyvalue
         */
        kv->type         = KV_SCALAR;
        kv->scalar       = copy_scalar(stp);
        if (! kv->scalar)
            return 1;
        kv->valuemark    = stp->event.start_mark;
        stp->curkv       = NULL;
    } 
    else 
    {
        /*
         * this scalar is the name of a new keyvalue pair
         */
        if (stp->event.data.scalar.length > MAX_KEYLEN) 
        {
            snprintf(stp->errbuf, sizeof(stp->errbuf), 
					 "%s:%zu:%zu:key name exceeds maximum length %d",
                     stp->filename, stp->event.start_mark.line+1, stp->event.start_mark.column+1, MAX_KEYLEN);
            return 1;
        }
        kv = stp->curkv  = map->kvlist = new_keyvalue(stp, map->kvlist);
        if (! kv)
            return 1;
        kv->key          = copy_scalar(stp);
        if (! kv->key)
            return 1;
        kv->keymark      = stp->event.start_mark;
    }
    return 0;
}


int
stage1_parse(struct streamstate* stp, FILE* file, int verbose)
{
    int rc = 0;
    int done  = 0;
    int error = 0;

    rc = yaml_parser_initialize(&stp->parser);
	if (! rc) 
		return error_parser_initialize_failed(stp);

    yaml_parser_set_input_file(&stp->parser, file);

    while (!done) 
    {
        rc = yaml_parser_parse(&stp->parser, &stp->event);
        if (!rc) 
		{
			error = error_parser_parse_failed(stp);
			break;
		}

        rc = 0;
        switch (stp->event.type) 
        {
            case YAML_STREAM_START_EVENT:
				if (stp->stream_start)
					rc = error_invalid_stream_start(stp);
				else
					stp->stream_start = 1;
                break;

            case YAML_DOCUMENT_START_EVENT:
				if (stp->document_start)
					rc = error_invalid_document_start(stp);
				else
					stp->document_start = 1;
                break;

            case YAML_MAPPING_START_EVENT:
				if (! stp->stream_start)
					rc = error_stream_not_started(stp, "mapping_start_event");
				else if (! stp->document_start)
					rc = error_document_not_started(stp, "mapping_start_event");
				else
					rc = handle_mapping_start(stp);
                break;


            case YAML_MAPPING_END_EVENT:
				if (! stp->stream_start)
					rc = error_stream_not_started(stp, "mapping_end_event");
				else if (! stp->document_start)
					rc = error_document_not_started(stp, "mapping_end_event");
				else if (stp->curmap == NULL) 
					rc = error_no_current_mapping(stp, "mapping_end_event");
				else
					rc = handle_mapping_end(stp);
                break;

	        case YAML_SEQUENCE_START_EVENT:
		        if (!stp->stream_start)
			        rc = error_stream_not_started(stp, "sequence_start_event");
		        else if (!stp->document_start)
			        rc = error_document_not_started(stp, "sequence_start_event");
		        else
			        rc = handle_sequence_start(stp);
		        break;
	
	
	        case YAML_SEQUENCE_END_EVENT:
		        if (!stp->stream_start)
			        rc = error_stream_not_started(stp, "sequence_end_event");
		        else if (!stp->document_start)
			        rc = error_document_not_started(stp, "sequence_end_event");
		        else if (stp->curmap == NULL)
			        rc = error_no_current_mapping(stp, "mapping_end_event");
		        else
			        rc = handle_sequence_end(stp);
		        break;

            case YAML_SCALAR_EVENT:
				if (! stp->stream_start)
					rc = error_stream_not_started(stp, "scalar_event");
				else if (! stp->document_start)
					rc = error_document_not_started(stp, "scalar_event");
				else if (stp->curmap == NULL) 
					rc = error_no_current_mapping(stp, "scalar_event");
				else
					rc = handle_scalar(stp);
                break;

            case YAML_DOCUMENT_END_EVENT:
				if (! stp->stream_start)
					rc = error_stream_not_started(stp, "document_end_event");
				else if (! stp->document_start)
					rc = error_document_not_started(stp, "document_end_event");
				else
					stp->document_start = 0;
                break;

            case YAML_STREAM_END_EVENT:
				if (! stp->stream_start)
					rc = error_stream_not_started(stp, "stream_end_event");
				else
				{
					stp->stream_start = 0;
					done = 1;
				}
                break;
            default:
                rc = unexpected_event(stp);
                break;
        }

        yaml_event_delete(&stp->event);
        if (rc) 
        {
            error = 1;
            break;
        }
    }

    yaml_parser_delete(&stp->parser);
    return error;
}

/*
 * debugging/dumping/error handling
 */

void
dump_transform(struct transform* tr)
{
	printf(" %s:\n", tr->kv->key);
	printf("  TYPE: %s\n", tr->type == TR_INPUT ? "input" : (tr->type == TR_OUTPUT ? "output" : "unknown")); 
	printf("  COMMAND: %s\n", tr->command);
	printf("  STDERR: %s\n", tr->errs == TR_ER_CONSOLE ? "console" : (tr->errs == TR_ER_SERVER ? "server" : "unknown")); 
	printf("  CONTENTS: %s\n", tr->content == TR_FN_DATA ? "data" : (tr->content == TR_FN_PATHS ? "paths" : "unknown"));
	if (tr->safe)
		printf("  SAFE: %s\n", tr->safe);
}

void
debug_transforms(struct transform* trlist)
{
    struct transform* tr;
    printf("TRANSFORMATIONS:\n");
    for (tr = trlist; tr; tr=tr->nxt) 
		dump_transform(tr);
}

int
format_onlyfilename(struct parsestate* psp, char* fmt, char *extra_str)
{
    snprintf(psp->errbuf, sizeof(psp->errbuf), fmt, psp->filename, extra_str);
    return 1;
}

int
format_key1(struct parsestate* psp, char* fmt, yaml_mark_t mark, char* key)
{
    snprintf(psp->errbuf, sizeof(psp->errbuf), fmt, psp->filename, mark.line+1, mark.column+1, key);
    return 1;
}

int
format_key2(struct parsestate* psp, char* fmt, yaml_mark_t mark, char* key, char* value)
{
    snprintf(psp->errbuf, sizeof(psp->errbuf), fmt, psp->filename, mark.line+1, mark.column+1, key, value);
    return 1;
}

int
format_key3(struct parsestate* psp, char* fmt, yaml_mark_t mark,  char* key, char* value1, char* value2)
{
    snprintf(psp->errbuf, sizeof(psp->errbuf), fmt, psp->filename, mark.line+1, mark.column+1, key, value1, value2);
    return 1;
}
static int 
format_key4(struct parsestate* psp, char* fmt, yaml_mark_t mark,  char* key, char* value1, char* value2, char *value3)
{
    snprintf(psp->errbuf, sizeof(psp->errbuf), fmt, psp->filename, mark.line+1, mark.column+1, key, value1, value2, value3);
    return 1;
}

int
error_not_proper_yaml_map(struct parsestate* psp, struct keyvalue* kv)
{
    char* fmt = ERRFMT "key '%s' is not a proper YAML map\n";
    return format_key1(psp, fmt, kv->keymark, kv->key);
}

int
error_not_supported_x(struct parsestate *psp, struct keyvalue *kv)
{
	char *fmt = ERRFMT "%s '%s': not a supported %s\n";
	return format_key3(psp, fmt, kv->keymark, kv->key, kv->scalar, kv->key);
}

int
error_failed_to_find_key_for_conf(struct parsestate* psp, struct keyvalue* kv, char *keyword, char *conf_name)
{
    char* fmt = ERRFMT "%s '%s': failed to find %s\n";
    return format_key3(psp, fmt, kv->keymark, conf_name, kv->key, keyword);
}

static int
error_conf_key_not_scalar(struct parsestate* psp, struct keyvalue* trkv, struct keyvalue* kv, char *keyword, char *conf_name)
{
    char* fmt = ERRFMT "%s '%s': %s not a scalar\n";
    return format_key3(psp, fmt, kv->keymark, conf_name, trkv->key, keyword);
}

static int
error_conf_key_not_mapping(struct parsestate *psp, struct keyvalue *trkv, struct keyvalue *kv, char *keyword, char *conf_name)
{
	char *fmt = ERRFMT "%s '%s': %s not a mapping\n";
	return format_key3(psp, fmt, kv->keymark, conf_name, trkv->key, keyword);
}

static int
error_invalid_key_for_conf(struct parsestate* psp, struct keyvalue* trkv, struct keyvalue* kv, char *keyword, char *conf_name)
{
    char* fmt = ERRFMT "%s '%s': invalid %s: '%s' (expected: QCloud)\n";
    return format_key4(psp, fmt, kv->keymark, conf_name, trkv->key, keyword, kv->scalar);
}

static int
error_fail_to_set_kafka_conf(struct parsestate *psp, struct keyvalue *kv,
                                 char *keyword, char *conf_name, char *extra_err)
{
	char *fmt = ERRFMT "Failed to set %s conf parameter '%s' (taken from option '%s'): %s\n";
	snprintf(psp->errbuf, sizeof(psp->errbuf), fmt, psp->filename, kv->keymark.line + 1, kv->keymark.column + 1,
	         keyword, conf_name, kv->scalar, extra_err);
	return 1;
}

static int
error_fail_to_set_kafka_conf1(struct parsestate *psp, char *keyword, char *conf_name, char *extra_err)
{
	char *fmt = "%s: Failed to set %s conf parameter '%s' : %s\n";
	snprintf(psp->errbuf, sizeof(psp->errbuf), fmt, psp->filename, keyword, conf_name, extra_err);
	return 1;
}

int
error_failed_to_find_command_for_transformation(struct parsestate* psp, struct keyvalue* kv)
{
    char* fmt = ERRFMT "transformation '%s': failed to find COMMAND\n";
    return format_key1(psp, fmt, kv->keymark, kv->key);
}

int
error_invalid_command_for_transformation(struct parsestate* psp, struct keyvalue* trkv, struct keyvalue* kv)
{
    char* fmt = ERRFMT "transformation '%s': invalid COMMAND (expected a simple non-empty string)\n";
    return format_key1(psp, fmt, kv->keymark, trkv->key);
}

int
error_stderr_not_scalar(struct parsestate* psp, struct keyvalue* trkv, struct keyvalue* kv)
{
	char* fmt = ERRFMT "transformation '%s': STDERR not a scalar (expected a simple non-empty string)\n";
	return format_key1(psp, fmt, kv->keymark, trkv->key);
}

int
error_content_not_scalar(struct parsestate* psp, struct keyvalue* trkv, struct keyvalue* kv)
{
	char* fmt = ERRFMT "transformation '%s': CONTENT not a scalar (expected CONTENT:data or CONTENT:paths)\n";
	return format_key1(psp, fmt, kv->keymark, trkv->key);
}

int
error_safe_not_scalar(struct parsestate* psp, struct keyvalue* trkv, struct keyvalue* kv)
{
    char* fmt = ERRFMT "transformation '%s': SAFE not a scalar (expected a simple non-empty string)\n";
    return format_key1(psp, fmt, kv->keymark, trkv->key);
}

int
error_safe_not_valid_regex(struct parsestate* psp, struct keyvalue* trkv, struct keyvalue* kv, regex_t* r, int rc)
{
	char* fmt = ERRFMT "transformation '%s': could not compile SAFE regex: ";
	int   len = snprintf(psp->errbuf, sizeof(psp->errbuf), fmt, psp->filename, kv->keymark.line+1, kv->keymark.column+1, trkv->key);
	char* end = psp->errbuf + len;
	char* lim = psp->errbuf + sizeof(psp->errbuf) - 2;
	if (end < lim) {
		regerror(rc, r, end, lim-end);
		end += strlen(end);
		*end++ = '\n';
		*end++ = 0;
	}
	return 1;
}

static int
error_not_handled_conf(struct parsestate* psp, char* keyword)
{
	return format_onlyfilename(psp, "%s:0:0:not handled conf %s", keyword);
}

/*
 * transform validation and construction
 */

struct transform*
new_transform(struct parsestate* psp, struct transform* nxt)
{
    struct transform* tr = calloc(1, sizeof(struct transform));
    if (tr)
        tr->nxt = nxt;
    else
        snprintf(psp->errbuf, sizeof(psp->errbuf), 
				 "%s:0:0:allocation failed for calloc(1, sizeof(struct transform))\n",
                 psp->filename);
    return tr;
}

static datasource *
new_cosconf(struct parsestate *psp, datasource *nxt)
{
	datasource *ds = calloc(1, sizeof(datasource));
	if (ds)
	{
		ds->proto_type = REMOTE_COS_PROTOCOL;
		ds->nxt = nxt;
		ds->oss_conf = (ossConf *) calloc(1, sizeof(ossConf));
		if (!ds->oss_conf)
			snprintf(psp->errbuf, sizeof(psp->errbuf),
			         "%s:0:0:allocation failed for calloc(1, sizeof(ossConf))\n",
			         psp->filename);
	}
	else
		snprintf(psp->errbuf, sizeof(psp->errbuf),
		         "%s:0:0:allocation failed for calloc(1, sizeof(struct datasource))\n",
		         psp->filename);
	return ds;
}

static datasource *
new_kafkaconf(struct parsestate *psp, datasource *nxt)
{
	datasource *ds = calloc(1, sizeof(datasource));
	if (ds)
	{
		ds->proto_type = REMOTE_KAFKA_PROTOCOL;
		ds->nxt = nxt;
		ds->kafka_conf = rd_kafka_conf_new();
		if (!ds->kafka_conf)
			snprintf(psp->errbuf, sizeof(psp->errbuf),
			         "%s:0:0:allocation failed for kafka_conf\n",
			         psp->filename);
		ds->kafka_msg_format = calloc(1, sizeof(kafka_msg_info));
	}
	else
		snprintf(psp->errbuf, sizeof(psp->errbuf),
		         "%s:0:0:allocation failed for kafka_conf\n",
		         psp->filename);
	return ds;
}

struct keyvalue*
find_keyvalue(struct keyvalue* kvlist, char* name)
{
    struct keyvalue* kv;
    for (kv = kvlist; kv; kv=kv->nxt) 
    {
        if (strcmp(kv->key, name) == 0)
            return kv;
    }
    return NULL;
}

int
validate_transform(struct parsestate* psp, struct transform* tr, struct mapping* map)
{
    struct keyvalue* kv = NULL;

    kv = find_keyvalue(map->kvlist, "TYPE");
    if (! kv)
        return error_failed_to_find_key_for_conf(psp, tr->kv, "TYPE", TRANSFORM_CONF);

    if (kv->type != KV_SCALAR)
        return error_conf_key_not_scalar(psp, tr->kv, kv, "TYPE", TRANSFORM_CONF);

    if (0 == strcasecmp(kv->scalar, "input"))
        tr->type = TR_INPUT;
    else if (0 == strcasecmp(kv->scalar, "output"))
        tr->type = TR_OUTPUT;
    else
        return error_invalid_key_for_conf(psp, tr->kv, kv, "TYPE", TRANSFORM_CONF);
    
    kv = find_keyvalue(map->kvlist, "COMMAND");
    if (! kv)
        return error_failed_to_find_command_for_transformation(psp, tr->kv);

    if (kv->type != KV_SCALAR || strlen(kv->scalar) < 1)
        return error_invalid_command_for_transformation(psp, tr->kv, kv);

    tr->command = kv->scalar;

	kv = find_keyvalue(map->kvlist, "STDERR");
	if (kv) 
	{
		if (kv->type != KV_SCALAR)
			return error_stderr_not_scalar(psp, tr->kv, kv);

		if (0 == strcasecmp(kv->scalar, "console"))
			tr->errs = TR_ER_CONSOLE;
		else if (0 == strcasecmp(kv->scalar, "server"))
			tr->errs = TR_ER_SERVER;
		else
			return error_invalid_key_for_conf(psp, tr->kv, kv, "STDERR", TRANSFORM_CONF);
	}
	else
	{
		/* 'server' is the default when stderr is not specified */
		tr->errs = TR_ER_SERVER;
	}

    kv = find_keyvalue(map->kvlist, "CONTENT");
    if (kv) 
	{
		if (kv->type != KV_SCALAR)
			return error_content_not_scalar(psp, tr->kv, kv);

		if (0 == strcasecmp(kv->scalar, "data"))
			tr->content = TR_FN_DATA;
		else if (0 == strcasecmp(kv->scalar, "paths"))
			tr->content = TR_FN_PATHS;
		else
			return error_invalid_key_for_conf(psp, tr->kv, kv, "CONTENT", TRANSFORM_CONF);
	}
	else
	{
		/* 'data' is the default when filename_contents is not specified */
		tr->content = TR_FN_DATA;
	}

    kv = find_keyvalue(map->kvlist, "SAFE");
    if (kv) 
	{
		int rc;

		if (kv->type != KV_SCALAR || strlen(kv->scalar) < 1)
			return error_safe_not_scalar(psp, tr->kv, kv);

		tr->safe = kv->scalar;

		rc = regcomp( &(tr->saferegex), tr->safe, REG_EXTENDED|REG_NOSUB);
		if (rc != 0)
			return error_safe_not_valid_regex(psp, tr->kv, kv, &(tr->saferegex), rc);
	}

    return 0;
}

int
validate_transformation(struct parsestate *psp, struct keyvalue *kv)
{
	int rc;
	struct mapping *trmapping = NULL;
	
	trmapping = kv->map;
	for (kv = trmapping->kvlist; kv; kv = kv->nxt)
	{
		struct transform *tr;
		if (kv->type != KV_MAPPING)
			return error_not_proper_yaml_map(psp, kv);
		
		tr = psp->trlist = new_transform(psp, psp->trlist);
		tr->kv = kv;
		rc = validate_transform(psp, tr, kv->map);
		if (rc)
			return 1;
	}
	return 0;
}

static int
validate_cos_conf(struct parsestate *psp, datasource *dp, struct mapping *map)
{
	struct keyvalue *kv = NULL;
	
	for (kv = map->kvlist; kv; kv = kv->nxt)
	{
		if (kv->type != KV_SCALAR)
			return error_conf_key_not_scalar(psp, dp->kv, kv, kv->key, DATA_SOURCE_CONF);
		
		if (strcmp(kv->key, "Type") == 0)
		{
			if (0 == strcasecmp(kv->scalar, TX_TYPE))
				dp->oss_conf->ossType = kv->scalar;
			else
				return error_invalid_key_for_conf(psp, dp->kv, kv, "Type", kv->scalar);
		}
		else if (strcmp(kv->key, "AccessKey") == 0)
		{
			dp->oss_conf->accessKeyId = kv->scalar;
		}
		else if (strcmp(kv->key, "SecretKey") == 0)
		{
			dp->oss_conf->secretAccessKey = kv->scalar;
		}
		else if (strcmp(kv->key, "Region") == 0)
		{
			dp->oss_conf->zone = kv->scalar;
		}
		else if (strcmp(kv->key, "Endpoint") == 0)
		{
			dp->oss_conf->endpoint = kv->scalar;
		}
		else if (strcmp(kv->key, "logLevel") == 0)
		{
			LogSeverity tmp;
			
			if (0 == strcmp(kv->scalar, "OSS_FATAL"))
			{
				tmp = OSS_FATAL;
			}
			else if (0 == strcmp(kv->scalar, "OSS_ERROR"))
			{
				tmp = OSS_ERROR;
			}
			else if (0 == strcmp(kv->scalar, "OSS_WARNING"))
			{
				tmp = OSS_WARNING;
			}
			else if (0 == strcmp(kv->scalar, "OSS_INFO"))
			{
				tmp = OSS_INFO;
			}
			else if (0 == strcmp(kv->scalar, "OSS_DEBUG1"))
			{
				tmp = OSS_DEBUG1;
			}
			else if (0 == strcmp(kv->scalar, "OSS_DEBUG2"))
			{
				tmp = OSS_DEBUG2;
			}
			else if (0 == strcmp(kv->scalar, "OSS_DEBUG3"))
			{
				tmp = OSS_DEBUG3;
			}
			else
			{
				tmp = OSS_WARNING;
			}
			
			dp->oss_conf->logLevel = tmp;
		}
		else if (strcmp(kv->key, "UseVirtualHost") == 0)
		{
			dp->oss_conf->useVirtualHost = atoi(kv->scalar);
		}
		else if (strcmp(kv->key, "UseHttps") == 0)
		{
			dp->oss_conf->useHttps = atoi(kv->scalar);
		}
		else if (strcmp(kv->key, "MaxReadConn") == 0)
		{
			dp->oss_conf->maxReadConn = atoi(kv->scalar);
		}
		else if (strcmp(kv->key, "MaxHttpRetry") == 0)
		{
			dp->oss_conf->maxHttpRetry = atoi(kv->scalar);
		}
		else if (strcmp(kv->key, "ReadTimeOut") == 0)
		{
			dp->oss_conf->readTimeOut = atoi(kv->scalar);
		}
		else if (strcmp(kv->key, "WriteTimeOut") == 0)
		{
			dp->oss_conf->writeTimeOut = atoi(kv->scalar);
		}
		else if (strcmp(kv->key, "FragmentSize") == 0)
		{
			dp->oss_conf->fragmentSize = atol(kv->scalar);
		}
		else if (strcmp(kv->key, "UseWriteCache") == 0)
		{
			dp->oss_conf->useWriteCache = atoi(kv->scalar);
		}
		else if (strcmp(kv->key, "AmazonSignatureVersion") == 0)
		{
			dp->oss_conf->AmzVersion = atoi(kv->scalar);
		}
	}
	dp->oss_conf->useWriteCache = true;
	
	if (!dp->oss_conf->ossType)
		return error_failed_to_find_key_for_conf(psp, dp->kv, "Type", DATA_SOURCE_CONF);
	else if (!dp->oss_conf->zone && !dp->oss_conf->endpoint)
		return error_failed_to_find_key_for_conf(psp, dp->kv, "Region|EndPoint", DATA_SOURCE_CONF);
	else if (!dp->oss_conf->accessKeyId)
		return error_failed_to_find_key_for_conf(psp, dp->kv, "AccessKey", DATA_SOURCE_CONF);
	else if (!dp->oss_conf->secretAccessKey)
		return error_failed_to_find_key_for_conf(psp, dp->kv, "SecretKey", DATA_SOURCE_CONF);
	
	return 0;
}

static int
validate_kafka_conf(struct parsestate *psp, datasource *dp, struct mapping *map)
{
	struct keyvalue *kv = NULL;
	rd_kafka_conf_t *kafka_conf = dp->kafka_conf;
	kafka_msg_info *msg_format = dp->kafka_msg_format;
	char errstr[512];
	
	for (kv = map->kvlist; kv; kv = kv->nxt)
	{
		/* KAFKA PARAM: k_brokers,k_consumer_group,k_timeout_ms are necessary */
		if (strcasecmp(kv->key, "datetime_convert") == 0)
		{
			if (kv->type != KV_SCALAR)
				return error_conf_key_not_scalar(psp, dp->kv, kv, "datetime_convert", PROTOCOL_KAFKA_STR);
			if (strcmp(kv->scalar, "true") == 0)
				msg_format->datetime_need_convert = true;
		}
		if (strcasecmp(kv->key, "k_brokers") == 0)
		{
			if (kv->type != KV_SCALAR)
				return error_conf_key_not_scalar(psp, dp->kv, kv, "k_brokers", PROTOCOL_KAFKA_STR);
			// kafka_conf->bootstrap_servers = kv->scalar;
			if (rd_kafka_conf_set(kafka_conf, "bootstrap.servers", kv->scalar,
			                      errstr, sizeof(errstr)))
				error_fail_to_set_kafka_conf(psp, kv, PROTOCOL_KAFKA_STR, "bootstrap.servers", errstr);
		}
		else if (strcasecmp(kv->key, "k_consumer_group") == 0)
		{
			if (kv->type != KV_SCALAR)
				return error_conf_key_not_scalar(psp, dp->kv, kv, "k_consumer_group", PROTOCOL_KAFKA_STR);
			// kafka_conf->group_id = kv->scalar;
			if (rd_kafka_conf_set(kafka_conf, "group.id", kv->scalar,
			                      errstr, sizeof(errstr)))
				error_fail_to_set_kafka_conf(psp, kv, PROTOCOL_KAFKA_STR, "group.id", errstr);
		}
		else if (strcasecmp(kv->key, "k_timeout_ms") == 0)
		{
			if (kv->type != KV_SCALAR)
				return error_conf_key_not_scalar(psp, dp->kv, kv, "k_timeout_ms", PROTOCOL_KAFKA_STR);
			msg_format->k_timeout_ms = atoi(kv->scalar);
		}
		else if (strcasecmp(kv->key, "k_msg_batch") == 0)
		{
			if (kv->type != KV_SCALAR)
				return error_conf_key_not_scalar(psp, dp->kv, kv, "k_msg_batch", PROTOCOL_KAFKA_STR);
			msg_format->k_msg_batch = atol(kv->scalar);
		}
		else if (strcasecmp(kv->key, "enable_auto_commit") == 0)
		{
			if (kv->type != KV_SCALAR)
				return error_conf_key_not_scalar(psp, dp->kv, kv, "enable_auto_commit", PROTOCOL_KAFKA_STR);
			msg_format->k_msg_batch = atol(kv->scalar);
			if (pg_strncasecmp(kv->scalar, "no", 2) == 0 ||
			    pg_strncasecmp(kv->scalar, "false", 5) == 0 ||
			    pg_strncasecmp(kv->scalar, "0", 1) == 0)
			{
				if (rd_kafka_conf_set(kafka_conf, "enable.auto.commit", "false", errstr, sizeof(errstr)))
					error_fail_to_set_kafka_conf(psp, kv, PROTOCOL_KAFKA_STR, "enable.auto.commit", errstr);
				if (rd_kafka_conf_set(kafka_conf, "enable.auto.offset.store", "false", errstr, sizeof(errstr)))
					error_fail_to_set_kafka_conf(psp, kv, PROTOCOL_KAFKA_STR, "enable.auto.offset.store", errstr);
				if (rd_kafka_conf_set(kafka_conf, "auto.offset.reset", "earliest", errstr, sizeof(errstr)))
					error_fail_to_set_kafka_conf(psp, kv, PROTOCOL_KAFKA_STR, "auto.offset.reset", errstr);
			}
		}
		/* DATA FORMAT: */
		else if (strcasecmp(kv->key, "MSGFORMAT") == 0)
		{
			struct keyvalue *format_kv = NULL;
			struct mapping *format_map = kv->map;

			if (kv->type != KV_MAPPING)
				return error_conf_key_not_mapping(psp, dp->kv, kv, "MSGFORMAT", PROTOCOL_KAFKA_STR);
			
			for (format_kv = format_map->kvlist; format_kv; format_kv = format_kv->nxt)
			{
				if (strcasecmp(format_kv->key, "FORMAT") == 0)
				{
					if (format_kv->type != KV_SCALAR)
						return error_conf_key_not_scalar(psp, kv, format_kv, "FORMAT", PROTOCOL_KAFKA_STR);
					if (strcasecmp(format_kv->scalar, "json") == 0)
						msg_format->format = JSON_FORMAT;
					else
						return error_not_supported_x(psp, format_kv);
				}
				else if (strcasecmp(format_kv->key, "OP") == 0)
				{
					if (format_kv->type != KV_SCALAR)
						return error_conf_key_not_scalar(psp, kv, format_kv, "OP", PROTOCOL_KAFKA_STR);
					msg_format->op_pos = format_kv->scalar;
				}
				else if (strcasecmp(format_kv->key, "BEFORE") == 0)
				{
					if (format_kv->type != KV_SCALAR)
						return error_conf_key_not_scalar(psp, kv, format_kv, "BEFORE", PROTOCOL_KAFKA_STR);
					msg_format->before_pos = format_kv->scalar;
				}
				else if (strcasecmp(format_kv->key, "AFTER") == 0)
				{
					if (format_kv->type != KV_SCALAR)
						return error_conf_key_not_scalar(psp, kv, format_kv, "AFTER", PROTOCOL_KAFKA_STR);
					msg_format->after_pos = format_kv->scalar;
				}
			}
			
		}
		/* KAFKA PARAM: othres are optional */
		else if (strcmp(kv->key, "k_security_protocol") == 0)
		{
			if (kv->type != KV_SCALAR)
				return error_conf_key_not_scalar(psp, dp->kv, kv, "k_security_protocol", PROTOCOL_KAFKA_STR);
			if (rd_kafka_conf_set(kafka_conf, "security.protocol", kv->scalar, errstr, sizeof(errstr)))
				error_fail_to_set_kafka_conf(psp, kv, PROTOCOL_KAFKA_STR, "security.protocol", errstr);
		}
		else if (strcmp(kv->key, "kerberos_keytab") == 0)
		{
			if (kv->type != KV_SCALAR)
				return error_conf_key_not_scalar(psp, dp->kv, kv, "kerberos_keytab", PROTOCOL_KAFKA_STR);
			if (rd_kafka_conf_set(kafka_conf, "sasl.kerberos.keytab", kv->scalar, errstr, sizeof(errstr)))
				error_fail_to_set_kafka_conf(psp, kv, PROTOCOL_KAFKA_STR, "sasl.kerberos.keytab", errstr);
		}
		else if (strcmp(kv->key, "kerberos_principal") == 0)
		{
			if (kv->type != KV_SCALAR)
				return error_conf_key_not_scalar(psp, dp->kv, kv, "kerberos_principal", PROTOCOL_KAFKA_STR);
			if (rd_kafka_conf_set(kafka_conf, "sasl.kerberos.principal", kv->scalar, errstr, sizeof(errstr)))
				error_fail_to_set_kafka_conf(psp, kv, PROTOCOL_KAFKA_STR, "sasl.kerberos.keytab", errstr);
		}
		else if (strcmp(kv->key, "kerberos_service_name") == 0)
		{
			if (kv->type != KV_SCALAR)
				return error_conf_key_not_scalar(psp, dp->kv, kv, "kerberos_service_name", PROTOCOL_KAFKA_STR);
			if (rd_kafka_conf_set(kafka_conf, "sasl.kerberos.service.name", kv->scalar, errstr, sizeof(errstr)))
				error_fail_to_set_kafka_conf(psp, kv, PROTOCOL_KAFKA_STR, "sasl.kerberos.service.name", errstr);
		}
		else if (strcmp(kv->key, "kerberos_min_time_before_relogin") == 0)
		{
			if (kv->type != KV_SCALAR)
				return error_conf_key_not_scalar(psp,
				                                 dp->kv,
				                                 kv,
				                                 "kerberos_min_time_before_relogin",
				                                 PROTOCOL_KAFKA_STR);
			if (rd_kafka_conf_set(kafka_conf,
			                      "sasl.kerberos.min.time.before.relogin",
			                      kv->scalar,
			                      errstr,
			                      sizeof(errstr)))
				error_fail_to_set_kafka_conf(psp,
				                             kv,
				                             PROTOCOL_KAFKA_STR,
				                             "sasl.kerberos.min.time.before.relogin",
				                             errstr);
		}
	}
	
	if (rd_kafka_conf_set(kafka_conf, "enable.partition.eof", "true", errstr, sizeof(errstr)))
		error_fail_to_set_kafka_conf1(psp, PROTOCOL_KAFKA_STR, "enable.partition.eof", errstr);
	if (rd_kafka_conf_set(kafka_conf, "allow.auto.create.topics", "false", errstr, sizeof(errstr)))
		error_fail_to_set_kafka_conf1(psp, PROTOCOL_KAFKA_STR, "allow.auto.create.topics", errstr);
	
	return 0;
}

int
validate_datasource(struct parsestate *psp, struct keyvalue *kv)
{
	struct mapping *trmapping = NULL;
	
	trmapping = kv->map;
	for (kv = trmapping->kvlist; kv; kv = kv->nxt)
	{
		if (kv->type != KV_MAPPING)
			return error_not_proper_yaml_map(psp, kv);
		
		if (strncasecmp(kv->key, PROTOCOL_COS_STR, strlen(PROTOCOL_COS_STR)) == 0)
		{
			datasource *ds_conf = psp->dslist = new_cosconf(psp, psp->dslist);
			ds_conf->kv = kv;
			if (validate_cos_conf(psp, ds_conf, kv->map))
				return 1;
		}
		if (strncasecmp(kv->key, PROTOCOL_KAFKA_STR, strlen(PROTOCOL_KAFKA_STR)) == 0)
		{
			datasource *ds_conf = psp->dslist = new_kafkaconf(psp, psp->dslist);
			ds_conf->kv = kv;
			if (validate_kafka_conf(psp, ds_conf, kv->map))
				return 1;
		}
		else
		{
			error_not_supported_x(psp, kv);
		}
	}
	return 0;
}

static int
validate_conf(struct parsestate *psp, struct streamstate *stp)
{
	struct mapping *document = stp->document;
	struct keyvalue *kv;
	
	for (kv = document->kvlist; kv; kv = kv->nxt)
	{
		
		if (strcmp(kv->key, TRANSFORM_CONF) == 0)
		{
			if (kv->type != KV_MAPPING)
				return error_not_proper_yaml_map(psp, kv);
			return validate_transformation(psp, kv);
		}
		else if (strcmp(kv->key, DATA_SOURCE_CONF) == 0)
		{
			if (kv->type != KV_MAPPING)
				return error_not_proper_yaml_map(psp, kv);
			return validate_datasource(psp, kv);
		}
		else if (strcmp(kv->key, VERSION_CONF) == 0)
		{
			psp->version_str = kv->scalar;
			printf("TDX YAML VERSION: %s\n", psp->version_str);
		}
		else
		{
			return error_not_handled_conf(psp, kv->key);
		}
	}
	return 0;
}

/*
 * configure transforms
 */
int
transform_config(const char *filename, struct transform **trlistp, datasource **dolistp, int verbose)
{
	struct parsestate  ps;
	FILE              *file;
	struct streamstate st;
	int                rc;

	memset(&ps, 0, sizeof(ps));
	ps.filename = filename;
	
	file = fopen(ps.filename, "rb");
	if (!file)
	{
		fprintf(stderr, "unable to open file %s: %s\n", ps.filename, strerror(errno));
		return 1;
	}

	memset(&st, 0, sizeof(st));
	st.filename = ps.filename;
	
	rc = stage1_parse(&st, file, verbose);
	fclose(file);
	
	if (rc)
		fprintf(stderr, "failed to parse file: %s\n", ps.filename);
	
	if (rc || verbose)
	{
		if (st.document)
			debug_mapping(st.document, 0);
	}
	
	if (rc)
		return 1;
	
	if (validate_conf(&ps, &st))
	{
		fprintf(stderr, "failed to validate file: %s\n", ps.filename);
		fprintf(stderr, "%s", ps.errbuf);
		return 1;
	}
	
	(*trlistp) = ps.trlist;
	(*dolistp) = ps.dslist;
	return 0;
}


/*
 * lookup transformation
 */

struct transform*
transform_lookup(struct transform* trlist, const char* name, int for_write, int verbose)
{
	struct transform* tr;

	for (tr = trlist; tr; tr = tr->nxt)
    {

        /* ignore transforms not of proper type */
        if (for_write) 
        {
            if (tr->type != TR_OUTPUT)
                continue;
        } 
        else 
        {
            if (tr->type != TR_INPUT)
                continue;
        }

        /* when we've found a match, return the corresponding command */
        if (0 == strcmp(tr->kv->key, name)) 
		{
			if (verbose)
				dump_transform(tr);

            return tr;
		}
    }
    return NULL;
}

/* lookup datasource conf */
datasource *
datasource_lookup(datasource *dslist, const char *proto_name, int verbose)
{
	datasource *ds;
	
	for (ds = dslist; ds; ds = ds->nxt)
	{
		/* when we've found a match, return the corresponding command */
		if (0 == strncasecmp(ds->kv->key, proto_name, strlen(proto_name)))
		{
			if (verbose)
				dump_datasource(ds);
			
			return ds;
		}
	}
	return NULL;
}

void
dump_datasource(datasource *ds_conf)
{
	size_t cntp = 0;
	int i;
	const char **kafak_conf_ptr;
	switch (ds_conf->proto_type)
	{
		case REMOTE_COS_PROTOCOL:
			printf("  PROTO_TYPE: %s\n", ds_conf->kv->key);
			printf("  COS_OSSTYPE: %s\n", ds_conf->oss_conf->ossType);
			printf("  COS_ZONE: %s\n", ds_conf->oss_conf->zone);
			break;
		case REMOTE_KAFKA_PROTOCOL:
			/* rd_kafka_conf_get */
			printf("  PROTO_TYPE: %s\n", ds_conf->kv->key);
			kafak_conf_ptr = rd_kafka_conf_dump(ds_conf->kafka_conf, &cntp);
			for (i = 0; i < cntp; ++i)
			{
				printf("  %s", kafak_conf_ptr[i]);
			}
			rd_kafka_conf_dump_free(kafak_conf_ptr, cntp);
			break;
		default:
			printf("  TYPE: %s\n", "LOCAL");
	}
}

/*
 * transformation accessors
 */

char*
transform_command(struct transform* tr)
{
	return tr->command;
}

int
transform_stderr_server(struct transform* tr)
{
	return (tr->errs == TR_ER_SERVER);
}

int
transform_content_paths(struct transform* tr)
{
	return (tr->content == TR_FN_PATHS);
}

char*
transform_safe(struct transform* tr)
{
	return tr->safe;
}

regex_t*
transform_saferegex(struct transform* tr)
{
	return &(tr->saferegex);
}

#endif
