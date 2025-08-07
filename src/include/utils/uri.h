/*-------------------------------------------------------------------------
 *
 * uri.h
 *	  Definitions for URI strings
 *
 * src/include/utils/uri.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef URI_H
#define URI_H

typedef enum UriProtocol
{
	URI_FILE,
	URI_FTP,
	URI_HTTP,
	URI_TDX,
	URI_CUSTOM,
	URI_TDXS
}	UriProtocol;

#define PROTOCOL_FILE		"file://"
#define PROTOCOL_FTP		"ftp://"
#define PROTOCOL_HTTP		"http://"
/*	according to "gpfdist://" */
#define PROTOCOL_TDX	"tdx://"
/*	according to "gpfdists://" */
#define PROTOCOL_TDXS	"tdxs://"
#define PROTOCOL_COS    "cos://"
#define PROTOCOL_KAFKA  "kafka://"

/* COS */
#define COS_BUCKET_CONF "bucket"

/* KAFKA */
#define KAFKA_BROKERS_CONF "k_brokers"
#define KAFKA_TOPIC_CONF "k_topic"
#define KAFKA_CONSUMER_GROUP_CONF "k_consumer_group"
#define KAFKA_BATCH_SIZE_CONF "session_prt_batch"   /* max number of msgs would be processed in a request */

#define TDXSERVERDELIM  "|"
#define TDXPATHDELIM    '|'
/* 
 *  * sometimes we don't want to parse the whole URI but just take a peek at
 *   * which protocol it is. We can use these macros to do just that.
 *    */
#define IS_FILE_URI(uri_str) (pg_strncasecmp(uri_str, PROTOCOL_FILE, strlen(PROTOCOL_FILE)) == 0)
#define IS_HTTP_URI(uri_str) (pg_strncasecmp(uri_str, PROTOCOL_HTTP, strlen(PROTOCOL_HTTP)) == 0)
#define IS_TDX_URI(uri_str) (pg_strncasecmp(uri_str, PROTOCOL_TDX, strlen(PROTOCOL_TDX)) == 0)
#define IS_TDXS_URI(uri_str) (pg_strncasecmp(uri_str, PROTOCOL_TDXS, strlen(PROTOCOL_TDXS)) == 0) 
#define IS_FTP_URI(uri_str) (pg_strncasecmp(uri_str, PROTOCOL_FTP, strlen(PROTOCOL_FTP)) == 0)
#define IS_COS_URI(uri_str) (pg_strncasecmp(uri_str, PROTOCOL_COS, strlen(PROTOCOL_COS)) == 0)
#define IS_KAFKA_URI(uri_str) (pg_strncasecmp(uri_str, PROTOCOL_KAFKA, strlen(PROTOCOL_KAFKA)) == 0)

typedef struct Uri
{
	UriProtocol protocol;
	char	   *hostname;
	int         port;
	char	   *path;
	char	   *customprotocol;
}	Uri;

extern Uri *ParseExternalTableUri(const char *uri);
extern void FreeExternalTableUri(Uri *uri);

#endif   /* URI_H */

