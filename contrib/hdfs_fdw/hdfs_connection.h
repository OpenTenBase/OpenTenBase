#ifndef HDFS_CONNECTION_H
#define HDFS_CONNECTION_H

#include "hdfs_fdw.h"

/* libhive API declarations */
extern int DBOpenConnection(char *host, int port, char *dbname, char *username, 
                           char *password, int connect_timeout, int receive_timeout,
                           char *auth_type, int client_type, char **err_buf);
extern int DBCloseConnection(int con_index);

#endif /* HDFS_CONNECTION_H */
