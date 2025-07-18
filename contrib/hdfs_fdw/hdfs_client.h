#ifndef HDFS_CLIENT_H
#define HDFS_CLIENT_H

#include "hdfs_fdw.h"

/* libhive client API declarations */
extern int DBOpenConnection(char *host, int port, char *dbname, 
                           char *username, char *password,
                           int connect_timeout, int receive_timeout,
                           char *auth_type, int client_type, char **err_buf);
extern int DBCloseConnection(int con_index);
extern int DBExecute(int con_index, char *query, int fetch_size, char **err_buf);
extern int DBFetch(int con_index, char **err_buf);
extern int DBGetFieldAsCString(int con_index, int field_idx, char **value, char **err_buf);
extern int DBGetColumnCount(int con_index, char **err_buf);

#endif /* HDFS_CLIENT_H */
