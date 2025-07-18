/*-------------------------------------------------------------------------
 *
 * hiveclient.h
 * 		Wrapper functions to call functions in the Java class HiveJdbcClient
 * 		from C/C++
 *
 * Copyright (c) 2019-2025, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		hiveclient.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __hive_client_h__
#define __hive_client_h__

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <inttypes.h>
#include <stdint.h>
#include <jni.h>

#include "data.h"

/******************************************************************************
 * Hive Client C++ Class Placeholders
 *****************************************************************************/


#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

typedef enum CLIENT_TYPE
{
	HIVESERVER2 = 0,
	SPARKSERVER
} CLIENT_TYPE;

typedef enum AUTH_TYPE
{
	AUTH_TYPE_UNSPECIFIED = 0,
	AUTH_TYPE_NOSASL,
	AUTH_TYPE_LDAP
} AUTH_TYPE;

/******************************************************************************
 * Global Hive Client Functions (usable as C callback functions)
 *****************************************************************************/

/**
 * @brief Initialize JNI, Create JVM & initialize all interface functions.
 *
 * @return Any negative value indicates an error, 0 or +ve means success.
 *         The return value is the version of the JNI.
 */
int Initialize(void);

/**
 * @brief Destroy JVM.
 *
 * @return Any negative value indicates an error, 0 or +ve means success.
 */
int Destroy(void);

/**
 * @brief Connect to a Hive database.
 *
 * Connects to a Hive database on the specified Hive server.
 * The underlying connection object will be saved in the underlying Java object
 * To free the memory and connection call DBCloseConnection.
 *
 * @see DBCloseConnection()
 *
 * @param host           IP address/Host Name of computer running the Hive server.
 * @param port           Port on which the Hive server is listening.
 * @param databaseName   Name of the database on the Hive server to which to connect.
 * @param username       The name of the user for authentication on the Hive server.
 * @param password       The password of the user for authentication on the Hive server.
 * @param connectTimeout Time in seconds to wait for the connection to get established
                         and authenticated.
 * @param receiveTimeout Time in seconds to wait for queries to get executed on this connection.
                         This time will be used in calls to JDBC setQueryTimeout.
 * @param errBuf         Buffer to receive an error message if any.
 *                       It receives a copy of the pointer to the already allocated
 *                       memory that the caller does not need to worry about.
 * @return Any negative value indicates an error, 0 or +ve means success.
 *         The return value is the index of the connection object.
 *         Error messages will be stored in errBuf.
 */
int DBOpenConnection(char *host, int port, char *databaseName,
					char *username, char *password,
					int connectTimeout, int receiveTimeout,
					AUTH_TYPE auth_type, CLIENT_TYPE client_type, char **errBuf);

/**
 * @brief Disconnect from a Hive database.
 *
 * Disconnects from a Hive database and destroys the saved underlying connection object.
 * This function should eventually be called for every connection created by
 * DBOpenConnection.
 *
 * @see DBOpenConnection()
 *
 * @param index          Index of the connection object to close.
 * @return Any negative value indicates an error, 0 means success.
 */
int DBCloseConnection(int con_index);

/**
 * @brief Close all connections required by nested queries to Hive.
 *
 * Closes all connections that may have been initiated by any level
 * of nested queries from the Hive database
 *
 * @see DBCloseConnection()
 *
 * @return Any negative value indicates an error, 0 or +ve means success.
 *         The value returned is the number of connections closed
 */
int DBCloseAllConnections();

/**
 * @brief Execute a query.
 *
 * Executes a query on a Hive connection and associates a result set with it.
 * There is no need for this function to return result set to the caller.
 * The result set can be held by the underlying java object
 * and there is no need to return it to the caller.
 * This design change simplifies and reduces the amount of coding we have to do
 * while trying to write the JNI code to access Java class functions from C++.
 * Caller is responsible for deallocating the result set object by
 * calling DBCloseResultSet.
 *
 * @see DBCloseResultSet()
 *
 * @param index          Index of the result set object to use.
 * @param query          The HQL query string to be executed.
 * @param maxRows        Max number of rows to buffer in the result set for the query results
 * @param errBuf         Buffer to receive an error message if any.
 *                       It receives a copy of the pointer to the already allocated
 *                       memory that the caller does not need to worry about.
 *
 * @return Any negative value indicates an error, 0 means success.
 *         Error messages will be stored in errBuf.
 */
int DBExecute(int con_index, const char* query, int maxRows, char **errBuf);

/**
 * @brief Prepare a query for execution later.
 *
 * Prepares a query on a Hive connection.
 *
 * @see DBExecutePrepared()
 *
 * @param index          Index of the result set object to use.
 * @param query          The HQL query string to be prepared.
 * @param maxRows        Max number of rows to buffer in the result set for the query results
 * @param errBuf         Buffer to receive an error message if any.
 *                       It receives a copy of the pointer to the already allocated
 *                       memory that the caller does not need to worry about.
 *
 * @return Any negative value indicates an error, 0 means success.
 *         Error messages will be stored in errBuf.
 */
int DBPrepare(int con_index, const char* query, int maxRows, char **errBuf);

/**
 * @brief Execute a prepared query.
 *
 * Executes a query on a Hive connection and associates a result set with it.
 * There is no need for this function to return result set to the caller.
 * The result set can be held by the underlying java object
 * and there is no need to return it to the caller.
 * This design change simplifies and reduces the amount of coding we have to do
 * while trying to write the JNI code to access Java class functions from C++.
 * Caller is responsible for deallocating the result set object by
 * calling DBCloseResultSet.
 *
 * @see DBCloseResultSet()
 *
 * @param index          Index of the result set object to use.
 * @param errBuf         Buffer to receive an error message if any.
 *                       It receives a copy of the pointer to the already allocated
 *                       memory that the caller does not need to worry about.
 *
 * @return Any negative value indicates an error, 0 means success.
 *         Error messages will be stored in errBuf.
 */
int DBExecutePrepared(int con_index, char **errBuf);

/**
 * @brief Execute a utility query.
 *
 * Utility queries are not supposed to return any result set
 *
 * @param index          Index of the result set object to use.
 * @param query          The HQL query string to be executed.
 * @param errBuf         Buffer to receive an error message if any.
 *                       It receives a copy of the pointer to the already allocated
 *                       memory that the caller does not need to worry about.
 *
 * @return Any negative value indicates an error, 0 means success.
 *         Error messages will be stored in errBuf.
 */
int DBExecuteUtility(int con_index, const char* query, char **errBuf);

/**
 * @brief Destroys the underlying result set object.
 *
 * Destroys the underlying result set object. The result set is stored in the
 * underlying JDBC based java object.
 *
 * @param index          Index of the result set object to close.
 * @param errBuf         Buffer to receive an error message if any.
 *                       It receives a copy of the pointer to the already allocated
 *                       memory that the caller does not need to worry about.
 *
 * @return Any negative value indicates an error, 0 means success.
 *         Error messages will be stored in errBuf.
 */
int DBCloseResultSet(int con_index, char **errBuf);

/**
 * @brief Fetches the next unfetched row in a the underlying result set.
 *
 * Fetches the next unfetched row in a the underlying result set.
 * The fetched row will be stored internally within the result set
 * and may be accessed through other DB functions.
 *
 * @param index          Index of the result set object to use.
 * @param errBuf         Buffer to receive an error message if any.
 *                       It receives a copy of the pointer to the already allocated
 *                       memory that the caller does not need to worry about.
 *
 * @return  0 means row was fetched succesfully.
 *         -1 means there are no more rows to fetch.
 *         any other negative value means an error.
 *         Error messages will be stored in errBuf.
 */
int DBFetch(int con_index, char **errBuf);

/**
 * @brief Determines the number of columns in the underlying result set.
 *
 * @param index          Index of the result set object to use.
 * @param errBuf         Buffer to receive an error message if any.
 *                       It receives a copy of the pointer to the already allocated
 *                       memory that the caller does not need to worry about.
 *
 * @return  0 or any positive value is the number of columns.
 *         any other negative value means an error.
 *         Error messages will be stored in errBuf.
 */
int DBGetColumnCount(int con_index, char **errBuf);

/**
 * @brief Get a field as a C string.
 *
 * Reads out a field from the currently fetched rowset in a resultset as a C String.
 *
 * @param index          Index of the result set object to use.
 * @param columnIdx      Zero based offset index of the column.
 * @param buffer         Pointer to a buffer that will receive the data.
 *                       It receives a copy of the pointer to the already allocated
 *                       memory that the caller does not need to worry about.
 * @param errBuf         Buffer to receive an error message if any.
 *                       It receives a copy of the pointer to the already allocated
 *                       memory that the caller does not need to worry about.
 *
 * @return  0 or any positive value would mean the length of data copied in the buffer
 *            which would always be 1 less than bufferSize
 *         -1 would indicate a null value
 *         -2 would mean that bufferSize is too small to receive the data available in the column
 *         any other negative value means an error.
 *         Error messages will be stored in errBuf.
 */
int DBGetFieldAsCString(int con_index, int columnIdx, char **buffer, char **errBuf);


/**
 * @brief Bind a value to a parameter in a parameterized query
 *
 * The function keeps track of the parameter number itself.
 *
 * @param index          Index of the result set object to use.
 * @param param_index    One based Index of the param to bind.
 * @param type           PG Type of the parameter.
 * @param value          PG value of the parameter.
 * @param isnull         Is value to be bound a null.
 * @param errBuf         Buffer to receive an error message if any.
 *                       It receives a copy of the pointer to the already allocated
 *                       memory that the caller does not need to worry about.
 *
 * @return Any negative value indicates an error, 0 means success.
 *         Error messages will be stored in errBuf.
 */
int DBBindVar(int con_index, int param_index, Oid type, void *value, bool *isnull, char **errBuf);

#ifdef __cplusplus
} // extern "C"
#endif // __cpluscplus


#endif // __hive_client_h__
