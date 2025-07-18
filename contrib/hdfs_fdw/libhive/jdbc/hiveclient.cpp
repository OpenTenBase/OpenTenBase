/*-------------------------------------------------------------------------
 *
 * hiveclient.cpp
 * 		Wrapper functions to call functions in the Java class HiveJdbcClient
 * 		from C/C++
 * 
 * Copyright (c) 2019-2025, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		hiveclient.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <assert.h>
#include <iostream>
#include <string.h>


#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dlfcn.h>


#include "postgres.h"
#include "catalog/pg_type.h"
#include "utils/timestamp.h"

#include "hiveclient.h"

/*
 * Since PG12, sprintf is redefined as pg_sprintf.  If we want to use that,
 * we have to include src/port/snpritf.c in our build.  For now, just use
 * the default sprintf function.
 */
#ifdef sprintf
#undef sprintf
#endif

using namespace std;

static JavaVM *g_jvm = NULL;
static JNIEnv *g_jni = NULL;
static jclass g_clsMsgBuf = NULL;
static jclass g_clsJDBCType = NULL;
static jobject g_objMsgBuf = NULL;
static jobject g_objValBuf = NULL;
static jclass g_clsJdbcClient = NULL;
static jobject g_objJdbcClient = NULL;
static jmethodID g_getVal = NULL;
static jmethodID g_resetVal = NULL;
static jmethodID g_DBOpenConnection = NULL;
static jmethodID g_DBCloseConnection = NULL;
static jmethodID g_DBCloseAllConnections = NULL;
static jmethodID g_DBExecutePrepared = NULL;
static jmethodID g_DBPrepare = NULL;
static jmethodID g_DBBindVar = NULL;
static jmethodID g_DBExecute = NULL;
static jmethodID g_DBExecuteUtility = NULL;
static jmethodID g_DBCloseResultSet = NULL;
static jmethodID g_DBFetch = NULL;
static jmethodID g_DBGetColumnCount = NULL;
static jmethodID g_DBGetFieldAsCString = NULL;
static jmethodID g_consJDBCType = NULL;
static jmethodID g_setBool = NULL;
static jmethodID g_setShort = NULL;
static jmethodID g_setInt = NULL;
static jmethodID g_setLong = NULL;
static jmethodID g_setDoub = NULL;
static jmethodID g_setFloat = NULL;
static jmethodID g_setString = NULL;
static jmethodID g_setDate = NULL;
static jmethodID g_setTime = NULL;
static jmethodID g_setStamp = NULL;

typedef jint ((*_JNI_CreateJavaVM_PTR)(JavaVM **p_vm, JNIEnv **p_env, void *vm_args));
_JNI_CreateJavaVM_PTR _JNI_CreateJavaVM;
void* hdfs_dll_handle = NULL;

int Initialize()
{
	jint            ver;
	jint            rc;
	JavaVMInitArgs  vm_args;
	JavaVMOption*   options;
	jmethodID       consMsgBuf;
	jmethodID       consJdbcClient;
	int             len;
    char            *libjvm;

    len = strlen(g_jvmpath);
    libjvm = new char[len + 15];
    sprintf(libjvm, "%s/%s", g_jvmpath, "libjvm.so");

    hdfs_dll_handle = dlopen(libjvm, RTLD_LAZY);
    if(hdfs_dll_handle == NULL)
    {
        return -1;
    }
    delete[] libjvm;

    _JNI_CreateJavaVM = (_JNI_CreateJavaVM_PTR)dlsym(hdfs_dll_handle, "JNI_CreateJavaVM");

    options = new JavaVMOption[1];

	len = strlen(g_classpath) + 100;

	options[0].optionString = new char[len];
	strcpy(options[0].optionString, "-Djava.class.path=");
	strcat(options[0].optionString, g_classpath);

	vm_args.version = JNI_VERSION_1_6;
	vm_args.nOptions = 1;
	vm_args.options = options;
	vm_args.ignoreUnrecognized = false;

    rc = _JNI_CreateJavaVM(&g_jvm, &g_jni, &vm_args);

    delete[] options[0].optionString;
	delete[] options;
	if (rc != JNI_OK)
	{
		return(-1);
	}
	ver = g_jni->GetVersion();

	g_clsMsgBuf = g_jni->FindClass("MsgBuf");
	if (g_clsMsgBuf == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-2);
	}

	g_clsJDBCType = g_jni->FindClass("JDBCType");
	if (g_clsJDBCType == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-4);
	}

	g_clsJdbcClient = g_jni->FindClass("HiveJdbcClient");
	if (g_clsJdbcClient == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-6);
	}

	consMsgBuf = g_jni->GetMethodID(g_clsMsgBuf, "<init>", "(Ljava/lang/String;)V");
	if (consMsgBuf == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-7);
	}

	g_consJDBCType = g_jni->GetMethodID(g_clsJDBCType, "<init>", "(I)V");
	if (g_consJDBCType == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-8);
	}

	consJdbcClient = g_jni->GetMethodID(g_clsJdbcClient, "<init>", "()V");
	if (consJdbcClient == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-10);
	}

	g_objMsgBuf = g_jni->NewObject(g_clsMsgBuf, consMsgBuf, g_jni->NewStringUTF(""));
	if (g_objMsgBuf == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-12);
	}

	g_objValBuf = g_jni->NewObject(g_clsMsgBuf, consMsgBuf, g_jni->NewStringUTF(""));
	if (g_objValBuf == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-14);
	}

	g_objJdbcClient = g_jni->NewObject(g_clsJdbcClient, consJdbcClient);
	if (g_objJdbcClient == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-16);
	}

	g_getVal = g_jni->GetMethodID(g_clsMsgBuf, "getVal", "()Ljava/lang/String;");
	if (g_getVal == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-18);
	}

	g_resetVal = g_jni->GetMethodID(g_clsMsgBuf, "resetVal", "()V");
	if (g_resetVal == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-20);
	}

	g_DBOpenConnection = g_jni->GetMethodID(g_clsJdbcClient, "DBOpenConnection",
				"(Ljava/lang/String;ILjava/lang/String;Ljava/lang/String;Ljava/lang/String;IIIILMsgBuf;)I");
	if (g_DBOpenConnection == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-22);
	}

	g_DBCloseConnection = g_jni->GetMethodID(g_clsJdbcClient, "DBCloseConnection","(I)I");
	if (g_DBCloseConnection == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-24);
	}

	g_DBCloseAllConnections = g_jni->GetMethodID(g_clsJdbcClient, "DBCloseAllConnections","()I");
	if (g_DBCloseAllConnections == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-26);
	}

	g_DBExecutePrepared = g_jni->GetMethodID(g_clsJdbcClient, "DBExecutePrepared", "(ILMsgBuf;)I");
	if (g_DBExecutePrepared == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-28);
	}

	g_DBPrepare = g_jni->GetMethodID(g_clsJdbcClient, "DBPrepare", "(ILjava/lang/String;ILMsgBuf;)I");
	if (g_DBPrepare == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-30);
	}

	g_DBBindVar = g_jni->GetMethodID(g_clsJdbcClient, "DBBindVar", "(IILJDBCType;LMsgBuf;)I");
	if (g_DBBindVar == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-32);
	}

	g_DBExecute = g_jni->GetMethodID(g_clsJdbcClient, "DBExecute", "(ILjava/lang/String;ILMsgBuf;)I");
	if (g_DBExecute == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-33);
	}

	g_DBExecuteUtility = g_jni->GetMethodID(g_clsJdbcClient, "DBExecuteUtility", "(ILjava/lang/String;LMsgBuf;)I");
	if (g_DBExecuteUtility == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-34);
	}

	g_DBCloseResultSet = g_jni->GetMethodID(g_clsJdbcClient, "DBCloseResultSet", "(ILMsgBuf;)I");
	if (g_DBCloseResultSet == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-36);
	}

	g_DBFetch = g_jni->GetMethodID(g_clsJdbcClient, "DBFetch", "(ILMsgBuf;)I");
	if (g_DBFetch == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-40);
	}

	g_DBGetColumnCount = g_jni->GetMethodID(g_clsJdbcClient, "DBGetColumnCount", "(ILMsgBuf;)I");
	if (g_DBGetColumnCount == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-42);
	}

	g_DBGetFieldAsCString = g_jni->GetMethodID(g_clsJdbcClient, "DBGetFieldAsCString", "(IILMsgBuf;LMsgBuf;)I");
	if (g_DBGetFieldAsCString == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-46);
	}

	g_setBool = g_jni->GetMethodID(g_clsJDBCType, "setBool", "(Z)V");
	if (g_setBool == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-48);
	}

	g_setShort = g_jni->GetMethodID(g_clsJDBCType, "setShort", "(S)V");
	if (g_setShort == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-50);
	}

	g_setInt = g_jni->GetMethodID(g_clsJDBCType, "setInt", "(I)V");
	if (g_setInt == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-52);
	}

	g_setLong = g_jni->GetMethodID(g_clsJDBCType, "setLong", "(J)V");
	if (g_setLong == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-54);
	}

	g_setDoub = g_jni->GetMethodID(g_clsJDBCType, "setDoub", "(D)V");
	if (g_setDoub == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-56);
	}

	g_setFloat = g_jni->GetMethodID(g_clsJDBCType, "setFloat", "(F)V");
	if (g_setFloat == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-58);
	}

	g_setString = g_jni->GetMethodID(g_clsJDBCType, "setString", "(Ljava/lang/String;)V");
	if (g_setString == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-60);
	}

	g_setDate = g_jni->GetMethodID(g_clsJDBCType, "setDate", "(Ljava/lang/String;)V");
	if (g_setDate == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-62);
	}

	g_setTime = g_jni->GetMethodID(g_clsJDBCType, "setTime", "(Ljava/lang/String;)V");
	if (g_setTime == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-64);
	}

	g_setStamp = g_jni->GetMethodID(g_clsJDBCType, "setStamp", "(Ljava/lang/String;)V");
	if (g_setStamp == NULL)
	{
		g_jvm->DestroyJavaVM();
		g_jvm = NULL;
		return(-66);
	}

	return(ver);
}

int Destroy()
{
    dlclose(hdfs_dll_handle);
	if (g_jvm != NULL)
		g_jvm->DestroyJavaVM();
	return(0);
}

int DBOpenConnection(char *host, int port, char *databaseName,
					char *username, char *password,
					int connectTimeout, int receiveTimeout,
					AUTH_TYPE auth_type, CLIENT_TYPE client_type, char **errBuf)
{
	int rc;
	jstring rv;
	jboolean isCopy = JNI_FALSE;

	if (g_jni == NULL || g_objJdbcClient == NULL || g_resetVal == NULL ||
		g_DBOpenConnection == NULL || g_objMsgBuf == NULL ||
		g_getVal == NULL)
		return(-10);

	g_jni->CallVoidMethod(g_objMsgBuf, g_resetVal);

	rc = g_jni->CallIntMethod(g_objJdbcClient, g_DBOpenConnection,
							g_jni->NewStringUTF(host),
							port,
							g_jni->NewStringUTF(databaseName),
							g_jni->NewStringUTF(username),
							g_jni->NewStringUTF(password),
							connectTimeout,
							receiveTimeout,
							auth_type,
							client_type,
							g_objMsgBuf);

	rv = (jstring)g_jni->CallObjectMethod(g_objMsgBuf, g_getVal);
	*errBuf = (char *)g_jni->GetStringUTFChars(rv, &isCopy);

	return rc;
}

int DBCloseConnection(int con_index)
{
	if (g_jni == NULL || g_objJdbcClient == NULL ||
		g_DBCloseConnection == NULL || con_index < 0)
		return(-10);

	return g_jni->CallIntMethod(g_objJdbcClient, g_DBCloseConnection, con_index);
}

int DBCloseAllConnections()
{
	if (g_jni == NULL || g_objJdbcClient == NULL ||
		g_DBCloseAllConnections == NULL)
		return(-10);

	return g_jni->CallIntMethod(g_objJdbcClient, g_DBCloseAllConnections);
}

int DBBindVar(int con_index, int param_index, Oid type,
				void *value, bool *isnull, char **errBuf)
{
	int rc;
	jstring rv;
	jboolean isCopy = JNI_FALSE;
	jobject objJDBCType = NULL;

	if (g_jni == NULL || g_objJdbcClient == NULL || g_DBBindVar == NULL ||
		g_objMsgBuf == NULL || g_resetVal == NULL || g_getVal == NULL ||
		con_index < 0)
		return(-10);

	objJDBCType = g_jni->NewObject(g_clsJDBCType, g_consJDBCType, 0);
	if (objJDBCType == NULL)
	{
		return(-20);
	}

	if (*isnull)
	{

	}

	switch(type)
	{
		case INT2OID:
		{
			int16 dat = *((int16 *)value);
			g_jni->CallVoidMethod(objJDBCType, g_setShort, dat);
			break;
		}
		case INT4OID:
		{
			int32 dat = *((int32 *)value);
			g_jni->CallVoidMethod(objJDBCType, g_setInt, dat);
			break;
		}
		case INT8OID:
		{
			int64 dat = *((int64 *)value);
			g_jni->CallVoidMethod(objJDBCType, g_setLong, dat);
			break;
		}
		case FLOAT4OID:
		{
			float4 dat = *((float4 *)value);
			g_jni->CallVoidMethod(objJDBCType, g_setFloat, dat);
			break;
		}
		case FLOAT8OID:
		{
			float8 dat = *((float8 *)value);
			g_jni->CallVoidMethod(objJDBCType, g_setDoub, dat);
			break;
		}
		case NUMERICOID:
		{
			float8 dat = *((float8 *)value);
			g_jni->CallVoidMethod(objJDBCType, g_setDoub, dat);
			break;
		}
		case BOOLOID:
		{
			bool v = *((bool *)value);
			g_jni->CallVoidMethod(objJDBCType, g_setBool, v);
			break;
		}

		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
		case JSONOID:
		{
			char *outputString =(char *)value;
			g_jni->CallVoidMethod(objJDBCType, g_setString, g_jni->NewStringUTF(outputString));
			break;
		}
		case NAMEOID:
		{
			char *outputString = (char *)value;
			g_jni->CallVoidMethod(objJDBCType, g_setString, g_jni->NewStringUTF(outputString));
			break;
		}
		case DATEOID:
		{
			char *valueDate =(char *)value;
			g_jni->CallVoidMethod(objJDBCType, g_setDate, g_jni->NewStringUTF(valueDate));
			break;
		}
		case TIMEOID:
		{
			char *valueTime =(char *)value;
			g_jni->CallVoidMethod(objJDBCType, g_setTime, g_jni->NewStringUTF(valueTime));
			break;
		}
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		{
			char *valueTimestamp =(char *)value;
			g_jni->CallVoidMethod(objJDBCType, g_setStamp, g_jni->NewStringUTF(valueTimestamp));
			break;
		}
		case BITOID:
		{
			bool v = *((bool *)value);
			g_jni->CallVoidMethod(objJDBCType, g_setBool, v);
			break;
		}

		default:
		{
			return (-30);
		}
	}

	g_jni->CallVoidMethod(g_objMsgBuf, g_resetVal);

	rc = g_jni->CallIntMethod(g_objJdbcClient, g_DBBindVar,
							con_index,
							param_index,
							objJDBCType,
							g_objMsgBuf);
	if (rc < 0)
	{
		rv = (jstring)g_jni->CallObjectMethod(g_objMsgBuf, g_getVal);
		*errBuf = (char *)g_jni->GetStringUTFChars(rv, &isCopy);
	}

	g_jni->DeleteLocalRef(objJDBCType);

	return(rc);
}

int DBPrepare(int con_index, const char* query, int maxRows, char **errBuf)
{
	int rc;
	jstring rv;
	jboolean isCopy = JNI_FALSE;

	if (g_jni == NULL || g_objJdbcClient == NULL || g_DBPrepare == NULL ||
		g_objMsgBuf == NULL || g_resetVal == NULL || g_getVal == NULL ||
		query == NULL || con_index < 0)
		return(-10);

	g_jni->CallVoidMethod(g_objMsgBuf, g_resetVal);

	rc = g_jni->CallIntMethod(g_objJdbcClient, g_DBPrepare,
							con_index,
							g_jni->NewStringUTF(query),
							maxRows,
							g_objMsgBuf);
	if (rc < 0)
	{
		rv = (jstring)g_jni->CallObjectMethod(g_objMsgBuf, g_getVal);
		*errBuf = (char *)g_jni->GetStringUTFChars(rv, &isCopy);
	}

	return(rc);
}

int DBExecutePrepared(int con_index, char **errBuf)
{
	int rc;
	jstring rv;
	jboolean isCopy = JNI_FALSE;

	if (g_jni == NULL || g_objJdbcClient == NULL || g_DBExecutePrepared == NULL ||
		g_objMsgBuf == NULL || g_resetVal == NULL || g_getVal == NULL ||
		con_index < 0)
		return(-10);

	g_jni->CallVoidMethod(g_objMsgBuf, g_resetVal);

	rc = g_jni->CallIntMethod(g_objJdbcClient, g_DBExecutePrepared,
							con_index,
							g_objMsgBuf);
	if (rc < 0)
	{
		rv = (jstring)g_jni->CallObjectMethod(g_objMsgBuf, g_getVal);
		*errBuf = (char *)g_jni->GetStringUTFChars(rv, &isCopy);
	}

	return(rc);
}

int DBExecute(int con_index, const char* query, int maxRows, char **errBuf)
{
	int rc;
	jstring rv;
	jboolean isCopy = JNI_FALSE;

	if (g_jni == NULL || g_objJdbcClient == NULL || g_DBExecute == NULL ||
		g_objMsgBuf == NULL || g_resetVal == NULL || g_getVal == NULL ||
		query == NULL || con_index < 0)
		return(-10);

	g_jni->CallVoidMethod(g_objMsgBuf, g_resetVal);

	rc = g_jni->CallIntMethod(g_objJdbcClient, g_DBExecute,
							con_index,
							g_jni->NewStringUTF(query),
							maxRows,
							g_objMsgBuf);
	if (rc < 0)
	{
		rv = (jstring)g_jni->CallObjectMethod(g_objMsgBuf, g_getVal);
		*errBuf = (char *)g_jni->GetStringUTFChars(rv, &isCopy);
	}
	return(rc);
}


int DBExecuteUtility(int con_index, const char* query, char **errBuf)
{
	int rc;
	jstring rv;
	jboolean isCopy = JNI_FALSE;

	if (g_jni == NULL || g_objJdbcClient == NULL || g_DBExecuteUtility == NULL ||
		g_objMsgBuf == NULL || g_resetVal == NULL || g_getVal == NULL ||
		query == NULL || con_index < 0)
		return(-10);

	g_jni->CallVoidMethod(g_objMsgBuf, g_resetVal);

	rc = g_jni->CallIntMethod(g_objJdbcClient, g_DBExecuteUtility,
							con_index,
							g_jni->NewStringUTF(query),
							g_objMsgBuf);

	if (rc < 0)
	{
		rv = (jstring)g_jni->CallObjectMethod(g_objMsgBuf, g_getVal);
		*errBuf = (char *)g_jni->GetStringUTFChars(rv, &isCopy);
	}

	return(rc);
}

int DBCloseResultSet(int con_index, char **errBuf)
{
	int rc;
	jstring rv;
	jboolean isCopy = JNI_FALSE;

	if (g_jni == NULL || g_objJdbcClient == NULL || g_DBCloseResultSet == NULL ||
		g_objMsgBuf == NULL || g_resetVal == NULL || g_getVal == NULL ||
		con_index < 0)
		return(-10);

	g_jni->CallVoidMethod(g_objMsgBuf, g_resetVal);

	rc = g_jni->CallIntMethod(g_objJdbcClient, g_DBCloseResultSet, con_index, g_objMsgBuf);

	if (rc < 0)
	{
		rv = (jstring)g_jni->CallObjectMethod(g_objMsgBuf, g_getVal);
		*errBuf = (char *)g_jni->GetStringUTFChars(rv, &isCopy);
	}

	return(rc);
}

int DBFetch(int con_index, char **errBuf)
{
	int rc;
	jstring rv;
	jboolean isCopy = JNI_FALSE;

	if (g_jni == NULL || g_objJdbcClient == NULL || g_DBFetch == NULL ||
		g_objMsgBuf == NULL || g_resetVal == NULL || g_getVal == NULL ||
		con_index < 0)
		return(-10);

	g_jni->CallVoidMethod(g_objMsgBuf, g_resetVal);

	rc = g_jni->CallIntMethod(g_objJdbcClient, g_DBFetch, con_index, g_objMsgBuf);

	if (rc < 0)
	{
		rv = (jstring)g_jni->CallObjectMethod(g_objMsgBuf, g_getVal);
		*errBuf = (char *)g_jni->GetStringUTFChars(rv, &isCopy);
	}

	return(rc);
}

int DBGetColumnCount(int con_index, char **errBuf)
{
	int rc;
	jstring rv;
	jboolean isCopy = JNI_FALSE;

	if (g_jni == NULL || g_objJdbcClient == NULL || g_DBGetColumnCount == NULL ||
		g_objMsgBuf == NULL || g_resetVal == NULL || g_getVal == NULL ||
		con_index < 0)
		return(-10);

	g_jni->CallVoidMethod(g_objMsgBuf, g_resetVal);

	rc = g_jni->CallIntMethod(g_objJdbcClient, g_DBGetColumnCount, con_index, g_objMsgBuf);

	if (rc < 0)
	{
		rv = (jstring)g_jni->CallObjectMethod(g_objMsgBuf, g_getVal);
		*errBuf = (char *)g_jni->GetStringUTFChars(rv, &isCopy);
	}

	return(rc);
}

int DBGetFieldAsCString(int con_index, int columnIdx, char **buffer, char **errBuf)
{
	int rc;
	jstring rv;
	jboolean isCopy = JNI_FALSE;

	if (g_jni == NULL || g_objJdbcClient == NULL || g_DBGetFieldAsCString == NULL ||
		g_objMsgBuf == NULL || g_resetVal == NULL || g_getVal == NULL ||
		g_objValBuf == NULL || con_index < 0)
		return(-10);

	g_jni->CallVoidMethod(g_objMsgBuf, g_resetVal);

	g_jni->CallVoidMethod(g_objValBuf, g_resetVal);

	rc = g_jni->CallIntMethod(g_objJdbcClient, g_DBGetFieldAsCString,
							con_index, columnIdx, g_objValBuf, g_objMsgBuf);

	if (rc < 0)
	{
		rv = (jstring)g_jni->CallObjectMethod(g_objMsgBuf, g_getVal);
		*errBuf = (char *)g_jni->GetStringUTFChars(rv, &isCopy);
		return(rc);
	}

	rv = (jstring)g_jni->CallObjectMethod(g_objValBuf, g_getVal);
	*buffer = (char *)g_jni->GetStringUTFChars(rv, &isCopy);

	return(strlen(*buffer));
}
