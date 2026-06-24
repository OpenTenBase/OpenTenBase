/*-------------------------------------------------------------------------
 *
 * xact_whitebox.c
 *	  xact_whitebox stub injection structural definitions
 *
 * src/include/utils/xact_whitebox.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef XACT_WHITEBOX_H
#define XACT_WHITEBOX_H

#include "utils/guc.h"
#include "utils/elog.h"
#include "utils/xact_whitebox_stubnames.h"

#ifdef USE_WHITEBOX_INJECTION
#define PROBABILITY_UPPERBOUND (10000)
#define PROBABILITY_LOWERBOUND (-1)
#define SLEEPTIME_UPPERBOUND (300)
#define SLEEPTIME_LOWERBOUND (-1)

/* 
 * Note: make sure MAX_GID_LEN is aligned with that of preparedGID
 */
#define MAX_NAME_STR_LEN 256
#define MAX_INFO_STR_LEN 512
#define MAX_GID_LEN 256

#define INJECTION_LOCATION __FILE__, __LINE__, PG_FUNCNAME_MACRO

/*
 * MAX_FAILURE_TYPE - InjectionFailureType/InjectionFailureTypeStrings
 * MAX_ERROR_LEVEL - InjectionFailureErrorLevelStrings
 * MAX_TRIGGER_TYPE - InjectionTriggeringType
 */
#define MAX_FAILURE_TYPE 5
#define MAX_ERROR_LEVEL 4
#define MAX_TRIGGER_TYPE 4
#define MAX_FAILURE_NODE_TYPE 6

/*
 * We have two different struct for Injection failure itself and its
 * triggering. The inline reason for this is to leave REPEAT as an
 * failure type out of user designation.
 *
 * Only specific stubs can be used for injection repeat sending msgs.
 * So REPEAT is only for those stubs, if it actually triggers.
 *
 * Therefore we have InjectionFailureType, used mostly as input
 * parameters, and InjectionTriggeringType used for trigger impl.
 */
typedef enum InjectionFailureType
{
	WHITEBOX_INACTIVE = 0,
	WHITEBOX_ERROR,
	WHITEBOX_FATAL,
	WHITEBOX_PANIC,
	WHITEBOX_SLEEP
} InjectionFailureType;

typedef enum InjectionFailureNode
{
	LOCAL_CN = 0,
	ONE_CN,
	ALL_CN,
	ONE_DN,
	ALL_DN,
	ALL_NODE
} InjectionFailureNode;

typedef enum InjectionTriggeringType
{
	WHITEBOX_TRIGGER_DEFAULT = 0,
	WHITEBOX_TRIGGER_SLEEP,
	WHITEBOX_TRIGGER_REPEAT,
	WHITEBOX_TRIGGER_SETTING
} InjectionTriggeringType;

/*
 * Core struct for whitebox injection.
 */
typedef struct InjectionStub
{
	char stub_name[MAX_NAME_STR_LEN];
	InjectionFailureType failure_type;
	int trigger_probability;
	int sleep_time;
	int failed_node;
} InjectionStub;

/*
 * InjectionStub but for GUC input.
 */
typedef struct InjectionStubInput
{
	InjectionStub stub;
	int stub_index;
} InjectionStubInput;


/*
 * InjectionStub info to pass on if triggered.
 */
typedef struct WhiteBoxInjectionInfo
{
	char filename[MAX_NAME_STR_LEN];
	int lineno;
	char funcname[MAX_NAME_STR_LEN];

	TransactionId xid;
	char gid[MAX_GID_LEN];
	GlobalTimestamp gts;
	char info_string[MAX_INFO_STR_LEN];
} WhiteBoxInjectionInfo;

extern InjectionStub * MainInjectionStubArray;
extern char * injection_stub_setting_placeholder;

extern int 	injection_stub_default_probability;
extern int 	injection_stub_default_failure_type;
extern int 	injection_stub_default_sleep_time;
extern int 	injection_stub_default_failure_node;

extern void initialize_injection_array(void);
extern bool check_whitebox_injection(char ** newval, void **extra, GucSource source);
extern void assign_whitebox_injection(const char* newval, void* extra);


extern bool whitebox_trigger_generic(InjectionStub * stub, InjectionTriggeringType trigger_type, 
								const char * filename, int lineno, const char * funcname, const char * gid, const char * info_string);
extern bool whitebox_trigger_setting(InjectionStub * stub, InjectionTriggeringType trigger_type);
#endif
#endif
