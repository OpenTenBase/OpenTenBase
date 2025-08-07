/*-------------------------------------------------------------------------
 *
 * xact_whitebox.c
 *	  xact_whitebox stub injection implementations
 *
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/xact_whitebox/xact_whitebox.c
 *
 * NOTES:
 *
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "access/parallel.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "postmaster/autovacuum.h"
#include "postmaster/clean2pc.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/sampling.h"
#include "utils/varlena.h"
#include "utils/xact_whitebox.h"

#ifdef USE_WHITEBOX_INJECTION
InjectionStub * MainInjectionStubArray = NULL;
char * injection_stub_setting_placeholder = NULL;

extern char * XactWhiteBoxStubNames[];
extern char * InjectionFailureTypeStrings[];
extern char * InjectionFailureErrorLevelStrings[];
extern char * InjectionSleepOnlyStub[];
extern char * InjectionFailureNodeStrings[];

int 	injection_stub_default_probability = PROBABILITY_LOWERBOUND;
int 	injection_stub_default_failure_type = WHITEBOX_INACTIVE;
int 	injection_stub_default_sleep_time = SLEEPTIME_LOWERBOUND;
int		injection_stub_default_failure_node = ALL_NODE;

/*
 * Stubs here defined for sleep only purpose. They can
 * only be triggered with sleep type failure.
 *
 * Note that repeat implementation takes a different
 * route, as they can not be designated from input.
 */
#define NUM_SLEEPONLY_STUB (4)
int InjectionSleepOnlyStubIndex[] =
{
	SYNC_REPLICATE_CYCLE_INDEX,
	XLOG_FLUSH_INDEX,
	WAL_SENDER_MAINLOOP_INDEX,
	WAL_RECEIVER_PROCESS_DATA_INDEX
};


/*
 * Whitebox-failureType related methods.
 */

static bool
check_whitebox_failure_type(const char* failure_type)
{
	int i;

	for(i = 0; i < MAX_FAILURE_TYPE; i++)
	{
		if (pg_strcasecmp(failure_type, InjectionFailureTypeStrings[i]) == 0)
			return true;
	}

	return false;
}

static bool
check_whitebox_failure_node(const char* failure_node)
{
	int i;

	for(i = 0; i < MAX_FAILURE_NODE_TYPE; i++)
	{
		if (pg_strcasecmp(failure_node, InjectionFailureNodeStrings[i]) == 0)
			return true;
	}

	return false;
}

static InjectionFailureType
get_whitebox_failure_type(const char* failure_type)
{
	int i;
	for(i = 0; i < MAX_FAILURE_TYPE; i++)
	{
		if (pg_strcasecmp(failure_type, InjectionFailureTypeStrings[i]) == 0)
			return (InjectionFailureType) i;
	}

	/* Keep compiler quiet */
	return WHITEBOX_INACTIVE;
}

static InjectionFailureNode
get_whitebox_failure_node(const char* failure_node)
{
	int i;
	for(i = 0; i < MAX_FAILURE_NODE_TYPE; i++)
	{
		if (pg_strcasecmp(failure_node, InjectionFailureNodeStrings[i]) == 0)
			return (InjectionFailureNode) i;
	}

	/* Keep compiler quiet */
	return ALL_NODE;
}

static int
get_whitebox_error_level_from_failure(InjectionFailureType type)
{
	switch(type)
	{
		case WHITEBOX_ERROR:
			return ERROR;
			break;
		case WHITEBOX_FATAL:
			return FATAL;
			break;
		case WHITEBOX_PANIC:
			return PANIC;
			break;
		case WHITEBOX_SLEEP:
			return LOG;
			break;
		default:
			return DEBUG5;
			break;
	}
}

/*
 * Input-check related methods.
 */
static inline bool
check_whitebox_probability(int probability)
{
	return ((probability >= PROBABILITY_LOWERBOUND)&&(probability <= PROBABILITY_UPPERBOUND));
}

static inline bool
check_whitebox_sleeptime(int probability)
{
	return ((probability >= SLEEPTIME_LOWERBOUND)&&(probability <= SLEEPTIME_UPPERBOUND));
}

static bool
check_whitebox_sleep_only_stub(const char * stubname, const char * failure_type)
{
	int i;

	for (i = 0; i < NUM_SLEEPONLY_STUB; i++)
	{
		if ((pg_strcasecmp(stubname, InjectionSleepOnlyStub[i]) == 0)
			&& (pg_strcasecmp(failure_type, InjectionFailureTypeStrings[5]) != 0))
		return false;
	}

	return true;
}


/*
 * Usually called when a normal postgres backend is initialized.
 */
void
initialize_injection_array()
{
    MemoryContext oldContext;
	int i;
    
    oldContext = MemoryContextSwitchTo(TopMemoryContext);
	MainInjectionStubArray = (InjectionStub *) palloc0((MAX_WHITEBOX_STUB_NUM + 1) * sizeof(InjectionStub));
	for (i = 1; i <= MAX_WHITEBOX_STUB_NUM; i++)
	{
		strcpy(MainInjectionStubArray[i].stub_name, XactWhiteBoxStubNames[i]);
		MainInjectionStubArray[i].failure_type = WHITEBOX_INACTIVE;
		MainInjectionStubArray[i].trigger_probability = -1;
		MainInjectionStubArray[i].sleep_time = -1;
		MainInjectionStubArray[i].failed_node = LOCAL_CN;
	}
    
    MemoryContextSwitchTo(oldContext);
}


/*
 * "injection_stub_setting" GUC processing.
 * It takes a input and apply to that specific stub.
 * 
 * Note that this utilize guc mem interfaces as demanded.
 */

bool
check_whitebox_injection(char ** newval, void ** extra, GucSource source)
{
	char * input_string = NULL;
	List * params = NIL;
	int	num_params = 0;
	int i;
	InjectionStubInput * input;

	if (*newval == NULL)
		return true;

	input_string = guc_strdup(ERROR, *newval);
	if (!SplitIdentifierString(input_string, ',', &params))
	{
		GUC_check_errdetail("Format of given whitebox injection stub \"%s\" is incorrect.", input_string);
		goto return_error;
	}

	num_params = list_length(params);

	if (num_params != 5)
	{
		GUC_check_errdetail("Number of parameters in given whitebox "
							"injection stub \"%s\" is incorrect, expecting 5 parameters.", input_string);
		goto return_error;
	}

	if (!check_whitebox_failure_type((char*)list_nth(params, 1)))
	{
		GUC_check_errdetail("Failure type in given whitebox injection stub \"%s\" is incorrect.", input_string);
		goto return_error;
	}

	if (!check_whitebox_probability(pg_strtoint32((char*)list_nth(params, 2))))
	{
		GUC_check_errdetail("Probablity setting in given whitebox injection stub \"%s\" is incorrect.", input_string);
		goto return_error;
	}

	if (!check_whitebox_sleeptime(pg_strtoint32((char*)list_nth(params, 3))))
	{
		GUC_check_errdetail("sleep time setting in given whitebox injection stub \"%s\" is incorrect.", input_string);
		goto return_error;
	}	

	if (!check_whitebox_failure_node((char*)list_nth(params, 4)))
	{
		GUC_check_errdetail("Failure node in given whitebox injection stub \"%s\" is incorrect.", input_string);
		goto return_error;
	}

	if (!check_whitebox_sleep_only_stub((char*)list_nth(params, 1), (char*)list_nth(params, 2)))
	{
		GUC_check_errdetail("Given whitebox injection stub \"%s\" can only have SLEEP as failure type.", input_string);
		goto return_error;
	}	

	if (MainInjectionStubArray == NULL)
	{
		initialize_injection_array();
	}

	input = (InjectionStubInput *) guc_malloc(ERROR, sizeof(InjectionStubInput));
	memset(input, 0, sizeof(InjectionStubInput));

	for (i = 1; i <= MAX_WHITEBOX_STUB_NUM; i++)
	{
		if (pg_strcasecmp(MainInjectionStubArray[i].stub_name, (char*)list_nth(params, 0)) == 0)
		{
			input->stub_index = i;
			input->stub.failure_type = get_whitebox_failure_type((char*)list_nth(params, 1));
			input->stub.trigger_probability = pg_strtoint32((char*)list_nth(params, 2));
			input->stub.sleep_time = pg_strtoint32((char*)list_nth(params, 3));
			input->stub.failed_node = get_whitebox_failure_node((char*)list_nth(params, 4));
		}
	}

	if (input->stub_index == 0)
		goto return_error;

	*extra = (void *)input;

	free(input_string);
	list_free_ext(params);
	return true;

return_error:
	free(input_string);
	list_free_ext(params);
	return false;

}

void
assign_whitebox_injection(const char* newval, void* extra)
{
	InjectionStubInput * input;

	if (extra == NULL)
		return;

	input = (InjectionStubInput *)extra;
	MainInjectionStubArray[input->stub_index].failure_type =  input->stub.failure_type;
	MainInjectionStubArray[input->stub_index].trigger_probability = input->stub.trigger_probability;
	MainInjectionStubArray[input->stub_index].sleep_time = input->stub.sleep_time;
	MainInjectionStubArray[input->stub_index].failed_node = input->stub.failed_node;
}

static bool
is_stub_triggered(int probability)
{
	double random = 0.0;
	double probability_precise = -1.0;

	if (probability < 0)
		return false;

	probability_precise = probability/(PROBABILITY_UPPERBOUND * 1.0);

	random = anl_random_fract();
	return (random <= probability_precise);
}

static inline bool
is_fast_exit()
{
	/* Invoke only in normal backend processing */
	if (proc_exit_inprogress
		|| IsInplaceUpgrade
		|| RecoveryInProgress()
		|| IsParallelWorker()
		|| !IsNormalProcessingMode()
		|| !IsUnderPostmaster
		|| IsBackgroundWorker)
		return true;

	/* Invoke only on normal postgres backend */
	if (IsAnyAutoVacuumProcess()
		|| IsAnyClean2pcProcess())
		return true;

	if (MainInjectionStubArray == NULL)
		return true;

	return false;
}

/*
 * Sleep triggering function. It will hold interrupt during sleep, as in design.
 *
 * Takes independent setting as higher priority. Output only on LOG level.
 */
static bool
whitebox_trigger_sleep(InjectionStub * stub, WhiteBoxInjectionInfo * info)
{
	int elevel = get_whitebox_error_level_from_failure(WHITEBOX_SLEEP);
	int sleeptime
		= (stub->sleep_time == -1)
			? ((injection_stub_default_sleep_time == -1) ? -1 : injection_stub_default_sleep_time)
			: stub->sleep_time;
	
	if (sleeptime <= 0)
		return false;

	ereport(elevel,
		(errcode(ERRCODE_INTERNAL_ERROR),
		 errmsg("Transaction whitebox testing stub %s triggered in %s:%d-(%s), "
		 		"sleep starts now for %d seconds.",
				stub->stub_name, info->filename, info->lineno, info->funcname, stub->sleep_time)));	

	/*
	 * Here we try to mimic ideas that our backend has come to a hang-up and no longer
	 * responding. In this case we hold the intterupts for the effect.
	 *
	 * Also we try to sleep for a full length of what user desired. If we would receive
	 * unexpected interrupt, continue sleeping.
	 */
	HOLD_INTERRUPTS();
	pg_usleep_well(sleeptime * 1000000L, 10000);
	RESUME_INTERRUPTS();
	
	ereport(elevel,
		(errcode(ERRCODE_INTERNAL_ERROR),
		 errmsg("Transaction whitebox testing stub %s triggered in %s:%d-(%s), "
		 		"sleep end now for %d seconds.",
				stub->stub_name, info->filename, info->lineno, info->funcname, stub->sleep_time)));	

	return true;
}

/*
 * Fault(ERROR/FATAL/PANIC) triggering function. Call sleep triggering function if set to sleep.
 *
 * Takes independent setting as higher priority.
 */
static bool
whitebox_trigger_default(InjectionStub * stub, WhiteBoxInjectionInfo * info)
{
	int elevel = DEBUG5;
	bool result = false;
	InjectionFailureType failure_type
		= (stub->failure_type == WHITEBOX_INACTIVE)
			? ((injection_stub_default_failure_type == WHITEBOX_INACTIVE) ? WHITEBOX_INACTIVE : injection_stub_default_failure_type)
			: stub->failure_type;

	if (failure_type == WHITEBOX_INACTIVE)
		return false;

	switch(failure_type)
	{
		case WHITEBOX_INACTIVE:
			result = false;
			break;
		case WHITEBOX_SLEEP:
			result = whitebox_trigger_sleep(stub, info);
			break;
		case WHITEBOX_ERROR:
		case WHITEBOX_FATAL:
		case WHITEBOX_PANIC:
			elevel = get_whitebox_error_level_from_failure(failure_type);
			ereport(elevel,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Transaction whitebox testing stub %s triggered in %s:%d-(%s), "
				 		"with current xid %d, gid (if any) %s, info: %s.",
						stub->stub_name, info->filename, info->lineno, info->funcname, info->xid, info->gid, info->info_string)));
			result = true;
			break;
		default:
			ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Unexpected GUC setting for Transaction Whitebox testing stub %s.", stub->stub_name)));
			result = false;
			break;
	}

	return result;
}

/*
 * Whitebox API for calling from the outside.
 * 
 * Will trigger for types other than repeat. For repeat, if will only tells the caller the stub is triggered.
 * Info_string is designed to log or print info because ereport will long jump away from caller follow-up code.
 *
 * Note: Maybe consider using const expr for less performance lost?
 */
bool
whitebox_trigger_generic(InjectionStub * stub, InjectionTriggeringType trigger_type, 
								const char * filename, int lineno, const char * funcname, const char * gid, const char * info_string)
{
	bool result = false;
	int probability = PROBABILITY_LOWERBOUND;
	int failed_node;
	WhiteBoxInjectionInfo info = {"", 0, "", InvalidTransactionId, "", InvalidGlobalTimestamp, ""};

	if (likely(is_fast_exit()))
		return false;
	
	if (stub->trigger_probability != PROBABILITY_LOWERBOUND)
	{
		probability = stub->trigger_probability;
		failed_node = stub->failed_node;
	}
	else if (injection_stub_default_probability != PROBABILITY_LOWERBOUND)
	{
		probability = injection_stub_default_probability;
		failed_node = injection_stub_default_failure_node;
	}
	else
	{
		return false;
	}

	switch (failed_node)
	{
		case LOCAL_CN:
			if (!IS_PGXC_COORDINATOR || IsConnFromCoord())
				return false;
			break;
		case ONE_CN:
			if (!IS_PGXC_COORDINATOR || !IsConnFromCoord() || PGXCNodeId != 2)
				return false;
			break;
		case ALL_CN:
			if (!IS_PGXC_COORDINATOR)
				return false;
			break;
		case ONE_DN:
			if (!IS_PGXC_DATANODE || PGXCNodeId != 1)
				return false;
			break;
		case ALL_DN:
			if (!IS_PGXC_DATANODE)
				return false;
			break;
		case ALL_NODE:
			break;
	}

	if (!is_stub_triggered(probability))
		return false;

	if (pg_strcasecmp(stub->stub_name, "SYNC_REL_RECEIVE_CANCELED") == 0)
	{
		QueryCancelPending = true;
		ereport(WARNING,
				(errmsg("Transaction whitebox testing stub SYNC_REL_RECEIVE_CANCELED")));
		return true;
	}

	/* Save info for logging and printing */
	strcpy(info.filename, filename);
	info.lineno = lineno;
	strcpy(info.funcname, funcname);
	info.xid = GetCurrentTransactionIdIfAny();
	if (gid == NULL || strlen(gid) > MAX_GID_LEN - 1)
	{
		strcpy(info.gid, "NULL");
	}
	else
	{
		strcpy(info.gid, gid);
	}
	info.gid[MAX_GID_LEN - 1] = '\0';
	if (!(info_string == NULL || strlen(info_string) > MAX_GID_LEN - 1))
	{
		strcpy(info.info_string, info_string);
	}
	info.info_string[MAX_GID_LEN - 1] = '\0';

	/* ... and then do the work */
	switch(trigger_type)
	{
		case WHITEBOX_TRIGGER_DEFAULT:
			result = whitebox_trigger_default(stub, &info);
			break;
		case WHITEBOX_TRIGGER_SLEEP:
			result = whitebox_trigger_sleep(stub, &info);
			break;
		case WHITEBOX_TRIGGER_REPEAT:
			result = true;
			break;
		default:
			/* Silently return without triggering anything */
			result = false;
			break;
	}

	return result;
}

/*
 * Whitebox API for calling from the outside for those stubs serves only for setting purpose.
 */
bool
whitebox_trigger_setting(InjectionStub * stub, InjectionTriggeringType trigger_type)
{
	if (likely(is_fast_exit()) || stub == NULL)
		return false;

	if ((stub->trigger_probability != PROBABILITY_LOWERBOUND)
		|| (injection_stub_default_probability != PROBABILITY_LOWERBOUND))
		return true;

	return false;
}
#endif
