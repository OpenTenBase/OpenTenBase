/*-------------------------------------------------------------------------
 *
 * elog.h
 *	  POSTGRES error reporting/logging definitions.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/elog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ELOG_H
#define ELOG_H

#include <setjmp.h>
#ifdef __TDX__
#include <pthread.h>
#include "pgtime.h"
#endif
#include "nodes/pg_list.h"
#include "lib/stringinfo.h"

#define ERR_MSGSIZE             (256)

/* Error level codes */
#define DEBUG12		9
#define	DEBUG11		9
#define	DEBUG10		9
#define DEBUG9		9         /* Debugging sending prepare timestamp */
#define DEBUG8		9
#define DEBUG7		9
#define DEBUG6		9

#define DEBUG5		10			/* Debugging messages, in categories of
								 * decreasing detail. */
#define DEBUG4		11
#define DEBUG3		12
#define DEBUG2		13
#define DEBUG1		14			/* used by GUC debug_* variables */
#define LOG			15			/* Server operational messages; sent only to
								 * server log by default. */
#define LOG_SERVER_ONLY 16		/* Same as LOG for server reporting, but never
								 * sent to client. */
#define COMMERROR	LOG_SERVER_ONLY /* Client communication problems; same as
									 * LOG for server reporting, but never
									 * sent to client. */
#define INFO		17			/* Messages specifically requested by user (eg
								 * VACUUM VERBOSE output); always sent to
								 * client regardless of client_min_messages,
								 * but by default not sent to server log. */
#define NOTICE		18			/* Helpful messages to users about query
								 * operation; sent to client and not to server
								 * log by default. */
#define WARNING		19			/* Warnings.  NOTICE is for expected messages
								 * like implicit sequence creation by SERIAL.
								 * WARNING is for unexpected messages. */
#define ERROR		20			/* user error - abort transaction; return to
								 * known state */
/* Save ERROR value in PGERROR so it can be restored when Win32 includes
 * modify it.  We have to use a constant rather than ERROR because macros
 * are expanded only when referenced outside macros.
 */
#ifdef WIN32
#define PGERROR		20
#endif
#define FATAL		21			/* fatal error - abort process */
#define PANIC		22			/* take down the other backends with me */
#ifdef __OPENTENBASE__
#define STOP		23			/* take down the other backends with me, but without coredump */
#endif


 /* #define DEBUG DEBUG1 */	/* Backward compatibility with pre-7.3 */


/* macros for representing SQLSTATE strings compactly */
#define PGSIXBIT(ch)	(((ch) - '0') & 0x3F)
#define PGUNSIXBIT(val) (((val) & 0x3F) + '0')

#define MAKE_SQLSTATE(ch1,ch2,ch3,ch4,ch5)	\
	(PGSIXBIT(ch1) + (PGSIXBIT(ch2) << 6) + (PGSIXBIT(ch3) << 12) + \
	 (PGSIXBIT(ch4) << 18) + (PGSIXBIT(ch5) << 24))

#ifdef _PG_ORCL_
#define IS_ORA_SQLSTATE(_c) ((_c) & (1<<30))
#define MAKE_ORA_SQLSTATE(ch1,ch2,ch3,ch4,ch5)	\
	((PGSIXBIT(ch1) + (PGSIXBIT(ch2) << 6) + (PGSIXBIT(ch3) << 12) + \
	 (PGSIXBIT(ch4) << 18) + (PGSIXBIT(ch5) << 24)) | 1<<30)
#endif

/* These macros depend on the fact that '0' becomes a zero in SIXBIT */
#define ERRCODE_TO_CATEGORY(ec)  ((ec) & ((1 << 12) - 1))
#define ERRCODE_IS_CATEGORY(ec)  (((ec) & ~((1 << 12) - 1)) == 0)

/* SQLSTATE codes for errors are defined in a separate file */
#include "utils/errcodes.h"


/*----------
 * New-style error reporting API: to be used in this way:
 *		ereport(ERROR,
 *				(errcode(ERRCODE_UNDEFINED_CURSOR),
 *				 errmsg("portal \"%s\" not found", stmt->portalname),
 *				 ... other errxxx() fields as needed ...));
 *
 * The error level is required, and so is a primary error message (errmsg
 * or errmsg_internal).  All else is optional.  errcode() defaults to
 * ERRCODE_INTERNAL_ERROR if elevel is ERROR or more, ERRCODE_WARNING
 * if elevel is WARNING, or ERRCODE_SUCCESSFUL_COMPLETION if elevel is
 * NOTICE or below.
 *
 * ereport_domain() allows a message domain to be specified, for modules that
 * wish to use a different message catalog from the backend's.  To avoid having
 * one copy of the default text domain per .o file, we define it as NULL here
 * and have errstart insert the default text domain.  Modules can either use
 * ereport_domain() directly, or preferably they can override the TEXTDOMAIN
 * macro.
 *
 * If elevel >= ERROR, the call will not return; we try to inform the compiler
 * of that via pg_unreachable().  However, no useful optimization effect is
 * obtained unless the compiler sees elevel as a compile-time constant, else
 * we're just adding code bloat.  So, if __builtin_constant_p is available,
 * use that to cause the second if() to vanish completely for non-constant
 * cases.  We avoid using a local variable because it's not necessary and
 * prevents gcc from making the unreachability deduction at optlevel -O0.
 *----------
 */
#ifdef USE_MODULE_MSGIDS
#ifdef HAVE__BUILTIN_CONSTANT_P
#define ereport_domain(elevel, domain, rest)	\
	do { \
		if (errstart(elevel, __FILE__, __LINE__, PGXL_MSG_MODULE, PGXL_MSG_FILEID, __COUNTER__, PG_FUNCNAME_MACRO, domain)) \
			errfinish rest; \
		if (__builtin_constant_p(elevel) && (elevel) >= ERROR) \
			pg_unreachable(); \
	} while(0)
#else							/* !HAVE__BUILTIN_CONSTANT_P */
#define ereport_domain(elevel, domain, rest)	\
	do { \
		const int elevel_ = (elevel); \
		if (errstart(elevel, __FILE__, __LINE__, PGXL_MSG_MODULE, PGXL_MSG_FILEID, __COUNTER__, PG_FUNCNAME_MACRO, domain)) \
			errfinish rest; \
		if (elevel_ >= ERROR) \
			pg_unreachable(); \
	} while(0)
#endif   /* HAVE__BUILTIN_CONSTANT_P */
#else
#ifdef HAVE__BUILTIN_CONSTANT_P
#define ereport_domain(elevel, domain, rest)	\
	do { \
		if (errstart(elevel, __FILE__, __LINE__, PG_FUNCNAME_MACRO, domain)) \
			errfinish rest; \
		if (__builtin_constant_p(elevel) && (elevel) >= ERROR) \
			pg_unreachable(); \
	} while(0)
#else							/* !HAVE__BUILTIN_CONSTANT_P */
#define ereport_domain(elevel, domain, rest)	\
	do { \
		const int elevel_ = (elevel); \
		if (errstart(elevel_, __FILE__, __LINE__, PG_FUNCNAME_MACRO, domain)) \
			errfinish rest; \
		if (elevel_ >= ERROR) \
			pg_unreachable(); \
	} while(0)
#endif							/* HAVE__BUILTIN_CONSTANT_P */
#endif

#define ereport(elevel, rest)	\
	ereport_domain(elevel, TEXTDOMAIN, rest)

#define TEXTDOMAIN NULL

typedef struct pgxc_node_handle PGXCNodeHandle;

extern bool errstart(int elevel, const char *filename, int lineno,
#ifdef USE_MODULE_MSGIDS
		 int moduleid, int fileid, int msgid,
#endif
		 const char *funcname, const char *domain
		 );
extern void errfinish(int dummy,...);

#ifdef _PG_ORCL_
extern int errname(char *name);
#endif

extern int	errcode(int sqlerrcode);

extern int	errcode_for_file_access(void);
extern int	errcode_for_socket_access(void);

extern int	errmsg(const char *fmt,...) pg_attribute_printf(1, 2);
extern int	errmsg_internal(const char *fmt,...) pg_attribute_printf(1, 2);

extern int errmsg_plural(const char *fmt_singular, const char *fmt_plural,
			  unsigned long n,...) pg_attribute_printf(1, 4) pg_attribute_printf(2, 4);

extern int	errdetail(const char *fmt,...) pg_attribute_printf(1, 2);
extern int	errdetail_internal(const char *fmt,...) pg_attribute_printf(1, 2);

extern int	errdetail_log(const char *fmt,...) pg_attribute_printf(1, 2);

extern int errdetail_log_plural(const char *fmt_singular,
					 const char *fmt_plural,
					 unsigned long n,...) pg_attribute_printf(1, 4) pg_attribute_printf(2, 4);

extern int errdetail_plural(const char *fmt_singular, const char *fmt_plural,
				 unsigned long n,...) pg_attribute_printf(1, 4) pg_attribute_printf(2, 4);

extern int	errhint(const char *fmt,...) pg_attribute_printf(1, 2);

/*
 * errcontext() is typically called in error context callback functions, not
 * within an ereport() invocation. The callback function can be in a different
 * module than the ereport() call, so the message domain passed in errstart()
 * is not usually the correct domain for translating the context message.
 * set_errcontext_domain() first sets the domain to be used, and
 * errcontext_msg() passes the actual message.
 */
#define errcontext	set_errcontext_domain(TEXTDOMAIN),	errcontext_msg

extern int	set_errcontext_domain(const char *domain);

extern int	errcontext_msg(const char *fmt,...) pg_attribute_printf(1, 2);

extern int	errhidestmt(bool hide_stmt);
extern int	errhidecontext(bool hide_ctx);

extern int	errnode(PGXCNodeHandle *conn);

extern int	errnodepid(const char *nodename, int pid);

extern int	errfunction(const char *funcname);
extern int	errposition(int cursorpos);

extern int	internalerrposition(int cursorpos);
extern int	internalerrquery(const char *query);

extern int	err_generic_string(int field, const char *str);

extern int	geterrcode(void);
extern int	geterrposition(void);
extern int	getinternalerrposition(void);
extern char *geterrorcontext(void);
extern int geterrorlevel(void);
extern void reseterrorcontext(void);

/*----------
 * Old-style error reporting API: to be used in this way:
 *		elog(ERROR, "portal \"%s\" not found", stmt->portalname);
 *----------
 */
#ifdef USE_MODULE_MSGIDS
#ifdef HAVE__VA_ARGS
/*
 * If we have variadic macros, we can give the compiler a hint about the
 * call not returning when elevel >= ERROR.  See comments for ereport().
 * Note that historically elog() has called elog_start (which saves errno)
 * before evaluating "elevel", so we preserve that behavior here.
 */
#ifdef HAVE__BUILTIN_CONSTANT_P
#define elog(elevel, ...)  \
	do { \
		elog_start(__FILE__, __LINE__, PGXL_MSG_MODULE, PGXL_MSG_FILEID, __COUNTER__, PG_FUNCNAME_MACRO); \
		elog_finish(elevel, __VA_ARGS__); \
		if (__builtin_constant_p(elevel) && (elevel) >= ERROR) \
			pg_unreachable(); \
	} while(0)
#else							/* !HAVE__BUILTIN_CONSTANT_P */
#define elog(elevel, ...)  \
	do { \
		int		elevel_; \
		elog_start(__FILE__, __LINE__, PGXL_MSG_MODULE, PGXL_MSG_FILEID, __COUNTER__, PG_FUNCNAME_MACRO); \
		elevel_ = (elevel); \
		elog_finish(elevel_, __VA_ARGS__); \
		if (elevel_ >= ERROR) \
			pg_unreachable(); \
	} while(0)
#endif   /* HAVE__BUILTIN_CONSTANT_P */
#else							/* !HAVE__VA_ARGS */
#define elog  \
	do { \
		elog_start(__FILE__, __LINE__, PGXL_MSG_MODULE, PGXL_MSG_FILEID, __COUNTER__, PG_FUNCNAME_MACRO); \
	} while (0); \
	elog_finish
#endif   /* HAVE__VA_ARGS */
#else
#ifdef HAVE__VA_ARGS
#ifdef HAVE__BUILTIN_CONSTANT_P
#define elog(elevel, ...)  \
	do { \
		elog_start(__FILE__, __LINE__, PG_FUNCNAME_MACRO); \
		elog_finish(elevel, __VA_ARGS__); \
		if (__builtin_constant_p(elevel) && (elevel) >= ERROR) \
			pg_unreachable(); \
	} while(0)
#else							/* !HAVE__BUILTIN_CONSTANT_P */
#define elog(elevel, ...)  \
	do { \
		elog_start(__FILE__, __LINE__, PG_FUNCNAME_MACRO); \
		{ \
			const int elevel_ = (elevel); \
			elog_finish(elevel_, __VA_ARGS__); \
			if (elevel_ >= ERROR) \
				pg_unreachable(); \
		} \
	} while(0)
#endif							/* HAVE__BUILTIN_CONSTANT_P */
#else							/* !HAVE__VA_ARGS */
#define elog  \
	do { \
		elog_start(__FILE__, __LINE__, PG_FUNCNAME_MACRO); \
	} while (0); \
	elog_finish
#endif							/* HAVE__VA_ARGS */
#endif

extern void elog_start(const char *filename, int lineno,
#ifdef USE_MODULE_MSGIDS
		int moduleid, int flieid, int msgid,
#endif
		const char *funcname
		);
extern void elog_finish(int elevel, const char *fmt,...) pg_attribute_printf(2, 3);


/* Support for constructing error strings separately from ereport() calls */

extern void pre_format_elog_string(int errnumber, const char *domain);
extern char *format_elog_string(const char *fmt,...) pg_attribute_printf(1, 2);


/* Support for attaching context information to error reports */

typedef struct ErrorContextCallback
{
	struct ErrorContextCallback *previous;
	void		(*callback) (void *arg);
	void	   *arg;
} ErrorContextCallback;

extern PGDLLIMPORT __thread ErrorContextCallback *error_context_stack;


#ifndef __TDX__
/*----------
 * API for catching ereport(ERROR) exits.  Use these macros like so:
 *
 *		PG_TRY();
 *		{
 *			... code that might throw ereport(ERROR) ...
 *		}
 *		PG_CATCH();
 *		{
 *			... error recovery code ...
 *		}
 *		PG_END_TRY();
 *
 * (The braces are not actually necessary, but are recommended so that
 * pgindent will indent the construct nicely.)	The error recovery code
 * can optionally do PG_RE_THROW() to propagate the same error outwards.
 *
 * Note: while the system will correctly propagate any new ereport(ERROR)
 * occurring in the recovery section, there is a small limit on the number
 * of levels this will work for.  It's best to keep the error recovery
 * section simple enough that it can't generate any new errors, at least
 * not before popping the error stack.
 *
 * Note: an ereport(FATAL) will not be caught by this construct; control will
 * exit straight through proc_exit().  Therefore, do NOT put any cleanup
 * of non-process-local resources into the error recovery section, at least
 * not without taking thought for what will happen during ereport(FATAL).
 * The PG_ENSURE_ERROR_CLEANUP macros provided by storage/ipc.h may be
 * helpful in such cases.
 *
 * Note: if a local variable of the function containing PG_TRY is modified
 * in the PG_TRY section and used in the PG_CATCH section, that variable
 * must be declared "volatile" for POSIX compliance.  This is not mere
 * pedantry; we have seen bugs from compilers improperly optimizing code
 * away when such a variable was not marked.  Beware that gcc's -Wclobbered
 * warnings are just about entirely useless for catching such oversights.
 *----------
 */
#define PG_TRY()  \
	do { \
		sigjmp_buf *save_exception_stack = PG_exception_stack; \
		ErrorContextCallback *save_context_stack = error_context_stack; \
		sigjmp_buf local_sigjmp_buf; \
		if (sigsetjmp(local_sigjmp_buf, 0) == 0) \
		{ \
			PG_exception_stack = &local_sigjmp_buf

#define PG_CATCH()	\
		} \
		else \
		{ \
			PG_exception_stack = save_exception_stack; \
			error_context_stack = save_context_stack

#define PG_END_TRY()  \
		} \
		PG_exception_stack = save_exception_stack; \
		error_context_stack = save_context_stack; \
	} while (0)
#endif

/*
 * Some compilers understand pg_attribute_noreturn(); for other compilers,
 * insert pg_unreachable() so that the compiler gets the point.
 */
#ifdef HAVE_PG_ATTRIBUTE_NORETURN
#define PG_RE_THROW()  \
	pg_re_throw()
#else
#define PG_RE_THROW()  \
	(pg_re_throw(), pg_unreachable())
#endif

#ifndef __TDX__
extern PGDLLIMPORT __thread sigjmp_buf *PG_exception_stack;
#endif


typedef struct ErrorHintData
{
	char column_name[NAMEDATALEN];
} ErrorHintData;

/* Stuff that error handlers might want to use */

/*
 * ErrorData holds the data accumulated during any one ereport() cycle.
 * Any non-NULL pointers must point to palloc'd data.
 * (The const pointers are an exception; we assume they point at non-freeable
 * constant strings.)
 */
typedef struct ErrorData
{
	int			elevel;			/* error level */
	bool		output_to_server;	/* will report to server log? */
	bool		output_to_client;	/* will report to client? */
	bool		show_funcname;	/* true to force funcname inclusion */
	bool		hide_stmt;		/* true to prevent STATEMENT: inclusion */
	bool		hide_ctx;		/* true to prevent CONTEXT: inclusion */
	const char *filename;		/* __FILE__ of ereport() call */
	int			lineno;			/* __LINE__ of ereport() call */
	const char *funcname;		/* __func__ of ereport() call */
	const char *domain;			/* message domain */
	const char *context_domain; /* message domain for context message */
	int			sqlerrcode;		/* encoded ERRSTATE */
	char       *nodename;       /* error node */
	int         nodepid;        /* error node pid */
	char	   *message;		/* primary error message (translated) */
	char	   *detail;			/* detail error message */
	char	   *detail_log;		/* detail error message for server log only */
	char	   *hint;			/* hint message */
	char	   *context;		/* context message */
	const char *message_id;		/* primary message's id (original string) */
	char	   *schema_name;	/* name of schema */
	char	   *table_name;		/* name of table */
	char	   *column_name;	/* name of column */
	char	   *datatype_name;	/* name of datatype */
	char	   *constraint_name;	/* name of constraint */
	int			cursorpos;		/* cursor index into query string */
	int			internalpos;	/* cursor index into internalquery */
	char	   *internalquery;	/* text of internally-generated query */
	int			saved_errno;	/* errno at entry */
#ifdef _PG_ORCL_
	NameData	errname; /* short name of current error */
#endif
#ifdef USE_MODULE_MSGIDS
	int			moduleid;
	int			fileid;
	int			msgid;			/* msgid */
#endif
	void	   *stacktracearray[30];
	size_t		stacktracesize;

	int			spi_connected;

	/* context containing associated non-constant strings */
	struct MemoryContextData *assoc_context;
} ErrorData;

#ifdef __TDX__
#define ERRORDATA_STACK_SIZE  20
extern pthread_key_t threadinfo_key;

typedef struct TDX_ThreadInfo
{
	pthread_t thr_id;
	sigjmp_buf *thr_sigjmp_buf;
	ErrorData thr_error_data[ERRORDATA_STACK_SIZE];
	int thr_error_stack_depth;
	int thr_error_recursion_depth;
	bool opentenbase_ora_compatible;
	pg_tz *session_tz;
} TDX_ThreadInfo;

#define SetTDXThreadInfo(thrinfo)        pthread_setspecific(threadinfo_key, (thrinfo))
#define GetTDXThreadInfo                ((TDX_ThreadInfo *)pthread_getspecific(threadinfo_key))
#define PG_exception_stack              (GetTDXThreadInfo->thr_sigjmp_buf)

#define PG_TRY()  \
	do { \
		sigjmp_buf *save_exception_stack = PG_exception_stack; \
		sigjmp_buf local_sigjmp_buf; \
		if (sigsetjmp(local_sigjmp_buf, 0) == 0) \
		{ \
			PG_exception_stack = &local_sigjmp_buf

#define PG_CATCH()	\
		} \
		else \
		{ \
			PG_exception_stack = save_exception_stack

#define PG_END_TRY()  \
		} \
		PG_exception_stack = save_exception_stack; \
	} while (0)
#endif

/* Use local_sigjmp_buf to handle sub thread local longjmp
 *
 * If an exception is encountered, processing here.	\
 * Processing a long jump cause by error in the sub thread.	\
 */
#define SUB_THREAD_LONGJMP_DEFAULT_CHECK()  \
	sigjmp_buf local_sigjmp_buf;	\
	am_sub_thread = true;	\
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)	\
	{	\
		/* never be here */	\
		ErrorData *edata = CopyErrorData();	\
		elog(LOG, "got error: %s", edata->message);	\
		elog(PANIC, "unexpected long jump in this thread");	\
	}	\
	PG_exception_stack = &local_sigjmp_buf

#ifdef USE_MODULE_MSGIDS
#define PGXL_MSG_MAX_MODULES			256
#define PGXL_MSG_MAX_FILEIDS_PER_MODULE	100
#define PGXL_MSG_MAX_MSGIDS_PER_FILE	300

extern Size MsgModuleShmemSize(void);
extern void MsgModuleShmemInit(void);
extern void AtProcStart_MsgModule(void);
#endif

extern void EmitErrorReport(void);
extern ErrorData *CopyErrorData(void);
extern char *GetErrorMessage(void);
extern void FreeErrorData(ErrorData *edata);
extern void FlushErrorState(void);
extern void ReThrowError(ErrorData *edata) pg_attribute_noreturn();
extern void ThrowErrorData(ErrorData *edata);
extern void pg_re_throw(void) pg_attribute_noreturn();

extern char *GetErrorContextStack(void);
extern void push_err_ctxt(void *ctxt);
extern List *get_errctxtstack(void);
extern void set_curr_error_data(ErrorData *curr);
extern void trunc_errctxtstack(void);

/* Hook for intercepting messages before they are sent to the server log */
typedef void (*emit_log_hook_type) (ErrorData *edata);
extern PGDLLIMPORT __thread emit_log_hook_type emit_log_hook;

/* only for error-handler. */
int elog_geterrcode(void);

/* GUC-configurable parameters */

typedef enum
{
	PGERROR_TERSE,				/* single-line error messages */
	PGERROR_DEFAULT,			/* recommended style */
	PGERROR_VERBOSE				/* all the facts, ma'am */
}			PGErrorVerbosity;

extern int	Log_error_verbosity;
extern char *Log_line_prefix;
extern int	Log_destination;
extern char *Log_destination_string;
extern bool syslog_sequence_numbers;
extern bool syslog_split_messages;
extern int   error_stacktrace_level;

/* Log destination bitmap */
#define LOG_DESTINATION_STDERR	 1
#define LOG_DESTINATION_SYSLOG	 2
#define LOG_DESTINATION_EVENTLOG 4
#define LOG_DESTINATION_CSVLOG	 8

/* Error message */
#define ERRMSG_SELECT_INTO_NO_DATA_FOUND "query returned no rows"
#define ERRMSG_ATXACT_UNCOMPLETED "active autonomous transaction is not completed and rollback it"

/* Other exported functions */
extern void DebugFileOpen(void);
extern char *unpack_sql_state(int sql_state);
extern bool in_error_recursion_trouble(void);
extern void print_stacktrace(void);

#ifdef HAVE_SYSLOG
extern void set_syslog_parameters(const char *ident, int facility);
#endif

typedef enum
{
	STACKTRACE_OFF = 0,			/* no action */
	STACKTRACE_LOG = 1,			/* print stacktrace in log */
	STACKTRACE_CORE = 2			/* core immediate */
} ErrorStacktraceLevel;

typedef enum
{
	VERIFY_ACTION_NONE = 0,
	VERIFY_ACTION_TEST = 1,
	VERIFY_ACTION_WARNING = WARNING,
	VERIFY_ACTION_ERROR = ERROR,
	VERIFY_ACTION_PANIC = PANIC
} AdvVerifyAction;

typedef enum
{
	HINT_COLUMN = 0,
}HintName;

/*
 * Write errors to stderr (or by equal means when stderr is
 * not available). Used before ereport/elog can be used
 * safely (memory context, GUC load etc)
 */
extern void write_stderr(const char *fmt, ...) pg_attribute_printf(1, 2);

/* opentenbase_ora: Get and set the global current error data. */
extern ErrorData *get_curr_error_data(void);
extern void set_curr_error_data(ErrorData *curr);
extern void elog_hint(int hintType, const char *hintmsg);
extern void append_hint_message(ErrorData *edata);
extern void elog_hint_reset(void);

#endif							/* ELOG_H */
