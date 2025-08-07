/*-------------------------------------------------------------------------
 *
 * miscinit.c
 *	  miscellaneous initialization support stuff
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/init/miscinit.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/param.h>
#include <signal.h>
#include <time.h>
#include <sys/file.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/prctl.h>
#include <fcntl.h>
#include <unistd.h>
#include <grp.h>
#include <pwd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#ifdef HAVE_UTIME_H
#include <utime.h>
#endif

#ifdef XCP
#include "catalog/namespace.h"
#endif
#include "access/htup_details.h"
#include "catalog/pg_authid.h"
#include "libpq/libpq.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "access/parallel.h"
#ifdef XCP
#include "pgxc/execRemote.h"
#endif
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/clean2pc.h"
#include "postmaster/postmaster.h"
#include "postmaster/job_scheduler.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pg_shmem.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "commands/tablespace.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#ifdef XCP
#include "utils/snapmgr.h"
#endif
#include "utils/pidfile.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/varlena.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#ifdef _MLS_
#include "utils/mls.h"
#endif
#ifdef __OPENTENBASE_C__
#include "postmaster/forward.h"
#endif
#include "pgxc/poolmgr.h"

#define DIRECTORY_LOCK_FILE		"postmaster.pid"

ProcessingMode Mode = InitProcessing;

/* List of lock files to be removed at proc exit */
static List *lock_files = NIL;

static Latch LocalLatchData;

/* ----------------------------------------------------------------
 *		ignoring system indexes support stuff
 *
 * NOTE: "ignoring system indexes" means we do not use the system indexes
 * for lookups (either in hardwired catalog accesses or in planner-generated
 * plans).  We do, however, still update the indexes when a catalog
 * modification is made.
 * ----------------------------------------------------------------
 */

bool		IgnoreSystemIndexes = false;
#ifdef _MLS_
static bool is_postmaster_or_pooler(pid_t other_pid);
#endif

/* ----------------------------------------------------------------
 *				database path / name support stuff
 * ----------------------------------------------------------------
 */

void
SetDatabasePath(const char *path)
{
	/* This should happen only once per process */
	Assert(!DatabasePath);
	DatabasePath = MemoryContextStrdup(TopMemoryContext, path);
}

/*
 * Set data directory, but make sure it's an absolute path.  Use this,
 * never set DataDir directly.
 */
void
SetDataDir(const char *dir)
{
	char	   *new;

	AssertArg(dir);

	/* If presented path is relative, convert to absolute */
	new = make_absolute_path(dir);

	if (DataDir)
		free(DataDir);
	DataDir = new;
}

/*
 * Change working directory to DataDir.  Most of the postmaster and backend
 * code assumes that we are in DataDir so it can use relative paths to access
 * stuff in and under the data directory.  For convenience during path
 * setup, however, we don't force the chdir to occur during SetDataDir.
 */
void
ChangeToDataDir(void)
{
	AssertState(DataDir);

	if (chdir(DataDir) < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not change directory to \"%s\": %m",
						DataDir)));
}


/* ----------------------------------------------------------------
 *	User ID state
 *
 * We have to track several different values associated with the concept
 * of "user ID".
 *
 * AuthenticatedUserId is determined at connection start and never changes.
 *
 * SessionUserId is initially the same as AuthenticatedUserId, but can be
 * changed by SET SESSION AUTHORIZATION (if AuthenticatedUserIsSuperuser).
 * This is the ID reported by the SESSION_USER SQL function.
 *
 * OuterUserId is the current user ID in effect at the "outer level" (outside
 * any transaction or function).  This is initially the same as SessionUserId,
 * but can be changed by SET ROLE to any role that SessionUserId is a
 * member of.  (XXX rename to something like CurrentRoleId?)
 *
 * CurrentUserId is the current effective user ID; this is the one to use
 * for all normal permissions-checking purposes.  At outer level this will
 * be the same as OuterUserId, but it changes during calls to SECURITY
 * DEFINER functions, as well as locally in some specialized commands.
 *
 * SecurityRestrictionContext holds flags indicating reason(s) for changing
 * CurrentUserId.  In some cases we need to lock down operations that are
 * not directly controlled by privilege settings, and this provides a
 * convenient way to do it.
 * ----------------------------------------------------------------
 */
static Oid	AuthenticatedUserId = InvalidOid;
static Oid	SessionUserId = InvalidOid;
static Oid	OuterUserId = InvalidOid;
static Oid	CurrentUserId = InvalidOid;
static NameData AuthenticatedUser = {{0}};

/* We also have to remember the superuser state of some of these levels */
static bool AuthenticatedUserIsSuperuser = false;
static bool SessionUserIsSuperuser = false;

static int	SecurityRestrictionContext = 0;

/* We also remember if a SET ROLE is currently active */
static bool SetRoleIsActive = false;

/*
 * Initialize the basic environment for a postmaster child
 *
 * Should be called as early as possible after the child's startup.
 */
void
InitPostmasterChild(void)
{
	IsUnderPostmaster = true;	/* we are a postmaster subprocess now */

	MyProcPid = getpid();		/* reset MyProcPid */

	MyStartTime = time(NULL);	/* set our start time in case we call elog */

	MyMainThreadId = pthread_self();
	MyLocalThreadId = MyMainThreadId;

	/*
	 * make sure stderr is in binary mode before anything can possibly be
	 * written to it, in case it's actually the syslogger pipe, so the pipe
	 * chunking protocol isn't disturbed. Non-logpipe data gets translated on
	 * redirection (e.g. via pg_ctl -l) anyway.
	 */
#ifdef WIN32
	_setmode(fileno(stderr), _O_BINARY);
#endif

	/* We don't want the postmaster's proc_exit() handlers */
	on_exit_reset();

	/* Initialize process-local latch support */
	InitializeLatchSupport();
	MyLatch = &LocalLatchData;
	InitLatch(MyLatch);

	/*
	 * If possible, make this process a group leader, so that the postmaster
	 * can signal any child processes too. Not all processes will have
	 * children, but for consistency we make all postmaster child processes do
	 * this.
	 */
#ifdef HAVE_SETSID
	if (setsid() < 0)
		elog(FATAL, "setsid() failed: %m");
#endif

#ifdef __OPENTENBASE__
    prctl(PR_SET_PDEATHSIG, SIGKILL);
#endif
}

/*
 * Initialize the basic environment for a standalone process.
 *
 * argv0 has to be suitable to find the program's executable.
 */
void
InitStandaloneProcess(const char *argv0)
{
	Assert(!IsPostmasterEnvironment);

	MyProcPid = getpid();		/* reset MyProcPid */

	MyStartTime = time(NULL);	/* set our start time in case we call elog */

	MyMainThreadId = pthread_self();
	MyLocalThreadId = MyMainThreadId;

	/* Initialize process-local latch support */
	InitializeLatchSupport();
	MyLatch = &LocalLatchData;
	InitLatch(MyLatch);

	/* Compute paths, no postmaster to inherit from */
	if (my_exec_path[0] == '\0')
	{
		if (find_my_exec(argv0, my_exec_path) < 0)
			elog(FATAL, "%s: could not locate my own executable path",
				 argv0);
	}

	if (pkglib_path[0] == '\0')
		get_pkglib_path(my_exec_path, pkglib_path);
}

void
SwitchToSharedLatch(void)
{
	Assert(MyLatch == &LocalLatchData);
	Assert(MyProc != NULL);

	MyLatch = &MyProc->procLatch;

	if (FeBeWaitSet)
		ModifyWaitEvent(FeBeWaitSet, 1, WL_LATCH_SET, MyLatch);

	/*
	 * Set the shared latch as the local one might have been set. This
	 * shouldn't normally be necessary as code is supposed to check the
	 * condition before waiting for the latch, but a bit care can't hurt.
	 */
	SetLatch(MyLatch);
}

void
SwitchBackToLocalLatch(void)
{
	Assert(MyLatch != &LocalLatchData);
	Assert(MyProc != NULL && MyLatch == &MyProc->procLatch);

	MyLatch = &LocalLatchData;

	if (FeBeWaitSet)
		ModifyWaitEvent(FeBeWaitSet, 1, WL_LATCH_SET, MyLatch);

	SetLatch(MyLatch);
}

/*
 * GetUserId - get the current effective user ID.
 *
 * Note: there's no SetUserId() anymore; use SetUserIdAndSecContext().
 */
Oid
GetUserId(void)
{
	AssertState(OidIsValid(CurrentUserId));
	return CurrentUserId;
}


/*
 * GetOuterUserId/SetOuterUserId - get/set the outer-level user ID.
 */
Oid
GetOuterUserId(void)
{
	AssertState(OidIsValid(OuterUserId));
	return OuterUserId;
}

static void
SetOuterUserId(Oid userid, bool is_superuser)
{
	AssertState(SecurityRestrictionContext == 0);
	AssertArg(OidIsValid(userid));
	OuterUserId = userid;

	/* We force the effective user ID to match, too */
	CurrentUserId = userid;

	/* Also update the is_superuser GUC to match OuterUserId's property */
	SetConfigOption("is_superuser",
					is_superuser ? "on" : "off",
					PGC_INTERNAL, PGC_S_OVERRIDE);
}

/*
 * GetSessionUserId/SetSessionUserId - get/set the session user ID.
 */
Oid
GetSessionUserId(void)
{
	AssertState(OidIsValid(SessionUserId));
	return SessionUserId;
}

bool
GetSessionUserIsSuperuser(void)
{
	Assert(OidIsValid(SessionUserId));
	return SessionUserIsSuperuser;
}

static void
SetSessionUserId(Oid userid, bool is_superuser)
{
	AssertState(SecurityRestrictionContext == 0);
	AssertArg(OidIsValid(userid));
	SessionUserId = userid;
	SessionUserIsSuperuser = is_superuser;
}

/*
 * GetAuthenticatedUserId/SetAuthenticatedUserId - get/set the authenticated
 * user ID
 */
Oid
GetAuthenticatedUserId(void)
{
	AssertState(OidIsValid(AuthenticatedUserId));
	return AuthenticatedUserId;
}

/*
 * Return whether the authenticated user was superuser at connection start.
 */
bool
GetAuthenticatedUserIsSuperuser(void)
{
	Assert(OidIsValid(AuthenticatedUserId));
	return AuthenticatedUserIsSuperuser;
}

void
SetAuthenticatedUserId(Oid userid, bool is_superuser)
{
	Assert(OidIsValid(userid));

	/* In pooler stateless reuse mode, to reset session userid */
	if (!PoolerStatelessReuse)
	{
		/* call only once */
		Assert(!OidIsValid(AuthenticatedUserId));
	}

	AuthenticatedUserId = userid;
	AuthenticatedUserIsSuperuser = is_superuser;

	/* Also mark our PGPROC entry with the authenticated user id */
	/* (We assume this is an atomic store so no lock is needed) */
	MyProc->roleId = userid;
}


/*
 * GetUserIdAndSecContext/SetUserIdAndSecContext - get/set the current user ID
 * and the SecurityRestrictionContext flags.
 *
 * Currently there are three valid bits in SecurityRestrictionContext:
 *
 * SECURITY_LOCAL_USERID_CHANGE indicates that we are inside an operation
 * that is temporarily changing CurrentUserId via these functions.  This is
 * needed to indicate that the actual value of CurrentUserId is not in sync
 * with guc.c's internal state, so SET ROLE has to be disallowed.
 *
 * SECURITY_RESTRICTED_OPERATION indicates that we are inside an operation
 * that does not wish to trust called user-defined functions at all.  This
 * bit prevents not only SET ROLE, but various other changes of session state
 * that normally is unprotected but might possibly be used to subvert the
 * calling session later.  An example is replacing an existing prepared
 * statement with new code, which will then be executed with the outer
 * session's permissions when the prepared statement is next used.  Since
 * these restrictions are fairly draconian, we apply them only in contexts
 * where the called functions are really supposed to be side-effect-free
 * anyway, such as VACUUM/ANALYZE/REINDEX.
 *
 * SECURITY_NOFORCE_RLS indicates that we are inside an operation which should
 * ignore the FORCE ROW LEVEL SECURITY per-table indication.  This is used to
 * ensure that FORCE RLS does not mistakenly break referential integrity
 * checks.  Note that this is intentionally only checked when running as the
 * owner of the table (which should always be the case for referential
 * integrity checks).
 *
 * Unlike GetUserId, GetUserIdAndSecContext does *not* Assert that the current
 * value of CurrentUserId is valid; nor does SetUserIdAndSecContext require
 * the new value to be valid.  In fact, these routines had better not
 * ever throw any kind of error.  This is because they are used by
 * StartTransaction and AbortTransaction to save/restore the settings,
 * and during the first transaction within a backend, the value to be saved
 * and perhaps restored is indeed invalid.  We have to be able to get
 * through AbortTransaction without asserting in case InitPostgres fails.
 */
void
GetUserIdAndSecContext(Oid *userid, int *sec_context)
{
	*userid = CurrentUserId;
	*sec_context = SecurityRestrictionContext;
}

void
SetUserIdAndSecContext(Oid userid, int sec_context)
{
	CurrentUserId = userid;
	SecurityRestrictionContext = sec_context;
}


/*
 * InLocalUserIdChange - are we inside a local change of CurrentUserId?
 */
bool
InLocalUserIdChange(void)
{
	return (SecurityRestrictionContext & SECURITY_LOCAL_USERID_CHANGE) != 0;
}

/*
 * InSecurityRestrictedOperation - are we inside a security-restricted command?
 */
bool
InSecurityRestrictedOperation(void)
{
	return (SecurityRestrictionContext & SECURITY_RESTRICTED_OPERATION) != 0;
}

/*
 * InNoForceRLSOperation - are we ignoring FORCE ROW LEVEL SECURITY ?
 */
bool
InNoForceRLSOperation(void)
{
	return (SecurityRestrictionContext & SECURITY_NOFORCE_RLS) != 0;
}


/*
 * These are obsolete versions of Get/SetUserIdAndSecContext that are
 * only provided for bug-compatibility with some rather dubious code in
 * pljava.  We allow the userid to be set, but only when not inside a
 * security restriction context.
 */
void
GetUserIdAndContext(Oid *userid, bool *sec_def_context)
{
	*userid = CurrentUserId;
	*sec_def_context = InLocalUserIdChange();
}

void
SetUserIdAndContext(Oid userid, bool sec_def_context)
{
	/* We throw the same error SET ROLE would. */
	if (InSecurityRestrictedOperation())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("cannot set parameter \"%s\" within security-restricted operation",
						"role")));
	CurrentUserId = userid;
	if (sec_def_context)
		SecurityRestrictionContext |= SECURITY_LOCAL_USERID_CHANGE;
	else
		SecurityRestrictionContext &= ~SECURITY_LOCAL_USERID_CHANGE;
}


/*
 * Check whether specified role has explicit REPLICATION privilege
 */
bool
has_rolreplication(Oid roleid)
{
	bool		result = false;
	HeapTuple	utup;

	utup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
	if (HeapTupleIsValid(utup))
	{
		result = ((Form_pg_authid) GETSTRUCT(utup))->rolreplication;
		ReleaseSysCache(utup);
	}
	return result;
}

/*
 * Initialize user identity during normal backend startup
 */
void
InitializeSessionUserId(const char *rolename, Oid roleid, bool reset)
{
	HeapTuple	roleTup;
	Form_pg_authid rform;
	char	   *rname;
	bool		is_superuser;

	/*
	 * Don't do scans if we're bootstrapping, none of the system catalogs
	 * exist yet, and they should be owned by postgres anyway.
	 */
	AssertState(!IsBootstrapProcessingMode());

	/*
	 * Look up the role, either by name if that's given or by OID if not.
	 * Normally we have to fail if we don't find it, but in parallel workers
	 * just return without doing anything: all the critical work has been done
	 * already.  The upshot of that is that if the role has been deleted, we
	 * will not enforce its rolconnlimit against parallel workers anymore.
	 */
	if (rolename != NULL)
	{
		roleTup = SearchSysCache1(AUTHNAME, PointerGetDatum(rolename));
		if (!HeapTupleIsValid(roleTup))
		{
			if (InitializingParallelWorker)
				return;
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
					 errmsg("role \"%s\" does not exist", rolename)));
		}
	}
	else
	{
		roleTup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
		if (!HeapTupleIsValid(roleTup))
		{
			if (InitializingParallelWorker)
				return;
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
					 errmsg("role with OID %u does not exist", roleid)));
		}
	}

	rform = (Form_pg_authid) GETSTRUCT(roleTup);
	roleid = HeapTupleGetOid(roleTup);
	rname = NameStr(rform->rolname);
	is_superuser = rform->rolsuper;
	strlcpy(AuthenticatedUser.data, rname, NAMEDATALEN);
	
	/* In a parallel worker, ParallelWorkerMain already set these variables */
	if (!InitializingParallelWorker)
	{
		SetAuthenticatedUserId(roleid, is_superuser);

		/*
		 * Set SessionUserId and related variables, including "role", via the
		 * GUC mechanisms.
		 *
		 * Note: ideally we would use PGC_S_DYNAMIC_DEFAULT here, so that
		 * session_authorization could subsequently be changed from
		 * pg_db_role_setting entries.  Instead, session_authorization in
		 * pg_db_role_setting has no effect.  Changing that would require
		 * solving two problems:
		 *
		 * 1. If pg_db_role_setting has values for both session_authorization
		 * and role, we could not be sure which order those would be applied
		 * in, and it would matter.
		 *
		 * 2. Sites may have years-old session_authorization entries.  There's
		 * not been any particular reason to remove them.  Ending the dormancy
		 * of those entries could seriously change application behavior, so
		 * only a major release should do that.
		 */
		SetConfigOption("session_authorization", rname,
						PGC_BACKEND, PGC_S_OVERRIDE);
	}

	/*
	 * These next checks are not enforced when in standalone mode, so that
	 * there is a way to recover from sillinesses like "UPDATE pg_authid SET
	 * rolcanlogin = false;".
	 * only check in InitPostgres, if is reset user, skip this check
	 */
	if (IsUnderPostmaster && !reset)
	{
		/*
		 * Is role allowed to login at all?
		 */
		if (!rform->rolcanlogin)
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
					 errmsg("role \"%s\" is not permitted to log in",
							rname)));

		/*
		 * Check connection limit for this role.
		 *
		 * There is a race condition here --- we create our PGPROC before
		 * checking for other PGPROCs.  If two backends did this at about the
		 * same time, they might both think they were over the limit, while
		 * ideally one should succeed and one fail.  Getting that to work
		 * exactly seems more trouble than it is worth, however; instead we
		 * just document that the connection limit is approximate.
		 */
		if (rform->rolconnlimit >= 0 &&
			!is_superuser &&
			CountUserBackends(roleid) > rform->rolconnlimit)
			ereport(FATAL,
					(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
					 errmsg("too many connections for role \"%s\"",
							rname)));
	}

#ifdef _MLS_
    if(is_mls_or_audit_user())
    {
        SetConfigOption("is_mls_or_audit_user", "on", PGC_INTERNAL, PGC_S_OVERRIDE);
    }
    else
    {
        SetConfigOption("is_mls_or_audit_user", "off", PGC_INTERNAL, PGC_S_OVERRIDE);
    }
#endif

	ReleaseSysCache(roleTup);
}


/*
 * Initialize user identity during special backend startup
 */
void
InitializeSessionUserIdStandalone(void)
{
	/*
	 * This function should only be called in single-user mode, in autovacuum
	 * workers, and in background workers.
	 */
	AssertState(!IsUnderPostmaster || IsAutoVacuumWorkerProcess() ||
				 IsBackgroundWorker || IsClean2pcWorker() || IsForwardSenderProcess());
	/* In pooler stateless reuse mode, to reset session userid */
	if (!PoolerStatelessReuse)
	{
		/* call only once */
		AssertState(!OidIsValid(AuthenticatedUserId));
	}

	AuthenticatedUserId = BOOTSTRAP_SUPERUSERID;
	AuthenticatedUserIsSuperuser = true;

	/*
	 * XXX Ideally we'd do this via SetConfigOption("session_authorization"),
	 * but we lack the role name needed to do that, and we can't fetch it
	 * because one reason for this special case is to be able to start up even
	 * if something's happened to the BOOTSTRAP_SUPERUSERID's pg_authid row.
	 * Since we don't set the GUC itself, C code will see the value as NULL,
	 * and current_setting() will report an empty string within this session.
	 */
	SetSessionAuthorization(BOOTSTRAP_SUPERUSERID, true);

	/* We could do SetConfigOption("role"), but let's be consistent */
	SetCurrentRoleId(InvalidOid, false);
}


/*
 * Change session auth ID while running
 *
 * The SQL standard says that SET SESSION AUTHORIZATION implies SET ROLE NONE.
 * We mechanize that at higher levels not here, because this is the GUC
 * assign hook for "session_authorization", and it must be commutative with
 * SetCurrentRoleId (the hook for "role") because guc.c provides no guarantees
 * which will run first during cases such as transaction rollback.  Therefore,
 * we update derived state (OuterUserId/CurrentUserId/is_superuser) only if
 * !SetRoleIsActive.
 */
void
SetSessionAuthorization(Oid userid, bool is_superuser)
{
	/* Must have authenticated already, else can't make permission check */
	AssertState(OidIsValid(AuthenticatedUserId));

	if (!((IsConnFromCoord() || IsConnFromDatanode()) && PoolerStatelessReuse))
	{
		if (userid != AuthenticatedUserId &&
			!AuthenticatedUserIsSuperuser)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							errmsg("permission denied to set session authorization")));
	}

	SetSessionUserId(userid, is_superuser);

	if (!SetRoleIsActive)
		SetOuterUserId(userid, is_superuser);
}


#ifdef XCP
void
SetGlobalSession(Oid coordid, int coordpid, uint64 timestamp)
{
	bool 			reset = false;
	int				bCount = 0;
	int				bPids[MaxBackends];
	FirstBackendInfo  firstBackendInfo = {InvalidBackendId, InvalidBackendId, 0};

	/* If nothing changed do nothing */
	if ((MyCoordId == coordid && 
		 MyCoordPid == coordpid && 
		 MyCoordTimestamp == timestamp) 
		 || proc_exit_inprogress)
	{
		return;
	}

	/*
	 * Need to reset pool manager agent if the backend being assigned to
	 * different global session or assignment is canceled.
	 */
	if (OidIsValid(MyCoordId) &&
			(MyCoordId != coordid || MyCoordPid != coordpid || MyCoordTimestamp != timestamp))
	 {
		reset = true;
	 }
retry:
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	/* Expose distributed session id in the PGPROC structure */
	MyProc->coordId = coordid;
	MyProc->coordPid = coordpid;
	MyProc->coordTimestamp = timestamp;

		/*
	 * Determine first backend id.
	 * If this backend is the first backend of the distributed session on the
	 * node we should clean up the temporary namespace.
	 * Backend is the first if no backends with such distributed session id.
	 * If such backends are found we can copy first found valid firstBackendId.
	 * If none of them valid that means the first is still cleaning up the
	 * temporary namespace.
	 */
	if (OidIsValid(coordid))
		GetFirstBackendId(&bCount, bPids, &firstBackendInfo);

	/* If first backend id is defined set it right now */
	if (firstBackendInfo.backendId != InvalidBackendId)
	{
		MyProc->firstBackendId = firstBackendInfo.backendId;
		MyProc->firstProcPid = firstBackendInfo.procPid;
		MyProc->firstStartTime = firstBackendInfo.startTime;
	}
	LWLockRelease(ProcArrayLock);

	MyCoordId = coordid;
	MyCoordPid = coordpid;
	MyCoordTimestamp = timestamp;

	if (OidIsValid(coordid) && firstBackendInfo.backendId == InvalidBackendId)
	{
		/*
		 * We are the first or need to retry
		 */
		if (bCount > 0)
		{
			/* XXX sleep ? */
			goto retry;
		}
		else
		{
			/* Set globals for this backend */
			MyFirstBackendId = MyBackendId;
			MyFirstProcPid = MyProcPid;
			MyFirstProcStartTime = MyStartTime;
			
			/* XXX Maybe this lock is not needed because of atomic operation? */
			LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
			MyProc->firstBackendId = MyBackendId;
			MyProc->firstProcPid = MyProcPid;
			MyProc->firstStartTime = MyStartTime;
			LWLockRelease(ProcArrayLock);
		}
	}
	else
	{
		/* Set globals for this backend */
		MyFirstBackendId = firstBackendInfo.backendId;;
		MyFirstProcPid = firstBackendInfo.procPid;;
		MyFirstProcStartTime = firstBackendInfo.startTime;
	}

	/*
	 * Also invalidate any catalog snapshot which may become stale since this
	 * session may join some open transaction which may have done catalog
	 * changes that are not reflected in the current catalog snapshot
	 */
	InvalidateCatalogSnapshot();

	if (reset)
	{
		MyProc->coordId = InvalidOid;
		MyProc->coordPid = 0;
		MyProc->coordTimestamp = 0;

		MyCoordId = InvalidOid;
		MyCoordPid = 0;
		MyCoordTimestamp = 0;

		/*
		 * Next time when backend will be assigned to a global session it will
		 * be referencing different temp namespace and gtt-temp-tablespace.
		 */
		ForgetTempTableNamespace();
		/*
		 * Forget all local and session parameters cached for the Datanodes.
		 * They do not belong to that session.
		 */
		session_param_list_reset(false);
		/*
		 * Release node connections, if still held.
		 * Need clean up remote sessions first.
		 */
		pgxc_node_cleanup_and_release_handles(false, false);
	}
}


/*
 * Returns the name of the role that should be used to access other cluster
 * nodes.
 */
char *
GetClusterUserName(void)
{
	char *cur_username = NameStr(AuthenticatedUser);

	if (cur_username && *cur_username != '\0')
		return cur_username;
	else
		return GetUserNameFromId(AuthenticatedUserId, false);
}

/*
 * Similar to GetClusterUserName, but without searching the table
 */
char *
GetClusterUserNameDirectly(void)
{
	char *cur_username = NameStr(AuthenticatedUser);

	if (cur_username && *cur_username != '\0')
		return cur_username;
	return NULL;
}

char *
GetClusterDBName(void)
{
	char *cur_dbname = NameStr(MyDatabaseName);

	if (cur_dbname && *cur_dbname != '\0')
		return cur_dbname;
	else
		return get_database_name(MyDatabaseId);
}

/* 
 * Readonly process should obtain the latest temp namespace
 */
bool
NeedLatestTempNamespace(void)
{
	if (IS_CENTRALIZED_MODE)
		return false;

	if (!IS_PGXC_DATANODE)
		return false;
	
	if (!OidIsValid(MyCoordId))
		return false;
	return g_twophase_state.is_readonly;
}
#endif


/*
 * Report current role id
 *		This follows the semantics of SET ROLE, ie return the outer-level ID
 *		not the current effective ID, and return InvalidOid when the setting
 *		is logically SET ROLE NONE.
 */
Oid
GetCurrentRoleId(void)
{
	if (SetRoleIsActive)
		return OuterUserId;
	else
		return InvalidOid;
}

/*
 * Change Role ID while running (SET ROLE)
 *
 * If roleid is InvalidOid, we are doing SET ROLE NONE: revert to the
 * session user authorization.  In this case the is_superuser argument
 * is ignored.
 *
 * When roleid is not InvalidOid, the caller must have checked whether
 * the session user has permission to become that role.  (We cannot check
 * here because this routine must be able to execute in a failed transaction
 * to restore a prior value of the ROLE GUC variable.)
 */
void
SetCurrentRoleId(Oid roleid, bool is_superuser)
{
	/*
	 * Get correct info if it's SET ROLE NONE
	 *
	 * If SessionUserId hasn't been set yet, do nothing beyond updating
	 * SetRoleIsActive --- the eventual SetSessionAuthorization call will
	 * update the derived state.  This is needed since we will get called
	 * during GUC initialization.
	 */
	if (!OidIsValid(roleid))
	{
		SetRoleIsActive = false;

		if (!OidIsValid(SessionUserId))
			return;

		roleid = SessionUserId;
		is_superuser = SessionUserIsSuperuser;
	}
	else
		SetRoleIsActive = true;

	SetOuterUserId(roleid, is_superuser);
}


/*
 * Get user name from user oid, returns NULL for nonexistent roleid if noerr
 * is true.
 */
char *
GetUserNameFromId(Oid roleid, bool noerr)
{
	HeapTuple	tuple;
	char	   *result;

	tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
	if (!HeapTupleIsValid(tuple))
	{
		if (!noerr)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("invalid role OID: %u", roleid)));
		result = NULL;
	}
	else
	{
		result = pstrdup(NameStr(((Form_pg_authid) GETSTRUCT(tuple))->rolname));
		ReleaseSysCache(tuple);
	}
	return result;
}


/*-------------------------------------------------------------------------
 *				Interlock-file support
 *
 * These routines are used to create both a data-directory lockfile
 * ($DATADIR/postmaster.pid) and Unix-socket-file lockfiles ($SOCKFILE.lock).
 * Both kinds of files contain the same info initially, although we can add
 * more information to a data-directory lockfile after it's created, using
 * AddToDataDirLockFile().  See miscadmin.h for documentation of the contents
 * of these lockfiles.
 *
 * On successful lockfile creation, a proc_exit callback to remove the
 * lockfile is automatically created.
 *-------------------------------------------------------------------------
 */

/*
 * proc_exit callback to remove lockfiles.
 */
static void
UnlinkLockFiles(int status, Datum arg)
{
	ListCell   *l;

	foreach(l, lock_files)
	{
		char	   *curfile = (char *) lfirst(l);

		unlink(curfile);
		/* Should we complain if the unlink fails? */
	}
	/* Since we're about to exit, no need to reclaim storage */
	lock_files = NIL;

	/*
	 * Lock file removal should always be the last externally visible action
	 * of a postmaster or standalone backend, while we won't come here at all
	 * when exiting postmaster child processes.  Therefore, this is a good
	 * place to log completion of shutdown.  We could alternatively teach
	 * proc_exit() to do it, but that seems uglier.  In a standalone backend,
	 * use NOTICE elevel to be less chatty.
	 */
	ereport(IsPostmasterEnvironment ? LOG : NOTICE,
			(errmsg("database system is shut down")));
}

/*
 * Create a lockfile.
 *
 * filename is the path name of the lockfile to create.
 * amPostmaster is used to determine how to encode the output PID.
 * socketDir is the Unix socket directory path to include (possibly empty).
 * isDDLock and refName are used to determine what error message to produce.
 */
static void
CreateLockFile(const char *filename, bool amPostmaster,
			   const char *socketDir,
			   bool isDDLock, const char *refName)
{
	int			fd;
	char		buffer[MAXPGPATH * 2 + 256];
	int			ntries;
	int			len;
	int			encoded_pid;
	pid_t		other_pid;
	pid_t		my_pid,
				my_p_pid,
				my_gp_pid;
	const char *envvar;

	/*
	 * If the PID in the lockfile is our own PID or our parent's or
	 * grandparent's PID, then the file must be stale (probably left over from
	 * a previous system boot cycle).  We need to check this because of the
	 * likelihood that a reboot will assign exactly the same PID as we had in
	 * the previous reboot, or one that's only one or two counts larger and
	 * hence the lockfile's PID now refers to an ancestor shell process.  We
	 * allow pg_ctl to pass down its parent shell PID (our grandparent PID)
	 * via the environment variable PG_GRANDPARENT_PID; this is so that
	 * launching the postmaster via pg_ctl can be just as reliable as
	 * launching it directly.  There is no provision for detecting
	 * further-removed ancestor processes, but if the init script is written
	 * carefully then all but the immediate parent shell will be root-owned
	 * processes and so the kill test will fail with EPERM.  Note that we
	 * cannot get a false negative this way, because an existing postmaster
	 * would surely never launch a competing postmaster or pg_ctl process
	 * directly.
	 */
	my_pid = getpid();

#ifndef WIN32
	my_p_pid = getppid();
#else

	/*
	 * Windows hasn't got getppid(), but doesn't need it since it's not using
	 * real kill() either...
	 */
	my_p_pid = 0;
#endif

	envvar = getenv("PG_GRANDPARENT_PID");
	if (envvar)
		my_gp_pid = atoi(envvar);
	else
		my_gp_pid = 0;

	/*
	 * We need a loop here because of race conditions.  But don't loop forever
	 * (for example, a non-writable $PGDATA directory might cause a failure
	 * that won't go away).  100 tries seems like plenty.
	 */
	for (ntries = 0;; ntries++)
	{
		/*
		 * Try to create the lock file --- O_EXCL makes this atomic.
		 *
		 * Think not to make the file protection weaker than 0600.  See
		 * comments below.
		 */
		fd = open(filename, O_RDWR | O_CREAT | O_EXCL, 0600);
		if (fd >= 0)
			break;				/* Success; exit the retry loop */

		/*
		 * Couldn't create the pid file. Probably it already exists.
		 */
		if ((errno != EEXIST && errno != EACCES) || ntries > 100)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not create lock file \"%s\": %m",
							filename)));

		/*
		 * Read the file to get the old owner's PID.  Note race condition
		 * here: file might have been deleted since we tried to create it.
		 */
		fd = open(filename, O_RDONLY, 0600);
		if (fd < 0)
		{
			if (errno == ENOENT)
				continue;		/* race condition; try again */
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not open lock file \"%s\": %m",
							filename)));
		}
		pgstat_report_wait_start(WAIT_EVENT_LOCK_FILE_CREATE_READ);
		if ((len = read(fd, buffer, sizeof(buffer) - 1)) < 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not read lock file \"%s\": %m",
							filename)));
		pgstat_report_wait_end();
		close(fd);

		if (len == 0)
		{
			ereport(FATAL,
					(errcode(ERRCODE_LOCK_FILE_EXISTS),
					 errmsg("lock file \"%s\" is empty", filename),
					 errhint("Either another server is starting, or the lock file is the remnant of a previous server startup crash.")));
		}

		buffer[len] = '\0';
		encoded_pid = atoi(buffer);

		/* if pid < 0, the pid is for postgres, not postmaster */
		other_pid = (pid_t) (encoded_pid < 0 ? -encoded_pid : encoded_pid);

		if (other_pid <= 0)
			elog(FATAL, "bogus data in lock file \"%s\": \"%s\"",
				 filename, buffer);

		/*
		 * Check to see if the other process still exists
		 *
		 * Per discussion above, my_pid, my_p_pid, and my_gp_pid can be
		 * ignored as false matches.
		 *
		 * Normally kill() will fail with ESRCH if the given PID doesn't
		 * exist.
		 *
		 * We can treat the EPERM-error case as okay because that error
		 * implies that the existing process has a different userid than we
		 * do, which means it cannot be a competing postmaster.  A postmaster
		 * cannot successfully attach to a data directory owned by a userid
		 * other than its own.  (This is now checked directly in
		 * checkDataDir(), but has been true for a long time because of the
		 * restriction that the data directory isn't group- or
		 * world-accessible.)  Also, since we create the lockfiles mode 600,
		 * we'd have failed above if the lockfile belonged to another userid
		 * --- which means that whatever process kill() is reporting about
		 * isn't the one that made the lockfile.  (NOTE: this last
		 * consideration is the only one that keeps us from blowing away a
		 * Unix socket file belonging to an instance of Postgres being run by
		 * someone else, at least on machines where /tmp hasn't got a
		 * stickybit.)
		 */
		if (other_pid != my_pid && other_pid != my_p_pid &&
			other_pid != my_gp_pid)
		{
			if (kill(other_pid, 0) == 0 ||
				(errno != ESRCH && errno != EPERM))
			{
#ifdef _MLS_
                if (is_postmaster_or_pooler(other_pid))
                {
#endif                    
    				/* lockfile belongs to a live process */
    				ereport(FATAL,
						(errcode(ERRCODE_LOCK_FILE_EXISTS),
						 errmsg("lock file \"%s\" already exists",
								filename),
						 isDDLock ?
						 (encoded_pid < 0 ?
						  errhint("Is another postgres (PID %d) running in data directory \"%s\"?",
								  (int) other_pid, refName) :
						  errhint("Is another postmaster (PID %d) running in data directory \"%s\"?",
								  (int) other_pid, refName)) :
						 (encoded_pid < 0 ?
						  errhint("Is another postgres (PID %d) using socket file \"%s\"?",
								  (int) other_pid, refName) :
						  errhint("Is another postmaster (PID %d) using socket file \"%s\"?",
								  (int) other_pid, refName))));
#ifdef _MLS_
                }
#endif
            }
		}

		/*
		 * No, the creating process did not exist.  However, it could be that
		 * the postmaster crashed (or more likely was kill -9'd by a clueless
		 * admin) but has left orphan backends behind.  Check for this by
		 * looking to see if there is an associated shmem segment that is
		 * still in use.
		 *
		 * Note: because postmaster.pid is written in multiple steps, we might
		 * not find the shmem ID values in it; we can't treat that as an
		 * error.
		 */
		if (isDDLock)
		{
			char	   *ptr = buffer;
			unsigned long id1,
						id2;
			int			lineno;

			for (lineno = 1; lineno < LOCK_FILE_LINE_SHMEM_KEY; lineno++)
			{
				if ((ptr = strchr(ptr, '\n')) == NULL)
					break;
				ptr++;
			}

			if (ptr != NULL &&
				sscanf(ptr, "%lu %lu", &id1, &id2) == 2)
			{
				if (PGSharedMemoryIsInUse(id1, id2))
					ereport(FATAL,
							(errcode(ERRCODE_LOCK_FILE_EXISTS),
							 errmsg("pre-existing shared memory block "
									"(key %lu, ID %lu) is still in use",
									id1, id2),
							 errhint("If you're sure there are no old "
									 "server processes still running, remove "
									 "the shared memory block "
									 "or just delete the file \"%s\".",
									 filename)));
			}
		}

		/*
		 * Looks like nobody's home.  Unlink the file and try again to create
		 * it.  Need a loop because of possible race condition against other
		 * would-be creators.
		 */
		if (unlink(filename) < 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not remove old lock file \"%s\": %m",
							filename),
					 errhint("The file seems accidentally left over, but "
							 "it could not be removed. Please remove the file "
							 "by hand and try again.")));
	}

	/*
	 * Successfully created the file, now fill it.  See comment in miscadmin.h
	 * about the contents.  Note that we write the same first five lines into
	 * both datadir and socket lockfiles; although more stuff may get added to
	 * the datadir lockfile later.
	 */
	snprintf(buffer, sizeof(buffer), "%d\n%s\n%ld\n%d\n%s\n",
			 amPostmaster ? (int) my_pid : -((int) my_pid),
			 DataDir,
			 (long) MyStartTime,
			 PostPortNumber,
			 socketDir);

	/*
	 * In a standalone backend, the next line (LOCK_FILE_LINE_LISTEN_ADDR)
	 * will never receive data, so fill it in as empty now.
	 */
	if (isDDLock && !amPostmaster)
		strlcat(buffer, "\n", sizeof(buffer));

	errno = 0;
	pgstat_report_wait_start(WAIT_EVENT_LOCK_FILE_CREATE_WRITE);
	if (write(fd, buffer, strlen(buffer)) != strlen(buffer))
	{
		int			save_errno = errno;

		close(fd);
		unlink(filename);
		/* if write didn't set errno, assume problem is no disk space */
		errno = save_errno ? save_errno : ENOSPC;
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not write lock file \"%s\": %m", filename)));
	}
	pgstat_report_wait_end();

	pgstat_report_wait_start(WAIT_EVENT_LOCK_FILE_CREATE_SYNC);
	if (pg_fsync(fd) != 0)
	{
		int			save_errno = errno;

		close(fd);
		unlink(filename);
		errno = save_errno;
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not write lock file \"%s\": %m", filename)));
	}
	pgstat_report_wait_end();
	if (close(fd) != 0)
	{
		int			save_errno = errno;

		unlink(filename);
		errno = save_errno;
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not write lock file \"%s\": %m", filename)));
	}

	/*
	 * Arrange to unlink the lock file(s) at proc_exit.  If this is the first
	 * one, set up the on_proc_exit function to do it; then add this lock file
	 * to the list of files to unlink.
	 */
	if (lock_files == NIL)
		on_proc_exit(UnlinkLockFiles, 0);

	/*
	 * Use lcons so that the lock files are unlinked in reverse order of
	 * creation; this is critical!
	 */
	lock_files = lcons(pstrdup(filename), lock_files);
}

void
ForgetLockFiles()
{
	lock_files = NIL;
}

/*
 * Create the data directory lockfile.
 *
 * When this is called, we must have already switched the working
 * directory to DataDir, so we can just use a relative path.  This
 * helps ensure that we are locking the directory we should be.
 *
 * Note that the socket directory path line is initially written as empty.
 * postmaster.c will rewrite it upon creating the first Unix socket.
 */
void
CreateDataDirLockFile(bool amPostmaster)
{
	CreateLockFile(DIRECTORY_LOCK_FILE, amPostmaster, "", true, DataDir);
}

/*
 * Create a lockfile for the specified Unix socket file.
 */
void
CreateSocketLockFile(const char *socketfile, bool amPostmaster,
					 const char *socketDir)
{
	char		lockfile[MAXPGPATH];

	snprintf(lockfile, sizeof(lockfile), "%s.lock", socketfile);
	CreateLockFile(lockfile, amPostmaster, socketDir, false, socketfile);
}

/*
 * TouchSocketLockFiles -- mark socket lock files as recently accessed
 *
 * This routine should be called every so often to ensure that the socket
 * lock files have a recent mod or access date.  That saves them
 * from being removed by overenthusiastic /tmp-directory-cleaner daemons.
 * (Another reason we should never have put the socket file in /tmp...)
 */
void
TouchSocketLockFiles(void)
{
	ListCell   *l;

	foreach(l, lock_files)
	{
		char	   *socketLockFile = (char *) lfirst(l);

		/* No need to touch the data directory lock file, we trust */
		if (strcmp(socketLockFile, DIRECTORY_LOCK_FILE) == 0)
			continue;

		/*
		 * utime() is POSIX standard, utimes() is a common alternative; if we
		 * have neither, fall back to actually reading the file (which only
		 * sets the access time not mod time, but that should be enough in
		 * most cases).  In all paths, we ignore errors.
		 */
#ifdef HAVE_UTIME
		utime(socketLockFile, NULL);
#else							/* !HAVE_UTIME */
#ifdef HAVE_UTIMES
		utimes(socketLockFile, NULL);
#else							/* !HAVE_UTIMES */
		int			fd;
		char		buffer[1];

		fd = open(socketLockFile, O_RDONLY | PG_BINARY, 0);
		if (fd >= 0)
		{
			read(fd, buffer, sizeof(buffer));
			close(fd);
		}
#endif							/* HAVE_UTIMES */
#endif							/* HAVE_UTIME */
	}
}


/*
 * Add (or replace) a line in the data directory lock file.
 * The given string should not include a trailing newline.
 *
 * Note: because we don't truncate the file, if we were to rewrite a line
 * with less data than it had before, there would be garbage after the last
 * line.  While we could fix that by adding a truncate call, that would make
 * the file update non-atomic, which we'd rather avoid.  Therefore, callers
 * should endeavor never to shorten a line once it's been written.
 */
void
AddToDataDirLockFile(int target_line, const char *str)
{
	int			fd;
	int			len;
	int			lineno;
	char	   *srcptr;
	char	   *destptr;
	char		srcbuffer[BLCKSZ];
	char		destbuffer[BLCKSZ];

	fd = open(DIRECTORY_LOCK_FILE, O_RDWR | PG_BINARY, 0);
	if (fd < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						DIRECTORY_LOCK_FILE)));
		return;
	}
	pgstat_report_wait_start(WAIT_EVENT_LOCK_FILE_ADDTODATADIR_READ);
	len = read(fd, srcbuffer, sizeof(srcbuffer) - 1);
	pgstat_report_wait_end();
	if (len < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not read from file \"%s\": %m",
						DIRECTORY_LOCK_FILE)));
		close(fd);
		return;
	}
	srcbuffer[len] = '\0';

	/*
	 * Advance over lines we are not supposed to rewrite, then copy them to
	 * destbuffer.
	 */
	srcptr = srcbuffer;
	for (lineno = 1; lineno < target_line; lineno++)
	{
		char	   *eol = strchr(srcptr, '\n');

		if (eol == NULL)
			break;				/* not enough lines in file yet */
		srcptr = eol + 1;
	}
	memcpy(destbuffer, srcbuffer, srcptr - srcbuffer);
	destptr = destbuffer + (srcptr - srcbuffer);

	/*
	 * Fill in any missing lines before the target line, in case lines are
	 * added to the file out of order.
	 */
	for (; lineno < target_line; lineno++)
	{
		if (destptr < destbuffer + sizeof(destbuffer))
			*destptr++ = '\n';
	}

	/*
	 * Write or rewrite the target line.
	 */
	snprintf(destptr, destbuffer + sizeof(destbuffer) - destptr, "%s\n", str);
	destptr += strlen(destptr);

	/*
	 * If there are more lines in the old file, append them to destbuffer.
	 */
	if ((srcptr = strchr(srcptr, '\n')) != NULL)
	{
		srcptr++;
		snprintf(destptr, destbuffer + sizeof(destbuffer) - destptr, "%s",
				 srcptr);
	}

	/*
	 * And rewrite the data.  Since we write in a single kernel call, this
	 * update should appear atomic to onlookers.
	 */
	len = strlen(destbuffer);
	errno = 0;
	pgstat_report_wait_start(WAIT_EVENT_LOCK_FILE_ADDTODATADIR_WRITE);
	if (lseek(fd, (off_t) 0, SEEK_SET) != 0 ||
		(int) write(fd, destbuffer, len) != len)
	{
		pgstat_report_wait_end();
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m",
						DIRECTORY_LOCK_FILE)));
		close(fd);
		return;
	}
	pgstat_report_wait_end();
	pgstat_report_wait_start(WAIT_EVENT_LOCK_FILE_ADDTODATADIR_SYNC);
	if (pg_fsync(fd) != 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m",
						DIRECTORY_LOCK_FILE)));
	}
	pgstat_report_wait_end();
	if (close(fd) != 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m",
						DIRECTORY_LOCK_FILE)));
	}
}


/*
 * Recheck that the data directory lock file still exists with expected
 * content.  Return TRUE if the lock file appears OK, FALSE if it isn't.
 *
 * We call this periodically in the postmaster.  The idea is that if the
 * lock file has been removed or replaced by another postmaster, we should
 * do a panic database shutdown.  Therefore, we should return TRUE if there
 * is any doubt: we do not want to cause a panic shutdown unnecessarily.
 * Transient failures like EINTR or ENFILE should not cause us to fail.
 * (If there really is something wrong, we'll detect it on a future recheck.)
 */
bool
RecheckDataDirLockFile(void)
{
	int			fd;
	int			len;
	long		file_pid;
	char		buffer[BLCKSZ];

	fd = open(DIRECTORY_LOCK_FILE, O_RDWR | PG_BINARY, 0);
	if (fd < 0)
	{
		/*
		 * There are many foreseeable false-positive error conditions.  For
		 * safety, fail only on enumerated clearly-something-is-wrong
		 * conditions.
		 */
		switch (errno)
		{
			case ENOENT:
			case ENOTDIR:
				/* disaster */
				ereport(LOG,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\": %m",
								DIRECTORY_LOCK_FILE)));
				return false;
			default:
				/* non-fatal, at least for now */
				ereport(LOG,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\": %m; continuing anyway",
								DIRECTORY_LOCK_FILE)));
				return true;
		}
	}
	pgstat_report_wait_start(WAIT_EVENT_LOCK_FILE_RECHECKDATADIR_READ);
	len = read(fd, buffer, sizeof(buffer) - 1);
	pgstat_report_wait_end();
	if (len < 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not read from file \"%s\": %m",
						DIRECTORY_LOCK_FILE)));
		close(fd);
		return true;			/* treat read failure as nonfatal */
	}
	buffer[len] = '\0';
	close(fd);
	file_pid = atol(buffer);
	if (file_pid == getpid())
		return true;			/* all is well */

	/* Trouble: someone's overwritten the lock file */
	ereport(LOG,
			(errmsg("lock file \"%s\" contains wrong PID: %ld instead of %ld",
					DIRECTORY_LOCK_FILE, file_pid, (long) getpid())));
	return false;
}


/*-------------------------------------------------------------------------
 *				Version checking support
 *-------------------------------------------------------------------------
 */

/*
 * Determine whether the PG_VERSION file in directory `path' indicates
 * a data version compatible with the version of this program.
 *
 * If compatible, return. Otherwise, ereport(FATAL).
 */
void
ValidatePgVersion(const char *path)
{
	char		full_path[MAXPGPATH];
	FILE	   *file;
	int			ret;
	long		file_major;
	long		my_major;
	char	   *endptr;
	char		file_version_string[64];
	const char *my_version_string = PG_VERSION;

	my_major = strtol(my_version_string, &endptr, 10);

	snprintf(full_path, sizeof(full_path), "%s/PG_VERSION", path);

	file = AllocateFile(full_path, "r");
	if (!file)
	{
		if (errno == ENOENT)
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("\"%s\" is not a valid data directory",
							path),
					 errdetail("File \"%s\" is missing.", full_path)));
		else
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", full_path)));
	}

	file_version_string[0] = '\0';
	ret = fscanf(file, "%63s", file_version_string);
	file_major = strtol(file_version_string, &endptr, 10);

	if (ret != 1 || endptr == file_version_string)
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"%s\" is not a valid data directory",
						path),
				 errdetail("File \"%s\" does not contain valid data.",
						   full_path),
				 errhint("You might need to initdb.")));

	FreeFile(file);

	if (my_major != file_major)
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("database files are incompatible with server"),
				 errdetail("The data directory was initialized by PostgreSQL version %s, "
						   "which is not compatible with this version %s.",
						   file_version_string, my_version_string)));
}

/*-------------------------------------------------------------------------
 *				Library preload support
 *-------------------------------------------------------------------------
 */

/*
 * GUC variables: lists of library names to be preloaded at postmaster
 * start and at backend start
 */
char	   *session_preload_libraries_string = NULL;
char	   *shared_preload_libraries_string = NULL;
char	   *local_preload_libraries_string = NULL;

/* Flag telling that we are loading shared_preload_libraries */
bool		process_shared_preload_libraries_in_progress = false;

/*
 * load the shared libraries listed in 'libraries'
 *
 * 'gucname': name of GUC variable, for error reports
 * 'restricted': if true, force libraries to be in $libdir/plugins/
 */
static void
load_libraries(const char *libraries, const char *gucname, bool restricted)
{
	char	   *rawstring;
	List	   *elemlist;
	ListCell   *l;

	if (libraries == NULL || libraries[0] == '\0')
		return;					/* nothing to do */

	/* Need a modifiable copy of string */
	rawstring = pstrdup(libraries);

	/* Parse string into list of filename paths */
	if (!SplitDirectoriesString(rawstring, ',', &elemlist))
	{
		/* syntax error in list */
		list_free_deep(elemlist);
		pfree(rawstring);
		ereport(LOG,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid list syntax in parameter \"%s\"",
						gucname)));
		return;
	}

	foreach(l, elemlist)
	{
		/* Note that filename was already canonicalized */
		char	   *filename = (char *) lfirst(l);
		char	   *expanded = NULL;

		/* If restricting, insert $libdir/plugins if not mentioned already */
		if (restricted && first_dir_separator(filename) == NULL)
		{
			expanded = psprintf("$libdir/plugins/%s", filename);
			filename = expanded;
		}
		load_file(filename, restricted);
		ereport(DEBUG1,
				(errmsg("loaded library \"%s\"", filename)));
		if (expanded)
			pfree(expanded);
	}

	list_free_deep(elemlist);
	pfree(rawstring);
}

/*
 * process any libraries that should be preloaded at postmaster start
 */
void
process_shared_preload_libraries(void)
{
	process_shared_preload_libraries_in_progress = true;
	load_libraries(shared_preload_libraries_string,
				   "shared_preload_libraries",
				   false);
	process_shared_preload_libraries_in_progress = false;
}

/*
 * process any libraries that should be preloaded at backend start
 */
void
process_session_preload_libraries(void)
{
	load_libraries(session_preload_libraries_string,
				   "session_preload_libraries",
				   false);
	load_libraries(local_preload_libraries_string,
				   "local_preload_libraries",
				   true);
}

void
pg_bindtextdomain(const char *domain)
{
#ifdef ENABLE_NLS
	if (my_exec_path[0] != '\0')
	{
		char		locale_path[MAXPGPATH];

		get_locale_path(my_exec_path, locale_path);
		bindtextdomain(domain, locale_path);
		pg_bind_textdomain_codeset(domain);
	}
#endif
}

#ifdef _MLS_
static bool is_postmaster_or_pooler(pid_t other_pid)
{
    char		cmd[MAXPGPATH];
    bool        ret = false;
    FILE *      fp;
    
    snprintf(cmd, MAXPGPATH, "ps -ef|grep %d|grep -E \"datanode|coordinator|pooler\" |grep -v grep ", other_pid);

    fp = popen(cmd, "r");
	if (NULL == fp)
	{
		elog(FATAL, "fail to read popen, cmd:%s", cmd);
		return false;
	}	

    while (!feof(fp))
    {
        cmd[0] = '\0';
        fgets(cmd, MAXPGPATH, fp);
        if (strlen(cmd) == 0)
        {
            continue;
        }

        if (strstr(cmd, "datanode") != NULL)
		{
			elog(LOG, "other_pid is datanode, result:%s", cmd);
            ret = true;
            break;
		}
        else if (strstr(cmd, "coordinator") != NULL)
		{
			elog(LOG, "other_pid is coordinator, result:%s", cmd);
            ret = true;
            break;
		}
        else if (strstr(cmd, "pooler") != NULL)
		{
            elog(LOG, "other_pid is pooler, result:%s", cmd);
            ret = true;
            break;
		}
    }

    return ret;
}

#endif
