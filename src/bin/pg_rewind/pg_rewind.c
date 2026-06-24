/*-------------------------------------------------------------------------
 *
 * pg_rewind.c
 *	  Synchronizes a PostgreSQL data directory to a new timeline
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <unistd.h>

#include "pg_rewind.h"
#include "fetch.h"
#include "file_ops.h"
#include "filemap.h"
#include "logging.h"
#include "disk_verify.h"
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "common/controldata_utils.h"
#include "utils/pidfile.h"
#include "access/timeline.h"
#include "access/xlog_internal.h"
#include "catalog/catversion.h"
#include "catalog/pg_control.h"
#include "common/restricted_token.h"
#include "getopt_long.h"
#include "storage/bufpage.h"

static void usage(const char *progname);

static void createBackupLabel(XLogRecPtr startpoint, TimeLineID starttli,
				  XLogRecPtr checkpointloc);

static void digestControlFile(ControlFileData *ControlFile, char *source,
				  size_t size);
static void updateControlFile(ControlFileData *ControlFile);
static void syncTargetDirectory(const char *argv0);
static void sanityChecks(void);
static TimeLineHistoryEntry *getTimelineHistory(TimeLineID tli, bool is_source,
												int *nentries);
static void findCommonAncestorTimeline(TimeLineHistoryEntry *a_history,
									   int a_nentries,
									   TimeLineHistoryEntry *b_history,
									   int b_nentries,
									   XLogRecPtr *recptr, int *tliIndex);

static ControlFileData ControlFile_target;
static ControlFileData ControlFile_source;

/* PID can be negative for standalone backend */
typedef long pgpid_t;

const char *progname;

/* Configuration options */
char	   *datadir_target = NULL;
char	   *datadir_source = NULL;
char	   *connstr_source = NULL;
char       *pgxcNodeName = NULL;
char       *checksum_verify_file = "checksum_verify_file";
char       *checksum_file_path = NULL;

bool		debug = false;
bool		showprogress = false;
bool		dry_run = false;
bool		checkTimelineStatus = false;

/* Target history */
TimeLineHistoryEntry *targetHistory;
int			targetNentries;
XLogSegNo	copylogSegNo = 0;

static void
usage(const char *progname)
{
	printf(_("%s resynchronizes a PostgreSQL cluster with another copy of the cluster.\n\n"), progname);
	printf(_("Usage:\n  %s [OPTION]...\n\n"), progname);
	printf(_("Options:\n"));
	printf(_("  -D, --target-pgdata=DIRECTORY  existing data directory to modify\n"));
	printf(_("      --source-pgdata=DIRECTORY  source data directory to synchronize with\n"));
	printf(_("      --source-server=CONNSTR    source server to synchronize with\n"));
	printf(_("  -n, --dry-run                  stop before modifying anything\n"));
	printf(_("  -T, --test                     check whether rewind is needed, then exit\n"));
	printf(_("  -P, --progress                 write progress messages\n"));
	printf(_("  -X, --node-name=NODENAME       the server's node name\n"));
	printf(_("      --debug                    write a lot of debug messages\n"));
	printf(_("  -V, --version                  output version information, then exit\n"));
	printf(_("  -e, --verify                   verify checksum of all page, the result will be record in\n"));
	printf(_("                                 the file specified by checksum_verify_file or 'checksum_verify_file'\n"));
	printf(_("                                 without checksum_verify_file argument\n"));
	printf(_("  -r, --repaire                  fix the corrupted pages in the specified checksum_verify_file or\n"));
	printf(_("                                 'checksum_verify_file' without checksum_verify_file argument\n"));
	printf(_("  -j, --jthread=N                threads for checksum verification\n"));
	printf(_("  -f, --checksum_verify_file=file     file to store checksum result information with the format of\n"));
	printf(_("  							        'relative_filepath:offset\n"));
	printf(_("  -c, --checksum_file_path=dir/file   path of files need to be checksum verify, the result will be\n"));
	printf(_("                                      record in 'checksum_verify_file' with the format of \n"));
	printf(_("                                      'filename:offset:size:checksum_enabled:checksum_verified'\n"));
	printf(_("  -?, --help                          show this help, then exit\n"));
	printf(_("\nReport bugs to <pgsql-bugs@postgresql.org>.\n"));
}


int
main(int argc, char **argv)
{
	static struct option long_options[] = {
		{"help", no_argument, NULL, '?'},
		{"target-pgdata", required_argument, NULL, 'D'},
		{"source-pgdata", required_argument, NULL, 1},
		{"source-server", required_argument, NULL, 2},
		{"version", no_argument, NULL, 'V'},
		{"dry-run", no_argument, NULL, 'n'},
		{"progress", no_argument, NULL, 'P'},
		{"verify", no_argument, NULL, 'e'},
		{"repaire", no_argument, NULL, 'r'},
        {"jthread", required_argument, NULL, 'j'},
		{"test", no_argument, NULL, 'T'},
		{"debug", no_argument, NULL, 3},
		{"node-name", required_argument, NULL, 'X'},
		{"checksum_verify_file", required_argument, NULL, 'f'},
		{"checksum_file_path", required_argument, NULL, 'c'},
		{NULL, 0, NULL, 0}
	};
	int			option_index;
	int			c;
	XLogRecPtr	divergerec;
	int			lastcommontliIndex;
	XLogRecPtr	chkptrec;
	TimeLineID	chkpttli;
	XLogRecPtr	chkptredo;
	TimeLineID	source_tli;
	TimeLineID	target_tli;
	XLogRecPtr	target_wal_endrec;
	size_t		size;
	char	   *buffer;
	bool		rewind_needed;
	XLogRecPtr	endrec;
	TimeLineID	endtli;
	ControlFileData ControlFile_new;
    bool        disk_verify_needed = false;
	bool		repaire_corrupted_pages = false;
    int         num_threads = 2;
    char        *pstr;

	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_rewind"));
	progname = get_progname(argv[0]);

	/* Process command-line arguments */
	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage(progname);
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("pg_rewind (PostgreSQL) " PG_VERSION);
			exit(0);
		}
	}

	while ((c = getopt_long(argc, argv, "D:nPTX:erj:f:c:", long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case '?':
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
				exit(1);

			case 'P':
				showprogress = true;
				break;

			case 'n':
				dry_run = true;
				break;

			case 'T':
				checkTimelineStatus = true;
				break;

			case 3:
				debug = true;
				break;

			case 'D':			/* -D or --target-pgdata */
				datadir_target = pg_strdup(optarg);
				break;
			case 'X':
				pgxcNodeName = pg_strdup(optarg);
				break;
			case 1:				/* --source-pgdata */
				datadir_source = pg_strdup(optarg);
				break;
			case 2:				/* --source-server */
				connstr_source = pg_strdup(optarg);
				break;
            case 'e':
                disk_verify_needed = true;
                break;
			case 'r':
				repaire_corrupted_pages = true;
				break;
            case 'j':
                pstr = pg_strdup(optarg);
                if (!pstr || strcmp("\0", pstr) == 0) 
                {
                    fprintf(stderr, _("%s: No number of threads provided, use default=2\n"), progname);
                } 
                else 
                {
                    num_threads = atoi(pstr);
					if (num_threads < 2)
					{
						num_threads = 2;
						fprintf(stderr, _("At least two threads for disk verification, number of threads sets to 2. \n"));
					}
                }
                break;
			case 'f':
				checksum_verify_file = pg_strdup(optarg);
				break;
			case 'c':
				checksum_file_path = pg_strdup(optarg);
				break;
		}
	}

	if (disk_verify_needed && repaire_corrupted_pages)
	{
		fprintf(stderr, _("%s: only one of --verify or --repaire can be specified\n"), progname);
		fprintf(stderr, _("first, verify, then repaire.\n"));
		exit(1);
	}

	if (datadir_source == NULL && connstr_source == NULL)
	{
		fprintf(stderr, _("%s: no source specified (--source-pgdata or --source-server)\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	if (datadir_source != NULL && connstr_source != NULL)
	{
		fprintf(stderr, _("%s: only one of --source-pgdata or --source-server can be specified\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	if (datadir_target == NULL)
	{
		fprintf(stderr, _("%s: no target data directory specified (--target-pgdata)\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	if (checksum_file_path && (!disk_verify_needed || !connstr_source))
	{
		fprintf(stderr, _("%s: verify files must specify verify with -e and must specify server with --source-server \n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	if (disk_verify_needed && !connstr_source)
	{
		fprintf(stderr, _("%s: verify must specify server with --source-server for estore file verify \n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	if (pgxcNodeName == NULL && !checkTimelineStatus)
	{
		fprintf(stderr, _("%s: no node name specified (--node-name)\n"), progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	if (optind < argc)
	{
		fprintf(stderr, _("%s: too many command-line arguments (first is \"%s\")\n"),
				progname, argv[optind]);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	/*
	 * Don't allow pg_rewind to be run as root, to avoid overwriting the
	 * ownership of files in the data directory. We need only check for root
	 * -- any other user won't have sufficient permissions to modify files in
	 * the data directory.
	 */
#ifndef WIN32
	if (geteuid() == 0)
	{
		fprintf(stderr, _("cannot be executed by \"root\"\n"));
		fprintf(stderr, _("You must run %s as the PostgreSQL superuser.\n"),
				progname);
	}
#endif

	get_restricted_token(progname);

	/* Connect to remote server */
	if (connstr_source)
		libpqConnect(connstr_source);

	if (checksum_file_path)
	{
		verifyFiles(datadir_target, checksum_file_path);
		printf(_("Checksum verify files finished\n"));
		exit(0);
	}

	/* 
	 * If disk verification is needed,  verifyDisk scans all data pages to find 
	 * the cracked pages, then mark the pages on checksum_verify_file.
	 */
	if (disk_verify_needed)
	{
		verifyDisk(datadir_target, num_threads, checksum_verify_file);
		printf(_("Checksum verify finished\n"));
		exit(0);
	}

	if (repaire_corrupted_pages)
	{
		copy_crackedpages(checksum_verify_file, datadir_source == NULL);
		printf(_("repaire corrupted pages finished\n"));
		exit(0);
	}

	/*
	 * Ok, we have all the options and we're ready to start. Read in all the
	 * information we need from both clusters.
	 */
	buffer = slurpFile(datadir_target, "global/pg_control", &size);
	digestControlFile(&ControlFile_target, buffer, size);
	pg_free(buffer);

	buffer = fetchFile("global/pg_control", &size);
	digestControlFile(&ControlFile_source, buffer, size);
	pg_free(buffer);

	sanityChecks();

	/*
	 * Usually, the TLI can be found in the latest checkpoint record. But if
	 * the source server is just being promoted (or it's a standby that's
	 * following a primary that's just being promoted), and the checkpoint
	 * requested by the promotion hasn't completed yet, the latest timeline is
	 * in minRecoveryPoint. So we check which is later, the TLI of the
	 * minRecoveryPoint or the latest checkpoint.
	 */
	source_tli = Max(ControlFile_source.minRecoveryPointTLI,
					 ControlFile_source.checkPointCopy.ThisTimeLineID);

	/* Similarly for the target. */
	target_tli = Max(ControlFile_target.minRecoveryPointTLI,
					 ControlFile_target.checkPointCopy.ThisTimeLineID);

	/*
	 * If both clusters are already on the same timeline, there's nothing to
	 * do.
	 */
	if (target_tli == source_tli)
	{
		printf(_("source and target cluster are on the same timeline\n"));
		rewind_needed = false;
		target_wal_endrec = 0;
	}
	else
	{
		XLogRecPtr	chkptendrec;
		TimeLineHistoryEntry *sourceHistory;
		int			sourceNentries;

		/*
		 * Retrieve timelines for both source and target, and find the point
		 * where they diverged.
		 */
		sourceHistory = getTimelineHistory(source_tli, true, &sourceNentries);
		targetHistory = getTimelineHistory(target_tli, false, &targetNentries);

		findCommonAncestorTimeline(sourceHistory, sourceNentries,
								   targetHistory, targetNentries,
								   &divergerec, &lastcommontliIndex);

		printf(_("servers diverged at WAL location %X/%X on timeline %u\n"),
			   (uint32) (divergerec >> 32), (uint32) divergerec,
			   targetHistory[lastcommontliIndex].tli);
		/*
		 * Don't need the source history anymore. The target history is still
		 * needed by the routines in parsexlog.c, when we read the target WAL.
		 */
		pfree(sourceHistory);

		/*
		 * Determine the end-of-WAL on the target.
		 *
		 * The WAL ends at the last shutdown checkpoint, or at
		 * minRecoveryPoint if it was a standby. (If we supported rewinding a
		 * server that was not shut down cleanly, we would need to replay
		 * until we reach the first invalid record, like crash recovery does.)
		 */

		/* read the checkpoint record on the target to see where it ends. */
		chkptendrec = readEndRecord(datadir_target,
		                            ControlFile_target.checkPoint,
		                            targetNentries - 1);

		if (ControlFile_target.minRecoveryPoint > chkptendrec)
		{
			target_wal_endrec = ControlFile_target.minRecoveryPoint;
		}
		else
		{
			target_wal_endrec = chkptendrec;
		}

		/*
		 * Check for the possibility that the target is in fact a direct
		 * ancestor of the source. In that case, there is no divergent history
		 * in the target that needs rewinding.
		 */
		if (target_wal_endrec > divergerec)
		{
			rewind_needed = true;
		}
		else
		{
			/* the last common checkpoint record must be part of target WAL */
			Assert(target_wal_endrec == divergerec);

			rewind_needed = false;
		}
	}

	if (!rewind_needed)
	{
		printf(_("no rewind required\n"));
		exit(0);
	}

	/*
	 * If only checking the timeline status, then return directly from here,
	 * because the following are all handling situations where the timeline forks
     */
	if (checkTimelineStatus)
	{
		printf(_("rewind required\n"));
		exit(1);
	}

	findLastCheckpoint(datadir_target, divergerec,
					   lastcommontliIndex,
					   &chkptrec, &chkpttli, &chkptredo);
	printf(_("rewinding from last common checkpoint at %X/%X on timeline %u\n"),
		   (uint32) (chkptrec >> 32), (uint32) chkptrec,
		   chkpttli);

    /* 
     * Calculate minimal wal copy location.
     * note: local mode is not supported for now.
     */
    if (connstr_source)
    {
        XLogRecPtr minSlot= InvalidXLogRecPtr;
        XLogRecPtr xlogCopyPoint = InvalidXLogRecPtr;

        minSlot = libpqGetMinReplicationSlotLocation();
        if (minSlot != InvalidXLogRecPtr)
            printf(_("minimal replication slot lsn is at %X/%X\n"),
                   (uint32) (minSlot >> 32), (uint32) minSlot);

        if (minSlot != InvalidXLogRecPtr && minSlot < chkptrec)
            xlogCopyPoint = minSlot;
        else
            xlogCopyPoint = chkptrec;

        XLByteToSeg(xlogCopyPoint, copylogSegNo);
        printf(_("skip wal record older than %X/%X\n"),
               (uint32) (xlogCopyPoint >> 32), (uint32) xlogCopyPoint);
    }

	/*
	 * Build the filemap, by comparing the source and target data directories.
	 */
	filemap_create();
	pg_log(PG_PROGRESS, "reading source file list\n");
	fetchSourceFileList();
	pg_log(PG_PROGRESS, "reading target file list\n");
	traverse_datadir(datadir_target, &process_target_file);

	/*
	* Read the target WAL from last checkpoint before the point of fork, to
	* extract all the pages that were modified on the target cluster after
	* the fork.
	*/
	pg_log(PG_PROGRESS, "reading WAL in target\n");
	extractPageMap(datadir_target, chkptrec, lastcommontliIndex,
				   target_wal_endrec);
	filemap_finalize();

	if (showprogress)
		calculate_totals();

	/* this is too verbose even for verbose mode */
	if (debug)
		print_filemap();

	/*
	 * Ok, we're ready to start copying things over.
	 */
	if (showprogress)
	{
		pg_log(PG_PROGRESS, "need to copy %lu MB (total source directory size is %lu MB)\n",
			   (unsigned long) (filemap->fetch_size / (1024 * 1024)),
			   (unsigned long) (filemap->total_size / (1024 * 1024)));

		fetch_size = filemap->fetch_size;
		fetch_done = 0;
	}

	/*
	 * This is the point of no return. Once we start copying things, we have
	 * modified the target directory and there is no turning back!
	 */

	executeFileMap(repaire_corrupted_pages);

	progress_report(true);

	pg_log(PG_PROGRESS, "\ncreating backup label and updating control file\n");
	createBackupLabel(chkptredo, chkpttli, chkptrec);

	/*
	 * Update control file of target. Make it ready to perform archive
	 * recovery when restarting.
	 *
	 * minRecoveryPoint is set to the current WAL insert location in the
	 * source server. Like in an online backup, it's important that we recover
	 * all the WAL that was generated while we copied the files over.
	 */
	memcpy(&ControlFile_new, &ControlFile_source, sizeof(ControlFileData));

	if (connstr_source)
	{
		endrec = libpqGetCurrentXlogInsertLocation();
    	endtli = Max(ControlFile_source.checkPointCopy.ThisTimeLineID,
					 ControlFile_source.minRecoveryPointTLI);
	}
	else
	{
		endrec = ControlFile_source.checkPoint;
		endtli = ControlFile_source.checkPointCopy.ThisTimeLineID;
	}
	ControlFile_new.minRecoveryPoint = endrec;
	ControlFile_new.minRecoveryPointTLI = endtli;
	ControlFile_new.state = DB_IN_ARCHIVE_RECOVERY;
	updateControlFile(&ControlFile_new);

	pg_log(PG_PROGRESS, "syncing target data directory\n");
	syncTargetDirectory(argv[0]);

	printf(_("Done!\n"));

	return 0;
}

/*
 *	Acquire pid from postmaster.pid
 */
static pgpid_t
getpgpid(const char *datadir)
{
	FILE	   *pidf;
	long		pid;
	char pid_file[MAXPGPATH];

	snprintf(pid_file, MAXPGPATH, "%s/postmaster.pid", datadir);

	pidf = fopen(pid_file, "r");
	if (pidf == NULL)
	{
		/* No pid file, not an error on startup */
		if (errno == ENOENT)
			return 0;
		else
		{
			pg_fatal(_("%s: could not open PID file \"%s\": %s\n"),
			             progname, pid_file, strerror(errno));
		}
	}
	if (fscanf(pidf, "%ld", &pid) != 1)
	{
		/* Is the file empty? */
		if (ftell(pidf) == 0 && feof(pidf))
			pg_fatal(_("%s: the PID file \"%s\" is empty\n"),
			             progname, pid_file);
		else
			pg_fatal(_("%s: invalid data in PID file \"%s\"\n"),
			             progname, pid_file);
	}
	fclose(pidf);
	return (pgpid_t) pid;
}

/*
 * Check postmaster with specific pid is alive.
 */
static bool
postmasterIsAlive(pid_t pid)
{
	/*
	 * Test to see if the process is still there.  Note that we do not
	 * consider an EPERM failure to mean that the process is still there;
	 * EPERM must mean that the given PID belongs to some other userid, and
	 * considering the permissions on $PGDATA, that means it's not the
	 * postmaster we are after.
	 *
	 * Don't believe that our own PID or parent shell's PID is the postmaster,
	 * either.  (Windows hasn't got getppid(), though.)
	 */
	if (pid == getpid())
		return false;
#ifndef WIN32
	if (pid == getppid())
		return false;
#endif
	if (kill(pid, 0) == 0)
		return true;
	return false;
}

/*
 *	Check whether the server is running.
 */
static bool
isServerRunning(const char *datadir)
{
	pgpid_t		pid;

	pid = getpgpid(datadir);
	/* Is there a pid file? */
	if (pid != 0)
	{
		/* standalone backend? */
		if (pid < 0)
		{
			pid = -pid;
			if (postmasterIsAlive((pid_t) pid))
			{
				/* single-user server is running */
				return true;
			}
		}
		/* must be a postmaster */
		else if (postmasterIsAlive((pid_t) pid))
		{
			return true;
		}
	}
	return false;
}

/*
 * Check whether backup_label exists.
 *
 * pg_rewind rollback data blocks from since last checkpoint.
 * In backup restore mode, it should start from backuplabel point
 * which we don't support right now.
 */
static void
checkBackupLabel(void)
{
	struct stat statbuf;
	char backup_file[MAXPGPATH];

	snprintf(backup_file, MAXPGPATH, "%s/backup_label", datadir_target);

	if (stat(backup_file, &statbuf) == 0)
	{
		pg_fatal("Could not rewind freshly backup server cluster.");
	}
}

static void
sanityChecks(void)
{
	/* TODO Check that there's no backup_label in either cluster */

	/* Check system_id match */
	if (ControlFile_target.system_identifier != ControlFile_source.system_identifier)
		pg_fatal("source and target clusters are from different systems\n");

	/* check version */
	if (ControlFile_target.pg_control_version != PG_CONTROL_VERSION ||
		ControlFile_source.pg_control_version != PG_CONTROL_VERSION ||
		ControlFile_target.catalog_version_no != CATALOG_VERSION_NO ||
		ControlFile_source.catalog_version_no != CATALOG_VERSION_NO)
	{
		pg_fatal("clusters are not compatible with this version of pg_rewind\n");
	}

	/*
	 * Target cluster need to use checksums or hint bit wal-logging, this to
	 * prevent from data corruption that could occur because of hint bits.
	 */
	if (ControlFile_target.data_checksum_version != PG_DATA_CHECKSUM_VERSION &&
		!ControlFile_target.wal_log_hints)
	{
		pg_fatal("target server needs to use either data checksums or \"wal_log_hints = on\"\n");
	}

	/*
	 * Target cluster better not be running. This doesn't guard against
	 * someone starting the cluster concurrently.
	 * In test mode, we don't check if the server is running.
	 */
	if (!checkTimelineStatus && isServerRunning(datadir_target))
	{
		pg_fatal("target server must be shut down cleanly\n");
	}

	/*
	 * When the source is a data directory, also require that the source
	 * server is shut down. There isn't any very strong reason for this
	 * limitation, but better safe than sorry.
	 */
	if (datadir_source &&
		ControlFile_source.state != DB_SHUTDOWNED &&
		ControlFile_source.state != DB_SHUTDOWNED_IN_RECOVERY)
		pg_fatal("source data directory must be shut down cleanly\n");

	/*
	 * Check backup_label.
	 */
	checkBackupLabel();
}

/*
 * Find minimum from two WAL locations assuming InvalidXLogRecPtr means
 * infinity as src/include/access/timeline.h states. This routine should
 * be used only when comparing WAL locations related to history files.
 */
static XLogRecPtr
MinXLogRecPtr(XLogRecPtr a, XLogRecPtr b)
{
	if (XLogRecPtrIsInvalid(a))
		return b;
	else if (XLogRecPtrIsInvalid(b))
		return a;
	else
		return Min(a, b);
}

/*
 * Retrieve timeline history for the source or target system.
 */
static TimeLineHistoryEntry *
getTimelineHistory(TimeLineID tli, bool is_source, int *nentries)
{
	TimeLineHistoryEntry *history;

	/*
	 * Timeline 1 does not have a history file, so there is no need to check
	 * and fake an entry with infinite start and end positions.
	 */
	if (tli == 1)
	{
		history = (TimeLineHistoryEntry *) pg_malloc(sizeof(TimeLineHistoryEntry));
		history->tli = tli;
		history->begin = history->end = InvalidXLogRecPtr;
		*nentries = 1;
	}
	else
	{
		char		path[MAXPGPATH];
		char	   *histfile;

		TLHistoryFilePath(path, tli);

		/* Get history file from appropriate source */
		if (is_source)
			histfile = fetchFile(path, NULL);
		else
			histfile = slurpFile(datadir_target, path, NULL);

		history = rewind_parseTimeLineHistory(histfile, tli, nentries);
		pg_free(histfile);
	}

	if (debug)
	{
		int			i;

		if (is_source)
			pg_log(PG_DEBUG, "Source timeline history:\n");
		else
			pg_log(PG_DEBUG, "Target timeline history:\n");

		/*
		 * Print the target timeline history.
		 */
		for (i = 0; i < targetNentries; i++)
		{
			TimeLineHistoryEntry *entry;

			entry = &history[i];
			pg_log(PG_DEBUG,
			/* translator: %d is a timeline number, others are LSN positions */
				   "%d: %X/%X - %X/%X\n", entry->tli,
				   (uint32) (entry->begin >> 32), (uint32) (entry->begin),
				   (uint32) (entry->end >> 32), (uint32) (entry->end));
		}
	}

	return history;
}

/*
 * Determine the TLI of the last common timeline in the timeline history of
 * two clusters. *tliIndex is set to the index of last common timeline in
 * the arrays, and *recptr is set to the position where the timeline history
 * diverged (ie. the first WAL record that's not the same in both clusters).
 */
static void
findCommonAncestorTimeline(TimeLineHistoryEntry *a_history, int a_nentries,
						   TimeLineHistoryEntry *b_history, int b_nentries,
						   XLogRecPtr *recptr, int *tliIndex)
{
	int			i,
				n;

	/*
	 * Trace the history forward, until we hit the timeline diverge. It may
	 * still be possible that the source and target nodes used the same
	 * timeline number in their history but with different start position
	 * depending on the history files that each node has fetched in previous
	 * recovery processes. Hence check the start position of the new timeline
	 * as well and move down by one extra timeline entry if they do not match.
	 */
	n = Min(a_nentries, b_nentries);
	for (i = 0; i < n; i++)
	{
		if (a_history[i].tli != b_history[i].tli ||
			a_history[i].begin != b_history[i].begin)
			break;
	}

	if (i > 0)
	{
		i--;
		*recptr = MinXLogRecPtr(a_history[i].end, b_history[i].end);
		*tliIndex = i;
		return;
	}
	else
	{
		pg_fatal("could not find common ancestor of the source and target cluster's timelines\n");
	}
}


/*
 * Create a backup_label file that forces recovery to begin at the last common
 * checkpoint.
 */
static void
createBackupLabel(XLogRecPtr startpoint, TimeLineID starttli, XLogRecPtr checkpointloc)
{
	XLogSegNo	startsegno;
	time_t		stamp_time;
	char		strfbuf[128];
	char		xlogfilename[MAXFNAMELEN];
	struct tm  *tmp;
	char		buf[1000];
	int			len;

	XLByteToSeg(startpoint, startsegno);
	XLogFileName(xlogfilename, starttli, startsegno);

	/*
	 * Construct backup label file
	 */
	stamp_time = time(NULL);
	tmp = localtime(&stamp_time);
	strftime(strfbuf, sizeof(strfbuf), "%Y-%m-%d %H:%M:%S %Z", tmp);

	len = snprintf(buf, sizeof(buf),
				   "START WAL LOCATION: %X/%X (file %s)\n"
				   "CHECKPOINT LOCATION: %X/%X\n"
				   "BACKUP METHOD: pg_rewind\n"
				   "BACKUP FROM: standby\n"
				   "START TIME: %s\n",
	/* omit LABEL: line */
				   (uint32) (startpoint >> 32), (uint32) startpoint, xlogfilename,
				   (uint32) (checkpointloc >> 32), (uint32) checkpointloc,
				   strfbuf);
	if (len >= sizeof(buf))
		pg_fatal("backup label buffer too small\n");	/* shouldn't happen */

	/* TODO: move old file out of the way, if any. */
	open_target_file("backup_label", true); /* BACKUP_LABEL_FILE */
	write_target_range(buf, 0, len);
	close_target_file();
}

/*
 * Check CRC of control file
 */
static void
checkControlFile(ControlFileData *ControlFile)
{
	pg_crc32c	crc;

	/* Calculate CRC */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (char *) ControlFile, offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);

	/* And simply compare it */
	if (!EQ_CRC32C(crc, ControlFile->crc))
		pg_fatal("unexpected control file CRC\n");
}

/*
 * Verify control file contents in the buffer src, and copy it to *ControlFile.
 */
static void
digestControlFile(ControlFileData *ControlFile, char *src, size_t size)
{
	if (size != PG_CONTROL_FILE_SIZE)
		pg_fatal("unexpected control file size %d, expected %d\n",
				 (int) size, PG_CONTROL_FILE_SIZE);

	memcpy(ControlFile, src, sizeof(ControlFileData));

	/* Additional checks on control file */
	checkControlFile(ControlFile);
}

/*
 * Update the target's control file.
 */
static void
updateControlFile(ControlFileData *ControlFile)
{
	char		buffer[PG_CONTROL_FILE_SIZE];

	/*
	 * For good luck, apply the same static assertions as in backend's
	 * WriteControlFile().
	 */
	StaticAssertStmt(sizeof(ControlFileData) <= PG_CONTROL_MAX_SAFE_SIZE,
					 "pg_control is too large for atomic disk writes");
	StaticAssertStmt(sizeof(ControlFileData) <= PG_CONTROL_FILE_SIZE,
					 "sizeof(ControlFileData) exceeds PG_CONTROL_FILE_SIZE");

	/* Recalculate CRC of control file */
	INIT_CRC32C(ControlFile->crc);
	COMP_CRC32C(ControlFile->crc,
				(char *) ControlFile,
				offsetof(ControlFileData, crc));
	FIN_CRC32C(ControlFile->crc);

	/*
	 * Write out PG_CONTROL_FILE_SIZE bytes into pg_control by zero-padding
	 * the excess over sizeof(ControlFileData), to avoid premature EOF related
	 * errors when reading it.
	 */
	memset(buffer, 0, PG_CONTROL_FILE_SIZE);
	memcpy(buffer, ControlFile, sizeof(ControlFileData));

	open_target_file("global/pg_control", false);

	write_target_range(buffer, 0, PG_CONTROL_FILE_SIZE);

	close_target_file();
}

/*
 * Sync target data directory to ensure that modifications are safely on disk.
 *
 * We do this once, for the whole data directory, for performance reasons.  At
 * the end of pg_rewind's run, the kernel is likely to already have flushed
 * most dirty buffers to disk. Additionally initdb -S uses a two-pass approach
 * (only initiating writeback in the first pass), which often reduces the
 * overall amount of IO noticeably.
 */
static void
syncTargetDirectory(const char *argv0)
{
	int			ret;
#define MAXCMDLEN (2 * MAXPGPATH)
	char		exec_path[MAXPGPATH];
	char		cmd[MAXCMDLEN];

	/* locate initdb binary */
	if ((ret = find_other_exec(argv0, "initdb",
							   "initdb (PostgreSQL) " PG_VERSION "\n",
							   exec_path)) < 0)
	{
		char		full_path[MAXPGPATH];

		if (find_my_exec(argv0, full_path) < 0)
			strlcpy(full_path, progname, sizeof(full_path));

		if (ret == -1)
			pg_fatal("The program \"initdb\" is needed by %s but was\n"
					 "not found in the same directory as \"%s\".\n"
					 "Check your installation.\n", progname, full_path);
		else
			pg_fatal("The program \"initdb\" was found by \"%s\"\n"
					 "but was not the same version as %s.\n"
					 "Check your installation.\n", full_path, progname);
	}

	/* only skip processing after ensuring presence of initdb */
	if (dry_run)
		return;

	/* finally run initdb -S */
	if (debug)
		snprintf(cmd, MAXCMDLEN, "\"%s\" -D \"%s\" -S",
				 exec_path, datadir_target);
	else
		snprintf(cmd, MAXCMDLEN, "\"%s\" -D \"%s\" -S > \"%s\"",
				 exec_path, datadir_target, DEVNULL);

	if (system(cmd) != 0)
		pg_fatal("sync of target directory failed\n");
}
