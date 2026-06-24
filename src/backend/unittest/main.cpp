/*-------------------------------------------------------------------------
 *
 * main.cpp
 *	  Stub main() routine for the postgres_test executable.
 *
 * Unite test main function.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/unittest/main.cpp
 *
 *-------------------------------------------------------------------------
 */
extern "C"
{
#include "postgres.h"
#include "c.h"

#include <unistd.h>
#include <ctype.h>
#include <math.h>
#include <time.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>

#include "bootstrap/bootstrap.h"
#include "common/username.h"
#include "port/atomics.h"
#include "postmaster/postmaster.h"
#include "storage/s_lock.h"
#include "storage/spin.h"
#include "storage/large_object.h"
#include "tcop/tcopprot.h"
#include "utils/help_config.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/ps_status.h"
#include "postgres.h"

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/csnlog.h"
#include "access/lru.h"
#include "access/multixact.h"
#include "access/rewriteheap.h"
#include "access/subtrans.h"
#include "access/timeline.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "catalog/catversion.h"
#include "catalog/pg_control.h"
#include "catalog/pg_database.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/sinvaladt.h"

const char *progname;
const char *exename;
}

#undef Max
#undef StrNCpy

#include "gtest/gtest.h"

/*
 * Init ControlFile Data for uint test
 */
static void
InitUTestControlFile(void)
{
	int			fd;
	char		buffer[PG_CONTROL_FILE_SIZE];	/* need not be aligned */
	ControlFileData	*control_data;

	memset(buffer, 0, PG_CONTROL_FILE_SIZE);

	control_data = (ControlFileData *) buffer;

	/*
	 * Initialize version and compatibility-check fields
	 */
	control_data->pg_control_version = PG_CONTROL_VERSION;
	control_data->catalog_version_no = CATALOG_VERSION_NO;
	control_data->data_checksum_version = 1;

	control_data->maxAlign = MAXIMUM_ALIGNOF;
	control_data->floatFormat = FLOATFORMAT_VALUE;

	control_data->blcksz = BLCKSZ;
	control_data->relseg_size = RELSEG_SIZE;
	control_data->xlog_blcksz = XLOG_BLCKSZ;
	control_data->xlog_seg_size = XLOG_SEG_SIZE;

	control_data->nameDataLen = NAMEDATALEN;
	control_data->indexMaxKeys = INDEX_MAX_KEYS;

	control_data->toast_max_chunk_size = TOAST_MAX_CHUNK_SIZE;
	control_data->loblksize = LOBLKSIZE;

	control_data->float4ByVal = FLOAT4PASSBYVAL;
	control_data->float8ByVal = FLOAT8PASSBYVAL;

	/* Contents are protected with a CRC */
	INIT_CRC32C(control_data->crc);
	COMP_CRC32C(control_data->crc,
	            (char *) control_data,
	            offsetof(ControlFileData, crc));
	FIN_CRC32C(control_data->crc);

	fd = BasicOpenFile((char *) XLOG_CONTROL_FILE,
					   O_RDWR | O_CREAT | PG_BINARY,
					   S_IRUSR | S_IWUSR);
	if (fd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not create control file \"%s\": %m",
						XLOG_CONTROL_FILE)));

	errno = 0;
	if (write(fd, buffer, PG_CONTROL_FILE_SIZE) != PG_CONTROL_FILE_SIZE)
	{
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not write to control file: %m")));
	}

	if (close(fd))
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not close control file: %m")));
}

static void
custom_guc(void)
{
	DataDir = (char *) "/tmp/opentenbase_ut";
}

static void
create_data_dir(void)
{
	struct stat statbuf;
	StringInfoData	sb;

	if (stat(DataDir, &statbuf) == 0)
		return;

	if (errno != ENOENT)
		elog(ERROR, "failed to stat test director");

	if (mkdir(DataDir, S_IRWXU) != 0)
		elog(ERROR, "failed to create test director");

	initStringInfo(&sb);
	appendStringInfo(&sb, "%s/global", DataDir);

	if (mkdir(sb.data, S_IRWXU) != 0)
		elog(ERROR, "failed to create test director");

	resetStringInfo(&sb);
	appendStringInfo(&sb, "%s/pg_notify", DataDir);

	if (mkdir(sb.data, S_IRWXU) != 0)
		elog(ERROR, "failed to create test director");
}

int 
main(int argc, char *argv[])
{
	MemoryContextInit();
	InitializeGUCOptions();

	custom_guc();
	create_data_dir();
	strncpy(my_exec_path, argv[0], MAXPGPATH);
	my_exec_path[MAXPGPATH - 1] = 0;

	ChangeToDataDir(); /* all init done, change current work directory */
	InitUTestControlFile();
	MyProcPid = PostmasterPid = getpid();

	InitializeMaxBackends();
	CreateSharedMemoryAndSemaphores(true, 0);

	/* Backend specification initialize */
	InitStandaloneProcess(argv[0]);
	InitProcess();
	InitProcessPhase2();

	MyBackendId = InvalidBackendId;
	SharedInvalBackendInit(false);

	RelationCacheInitialize();

	testing::InitGoogleTest(&argc,argv);
	return RUN_ALL_TESTS();
}
