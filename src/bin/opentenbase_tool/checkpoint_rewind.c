#include <unistd.h>

#include "postgres_fe.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "opentenbase_tool.h"

typedef struct XLogPageReadPrivate
{
	const char *datadir;
	int			tli;
} XLogPageReadPrivate;

static int	xlogreadfd = -1;
static XLogSegNo xlogreadsegno = -1;
static char xlogfpath[MAXPGPATH];

static int SimpleXLogPageRead(XLogReaderState *xlogreader,
				   XLogRecPtr targetPagePtr,
				   int reqLen, XLogRecPtr targetRecPtr, char *readBuf,
				   TimeLineID *pageTLI);

static void
usage(void)
{
	printf("Modify checkpoint location in pg_control file.\n\n");
	printf("Usage:\n");
	printf("  opentenbase_tool --checkpoint [OPTION]\n");
	printf("\nOptions:\n");
	printf("  -D DATADIR     data directory\n");
	printf("  -c, --checkpoint-location\n");
	printf("  -?, --help show this help, then exit\n");

}

static struct option long_options[] = {
	{"data-dir", required_argument, NULL, 'D'},
	{"checkpoint-location", required_argument, NULL, 'c'},
	{"help", no_argument, NULL, '?'},
	{NULL, 0, NULL, 0}
};

static bool
write_control_data(char *file_path, ControlFileData *data)
{
	FILE *fp;
	bool succeed = false; 

	pg_crc32c	crc;
	/* Check the CRC. */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc,
				(char *) data,
				offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);
	
	data->crc = crc;

	fp = fopen(file_path, "w");
	if (!fp || fwrite(data, 1, sizeof(ControlFileData), fp) != sizeof(ControlFileData))
	{
		fprintf(stderr, "Failed to open or write control data\n");
	}
	else
		succeed = true;
	
	fclose(fp);

	return succeed;
}

static ControlFileData *
read_control_data(char *file_path)
{
	FILE *fp = fopen(file_path, "r");
	ControlFileData *data = NULL;
	pg_crc32c	crc;

	if (!fp)
	{
		fprintf(stderr, "Failed to open %s\n", file_path);
		return NULL;
	}
    
	data = palloc0(sizeof(ControlFileData));

	if (fread(data, 1, sizeof(ControlFileData), fp) != sizeof(ControlFileData))
	{
		fprintf(stderr, "Failed to read ControlFileData\n");
		data = NULL;
	}

	fclose(fp);
	
	/* Check the CRC. */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc,
				(char *) data,
				offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);

	if (!EQ_CRC32C(crc, data->crc))
	{
		fprintf(stderr, "Failed to check crc, expected %X but got %X\n", data->crc, crc);
		pfree(data);
		return NULL;
	}

	/* Make sure the control file is valid byte order. */
	if (data->pg_control_version % 65536 == 0 &&
		data->pg_control_version / 65536 != 0)
		printf("WARNING: possible byte ordering mismatch\n");

	return data;
}

/* XLogreader callback function, to read a WAL page */
static int
SimpleXLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr,
				   int reqLen, XLogRecPtr targetRecPtr, char *readBuf,
				   TimeLineID *pageTLI)
{
	XLogPageReadPrivate *private = (XLogPageReadPrivate *) xlogreader->private_data;
	uint32		targetPageOff;

	targetPageOff = targetPagePtr % XLogSegSize;

	/*
	 * See if we need to switch to a new segment because the requested record
	 * is not in the currently open one.
	 */
	if (xlogreadfd >= 0 && !XLByteInSeg(targetPagePtr, xlogreadsegno))
	{
		close(xlogreadfd);
		xlogreadfd = -1;
	}

	XLByteToSeg(targetPagePtr, xlogreadsegno);

	if (xlogreadfd < 0)
	{
		char		xlogfname[MAXFNAMELEN];

		XLogFileName(xlogfname, private->tli, xlogreadsegno);

		snprintf(xlogfpath, MAXPGPATH, "%s/" XLOGDIR "/%s", private->datadir, xlogfname);

		xlogreadfd = open(xlogfpath, O_RDONLY | PG_BINARY, 0);

		if (xlogreadfd < 0)
		{
			printf(_("could not open file \"%s\": %s\n"), xlogfpath,
				   strerror(errno));
			return -1;
		}
	}

	/*
	 * At this point, we have the right segment open.
	 */
	Assert(xlogreadfd != -1);

	/* Read the requested page */
	if (lseek(xlogreadfd, (off_t) targetPageOff, SEEK_SET) < 0)
	{
		printf(_("could not seek in file \"%s\": %s\n"), xlogfpath,
			   strerror(errno));
		return -1;
	}

	if (read(xlogreadfd, readBuf, XLOG_BLCKSZ) != XLOG_BLCKSZ)
	{
		printf(_("could not read from file \"%s\": %s\n"), xlogfpath,
			   strerror(errno));
		return -1;
	}

	*pageTLI = private->tli;
	return XLOG_BLCKSZ;
}

int 
checkpoint_rewind(int argc, char **argv)
{
	int option;
	int optindex = 0;
	char *file_path = NULL;
	char *new_file_path = NULL;
	const char *DataDir = NULL;
	ControlFileData *data;
	XLogRecPtr checkpoint_loc = InvalidXLogRecPtr;
	XLogSegNo	segno;
	char		xlogfilename[MAXFNAMELEN];
	int 		suffix = 1;
	bool 		succeed = false;

	if (argc <= 1)
	{
		usage();
		return -1;
	}

	while ((option = getopt_long(argc, argv, "D:c:?", long_options, &optindex)) != -1)
	{
		switch (option)
		{
			case 'D':
				DataDir = optarg;
				break;
			case 'c':
				if (sscanf(optarg, "%lX", &checkpoint_loc) != 1)
				{
					fprintf(stderr, "Could not parse checkpoint position\n");
					exit(-1);
				}	
				break;
			case '?':
				usage();
				exit(0);
				break;
			default:
				usage();
				exit(-1);
		}
	}

	file_path = psprintf("%s/global/pg_control", DataDir);
	data = read_control_data(file_path);

	if (!data)
	{
		pfree(file_path);
		return -1;
	}
	
	while (!new_file_path)
	{
		new_file_path = psprintf("%s/global/pg_control.%d", DataDir, suffix++);
		if(access(new_file_path,F_OK) != 0) {
			break;
		}
		pfree(new_file_path);
		new_file_path = NULL;
		if (suffix > 10)
		{
			fprintf(stderr, "Error: Have more than 10 generated pg_control files under global/, " \
				   "remove some before next attempt.\n");
			pfree(file_path);
			return -1;
		}
	}

	if (checkpoint_loc != InvalidXLogRecPtr)
	{
		XLogReaderState *xlogreader;
		XLogPageReadPrivate private;
		XLogRecord *record;
		char	   *errormsg;
		uint8		info;

		private.datadir = DataDir;
		private.tli = data->checkPointCopy.ThisTimeLineID;
		xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, &private);
		/* ensure it's valid checkpoint record */
		record = XLogReadRecord(xlogreader, checkpoint_loc, &errormsg);
		if (record == NULL)
		{
			if (errormsg)
				fprintf(stderr, "could not find checkpoint WAL record at %X/%X: %s\n",
						(uint32) (checkpoint_loc >> 32), (uint32) (checkpoint_loc),
						 errormsg);
			else
				fprintf(stderr, "could not find checkpoint WAL record at %X/%X\n",
						 (uint32) (checkpoint_loc >> 32), (uint32) (checkpoint_loc));
			
			pfree(file_path);
			pfree(new_file_path);
			return -1;
		}

		/*
		 * Check if it is a checkpoint record. 
		 */
		info = XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK;
		if (XLogRecGetRmid(xlogreader) == RM_XLOG_ID &&
			(info == XLOG_CHECKPOINT_SHUTDOWN ||
			 info == XLOG_CHECKPOINT_ONLINE))
		{
			CheckPoint	checkPoint;

			memcpy(&checkPoint, XLogRecGetData(xlogreader), sizeof(CheckPoint));
			data->checkPoint = checkpoint_loc;
			data->checkPointCopy.redo = checkPoint.redo;
			data->checkPointCopy.ThisTimeLineID = checkPoint.ThisTimeLineID;
		}
		else
		{
			fprintf(stderr, "WAL record at %X/%X is not checkpoint record\n",
						(uint32) (checkpoint_loc >> 32), (uint32) (checkpoint_loc));
			pfree(file_path);
			pfree(new_file_path);
			return -1;
		}
		
	}

	/*
	 * Calculate name of the WAL file containing the latest checkpoint's REDO
	 * start point.
	 */
	XLByteToSeg(data->checkPointCopy.redo, segno);
	XLogFileName(xlogfilename, data->checkPointCopy.ThisTimeLineID, segno);

	printf("rewind latest checkpoint record to %lX (redo: %lX) located at pg_wal/%s\n", checkpoint_loc, data->checkPointCopy.redo, xlogfilename);

	if (write_control_data(new_file_path, data))
		succeed = true;

	pfree(file_path);
	pfree(new_file_path);
	return succeed ? 0 : -1;
}
