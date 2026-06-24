/*-------------------------------------------------------------------------
 *
 * opentenbase_tool.c
 *
 * Copyright (c) 2024-2024, Tencent Holdings Ltd.
 * 
 * src/bin/opentenbase_tool/opentenbase_tool.c
 *
 *-------------------------------------------------------------------------
 */

#include "pg_getopt.h"

#include "opentenbase_tool.h"

static void
usage(const char* progname)
{
	printf(_("%s a basket of tools for emergency recovery and diagnosis.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_(" --checkpoint 	modify checkpoint location\n"));
	printf(_("  -?, --help      show this help, then exit\n"));
	printf(_("\nIf no data directory (DATADIR) is specified, "
			"the environment variable PGDATA\nis used.\n\n"));
}

int
main(int argc, char **argv)
{
	const char *progname;

	progname = get_progname(argv[0]);
	if (argc <= 1)
	{
		usage(progname);
		return -1;
	}

	if (strcmp(argv[1], "--checkpoint") == 0)
		return checkpoint_rewind(--argc, ++argv);
	else
		usage(progname);

	return -1;
}
