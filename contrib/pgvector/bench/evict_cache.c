#define _POSIX_C_SOURCE 200112L

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static off_t
evict_file(const char *path)
{
	struct stat st;
	int			fd;

	fd = open(path, O_RDONLY);
	if (fd < 0)
	{
		if (errno == ENOENT)
			return 0;
		fprintf(stderr, "open %s: %s\n", path, strerror(errno));
		exit(EXIT_FAILURE);
	}
	if (fstat(fd, &st) != 0)
	{
		fprintf(stderr, "stat %s: %s\n", path, strerror(errno));
		close(fd);
		exit(EXIT_FAILURE);
	}
	if (posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED) != 0)
	{
		fprintf(stderr, "posix_fadvise %s failed\n", path);
		close(fd);
		exit(EXIT_FAILURE);
	}
	close(fd);
	return st.st_size;
}

int
main(int argc, char **argv)
{
	off_t		total = 0;

	if (argc < 2)
	{
		fprintf(stderr, "usage: %s RELATION_FILE_PREFIX...\n", argv[0]);
		return EXIT_FAILURE;
	}

	for (int arg = 1; arg < argc; arg++)
	{
		char		path[4096];

		total += evict_file(argv[arg]);
		for (int segment = 1;; segment++)
		{
			off_t		size;

			if (snprintf(path, sizeof(path), "%s.%d", argv[arg], segment) >= (int) sizeof(path))
			{
				fprintf(stderr, "path is too long: %s\n", argv[arg]);
				return EXIT_FAILURE;
			}
			size = evict_file(path);
			if (size == 0)
				break;
			total += size;
		}
	}

	fprintf(stderr, "advised kernel to evict %lld bytes\n", (long long) total);
	return EXIT_SUCCESS;
}
