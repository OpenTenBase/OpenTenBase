#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define MAX_DIMENSIONS 2000

static uint64_t
mix64(uint64_t value)
{
	value += UINT64_C(0x9e3779b97f4a7c15);
	value = (value ^ (value >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
	value = (value ^ (value >> 27)) * UINT64_C(0x94d049bb133111eb);
	return value ^ (value >> 31);
}

static float
unit_float(uint64_t value)
{
	return (float) ((mix64(value) >> 40) * (1.0 / 16777216.0));
}

static void
write_u16(uint16_t value)
{
	unsigned char bytes[2] = {
		(unsigned char) (value >> 8),
		(unsigned char) value
	};

	fwrite(bytes, sizeof(bytes), 1, stdout);
}

static void
write_u32(uint32_t value)
{
	unsigned char bytes[4] = {
		(unsigned char) (value >> 24),
		(unsigned char) (value >> 16),
		(unsigned char) (value >> 8),
		(unsigned char) value
	};

	fwrite(bytes, sizeof(bytes), 1, stdout);
}

static void
write_u64(uint64_t value)
{
	write_u32((uint32_t) (value >> 32));
	write_u32((uint32_t) value);
}

static void
write_float(float value)
{
	union
	{
		float		f;
		uint32_t	u;
	} bits;

	bits.f = value;
	write_u32(bits.u);
}

static uint64_t
parse_u64(const char *value, const char *name)
{
	char	   *end;
	uint64_t	parsed;

	errno = 0;
	parsed = strtoull(value, &end, 10);
	if (errno != 0 || *value == '\0' || *end != '\0')
	{
		fprintf(stderr, "invalid %s: %s\n", name, value);
		exit(EXIT_FAILURE);
	}
	return parsed;
}

int
main(int argc, char **argv)
{
	static const unsigned char signature[] = {
		'P', 'G', 'C', 'O', 'P', 'Y', '\n', 0xff, '\r', '\n', 0
	};
	uint64_t	rows;
	uint64_t	seed;
	uint64_t	clusters;
	int			dimensions;

	if (argc != 5)
	{
		fprintf(stderr, "usage: %s ROWS DIMENSIONS SEED CLUSTERS\n", argv[0]);
		return EXIT_FAILURE;
	}

	rows = parse_u64(argv[1], "rows");
	dimensions = (int) parse_u64(argv[2], "dimensions");
	seed = parse_u64(argv[3], "seed");
	clusters = parse_u64(argv[4], "clusters");
	if (rows == 0 || dimensions < 1 || dimensions > MAX_DIMENSIONS || clusters == 0)
	{
		fprintf(stderr, "rows and clusters must be positive; dimensions must be 1..%d\n",
				MAX_DIMENSIONS);
		return EXIT_FAILURE;
	}

	setvbuf(stdout, NULL, _IOFBF, 1024 * 1024);
	fwrite(signature, sizeof(signature), 1, stdout);
	write_u32(0);
	write_u32(0);

	for (uint64_t row = 1; row <= rows; row++)
	{
		uint64_t	cluster = row % clusters;
		float		scale = 0.75f + 0.5f * unit_float(seed ^ row ^ UINT64_C(0xd6e8feb86659fd93));

		write_u16(2);
		write_u32(8);
		write_u64(row);
		write_u32((uint32_t) (sizeof(uint16_t) * 2 + sizeof(float) * dimensions));
		write_u16((uint16_t) dimensions);
		write_u16(0);

		for (int dim = 0; dim < dimensions; dim++)
		{
			uint64_t center_key = seed ^ (cluster * UINT64_C(0x9e3779b97f4a7c15)) ^
				((uint64_t) dim * UINT64_C(0xbf58476d1ce4e5b9));
			uint64_t noise_key = (seed + row * UINT64_C(0x94d049bb133111eb)) ^
				((uint64_t) dim * UINT64_C(0xd6e8feb86659fd93));
			float		center = unit_float(center_key) * 2.0f - 1.0f;
			float		noise = unit_float(noise_key) * 2.0f - 1.0f;

			write_float(scale * (center + 0.15f * noise));
		}
	}

	write_u16(UINT16_MAX);
	if (ferror(stdout) || fflush(stdout) != 0)
	{
		fprintf(stderr, "failed to write binary COPY stream\n");
		return EXIT_FAILURE;
	}

	return EXIT_SUCCESS;
}
