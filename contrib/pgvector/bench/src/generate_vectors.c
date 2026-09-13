/* 
 * Deterministic vector data generator for the pgvector IVFFlat benchmark
 */
#include <errno.h>
#include <inttypes.h>
#include <locale.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


/* IVFFlat supports vector indexes with up to 2000 dimensions. */
#define MAX_IVFFLAT_DIMENSIONS 2000


/* SplitMix64 finalizer */
/*
 * The output of rand() and random() may vary across different environments.
 * A deterministic pseudo-random value is used here to ensure that benchmark
 * remains stable and reproducible.
 */
static uint64_t 
mix_uint64(uint64_t value) 
{
    value += UINT64_C(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)) * UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31);
}

/* Convert a mixed 64-bit value into a float in the range [0, 1). */
static float 
unit_float(uint64_t key) 
{
    uint64_t mixed = mix_uint64(key);
    uint32_t value = (uint32_t) (mixed >> 40);

    return (float) value / 16777216.0f;
}

static float 
signed_unit_float(uint64_t key) 
{
    return unit_float(key) * 2.0f - 1.0f;
}

static uint64_t 
parse_positive_uint64(const char *text, const char *option_name) 
{
    char *end = NULL;
    unsigned long long value;

    if (text == NULL || text[0] == '\0' || text[0] == '-') {
        fprintf(stderr, "%s must be a positive integer\n", option_name);
        exit(EXIT_FAILURE);
    }

    errno = 0;
    value = strtoull(text, &end, 10);
    
    if (errno == ERANGE || end == text || *end != '\0' || value == 0) {
        fprintf(stderr, "invalid value for %s: %s\n", option_name, text);
        exit(EXIT_FAILURE);
    }

    return (uint64_t) value;
}

static void 
print_usage(FILE *stream, const char *program_name) 
{
	fprintf(stream, "Usage:\n"
					"  %s --rows ROWS --dimensions DIMENSIONS \\\n"
					"     --seed SEED --clusters CLUSTERS --output FILE\n"
					"\n"
					"Generate deterministic clustered vectors for the pgvector\n"
					"IVFFlat benchmark.\n"
					"\n"
					"Output format:\n"
					"  id<TAB>[value1,value2,...]\n"
					"\n"
					"Options:\n"
					"  --rows ROWS\n"
					"      Number of vectors to generate. Must be greater than zero.\n"
					"\n"
					"  --dimensions DIMENSIONS\n"
					"      Number of dimensions in each vector. Must be between\n"
					"      1 and %d for an IVFFlat index.\n"
					"\n"
					"  --seed SEED\n"
					"      Positive integer used to generate deterministic values.\n"
					"\n"
					"  --clusters CLUSTERS\n"
					"      Number of synthetic clusters. Must be greater than zero.\n"
					"\n"
					"  --output FILE\n"
					"      Write TSV data to FILE. Use '-' to write to stdout.\n"
					"\n"
					"  -h, --help\n"
					"      Show this help message and exit.\n"
					"\n"
					"Example:\n"
					"  %s --rows 10000 --dimensions 32 \\\n"
					"     --seed 20260901 --clusters 100 \\\n"
					"     --output /tmp/items.tsv\n",
					program_name,
					MAX_IVFFLAT_DIMENSIONS,
					program_name);
}

/*
 * Generate one vector component.
 * 
 * A vector component generated as:
 *     scale * (cluster center + noise)
 * 
 *  - cluster_center is shared by all rows in the same cluster, 
 *    which makes vectors within a cluster similar to each other.
 *  - noise is unique to the row and dimension.
 *  - scale varies between rows, creating different vector magnitudes.
 *    This is important for benchmarking different similarity metrics:
 *      - Cosine ignores vector magnitude.
 *      - Inner Product is affected by vector magnitude.
 */
static float
generate_component(uint64_t seed, uint64_t row_id, uint64_t cluster_id, uint64_t dimension_id)
{
    uint64_t center_key;
    uint64_t noise_key;
    uint64_t scale_key;
    float    center;
    float    noise;
    float    scale;

    center_key = seed ^ (cluster_id * UINT64_C(0x9e3779b97f4a7c15)) ^ (dimension_id * UINT64_C(0xbf58476d1ce4e5b9));
    noise_key = (seed + row_id * UINT64_C(0x94d049bb133111eb)) ^ (dimension_id * UINT64_C(0xd6e8feb86659fd93));
    scale_key = seed ^ row_id ^ UINT64_C(0xa0761d6478bd642f);

    center = signed_unit_float(center_key);
    noise = signed_unit_float(noise_key);

    // Generate a scale in approximately [0.75, 1.25).
    scale = 0.75f + 0.5f * unit_float(scale_key);

    return scale * (center + 0.15f * noise);
}

static void 
generate_vectors(FILE *output, uint64_t rows, int dimensions, uint64_t seed, uint64_t clusters) 
{
    uint64_t row_id;

    for (row_id = 1; row_id <= rows; row_id++)
    {
        uint64_t cluster_id;
        int dimension;
        
        // Use row_id - 1 so the first row belongs to cluster zero.
        cluster_id = (row_id - 1) % clusters;

        if (fprintf(output, "%" PRIu64 "\t[", row_id) < 0)
        {
            fprintf(stderr, "failed to write output\n");
            exit(EXIT_FAILURE);
        }

        for (dimension = 0; dimension < dimensions; dimension++) 
        {
            float component;

            component = generate_component(seed, row_id, cluster_id, (uint64_t) dimension);

            if (dimension > 0) 
            {
                if (fputc(',', output) == EOF)
                {
                    fprintf(stderr, "failed to write output\n");
                    exit(EXIT_FAILURE);
                }
            }

            // Nine significant digits are sufficient to round-trip a 32-bit float through PostgreSQL's text input.
            if (fprintf(output, "%.9g", component) < 0)
            {
                fprintf(stderr, "failed to write output\n");
                exit(EXIT_FAILURE);
            }
        }

        if (fputs("]\n", output) == EOF) 
        {
            fprintf(stderr, "failed to write output\n");
            exit(EXIT_FAILURE);
        }
    }
}

int 
main(int argc, char **argv)
{
    uint64_t    rows = 0;
    uint64_t    seed = 0;
    uint64_t    clusters = 0;
    uint64_t    dimensions_value = 0;
    int         dimensions;
    const char *output_path = NULL;
    FILE       *output = NULL;
    int         argument_index;

    // Force decimal output to use a dot. 
    // some locales may format floating-point numbers with a comma, 
    // which would conflict with pgvector's comma-separated syntax.
	if (setlocale(LC_NUMERIC, "C") == NULL)
    {
        fprintf(stderr, "failed to set numeric locale to C\n");
        return EXIT_FAILURE;
    }

    for (argument_index = 1; argument_index < argc; argument_index++) 
    {
        const char *argument = argv[argument_index];

        if (strcmp(argument, "--rows") == 0) 
        {
            if (++argument_index >= argc)
            {
                fprintf(stderr, "--rows requires a value\n");
                return EXIT_FAILURE;
            }
            rows = parse_positive_uint64(argv[argument_index], "--rows");
        }
        else if (strcmp(argument, "--dimensions") == 0)
        {
            if (++argument_index >= argc)
            {
                fprintf(stderr, "--dimensions requires a value\n");
                return EXIT_FAILURE;
            }

            dimensions_value = parse_positive_uint64(argv[argument_index], "--dimensions");
        }
        else if (strcmp(argument, "--seed") == 0)
        {
            if (++argument_index >= argc)
            {
                fprintf(stderr, "--seed requires a value\n");
                return EXIT_FAILURE;
            }

            seed = parse_positive_uint64(argv[argument_index], "--seed");
        }
        else if (strcmp(argument, "--clusters") == 0)
        {
            if (++argument_index >= argc)
            {
                fprintf(stderr, "--clusters requires a value\n");
                return EXIT_FAILURE;
            }

            clusters = parse_positive_uint64(argv[argument_index], "--clusters");
        }
        else if (strcmp(argument, "--output") == 0)
        {
            if (++argument_index >= argc)
            {
                fprintf(stderr, "--output requires a value\n");
                return EXIT_FAILURE;
            }

			output_path = argv[argument_index];
        }
        else if (strcmp(argument, "--help") == 0 || strcmp(argument, "-h") == 0)
        {
            print_usage(stdout, argv[0]);
            return EXIT_SUCCESS;
        }
        else 
        {
            fprintf(stderr, "unknown option: %s\n", argument);
            print_usage(stderr, argv[0]);
            return EXIT_FAILURE;
        }
    }

    if (rows == 0 || dimensions_value == 0 || seed == 0 || clusters == 0 || output_path == NULL)
    {
        fprintf(stderr, "all required options must be specified\n");
        print_usage(stderr, argv[0]);
        return EXIT_FAILURE;
    }

	if (dimensions_value > MAX_IVFFLAT_DIMENSIONS)
    {
		fprintf(stderr, "--dimensions must not exceed %d for IVFFlat\n",
				MAX_IVFFLAT_DIMENSIONS);
        return EXIT_FAILURE;
    }

    dimensions = (int) dimensions_value;

    // '-' meas standard output.
    if (strcmp(output_path, "-") == 0)
    {
        output = stdout;
    }
    else 
    {
        output = fopen(output_path, "w");
        if (output == NULL)
        {
            fprintf(stderr, "could not open output file %s: %s\n", output_path, strerror(errno));
            return EXIT_FAILURE;
        }
    }

    if (setvbuf(output, NULL, _IOFBF, 1024 * 1024) != 0)
    {
        fprintf(stderr, "failed to configure output buffering\n");

        if (output != stdout) fclose(output);
        return EXIT_FAILURE;
    }

    generate_vectors(output, rows, dimensions, seed, clusters);

    if (fflush(output) != 0)
    {
        fprintf(stderr, "failed to flush output: %s\n", strerror(errno));
        
        if (output != stdout) fclose(output);
        return EXIT_FAILURE;
    }

    if (output != stdout && fclose(output) != 0)
    {
        fprintf(stderr, "failed to close output file %s: %s\n", output_path, strerror(errno));
        return EXIT_FAILURE;
    }

    // Print metadata to stderr rather than stdout.
    // This keeps stdout clean when it is piped directly into PostgreSQL COPY
    fprintf(stderr, "generated rows=%" PRIu64
                    " dimensions=%d seed=%" PRIu64
                    " clusters=%" PRIu64
                    " output=%s\n", rows, dimensions, seed, clusters, output_path);
                    
    return EXIT_SUCCESS;
}
