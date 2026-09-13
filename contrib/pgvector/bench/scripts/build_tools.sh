#!/usr/bin/env bash
# Run this script with Bash.

set -euo pipefail
# -e: exit immediately when a command fails.
# -u: exit immediately when an undefined variable is used.
# pipefail: make a pipeline fail when any command in it fails.

# Resolve the absolute path of this script's directory.
script_dir=$(
	cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
	pwd
)

# Resolve the absolute path of the benchmark root directory.
bench_dir=$(
	cd -- "$script_dir/.."
	pwd
)

make_program=${MAKE:-make}
compiler=${CC:-cc}

# Print command-line usage information.
print_usage()
{
	cat <<EOF
Usage:
  $0 [OPTIONS]

Build benchmark helper programs.

Options:
  --clean
      Remove benchmark build products before rebuilding.

  --compiler COMPILER
      Use COMPILER as the C compiler.
      Example: --compiler clang

  -h, --help
      Show this help message and exit.
EOF
}

# Whether to remove old build products before compiling.
clean_first=0

# Parse command-line arguments.
while (($# > 0)); do
	case "$1" in
		--clean)
			clean_first=1
			shift
			;;

		--compiler)
			if (($# < 2)); then
				printf '%s\n' '--compiler requires a value' >&2
				print_usage >&2
				exit 1
			fi

			compiler=$2
			shift 2
			;;

		-h|--help)
			print_usage
			exit 0
			;;

		*)
			printf 'unknown option: %s\n' "$1" >&2
			print_usage >&2
			exit 1
			;;
	esac
done

# Check whether a command or executable path exists.
command_available()
{
	local command_name=$1

	if [[ "$command_name" == */* ]]; then
		[[ -x "$command_name" ]]
	else
		command -v "$command_name" >/dev/null 2>&1
	fi
}


if ! command_available "$make_program"; then
	printf 'make program not found: %s\n' "$make_program" >&2
	exit 1
fi

if ! command_available "$compiler"; then
	printf 'C compiler not found: %s\n' "$compiler" >&2
	exit 1
fi

if [[ ! -d "$bench_dir" ]]; then
	printf 'benchmark directory not found: %s\n' "$bench_dir" >&2
	exit 1
fi

if [[ ! -f "$bench_dir/Makefile" ]]; then
	printf 'benchmark Makefile not found: %s\n' "$bench_dir/Makefile" >&2
	exit 1
fi

if [[ ! -f "$bench_dir/src/generate_vectors.c" ]]; then
	printf 'vector generator source not found: %s\n' \
		"$bench_dir/src/generate_vectors.c" >&2
	exit 1
fi

# Clean benchmark build products first when --clean is specified.
if ((clean_first)); then
	printf '%s\n' 'Cleaning benchmark build products...'
	"$make_program" -C "$bench_dir" clean
fi

printf 'Benchmark directory: %s\n' "$bench_dir"
printf 'Compiler: %s\n' "$compiler"
printf 'Make program: %s\n' "$make_program"

# Build all currently implemented benchmark helper programs.
"$make_program" -C "$bench_dir" CC="$compiler"

# Verify that the vector generator was successfully generated.
generator_binary="$bench_dir/build/generate_vectors"

if [[ ! -x "$generator_binary" ]]; then
	printf 'expected executable was not generated: %s\n' \
		"$generator_binary" >&2
	exit 1
fi


printf 'Built benchmark tool: %s\n' "$generator_binary"
