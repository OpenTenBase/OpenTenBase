# Makefile for pgvector benchmark helper programs.

# -- Compiler configuration.

# C compiler.
CC ?= cc
# Additional preprocessor flags.
# Example: make CPPFLAGS="-DDEBUG_BENCHMARK"
CPPFLAGS ?=
# C compiler flags
CFLAGS ?= -O2 -std=c11 -Wall -Wextra -Werror
# Linker flags.
LDFLAGS ?=
# Libraries passed to the linker.
LDLIBS ?=


# -- Directory and source-file configuration.

BUILD_DIR := build
GENERATOR_SOURCE := src/generate_vectors.c
GENERATOR_BINARY := $(BUILD_DIR)/generate_vectors
# List of currently implemented benchmark tools.
# Add new binary files here when additional tools are implemented.
PROGRAMS := $(GENERATOR_BINARY)


# -- Public Make targets.

.PHONY: all
all: $(PROGRAMS)
.PHONY: generate_vectors
generate_vectors: $(GENERATOR_BINARY)
# Display the available Make targets.
.PHONY: help
help:
	@printf '%s\n' \
			'Available targets:' \
			'  make 				 Build all benchmark helper programs' \
			'  make generate_vectors Build only the vector generator' \
			'  make clean 			 Remove benchmark build products' \
			'  make help 			 Show this help message'
.PHONY: clean
clean:
	rm -rf $(BUILD_DIR)



# -- Private build rules.

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(GENERATOR_BINARY): $(GENERATOR_SOURCE) | $(BUILD_DIR)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(LDFLAGS) $< $(LDLIBS) -o $@
