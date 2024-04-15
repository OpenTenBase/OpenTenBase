# The name of the plugin
PLUGIN_NAME = slowquery

# The directory where the plugin's source files are located
PLUGIN_SOURCE_DIR = xxx

# PostgreSQL configuration variables
PG_CONFIG = pg_config

config=$(top_srcdir)/contrib/slowquery/postgresql.conf

PG_INCLUDE_PATH = $(shell $(PG_CONFIG) --includedir)
PG_LIB_PATH = $(shell $(PG_CONFIG) --libdir)

PG_VERSION := $(shell $(PG_CONFIG) --version | cut -d ' ' -f 2 | cut -d '.' -f 1,2)

SRCS = slowquery.c
OBJS = $(SRCS:.c=.o)