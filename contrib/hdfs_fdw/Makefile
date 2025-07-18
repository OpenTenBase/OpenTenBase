MODULE_big = hdfs_fdw
OBJS = hdfs_fdw.o hdfs_connection.o hdfs_client.o hdfs_deparse.o hdfs_query.o hdfs_option.o

EXTENSION = hdfs_fdw
DATA = hdfs_fdw--1.0.sql

# Build libhive first
SUBDIRS = libhive

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/hdfs_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# Link libhive and dependencies  
SHLIB_LINK += -L$(shell pwd)/libhive -lhive
