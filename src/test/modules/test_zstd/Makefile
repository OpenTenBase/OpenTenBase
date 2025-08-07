# src/test/modules/test_zstd/Makefile

MODULE_big = test_zstd
OBJS = test_zstd.o $(WIN32RES)
PGFILEDESC = "test_zstd - example of a custom parser for full-text search"

EXTENSION = test_zstd
DATA = test_zstd--1.0.sql

REGRESS = test_zstd

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_zstd
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
