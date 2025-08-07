# src/test/modules/test_csnlog_compress/Makefile

MODULE_big = test_csnlog_compress
OBJS = test_csnlog_compress.o $(WIN32RES)
PGFILEDESC = "test_csnlog_compress - example of a custom parser for full-text search"

EXTENSION = test_csnlog_compress
DATA = test_csnlog_compress--1.0.sql

REGRESS = test_csnlog_compress

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_csnlog_compress
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
