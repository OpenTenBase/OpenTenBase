# contrib/etime/Makefile

MODULE_big = etime
OBJS = etime.o

EXTENSION = etime
DATA = etime--1.0.sql
PGFILEDESC = "etime - an util to record exec time"

REGRESS = etime
REGRESS_OPTS = --temp-config=$(top_srcdir)/contrib/etime/etime.conf

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/etime
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

EXTRA_INSTALL += contrib/pg_stat_statements
