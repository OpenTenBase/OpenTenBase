# contrib/Eds/Makefile

MODULE_big = eds
OBJS = eds.o
PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)

EXTENSION = eds
DATA = eds--1.0.sql
PGFILEDESC = "eds - A Demo Extension for Postgres,Just for fun!"

#regression test
REGRESS = eds
REGRESS_OPTS = --dlpath=$(top_builddir)/src/test/regress
# EXTRA_CLEAN = sql/paths.sql expected/paths.out

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/eds
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
