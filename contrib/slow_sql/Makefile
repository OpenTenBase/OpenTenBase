# contrib/slow_sql/Makefile

MODULE_big = slow_sql
OBJS = slow_sql.o $(WIN32RES)

EXTENSION = slow_sql
DATA = slow_sql--1.0.sql
PGFILEDESC = "slow_sql - detects and logs slow queries"

REGRESS = check check_btree

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/slow_sql
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif