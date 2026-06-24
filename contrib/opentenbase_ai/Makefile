# contrib/opentenbase_ai/Makefile
EXTENSION = opentenbase_ai
MODULES = ai
DATA = opentenbase_ai--1.0.sql 
PGFILEDESC = "opentenbase_ai - opentenbase extension for AI"

OBJS = ai.o
REGRESS = opentenbase_ai
EXTRA_INSTALL = contrib/pgsql-http

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/opentenbase_ai
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
