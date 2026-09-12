# contrib/opentenbase_graph/Makefile

EXTENSION = opentenbase_graph
DATA = opentenbase_graph--1.0.sql opentenbase_graph--1.0--1.1.sql
PGFILEDESC = "opentenbase_graph - lightweight graph traversal templates"

REGRESS = graph
REGRESS_OPTS = --inputdir=test --load-extension=$(EXTENSION)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/opentenbase_graph
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif