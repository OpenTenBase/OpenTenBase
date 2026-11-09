EXTENSION = pg_graph_search
DATA = pg_graph_search--1.0.0.sql
REGRESS = pg_graph_search
PGFILEDESC = "pg_graph_search - property graph search for OpenTenBase"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_graph_search
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

$(DATA): $(wildcard ddl/*.sql)
	@cat $^ > $@

