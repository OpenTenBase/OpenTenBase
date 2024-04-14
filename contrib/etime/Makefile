# contrib/etime/Makefile

MODULE_big = etime
OBJS = etime.o

EXTENSION = etime
DATA = etime--1.0.sql
PGFILEDESC = "etime - an util to record exec time"

REGRESS = etime
REGRESS_OPTS = --inputdir=./ --outputdir=results


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

# params of connection to psql
PGPORT = 30004
PGHOST = localhost
PGDATABASE = testdb
PGUSER = opentenbase

.PHONY: installcheck

installcheck:
# $(pg_regress_installcheck) $(REGRESS_OPTS) $(REGRESS)
	PGUSER=$(PGUSER) PGPORT=$(PGPORT) PGHOST=$(PGHOST) PGDATABASE=$(PGDATABASE) $(pg_regress_installcheck) $(REGRESS_OPTS) $(REGRESS)
# EXTRA_INSTALL += contrib/pg_stat_statements
