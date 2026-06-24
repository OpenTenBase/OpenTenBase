# contrib/dblink/Makefile

MODULE_big = dblink
OBJS	= dblink.o $(WIN32RES)
PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)

EXTENSION = dblink
DATA = dblink--1.2.sql dblink--1.1--1.2.sql dblink--1.0--1.1.sql \
	dblink--unpackaged--1.0.sql
PGFILEDESC = "dblink - connect to other PostgreSQL databases"

REGRESS = paths dblink paths_ora dblink_ora

ifdef IS_DN_REGRESS
REGRESS_OPTS = --input=regress_dn --output=regress_dn --dlpath=$(top_builddir)/src/test/regress_dn
EXTRA_CLEAN = regress_dn/sql/paths.sql regress_dn/sql/paths_ora.sql regress_dn/expected/paths.out regress_dn/expected/paths_ora.out
else
REGRESS_OPTS = --dlpath=$(top_builddir)/src/test/regress
EXTRA_CLEAN = sql/paths.sql sql/paths_ora.sql expected/paths.out expected/paths_ora.out
endif

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/dblink
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
