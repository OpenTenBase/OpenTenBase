# contrib/file_fdw/Makefile

MODULES = file_fdw

EXTENSION = file_fdw
DATA = file_fdw--1.0.sql
PGFILEDESC = "file_fdw - foreign data wrapper for files"

REGRESS = file_fdw file_fdw_ora

ifdef IS_DN_REGRESS
REGRESS_OPTS = --input=regress_dn --output=regress_dn
EXTRA_CLEAN = regress_dn/sql/file_fdw.sql regress_dn/expected/file_fdw.out regress_dn/sql/file_fdw_ora.sql regress_dn/expected/file_fdw_ora.out
else
EXTRA_CLEAN = sql/file_fdw.sql expected/file_fdw.out sql/file_fdw_ora.sql expected/file_fdw_ora.out
endif

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/file_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
