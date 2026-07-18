# contrib/opentenbase_mysql_compat/Makefile
#
# Copyright (c) 2026 OpenTenBase Contributors
#
# This file is licensed under the same terms as OpenTenBase. See LICENSE.txt
# in the repository root for details.

EXTENSION = opentenbase_mysql_compat
DATA = opentenbase_mysql_compat--1.0.sql
PGFILEDESC = "opentenbase_mysql_compat - MySQL function compatibility helpers"

REGRESS = opentenbase_mysql_compat

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/opentenbase_mysql_compat
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
