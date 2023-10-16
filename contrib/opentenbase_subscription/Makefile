# contrib/opentenbase_subscription/Makefile
#
# Copyright (c) 2023 THL A29 Limited, a Tencent company.
#
# This source code file is licensed under the BSD 3-Clause License,
# you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
#

MODULE_big = opentenbase_subscription
OBJS = opentenbase_subscription.o

EXTENSION = opentenbase_subscription
DATA = opentenbase_subscription--1.0.sql \
	opentenbase_subscription--unpackaged--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/opentenbase_subscription
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
