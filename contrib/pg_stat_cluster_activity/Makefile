# contrib/pg_stat_cluster_activity/Makefile
#
# Copyright (c) 2023 THL A29 Limited, a Tencent company.
#
# This source code file is licensed under the BSD 3-Clause License,
# you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
#

MODULE_big = pg_stat_cluster_activity
OBJS = pg_stat_cluster_activity.o $(WIN32RES)

EXTENSION = pg_stat_cluster_activity
DATA = pg_stat_cluster_activity--1.0.sql
PGFILEDESC = "pg_stat_cluster_activity - execution of cluster statistics"

LDFLAGS_SL += $(filter -lm, $(LIBS))

REGRESS_OPTS = --temp-config $(top_srcdir)/contrib/pg_stat_cluster_activity/pg_stat_cluster_activity.conf
REGRESS = pg_stat_cluster_activity
# Disabled because these tests require "shared_preload_libraries=pg_stat_cluster_activity",
# which typical installcheck users do not have (e.g. buildfarm clients).
NO_INSTALLCHECK = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_stat_cluster_activity
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
