# opentenbase_mysql_compat Makefile

MODULE_big = opentenbase_mysql_compat
OBJS = opentenbase_mysql_compat.o mysql_string.o mysql_datetime.o mysql_math.o mysql_flow.o mysql_aggregate.o

EXTENSION = opentenbase_mysql_compat
DATA = opentenbase_mysql_compat--1.0.sql
DOCS = README.md README_ZH.md

PG_CONFIG ?= pg_config

REGRESS = opentenbase_mysql_compat
REGRESS_OPTS = --encoding=utf8 --dbname=opentenbase_mysql_compat_regression

ifdef IS_DN_REGRESS
REGRESS_OPTS += --inputdir=$(srcdir)/regress_dn --outputdir=regress_dn
endif

# make "all" the default target
all:

ifndef USE_PGXS
subdir = contrib/$(MODULE_big)
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
else
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
endif
