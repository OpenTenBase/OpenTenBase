# contrib/age/Makefile

MODULE_big = age
OBJS = age.o

EXTENSION = age
DATA = age--1.0.0.sql
PGFILEDESC = "age - distributed property graph extension for OpenTenBase"

REGRESS = age_distributed

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/age
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
