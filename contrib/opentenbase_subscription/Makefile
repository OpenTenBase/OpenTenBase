# contrib/opentenbase_subscription/Makefile

MODULE_big = opentenbase_subscription
OBJS = opentenbase_subscription.o

EXTENSION = opentenbase_subscription
DATA = opentenbase_subscription--1.0.sql \
	opentenbase_subscription--unpackaged--1.0.sql

REGRESS = opentenbase_subscription

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
