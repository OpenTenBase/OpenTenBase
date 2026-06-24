# contrib/opentenbase_mls/Makefile

EXTENSION = opentenbase_mls

DATA = opentenbase_mls--1.0.sql opentenbase_mls--unpackaged--1.0.sql
 
PGFILEDESC = "opentenbase_mls - OpenTenBase Multi Level Security"

LDFLAGS_SL += $(filter -lm, $(LIBS))

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/opentenbase_mls
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
