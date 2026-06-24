
MODULE_big = http
OBJS = http.o
EXTENSION = http

DATA = $(wildcard *.sql)

REGRESS = http
EXTRA_CLEAN =

CURL_CONFIG = curl-config
PG_CONFIG = pg_config

CFLAGS += $(shell $(CURL_CONFIG) --cflags)
LIBS += $(shell $(CURL_CONFIG) --libs)
SHLIB_LINK := $(LIBS)

ifdef DEBUG
COPT			+= -O0 -Werror -g
endif

ifdef USE_PGXS
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pgsql-http
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

