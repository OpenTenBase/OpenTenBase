
MODULE_big = redis_fdw
OBJS = redis_fdw.o

EXTENSION = redis_fdw
DATA = redis_fdw--1.0.sql

SHLIB_LINK += -lhiredis

PG_CPPFLAGS+= -DWRITE_API -g

ifdef DEBUG
PG_CPPFLAGS+= -DDO_DEBUG
endif

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

