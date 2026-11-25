# contrib/opentenbase_mysql_compat/Makefile

MODULES = opentenbase_mysql_compat

OBJS = src/opentenbase_mysql_compat.o \
       src/mysql_string.o \
       src/mysql_datetime.o \
       src/mysql_math.o \
       src/mysql_flow.o \
       src/mysql_aggregate.o

EXTENSION = opentenbase_mysql_compat
DATA = opentenbase_mysql_compat--1.0.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
