# contrib/check_sql_time/Makefile

MODULE_big = check_sql_time
OBJS = check_sql_time.o $(WIN32RES)

EXTENSION = check_sql_time
DATA = check_sql_time--1.0.sql
PGFILEDESC = "check_sql_time - find out the sql that is exceeding expectations"

REGRESS = check check_btree

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)