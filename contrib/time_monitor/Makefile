MODULE_big	= time_monitor
OBJS		= time_monitor.o $(WIN32RES)

EXTENSION = time_monitor
DATA = time_monitor--1.0.sql
PGFILEDESC = "Monitor the execution time of SQL statements"

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)