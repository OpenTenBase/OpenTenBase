EXTENSION  = pg_ora_sysview
EXTVERSION = $(shell grep default_version $(EXTENSION).control | \
		sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

PGFILEDESC = "pg_ora_sysview - Propose Oracle SYSTEM VIEW compatibility for OpenTenBase"

PG_CONFIG = pg_config
OTB2 = $(shell $(PG_CONFIG) --version | grep "OpenTenBase"|grep "V2" > /dev/null && echo yes || echo no)

ifeq ($(OTB2),yes)
DOCS = $(wildcard README*)

DATA = $(wildcard updates/*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql
else
$(error Minimum version of OpenTenbase required is 2.0.0)
endif

TESTS        = 00_init 01_all_objects 02_dba_objects

REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

