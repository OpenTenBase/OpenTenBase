EXTENSION  = pgxc_ora_sysview
EXTVERSION = $(shell grep default_version $(EXTENSION).control | \
		sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

PGFILEDESC = "pgxc_ora_sysview - Propose Oracle SYSTEM VIEW compatibility for OpenTenBase"

PG_CONFIG = pg_config
OTB2 = $(shell $(PG_CONFIG) --version | grep "OpenTenBase"|grep "V2" > /dev/null && echo yes || echo no)

ifeq ($(OTB2),yes)
DOCS = $(wildcard README*)

DATA = $(wildcard updates/*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql
else
$(error Minimum version of OpenTenbase required is 2.0.0)
endif

TESTS        = 00_ora_init 01_ora_objects 02_ora_proc 03_ora_tables 05_ora_clean

REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

