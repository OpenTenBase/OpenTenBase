# contrib/audit_test/Makefile

PGFILEDESC = "audit_test - examine the file structure"
PGAPPICON = win32

PROGRAM = audit_test
OBJS	= audit_test.o audit_test_AlogQueue.o audit_test_PQexecParams.o $(WIN32RES)

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/audit_test
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
