#-------------------------------------------------------------------------
#
# Makefile for backend/unittest
#
# src/backend/unittest/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/unittest
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS = backend
include $(top_srcdir)/src/backend/common.mk

# make LIBS
LIBS := $(filter-out -lpgport -lpgcommon, $(LIBS)) $(LDAP_LIBS_BE)
# The unit testcases like backend doesn't need everything that's in LIBS, however
LIBS := $(filter-out -lz -lreadline -ledit -ltermcap -lncurses -lcurses, $(LIBS))
ifeq ($(with_systemd),yes)
LIBS += -lsystemd
endif

EXTLIBS = -lpgport_srv -lpgcommon_srv \
	$(top_builddir)/src/gtm/client/libgtmclient.a \
	$(top_builddir)/src/gtm/common/libgtm.a \
	$(top_builddir)/src/gtm/libpq/libpqcomm.a
 
UTOBJS = $(shell find $(top_builddir)/src/backend/unittest -name "*.o")

POSTGRESLIB = $(top_srcdir)/src/backend/libpostgres_ut.a
UTSTUBLIBS = $(shell find $(top_builddir)/src/backend/unittest -name "libstub_ut.a")

uttest: main.o $(UTOBJS)
	g++ $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) $(export_dynamic) $(UTOBJS) $(POSTGRESLIB) $(UTSTUBLIBS) $(LIBS) $(ICU_LIBS) $(EXTLIBS) -lgtest -lz -o $@

%.o : %.cpp
	g++  $(filter-out -Wdeclaration-after-statement -Wmissing-prototypes -fexcess-precision=standard, $(CFLAGS)) $(subst -DFRONTEND ,, $(CPPFLAGS)) -c $< -o $@

#all:
#	+@echo "please use cmd: make uttest."
#uttest: $(UNIT_OBJS)
UNIT_OBJS = main.o checksum.o
all: $(UNIT_OBJS)

clean:
	rm -rf $(UNIT_OBJS) uttest
