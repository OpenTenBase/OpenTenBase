#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_rewind/unittest
#
# src/bin/pg_rewind/unittest/Makefile
#
#-------------------------------------------------------------------------

subdir = src/bin/pg_rewind/unittest
top_builddir = ../../../..
pg_rewind_dir = ..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/src/backend/common.mk

EXTRA_INCLUDE = -I$(pg_rewind_dir)
override CPPFLAGS := -I$(libpq_srcdir) -DFRONTEND $(CPPFLAGS) $(EXTRA_INCLUDE)
override LDFLAGS := $(libpq_pgport) $(LDFLAGS)


OBJS = disk_verify.o filemap.o datapagemap.o copy_fetch.o logging.o file_ops.o
CPP_OBJS = main.o mt_checksum.o


all:
	+@echo "please use cmd: make uttest."
uttest: pg_rewind_test


%.o :  %.c | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

%.o : %.cpp
	g++  $(filter-out -Wdeclaration-after-statement -Wmissing-prototypes -fexcess-precision=standard, $(CFLAGS)) $(subst -DFRONTEND ,, $(CPPFLAGS)) -c $< -o $@

pg_rewind_test: $(OBJS) $(CPP_OBJS) | submake-libpq submake-libpgport
	g++ $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX) $(export_dynamic) -L$(top_builddir)/src/gtm/libpg  $(filter-out main/main.o,$(call expand_subsys,$^)) $(LIBS) $(ICU_LIBS) -lgtest -lz -o $@


disk_verify.c: % : $(pg_rewind_dir)/%
	rm -f $@ && $(LN_S) $< . 

filemap.c: % : $(pg_rewind_dir)/%
	rm -f $@ && $(LN_S) $< . 

datapagemap.c: % : $(pg_rewind_dir)/%
	rm -f $@ && $(LN_S) $< . 

copy_fetch.c: % : $(pg_rewind_dir)/%
	rm -f $@ && $(LN_S) $< . 

logging.c: % : $(pg_rewind_dir)/%
	rm -f $@ && $(LN_S) $< . 

file_ops.c: % : $(pg_rewind_dir)/%
	rm -f $@ && $(LN_S) $< . 

clean:
	rm -f $(OBJS)
	rm -f $(CPP_OBJS)
	rm -f pg_rewind_test

