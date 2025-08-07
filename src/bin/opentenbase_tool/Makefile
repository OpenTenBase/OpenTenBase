#-------------------------------------------------------------------------
#
# Makefile for src/bin/opentenbase_tool
#
# Portions Copyright (c) 2024-2024, Tencent Inc.
#
# src/bin/opentenbase_tool/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "opentenbase_tool - a basket of tools for emergency recovery and diagnosis"
PGAPPICON = win32

subdir = src/bin/opentenbase_tool
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) -DFRONTEND $(CPPFLAGS)
override LDFLAGS := $(libpq_pgport) $(LDFLAGS)

OBJS	= opentenbase_tool.o checkpoint_rewind.o xlogreader.o \
	$(WIN32RES)

EXTRA_CLEAN = xlogreader.c

all: opentenbase_tool

opentenbase_tool: $(OBJS) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

xlogreader.c: % : $(top_srcdir)/src/backend/access/transam/%
	rm -f $@ && $(LN_S) $< .

install: all installdirs
	$(INSTALL_PROGRAM) opentenbase_tool$(X) '$(DESTDIR)$(bindir)/opentenbase_tool$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/opentenbase_tool$(X)'

clean distclean maintainer-clean:
	rm -f opentenbase_tool$(X) $(OBJS) xlogreader.c
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)


$(recurse)
$(recurse_always)
