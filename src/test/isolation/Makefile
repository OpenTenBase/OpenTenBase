#
# Makefile for isolation tests
#

PGFILEDESC = "pg_isolation_regress/isolationtester - multi-client test driver"
PGAPPICON = win32

subdir = src/test/isolation
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) -I$(libpq_srcdir) \
	-I$(srcdir)/../regress $(CPPFLAGS)

OBJS = \
	$(WIN32RES) \
	isolationtester.o \
	specparse.o \
	specscanner.o

all: isolationtester$(X) pg_isolation_regress$(X)

install: all installdirs
	$(INSTALL_PROGRAM) pg_isolation_regress$(X) '$(DESTDIR)$(pgxsdir)/$(subdir)/pg_isolation_regress$(X)'
	$(INSTALL_PROGRAM) isolationtester$(X) '$(DESTDIR)$(pgxsdir)/$(subdir)/isolationtester$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(pgxsdir)/$(subdir)'

uninstall:
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/pg_isolation_regress$(X)'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/isolationtester$(X)'

submake-regress:
	@if [ "$(IS_CHECK_DN)" = "1" ]; then \
		$(MAKE) -C $(top_builddir)/src/test/regress_dn pg_regress.o; \
	else \
		$(MAKE) -C $(top_builddir)/src/test/regress pg_regress.o; \
	fi

pg_regress.o: | submake-regress
	@if [ "$(IS_CHECK_DN)" = "1" ]; then \
		rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress_dn/pg_regress.o .; \
	else \
		rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/pg_regress.o .; \
	fi

pg_isolation_regress$(X): isolation_main.o pg_regress.o $(WIN32RES)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@

isolationtester$(X): $(OBJS) | submake-libpq submake-libpgport
	$(CC) $(CFLAGS) $^ $(libpq_pgport) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@

distprep: specparse.c specscanner.c

# See notes in src/backend/parser/Makefile about the following two rules
specparse.h: specparse.c
	touch $@

specparse.c: BISONFLAGS += -d

# Force these dependencies to be known even without dependency info built:
specparse.o specscanner.o: specparse.h

# specparse.c and specscanner.c are in the distribution tarball,
# so do not clean them here
clean distclean:
	rm -f isolationtester$(X) pg_isolation_regress$(X) $(OBJS) isolation_main.o
	rm -f pg_regress.o
	@if [ "$(IS_CHECK_DN)" = "1" ]; then \
		cd regress_dn; \
		rm -rf $(pg_regress_clean_files); \
		cd ..; \
	else \
		rm -rf $(pg_regress_clean_files); \
	fi

maintainer-clean: distclean
	rm -f specparse.h specparse.c specscanner.c

installcheck:
	$(MAKE) clean
	$(MAKE) all
	$(pg_isolation_regress_installcheck) --schedule=$(srcdir)/isolation_schedule

installcheck_dn:
	$(MAKE) IS_CHECK_DN=1 clean
	$(MAKE) IS_CHECK_DN=1 all
	$(pg_isolation_regress_dn_installcheck) --schedule=$(srcdir)/regress_dn/isolation_schedule

check: all
	$(MAKE) clean
	$(MAKE) all
	$(pg_isolation_regress_check) --schedule=$(srcdir)/isolation_schedule

check_dn:
	$(MAKE) IS_CHECK_DN=1 clean
	$(MAKE) IS_CHECK_DN=1 all
	$(pg_isolation_regress_dn_check) --schedule=$(srcdir)/regress_dn/isolation_schedule

# Non-default tests.  It only makes sense to run these if set up to use
# prepared transactions, via TEMP_CONFIG for the check case, or via the
# postgresql.conf for the installcheck case.
installcheck-prepared-txns: all temp-install
	$(pg_isolation_regress_installcheck) --schedule=$(srcdir)/isolation_schedule prepared-transactions prepared-transactions-cic

check-prepared-txns: all temp-install
	$(pg_isolation_regress_check) --schedule=$(srcdir)/isolation_schedule prepared-transactions prepared-transactions-cic
