#-------------------------------------------------------------------------
#
# Makefile for src/bin/initdb
#
# Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/bin/initdb/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "initdb - initialize a new database cluster"
PGAPPICON=win32

subdir = src/bin/pg_license
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -DFRONTEND -I$(libpq_srcdir) -I$(top_srcdir)/src/timezone $(CPPFLAGS)

# use system timezone data?
ifneq (,$(with_system_tzdata))
override CPPFLAGS += '-DSYSTEMTZDIR="$(with_system_tzdata)"'
endif

OBJS=	blf.o crypt-blowfish.o crypt-des.o crypt-gensalt.o crypt-md5.o fortuna.o \
     imath.o license.o internal.o internal-sha2.o license_timestamp.o mbuf.o md5.o pg_license.o \
	pgp-armor.o pgp.o pgp-cfb.o pgp-compress.o pgp-decrypt.o pgp-encrypt.o pgp-info.o pgp-mpi.o \
	pgp-mpi-internal.o  pgp-pgsql.o pgp-pubdec.o pgp-pubenc.o pgp-pubkey.o pgp-s2k.o \
	px.o px-crypt.o px-hmac.o random.o rijndael.o stringinfo.o common.o \
	sha1.o sha2.o

STATICOBJS=	blf.o crypt-blowfish.o crypt-des.o crypt-gensalt.o crypt-md5.o fortuna.o \
     imath.o license.o internal.o internal-sha2.o license_timestamp.o mbuf.o md5.o  \
	pgp-armor.o pgp.o pgp-cfb.o pgp-compress.o pgp-decrypt.o pgp-encrypt.o pgp-info.o pgp-mpi.o \
	pgp-mpi-internal.o  pgp-pgsql.o pgp-pubdec.o pgp-pubenc.o pgp-pubkey.o pgp-s2k.o \
	px.o px-crypt.o px-hmac.o random.o rijndael.o stringinfo.o  common.o \
	sha1.o sha2.o

all: pg_license libpglicense.a

pg_license: $(OBJS)
	$(CC) $(CFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

libpglicense.a: $(STATICOBJS)
	$(AR) rcs $@$(X) $(STATICOBJS)

# We used to pull in all of libpq to get encnames.c, but that
# exposes us to risks of version skew if we link to a shared library.
# Do it the hard way, instead, so that we're statically linked.

encnames.c: % : $(top_srcdir)/src/backend/utils/mb/%
	rm -f $@ && $(LN_S) $< .

# Likewise, pull in localtime.c from src/timezones

localtime.c: % : $(top_srcdir)/src/timezone/%
	rm -f $@ && $(LN_S) $< .

install: all installdirs
	$(INSTALL_PROGRAM) pg_license$(X) '$(DESTDIR)$(bindir)/pg_license$(X)'
	$(INSTALL_STLIB) libpglicense.a '$(DESTDIR)$(libdir)/libpglicense.a'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_license$(X)'

clean distclean maintainer-clean:
	rm -f  pg_license$(X) $(OBJS) 


# ensure that changes in datadir propagate into object file
#pg_license.o: pg_license.c $(top_builddir)/src/Makefile.global
