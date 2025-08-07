#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/common
#
# IDENTIFICATION
#    src/backend/access/common/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/contrib/pgcrypto
top_builddir = ../../../..

NO_PGXS = 1
with_llvm = 1
include $(top_builddir)/src/Makefile.global

INT_SRCS = md5.c sha1.c sha2.c internal.c internal-sha2.c blf.c rijndael.c \
		pgp-mpi-internal.c imath.c
INT_TESTS = sha2

OSSL_SRCS = openssl.c pgp-mpi-openssl.c
OSSL_TESTS = sha2 des 3des cast5

ZLIB_TST = pgp-compression
ZLIB_OFF_TST = pgp-zlib-DISABLED

ifeq ($(with_openssl),no)
    CF_SRCS = $(INT_SRCS)
	CF_TESTS = $(INT_TESTS)
else ifeq ($(with_3rdpartydir),NONE)
 	CF_SRCS = $(INT_SRCS)
	CF_TESTS = $(INT_TESTS)
else
    CF_SRCS = $(OSSL_SRCS)
	CF_TESTS = $(OSSL_TESTS)
endif
CF_PGP_TESTS = $(if $(subst no,,$(with_zlib)), $(ZLIB_TST), $(ZLIB_OFF_TST))

SRCS		= pgcrypto.c px.c px-hmac.c px-crypt.c \
		crypt-gensalt.c crypt-blowfish.c crypt-des.c \
		crypt-md5.c $(CF_SRCS) \
		mbuf.c pgp.c pgp-armor.c pgp-cfb.c pgp-compress.c \
		pgp-decrypt.c pgp-encrypt.c pgp-info.c pgp-mpi.c \
		pgp-pubdec.c pgp-pubenc.c pgp-pubkey.c pgp-s2k.c \
		pgp-pgsql.c 

MODULE_big	= pgcrypto

OBJS		= $(SRCS:.c=.o)  $(WIN32RES)  

EXTENSION = pgcrypto
DATA = pgcrypto--1.3.sql pgcrypto--1.2--1.3.sql pgcrypto--1.1--1.2.sql \
	pgcrypto--1.0--1.1.sql pgcrypto--unpackaged--1.0.sql
ifeq ($(with_sm),yes)
	DATA += pgcrypto--1.3--1.4.sql
endif
PGFILEDESC = "pgcrypto - cryptographic functions"

REGRESS = init md5 sha1 hmac-md5 hmac-sha1 blowfish rijndael \
	$(CF_TESTS) \
	crypt-des crypt-md5 crypt-blowfish crypt-xdes \
	pgp-armor pgp-decrypt pgp-encrypt $(CF_PGP_TESTS) \
	pgp-pubkey-decrypt pgp-pubkey-encrypt pgp-info
ifeq ($(with_sm),yes)
	REGRESS += sm
endif
EXTRA_CLEAN = gen-rtab

ifdef USE_PGXS
	PG_CONFIG = pg_config
	PGXS := $(shell $(PG_CONFIG) --pgxs)
	include $(PGXS)
else
	subdir = src/backend/contrib/pgcrypto
	top_builddir = ../../../..
	include $(top_srcdir)/src/makefiles/pgxs.mk
endif
# Add libraries that pgcrypto depends (or might depend) on into the
# shared library link.  (The order in which you list them here doesn't
# matter.)

ifeq ($(PORTNAME), win32)
SHLIB_LINK += $(filter -leay32, $(LIBS))
# those must be at the end
SHLIB_LINK += -lws2_32
endif

SHLIB_LINK += $(filter -lcrypto -lz, $(LIBS))
rijndael.o: rijndael.tbl

rijndael.tbl:
	$(CC) $(CPPFLAGS) $(CFLAGS) -DPRINT_TABS rijndael.c -o gen-rtab
	./gen-rtab > rijndael.tbl

include $(top_srcdir)/src/backend/common.mk
