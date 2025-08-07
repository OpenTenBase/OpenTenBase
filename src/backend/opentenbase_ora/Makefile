#-------------------------------------------------------------------------
#
# Makefile for backend/opentenbase_ora
#
# src/backend/opentenbase_ora/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/opentenbase_ora
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = others.o datefce.o plvstr.o convert.o charpad.o gram_ora.o scan_ora.o parser_ora.o userenv.o colletions.o

include $(top_srcdir)/src/backend/common.mk


gram_ora.h: gram_ora.c;

gram_ora.c: BISONFLAGS += -d
gram_ora.c: BISON_CHECK_CMD = $(PERL) $(top_srcdir)/src/backend/parser/check_keywords.pl $< $(top_srcdir)/src/include/opentenbase_ora/kwlist_ora.h 

scan_ora.c: FLEXFLAGS = -CF -p -p
scan_ora.c: FLEX_NO_BACKUP=yes
scan_ora.c: FLEX_FIX_WARNING=yes


# Force these dependencies to be known even without dependency info built:
gram_ora.o ora_scan.o parser.o: gram_ora.h
