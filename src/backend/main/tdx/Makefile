#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for tdx
#
# IDENTIFICATION
#    src/backend/main/tdx/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/main/tdx
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = tdx.o tdx_tool.o fstream.o gfile.o transform.o logical_proto.o threadpool.o
fstream.c: $(top_builddir)/src/backend/utils/misc/fstream/fstream.c
	ln -s $< $@

gfile.c: $(top_builddir)/src/backend/utils/misc/fstream/gfile.c
	ln -s $< $@

gfile.o: CPPFLAGS := $(CPPFLAGS) -DFRONTEND

include $(top_srcdir)/src/backend/common.mk
