#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/gdd
#
# IDENTIFICATION
#    src/backend/utils/gdd/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/utils/gdd
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

OBJS = gdd_backend.o gdd_detector.o gdd_common.o

include $(top_srcdir)/src/backend/common.mk
