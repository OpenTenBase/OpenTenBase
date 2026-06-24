#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for main
#
# IDENTIFICATION
#    src/backend/main/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/main
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

ifeq ($(enable_tdx), yes)
SUBDIRS = tdx
else
OBJS = main.o
endif

include $(top_srcdir)/src/backend/common.mk
