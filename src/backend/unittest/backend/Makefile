#-------------------------------------------------------------------------
#
# Makefile for backend/unittest
#
# src/backend/unittest/backend/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/unittest/backend
top_builddir = ../../../../
include $(top_builddir)/src/Makefile.global

SUBDIRS = utils access stub storage
include $(top_srcdir)/src/backend/common.mk

clean:
	rm -rf $(UNIT_OBJS)
