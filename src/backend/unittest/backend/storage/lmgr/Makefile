#-------------------------------------------------------------------------
#
# Makefile for backend/unittest
#
# src/backend/unittest/backend/storage/lmgr/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/unittest/backend/storage/lmgr/
top_builddir = ../../../../../../
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/src/backend/common.mk

UNIT_OBJS = test_lock.o

%.o : %.cpp
	g++  $(filter-out -Wdeclaration-after-statement -Wmissing-prototypes -fexcess-precision=standard, $(CFLAGS)) $(subst -DFRONTEND ,, $(CPPFLAGS)) -c $< -o $@

all: $(UNIT_OBJS)

clean:
	rm -rf $(UNIT_OBJS)
