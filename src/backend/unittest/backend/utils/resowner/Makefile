#-------------------------------------------------------------------------
#
# Makefile for backend/unittest
#
# src/backend/unittest/backend/utils/resowner/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/unittest/backend/utils/resowner
top_builddir = ../../../../../../
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/src/backend/common.mk

UNIT_OBJS = test_resowner.o

%.o : %.cpp
	g++  $(filter-out -Wdeclaration-after-statement -Wmissing-prototypes -fexcess-precision=standard, $(CFLAGS)) $(subst -DFRONTEND ,, $(CPPFLAGS)) -c $< -o $@

all: $(UNIT_OBJS) $(SUBDIRS:%=%-recursive)

clean:
	rm -rf $(UNIT_OBJS)
