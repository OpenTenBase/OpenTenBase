#-------------------------------------------------------------------------
#
# Makefile for backend/unittest
#
# src/backend/unittest/make_stub.mk
#
# STUB_OBJS: all objects in this variable will be packed into a static
#            library.
#-------------------------------------------------------------------------

all: $(STUB_OBJS) libstub_ut.a

libstub_ut.a: $(STUB_OBJS)
	rm -rf libstub_ut.a
	$(AR) $(AROPT) $@ $^
	rm -rf $(STUB_OBJS)
	echo $(subst stub_,,$(STUB_OBJS)) >stub_objs.txt

# use C-compiler to make object files

clean:
	rm -rf $(STUB_OBJS)
	rm -rf libstub_ut.a stub_objs.txt
