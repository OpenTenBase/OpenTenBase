# src/bin/pg_waldump/Makefile

PGFILEDESC = "pg_waldump - decode and display WAL"
PGAPPICON=win32

subdir = src/test/forward
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = test_fnbuf.o test_stub.o \
	$(FNSTORAGEOBJS) $(FNTHREADOBJS) $(FNQUEUEOBJS) $(FNNODEOBJS) $(FNFLOWCTRLOBJS) \
	s_lock.o
#	$(RMGRDESCOBJS) $(WIN32RES)

#override CPPFLAGS := -DFRONTEND $(CPPFLAGS)
override CFLAGS := $(CFLAGS) -I ./include

FNSTORAGESOURCES = $(sort $(notdir $(wildcard $(top_srcdir)/src/backend/forward/storage/*.c)))
FNSTORAGEOBJS = $(patsubst %.c,%.o,$(FNSTORAGESOURCES))

FNTHREADSOURCES = $(sort $(notdir $(wildcard $(top_srcdir)/src/backend/forward/thread/*.c)))
FNTHREADOBJS = $(patsubst %.c,%.o,$(FNTHREADSOURCES))

FNQUEUESOURCES = $(sort $(notdir $(wildcard $(top_srcdir)/src/backend/forward/queue/*.c)))
FNQUEUEOBJS = $(patsubst %.c,%.o,$(FNQUEUESOURCES))

FNNODESOURCES = $(sort $(notdir $(wildcard $(top_srcdir)/src/backend/forward/node/*.c)))
FNNODEOBJS = $(patsubst %.c,%.o,$(FNNODESOURCES))

FNFLOWCTRLSOURCES = $(sort $(notdir $(wildcard $(top_srcdir)/src/backend/forward/flowctrl/*.c)))
FNFLOWCTRLOBJS = $(patsubst %.c,%.o,$(FNFLOWCTRLSOURCES))


all: test_fn

test_fn: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X) ./lib/libcunit.a

#xlogreader.c: % : $(top_srcdir)/src/backend/access/transam/%
#	rm -f $@ && $(LN_S) $< .

#$(RMGRDESCSOURCES): % : $(top_srcdir)/src/backend/access/rmgrdesc/%
	#rm -f $@ && $(LN_S) $< .

$(FNSTORAGESOURCES): % : $(top_srcdir)/src/backend/forward/storage/%
	rm -f $@ && $(LN_S) $< .

$(FNTHREADSOURCES): % : $(top_srcdir)/src/backend/forward/thread/%
	rm -f $@ && $(LN_S) $< .

$(FNQUEUESOURCES): % : $(top_srcdir)/src/backend/forward/queue/%
	rm -f $@ && $(LN_S) $< .

$(FNNODESOURCES): % : $(top_srcdir)/src/backend/forward/node/%
	rm -f $@ && $(LN_S) $< .

$(FNFLOWCTRLSOURCES): % : $(top_srcdir)/src/backend/forward/flowctrl/%
	rm -f $@ && $(LN_S) $< .

s_lock.c : % : $(top_srcdir)/src/backend/storage/lmgr/%
	rm -f $@ && $(LN_S) $< .


install: all installdirs
	$(INSTALL_PROGRAM) pg_waldump$(X) '$(DESTDIR)$(bindir)/pg_waldump$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_waldump$(X)'

clean distclean maintainer-clean:
	rm -f test_fn$(X) $(OBJS) $(FNTHREADSOURCES) $(FNQUEUESOURCES) $(FNNODESOURCES) $(FNFLOWCTRLSOURCES) $(FNSTORAGESOURCES)
