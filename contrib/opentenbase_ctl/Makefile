PGFILEDESC = "opentenbase_ctl - Operation and maintenance tool for opentenbase instance"
PROGRAM = opentenbase_ctl

subdir = contrib/opentenbase_ctl
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk


override CPPFLAGS += -I$(srcdir)/src
override CXXFLAGS += -std=c++17

CTL_LIBS = -lpthread -lssh2 -lssl -lstdc++

OBJS = src/main.o \
       src/ssh/remote_ssh.o \
       src/config/config.o \
       src/log/log.o \
       src/node/node.o \
       src/cluster/cluster.o \
       src/utils/utils.o \
       src/command/command.o \
       src/types/types.o \
       src/file/file.o

PORT_ALLOC_TEST = t/port_allocation_test
PORT_ALLOC_TEST_OBJS = t/port_allocation_test.o t/utils_port_test.o
PORT_PROBE_TEST = t/port_probe_output_test
PORT_PROBE_TEST_OBJS = t/port_probe_output_test.o t/remote_ssh_probe_test.o
TEST_DEPS = $(PORT_ALLOC_TEST_OBJS:.o=.d) $(PORT_PROBE_TEST_OBJS:.o=.d)
TEST_DEPFLAGS = -MMD -MP -MF $(@:.o=.d)
TEST_SECTION_CXXFLAGS = -ffunction-sections -fdata-sections
PORT_ALLOC_TEST_LIBS = -lpthread -lssl -lstdc++

comma := ,
ifeq ($(PORTNAME),darwin)
TEST_GC_LDFLAGS ?= -Wl$(comma)-dead_strip
else
TEST_GC_LDFLAGS ?= -Wl$(comma)--gc-sections
endif

-include $(TEST_DEPS)

all: opentenbase_ctl

opentenbase_ctl:  $(OBJS) 
	$(CXX) $(CXXFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -D_GLIBCXX_USE_CXX11_ABI=1 -o $@$(X) $(CTL_LIBS)
install: all installdirs
	$(INSTALL_PROGRAM) opentenbase_ctl$(X) '$(DESTDIR)$(bindir)'/opentenbase_ctl$(X)

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

check: all $(PORT_ALLOC_TEST) $(PORT_PROBE_TEST)
	./$(PORT_ALLOC_TEST)
	./$(PORT_PROBE_TEST)
	$(SHELL) $(srcdir)/t/cli_failure_test.sh ./opentenbase_ctl
	$(prove_check)

installcheck:
	$(prove_installcheck)

$(PORT_ALLOC_TEST): $(PORT_ALLOC_TEST_OBJS)
	$(MKDIR_P) $(@D)
	$(CXX) $(CXXFLAGS) $(PORT_ALLOC_TEST_OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(TEST_GC_LDFLAGS) $(LIBS) $(PORT_ALLOC_TEST_LIBS) -o $@

t/port_allocation_test.o: $(srcdir)/t/port_allocation_test.cpp
	$(MKDIR_P) $(@D)
	$(COMPILE.cc) $(TEST_DEPFLAGS) $(TEST_SECTION_CXXFLAGS) -o $@ $<

t/utils_port_test.o: $(srcdir)/src/utils/utils.cpp
	$(MKDIR_P) $(@D)
	$(COMPILE.cc) $(TEST_DEPFLAGS) $(TEST_SECTION_CXXFLAGS) -o $@ $<

$(PORT_PROBE_TEST): $(PORT_PROBE_TEST_OBJS)
	$(MKDIR_P) $(@D)
	$(CXX) $(CXXFLAGS) $(PORT_PROBE_TEST_OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(TEST_GC_LDFLAGS) $(LIBS) -o $@

t/port_probe_output_test.o: $(srcdir)/t/port_probe_output_test.cpp
	$(MKDIR_P) $(@D)
	$(COMPILE.cc) $(TEST_DEPFLAGS) $(TEST_SECTION_CXXFLAGS) -o $@ $<

t/remote_ssh_probe_test.o: $(srcdir)/src/ssh/remote_ssh.cpp
	$(MKDIR_P) $(@D)
	$(COMPILE.cc) $(TEST_DEPFLAGS) $(TEST_SECTION_CXXFLAGS) -o $@ $<

uninstall:
	rm -f $(addprefix '$(DESTDIR)$(bindir)'/, opentenbase_ctl$(X))

clean distclean maintainer-clean:
	rm -f opentenbase_ctl$(X)
	rm -f src/*.o
	rm -f src/**/*.o
	rm -f $(PORT_ALLOC_TEST) $(PORT_ALLOC_TEST_OBJS) $(PORT_PROBE_TEST) $(PORT_PROBE_TEST_OBJS) $(TEST_DEPS)
