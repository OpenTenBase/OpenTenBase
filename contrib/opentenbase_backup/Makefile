# contrib/opentenbase_backup/Makefile
#
# Copyright (c) 2026 OpenTenBase Contributors
#
# This file is licensed under the same terms as OpenTenBase. See LICENSE.txt
# in the repository root for details.

subdir = contrib/opentenbase_backup
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

PYTHON ?= python3
SCRIPT = opentenbase_backup.py

all:

install: installdirs
	$(INSTALL_SCRIPT) $(srcdir)/$(SCRIPT) '$(DESTDIR)$(bindir)/opentenbase_backup'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

check:
	$(PYTHON) -m unittest discover -s $(srcdir)/tests -v

installcheck: check

uninstall:
	rm -f '$(DESTDIR)$(bindir)/opentenbase_backup'

clean distclean maintainer-clean:

.PHONY: all install installdirs check installcheck uninstall clean distclean maintainer-clean
