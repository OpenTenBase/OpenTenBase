MODULE_big = opentenbase_ora_package_function
OBJS= parse_keyword.o convert.o file.o datefce.o magic.o others.o plvstr.o plvdate.o shmmc.o plvsubst.o utility.o plvlex.o alert.o \
	  pipe.o sqlparse.o putline.o assert.o plunit.o dbms_random.o opentenbase_ora_clob.o aggregate.o opentenbase_ora.o varchar2.o nvarchar2.o charpad.o \
	  charlen.o replace_empty_string.o dbms_lob.o dbms_metadata.o dbms_md_string_utils.o dbms_md_backup_db.o dbms_md_dumputils.o \
	  dbms_md_backup_archiver.o dbms_md_dump.o dbms_md_common.o dbms_md_dump_sort.o dbms_md_dump_to_string.o userenv.o \
	  dbms_session.o regexp_instr.o dbms_obfuscation_toolkit.o dbms_refresh.o value.o xml.o in_range.o

opentenbase_ora.o: CFLAGS += -I$(top_builddir)/src/pl/oraplsql/src

EXTENSION = opentenbase_ora_package_function
EXTRA_INSTALL+=contrib/opentenbase_ora_package_function

DATA = opentenbase_ora_package_function--3.13.sql opentenbase_ora_package_function--3.14.sql opentenbase_ora_package_function--3.15.sql \
	   		opentenbase_ora_package_function--3.16.sql opentenbase_ora_package_function--3.17.sql opentenbase_ora_package_function--3.17--3.18.sql \
			opentenbase_ora_package_function--3.18--3.19.sql opentenbase_ora_package_function--3.19--3.20.sql opentenbase_ora_package_function--3.20--3.21.sql \
			opentenbase_ora_package_function--3.21--3.22.sql opentenbase_ora_package_function--3.22--3.23.sql opentenbase_ora_package_function--3.23--3.24.sql \
			opentenbase_ora_package_function--3.24--3.25.sql opentenbase_ora_package_function--3.25--3.26.sql \
			opentenbase_ora_package_function--3.26--3.27.sql
DOCS = INSTALL.opentenbase_ora_package_function

PG_CONFIG ?= pg_config

# make "all" the default target
all:

EXTRA_CLEAN = sqlparse.c sqlparse.h

#REGRESS_OPTS = --load-language=plpgsql --schedule=parallel_schedule --encoding=utf8
ifdef IS_DN_REGRESS
REGRESS_OPTS = --encoding=utf8 --dbname=opentenbase_ora_package_function_regression --schedule=$(srcdir)/parallel_schedule --inputdir=regress_dn --outputdir=regress_dn
else
REGRESS_OPTS = --encoding=utf8 --dbname=opentenbase_ora_package_function_regression --schedule=$(srcdir)/parallel_schedule
endif

#override CFLAGS += -Wextra -Wimplicit-fallthrough=0

ifndef USE_PGXS
subdir = contrib/$(MODULE_big)
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
else
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
endif

ifeq ($(enable_nls), yes)
SHLIB_LINK += $(filter -lintl,$(LIBS))
endif

# remove dependency to libxml2 and libxslt
LIBS := $(filter-out -lxml2, $(LIBS))
LIBS := $(filter-out -lxslt, $(LIBS))

plvlex.o: sqlparse.o

sqlparse.o: $(srcdir)/sqlscan.c

$(srcdir)/sqlparse.h: $(srcdir)/sqlparse.c ;

$(srcdir)/sqlparse.c: sqlparse.y
ifdef BISON
	$(BISON) -d $(BISONFLAGS) -o $@ $<
else
ifdef YACC
	$(YACC) -d $(YFLAGS) -p cube_yy $<
	mv -f y.tab.c sqlparse.c
	mv -f y.tab.h sqlparse.h
else
	bison -d $(BISONFLAGS) -o $@ $<
endif
endif

$(srcdir)/sqlscan.c: sqlscan.l
ifdef FLEX
	$(FLEX) $(FLEXFLAGS) -o'$@' $<
else
	flex $(FLEXFLAGS) -o'$@' $<
endif

distprep: $(srcdir)/sqlparse.c $(srcdir)/sqlscan.c

check: all
ifdef IS_DN_REGRESS
	$(pg_regress_check_dn) $(REGRESS_OPTS)  $(MAXCONNOPT) $(EXTRA_TESTS)
else
	$(pg_regress_check) $(REGRESS_OPTS) $(MAXCONNOPT) $(EXTRA_TESTS)
endif

installcheck: $(REGRESSION_EXPECTED)
ifdef IS_DN_REGRESS
	$(pg_regress_installcheck_dn) $(REGRESS_OPTS) $(MAXCONNOPT) $(EXTRA_TESTS)
else
	$(pg_regress_installcheck) $(REGRESS_OPTS) $(MAXCONNOPT) $(EXTRA_TESTS)
endif

maintainer-clean:
	rm -f $(srcdir)/sqlparse.c $(srcdir)/sqlscan.c $(srcdir)/sqlparse.h $(srcdir)/y.tab.c $(srcdir)/y.tab.h
