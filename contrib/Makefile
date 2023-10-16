# contrib/Makefile
#
# This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
# All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
#

subdir = contrib
top_builddir = ..
include $(top_builddir)/src/Makefile.global

SUBDIRS = \
                adminpack       \
                amcheck         \
                auth_delay      \
                auto_explain    \
                btree_gin       \
                btree_gist      \
                chkpass         \
                citext          \
                cube            \
                dblink          \
                dict_int        \
                dict_xsyn       \
                earthdistance   \
                file_fdw        \
                fuzzystrmatch   \
                hstore          \
                intagg          \
                intarray        \
                isn             \
                lo              \
                ltree           \
                oid2name        \
                pageinspect     \
                passwordcheck   \
                pg_buffercache  \
                pg_freespacemap \
                pg_prewarm      \
                pg_standby      \
                uuid-ossp \
                pg_trgm         \
                pgcrypto        \
                pgrowlocks      \
                pgstattuple     \
                pgxc_clean      \
                pg_clean        \
                pg_unlock       \
                pgxc_ctl        \
                pgxc_monitor \
                pg_visibility   \
                postgres_fdw    \
                seg             \
                spi             \
                tablefunc       \
				opentenbase_gts_tools \
		        opentenbase_memory_tools	\
                tcn             \
                test_decoding   \
                tsm_system_rows \
                tsm_system_time \
                unaccent        \
                vacuumlo        \
                stormstats		\
                opentenbase_subscription  \
				opentenbase_pooler_stat \
                pg_stat_cluster_activity

ifeq ($(with_openssl),yes)
SUBDIRS += sslinfo
else
ALWAYS_SUBDIRS += sslinfo
endif

ifneq ($(with_uuid),no)
SUBDIRS += uuid-ossp
else
ALWAYS_SUBDIRS += uuid-ossp
endif

ifeq ($(with_libxml),yes)
SUBDIRS += xml2
else
ALWAYS_SUBDIRS += xml2
endif

ifeq ($(with_selinux),yes)
SUBDIRS += sepgsql
else
ALWAYS_SUBDIRS += sepgsql
endif

ifeq ($(with_perl),yes)
SUBDIRS += hstore_plperl
else
ALWAYS_SUBDIRS += hstore_plperl
endif

ifeq ($(with_python),yes)
SUBDIRS += hstore_plpython ltree_plpython
else
ALWAYS_SUBDIRS += hstore_plpython ltree_plpython
endif

# Missing:
#		start-scripts	\ (does not have a makefile)


$(recurse)
$(recurse_always)
