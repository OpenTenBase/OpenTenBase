# contrib/slow_sql_recorder/Makefile
MODULE_big = slow_sql_recorder
OBJS = slow_sql_recorder.o
    
EXTENSION = slow_sql_recorder  # 插件名
DATA = slow_sql_recorder--1.0.sql  # 插件的sql文件
PGFILEDESC = "slow_sql_recorder - recorder about slow sql executing over 10 second"

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

REGRESS = slow_sql_recorder # sql、expected 文件夹下的测试sql文件名

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/slow_sql_recorder
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

SHLIB_LINK += $(filter -lssl -lcrypto -lssleay32 -leay32, $(LIBS))