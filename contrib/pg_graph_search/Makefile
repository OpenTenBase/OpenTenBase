# pg_graph_search — 属性图搜索扩展（元函数驱动）
#
# 构建产物由 sql/ 片段拼装：
#   make                 生成 pg_graph_search--1.0.0.sql（OpenTenBase 版，注册表 REPLICATION）
#   make portable        生成 pg_graph_search--1.0.0-portable.sql（原生 PostgreSQL 验证用）
#   make install         安装到 $(DESTDIR)$(datadir)/extension
#   make installcheck    对运行中的实例跑 pg_regress（PGHOST/PGPORT/PGUSER 可用环境变量指定）
#   make lint            SQL 模板静态门禁
#
# OpenTenBase 适配：
#   注册表必须 DISTRIBUTE BY REPLICATION，否则默认按主键首列 HASH，
#   每个 DN 只看到一部分实例清单。子句通过 @DISTRIBUTE_CLAUSE@ 占位符注入，
#   这样同一份源码也能生成“可移植版”用于在原生 PostgreSQL 上做功能验证。

EXTENSION   = pg_graph_search
EXTVERSION  = 1.0.0
DATA_built  = pg_graph_search--$(EXTVERSION).sql
PGFILEDESC  = "pg_graph_search - property graph search for OpenTenBase"

REGRESS      = pg_graph_search
REGRESS_OPTS = --inputdir=test --outputdir=test

# OpenTenBase=1（默认）注入 DISTRIBUTE BY REPLICATION；=0 生成可移植版
OPENTENBASE ?= 1
ifeq ($(OPENTENBASE),0)
DISTRIBUTE_CLAUSE ?=
else
DISTRIBUTE_CLAUSE ?= DISTRIBUTE BY REPLICATION
endif

SQL_PARTS    = $(wildcard sql/*.sql)
PORTABLE_SQL = pg_graph_search--$(EXTVERSION)-portable.sql

$(DATA_built): $(SQL_PARTS) Makefile
	@echo "  GEN  $@  (DISTRIBUTE_CLAUSE='$(DISTRIBUTE_CLAUSE)')"
	@cat $(SQL_PARTS) | sed -e 's|@DISTRIBUTE_CLAUSE@|$(DISTRIBUTE_CLAUSE)|g' > $@

.PHONY: sql portable lint
sql: $(DATA_built)

# 原生 PostgreSQL（无 Postgres-XL 语法）版本：只用于本地/CI 功能验证，不安装。
# 同时生成一个去掉 OpenTenBase 专有参数 sql_mode 的 control（原生 PG 会把
# 未知参数当成 ERROR），验证时改名安装为 pg_graph_search.control 即可。
portable:
	@cat $(SQL_PARTS) | sed -e 's|@DISTRIBUTE_CLAUSE@||g' > $(PORTABLE_SQL)
	@grep -v '^sql_mode' pg_graph_search.control > pg_graph_search-portable.control
	@echo "  GEN  $(PORTABLE_SQL) + pg_graph_search-portable.control  (portable: no DISTRIBUTE BY, no sql_mode)"

lint:
	python3 test/lint_sql.py

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_graph_search
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
