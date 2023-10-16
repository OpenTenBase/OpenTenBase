# contrib/opentenbase_memory_tools/Makefile
#
# Copyright (c) 2023 THL A29 Limited, a Tencent company.
#
# This source code file is licensed under the BSD 3-Clause License,
# you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
#

MODULES = opentenbase_memory_tools

## 扩展名称;
EXTENSION = opentenbase_memory_tools

## 扩展安装的SQL文件;
DATA = opentenbase_memory_tools--1.0.sql

## 扩展描述;
PGFILEDESC = "opentenbase_memory_tools - memory wrapper for OpenTenBase"

### 以下为Pg构建扩展相关命令;
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)  ## 环境变量参数加载;
else
subdir = contrib/opentenbase_memory_tools
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
