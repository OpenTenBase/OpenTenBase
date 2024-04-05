#-------------------------------------------------------------------------
#
# Makefile for src/test/authentication
#
# 版权所有 (c) 1996-2017，PostgreSQL 全球开发集团
# 版权所有 (c) 1994，加州大学董事会
#
# src/test/authentication/Makefile
#
#-------------------------------------------------------------------------

# 子目录
subdir = src/test/authentication
# 顶层构建目录
top_builddir = ../../..
# 包含全局 Makefile
include $(top_builddir)/src/Makefile.global

# 运行测试
check:
	$(prove_check)

# 清理操作
clean distclean maintainer-clean:
	rm -rf tmp_check
