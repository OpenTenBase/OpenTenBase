/* contrib/lo/lo--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
-- 如果在psql中直接执行该脚本而不是通过ALTER EXTENSION命令执行，则报错提示
\echo Use "ALTER EXTENSION lo UPDATE TO '1.1'" to load this file. \quit
-- 使用 "ALTER EXTENSION lo UPDATE TO '1.1'" 命令来加载这个文件。

-- Mark the function as able to be executed in parallel query execution
-- 将函数标记为可在并行查询执行中运行的函数
ALTER FUNCTION lo_oid(lo) PARALLEL SAFE;
