/* contrib/spi/refint--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION refint" to load this file. \quit

-- 创建名为 check_primary_key() 的函数
CREATE FUNCTION check_primary_key()
RETURNS trigger -- 函数返回一个触发器
AS 'MODULE_PATHNAME' -- 触发器函数的实现位于 MODULE_PATHNAME 所指定的共享库文件中
LANGUAGE C; -- 使用 C 语言编写函数体

-- 创建名为 check_foreign_key() 的函数
CREATE FUNCTION check_foreign_key()
RETURNS trigger -- 函数返回一个触发器
AS 'MODULE_PATHNAME' -- 触发器函数的实现位于 MODULE_PATHNAME 所指定的共享库文件中
LANGUAGE C; -- 使用 C 语言编写函数体
