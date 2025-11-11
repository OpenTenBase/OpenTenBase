/* contrib/spi/autoinc--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
-- 如果脚本通过 psql 而不是通过 CREATE EXTENSION 加载，则给出警告
\echo Use "CREATE EXTENSION autoinc" to load this file. \quit

-- 创建名为 autoinc() 的函数
CREATE FUNCTION autoinc()
RETURNS trigger -- 函数返回一个触发器
AS 'MODULE_PATHNAME' -- 触发器函数的实现位于 MODULE_PATHNAME 所指定的共享库文件中
LANGUAGE C; -- 使用 C 语言编写函数体