/* contrib/spi/moddatetime--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION moddatetime" to load this file. \quit

-- 创建名为 moddatetime() 的函数
CREATE FUNCTION moddatetime()
RETURNS trigger -- 函数返回一个触发器
AS 'MODULE_PATHNAME' -- 触发器函数的实现位于 MODULE_PATHNAME 所指定的共享库文件中
LANGUAGE C; -- 使用 C 语言编写函数体
