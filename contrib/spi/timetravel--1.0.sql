/* contrib/spi/timetravel--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION timetravel" to load this file. \quit

-- 创建名为 timetravel() 的函数
CREATE FUNCTION timetravel()
RETURNS trigger -- 函数返回一个触发器
AS 'MODULE_PATHNAME' -- 触发器函数的实现位于 MODULE_PATHNAME 所指定的共享库文件中
LANGUAGE C; -- 使用 C 语言编写函数体

-- 创建名为 set_timetravel(name, int4) 的函数
CREATE FUNCTION set_timetravel(name text, value int)
RETURNS int -- 函数返回一个整数
AS 'MODULE_PATHNAME' -- 函数的实现位于 MODULE_PATHNAME 所指定的共享库文件中
LANGUAGE C 
RETURNS NULL ON NULL INPUT; -- 如果输入为 NULL，则返回 NULL

-- 创建名为 get_timetravel(name) 的函数
CREATE FUNCTION get_timetravel(name text)
RETURNS int -- 函数返回一个整数
AS 'MODULE_PATHNAME' -- 函数的实现位于 MODULE_PATHNAME 所指定的共享库文件中
LANGUAGE C 
RETURNS NULL ON NULL INPUT; -- 如果输入为 NULL，则返回 NULL
