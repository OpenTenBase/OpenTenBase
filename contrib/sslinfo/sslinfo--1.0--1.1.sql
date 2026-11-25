/* contrib/sslinfo/sslinfo--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION sslinfo UPDATE TO '1.1'" to load this file. \quit

-- 创建名为 ssl_extension_info() 的函数
CREATE FUNCTION ssl_extension_info(
    OUT name text, -- 输出参数：扩展名称
    OUT value text, -- 输出参数：扩展值
    OUT critical boolean -- 输出参数：是否关键扩展
) 
RETURNS SETOF record -- 函数返回一个记录集合
AS 'MODULE_PATHNAME', 'ssl_extension_info' -- 函数实现位于 MODULE_PATHNAME 共享库中的 ssl_extension_info 函数
LANGUAGE C STRICT; -- 使用 C 语言编写严格模式的函数体