/* contrib/spi/timetravel--unpackaged--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION timetravel FROM unpackaged" to load this file. \quit

-- 将 timetravel() 函数添加到 timetravel 扩展中
ALTER EXTENSION timetravel ADD function timetravel();

-- 将 set_timetravel(name, integer) 函数添加到 timetravel 扩展中
ALTER EXTENSION timetravel ADD function set_timetravel(name text, integer);

-- 将 get_timetravel(name) 函数添加到 timetravel 扩展中
ALTER EXTENSION timetravel ADD function get_timetravel(name text);
