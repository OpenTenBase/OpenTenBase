/* contrib/spi/autoinc--unpackaged--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
-- 如果脚本通过 psql 而不是通过 CREATE EXTENSION 加载，则给出警告
\echo Use "CREATE EXTENSION autoinc FROM unpackaged" to load this file. \quit

-- 将 autoinc() 函数添加到 autoinc 扩展中
ALTER EXTENSION autoinc ADD function autoinc();
