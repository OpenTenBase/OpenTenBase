/* contrib/spi/refint--unpackaged--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION refint FROM unpackaged" to load this file. \quit

-- 将 check_primary_key() 函数添加到 refint 扩展中
ALTER EXTENSION refint ADD function check_primary_key();

-- 将 check_foreign_key() 函数添加到 refint 扩展中
ALTER EXTENSION refint ADD function check_foreign_key();
