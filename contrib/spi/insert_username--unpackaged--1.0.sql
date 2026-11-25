/* contrib/spi/insert_username--unpackaged--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION insert_username FROM unpackaged" to load this file. \quit

-- 将 insert_username() 函数添加到 insert_username 扩展中
ALTER EXTENSION insert_username ADD function insert_username();
