/* contrib/spi/moddatetime--unpackaged--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION moddatetime FROM unpackaged" to load this file. \quit

-- 将 moddatetime() 函数添加到 moddatetime 扩展中
ALTER EXTENSION moddatetime ADD function moddatetime();
