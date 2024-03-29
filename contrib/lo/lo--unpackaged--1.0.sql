/* contrib/lo/lo--unpackaged--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION lo FROM unpackaged" to load this file. \quit

-- 为 Large Object (lo) 扩展添加域（domain）的声明
ALTER EXTENSION lo ADD domain lo;

-- 为 Large Object (lo) 扩展添加函数 lo_oid
ALTER EXTENSION lo ADD function lo_oid(lo);

-- 为 Large Object (lo) 扩展添加函数 lo_manage
ALTER EXTENSION lo ADD function lo_manage();