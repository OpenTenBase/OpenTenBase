/* contrib/chkpass/chkpass--unpackaged--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION chkpass FROM unpackaged" to load this file. \quit

-- 添加 chkpass 类型到 chkpass 扩展
ALTER EXTENSION chkpass ADD type chkpass;

-- 添加 chkpass_in 函数到 chkpass 扩展，用于将 cstring 转换为 chkpass 类型
ALTER EXTENSION chkpass ADD function chkpass_in(cstring);

-- 添加 chkpass_out 函数到 chkpass 扩展，用于将 chkpass 类型转换为文本表示形式
ALTER EXTENSION chkpass ADD function chkpass_out(chkpass);

-- 添加 raw 函数到 chkpass 扩展，用于将 chkpass 类型转换为内部形式
ALTER EXTENSION chkpass ADD function raw(chkpass);

-- 添加 eq 函数到 chkpass 扩展，用于比较 chkpass 类型和文本是否相等
ALTER EXTENSION chkpass ADD function eq(chkpass, text);

-- 添加 ne 函数到 chkpass 扩展，用于比较 chkpass 类型和文本是否不相等
ALTER EXTENSION chkpass ADD function ne(chkpass, text);

-- 添加 <> 操作符到 chkpass 扩展，用于比较 chkpass 类型和文本是否不相等
ALTER EXTENSION chkpass ADD operator <>(chkpass, text);

-- 添加 = 操作符到 chkpass 扩展，用于比较 chkpass 类型和文本是否相等
ALTER EXTENSION chkpass ADD operator =(chkpass, text);