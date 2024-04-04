/* contrib/chkpass/chkpass--unpackaged--1.0.sql */

-- 如果在 psql 中引入脚本，而不是通过 CREATE EXTENSION，请发出警告
\echo 使用 "CREATE EXTENSION chkpass FROM unpackaged" 命令来加载此文件。 \quit

-- 添加类型 chkpass
ALTER EXTENSION chkpass ADD type chkpass;

-- 添加函数 chkpass_in
ALTER EXTENSION chkpass ADD function chkpass_in(cstring);

-- 添加函数 chkpass_out
ALTER EXTENSION chkpass ADD function chkpass_out(chkpass);

-- 添加函数 raw
ALTER EXTENSION chkpass ADD function raw(chkpass);

-- 添加函数 eq
ALTER EXTENSION chkpass ADD function eq(chkpass,text);

-- 添加函数 ne
ALTER EXTENSION chkpass ADD function ne(chkpass,text);

-- 添加操作符 <>
ALTER EXTENSION chkpass ADD operator <>(chkpass,text);

-- 添加操作符 =
ALTER EXTENSION chkpass ADD operator =(chkpass,text);
