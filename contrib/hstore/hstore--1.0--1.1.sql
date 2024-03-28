/* contrib/hstore/hstore--1.0--1.1.sql */
-- complain if script is sourced in psql, rather than via ALTER EXTENSION
/*如果脚本通过 psql 而不是通过 ALTER EXTENSION 命令加载，则输出提示信息并退出 */
\echo Use "ALTER EXTENSION hstore UPDATE TO '1.1'" to load this file. \quit

/*移除 hstore 扩展中的操作符 => (text, text)*/
ALTER EXTENSION hstore DROP OPERATOR => (text, text);
DROP OPERATOR => (text, text);
