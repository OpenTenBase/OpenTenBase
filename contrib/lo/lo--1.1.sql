/* contrib/lo/lo--1.1.sql */

-- Ensure this script is run using the proper PostgreSQL extension mechanism
-- 确保此脚本通过正确的 PostgreSQL 扩展机制来运行
\echo Use "CREATE EXTENSION lo" to load this file. \quit

-- Define the 'lo' domain as an OID to manage large objects within PostgreSQL
-- 定义 'lo' 域作为 OID，以便在 PostgreSQL 内部管理大型对象
CREATE DOMAIN lo AS pg_catalog.oid;

-- For backward compatibility, provide a function named 'lo_oid'
-- The other functions defining explicit conversions are not required
-- because the implicit casting between a domain and its base type is covered by PostgreSQL
-- 为了向后兼容性，提供一个名为 'lo_oid' 的函数
-- 定义显式转换的其他函数不再需要，因为域与基础类型之间的隐式转换已由 PostgreSQL 处理
CREATE FUNCTION lo_oid(lo) RETURNS pg_catalog.oid AS
'SELECT $1::pg_catalog.oid' LANGUAGE SQL STRICT IMMUTABLE PARALLEL SAFE;

-- Define the 'lo_manage' function to handle large objects during trigger execution
-- Use C language function implementation referred by 'MODULE_PATHNAME'
-- 定义 'lo_manage' 函数，在触发器执行期间处理大型对象
-- 使用由 'MODULE_PATHNAME' 引用的 C 语言函数实现
CREATE FUNCTION lo_manage()
RETURNS pg_catalog.trigger
AS 'MODULE_PATHNAME'
LANGUAGE C;
