/* contrib/amcheck/amcheck--1.0.sql */

-- 如果脚本通过 psql 运行而不是通过 CREATE EXTENSION 运行，则报错
\echo 使用 "CREATE EXTENSION amcheck" 命令来加载此文件。 \quit

--
-- bt_index_check()
--

-- 检查 B-tree 索引
CREATE FUNCTION bt_index_check(index regclass)
RETURNS VOID
AS 'MODULE_PATHNAME', 'bt_index_check'
LANGUAGE C STRICT PARALLEL RESTRICTED;

--
-- bt_index_parent_check()
--

-- 检查 B-tree 索引的父节点
CREATE FUNCTION bt_index_parent_check(index regclass)
RETURNS VOID
AS 'MODULE_PATHNAME', 'bt_index_parent_check'
LANGUAGE C STRICT PARALLEL RESTRICTED;

-- 不希望这些函数对公众可见
REVOKE ALL ON FUNCTION bt_index_check(regclass) FROM PUBLIC;
REVOKE ALL ON FUNCTION bt_index_parent_check(regclass) FROM PUBLIC;
