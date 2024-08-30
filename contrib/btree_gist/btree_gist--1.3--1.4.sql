/* contrib/btree_gist/btree_gist--1.3--1.4.sql */

-- 如果在 psql 中引入脚本，而不是通过 CREATE EXTENSION，请发出警告
\echo 使用 "ALTER EXTENSION btree_gist UPDATE TO '1.4'" 命令来加载此文件。 \quit

-- 添加对 macaddr8 列的索引支持

-- 定义 GiST 支持方法
CREATE FUNCTION gbt_macad8_consistent(internal,macaddr8,int2,oid,internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_macad8_compress(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_macad8_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_macad8_penalty(internal,internal,internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_macad8_picksplit(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_macad8_union(internal, internal)
RETURNS gbtreekey16
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_macad8_same(gbtreekey16, gbtreekey16, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

-- 创建操作符类别
CREATE OPERATOR CLASS gist_macaddr8_ops
DEFAULT FOR TYPE macaddr8 USING gist
AS
	OPERATOR	1	<   ,  -- 小于
	OPERATOR	2	<=  ,  -- 小于等于
	OPERATOR	3	=   ,  -- 等于
	OPERATOR	4	>=  ,  -- 大于等于
	OPERATOR	5	>   ,  -- 大于
	FUNCTION	1	gbt_macad8_consistent (internal, macaddr8, int2, oid, internal),  -- 一致性检查函数
	FUNCTION	2	gbt_macad8_union (internal, internal),  -- 聚合函数
	FUNCTION	3	gbt_macad8_compress (internal),  -- 压缩函数
	FUNCTION	4	gbt_decompress (internal),  -- 解压函数
	FUNCTION	5	gbt_macad8_penalty (internal, internal, internal),  -- 惩罚函数
	FUNCTION	6	gbt_macad8_picksplit (internal, internal),  -- 分裂函数
	FUNCTION	7	gbt_macad8_same (gbtreekey16, gbtreekey16, internal),  -- 相同检查函数
	STORAGE		gbtreekey16;

-- 这些在 opfamily 中是“松散的”，以保持与 btree_gist 的其余部分一致
ALTER OPERATOR FAMILY gist_macaddr8_ops USING gist ADD
	OPERATOR	6	<>  (macaddr8, macaddr8) ,  -- 不等于
	FUNCTION	9 (macaddr8, macaddr8) gbt_macad8_fetch (internal);  -- 抓取函数
