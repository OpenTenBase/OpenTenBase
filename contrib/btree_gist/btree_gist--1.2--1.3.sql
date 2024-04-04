/* contrib/btree_gist/btree_gist--1.2--1.3.sql */

-- 如果在 psql 中引入脚本，而不是通过 CREATE EXTENSION，请发出警告
\echo 使用 "ALTER EXTENSION btree_gist UPDATE TO '1.3'" 命令来加载此文件。\quit

-- 添加对 UUID 列的索引支持

-- 定义 GiST 支持方法
CREATE FUNCTION gbt_uuid_consistent(internal,uuid,int2,oid,internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_uuid_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_uuid_compress(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_uuid_penalty(internal,internal,internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_uuid_picksplit(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_uuid_union(internal, internal)
RETURNS gbtreekey32
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbt_uuid_same(gbtreekey32, gbtreekey32, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

-- 创建操作符类别
CREATE OPERATOR CLASS gist_uuid_ops
DEFAULT FOR TYPE uuid USING gist
AS
	OPERATOR	1	<   ,
	OPERATOR	2	<=  ,
	OPERATOR	3	=   ,
	OPERATOR	4	>=  ,
	OPERATOR	5	>   ,
	FUNCTION	1	gbt_uuid_consistent (internal, uuid, int2, oid, internal),
	FUNCTION	2	gbt_uuid_union (internal, internal),
	FUNCTION	3	gbt_uuid_compress (internal),
	FUNCTION	4	gbt_decompress (internal),
	FUNCTION	5	gbt_uuid_penalty (internal, internal, internal),
	FUNCTION	6	gbt_uuid_picksplit (internal, internal),
	FUNCTION	7	gbt_uuid_same (gbtreekey32, gbtreekey32, internal),
	STORAGE		gbtreekey32;

-- 这些在 opfamily 中是“松散的”，以保持与 btree_gist 的其余部分一致
ALTER OPERATOR FAMILY gist_uuid_ops USING gist ADD
	OPERATOR	6	<>  (uuid, uuid) ,
	FUNCTION	9 (uuid, uuid) gbt_uuid_fetch (internal) ;
