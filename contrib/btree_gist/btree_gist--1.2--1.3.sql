/* contrib/btree_gist/btree_gist--1.2--1.3.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION btree_gist UPDATE TO '1.3'" to load this file. \quit

-- Add support for indexing UUID columns

-- define the GiST support methods
-- 检查 GiST 或 GIN 索引的一致性
CREATE FUNCTION gbt_uuid_consistent(internal, uuid, int2, oid, internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

-- 从索引中提取数据
CREATE FUNCTION gbt_uuid_fetch(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

-- 压缩数据以进行索引存储
CREATE FUNCTION gbt_uuid_compress(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

-- 计算 GiST 或 GIN 索引中两个键之间的惩罚
CREATE FUNCTION gbt_uuid_penalty(internal, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

-- 选择 GiST 或 GIN 索引中的分割点
CREATE FUNCTION gbt_uuid_picksplit(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

-- 合并 GiST 或 GIN 索引中的数据
CREATE FUNCTION gbt_uuid_union(internal, internal)
RETURNS gbtreekey32
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

-- 比较 GiST 或 GIN 索引中的两个键是否相同
CREATE FUNCTION gbt_uuid_same(gbtreekey32, gbtreekey32, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

-- Create the operator class
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

-- These are "loose" in the opfamily for consistency with the rest of btree_gist
ALTER OPERATOR FAMILY gist_uuid_ops USING gist ADD
	OPERATOR	6	<>  (uuid, uuid) ,
	FUNCTION	9 (uuid, uuid) gbt_uuid_fetch (internal) ;
