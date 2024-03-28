/* contrib/hstore/hstore--1.3--1.4.sql */
-- complain if script is sourced in psql, rather than via ALTER EXTENSION
-- 如果脚本通过 psql 而不是通过 ALTER EXTENSION 命令加载，则输出提示信息并退出
\echo Use "ALTER EXTENSION hstore UPDATE TO '1.4'" to load this file. \quit

/*更新过程签名的硬编码方式。
我们使用 to_regprocedure() 函数，以便在针对 9.6beta1 定义运行时，查询不会失败，
 在这种情况下，to_regprocedure() 将返回 NULL，不会进行更新。

通过将新类型转换为 OID 向 pg_proc 表中的函数更新参数类型和参数数量
*/
-- Update procedure signatures the hard way.
-- We use to_regprocedure() so that query doesn't fail if run against 9.6beta1 definitions,
-- wherein the signatures have been updated already.  In that case to_regprocedure() will
-- return NULL and no updates will happen.
UPDATE pg_catalog.pg_proc SET
  proargtypes = pg_catalog.array_to_string(newtypes::pg_catalog.oid[], ' ')::pg_catalog.oidvector,
  pronargs = pg_catalog.array_length(newtypes, 1)
FROM (VALUES
(NULL::pg_catalog.text, NULL::pg_catalog.regtype[]), -- 确定列类型
('ghstore_same(internal,internal,internal)', '{ghstore,ghstore,internal}'),
('ghstore_consistent(internal,internal,int4,oid,internal)', '{internal,hstore,int2,oid,internal}'),
('gin_extract_hstore(internal,internal)', '{hstore,internal}'),
('gin_extract_hstore_query(internal,internal,int2,internal,internal)', '{hstore,internal,int2,internal,internal}'),
('gin_consistent_hstore(internal,int2,internal,int4,internal,internal)', '{internal,int2,hstore,int4,internal,internal}')
) AS update_data (oldproc, newtypes)
WHERE oid = pg_catalog.to_regprocedure(oldproc);

-- 更新函数返回类型为 'ghstore' 的函数
UPDATE pg_catalog.pg_proc SET
  prorettype = 'ghstore'::pg_catalog.regtype
WHERE oid = pg_catalog.to_regprocedure('ghstore_union(internal,internal)');

-- 将指定函数设置为并行安全
ALTER FUNCTION hstore_in(cstring) PARALLEL SAFE;
ALTER FUNCTION hstore_out(hstore) PARALLEL SAFE;
ALTER FUNCTION hstore_recv(internal) PARALLEL SAFE;
ALTER FUNCTION hstore_send(hstore) PARALLEL SAFE;
ALTER FUNCTION hstore_version_diag(hstore) PARALLEL SAFE;
ALTER FUNCTION fetchval(hstore, text) PARALLEL SAFE;
ALTER FUNCTION slice_array(hstore, text[]) PARALLEL SAFE;
ALTER FUNCTION slice(hstore, text[]) PARALLEL SAFE;
ALTER FUNCTION isexists(hstore, text) PARALLEL SAFE;
ALTER FUNCTION exist(hstore, text) PARALLEL SAFE;
ALTER FUNCTION exists_any(hstore, text[]) PARALLEL SAFE;
ALTER FUNCTION exists_all(hstore, text[]) PARALLEL SAFE;
ALTER FUNCTION isdefined(hstore, text) PARALLEL SAFE;
ALTER FUNCTION defined(hstore, text) PARALLEL SAFE;
ALTER FUNCTION delete(hstore, text) PARALLEL SAFE;
ALTER FUNCTION delete(hstore, text[]) PARALLEL SAFE;
ALTER FUNCTION delete(hstore, hstore) PARALLEL SAFE;
ALTER FUNCTION hs_concat(hstore, hstore) PARALLEL SAFE;
ALTER FUNCTION hs_contains(hstore, hstore) PARALLEL SAFE;
ALTER FUNCTION hs_contained(hstore, hstore) PARALLEL SAFE;
ALTER FUNCTION tconvert(text, text) PARALLEL SAFE;
ALTER FUNCTION hstore(text, text) PARALLEL SAFE;
ALTER FUNCTION hstore(text[], text[]) PARALLEL SAFE;
ALTER FUNCTION hstore(text[]) PARALLEL SAFE;
ALTER FUNCTION hstore_to_json(hstore) PARALLEL SAFE;
ALTER FUNCTION hstore_to_json_loose(hstore) PARALLEL SAFE;
ALTER FUNCTION hstore_to_jsonb(hstore) PARALLEL SAFE;
ALTER FUNCTION hstore_to_jsonb_loose(hstore) PARALLEL SAFE;
ALTER FUNCTION hstore(record) PARALLEL SAFE;
ALTER FUNCTION hstore_to_array(hstore) PARALLEL SAFE;
ALTER FUNCTION hstore_to_matrix(hstore) PARALLEL SAFE;
ALTER FUNCTION akeys(hstore) PARALLEL SAFE;
ALTER FUNCTION avals(hstore) PARALLEL SAFE;
ALTER FUNCTION skeys(hstore) PARALLEL SAFE;
ALTER FUNCTION svals(hstore) PARALLEL SAFE;
ALTER FUNCTION each(hstore) PARALLEL SAFE;
ALTER FUNCTION populate_record(anyelement, hstore) PARALLEL SAFE;
ALTER FUNCTION hstore_eq(hstore, hstore) PARALLEL SAFE;
ALTER FUNCTION hstore_ne(hstore, hstore) PARALLEL SAFE;
ALTER FUNCTION hstore_gt(hstore, hstore) PARALLEL SAFE;
ALTER FUNCTION hstore_ge(hstore, hstore) PARALLEL SAFE;
ALTER FUNCTION hstore_lt(hstore, hstore) PARALLEL SAFE;
ALTER FUNCTION hstore_le(hstore, hstore) PARALLEL SAFE;
ALTER FUNCTION hstore_cmp(hstore, hstore) PARALLEL SAFE;
ALTER FUNCTION hstore_hash(hstore) PARALLEL SAFE;
ALTER FUNCTION ghstore_in(cstring) PARALLEL SAFE;
ALTER FUNCTION ghstore_out(ghstore) PARALLEL SAFE;
ALTER FUNCTION ghstore_compress(internal) PARALLEL SAFE;
ALTER FUNCTION ghstore_decompress(internal) PARALLEL SAFE;
ALTER FUNCTION ghstore_penalty(internal, internal, internal) PARALLEL SAFE;
ALTER FUNCTION ghstore_picksplit(internal, internal) PARALLEL SAFE;
ALTER FUNCTION ghstore_union(internal, internal) PARALLEL SAFE;
ALTER FUNCTION ghstore_same(ghstore, ghstore, internal) PARALLEL SAFE;
ALTER FUNCTION ghstore_consistent(internal, hstore, smallint, oid, internal) PARALLEL SAFE;
ALTER FUNCTION gin_extract_hstore(hstore, internal) PARALLEL SAFE;
ALTER FUNCTION gin_extract_hstore_query(hstore, internal, int2, internal, internal) PARALLEL SAFE;
ALTER FUNCTION gin_consistent_hstore(internal, int2, hstore, int4, internal, internal) PARALLEL SAFE;
