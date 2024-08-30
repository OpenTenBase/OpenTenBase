/* contrib/citext/citext--1.3--1.4.sql */

-- 如果在 psql 中引入脚本，而不是通过 ALTER EXTENSION，请发出警告
\echo 使用 "ALTER EXTENSION citext UPDATE TO '1.4'" 命令来加载此文件。 \quit

-- 创建函数 regexp_match，用于不区分大小写地匹配 citext 类型的字符串
CREATE FUNCTION regexp_match( citext, citext ) RETURNS TEXT[] AS $$
    SELECT pg_catalog.regexp_match( $1::pg_catalog.text, $2::pg_catalog.text, 'i' );
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- 创建函数 regexp_match，用于根据第三个参数是否包含 'c' 来确定是否区分大小写地匹配 citext 类型的字符串
CREATE FUNCTION regexp_match( citext, citext, text ) RETURNS TEXT[] AS $$
    SELECT pg_catalog.regexp_match( $1::pg_catalog.text, $2::pg_catalog.text, CASE WHEN pg_catalog.strpos($3, 'c') = 0 THEN  $3 || 'i' ELSE $3 END );
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
