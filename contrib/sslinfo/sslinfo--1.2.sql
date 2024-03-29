/* contrib/sslinfo/sslinfo--1.2.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION sslinfo" to load this file. \quit

-- 创建函数 ssl_client_serial()，返回类型为 numeric
CREATE FUNCTION ssl_client_serial() 
RETURNS numeric
AS 'MODULE_PATHNAME', 'ssl_client_serial'
LANGUAGE C STRICT PARALLEL RESTRICTED;

-- 创建函数 ssl_is_used()，返回类型为 boolean
CREATE FUNCTION ssl_is_used() 
RETURNS boolean
AS 'MODULE_PATHNAME', 'ssl_is_used'
LANGUAGE C STRICT PARALLEL RESTRICTED;

-- 创建函数 ssl_version()，返回类型为 text
CREATE FUNCTION ssl_version() 
RETURNS text
AS 'MODULE_PATHNAME', 'ssl_version'
LANGUAGE C STRICT PARALLEL RESTRICTED;

-- 创建函数 ssl_cipher()，返回类型为 text
CREATE FUNCTION ssl_cipher() 
RETURNS text
AS 'MODULE_PATHNAME', 'ssl_cipher'
LANGUAGE C STRICT PARALLEL RESTRICTED;

-- 创建函数 ssl_client_cert_present()，返回类型为 boolean
CREATE FUNCTION ssl_client_cert_present() 
RETURNS boolean
AS 'MODULE_PATHNAME', 'ssl_client_cert_present'
LANGUAGE C STRICT PARALLEL RESTRICTED;

-- 创建函数 ssl_client_dn_field(text)，返回类型为 text
CREATE FUNCTION ssl_client_dn_field(text) 
RETURNS text
AS 'MODULE_PATHNAME', 'ssl_client_dn_field'
LANGUAGE C STRICT PARALLEL RESTRICTED;

-- 创建函数 ssl_issuer_field(text)，返回类型为 text
CREATE FUNCTION ssl_issuer_field(text) 
RETURNS text
AS 'MODULE_PATHNAME', 'ssl_issuer_field'
LANGUAGE C STRICT PARALLEL RESTRICTED;

-- 创建函数 ssl_client_dn()，返回类型为 text
CREATE FUNCTION ssl_client_dn() 
RETURNS text
AS 'MODULE_PATHNAME', 'ssl_client_dn'
LANGUAGE C STRICT PARALLEL RESTRICTED;

-- 创建函数 ssl_issuer_dn()，返回类型为 text
CREATE FUNCTION ssl_issuer_dn() 
RETURNS text
AS 'MODULE_PATHNAME', 'ssl_issuer_dn'
LANGUAGE C STRICT PARALLEL RESTRICTED;

-- 创建函数 ssl_extension_info()，返回记录集合
CREATE FUNCTION ssl_extension_info(
    OUT name text,
    OUT value text,
    OUT critical boolean
) 
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'ssl_extension_info'
LANGUAGE C STRICT PARALLEL RESTRICTED;