/* contrib/sslinfo/sslinfo--unpackaged--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION sslinfo FROM unpackaged" to load this file. \quit

-- 添加函数 ssl_client_serial() 到 sslinfo 扩展
ALTER EXTENSION sslinfo ADD function ssl_client_serial();

-- 添加函数 ssl_is_used() 到 sslinfo 扩展
ALTER EXTENSION sslinfo ADD function ssl_is_used();

-- 添加函数 ssl_client_cert_present() 到 sslinfo 扩展
ALTER EXTENSION sslinfo ADD function ssl_client_cert_present();

-- 添加函数 ssl_client_dn_field(text) 到 sslinfo 扩展
ALTER EXTENSION sslinfo ADD function ssl_client_dn_field(text);

-- 添加函数 ssl_issuer_field(text) 到 sslinfo 扩展
ALTER EXTENSION sslinfo ADD function ssl_issuer_field(text);

-- 添加函数 ssl_client_dn() 到 sslinfo 扩展
ALTER EXTENSION sslinfo ADD function ssl_client_dn();

-- 添加函数 ssl_issuer_dn() 到 sslinfo 扩展
ALTER EXTENSION sslinfo ADD function ssl_issuer_dn();

-- These functions were not in 9.0:

-- 创建函数 ssl_version()，返回类型为 text
CREATE FUNCTION ssl_version() 
RETURNS text
AS 'MODULE_PATHNAME', 'ssl_version'
LANGUAGE C STRICT;

-- 创建函数 ssl_cipher()，返回类型为 text
CREATE FUNCTION ssl_cipher() 
RETURNS text
AS 'MODULE_PATHNAME', 'ssl_cipher'
LANGUAGE C STRICT;