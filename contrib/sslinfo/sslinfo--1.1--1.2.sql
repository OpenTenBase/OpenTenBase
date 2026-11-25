/* contrib/sslinfo/sslinfo--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION sslinfo UPDATE TO '1.2'" to load this file. \quit

-- 将 ssl_client_serial() 函数设置为 PARALLEL RESTRICTED
ALTER FUNCTION ssl_client_serial() PARALLEL RESTRICTED;

-- 将 ssl_is_used() 函数设置为 PARALLEL RESTRICTED
ALTER FUNCTION ssl_is_used() PARALLEL RESTRICTED;

-- 将 ssl_version() 函数设置为 PARALLEL RESTRICTED
ALTER FUNCTION ssl_version() PARALLEL RESTRICTED;

-- 将 ssl_cipher() 函数设置为 PARALLEL RESTRICTED
ALTER FUNCTION ssl_cipher() PARALLEL RESTRICTED;

-- 将 ssl_client_cert_present() 函数设置为 PARALLEL RESTRICTED
ALTER FUNCTION ssl_client_cert_present() PARALLEL RESTRICTED;

-- 将 ssl_client_dn_field(text) 函数设置为 PARALLEL RESTRICTED
ALTER FUNCTION ssl_client_dn_field(text) PARALLEL RESTRICTED;

-- 将 ssl_issuer_field(text) 函数设置为 PARALLEL RESTRICTED
ALTER FUNCTION ssl_issuer_field(text) PARALLEL RESTRICTED;

-- 将 ssl_client_dn() 函数设置为 PARALLEL RESTRICTED
ALTER FUNCTION ssl_client_dn() PARALLEL RESTRICTED;

-- 将 ssl_issuer_dn() 函数设置为 PARALLEL RESTRICTED
ALTER FUNCTION ssl_issuer_dn() PARALLEL RESTRICTED;

-- 将 ssl_extension_info() 函数设置为 PARALLEL RESTRICTED
ALTER FUNCTION ssl_extension_info() PARALLEL RESTRICTED;
