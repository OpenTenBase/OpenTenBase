/* contrib/opentenbase_gts/opentenbase_gts_tools--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "create EXTENSION opentenbase_gts_tools" to load this file. \quit

CREATE FUNCTION txid_gts(bigint)
RETURNS bigint
AS 'MODULE_PATHNAME', 'txid_gts'
LANGUAGE C STRICT;

CREATE FUNCTION txid_gts_raw(bigint)
    RETURNS bigint
AS 'MODULE_PATHNAME', 'txid_gts_raw'
LANGUAGE C STRICT;

CREATE FUNCTION txid_set_gts(IN  xid bigint,
                             IN  gts bigint)
    RETURNS bigint
AS 'MODULE_PATHNAME', 'txid_set_gts'
LANGUAGE C STRICT;

--
-- heap_page_items_with_gts()
-- according to heap_page_items_with_gts() from pageinspect--1.5.sql
--
CREATE FUNCTION heap_page_items_with_gts(IN page bytea,
    OUT lp smallint,
    OUT lp_off smallint,
    OUT lp_flags smallint,
    OUT lp_len smallint,
    OUT t_xmin xid,
    OUT t_xmax xid,
    OUT t_xmin_gts bigint,
    OUT t_xmax_gts bigint,
    OUT t_field3 int4,
    OUT t_ctid tid,
    OUT t_infomask2 integer,
    OUT t_infomask integer,
    OUT t_shard smallint,
    OUT t_hoff smallint,
    OUT t_bits text,
    OUT t_oid oid,
    OUT t_data bytea)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'heap_page_items_with_gts'
LANGUAGE C STRICT PARALLEL SAFE;


CREATE FUNCTION heap_page_ids(IN page bytea,
    OUT lp smallint,
    OUT lp_off smallint,
    OUT lp_flags smallint,
    OUT lp_len smallint,
    OUT t_xmin xid,
    OUT t_xmax xid,
    OUT t_xmin_gts bigint,
    OUT t_xmax_gts bigint,
    OUT t_field3 int4,
    OUT t_ctid tid,
    OUT t_infomask2 integer,
    OUT t_infomask integer,
    OUT t_shard smallint,
    OUT t_hoff smallint,
    OUT t_bits text,
    OUT t_oid oid,
    OUT t_data bytea)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'heap_page_ids'
LANGUAGE C STRICT PARALLEL SAFE;


CREATE FUNCTION heap_page_items_without_data(IN page bytea,
    OUT lp smallint,
    OUT lp_off smallint,
    OUT lp_flags smallint,
    OUT lp_len smallint,
    OUT t_xmin xid,
    OUT t_xmax xid,
    OUT t_xmin_gts bigint,
    OUT t_xmax_gts bigint,
    OUT t_field3 int4,
    OUT t_ctid tid,
    OUT t_infomask2 integer,
    OUT t_infomask integer,
    OUT t_shard smallint,
    OUT t_hoff smallint,
    OUT t_bits text,
    OUT t_oid oid,
    OUT t_data bytea)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'heap_page_items_without_data'
LANGUAGE C STRICT PARALLEL SAFE;


CREATE FUNCTION heap_page_items_with_gts_log(IN page bytea,
    OUT lp smallint,
    OUT lp_off smallint,
    OUT lp_flags smallint,
    OUT lp_len smallint,
    OUT t_xmin xid,
    OUT t_xmax xid,
    OUT t_xmin_gts bigint,
    OUT t_xmax_gts bigint,
    OUT t_field3 int4,
    OUT t_ctid tid,
    OUT t_infomask2 integer,
    OUT t_infomask integer,
    OUT t_shard smallint,
    OUT t_hoff smallint,
    OUT t_bits text,
    OUT t_oid oid,
    OUT t_data bytea)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'heap_page_items_with_gts_log'
LANGUAGE C STRICT PARALLEL SAFE;
