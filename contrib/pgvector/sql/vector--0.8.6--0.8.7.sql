-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION vector UPDATE TO '0.8.7'" to load this file. \quit

CREATE FUNCTION ivfflat_list_stats(regclass, real DEFAULT 1.0)
RETURNS TABLE (
	format_version integer,
	dimensions integer,
	declared_lists integer,
	list_id integer,
	list_page bigint,
	list_offset integer,
	start_page bigint,
	insert_page bigint,
	entry_pages bigint,
	physical_tuple_count bigint,
	usable_bytes bigint,
	used_bytes bigint,
	free_bytes bigint
)
	AS 'MODULE_PATHNAME', 'ivfflat_list_stats'
	LANGUAGE C STRICT VOLATILE PARALLEL RESTRICTED;

CREATE FUNCTION ivfflat_index_stats(regclass)
RETURNS TABLE (
	format_version integer,
	dimensions integer,
	declared_lists integer,
	list_pages bigint,
	entry_pages bigint,
	physical_indexed_tuples bigint,
	empty_lists bigint,
	usable_bytes bigint,
	used_bytes bigint,
	free_bytes bigint,
	scanned_pages bigint,
	scan_elapsed_ms double precision
)
	AS 'MODULE_PATHNAME', 'ivfflat_index_stats'
	LANGUAGE C STRICT VOLATILE PARALLEL RESTRICTED;
