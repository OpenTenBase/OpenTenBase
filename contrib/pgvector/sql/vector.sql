-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION vector" to load this file. \quit

-- vector type

CREATE TYPE vector;

CREATE FUNCTION vector_in(cstring, oid, integer) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_out(vector) RETURNS cstring
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_typmod_in(cstring[]) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_recv(internal, oid, integer) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_send(vector) RETURNS bytea
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE vector (
	INPUT     = vector_in,
	OUTPUT    = vector_out,
	TYPMOD_IN = vector_typmod_in,
	RECEIVE   = vector_recv,
	SEND      = vector_send,
	STORAGE   = external
);

-- vector functions

CREATE FUNCTION l2_distance(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION inner_product(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION cosine_distance(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION l1_distance(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_dims(vector) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_norm(vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION l2_normalize(vector) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION binary_quantize(vector) RETURNS bit
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION subvector(vector, int, int) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- vector private functions

CREATE FUNCTION vector_add(vector, vector) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_sub(vector, vector) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_mul(vector, vector) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_concat(vector, vector) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_lt(vector, vector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_le(vector, vector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_eq(vector, vector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_ne(vector, vector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_ge(vector, vector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_gt(vector, vector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_cmp(vector, vector) RETURNS int4
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_l2_squared_distance(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_negative_inner_product(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_spherical_distance(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_accum(double precision[], vector) RETURNS double precision[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_avg(double precision[]) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_combine(double precision[], double precision[]) RETURNS double precision[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- vector aggregates

CREATE AGGREGATE avg(vector) (
	SFUNC = vector_accum,
	STYPE = double precision[],
	FINALFUNC = vector_avg,
	COMBINEFUNC = vector_combine,
	INITCOND = '{0}',
	PARALLEL = SAFE
);

CREATE AGGREGATE sum(vector) (
	SFUNC = vector_add,
	STYPE = vector,
	COMBINEFUNC = vector_add,
	PARALLEL = SAFE
);

-- vector cast functions

CREATE FUNCTION vector(vector, integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_vector(integer[], integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_vector(real[], integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_vector(double precision[], integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_vector(numeric[], integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_to_float4(vector, integer, boolean) RETURNS real[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- vector casts

CREATE CAST (vector AS vector)
	WITH FUNCTION vector(vector, integer, boolean) AS IMPLICIT;

CREATE CAST (vector AS real[])
	WITH FUNCTION vector_to_float4(vector, integer, boolean) AS IMPLICIT;

CREATE CAST (integer[] AS vector)
	WITH FUNCTION array_to_vector(integer[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (real[] AS vector)
	WITH FUNCTION array_to_vector(real[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (double precision[] AS vector)
	WITH FUNCTION array_to_vector(double precision[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (numeric[] AS vector)
	WITH FUNCTION array_to_vector(numeric[], integer, boolean) AS ASSIGNMENT;

-- vector operators

CREATE OPERATOR <-> (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = l2_distance,
	COMMUTATOR = '<->'
);

CREATE OPERATOR <#> (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_negative_inner_product,
	COMMUTATOR = '<#>'
);

CREATE OPERATOR <=> (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = cosine_distance,
	COMMUTATOR = '<=>'
);

CREATE OPERATOR <+> (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = l1_distance,
	COMMUTATOR = '<+>'
);

CREATE OPERATOR + (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_add,
	COMMUTATOR = +
);

CREATE OPERATOR - (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_sub
);

CREATE OPERATOR * (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_mul,
	COMMUTATOR = *
);

CREATE OPERATOR || (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_concat
);

CREATE OPERATOR < (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_lt,
	COMMUTATOR = > , NEGATOR = >= ,
	RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR <= (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_le,
	COMMUTATOR = >= , NEGATOR = > ,
	RESTRICT = scalarlesel, JOIN = scalarlejoinsel
);

CREATE OPERATOR = (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_eq,
	COMMUTATOR = = , NEGATOR = <> ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

CREATE OPERATOR <> (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_ne,
	COMMUTATOR = <> , NEGATOR = = ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

CREATE OPERATOR >= (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_ge,
	COMMUTATOR = <= , NEGATOR = < ,
	RESTRICT = scalargesel, JOIN = scalargejoinsel
);

CREATE OPERATOR > (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_gt,
	COMMUTATOR = < , NEGATOR = <= ,
	RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

-- access methods

CREATE FUNCTION ivfflathandler(internal) RETURNS index_am_handler
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE ACCESS METHOD ivfflat TYPE INDEX HANDLER ivfflathandler;

COMMENT ON ACCESS METHOD ivfflat IS 'ivfflat index access method';

CREATE FUNCTION hnswhandler(internal) RETURNS index_am_handler
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE ACCESS METHOD hnsw TYPE INDEX HANDLER hnswhandler;

COMMENT ON ACCESS METHOD hnsw IS 'hnsw index access method';

-- access method private functions

CREATE FUNCTION ivfflat_halfvec_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION ivfflat_bit_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION hnsw_halfvec_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION hnsw_bit_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION hnsw_sparsevec_support(internal) RETURNS internal
	AS 'MODULE_PATHNAME' LANGUAGE C;

-- Statistics for the most recent HNSW index build performed by this session.
--
-- While the build is running, the same phases can be watched live through
-- pg_stat_progress_create_index, which also shows the completion percentage.
-- Times are in milliseconds, sizes in bytes.
CREATE FUNCTION vector_index_build_report(
	OUT init_ms float8,
	OUT load_ms float8,
	OUT flush_ms float8,
	OUT finalize_ms float8,
	OUT total_ms float8,
	OUT memory_peak bigint,
	OUT memory_total bigint,
	OUT tuples_done bigint,
	OUT flushed_to_disk boolean,
	OUT valid boolean)
RETURNS record
	AS 'MODULE_PATHNAME' LANGUAGE C STABLE;

COMMENT ON FUNCTION vector_index_build_report() IS 'statistics for the most recent HNSW index build in this session';

-- Estimate how much memory an HNSW index build will need, without building
-- anything. Useful for planning maintenance_work_mem ahead of time.
CREATE FUNCTION vector_index_memory_estimate(
	rows bigint,
	dims int,
	m int DEFAULT 16,
	OUT estimated_bytes bigint,
	OUT estimated_pretty text,
	OUT suggested_work_mem text)
RETURNS record
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE PARALLEL SAFE;

COMMENT ON FUNCTION vector_index_memory_estimate(bigint, int, int) IS
	'estimate the memory needed to build an HNSW index';

-- The same estimate, worked out for an existing table: the row count comes
-- from pg_class and the dimension from the vector column.
CREATE FUNCTION vector_index_memory_advise(
	tbl regclass,
	m int DEFAULT 16,
	OUT rows_est bigint,
	OUT dims int,
	OUT estimated_bytes bigint,
	OUT estimated_pretty text,
	OUT current_work_mem text,
	OUT suggested_work_mem text,
	OUT fits boolean)
RETURNS record
	AS 'MODULE_PATHNAME' LANGUAGE C STABLE;

COMMENT ON FUNCTION vector_index_memory_advise(regclass, int) IS
	'estimate the memory needed to build an HNSW index on an existing table';

-- Measure the actual recall of a vector index: runs the same nearest neighbour
-- queries through the index and through an exact (brute force) scan, then
-- reports how much of the exact answer the index found.
CREATE FUNCTION vector_index_recall_check(
	idx regclass,
	k int DEFAULT 10,
	samples int DEFAULT 20)
RETURNS TABLE(k_out int, samples_used int, recall numeric, assessment text, suggestion text)
LANGUAGE plpgsql
AS $fn$
DECLARE
	am text;
	tbl regclass;
	col text;
	coltype text;
	nq int;
	hits numeric;
	qv text;
	plan_line text;
	index_used boolean := false;
	cur_ef int;
	cur_probes int;
BEGIN
	IF k < 1 OR k > 1000 THEN
		RAISE EXCEPTION 'k must be between 1 and 1000';
	END IF;
	IF samples < 1 OR samples > 1000 THEN
		RAISE EXCEPTION 'samples must be between 1 and 1000';
	END IF;

	SELECT am.amname, pi.indrelid, a.attname, format_type(a.atttypid, a.atttypmod)
	  INTO am, tbl, col, coltype
	  FROM pg_class ci
	  JOIN pg_am am ON am.oid = ci.relam
	  JOIN pg_index pi ON pi.indexrelid = ci.oid
	  JOIN pg_attribute a ON a.attrelid = pi.indrelid AND a.attnum = pi.indkey[0]
	 WHERE ci.oid = idx;

	IF tbl IS NULL THEN
		RAISE EXCEPTION '% is not an index', idx;
	END IF;
	IF am NOT IN ('hnsw', 'ivfflat') THEN
		RAISE EXCEPTION 'index % uses the % access method, expected hnsw or ivfflat', idx, am;
	END IF;

	IF to_regclass('pg_temp.vector_recall_q') IS NOT NULL THEN
		DROP TABLE pg_temp.vector_recall_q;
	END IF;
	IF to_regclass('pg_temp.vector_recall_exact') IS NOT NULL THEN
		DROP TABLE pg_temp.vector_recall_exact;
	END IF;
	IF to_regclass('pg_temp.vector_recall_ann') IS NOT NULL THEN
		DROP TABLE pg_temp.vector_recall_ann;
	END IF;

	-- Sample query vectors straight from the table so the types always match
	EXECUTE format(
		'CREATE TEMP TABLE vector_recall_q AS
		   SELECT row_number() OVER () AS rn, q.v AS qv
		   FROM (SELECT %I AS v FROM %s ORDER BY random() LIMIT %s) q',
		col, tbl, samples);
	GET DIAGNOSTICS nq = ROW_COUNT;

	IF nq = 0 THEN
		RAISE EXCEPTION 'table % is empty', tbl;
	END IF;

	-- Exact nearest neighbours: the ground truth
	EXECUTE format(
		'CREATE TEMP TABLE vector_recall_exact AS
		   SELECT q.rn, t.ctid AS tid
		   FROM vector_recall_q q
		   CROSS JOIN LATERAL
		     (SELECT ctid FROM %s ORDER BY %I <-> q.qv LIMIT %s) t',
		tbl, col, k);

	-- The same queries through the index
	PERFORM set_config('enable_seqscan', 'off', true);

	-- Make sure the planner really can use the index for this query shape,
	-- otherwise the recall below would silently be measured against yet
	-- another sequential scan
	SELECT q.qv::text INTO qv FROM vector_recall_q q WHERE q.rn = 1;
	FOR plan_line IN
		EXECUTE format('EXPLAIN SELECT ctid FROM %s ORDER BY %I <-> %L::%s LIMIT %s',
					   tbl, col, qv, coltype, k)
	LOOP
		IF plan_line ILIKE '%index%scan%' THEN
			index_used := true;
		END IF;
	END LOOP;

	IF NOT index_used THEN
		PERFORM set_config('enable_seqscan', 'on', true);
		RAISE EXCEPTION 'the planner cannot use index % for this query shape', idx;
	END IF;

	EXECUTE format(
		'CREATE TEMP TABLE vector_recall_ann AS
		   SELECT q.rn, t.ctid AS tid
		   FROM vector_recall_q q
		   CROSS JOIN LATERAL
		     (SELECT ctid FROM %s ORDER BY %I <-> q.qv LIMIT %s) t',
		tbl, col, k);
	PERFORM set_config('enable_seqscan', 'on', true);

	SELECT count(*)
	  INTO hits
	  FROM vector_recall_ann a
	  JOIN vector_recall_exact e ON e.rn = a.rn AND e.tid = a.tid;

	recall := round(hits::numeric / (nq * k), 4);

	cur_ef := coalesce(nullif(current_setting('hnsw.ef_search', true), '')::int, 40);
	cur_probes := coalesce(nullif(current_setting('ivfflat.probes', true), '')::int, 1);

	IF recall >= 0.95 THEN
		assessment := 'good';
		suggestion := 'Recall looks healthy for this workload.';
	ELSIF recall >= 0.90 THEN
		assessment := 'acceptable';
		suggestion := CASE am
			WHEN 'hnsw' THEN format('If higher recall is needed, raise the search width with SET hnsw.ef_search = %s (currently %s).',
									greatest(cur_ef * 2, 100), cur_ef)
			ELSE format('If higher recall is needed, search more lists with SET ivfflat.probes = %s (currently %s).',
						greatest(cur_probes * 2, 2), cur_probes)
		END;
	ELSE
		assessment := 'low';
		suggestion := CASE am
			WHEN 'hnsw' THEN format('Recall is low. Raise the search width with SET hnsw.ef_search = %s (currently %s) and re-run this check; if it stays low, rebuild the index with a larger m.',
									greatest(cur_ef * 4, 40), cur_ef)
			ELSE format('Recall is low. Search more lists with SET ivfflat.probes = %s (currently %s) and re-run this check; if it stays low, rebuild the index with a different lists value.',
						greatest(cur_probes * 4, 10), cur_probes)
		END;
	END IF;

	DROP TABLE pg_temp.vector_recall_q;
	DROP TABLE pg_temp.vector_recall_exact;
	DROP TABLE pg_temp.vector_recall_ann;

	RETURN QUERY SELECT k, nq, recall, assessment, suggestion;
END;
$fn$;

COMMENT ON FUNCTION vector_index_recall_check(regclass, int, int) IS
	'measure the recall of a vector index against exact search';

-- One entry point that walks the common reasons a vector index ends up slow,
-- memory hungry or low on recall, and says what to do about each one.
CREATE FUNCTION vector_index_diagnose(idx regclass, measure_recall boolean DEFAULT false, k int DEFAULT 10, samples int DEFAULT 20)
RETURNS TABLE(aspect text, severity text, detail text, suggestion text)
LANGUAGE plpgsql
STABLE
AS $fn$
DECLARE
	am text;
	tbl regclass;
	col text;
	dims int;
	rows_est double precision;
	m int;
	ef_con int;
	lists int;
	ef_search int;
	probes int;
	est bigint;
	suggested text;
	work_mem_kb bigint;
	ideal_lists double precision;
BEGIN
	SELECT am.amname, pi.indrelid, a.attname, a.atttypmod
	  INTO am, tbl, col, dims
	  FROM pg_class ci
	  JOIN pg_am am ON am.oid = ci.relam
	  JOIN pg_index pi ON pi.indexrelid = ci.oid
	  JOIN pg_attribute a ON a.attrelid = pi.indrelid AND a.attnum = pi.indkey[0]
	 WHERE ci.oid = idx;

	IF tbl IS NULL THEN
		RAISE EXCEPTION '% is not an index', idx;
	END IF;
	IF am NOT IN ('hnsw', 'ivfflat') THEN
		RAISE EXCEPTION 'index % uses the % access method, expected hnsw or ivfflat', idx, am;
	END IF;

	SELECT c.reltuples
	  INTO rows_est
	  FROM pg_class c
	 WHERE c.oid = tbl;

	SELECT
	  coalesce((SELECT split_part(opt, '=', 2)::int FROM unnest(c.reloptions) opt WHERE opt LIKE 'm=%'), 16),
	  coalesce((SELECT split_part(opt, '=', 2)::int FROM unnest(c.reloptions) opt WHERE opt LIKE 'ef_construction=%'), 64),
	  coalesce((SELECT split_part(opt, '=', 2)::int FROM unnest(c.reloptions) opt WHERE opt LIKE 'lists=%'), 100)
	  INTO m, ef_con, lists
	  FROM pg_class c
	 WHERE c.oid = idx;

	ef_search := coalesce(nullif(current_setting('hnsw.ef_search', true), '')::int, 40);
	probes := coalesce(nullif(current_setting('ivfflat.probes', true), '')::int, 1);

	IF rows_est < 0 THEN
		RETURN QUERY SELECT 'statistics', 'warning',
			'the row count of the table is unknown',
			'Run ANALYZE on the table, so this check and the planner can work with a real row count.';
	END IF;

	IF rows_est >= 0 AND rows_est < 1000 THEN
		RETURN QUERY SELECT 'table size', 'info',
			format('the table has about %s rows', rows_est::bigint),
			'Tables this small are usually served just as well by an exact sequential scan; the index may not pay off.';
	END IF;

	IF am = 'hnsw' THEN
		IF rows_est >= 100000 AND m < 8 THEN
			RETURN QUERY SELECT 'm', 'warning',
				format('m = %s with about %s rows: few connections per node limit how well the graph connects', m, rows_est::bigint),
				'Rebuild with a larger m, for example CREATE INDEX ... WITH (m = 32); more connections improve recall at the cost of build time and memory.';
		END IF;

		IF rows_est >= 100000 AND ef_con < 64 THEN
			RETURN QUERY SELECT 'ef_construction', 'warning',
				format('ef_construction = %s: the candidate list during the build is short', ef_con),
				'Rebuild with a larger ef_construction (64 to 800 is the usual range) so the build picks better neighbours.';
		END IF;

		IF ef_search < 10 THEN
			RETURN QUERY SELECT 'ef_search', 'warning',
				format('hnsw.ef_search = %s in this session: the search stops very early', ef_search),
				format('Raise it with SET hnsw.ef_search = %s and measure the effect with vector_index_recall_check(%s).',
					   greatest(ef_search * 4, 40), idx);
		END IF;

		IF dims > 1000 THEN
			RETURN QUERY SELECT 'dimensions', 'info',
				format('the column has %s dimensions', dims),
				'High dimensions make distance calculations and the graph itself expensive; consider reducing the dimensionality, or the halfvec type to halve memory.';
		END IF;

		IF rows_est > 0 THEN
			SELECT estimated_bytes, suggested_work_mem
			  INTO est, suggested
			  FROM vector_index_memory_estimate(rows_est::bigint, dims, m);

			work_mem_kb := pg_size_bytes(current_setting('maintenance_work_mem')) / 1024;

			IF est > work_mem_kb * 1024 THEN
				RETURN QUERY SELECT 'memory', 'warning',
					format('rebuilding this index is estimated to need %s, maintenance_work_mem is %s kB',
						   pg_size_pretty(est), work_mem_kb),
					format('Set maintenance_work_mem to at least %s before rebuilding, otherwise the graph is materialized to disk and the build becomes much slower.', suggested);
			ELSE
				RETURN QUERY SELECT 'memory', 'ok',
					format('rebuilding this index is estimated to need %s, maintenance_work_mem is %s kB',
						   pg_size_pretty(est), work_mem_kb),
					'No change needed.';
			END IF;
		END IF;
	ELSE
		IF rows_est > 0 THEN
			ideal_lists := CASE
				WHEN rows_est <= 1000000 THEN rows_est / 1000.0
				ELSE sqrt(rows_est)
			END;

			IF ideal_lists >= 1 AND (lists > 4 * ideal_lists OR lists < ideal_lists / 4) THEN
				RETURN QUERY SELECT 'lists', 'warning',
					format('lists = %s with about %s rows, a value near %s would be more typical',
						   lists, rows_est::bigint, round(ideal_lists)),
					'Rebuild with the suggested value; too few lists hurt recall while too many make each query visit more centres than necessary.';
			END IF;

			IF probes = 1 AND lists > 10 THEN
				RETURN QUERY SELECT 'probes', 'warning',
					'ivfflat.probes = 1 in this session: only one list is searched per query',
					format('Raise it with SET ivfflat.probes = %s and measure the effect with vector_index_recall_check(%s).',
						   greatest(lists / 10, 2), idx);
			ELSIF probes >= lists THEN
				RETURN QUERY SELECT 'probes', 'info',
					format('ivfflat.probes = %s with lists = %s: every list is searched', probes, lists),
					'This scans the whole index; an exact scan may be simpler and just as fast.';
			END IF;
		END IF;

		IF dims > 1000 THEN
			RETURN QUERY SELECT 'dimensions', 'info',
				format('the column has %s dimensions', dims),
				'High dimensions make distance calculations expensive; consider reducing the dimensionality, or the halfvec type to halve memory.';
		END IF;
	END IF;

	IF measure_recall THEN
		RETURN QUERY
		SELECT 'recall (measured)',
		       CASE r.assessment
		           WHEN 'good' THEN 'ok'
		           WHEN 'acceptable' THEN 'info'
		           ELSE 'warning'
		       END,
		       format('recall@%s = %s over %s sample queries (%s)',
		              k, r.recall, r.samples_used, r.assessment),
		       r.suggestion
		FROM vector_index_recall_check(idx, k, samples) r;
	ELSE
		RETURN QUERY SELECT 'recall', 'info',
			'recall has not been measured',
			format('Pass measure_recall => true to measure it in the same call, or run vector_index_recall_check(%s) on its own.', idx);
	END IF;
END;
$fn$;

COMMENT ON FUNCTION vector_index_diagnose(regclass, boolean, int, int) IS
	'check a vector index for the common causes of slow builds, high memory and low recall';

-- vector opclasses

CREATE OPERATOR CLASS vector_ops
	DEFAULT FOR TYPE vector USING btree AS
	OPERATOR 1 < ,
	OPERATOR 2 <= ,
	OPERATOR 3 = ,
	OPERATOR 4 >= ,
	OPERATOR 5 > ,
	FUNCTION 1 vector_cmp(vector, vector);

CREATE OPERATOR CLASS vector_l2_ops
	DEFAULT FOR TYPE vector USING ivfflat AS
	OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_l2_squared_distance(vector, vector),
	FUNCTION 3 l2_distance(vector, vector);

CREATE OPERATOR CLASS vector_ip_ops
	FOR TYPE vector USING ivfflat AS
	OPERATOR 1 <#> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_negative_inner_product(vector, vector),
	FUNCTION 3 vector_spherical_distance(vector, vector),
	FUNCTION 4 vector_norm(vector);

CREATE OPERATOR CLASS vector_cosine_ops
	FOR TYPE vector USING ivfflat AS
	OPERATOR 1 <=> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_negative_inner_product(vector, vector),
	FUNCTION 2 vector_norm(vector),
	FUNCTION 3 vector_spherical_distance(vector, vector),
	FUNCTION 4 vector_norm(vector);

CREATE OPERATOR CLASS vector_l2_ops
	FOR TYPE vector USING hnsw AS
	OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_l2_squared_distance(vector, vector);

CREATE OPERATOR CLASS vector_ip_ops
	FOR TYPE vector USING hnsw AS
	OPERATOR 1 <#> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_negative_inner_product(vector, vector);

CREATE OPERATOR CLASS vector_cosine_ops
	FOR TYPE vector USING hnsw AS
	OPERATOR 1 <=> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_negative_inner_product(vector, vector),
	FUNCTION 2 vector_norm(vector);

CREATE OPERATOR CLASS vector_l1_ops
	FOR TYPE vector USING hnsw AS
	OPERATOR 1 <+> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 l1_distance(vector, vector);

-- halfvec type

CREATE TYPE halfvec;

CREATE FUNCTION halfvec_in(cstring, oid, integer) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_out(halfvec) RETURNS cstring
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_typmod_in(cstring[]) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_recv(internal, oid, integer) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_send(halfvec) RETURNS bytea
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE halfvec (
	INPUT     = halfvec_in,
	OUTPUT    = halfvec_out,
	TYPMOD_IN = halfvec_typmod_in,
	RECEIVE   = halfvec_recv,
	SEND      = halfvec_send,
	STORAGE   = external
);

-- halfvec functions

CREATE FUNCTION l2_distance(halfvec, halfvec) RETURNS float8
	AS 'MODULE_PATHNAME', 'halfvec_l2_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION inner_product(halfvec, halfvec) RETURNS float8
	AS 'MODULE_PATHNAME', 'halfvec_inner_product' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION cosine_distance(halfvec, halfvec) RETURNS float8
	AS 'MODULE_PATHNAME', 'halfvec_cosine_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION l1_distance(halfvec, halfvec) RETURNS float8
	AS 'MODULE_PATHNAME', 'halfvec_l1_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_dims(halfvec) RETURNS integer
	AS 'MODULE_PATHNAME', 'halfvec_vector_dims' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION l2_norm(halfvec) RETURNS float8
	AS 'MODULE_PATHNAME', 'halfvec_l2_norm' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION l2_normalize(halfvec) RETURNS halfvec
	AS 'MODULE_PATHNAME', 'halfvec_l2_normalize' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION binary_quantize(halfvec) RETURNS bit
	AS 'MODULE_PATHNAME', 'halfvec_binary_quantize' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION subvector(halfvec, int, int) RETURNS halfvec
	AS 'MODULE_PATHNAME', 'halfvec_subvector' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- halfvec private functions

CREATE FUNCTION halfvec_add(halfvec, halfvec) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_sub(halfvec, halfvec) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_mul(halfvec, halfvec) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_concat(halfvec, halfvec) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_lt(halfvec, halfvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_le(halfvec, halfvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_eq(halfvec, halfvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_ne(halfvec, halfvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_ge(halfvec, halfvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_gt(halfvec, halfvec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_cmp(halfvec, halfvec) RETURNS int4
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_l2_squared_distance(halfvec, halfvec) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_negative_inner_product(halfvec, halfvec) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_spherical_distance(halfvec, halfvec) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_accum(double precision[], halfvec) RETURNS double precision[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_avg(double precision[]) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_combine(double precision[], double precision[]) RETURNS double precision[]
	AS 'MODULE_PATHNAME', 'vector_combine' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- halfvec aggregates

CREATE AGGREGATE avg(halfvec) (
	SFUNC = halfvec_accum,
	STYPE = double precision[],
	FINALFUNC = halfvec_avg,
	COMBINEFUNC = halfvec_combine,
	INITCOND = '{0}',
	PARALLEL = SAFE
);

CREATE AGGREGATE sum(halfvec) (
	SFUNC = halfvec_add,
	STYPE = halfvec,
	COMBINEFUNC = halfvec_add,
	PARALLEL = SAFE
);

-- halfvec cast functions

CREATE FUNCTION halfvec(halfvec, integer, boolean) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_to_vector(halfvec, integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_to_halfvec(vector, integer, boolean) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_halfvec(integer[], integer, boolean) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_halfvec(real[], integer, boolean) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_halfvec(double precision[], integer, boolean) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_halfvec(numeric[], integer, boolean) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_to_float4(halfvec, integer, boolean) RETURNS real[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- halfvec casts

CREATE CAST (halfvec AS halfvec)
	WITH FUNCTION halfvec(halfvec, integer, boolean) AS IMPLICIT;

CREATE CAST (halfvec AS vector)
	WITH FUNCTION halfvec_to_vector(halfvec, integer, boolean) AS ASSIGNMENT;

CREATE CAST (vector AS halfvec)
	WITH FUNCTION vector_to_halfvec(vector, integer, boolean) AS IMPLICIT;

CREATE CAST (halfvec AS real[])
	WITH FUNCTION halfvec_to_float4(halfvec, integer, boolean) AS ASSIGNMENT;

CREATE CAST (integer[] AS halfvec)
	WITH FUNCTION array_to_halfvec(integer[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (real[] AS halfvec)
	WITH FUNCTION array_to_halfvec(real[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (double precision[] AS halfvec)
	WITH FUNCTION array_to_halfvec(double precision[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (numeric[] AS halfvec)
	WITH FUNCTION array_to_halfvec(numeric[], integer, boolean) AS ASSIGNMENT;

-- halfvec operators

CREATE OPERATOR <-> (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = l2_distance,
	COMMUTATOR = '<->'
);

CREATE OPERATOR <#> (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_negative_inner_product,
	COMMUTATOR = '<#>'
);

CREATE OPERATOR <=> (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = cosine_distance,
	COMMUTATOR = '<=>'
);

CREATE OPERATOR <+> (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = l1_distance,
	COMMUTATOR = '<+>'
);

CREATE OPERATOR + (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_add,
	COMMUTATOR = +
);

CREATE OPERATOR - (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_sub
);

CREATE OPERATOR * (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_mul,
	COMMUTATOR = *
);

CREATE OPERATOR || (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_concat
);

CREATE OPERATOR < (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_lt,
	COMMUTATOR = > , NEGATOR = >= ,
	RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR <= (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_le,
	COMMUTATOR = >= , NEGATOR = > ,
	RESTRICT = scalarlesel, JOIN = scalarlejoinsel
);

CREATE OPERATOR = (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_eq,
	COMMUTATOR = = , NEGATOR = <> ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

CREATE OPERATOR <> (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_ne,
	COMMUTATOR = <> , NEGATOR = = ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

CREATE OPERATOR >= (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_ge,
	COMMUTATOR = <= , NEGATOR = < ,
	RESTRICT = scalargesel, JOIN = scalargejoinsel
);

CREATE OPERATOR > (
	LEFTARG = halfvec, RIGHTARG = halfvec, PROCEDURE = halfvec_gt,
	COMMUTATOR = < , NEGATOR = <= ,
	RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

-- halfvec opclasses

CREATE OPERATOR CLASS halfvec_ops
	DEFAULT FOR TYPE halfvec USING btree AS
	OPERATOR 1 < ,
	OPERATOR 2 <= ,
	OPERATOR 3 = ,
	OPERATOR 4 >= ,
	OPERATOR 5 > ,
	FUNCTION 1 halfvec_cmp(halfvec, halfvec);

CREATE OPERATOR CLASS halfvec_l2_ops
	FOR TYPE halfvec USING ivfflat AS
	OPERATOR 1 <-> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 halfvec_l2_squared_distance(halfvec, halfvec),
	FUNCTION 3 l2_distance(halfvec, halfvec),
	FUNCTION 5 ivfflat_halfvec_support(internal);

CREATE OPERATOR CLASS halfvec_ip_ops
	FOR TYPE halfvec USING ivfflat AS
	OPERATOR 1 <#> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 halfvec_negative_inner_product(halfvec, halfvec),
	FUNCTION 3 halfvec_spherical_distance(halfvec, halfvec),
	FUNCTION 4 l2_norm(halfvec),
	FUNCTION 5 ivfflat_halfvec_support(internal);

CREATE OPERATOR CLASS halfvec_cosine_ops
	FOR TYPE halfvec USING ivfflat AS
	OPERATOR 1 <=> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 halfvec_negative_inner_product(halfvec, halfvec),
	FUNCTION 2 l2_norm(halfvec),
	FUNCTION 3 halfvec_spherical_distance(halfvec, halfvec),
	FUNCTION 4 l2_norm(halfvec),
	FUNCTION 5 ivfflat_halfvec_support(internal);

CREATE OPERATOR CLASS halfvec_l2_ops
	FOR TYPE halfvec USING hnsw AS
	OPERATOR 1 <-> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 halfvec_l2_squared_distance(halfvec, halfvec),
	FUNCTION 3 hnsw_halfvec_support(internal);

CREATE OPERATOR CLASS halfvec_ip_ops
	FOR TYPE halfvec USING hnsw AS
	OPERATOR 1 <#> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 halfvec_negative_inner_product(halfvec, halfvec),
	FUNCTION 3 hnsw_halfvec_support(internal);

CREATE OPERATOR CLASS halfvec_cosine_ops
	FOR TYPE halfvec USING hnsw AS
	OPERATOR 1 <=> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 halfvec_negative_inner_product(halfvec, halfvec),
	FUNCTION 2 l2_norm(halfvec),
	FUNCTION 3 hnsw_halfvec_support(internal);

CREATE OPERATOR CLASS halfvec_l1_ops
	FOR TYPE halfvec USING hnsw AS
	OPERATOR 1 <+> (halfvec, halfvec) FOR ORDER BY float_ops,
	FUNCTION 1 l1_distance(halfvec, halfvec),
	FUNCTION 3 hnsw_halfvec_support(internal);

-- bit functions

CREATE FUNCTION hamming_distance(bit, bit) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION jaccard_distance(bit, bit) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- bit operators

CREATE OPERATOR <~> (
	LEFTARG = bit, RIGHTARG = bit, PROCEDURE = hamming_distance,
	COMMUTATOR = '<~>'
);

CREATE OPERATOR <%> (
	LEFTARG = bit, RIGHTARG = bit, PROCEDURE = jaccard_distance,
	COMMUTATOR = '<%>'
);

-- bit opclasses

CREATE OPERATOR CLASS bit_hamming_ops
	FOR TYPE bit USING ivfflat AS
	OPERATOR 1 <~> (bit, bit) FOR ORDER BY float_ops,
	FUNCTION 1 hamming_distance(bit, bit),
	FUNCTION 3 hamming_distance(bit, bit),
	FUNCTION 5 ivfflat_bit_support(internal);

CREATE OPERATOR CLASS bit_hamming_ops
	FOR TYPE bit USING hnsw AS
	OPERATOR 1 <~> (bit, bit) FOR ORDER BY float_ops,
	FUNCTION 1 hamming_distance(bit, bit),
	FUNCTION 3 hnsw_bit_support(internal);

CREATE OPERATOR CLASS bit_jaccard_ops
	FOR TYPE bit USING hnsw AS
	OPERATOR 1 <%> (bit, bit) FOR ORDER BY float_ops,
	FUNCTION 1 jaccard_distance(bit, bit),
	FUNCTION 3 hnsw_bit_support(internal);

--- sparsevec type

CREATE TYPE sparsevec;

CREATE FUNCTION sparsevec_in(cstring, oid, integer) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_out(sparsevec) RETURNS cstring
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_typmod_in(cstring[]) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_recv(internal, oid, integer) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_send(sparsevec) RETURNS bytea
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE sparsevec (
	INPUT     = sparsevec_in,
	OUTPUT    = sparsevec_out,
	TYPMOD_IN = sparsevec_typmod_in,
	RECEIVE   = sparsevec_recv,
	SEND      = sparsevec_send,
	STORAGE   = external
);

-- sparsevec functions

CREATE FUNCTION l2_distance(sparsevec, sparsevec) RETURNS float8
	AS 'MODULE_PATHNAME', 'sparsevec_l2_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION inner_product(sparsevec, sparsevec) RETURNS float8
	AS 'MODULE_PATHNAME', 'sparsevec_inner_product' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION cosine_distance(sparsevec, sparsevec) RETURNS float8
	AS 'MODULE_PATHNAME', 'sparsevec_cosine_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION l1_distance(sparsevec, sparsevec) RETURNS float8
	AS 'MODULE_PATHNAME', 'sparsevec_l1_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION l2_norm(sparsevec) RETURNS float8
	AS 'MODULE_PATHNAME', 'sparsevec_l2_norm' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION l2_normalize(sparsevec) RETURNS sparsevec
	AS 'MODULE_PATHNAME', 'sparsevec_l2_normalize' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- sparsevec private functions

CREATE FUNCTION sparsevec_lt(sparsevec, sparsevec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_le(sparsevec, sparsevec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_eq(sparsevec, sparsevec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_ne(sparsevec, sparsevec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_ge(sparsevec, sparsevec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_gt(sparsevec, sparsevec) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_cmp(sparsevec, sparsevec) RETURNS int4
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_l2_squared_distance(sparsevec, sparsevec) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_negative_inner_product(sparsevec, sparsevec) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- sparsevec cast functions

CREATE FUNCTION sparsevec(sparsevec, integer, boolean) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_to_sparsevec(vector, integer, boolean) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_to_vector(sparsevec, integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION halfvec_to_sparsevec(halfvec, integer, boolean) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sparsevec_to_halfvec(sparsevec, integer, boolean) RETURNS halfvec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_sparsevec(integer[], integer, boolean) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_sparsevec(real[], integer, boolean) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_sparsevec(double precision[], integer, boolean) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_sparsevec(numeric[], integer, boolean) RETURNS sparsevec
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- sparsevec casts

CREATE CAST (sparsevec AS sparsevec)
	WITH FUNCTION sparsevec(sparsevec, integer, boolean) AS IMPLICIT;

CREATE CAST (sparsevec AS vector)
	WITH FUNCTION sparsevec_to_vector(sparsevec, integer, boolean) AS ASSIGNMENT;

CREATE CAST (vector AS sparsevec)
	WITH FUNCTION vector_to_sparsevec(vector, integer, boolean) AS IMPLICIT;

CREATE CAST (sparsevec AS halfvec)
	WITH FUNCTION sparsevec_to_halfvec(sparsevec, integer, boolean) AS ASSIGNMENT;

CREATE CAST (halfvec AS sparsevec)
	WITH FUNCTION halfvec_to_sparsevec(halfvec, integer, boolean) AS IMPLICIT;

CREATE CAST (integer[] AS sparsevec)
	WITH FUNCTION array_to_sparsevec(integer[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (real[] AS sparsevec)
	WITH FUNCTION array_to_sparsevec(real[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (double precision[] AS sparsevec)
	WITH FUNCTION array_to_sparsevec(double precision[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (numeric[] AS sparsevec)
	WITH FUNCTION array_to_sparsevec(numeric[], integer, boolean) AS ASSIGNMENT;

-- sparsevec operators

CREATE OPERATOR <-> (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = l2_distance,
	COMMUTATOR = '<->'
);

CREATE OPERATOR <#> (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_negative_inner_product,
	COMMUTATOR = '<#>'
);

CREATE OPERATOR <=> (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = cosine_distance,
	COMMUTATOR = '<=>'
);

CREATE OPERATOR <+> (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = l1_distance,
	COMMUTATOR = '<+>'
);

CREATE OPERATOR < (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_lt,
	COMMUTATOR = > , NEGATOR = >= ,
	RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR <= (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_le,
	COMMUTATOR = >= , NEGATOR = > ,
	RESTRICT = scalarlesel, JOIN = scalarlejoinsel
);

CREATE OPERATOR = (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_eq,
	COMMUTATOR = = , NEGATOR = <> ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

CREATE OPERATOR <> (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_ne,
	COMMUTATOR = <> , NEGATOR = = ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

CREATE OPERATOR >= (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_ge,
	COMMUTATOR = <= , NEGATOR = < ,
	RESTRICT = scalargesel, JOIN = scalargejoinsel
);

CREATE OPERATOR > (
	LEFTARG = sparsevec, RIGHTARG = sparsevec, PROCEDURE = sparsevec_gt,
	COMMUTATOR = < , NEGATOR = <= ,
	RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

-- sparsevec opclasses

CREATE OPERATOR CLASS sparsevec_ops
	DEFAULT FOR TYPE sparsevec USING btree AS
	OPERATOR 1 < ,
	OPERATOR 2 <= ,
	OPERATOR 3 = ,
	OPERATOR 4 >= ,
	OPERATOR 5 > ,
	FUNCTION 1 sparsevec_cmp(sparsevec, sparsevec);

CREATE OPERATOR CLASS sparsevec_l2_ops
	FOR TYPE sparsevec USING hnsw AS
	OPERATOR 1 <-> (sparsevec, sparsevec) FOR ORDER BY float_ops,
	FUNCTION 1 sparsevec_l2_squared_distance(sparsevec, sparsevec),
	FUNCTION 3 hnsw_sparsevec_support(internal);

CREATE OPERATOR CLASS sparsevec_ip_ops
	FOR TYPE sparsevec USING hnsw AS
	OPERATOR 1 <#> (sparsevec, sparsevec) FOR ORDER BY float_ops,
	FUNCTION 1 sparsevec_negative_inner_product(sparsevec, sparsevec),
	FUNCTION 3 hnsw_sparsevec_support(internal);

CREATE OPERATOR CLASS sparsevec_cosine_ops
	FOR TYPE sparsevec USING hnsw AS
	OPERATOR 1 <=> (sparsevec, sparsevec) FOR ORDER BY float_ops,
	FUNCTION 1 sparsevec_negative_inner_product(sparsevec, sparsevec),
	FUNCTION 2 l2_norm(sparsevec),
	FUNCTION 3 hnsw_sparsevec_support(internal);

CREATE OPERATOR CLASS sparsevec_l1_ops
	FOR TYPE sparsevec USING hnsw AS
	OPERATOR 1 <+> (sparsevec, sparsevec) FOR ORDER BY float_ops,
	FUNCTION 1 l1_distance(sparsevec, sparsevec),
	FUNCTION 3 hnsw_sparsevec_support(internal);
