-- pgvector 0.8.6 -> 0.8.7
--
-- OpenTenBase additions: index build progress reporting, memory
-- estimation and index diagnostics.
--
-- This file only contains what is new in 0.8.7. The full install
-- script is generated from sql/vector.sql at build time.

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
	bytes_per_dim int DEFAULT 4,
	OUT estimated_bytes bigint,
	OUT estimated_pretty text,
	OUT suggested_work_mem text)
RETURNS record
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE PARALLEL SAFE;

COMMENT ON FUNCTION vector_index_memory_estimate(bigint, int, int, int) IS
	'estimate the memory needed to build an HNSW index (bytes_per_dim: 4 for vector, 2 for halfvec)';

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
	idx_valid boolean;
BEGIN
	SELECT am.amname, pi.indrelid, a.attname, a.atttypmod, pi.indisvalid
	  INTO am, tbl, col, dims, idx_valid
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

	/*
	 * An invalid index is the one case that deserves an error: nothing below
	 * is meaningful while the index cannot be used at all. This is what a
	 * failed CREATE INDEX CONCURRENTLY leaves behind.
	 */
	IF NOT idx_valid THEN
		RETURN QUERY SELECT 'index validity', 'error',
			format('index %s is marked invalid and cannot be used for searches', idx),
			format('Rebuild it with REINDEX INDEX %s.', idx);
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

	/*
	 * Do not attempt a measurement on an invalid index: the scan would raise,
	 * and the validity row above already explains why.
	 */
	IF NOT idx_valid THEN
		RETURN QUERY SELECT 'recall (measured)', 'error',
			'not measured: the index is marked invalid',
			format('Rebuild it with REINDEX INDEX %s, then re-run.', idx);
	ELSIF measure_recall THEN
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
