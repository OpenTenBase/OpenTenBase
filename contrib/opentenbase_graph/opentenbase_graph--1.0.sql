/* contrib/opentenbase_graph/opentenbase_graph--1.0.sql */

-- ============================================================================
-- opentenbase_graph: lightweight graph traversal templates
-- Compatible with PostgreSQL 10+ (OpenTenBase fork is PG10-based)

CREATE SCHEMA IF NOT EXISTS opentenbase_graph;
--
-- Design notes (see doc/opentenbase_graph.md / doc/opentenbase_graph_zh.md):
--   * Application layer creates the nodes / edges tables (any schema).
--   * These functions operate on user-provided table/column names via
--     regclass + text parameters, with identifier whitelisting for safety.
--   * Cycle prevention uses a visited[] array (PG10 does not have CYCLE).
--   * No C code: pure PL/pgSQL, zero additional compile cost.
--   * Functions are STABLE (read-only, single query) so they can be inlined
--     into larger queries.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. bfs: breadth-first traversal from start_id up to max_depth hops
-- Returns (depth, node) for every visited node including the start itself.
-- ----------------------------------------------------------------------------
CREATE FUNCTION opentenbase_graph.bfs(
    nodes_table regclass,
    edges_table regclass,
    src_col     text,
    dst_col     text,
    start_id    bigint,
    max_depth   int DEFAULT 5
)
RETURNS TABLE(depth int, node bigint)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    qry text;
BEGIN
    IF nodes_table IS NULL OR edges_table IS NULL THEN
        RAISE EXCEPTION 'nodes_table and edges_table must not be null';
    END IF;
    IF max_depth < 0 OR max_depth > 1000 THEN
        RAISE EXCEPTION 'max_depth must be between 0 and 1000';
    END IF;
    IF src_col IS NULL OR dst_col IS NULL THEN
        RAISE EXCEPTION 'src_col and dst_col must not be null';
    END IF;
    IF src_col !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RAISE EXCEPTION 'invalid src_col identifier: %', src_col;
    END IF;
    IF dst_col !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RAISE EXCEPTION 'invalid dst_col identifier: %', dst_col;
    END IF;

    qry := format(
        'WITH RECURSIVE walk(n, d, v) AS (
            SELECT $1::bigint, 0, ARRAY[$1]::bigint[]
            UNION ALL
            SELECT e.%I, w.d + 1, w.v || e.%I
            FROM walk w
            JOIN %s e ON e.%I = w.n
            WHERE w.d < $2
              AND NOT (e.%I = ANY(w.v))
        )
        SELECT d, n FROM walk ORDER BY d, n',
        dst_col, dst_col, edges_table::text, src_col, dst_col
    );
    RETURN QUERY EXECUTE qry USING start_id, max_depth;
END;
$$;

-- ----------------------------------------------------------------------------
-- 2. shortest_path: unweighted shortest path from start_id to end_id
-- Returns the depth (hop count) and the full path as a bigint[].
-- Returns 0 rows if no path found within max_depth.
-- ----------------------------------------------------------------------------
CREATE FUNCTION opentenbase_graph.shortest_path(
    nodes_table regclass,
    edges_table regclass,
    src_col     text,
    dst_col     text,
    start_id    bigint,
    end_id      bigint,
    max_depth   int DEFAULT 1000
)
RETURNS TABLE(depth int, path bigint[])
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    qry text;
BEGIN
    IF nodes_table IS NULL OR edges_table IS NULL THEN
        RAISE EXCEPTION 'nodes_table and edges_table must not be null';
    END IF;
    IF max_depth < 1 OR max_depth > 1000 THEN
        RAISE EXCEPTION 'max_depth must be between 1 and 1000';
    END IF;
    IF src_col !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RAISE EXCEPTION 'invalid src_col identifier: %', src_col;
    END IF;
    IF dst_col !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RAISE EXCEPTION 'invalid dst_col identifier: %', dst_col;
    END IF;

    qry := format(
        'WITH RECURSIVE walk(n, d, p, v) AS (
            SELECT $1::bigint, 0,
                   ARRAY[$1]::bigint[],
                   ARRAY[$1]::bigint[]
            UNION ALL
            SELECT e.%I, w.d + 1,
                   w.p || e.%I,
                   w.v || e.%I
            FROM walk w
            JOIN %s e ON e.%I = w.n
            WHERE w.d < $2
              AND NOT (e.%I = ANY(w.v))
        )
        SELECT d, p FROM walk WHERE n = $3 ORDER BY d LIMIT 1',
        dst_col, dst_col, dst_col, edges_table::text, src_col, dst_col
    );
    RETURN QUERY EXECUTE qry USING start_id, max_depth, end_id;
END;
$$;

-- ----------------------------------------------------------------------------
-- 3. degree: in-degree / out-degree of a single node
-- in_degree  = number of edges whose dst = node_id
-- out_degree = number of edges whose src = node_id
-- ----------------------------------------------------------------------------
CREATE FUNCTION opentenbase_graph.degree(
    edges_table regclass,
    src_col     text,
    dst_col     text,
    node_id     bigint
)
RETURNS TABLE(in_degree bigint, out_degree bigint)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    qry text;
BEGIN
    IF edges_table IS NULL THEN
        RAISE EXCEPTION 'edges_table must not be null';
    END IF;
    IF src_col !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RAISE EXCEPTION 'invalid src_col identifier: %', src_col;
    END IF;
    IF dst_col !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RAISE EXCEPTION 'invalid dst_col identifier: %', dst_col;
    END IF;

    qry := format(
        'SELECT
            (SELECT count(*) FROM %s WHERE %I = $1)::bigint AS in_degree,
            (SELECT count(*) FROM %s WHERE %I = $1)::bigint AS out_degree',
        edges_table::text, dst_col,
        edges_table::text, src_col
    );
    RETURN QUERY EXECUTE qry USING node_id;
END;
$$;

-- ----------------------------------------------------------------------------
-- 4. reachable: boolean reachability check from start_id to end_id
-- Cheaper than shortest_path when only yes/no answer is needed.
-- ----------------------------------------------------------------------------
CREATE FUNCTION opentenbase_graph.reachable(
    nodes_table regclass,
    edges_table regclass,
    src_col     text,
    dst_col     text,
    start_id    bigint,
    end_id      bigint,
    max_depth   int DEFAULT 1000
)
RETURNS bool
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    qry text;
    hit int;
BEGIN
    IF nodes_table IS NULL OR edges_table IS NULL THEN
        RAISE EXCEPTION 'nodes_table and edges_table must not be null';
    END IF;
    IF max_depth < 1 OR max_depth > 1000 THEN
        RAISE EXCEPTION 'max_depth must be between 1 and 1000';
    END IF;
    IF src_col !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RAISE EXCEPTION 'invalid src_col identifier: %', src_col;
    END IF;
    IF dst_col !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RAISE EXCEPTION 'invalid dst_col identifier: %', dst_col;
    END IF;

    qry := format(
        'WITH RECURSIVE walk(n, d, v) AS (
            SELECT $1::bigint, 0, ARRAY[$1]::bigint[]
            UNION ALL
            SELECT e.%I, w.d + 1, w.v || e.%I
            FROM walk w
            JOIN %s e ON e.%I = w.n
            WHERE w.d < $2
              AND NOT (e.%I = ANY(w.v))
        )
        SELECT 1 FROM walk WHERE n = $3 LIMIT 1',
        dst_col, dst_col, edges_table::text, src_col, dst_col
    );
    EXECUTE qry INTO hit USING start_id, max_depth, end_id;
    RETURN hit IS NOT NULL;
END;
$$;