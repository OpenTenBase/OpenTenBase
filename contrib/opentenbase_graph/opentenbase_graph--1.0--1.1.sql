/* contrib/opentenbase_graph/opentenbase_graph--1.0--1.1.sql */

-- ============================================================================
-- opentenbase_graph v0.2 upgrade
--
-- Adds opentenbase_graph._graph_info(): a single-row diagnostic helper that
-- reports the global shape of a user-defined edges table:
--   * node_count    = number of distinct nodes appearing as either src or dst
--   * edge_count    = number of edges in the table
--   * max_in_degree / max_out_degree / max_total_degree = degree extremes
--   * density       = edge_count / (node_count * (node_count - 1))
--                     for a directed simple graph; 0 when node_count < 2.
--
-- Implemented as PL/pgSQL because the SQL function language cannot EXECUTE
-- dynamic table/column names. The same identifier whitelisting regex used by
-- the v1.0 functions is applied here so application table/column names cannot
-- inject SQL.
-- ============================================================================

CREATE FUNCTION opentenbase_graph._graph_info(
    edges_table regclass,
    src_col     text,
    dst_col     text
)
RETURNS TABLE(
    node_count        bigint,
    edge_count        bigint,
    max_in_degree     bigint,
    max_out_degree    bigint,
    max_total_degree  bigint,
    density           double precision
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    qry   text;
    e     bigint;
    nv    bigint;
    mi    bigint;
    mo    bigint;
    mt    bigint;
    dens  double precision;
BEGIN
    IF edges_table IS NULL THEN
        RAISE EXCEPTION 'edges_table must not be null';
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

    -- edge_count + node_count in a single pass
    qry := format(
        'SELECT
            (SELECT count(*) FROM %s)::bigint,
            (SELECT count(DISTINCT n) FROM (
                SELECT %I AS n FROM %s
                UNION
                SELECT %I AS n FROM %s
             ) s)::bigint',
        edges_table::text,
        src_col, edges_table::text,
        dst_col, edges_table::text
    );
    EXECUTE qry INTO e, nv;

    -- max in-degree
    qry := format(
        'SELECT coalesce(max(c), 0)::bigint FROM (
            SELECT count(*) AS c FROM %s GROUP BY %I
         ) s',
        edges_table::text, dst_col
    );
    EXECUTE qry INTO mi;

    -- max out-degree
    qry := format(
        'SELECT coalesce(max(c), 0)::bigint FROM (
            SELECT count(*) AS c FROM %s GROUP BY %I
         ) s',
        edges_table::text, src_col
    );
    EXECUTE qry INTO mo;

    -- max total-degree
    qry := format(
        'SELECT coalesce(max(c), 0)::bigint FROM (
            SELECT count(*) AS c FROM (
                SELECT %I AS n FROM %s
                UNION ALL
                SELECT %I AS n FROM %s
            ) s GROUP BY n
         ) t',
        src_col, edges_table::text,
        dst_col, edges_table::text
    );
    EXECUTE qry INTO mt;

    IF nv < 2 THEN
        dens := 0::double precision;
    ELSE
        dens := e::double precision
              / (nv::double precision * (nv - 1)::double precision);
    END IF;

    RETURN QUERY SELECT nv, e, mi, mo, mt, dens;
END;
$$;
