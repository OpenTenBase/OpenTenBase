/* contrib/opentenbase_graph/opentenbase_graph--1.1--1.2.sql */

-- ============================================================================
-- opentenbase_graph v1.1 → v1.2 upgrade
--
-- Adds opentenbase_graph.weighted_shortest_path(): a Dijkstra-based
-- single-source single-target shortest path query that respects per-edge
-- weights (cost / distance / latency / etc.).
--
-- Differences from v1.0 opentenbase_graph.shortest_path():
--   * Edge weights: an extra weight column is required.
--   * Distance metric: cost is summed edge weights (not hop count).
--   * Budget cap: max_cost short-circuits the search once the cheapest
--     frontier entry already exceeds the budget.
--   * Returns total_cost (sum of weights), hop count, and the full path.
--
-- Implementation notes:
--   * PL/pgSQL only — same as the rest of the extension, no C code.
--   * PG10+ compatible (OpenTenBase fork baseline). No CYCLE / MERGE / SQL/JSON
--     path expressions.
--   * A session-scoped TEMP table is used as a binary-heap stand-in:
--       cost-min extraction = ORDER BY cost LIMIT 1   (indexed)
--       visited/duplicate suppression = UNIQUE INDEX on node
--     This keeps the algorithm complexity at O((V + E) log V) for the
--     indexed access path and avoids the O(V^2) worst-case of a flat array
--     scan inside PL/pgSQL.
--   * The function is VOLATILE because it creates and drops a TEMP table
--     within the same session.
-- ============================================================================

CREATE FUNCTION opentenbase_graph.weighted_shortest_path(
    edges_table regclass,
    src_col     text,
    dst_col     text,
    weight_col  text,
    start_id    bigint,
    end_id      bigint,
    max_cost    double precision DEFAULT 1e18
)
RETURNS TABLE(
    total_cost  double precision,
    hops        int,
    path        bigint[]
)
LANGUAGE plpgsql
VOLATILE
AS $func$
DECLARE
    q text;
    rec record;
    cur_cost  double precision;
    cur_node  bigint;
    cur_path  bigint[];
    cur_visit bigint[];
    n_cost    double precision;
    n_node    bigint;
    found_target boolean := false;
BEGIN
    -- ------------------------------------------------------------------------
    -- input validation
    -- ------------------------------------------------------------------------
    IF edges_table IS NULL THEN
        RAISE EXCEPTION 'edges_table must not be null';
    END IF;
    IF src_col IS NULL OR dst_col IS NULL OR weight_col IS NULL THEN
        RAISE EXCEPTION 'src_col / dst_col / weight_col must not be null';
    END IF;
    IF src_col !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RAISE EXCEPTION 'invalid src_col identifier: %', src_col;
    END IF;
    IF dst_col !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RAISE EXCEPTION 'invalid dst_col identifier: %', dst_col;
    END IF;
    IF weight_col !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RAISE EXCEPTION 'invalid weight_col identifier: %', weight_col;
    END IF;
    IF max_cost IS NULL OR max_cost <= 0 OR max_cost = 'Infinity'::double precision THEN
        RAISE EXCEPTION 'max_cost must be a finite positive number';
    END IF;

    -- ------------------------------------------------------------------------
    -- session-scoped priority queue
    -- ------------------------------------------------------------------------
    DROP TABLE IF EXISTS _wsp_frontier;
    CREATE TEMP TABLE _wsp_frontier (
        cost    double precision,
        node    bigint,
        path    bigint[],
        visited bigint[]
    ) ON COMMIT DROP;
    CREATE INDEX _wsp_frontier_cost_idx  ON _wsp_frontier (cost);
    CREATE UNIQUE INDEX _wsp_frontier_node_idx ON _wsp_frontier (node);

    INSERT INTO _wsp_frontier VALUES (0.0, start_id, ARRAY[start_id], ARRAY[start_id]);

    -- ------------------------------------------------------------------------
    -- Dijkstra main loop: always expand the cheapest frontier entry first
    -- ------------------------------------------------------------------------
    LOOP
        SELECT f.cost, f.node, f.path, f.visited
          INTO cur_cost, cur_node, cur_path, cur_visit
          FROM _wsp_frontier f
          ORDER BY f.cost
          LIMIT 1;

        IF NOT FOUND THEN
            EXIT;                       -- frontier drained, no path
        END IF;

        IF cur_node = end_id THEN
            found_target := true;
            EXIT;                       -- optimal: first pop of end_id
        END IF;

        IF cur_cost > max_cost THEN
            EXIT;                       -- even the cheapest candidate exceeds budget
        END IF;

        -- remove the popped node from the frontier before expanding
        DELETE FROM _wsp_frontier WHERE node = cur_node;

        -- expand outgoing edges of cur_node
        q := format(
            'SELECT e.%I::bigint AS dst, e.%I::double precision AS w
             FROM %s e
             WHERE e.%I = $1',
            dst_col, weight_col, edges_table::text, src_col
        );

        FOR rec IN EXECUTE q USING cur_node LOOP
            n_node := rec.dst;

            -- skip self-loops / already-visited nodes (defensive — visited
            -- array is the authoritative cycle guard, but skipping here
            -- avoids wasted temp-table writes)
            IF n_node = ANY(cur_visit) THEN
                CONTINUE;
            END IF;

            n_cost := cur_cost + rec.w;

            IF n_cost > max_cost THEN
                CONTINUE;
            END IF;

            -- duplicate suppression: if frontier already contains n_node with
            -- a cost <= n_cost, the new path is strictly worse — skip.
            PERFORM 1 FROM _wsp_frontier
                WHERE node = n_node AND cost <= n_cost;
            IF FOUND THEN
                CONTINUE;
            END IF;

            -- replace any stale frontier entry for n_node with a better one
            DELETE FROM _wsp_frontier WHERE node = n_node;

            INSERT INTO _wsp_frontier VALUES (
                n_cost,
                n_node,
                cur_path || n_node,
                cur_visit || n_node
            );
        END LOOP;
    END LOOP;

    -- ------------------------------------------------------------------------
    -- emit the result row (zero rows when no path within budget)
    -- ------------------------------------------------------------------------
    IF found_target THEN
        total_cost := cur_cost;
        hops := array_length(cur_path, 1) - 1;
        path := cur_path;
        RETURN NEXT;
    END IF;

    RETURN;
END;
$func$;
