CREATE OR REPLACE FUNCTION graph_search.plan(p_spec jsonb)
RETURNS text[]
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    s jsonb := graph_search._norm(p_spec);
    qtable text := s->>'table';
    qs text := s->>'target_schema';
    p text := s->>'prefix';
    typ text := s->>'node_type';
    c1 text := s->>'c1';
    c2 text := s->>'c2';
    prop text := s->>'prop';
    cap integer := (s->>'max_depth_cap')::integer;
    directed boolean := (s->>'directed')::boolean;
    qview text := graph_search._qualified(qs, p || '_edges');
    search_fn text := graph_search._qualified(qs, p || '_search');
    cursor_fn text := graph_search._qualified(qs, p || '_cursor');
    shortest_fn text := graph_search._qualified(qs, p || '_shortest_path');
    subgraph_fn text := graph_search._qualified(qs, p || '_subgraph');
    stats_fn text := graph_search._qualified(qs, p || '_stats');
    prop_expr text;
    stmts text[] := ARRAY[]::text[];
    edge_sql text;
BEGIN
    prop_expr := CASE WHEN prop IS NULL THEN '''{}''::jsonb'
                      ELSE format('coalesce(e.%I, ''{}''::jsonb)', prop) END;
    edge_sql := format('CREATE VIEW %s AS SELECT e.%I::%s AS src, e.%I::%s AS dst, %s AS prop, coalesce((%s->>''weight'')::float8, 0) AS weight FROM %s e',
        qview, c1, typ, c2, typ, prop_expr, prop_expr, qtable);
    IF NOT directed THEN
        edge_sql := edge_sql || format(' UNION ALL SELECT e.%I::%s, e.%I::%s, %s, coalesce((%s->>''weight'')::float8, 0) FROM %s e',
            c2, typ, c1, typ, prop_expr, prop_expr, qtable);
    END IF;
    stmts := stmts || edge_sql;

    stmts := stmts || format($sql$
CREATE FUNCTION %s(root %s, depth integer DEFAULT 3, p_limit bigint DEFAULT 100, min_weight float8 DEFAULT 0)
RETURNS TABLE(o_path %s[], o_point1 %s, o_point2 %s, o_link_prop jsonb, o_depth integer)
LANGUAGE sql STABLE AS $fn$
WITH RECURSIVE walk(path, point1, point2, link_prop, d) AS (
  SELECT ARRAY[root]::%s[], NULL::%s, root, NULL::jsonb, 0
  UNION ALL
  SELECT w.path || e.dst, e.src, e.dst, e.prop, w.d + 1
  FROM walk w JOIN %s e ON e.src = w.point2
  WHERE w.d < depth AND w.d < %s AND e.weight >= min_weight
    AND NOT (e.dst = ANY(w.path))
)
SELECT path, point1, point2, link_prop, d FROM walk
WHERE d > 0 ORDER BY d, path LIMIT p_limit
$fn$
$sql$, search_fn, typ, typ, typ, typ, typ, typ, qview, cap);

    stmts := stmts || format($sql$
CREATE FUNCTION %s(root %s, curname name, depth integer DEFAULT 3, p_limit bigint DEFAULT 100, min_weight float8 DEFAULT 0)
RETURNS refcursor LANGUAGE plpgsql STABLE AS $fn$
BEGIN
  EXECUTE format('DECLARE %%I NO SCROLL CURSOR FOR SELECT * FROM %s($1,$2,$3,$4)', curname)
    USING root, depth, p_limit, min_weight;
  RETURN curname;
END
$fn$
$sql$, cursor_fn, typ, search_fn);

    stmts := stmts || format($sql$
CREATE FUNCTION %s(p1 %s, p2 %s, depth integer DEFAULT 3, p_limit bigint DEFAULT 2000000000, min_weight float8 DEFAULT 0)
RETURNS TABLE(o_path %s[], o_depth integer, o_link_props jsonb[])
LANGUAGE sql STABLE AS $fn$
WITH RECURSIVE walk(path, props, point, d) AS (
  SELECT ARRAY[p1]::%s[], ARRAY[]::jsonb[], p1, 0
  UNION ALL
  SELECT w.path || e.dst, w.props || e.prop, e.dst, w.d + 1
  FROM walk w JOIN %s e ON e.src = w.point
  WHERE w.d < depth AND w.d < %s AND e.weight >= min_weight
    AND NOT (e.dst = ANY(w.path))
)
SELECT path, d, props FROM walk WHERE point = p2 ORDER BY d, path LIMIT p_limit
$fn$
$sql$, shortest_fn, typ, typ, typ, typ, qview, cap);

    stmts := stmts || format($sql$
CREATE FUNCTION %s(root %s, depth integer DEFAULT 3, p_limit bigint DEFAULT 100, min_weight float8 DEFAULT 0)
RETURNS jsonb LANGUAGE plpgsql STABLE AS $fn$
DECLARE
  out_nodes jsonb := jsonb_build_array(jsonb_build_object('id', root));
  out_edges jsonb := '[]'::jsonb;
  seen %s[] := ARRAY[root]::%s[];
  v_node %s;
  rec record;
BEGIN
  FOR rec IN SELECT * FROM %s(root, depth, p_limit, min_weight) LOOP
    IF NOT (rec.o_point2 = ANY(seen)) THEN
      seen := seen || rec.o_point2;
      out_nodes := out_nodes || jsonb_build_array(jsonb_build_object('id', rec.o_point2));
    END IF;
    out_edges := out_edges || jsonb_build_array(jsonb_build_object(
      'source', rec.o_point1, 'target', rec.o_point2, 'prop', rec.o_link_prop));
  END LOOP;
  RETURN jsonb_build_object('root', root, 'depth', depth,
    'node_count', jsonb_array_length(out_nodes), 'edge_count', jsonb_array_length(out_edges),
    'nodes', out_nodes, 'edges', out_edges);
END
$fn$
$sql$, subgraph_fn, typ, typ, typ, typ, search_fn);

    stmts := stmts || format($sql$
CREATE FUNCTION %s(root %s, depth integer DEFAULT 3, p_limit bigint DEFAULT 100, min_weight float8 DEFAULT 0)
RETURNS TABLE(o_depth integer, o_edges bigint, o_new_nodes bigint, o_avg_path_len numeric)
LANGUAGE sql STABLE AS $fn$
WITH hits AS (SELECT * FROM %s(root, depth, p_limit, min_weight))
SELECT o_depth, count(*)::bigint, count(DISTINCT o_point2)::bigint, avg(cardinality(o_path))::numeric
FROM hits GROUP BY o_depth ORDER BY o_depth
$fn$
$sql$, stats_fn, typ, search_fn);

    stmts := stmts || format('COMMENT ON FUNCTION %s(%s,integer,bigint,double precision) IS %L',
        search_fn, typ, 'Generated by pg_graph_search; reinstall this instance to change its specification.');
    stmts := stmts || format('COMMENT ON FUNCTION %s(%s,name,integer,bigint,double precision) IS %L',
        cursor_fn, typ, 'Generated by pg_graph_search; reinstall this instance to change its specification.');
    stmts := stmts || format('COMMENT ON FUNCTION %s(%s,%s,integer,bigint,double precision) IS %L',
        shortest_fn, typ, typ, 'Generated by pg_graph_search; reinstall this instance to change its specification.');
    stmts := stmts || format('COMMENT ON FUNCTION %s(%s,integer,bigint,double precision) IS %L',
        subgraph_fn, typ, 'Generated by pg_graph_search; reinstall this instance to change its specification.');
    stmts := stmts || format('COMMENT ON FUNCTION %s(%s,integer,bigint,double precision) IS %L',
        stats_fn, typ, 'Generated by pg_graph_search; reinstall this instance to change its specification.');
    RETURN stmts;
END;
$$;

