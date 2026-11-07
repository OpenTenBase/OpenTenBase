-- pg_graph_search--1.0.0.sql
-- Generated from ddl/*.sql. Do not edit this assembled file directly.
--
-- A pure-SQL property-graph layer for OpenTenBase/PostgreSQL 10.  The
-- extension stores only instance metadata; each installed instance generates
-- a view and five schema-qualified application functions.

CREATE TABLE IF NOT EXISTS graph_search.registry (
    target_schema text NOT NULL,
    prefix text NOT NULL,
    spec jsonb NOT NULL,
    node_type regtype NOT NULL,
    installed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (target_schema, prefix)
) DISTRIBUTE BY REPLICATION;

COMMENT ON TABLE graph_search.registry IS
    'Installed pg_graph_search instances. This table is replicated intentionally.';

CREATE OR REPLACE FUNCTION graph_search._qualified(p_schema text, p_name text)
RETURNS text
LANGUAGE sql
IMMUTABLE
STRICT
AS $$ SELECT format('%I.%I', p_schema, p_name) $$;

CREATE OR REPLACE FUNCTION graph_search._norm(p_spec jsonb)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
STRICT
AS $$
DECLARE
    v_table regclass;
    v_c1 text;
    v_c2 text;
    v_prop text;
    v_schema text;
    v_prefix text;
    v_directed boolean;
    v_cap integer;
    v_c1_type oid;
    v_c2_type oid;
    v_type_sql text;
BEGIN
    IF jsonb_typeof(p_spec) <> 'object' THEN
        RAISE EXCEPTION 'spec must be a JSON object';
    END IF;
    IF coalesce(p_spec->>'table', '') = '' THEN
        RAISE EXCEPTION 'spec.table is required';
    END IF;
    v_table := to_regclass(p_spec->>'table');
    IF v_table IS NULL THEN
        RAISE EXCEPTION 'edge table % does not exist', p_spec->>'table';
    END IF;
    v_c1 := coalesce(p_spec->>'c1', 'c1');
    v_c2 := coalesce(p_spec->>'c2', 'c2');
    v_prop := nullif(p_spec->>'prop', '');
    v_schema := coalesce(nullif(p_spec->>'target_schema', ''), 'public');
    v_prefix := coalesce(nullif(p_spec->>'prefix', ''), 'graph');
    v_directed := coalesce((p_spec->>'directed')::boolean, false);
    v_cap := coalesce((p_spec->>'max_depth_cap')::integer, 64);

    IF v_prefix !~ '^[a-z_][a-z0-9_]*$' THEN
        RAISE EXCEPTION 'prefix must match ^[a-z_][a-z0-9_]*$: %', v_prefix;
    END IF;
    IF v_cap < 0 OR v_cap > 256 THEN
        RAISE EXCEPTION 'max_depth_cap must be between 0 and 256';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = v_schema) THEN
        RAISE EXCEPTION 'target schema % does not exist', v_schema;
    END IF;

    SELECT atttypid INTO v_c1_type
      FROM pg_attribute WHERE attrelid = v_table AND attname = v_c1
       AND attnum > 0 AND NOT attisdropped;
    SELECT atttypid INTO v_c2_type
      FROM pg_attribute WHERE attrelid = v_table AND attname = v_c2
       AND attnum > 0 AND NOT attisdropped;
    IF v_c1_type IS NULL OR v_c2_type IS NULL THEN
        RAISE EXCEPTION 'edge columns %.% and %.% must exist', v_table::text, v_c1, v_table::text, v_c2;
    END IF;
    IF v_c1_type <> v_c2_type THEN
        RAISE EXCEPTION 'edge endpoint columns must have the same type';
    END IF;
    IF v_c1_type NOT IN ('int2'::regtype, 'int4'::regtype, 'int8'::regtype,
                         'text'::regtype, 'varchar'::regtype, 'bpchar'::regtype,
                         'name'::regtype, 'uuid'::regtype, 'numeric'::regtype) THEN
        RAISE EXCEPTION 'unsupported endpoint type: %', format_type(v_c1_type, NULL);
    END IF;
    IF v_prop IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM pg_attribute WHERE attrelid = v_table AND attname = v_prop
          AND attnum > 0 AND NOT attisdropped) THEN
        RAISE EXCEPTION 'property column %.% does not exist', v_table::text, v_prop;
    END IF;
    IF v_prop IS NOT NULL AND (SELECT atttypid FROM pg_attribute
        WHERE attrelid = v_table AND attname = v_prop AND attnum > 0) <> 'jsonb'::regtype THEN
        RAISE EXCEPTION 'property column must be jsonb';
    END IF;

    v_type_sql := CASE WHEN v_c1_type IN ('varchar'::regtype, 'bpchar'::regtype, 'name'::regtype)
                       THEN 'text' ELSE format_type(v_c1_type, NULL) END;
    RETURN jsonb_build_object(
        'table', v_table::text, 'c1', v_c1, 'c2', v_c2, 'prop', v_prop,
        'target_schema', v_schema, 'prefix', v_prefix, 'directed', v_directed,
        'max_depth_cap', v_cap, 'node_type', v_type_sql);
END;
$$;

CREATE OR REPLACE FUNCTION graph_search.advise(p_spec jsonb)
RETURNS TABLE(severity text, message text)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE s jsonb;
BEGIN
    s := graph_search._norm(p_spec);
    RETURN QUERY SELECT 'info'::text, format('edge table: %s', s->>'table');
    RETURN QUERY SELECT 'info'::text, format('node type: %s; direction: %s', s->>'node_type',
        CASE WHEN (s->>'directed')::boolean THEN 'directed' ELSE 'undirected' END);
    IF NOT EXISTS (
        SELECT 1 FROM pg_index i WHERE i.indrelid = (s->>'table')::regclass
          AND pg_get_indexdef(i.indexrelid) ILIKE '%' || quote_ident(s->>'c1') || '%') THEN
        RETURN QUERY SELECT 'warning'::text, format('consider an index beginning with %I', s->>'c1');
    END IF;
    IF NOT (s->>'directed')::boolean THEN
        RETURN QUERY SELECT 'warning'::text, format('undirected traversal benefits from an index beginning with %I', s->>'c2');
    END IF;
END;
$$;

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

CREATE OR REPLACE FUNCTION graph_search.uninstall(p_schema text, p_prefix text, p_cascade boolean DEFAULT false)
RETURNS text[]
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    qname text;
    out text[] := ARRAY[]::text[];
    suffix text;
BEGIN
    FOREACH suffix IN ARRAY ARRAY['_cursor', '_stats', '_subgraph', '_shortest_path', '_search'] LOOP
        qname := graph_search._qualified(p_schema, p_prefix || suffix);
        -- The exact signature varies by endpoint type. Resolve all overloads
        -- from pg_proc instead of guessing a type at uninstall time.
        FOR qname IN SELECT format('%I.%I(%s)', n.nspname, p.proname,
                            pg_get_function_identity_arguments(p.oid))
          FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = p_schema AND p.proname = p_prefix || suffix LOOP
            EXECUTE 'DROP FUNCTION ' || qname || CASE WHEN p_cascade THEN ' CASCADE' ELSE '' END;
            out := out || ('DROP FUNCTION ' || qname || ';');
        END LOOP;
    END LOOP;
    qname := graph_search._qualified(p_schema, p_prefix || '_edges');
    IF to_regclass(qname) IS NOT NULL THEN
        EXECUTE 'DROP VIEW ' || qname || CASE WHEN p_cascade THEN ' CASCADE' ELSE '' END;
        out := out || ('DROP VIEW ' || qname || ';');
    END IF;
    DELETE FROM graph_search.registry WHERE target_schema = p_schema AND prefix = p_prefix;
    RETURN out;
END;
$$;

CREATE OR REPLACE FUNCTION graph_search.install(p_spec jsonb, p_replace boolean DEFAULT true)
RETURNS TABLE(ddl text)
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    s jsonb := graph_search._norm(p_spec);
    item text;
BEGIN
    IF EXISTS (SELECT 1 FROM graph_search.registry
               WHERE target_schema = s->>'target_schema' AND prefix = s->>'prefix') THEN
        IF NOT p_replace THEN
            RAISE EXCEPTION 'instance %.% already exists', s->>'target_schema', s->>'prefix';
        END IF;
        PERFORM graph_search.uninstall(s->>'target_schema', s->>'prefix');
    END IF;
    FOREACH item IN ARRAY graph_search.plan(s) LOOP
        EXECUTE item;
        ddl := item;
        RETURN NEXT;
    END LOOP;
    INSERT INTO graph_search.registry(target_schema, prefix, spec, node_type)
    VALUES (s->>'target_schema', s->>'prefix', s, (s->>'node_type')::regtype);
END;
$$;

CREATE OR REPLACE FUNCTION graph_search.list_installed()
RETURNS TABLE(target_schema text, prefix text, edge_table text, node_type regtype,
              directed boolean, max_depth_cap integer, api text[])
LANGUAGE sql
STABLE
AS $$
SELECT r.target_schema, r.prefix, r.spec->>'table', r.node_type,
       (r.spec->>'directed')::boolean, (r.spec->>'max_depth_cap')::integer,
       ARRAY[r.prefix || '_search', r.prefix || '_cursor', r.prefix || '_shortest_path',
             r.prefix || '_subgraph', r.prefix || '_stats']
FROM graph_search.registry r
ORDER BY r.target_schema, r.prefix
$$;

