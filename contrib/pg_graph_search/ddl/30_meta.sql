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

