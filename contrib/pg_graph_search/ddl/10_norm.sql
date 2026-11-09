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

