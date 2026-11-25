-- contrib/age/age--1.0.0.sql
-- Distributed Property Graph (AGE-compatible) extension for OpenTenBase
--
-- Architecture:
--   1. Metadata catalogs (ag_graph, ag_label) use DISTRIBUTE BY REPLICATION
--      so CN and all DNs share consistent graph/label definitions.
--   2. Vertex tables use DISTRIBUTE BY SHARD(id).
--   3. Edge tables use DISTRIBUTE BY SHARD(start_id) so 1-hop outgoing edge
--      traversal is co-located on the same DataNode as the source vertex.

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION age" to load this file. \quit

-- ============================================================================
-- 1. C-level GraphID Primitives (16-bit label_id | 48-bit entry_id)
-- ============================================================================
CREATE FUNCTION ag_catalog._graphid(label_id int, entry_id bigint)
RETURNS bigint
AS 'MODULE_PATHNAME', 'age_make_graphid'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION ag_catalog._label_id(gid bigint)
RETURNS int
AS 'MODULE_PATHNAME', 'age_get_label_id'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION ag_catalog._extract_id(gid bigint)
RETURNS bigint
AS 'MODULE_PATHNAME', 'age_get_entry_id'
LANGUAGE C IMMUTABLE STRICT;

-- ============================================================================
-- 2. Distributed Graph Catalog Tables (REPLICATION across all DNs)
-- ============================================================================
CREATE TABLE ag_catalog.ag_graph (
    graphid serial PRIMARY KEY,
    name name NOT NULL UNIQUE,
    namespace name NOT NULL UNIQUE
) DISTRIBUTE BY REPLICATION;

CREATE TABLE ag_catalog.ag_label (
    name name NOT NULL,
    graph integer NOT NULL,
    id integer NOT NULL,
    kind "char" NOT NULL CHECK (kind IN ('v', 'e')),
    relation name NOT NULL,
    seq_name name NOT NULL,
    PRIMARY KEY (graph, name),
    UNIQUE (graph, id)
) DISTRIBUTE BY REPLICATION;

-- ============================================================================
-- 3. Graph & Label Lifecycle Management
-- ============================================================================
CREATE OR REPLACE FUNCTION ag_catalog.create_graph(graph_name name)
RETURNS void AS $$
DECLARE
    v_gid integer;
BEGIN
    IF EXISTS (SELECT 1 FROM ag_catalog.ag_graph WHERE name = graph_name) THEN
        RAISE EXCEPTION 'graph "%" already exists', graph_name;
    END IF;

    EXECUTE format('CREATE SCHEMA %I', graph_name);

    INSERT INTO ag_catalog.ag_graph (name, namespace)
    VALUES (graph_name, graph_name)
    RETURNING graphid INTO v_gid;

    -- Create default base vertex and edge tables (sharded for distributed scale)
    PERFORM ag_catalog.create_vlabel(graph_name, '_ag_label_vertex');
    PERFORM ag_catalog.create_elabel(graph_name, '_ag_label_edge');
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ag_catalog.drop_graph(graph_name name, cascade boolean DEFAULT false)
RETURNS void AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM ag_catalog.ag_graph WHERE name = graph_name) THEN
        RAISE EXCEPTION 'graph "%" does not exist', graph_name;
    END IF;

    IF cascade THEN
        EXECUTE format('DROP SCHEMA %I CASCADE', graph_name);
    ELSE
        EXECUTE format('DROP SCHEMA %I RESTRICT', graph_name);
    END IF;

    DELETE FROM ag_catalog.ag_label
    WHERE graph = (SELECT graphid FROM ag_catalog.ag_graph WHERE name = graph_name);
    DELETE FROM ag_catalog.ag_graph WHERE name = graph_name;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ag_catalog.create_vlabel(graph_name name, label_name name)
RETURNS integer AS $$
DECLARE
    v_gid integer;
    v_lid integer;
    v_seq text;
BEGIN
    SELECT graphid INTO v_gid FROM ag_catalog.ag_graph WHERE name = graph_name;
    IF v_gid IS NULL THEN
        RAISE EXCEPTION 'graph "%" does not exist', graph_name;
    END IF;

    SELECT id INTO v_lid FROM ag_catalog.ag_label WHERE graph = v_gid AND name = label_name;
    IF v_lid IS NOT NULL THEN
        RETURN v_lid;
    END IF;

    SELECT COALESCE(MAX(id), 0) + 1 INTO v_lid FROM ag_catalog.ag_label WHERE graph = v_gid;
    v_seq := label_name || '_id_seq';

    EXECUTE format('CREATE SEQUENCE %I.%I', graph_name, v_seq);
    EXECUTE format(
        'CREATE TABLE %I.%I (
            id bigint NOT NULL,
            properties jsonb NOT NULL DEFAULT ''{}''::jsonb,
            PRIMARY KEY (id)
        ) DISTRIBUTE BY SHARD(id)',
        graph_name, label_name
    );

    INSERT INTO ag_catalog.ag_label (name, graph, id, kind, relation, seq_name)
    VALUES (label_name, v_gid, v_lid, 'v', label_name, v_seq);

    RETURN v_lid;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ag_catalog.create_elabel(graph_name name, label_name name)
RETURNS integer AS $$
DECLARE
    v_gid integer;
    v_lid integer;
    v_seq text;
BEGIN
    SELECT graphid INTO v_gid FROM ag_catalog.ag_graph WHERE name = graph_name;
    IF v_gid IS NULL THEN
        RAISE EXCEPTION 'graph "%" does not exist', graph_name;
    END IF;

    SELECT id INTO v_lid FROM ag_catalog.ag_label WHERE graph = v_gid AND name = label_name;
    IF v_lid IS NOT NULL THEN
        RETURN v_lid;
    END IF;

    SELECT COALESCE(MAX(id), 0) + 1 INTO v_lid FROM ag_catalog.ag_label WHERE graph = v_gid;
    v_seq := label_name || '_id_seq';

    EXECUTE format('CREATE SEQUENCE %I.%I', graph_name, v_seq);
    -- Co-locate outgoing edges with their source vertex via DISTRIBUTE BY SHARD(start_id)
    EXECUTE format(
        'CREATE TABLE %I.%I (
            id bigint NOT NULL,
            start_id bigint NOT NULL,
            end_id bigint NOT NULL,
            properties jsonb NOT NULL DEFAULT ''{}''::jsonb,
            PRIMARY KEY (start_id, id)
        ) DISTRIBUTE BY SHARD(start_id)',
        graph_name, label_name
    );
    EXECUTE format('CREATE INDEX %I ON %I.%I (end_id)', label_name || '_end_id_idx', graph_name, label_name);

    INSERT INTO ag_catalog.ag_label (name, graph, id, kind, relation, seq_name)
    VALUES (label_name, v_gid, v_lid, 'e', label_name, v_seq);

    RETURN v_lid;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 4. Distributed Vertex & Edge Operations
-- ============================================================================
CREATE OR REPLACE FUNCTION ag_catalog.create_vertex(
    graph_name name,
    label_name name,
    props jsonb DEFAULT '{}'::jsonb
)
RETURNS bigint AS $$
DECLARE
    v_lid integer;
    v_seq text;
    v_entry_id bigint;
    v_graphid bigint;
BEGIN
    v_lid := ag_catalog.create_vlabel(graph_name, label_name);
    v_seq := format('%I.%I', graph_name, label_name || '_id_seq');
    v_entry_id := nextval(v_seq);
    v_graphid := ag_catalog._graphid(v_lid, v_entry_id);

    EXECUTE format('INSERT INTO %I.%I (id, properties) VALUES ($1, $2)', graph_name, label_name)
    USING v_graphid, COALESCE(props, '{}'::jsonb);

    RETURN v_graphid;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ag_catalog.create_edge(
    graph_name name,
    label_name name,
    p_start_id bigint,
    p_end_id bigint,
    props jsonb DEFAULT '{}'::jsonb
)
RETURNS bigint AS $$
DECLARE
    v_lid integer;
    v_seq text;
    v_entry_id bigint;
    v_graphid bigint;
BEGIN
    v_lid := ag_catalog.create_elabel(graph_name, label_name);
    v_seq := format('%I.%I', graph_name, label_name || '_id_seq');
    v_entry_id := nextval(v_seq);
    v_graphid := ag_catalog._graphid(v_lid, v_entry_id);

    EXECUTE format(
        'INSERT INTO %I.%I (id, start_id, end_id, properties) VALUES ($1, $2, $3, $4)',
        graph_name, label_name
    )
    USING v_graphid, p_start_id, p_end_id, COALESCE(props, '{}'::jsonb);

    RETURN v_graphid;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ag_catalog.match_vertices(
    graph_name name,
    label_name name,
    filter jsonb DEFAULT '{}'::jsonb
)
RETURNS TABLE(id bigint, properties jsonb) AS $$
BEGIN
    RETURN QUERY EXECUTE format(
        'SELECT id, properties FROM %I.%I WHERE properties @> $1 ORDER BY id',
        graph_name, label_name
    ) USING COALESCE(filter, '{}'::jsonb);
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 5. Distributed Multi-Hop BFS Traversal (across sharded edge tables)
-- ============================================================================
CREATE OR REPLACE FUNCTION ag_catalog.traverse_bfs(
    graph_name name,
    edge_label name,
    p_start_id bigint,
    p_max_hops integer DEFAULT 3
)
RETURNS TABLE(hop integer, from_id bigint, to_id bigint, edge_id bigint, edge_props jsonb) AS $$
DECLARE
    v_hop integer := 1;
    v_frontier bigint[] := ARRAY[p_start_id];
    v_visited bigint[] := ARRAY[p_start_id];
    v_next_frontier bigint[];
    r RECORD;
BEGIN
    WHILE v_hop <= p_max_hops AND array_length(v_frontier, 1) > 0 LOOP
        v_next_frontier := ARRAY[]::bigint[];
        FOR r IN EXECUTE format(
            'SELECT id, start_id, end_id, properties FROM %I.%I WHERE start_id = ANY($1) ORDER BY id',
            graph_name, edge_label
        ) USING v_frontier
        LOOP
            hop := v_hop;
            from_id := r.start_id;
            to_id := r.end_id;
            edge_id := r.id;
            edge_props := r.properties;
            RETURN NEXT;

            IF NOT (r.end_id = ANY(v_visited)) THEN
                v_visited := array_append(v_visited, r.end_id);
                v_next_frontier := array_append(v_next_frontier, r.end_id);
            END IF;
        END LOOP;

        v_frontier := v_next_frontier;
        v_hop := v_hop + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 6. Lightweight Cypher Subset Executor (CREATE / MATCH)
-- ============================================================================
CREATE OR REPLACE FUNCTION ag_catalog.cypher_exec(
    graph_name name,
    cypher_query text
)
RETURNS SETOF jsonb AS $$
DECLARE
    v_q text := btrim(cypher_query);
    v_m text[];
    v_label name;
    v_props jsonb;
    v_vid bigint;
    r RECORD;
BEGIN
    -- Pattern 1: CREATE (:Label {json_props})
    v_m := regexp_match(v_q, '^CREATE\s*\(\s*:([A-Za-z_][A-Za-z0-9_]*)\s*(\{.*\})?\s*\)$', 'i');
    IF v_m IS NOT NULL THEN
        v_label := v_m[1];
        v_props := COALESCE(v_m[2]::jsonb, '{}'::jsonb);
        v_vid := ag_catalog.create_vertex(graph_name, v_label, v_props);
        RETURN NEXT jsonb_build_object('id', v_vid, 'label', v_label, 'properties', v_props);
        RETURN;
    END IF;

    -- Pattern 2: MATCH (n:Label) RETURN n
    v_m := regexp_match(v_q, '^MATCH\s*\(\s*[A-Za-z_][A-Za-z0-9_]*\s*:([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*RETURN\s+[A-Za-z_][A-Za-z0-9_]*$', 'i');
    IF v_m IS NOT NULL THEN
        v_label := v_m[1];
        FOR r IN SELECT id, properties FROM ag_catalog.match_vertices(graph_name, v_label, '{}'::jsonb) LOOP
            RETURN NEXT jsonb_build_object('id', r.id, 'label', v_label, 'properties', r.properties);
        END LOOP;
        RETURN;
    END IF;

    RAISE EXCEPTION 'Unsupported Cypher syntax in distributed cypher_exec: %', cypher_query;
END;
$$ LANGUAGE plpgsql;
