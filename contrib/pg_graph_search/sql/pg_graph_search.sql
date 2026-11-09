-- pg_graph_search: 15 correctness tests + 16 assertion regressions.
CREATE SCHEMA graph_demo;
CREATE TABLE graph_demo.edges (c1 int, c2 int, prop jsonb) DISTRIBUTE BY REPLICATION;
INSERT INTO graph_demo.edges VALUES
  (1, 2, '{"weight": 1}'), (2, 3, '{"weight": 1}'),
  (3, 4, '{"weight": 1}'), (1, 5, '{"weight": 0}');
CREATE TABLE graph_demo.no_prop (c1 int, c2 int) DISTRIBUTE BY REPLICATION;
INSERT INTO graph_demo.no_prop VALUES (10, 11);
CREATE TABLE graph_demo.text_edges (c1 text, c2 text, prop jsonb) DISTRIBUTE BY REPLICATION;
INSERT INTO graph_demo.text_edges VALUES ('a', 'b', '{}');
CREATE TABLE graph_demo.uuid_edges (c1 uuid, c2 uuid, prop jsonb) DISTRIBUTE BY REPLICATION;
INSERT INTO graph_demo.uuid_edges VALUES
 ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', '{}');
CREATE TABLE graph_demo.results (test text PRIMARY KEY, pass boolean NOT NULL) DISTRIBUTE BY REPLICATION;

CREATE EXTENSION pg_graph_search;

CREATE FUNCTION graph_demo.raises(p_sql text) RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE p_sql;
  RETURN false;
EXCEPTION WHEN OTHERS THEN
  RETURN true;
END
$$;

CREATE FUNCTION graph_demo.record(p_test text, p_sql text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE ok boolean;
BEGIN
  EXECUTE p_sql INTO ok;
  INSERT INTO graph_demo.results VALUES (p_test, ok);
END
$$;

CREATE FUNCTION graph_demo.temp_lifecycle() RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE installed integer; removed integer;
BEGIN
  SELECT count(*) INTO installed FROM graph_search.install(
    '{"table":"graph_demo.edges","target_schema":"graph_demo","prefix":"temp"}');
  SELECT count(*) INTO removed FROM graph_search.uninstall('graph_demo', 'temp');
  RETURN installed = 11 AND removed = 6
    AND NOT EXISTS (SELECT 1 FROM graph_search.registry WHERE target_schema = 'graph_demo' AND prefix = 'temp');
END
$$;

-- Primary undirected instance, plus a directed instance for contrast.
INSERT INTO graph_demo.results
SELECT 'A05_plan_has_11_statements', cardinality(graph_search.plan(
  '{"table":"graph_demo.edges","target_schema":"graph_demo","prefix":"user"}')) = 11;
SELECT count(*) = 11 AS a06_install_ok FROM graph_search.install(
  '{"table":"graph_demo.edges","target_schema":"graph_demo","prefix":"user","max_depth_cap":4}') \gset
INSERT INTO graph_demo.results VALUES ('A06_install_executes_11_statements', :'a06_install_ok'::boolean);
SELECT count(*) FROM graph_search.install(
  '{"table":"graph_demo.edges","target_schema":"graph_demo","prefix":"dir","directed":true,"max_depth_cap":4}');
SELECT count(*) FROM graph_search.install(
  '{"table":"graph_demo.no_prop","target_schema":"graph_demo","prefix":"plain"}');
SELECT count(*) FROM graph_search.install(
  '{"table":"graph_demo.text_edges","target_schema":"graph_demo","prefix":"textg"}');
SELECT count(*) FROM graph_search.install(
  '{"table":"graph_demo.uuid_edges","target_schema":"graph_demo","prefix":"uuidg"}');

-- 15 correctness tests.  Dynamic evaluation ensures graph functions run on CN.
SELECT graph_demo.record('C01_one_hop', $$SELECT count(*) = 2 FROM graph_demo.user_search(1,1,100,0)$$);
SELECT graph_demo.record('C02_two_hop', $$SELECT count(*) = 3 FROM graph_demo.user_search(1,2,100,0)$$);
SELECT graph_demo.record('C03_three_hop', $$SELECT count(*) = 4 FROM graph_demo.user_search(1,3,100,0)$$);
SELECT graph_demo.record('C04_paths_are_acyclic', $$SELECT NOT EXISTS (SELECT 1 FROM graph_demo.user_search(1,4,100,0) h WHERE EXISTS (SELECT 1 FROM unnest(h.o_path) x GROUP BY x HAVING count(*) > 1))$$);
SELECT graph_demo.record('C05_shortest_includes_endpoints', $$SELECT EXISTS (SELECT 1 FROM graph_demo.user_shortest_path(1,4,4,100,0) WHERE o_path = ARRAY[1,2,3,4] AND o_depth = 3)$$);
SELECT graph_demo.record('C06_same_endpoint_is_zero_hop', $$SELECT EXISTS (SELECT 1 FROM graph_demo.user_shortest_path(1,1,4,100,0) WHERE o_path = ARRAY[1] AND o_depth = 0)$$);
SELECT graph_demo.record('C07_unreachable_is_empty', $$SELECT NOT EXISTS (SELECT 1 FROM graph_demo.user_shortest_path(1,99,4,100,0))$$);
SELECT graph_demo.record('C08_weight_threshold_filters', $$SELECT NOT EXISTS (SELECT 1 FROM graph_demo.user_search(1,3,100,2))$$);
SELECT graph_demo.record('C09_directed_reverse_is_empty', $$SELECT NOT EXISTS (SELECT 1 FROM graph_demo.dir_shortest_path(4,1,4,100,0))$$);
SELECT graph_demo.record('C10_undirected_reverse_is_reachable', $$SELECT EXISTS (SELECT 1 FROM graph_demo.user_shortest_path(4,1,4,100,0))$$);
SELECT graph_demo.record('C11_level_stats_are_correct', $$SELECT (SELECT sum(o_edges) FROM graph_demo.user_stats(1,3,100,0)) = 4$$);
SELECT graph_demo.record('C12_subgraph_has_nodes_and_edges', $$SELECT jsonb_array_length(graph_demo.user_subgraph(1,3,100,0)->'nodes') = 5 AND jsonb_array_length(graph_demo.user_subgraph(1,3,100,0)->'edges') = 4$$);
SELECT graph_demo.record('C13_property_column_is_optional', $$SELECT EXISTS (SELECT 1 FROM graph_demo.plain_search(10,1,100,0) WHERE o_path = ARRAY[10,11])$$);
SELECT graph_demo.record('C14_text_node_type_works', $$SELECT EXISTS (SELECT 1 FROM graph_demo.textg_search('a',1,100,0) WHERE o_path = ARRAY['a','b']::text[])$$);
SELECT graph_demo.record('C15_uuid_node_type_works', $$SELECT EXISTS (SELECT 1 FROM graph_demo.uuidg_search('00000000-0000-0000-0000-000000000001'::uuid,1,100,0) WHERE o_depth = 1)$$);

-- 16 assertion regressions.
INSERT INTO graph_demo.results SELECT 'A01_extension_registered', EXISTS (
  SELECT 1 FROM pg_extension WHERE extname = 'pg_graph_search');
INSERT INTO graph_demo.results SELECT 'A02_registry_exists', EXISTS (
  SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = 'graph_search' AND c.relname = 'registry');
INSERT INTO graph_demo.results SELECT 'A03_five_generated_apis', count(*) = 5 FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
  WHERE n.nspname = 'graph_demo' AND p.proname ~ '^user_(search|cursor|shortest_path|subgraph|stats)$';
INSERT INTO graph_demo.results SELECT 'A04_generated_search_signature', EXISTS (
  SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
  WHERE n.nspname = 'graph_demo' AND p.proname = 'user_search'
    AND pg_get_function_identity_arguments(p.oid) = 'root integer, depth integer, p_limit bigint, min_weight double precision');
SELECT graph_demo.record('A07_list_installed_contains_user', $$SELECT EXISTS (SELECT 1 FROM graph_search.list_installed() WHERE target_schema = 'graph_demo' AND prefix = 'user')$$);
SELECT graph_demo.record('A08_reinstall_is_idempotent', $$SELECT count(*) = 11 FROM graph_search.install('{"table":"graph_demo.edges","target_schema":"graph_demo","prefix":"user","max_depth_cap":4}')$$);
SELECT graph_demo.record('A09_uninstall_removes_all_objects', $$SELECT graph_demo.temp_lifecycle()$$);
SELECT graph_demo.record('A10_missing_table_raises', $$SELECT graph_demo.raises($q$SELECT graph_search._norm('{"table":"graph_demo.missing"}'::jsonb)$q$)$$);
SELECT graph_demo.record('A11_invalid_prefix_raises', $$SELECT graph_demo.raises($q$SELECT graph_search._norm('{"table":"graph_demo.edges","prefix":"bad-name"}'::jsonb)$q$)$$);
SELECT graph_demo.record('A12_invalid_depth_cap_raises', $$SELECT graph_demo.raises($q$SELECT graph_search._norm('{"table":"graph_demo.edges","max_depth_cap":999}'::jsonb)$q$)$$);
SELECT graph_demo.record('A13_invalid_prop_type_raises', $$SELECT graph_demo.raises($q$SELECT graph_search._norm('{"table":"graph_demo.edges","prop":"c1"}'::jsonb)$q$)$$);
CREATE TABLE graph_demo.mismatch (c1 int, c2 text) DISTRIBUTE BY REPLICATION;
SELECT graph_demo.record('A14_mismatched_endpoints_raise', $$SELECT graph_demo.raises($q$SELECT graph_search._norm('{"table":"graph_demo.mismatch"}'::jsonb)$q$)$$);
SET search_path TO pg_catalog;
SELECT graph_demo.record('A15_schema_qualified_api_works', $$SELECT (SELECT count(*) FROM graph_demo.user_search(1,1,100,0)) = 2$$);
RESET search_path;
INSERT INTO graph_demo.results SELECT 'A16_extension_version_is_1_0_0', EXISTS (
  SELECT 1 FROM pg_extension WHERE extname = 'pg_graph_search' AND extversion = '1.0.0');

SELECT test, pass FROM graph_demo.results ORDER BY test;

DROP EXTENSION pg_graph_search;
DROP SCHEMA graph_demo CASCADE;
