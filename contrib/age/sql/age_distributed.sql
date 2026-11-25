-- Distributed AGE verification test on OpenTenBase
CREATE EXTENSION IF NOT EXISTS age;

-- 1. Test C-level graphid bit-packing
SELECT ag_catalog._graphid(3, 42) AS gid,
       ag_catalog._label_id(ag_catalog._graphid(3, 42)) AS lid,
       ag_catalog._extract_id(ag_catalog._graphid(3, 42)) AS eid;

-- 2. Create distributed graph
SELECT ag_catalog.create_graph('agent_mem_graph');

-- 3. Create vertices and edges across distributed shards
DO $$
DECLARE
    v_alice bigint;
    v_otb bigint;
    v_pgvec bigint;
    v_age bigint;
BEGIN
    v_alice := ag_catalog.create_vertex('agent_mem_graph', 'Entity', '{"name": "Alice", "type": "User"}'::jsonb);
    v_otb   := ag_catalog.create_vertex('agent_mem_graph', 'Entity', '{"name": "OpenTenBase", "type": "Database"}'::jsonb);
    v_pgvec := ag_catalog.create_vertex('agent_mem_graph', 'Entity', '{"name": "pgvector", "type": "Extension"}'::jsonb);
    v_age   := ag_catalog.create_vertex('agent_mem_graph', 'Entity', '{"name": "AGE", "type": "GraphExtension"}'::jsonb);

    PERFORM ag_catalog.create_edge('agent_mem_graph', 'RELATES_TO', v_alice, v_otb, '{"rel": "uses"}'::jsonb);
    PERFORM ag_catalog.create_edge('agent_mem_graph', 'RELATES_TO', v_otb, v_pgvec, '{"rel": "supports"}'::jsonb);
    PERFORM ag_catalog.create_edge('agent_mem_graph', 'RELATES_TO', v_otb, v_age, '{"rel": "adapts"}'::jsonb);
END $$;

-- 4. Verify Cypher MATCH & CREATE
SELECT ag_catalog.cypher_exec('agent_mem_graph', 'CREATE (:Entity {"name": "MemoryAgent", "type": "Agent"})');
SELECT ag_catalog.cypher_exec('agent_mem_graph', 'MATCH (n:Entity) RETURN n');

-- 5. Verify shard distribution across DNs
SELECT xc_node_id, count(*) AS vertex_count
FROM agent_mem_graph."Entity"
GROUP BY xc_node_id
ORDER BY xc_node_id;

-- 6. Verify multi-hop distributed BFS traversal (Alice -> OpenTenBase -> pgvector & AGE)
SELECT hop, from_id, to_id, edge_props
FROM ag_catalog.traverse_bfs(
    'agent_mem_graph',
    'RELATES_TO',
    (SELECT id FROM agent_mem_graph."Entity" WHERE properties->>'name' = 'Alice' LIMIT 1),
    3
);

-- 7. Cleanup
SELECT ag_catalog.drop_graph('agent_mem_graph', true);
