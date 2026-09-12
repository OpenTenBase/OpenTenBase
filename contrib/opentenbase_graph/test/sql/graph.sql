-- (CREATE EXTENSION loaded automatically by installcheck --load-extension)
-- simple linear chain 1 -> 2 -> 3 -> 4 -> 5
CREATE TABLE g_nodes (id bigserial PRIMARY KEY);
SELECT setval('g_nodes_id_seq', 5);
CREATE TABLE g_edges (src bigint, dst bigint);
INSERT INTO g_edges VALUES (1, 2), (2, 3), (3, 4), (4, 5);

-- bfs depth 0 (only start node)
SELECT * FROM opentenbase_graph.bfs('g_nodes', 'g_edges', 'src', 'dst', 1, 0)
ORDER BY depth, node;

-- bfs depth 2 from node 1
SELECT * FROM opentenbase_graph.bfs('g_nodes', 'g_edges', 'src', 'dst', 1, 2)
ORDER BY depth, node;

-- bfs from middle node
SELECT * FROM opentenbase_graph.bfs('g_nodes', 'g_edges', 'src', 'dst', 3, 2)
ORDER BY depth, node;

-- shortest_path: 1 -> 5 along the chain
SELECT * FROM opentenbase_graph.shortest_path('g_nodes', 'g_edges', 'src', 'dst', 1, 5, 10);

-- shortest_path: 5 -> 1 (reverse, no path)
SELECT * FROM opentenbase_graph.shortest_path('g_nodes', 'g_edges', 'src', 'dst', 5, 1, 10);

-- degree of middle node 3
SELECT * FROM opentenbase_graph.degree('g_edges', 'src', 'dst', 3);

-- degree of end node 5 (out=0, in=1)
SELECT * FROM opentenbase_graph.degree('g_edges', 'src', 'dst', 5);

-- reachable: forward direction works
SELECT opentenbase_graph.reachable('g_nodes', 'g_edges', 'src', 'dst', 1, 5, 10) AS r;

-- reachable: reverse direction fails
SELECT opentenbase_graph.reachable('g_nodes', 'g_edges', 'src', 'dst', 5, 1, 10) AS r;

-- branching tree (undirected): 1 - {2, 3}; 2 - {4, 5}; 3 - {6, 7}
TRUNCATE g_edges;
INSERT INTO g_edges VALUES
    (1, 2), (2, 1),
    (1, 3), (3, 1),
    (2, 4), (4, 2),
    (2, 5), (5, 2),
    (3, 6), (6, 3),
    (3, 7), (7, 3);

-- bfs depth 2 from root 1
SELECT * FROM opentenbase_graph.bfs('g_nodes', 'g_edges', 'src', 'dst', 1, 2)
ORDER BY depth, node;

-- shortest_path from leaf 4 to leaf 7 (via 4 -> 2 -> 1 -> 3 -> 7, depth 4)
SELECT * FROM opentenbase_graph.shortest_path('g_nodes', 'g_edges', 'src', 'dst', 4, 7, 10);

-- cycle detection: 1 -> 2 -> 3 -> 1 (loop)
TRUNCATE g_edges;
INSERT INTO g_edges VALUES (1, 2), (2, 3), (3, 1);

-- bfs must terminate and return each node exactly once
SELECT count(*), count(DISTINCT node) FROM opentenbase_graph.bfs('g_nodes', 'g_edges', 'src', 'dst', 1, 100);

-- reachable from 1 to 3 (via cycle, depth 2)
SELECT opentenbase_graph.reachable('g_nodes', 'g_edges', 'src', 'dst', 1, 3, 10) AS r;

-- bad src_col identifier is rejected
SELECT opentenbase_graph.bfs('g_nodes', 'g_edges', 'src; DROP TABLE x', 'dst', 1, 1);

-- _graph_info: cycle 1 -> 2 -> 3 -> 1 (3 nodes, 3 edges, max_total=2)
SELECT * FROM opentenbase_graph._graph_info('g_edges', 'src', 'dst');

-- _graph_info: branching tree 1 - {2,3}; 2 - {4,5}; 3 - {6,7}
-- 7 nodes, 12 edges (undirected double-edges), max_in=3, max_out=3, max_total=6
TRUNCATE g_edges;
INSERT INTO g_edges VALUES
    (1, 2), (2, 1),
    (1, 3), (3, 1),
    (2, 4), (4, 2),
    (2, 5), (5, 2),
    (3, 6), (6, 3),
    (3, 7), (7, 3);
SELECT * FROM opentenbase_graph._graph_info('g_edges', 'src', 'dst');

-- _graph_info: empty table (0 nodes, 0 edges, density=0)
TRUNCATE g_edges;
SELECT * FROM opentenbase_graph._graph_info('g_edges', 'src', 'dst');

-- _graph_info: bad src_col identifier is rejected
SELECT opentenbase_graph._graph_info('g_edges', 'src; DROP TABLE x', 'dst');

DROP TABLE g_edges;
DROP TABLE g_nodes;