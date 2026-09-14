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

-- ============================================================================
-- v1.2: weighted_shortest_path (Dijkstra over per-edge weights)
-- ============================================================================
ALTER TABLE g_edges ADD COLUMN weight double precision;
TRUNCATE g_edges;

-- 3-edge DAG: A->B (1) ->C (2) vs A->C (10); expect to take the cheap chain
INSERT INTO g_edges VALUES
    (1, 2, 1.0),
    (2, 3, 2.0),
    (1, 3, 10.0);
SELECT * FROM opentenbase_graph.weighted_shortest_path(
    'g_edges', 'src', 'dst', 'weight', 1, 3, 1000.0);

-- unreachable: 3 has no outgoing edges
SELECT * FROM opentenbase_graph.weighted_shortest_path(
    'g_edges', 'src', 'dst', 'weight', 3, 1, 1000.0);

-- budget too tight: cheapest path is cost=3, ask for <=2
SELECT * FROM opentenbase_graph.weighted_shortest_path(
    'g_edges', 'src', 'dst', 'weight', 1, 3, 2.0);

-- self-loop ignored: A->A (5), A->B (1); must pick A->B
TRUNCATE g_edges;
INSERT INTO g_edges VALUES (1, 1, 5.0), (1, 2, 1.0);
SELECT * FROM opentenbase_graph.weighted_shortest_path(
    'g_edges', 'src', 'dst', 'weight', 1, 2, 100.0);

-- cycle handling: A->B->C->A (1 each), A->D (5)
-- shortest from 1 to 3 goes via the cycle 1->2->3 (cost=2, hops=2), not 1->2->3->1->2->3 (would loop forever)
TRUNCATE g_edges;
INSERT INTO g_edges VALUES
    (1, 2, 1.0),
    (2, 3, 1.0),
    (3, 1, 1.0),
    (1, 4, 5.0);
SELECT * FROM opentenbase_graph.weighted_shortest_path(
    'g_edges', 'src', 'dst', 'weight', 1, 3, 100.0);

-- off-tree target: 1 -> 4 directly (cost=5, hops=1) via the D edge
SELECT * FROM opentenbase_graph.weighted_shortest_path(
    'g_edges', 'src', 'dst', 'weight', 1, 4, 100.0);

-- weighted_bad identifier: weight_col injection attempt must error
SELECT opentenbase_graph.weighted_shortest_path(
    'g_edges', 'src', 'dst', 'weight; DROP TABLE x', 1, 2, 100.0);

DROP TABLE g_edges;
DROP TABLE g_nodes;