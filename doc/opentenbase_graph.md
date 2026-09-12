# opentenbase_graph — Lightweight Graph Traversal Templates

> Author: Memsetqwq
> Version: 1.0
> Date: 2026-09-10
> Related: 犀牛鸟 Open Source Program 2026 — Task 3 (graph compute enhancement on OpenTenBase)
> See also: `doc/proposals/pgvector-pg18-v0.3-graph-design.md` (design rationale)

---

## Overview

`opentenbase_graph` is a PostgreSQL extension that provides lightweight graph traversal templates for OpenTenBase. It is implemented in **pure PL/pgSQL** (no C code), so it works on **PostgreSQL 10 and later**, including the OpenTenBase fork kernel (PG10-based).

The extension exposes four set-returning and scalar functions that operate on user-defined nodes/edges tables. Identifier parameters are whitelisted via regex (`^[a-zA-Z_][a-zA-Z0-9_]*$`) so application table/column names cannot inject SQL.

## Installation

```sql
CREATE EXTENSION opentenbase_graph;
```

This loads four functions into the `opentenbase_graph` schema.

## API

All functions are marked `STABLE` and can be inlined into larger queries.

### 1. `bfs` — Breadth-First Traversal

```sql
opentenbase_graph.bfs(
    nodes_table regclass,   -- table containing the nodes (must exist)
    edges_table regclass,   -- table containing the edges
    src_col     text,       -- edges.src column name
    dst_col     text,       -- edges.dst column name
    start_id    bigint,     -- starting node id
    max_depth   int DEFAULT 5
)
RETURNS TABLE(depth int, node bigint)
```

Returns `(depth, node)` for every visited node, including the start node at depth 0. Cycle prevention uses a `visited[]` array (since the OpenTenBase fork is PG10 and does not have `CYCLE`).

**Example:**

```sql
-- given: nodes(id bigint), edges(src bigint, dst bigint)
SELECT * FROM opentenbase_graph.bfs('nodes', 'edges', 'src', 'dst', 1, 3);
 depth | node
-------+------
     0 |    1
     1 |    2
     1 |    3
     2 |    4
     2 |    5
     ...
```

### 2. `shortest_path` — Unweighted Shortest Path

```sql
opentenbase_graph.shortest_path(
    nodes_table regclass,
    edges_table regclass,
    src_col     text,
    dst_col     text,
    start_id    bigint,
    end_id      bigint,
    max_depth   int DEFAULT 1000
)
RETURNS TABLE(depth int, path bigint[])
```

Returns the depth (hop count) and the full path as `bigint[]`. Returns **zero rows** if `end_id` is not reachable within `max_depth`.

**Example:**

```sql
SELECT * FROM opentenbase_graph.shortest_path('nodes', 'edges', 'src', 'dst', 1, 5, 100);
 depth |     path
-------+---------------
     4 | {1,2,3,4,5}
```

### 3. `degree` — In-Degree / Out-Degree

```sql
opentenbase_graph.degree(
    edges_table regclass,
    src_col     text,
    dst_col     text,
    node_id     bigint
)
RETURNS TABLE(in_degree bigint, out_degree bigint)
```

Returns the number of edges whose `dst = node_id` (in-degree) and whose `src = node_id` (out-degree).

**Example:**

```sql
SELECT * FROM opentenbase_graph.degree('edges', 'src', 'dst', 4);
 in_degree | out_degree
-----------+------------
         2 |          1
```

### 4. `reachable` — Boolean Reachability Check

```sql
opentenbase_graph.reachable(
    nodes_table regclass,
    edges_table regclass,
    src_col     text,
    dst_col     text,
    start_id    bigint,
    end_id      bigint,
    max_depth   int DEFAULT 1000
)
RETURNS bool
```

Cheaper than `shortest_path` when only a yes/no answer is needed.

**Example:**

```sql
SELECT opentenbase_graph.reachable('nodes', 'edges', 'src', 'dst', 1, 5, 100) AS can_reach;
 can_reach
-----------
 t
```

### 5. `_graph_info` — Global Graph Shape Diagnostics (v1.1+)

```sql
opentenbase_graph._graph_info(
    edges_table regclass,
    src_col     text,
    dst_col     text
)
RETURNS TABLE(
    node_count        bigint,
    edge_count        bigint,
    max_in_degree     bigint,
    max_out_degree    bigint,
    max_total_degree  bigint,
    density           double precision
)
```

Returns a single row summarising the global shape of an edges table. Useful
as a first step when debugging "this query is slow" or before deciding
whether the dataset is small enough for the v1.0 traversal templates or
needs Apache AGE.

- `node_count` = distinct nodes appearing as either `src_col` or `dst_col`
- `edge_count` = row count of the edges table
- `max_in_degree` / `max_out_degree` = max of `count(*) GROUP BY dst|src`
- `max_total_degree` = max over all nodes of `in_degree + out_degree`
- `density` = `edge_count / (node_count * (node_count - 1))` for a directed
  simple graph; `0` when `node_count < 2`.

**Example:**

```sql
SELECT * FROM opentenbase_graph._graph_info('edges', 'src', 'dst');
 node_count | edge_count | max_in_degree | max_out_degree | max_total_degree |      density
------------+------------+---------------+----------------+------------------+--------------------
          7 |         12 |             2 |              2 |                4 | 0.2857142857142857
```

## Schema Conventions

This extension does **not** create tables of its own — the application owns the schema. A minimal convention is:

```sql
CREATE TABLE my_nodes (id bigserial PRIMARY KEY, props jsonb);
CREATE TABLE my_edges (src bigint REFERENCES my_nodes,
                       dst bigint REFERENCES my_nodes,
                       weight float8 DEFAULT 1.0);
```

The column names `src` / `dst` are not enforced — you can use any column names by passing them as the `src_col` / `dst_col` arguments.

## Limitations

- Suitable for **small to medium graphs** (< 100k nodes, depth ≤ 10). For very large graphs, prefer Apache AGE (when backported) or a dedicated graph database.
- Functions use dynamic SQL via `EXECUTE` to support arbitrary table/column names; this prevents some planner optimisations.
- Cycle protection uses a `visited[]` array which costs O(depth) memory per row.
- No built-in support for weighted shortest path (`weight` column is ignored by `shortest_path` — use a separate function for Dijkstra).
- Not distributed: assumes all data lives in a single database/schema. Cross-node graph traversal is an OpenTenBase architectural concern outside this extension's scope.

## Security

Identifier parameters are whitelisted with `^[a-zA-Z_][a-zA-Z0-9_]*$`. SQL injection via table or column name arguments is not possible.

## License

Same license as OpenTenBase (see top-level `LICENSE`).

## Author

Memsetqwq — 犀牛鸟 Open Source Program 2026 (project submission)