# `contrib/age` — Distributed Property Graph Extension for OpenTenBase

`contrib/age` adapts the Apache AGE (`ag_catalog`) property graph model to OpenTenBase's shared-nothing distributed architecture (`GTM + CN + DN`).

## Distributed Architecture

1. **Replicated Graph Catalogs (`DISTRIBUTE BY REPLICATION`)**:
   - `ag_catalog.ag_graph` and `ag_catalog.ag_label` are replicated across all DataNodes so graph metadata is consistent cluster-wide without cross-node catalog skew.
2. **Sharded Vertex & Co-Located Edge Storage**:
   - Vertex label tables use `DISTRIBUTE BY SHARD(id)`.
   - Edge label tables use `DISTRIBUTE BY SHARD(start_id)`, co-locating outgoing edges on the same DataNode as their source vertex for single-DN 1-hop expansion.
3. **Apache AGE-Compatible 64-bit `graphid` (C Extension `age.c`)**:
   - Encodes `[16-bit label_id | 48-bit entry_id]` via `_graphid()`, `_label_id()`, and `_extract_id()`.
4. **Distributed Multi-Hop Traversal & Cypher Subset**:
   - `ag_catalog.traverse_bfs(graph_name, edge_label, start_id, max_hops)` executes multi-hop BFS across distributed edge shards.
   - `ag_catalog.cypher_exec(graph_name, cypher_query)` supports `CREATE (:Label {...})` and `MATCH (n:Label) RETURN n`.

## Build & Install

```bash
cd contrib/age
make USE_PGXS=1 PG_CONFIG=/path/to/opentenbase/bin/pg_config
make USE_PGXS=1 PG_CONFIG=/path/to/opentenbase/bin/pg_config install
```

> Note: Install the compiled `age.so` and extension files on the Coordinator (CN) and all DataNodes (DNs).

## Quick Example

```sql
CREATE EXTENSION age;
SELECT ag_catalog.create_graph('agent_mem_graph');

SELECT ag_catalog.cypher_exec('agent_mem_graph', 'CREATE (:Entity {"name": "Alice", "role": "Engineer"})');
SELECT ag_catalog.cypher_exec('agent_mem_graph', 'MATCH (n:Entity) RETURN n');
```
