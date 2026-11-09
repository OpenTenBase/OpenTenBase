# pg_graph_search

`pg_graph_search` is a pure-SQL property-graph extension for OpenTenBase. It
turns an existing edge table into a schema-qualified API for multi-hop search,
cursor iteration, shortest-path lookup, subgraph JSON, and per-level stats.

## Build

```sh
make -C contrib/pg_graph_search
make -C contrib/pg_graph_search install
make -C contrib/pg_graph_search installcheck
```

The generated `pg_graph_search--1.0.0.sql` is assembled from `ddl/`; do not
edit it by hand. Both in-tree and `USE_PGXS=1` builds are supported.

## Install an instance

```sql
CREATE EXTENSION pg_graph_search;
CREATE SCHEMA graph;
SELECT * FROM graph_search.install(
  '{"table":"public.edges","c1":"src","c2":"dst","prop":"prop",
     "target_schema":"graph","prefix":"user","directed":false,
     "max_depth_cap":64}'::jsonb);
```

`install()` records metadata in the replicated `graph_search.registry` table
and creates `graph.user_edges`, `user_search`, `user_cursor`,
`user_shortest_path`, `user_subgraph`, and `user_stats`.

## Deliberate PR1 boundary

This PR provides the extension packaging and centralized/replicated-table
baseline. HASH-distribution differential tests, CN/DN failure injection and
performance numbers are follow-up PRs from Issue #386.

