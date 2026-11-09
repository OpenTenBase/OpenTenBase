-- pg_graph_search--1.0.0.sql
-- Generated from ddl/*.sql. Do not edit this assembled file directly.
--
-- A pure-SQL property-graph layer for OpenTenBase/PostgreSQL 10.  The
-- extension stores only instance metadata; each installed instance generates
-- a view and five schema-qualified application functions.

CREATE TABLE IF NOT EXISTS graph_search.registry (
    target_schema text NOT NULL,
    prefix text NOT NULL,
    spec jsonb NOT NULL,
    node_type regtype NOT NULL,
    installed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (target_schema, prefix)
) DISTRIBUTE BY REPLICATION;

COMMENT ON TABLE graph_search.registry IS
    'Installed pg_graph_search instances. This table is replicated intentionally.';

