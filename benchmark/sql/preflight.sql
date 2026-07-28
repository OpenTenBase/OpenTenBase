-- Copyright (c) 2026 OpenTenBase Authors
-- Licensed under the BSD 3-Clause License.

\set ON_ERROR_STOP on
\pset pager off
\echo '=== version ==='
SELECT version();

\echo '=== topology ==='
SELECT
    node_name,
    node_type,
    node_host,
    node_port,
    nodeis_primary,
    nodeis_preferred
FROM pgxc_node
ORDER BY node_type, node_name;

\echo '=== node groups ==='
SELECT group_name, default_group, group_members
FROM pgxc_group
ORDER BY group_name;

\echo '=== sharding map ==='
SELECT
    g.group_name,
    count(m.shardgroupid) AS shard_count
FROM pgxc_group AS g
LEFT JOIN pgxc_shard_map AS m
    ON m.disgroup = g.oid
WHERE g.group_name = :'group_name'
GROUP BY g.group_name;

SELECT
    'benchmark_shard_count=' || count(m.shardgroupid)::text AS benchmark_check
FROM pgxc_group AS g
LEFT JOIN pgxc_shard_map AS m
    ON m.disgroup = g.oid
WHERE g.group_name = :'group_name';

\echo '=== relevant settings ==='
SELECT name, setting, unit
FROM pg_settings
WHERE name IN (
    'max_connections',
    'shared_buffers',
    'work_mem',
    'maintenance_work_mem',
    'effective_cache_size',
    'max_prepared_transactions',
    'synchronous_commit'
)
ORDER BY name;
