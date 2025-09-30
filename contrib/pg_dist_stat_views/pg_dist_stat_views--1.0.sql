/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause
 * 
 */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "CREATE EXTENSION pg_dist_stat_views" to load this file. \quit

/******************************************************************************
 * Section 1: C Function Definitions
 ******************************************************************************/

--
-- Name: dist_pg_stat_get_activity(...)
-- Description: The underlying C function for the distributed activity view.
--              It collects and fuses activity information from all cluster nodes.
--
CREATE OR REPLACE FUNCTION dist_pg_stat_get_activity(
    IN sessionid text,
    IN coordonly bool,
    IN localonly bool,

    OUT sessionid text,
    OUT pid integer,
    OUT client_addr inet,
    OUT client_hostname text,
    OUT client_port integer,
    OUT nodename text,
    OUT role text,
    OUT datname text,
    OUT usename text,
    OUT wait_event_type text,
    OUT wait_event text,
    OUT state text,
    OUT sqname text,
    OUT sqdone bool,
    OUT query text,
    OUT planstate text,
    OUT portal text,
    OUT cursors text,
    OUT backend_start timestamp with time zone,
    OUT xact_start timestamp with time zone,
    OUT query_start timestamp with time zone,
    OUT state_change timestamp with time zone,
    OUT application_name text,
    OUT backend_xid xid,
    OUT backend_xmin xid,
    OUT backend_type text,
    OUT gxid text,
    OUT global_query_id text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C;

--
-- Name: get_dist_pg_locks(boolean)
-- Description: The underlying C function for the distributed locks view.
--              It safely collects raw lock information and GXIDs from all nodes.
--              This function intentionally does not resolve blocking relationships.
--
CREATE OR REPLACE FUNCTION get_dist_pg_locks(
    IN localonly boolean DEFAULT false,
    OUT node_name text,
    OUT locktype text,
    OUT database oid,
    OUT relation oid,
    OUT page integer,
    OUT tuple smallint,
    OUT virtualxid text,
    OUT transactionid xid,
    OUT classid oid,
    OUT objid oid,
    OUT objsubid smallint,
    OUT virtualtransaction text,
    OUT pid integer,
    OUT mode text,
    OUT granted boolean,
    OUT fastpath boolean,
    OUT gxid text,
    OUT blocking_pid integer,
    OUT blocking_gxid text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'get_dist_pg_locks'
LANGUAGE C STRICT VOLATILE;

/******************************************************************************
 * Section 2: Core Data Views
 ******************************************************************************/

--
-- View: dist_pg_stat_activity
-- Description: Provides a cluster-wide, real-time view of backend activity,
--              analogous to the single-node pg_stat_activity.
--
CREATE OR REPLACE VIEW dist_pg_stat_activity AS
  SELECT * FROM dist_pg_stat_get_activity(NULL, false, false);

--
-- View: dist_pg_stat_activity_cn
-- Description: A convenience view showing activity only on coordinator nodes.
--
CREATE OR REPLACE VIEW dist_pg_stat_activity_cn AS
  SELECT * FROM dist_pg_stat_get_activity(NULL, true, false);

--
-- View: dist_pg_locks_raw
-- Description: Provides the raw, unmodified output from the C function,
--              serving as a stable base for further analysis.
--
CREATE OR REPLACE VIEW dist_pg_locks_raw AS
  SELECT * FROM get_dist_pg_locks(false);

--
-- View: dist_pg_locks
-- Description: The primary distributed locks view. It enhances the raw lock data by:
--              1. Adding the node name of the blocking process.
--              2. Creating a human-readable 'lock_target' description.
--              3. Filtering out redundant low-level locks for the same object
--                 to present the most significant lock wait.
--
CREATE OR REPLACE VIEW dist_pg_locks AS
WITH 
potential_blockers AS (
    SELECT
        nodename,
        pid,
        gxid
    FROM
        dist_pg_stat_activity
    WHERE
        gxid IS NOT NULL 
        AND state != 'idle'
),

enriched_locks AS (
    SELECT
        raw.*,
        blockers.nodename AS blocking_node_name,
        CASE 
            WHEN raw.locktype = 'relation' THEN
                raw.relation::regclass::text
            
            WHEN raw.locktype = 'extend' THEN
                'extend relation ' || raw.relation::regclass::text

            WHEN raw.locktype = 'page' THEN
                'page ' || raw.page || ' in ' || raw.relation::regclass::text

            WHEN raw.locktype = 'tuple' THEN 
                'row (' || raw.page || ',' || raw.tuple || ') in ' || raw.relation::regclass::text

            WHEN raw.locktype = 'transactionid' THEN
                'transaction ' || raw.transactionid::text

            WHEN raw.locktype = 'virtualxid' THEN
                'virtual transaction ' || raw.virtualxid

            WHEN raw.locktype = 'advisory' THEN
                'advisory lock [' || raw.classid::text || ',' || raw.objid::text || ']'

            WHEN raw.classid != 0 THEN
                pg_describe_object(raw.classid, raw.objid, raw.objsubid)
            
            ELSE
                raw.locktype
        END AS lock_target
    FROM
        dist_pg_locks_raw AS raw
    LEFT JOIN
        potential_blockers AS blockers
        ON raw.blocking_pid = blockers.pid AND raw.blocking_gxid = blockers.gxid
)

SELECT DISTINCT ON (node_name, pid, granted, lock_target)
    node_name,
    granted,
    gxid,
    pid,
    locktype,
    database,
    relation,
    page,
    tuple,
    virtualxid,
    transactionid,
    classid,
    objid,
    objsubid,
    virtualtransaction,
    mode,
    fastpath,
    blocking_node_name,
    blocking_pid,
    blocking_gxid,
    lock_target
FROM
    enriched_locks
ORDER BY
    node_name, pid, granted, lock_target,
    CASE mode
        WHEN 'AccessExclusiveLock' THEN 1
        WHEN 'ExclusiveLock' THEN 2
        WHEN 'ShareRowExclusiveLock' THEN 3
        WHEN 'RowExclusiveLock' THEN 4
        WHEN 'ShareLock' THEN 5
        ELSE 99
    END;

--
-- View: dist_pg_locks_all_info
-- Description: A comprehensive view that joins dist_pg_locks with activity
--              information for both the waiting and blocking processes.
--
CREATE OR REPLACE VIEW dist_pg_locks_all_info AS
SELECT 
    l.node_name,
    l.granted,
    
    l.gxid AS waiter_gxid,
    l.pid AS waiter_pid,
    act.usename AS waiter_usename,
    act.state AS waiter_state,
    act.query AS waiter_query,
    
    l.locktype,
    l.mode,
    l.lock_target,
    
    l.blocking_node_name,
    l.blocking_pid,
    l.blocking_gxid,
    blocker_act.usename AS blocker_usename,
    blocker_act.state AS blocking_state,
    blocker_act.query AS blocking_query
FROM 
    dist_pg_locks AS l
LEFT JOIN
    dist_pg_stat_activity AS act 
    ON l.pid = act.pid AND l.node_name = act.nodename
LEFT JOIN
    dist_pg_stat_activity AS blocker_act 
    ON l.blocking_pid = blocker_act.pid AND l.blocking_node_name = blocker_act.nodename;

/******************************************************************************
 * Section 3: Advanced Analytical Views
 ******************************************************************************/
 
--
-- View: dist_pg_stat_query_summary
-- Description: Provides a high-level summary for each active distributed query
--              using global_query_id(GID) as the unique identifier. Aggregates
--              states, wait events, and backend roles across all involved nodes.
--              Ideal for quick performance bottleneck identification and
--              cross-node query correlation.
--
-- Key Features:
--   - Uses global_query_id(GID) for unified query tracking
--   - Cross-node query correlation and analysis
--   - Real-time performance monitoring
--   - Wait event analysis across distributed cluster
CREATE OR REPLACE VIEW dist_pg_stat_query_summary AS

WITH query_activities AS (
    SELECT *
    FROM dist_pg_stat_activity
    WHERE global_query_id IS NOT NULL AND global_query_id != ''
),

activities_with_compound_role AS (
    SELECT
        *,
        (COALESCE(role, 'unknown_role') || '-' || COALESCE(backend_type, 'unknown_type')) 
            AS compound_role
    FROM
        query_activities
)

SELECT
    gid.global_query_id,
    MAX(gid.query) FILTER (WHERE gid.role = 'coordinator') AS top_level_query,
    MAX(gid.usename) FILTER (WHERE gid.role = 'coordinator') AS username,
    MAX(gid.application_name) FILTER (WHERE gid.role = 'coordinator') AS application_name,
    MAX(gid.client_addr) FILTER (WHERE gid.role = 'coordinator') AS client_address,
    (NOW() - MIN(gid.query_start))::interval(3) AS total_duration,
    COUNT(*) AS involved_processes,
    COUNT(DISTINCT gid.nodename) AS distinct_nodes,
    MIN((gid.backend_xmin::text)::bigint) AS cluster_xmin_horizon,
    (SELECT STRING_AGG(
                state_summary.state || '(' || state_summary.count || '){' || array_to_string(state_summary.node_pids, ',') || '}',
                ', '
            )
     FROM (SELECT 
                state, 
                COUNT(*) as count, 
                ARRAY_AGG(nodename || ':' || pid ORDER BY nodename, pid) as node_pids
           FROM query_activities
           WHERE global_query_id = gid.global_query_id
           GROUP BY state
           ORDER BY state
          ) AS state_summary
    ) AS states_summary,

    (SELECT STRING_AGG(
                wait_summary.wait_event || '(' || wait_summary.count || '){' || array_to_string(wait_summary.node_pids, ',') || '}',
                ', '
            )
     FROM (SELECT 
                wait_event, 
                COUNT(*) as count, 
                ARRAY_AGG(nodename || ':' || pid ORDER BY nodename, pid) as node_pids
           FROM query_activities
           WHERE global_query_id = gid.global_query_id AND wait_event IS NOT NULL
           GROUP BY wait_event
           ORDER BY wait_event
          ) AS wait_summary
    ) AS waits_summary,
    
    (SELECT STRING_AGG(
                role_summary.compound_role 
                || '(' || role_summary.count || '){' || array_to_string(role_summary.pids, ',') || '}',
                ', '
            )
     FROM (SELECT 
                compound_role, 
                COUNT(*) as count, 
                ARRAY_AGG(pid ORDER BY pid) as pids
           FROM activities_with_compound_role
           WHERE global_query_id = gid.global_query_id AND compound_role IS NOT NULL
           GROUP BY compound_role
           ORDER BY compound_role
          ) AS role_summary
    ) AS backends_summary

FROM
    query_activities AS gid
GROUP BY
    gid.global_query_id
HAVING
    COUNT(*) FILTER (WHERE gid.state = 'active') > 0
ORDER BY
    total_duration DESC;

--
-- View: dist_pg_stat_query_details
-- Description: Presents detailed, ordered activity information for each
--              distributed query, making it easy to trace a query's execution
--              flow from coordinator to datanodes.
--
CREATE OR REPLACE VIEW dist_pg_stat_query_details AS
SELECT
    gid.global_query_id,
    gid.sessionid,
    gid.nodename,
    gid.role,
    gid.pid,
    gid.usename,
    gid.datname,
    gid.application_name,
    gid.client_addr,
    gid.backend_type,
    gid.state,
    gid.wait_event_type,
    gid.wait_event,
    gid.backend_xid,
    gid.backend_xmin,
    gid.query_start,
    gid.xact_start,
    gid.backend_start,
    gid.state_change,
    gid.query,
    gid.planstate
FROM
    dist_pg_stat_activity AS gid
WHERE
    gid.global_query_id IS NOT NULL AND gid.global_query_id != ''
ORDER BY
    gid.global_query_id,
    CASE gid.role
        WHEN 'coordinator' THEN 1
        WHEN 'datanode' THEN 2
        ELSE 3
    END,
    gid.nodename;

--
-- View: dist_pg_lock_waits_summary
-- Description: Summarizes each direct lock-waiting relationship into a single row,
--              enriched with both "scene" (DN) and "source" (CN) information
--              for both waiter and blocker.
--
CREATE OR REPLACE VIEW dist_pg_lock_waits_summary AS
WITH 
cn_activities AS (
    SELECT DISTINCT ON (gxid)
        gxid,
        nodename,
        usename,
        datname,
        application_name,
        client_addr,
        query AS top_level_query
    FROM 
        dist_pg_stat_activity
    WHERE 
        role = 'coordinator' AND gxid IS NOT NULL
    ORDER BY gxid, query_start DESC
)
SELECT
    lw.waiter_gxid,
    lw.blocking_gxid,

    waiter_cn.nodename AS waiter_source_node,
    waiter_cn.usename AS waiter_user_source,
    waiter_cn.client_addr AS waiter_client_source,
    waiter_cn.top_level_query AS waiter_query_source,

    blocker_cn.nodename AS blocker_source_node,
    blocker_cn.usename AS blocker_user_source,
    blocker_cn.client_addr AS blocker_client_source,
    blocker_cn.top_level_query AS blocker_query_source,

    JSONB_AGG(
        DISTINCT jsonb_build_object(
            'wait_happen_node', lw.node_name,
            'waiter_pid', lw.waiter_pid,
            'waiter_state', lw.waiter_state,
            'blocking_node_name', lw.blocking_node_name,
            'blocking_pid', lw.blocking_pid,
            'blocking_state', lw.blocking_state
        )
    ) AS wait_scene_details,

    STRING_AGG(
        DISTINCT lw.mode || '{' || lw.lock_target || '}', 
        ', '
    ) AS locks_waited

FROM
    dist_pg_locks_all_info AS lw
LEFT JOIN
    cn_activities AS waiter_cn ON lw.waiter_gxid = waiter_cn.gxid
LEFT JOIN
    cn_activities AS blocker_cn ON lw.blocking_gxid = blocker_cn.gxid
WHERE
    NOT lw.granted
GROUP BY
    lw.waiter_gxid,
    lw.blocking_gxid,
    waiter_cn.nodename,
    waiter_cn.usename,
    waiter_cn.client_addr,
    waiter_cn.top_level_query,
    blocker_cn.nodename,
    blocker_cn.usename,
    blocker_cn.client_addr,
    blocker_cn.top_level_query;

--
-- View: dist_pg_lock_wait_chains
-- Description: Recursively reconstructs and displays the full wait chain for
--              complex, multi-level lock contentions, directly identifying the
--              "root blocker".
--
CREATE OR REPLACE VIEW dist_pg_lock_wait_chains AS
WITH RECURSIVE 
lock_chains_raw AS (
    SELECT
        1 AS level,
        locks.waiter_gxid,
        locks.waiter_pid,
        locks.node_name AS waiter_node,
        locks.blocking_gxid,
        locks.blocking_pid,
        locks.blocking_node_name,
        locks.lock_target,
        
        ARRAY[locks.waiter_gxid] AS gxid_path,
        ARRAY[locks.waiter_pid || ':' || locks.node_name] AS physical_path,
        
        locks.waiter_gxid AS chain_head_gxid,
        (locks.waiter_gxid || '|' || locks.lock_target) AS chain_identifier
        
    FROM 
        dist_pg_locks_all_info AS locks
    WHERE 
        NOT locks.granted AND locks.blocking_gxid IS NOT NULL

    UNION ALL

    SELECT
        lc.level + 1,
        
        lc.blocking_gxid AS waiter_gxid,
        locks.waiter_pid,
        locks.node_name AS waiter_node,
        
        locks.blocking_gxid,
        locks.blocking_pid,
        locks.blocking_node_name,
        locks.lock_target,
        
        lc.gxid_path || lc.blocking_gxid,
        lc.physical_path || (locks.waiter_pid || ':' || locks.node_name),
        
        lc.chain_head_gxid,
        lc.chain_identifier
    FROM 
        lock_chains_raw lc
    JOIN 
        dist_pg_locks_all_info AS locks 
      ON lc.blocking_gxid = locks.waiter_gxid
    WHERE 
        NOT locks.granted AND locks.blocking_gxid IS NOT NULL
      AND NOT (lc.blocking_gxid = ANY(lc.gxid_path))
),
unique_longest_chains AS (
    SELECT DISTINCT ON (chain_identifier)
        chain_identifier,
        chain_head_gxid,
        level AS chain_length,
        gxid_path,
        physical_path,
        blocking_gxid AS root_blocker_gxid,
        blocking_pid AS root_blocker_pid,
        blocking_node_name AS root_blocker_node,
        lock_target
    FROM lock_chains_raw
    ORDER BY chain_identifier, level DESC, chain_head_gxid
),
cn_activities AS (
    SELECT DISTINCT ON (gxid)
        gxid, usename, client_addr, query AS top_level_query, nodename AS source_node
    FROM dist_pg_stat_activity
    WHERE role = 'coordinator' AND gxid IS NOT NULL
    ORDER BY gxid, query_start DESC
)
SELECT 
    ulc.chain_length,
    ulc.lock_target AS waiting_for,
    array_to_string(ulc.gxid_path, ' -> ') || ' -> ' || ulc.root_blocker_gxid AS full_wait_chain_gxid,
    array_to_string(ulc.physical_path, ' -> ') || ' -> ' || (ulc.root_blocker_pid || ':' || ulc.root_blocker_node) AS full_wait_chain_pids,
    
    head_cn.source_node AS chain_head_source_node,
    head_cn.usename AS chain_head_user,
    ulc.chain_head_gxid,
    
    root_cn.source_node AS root_blocker_source_node,
    root_cn.usename AS root_blocker_user,
    ulc.root_blocker_gxid,
    
    head_cn.top_level_query AS chain_head_query,
    root_cn.top_level_query AS root_blocker_query
FROM
    unique_longest_chains ulc
LEFT JOIN
    cn_activities AS head_cn ON ulc.chain_head_gxid = head_cn.gxid
LEFT JOIN
    cn_activities AS root_cn ON ulc.root_blocker_gxid = root_cn.gxid
ORDER BY
    ulc.chain_length DESC,
    ulc.chain_head_gxid;

--
-- View: dist_pg_deadlocks
-- Description: Detects and displays circular lock dependencies (deadlocks).
--              Serves as a powerful tool for auditing and analyzing deadlock patterns.
--
CREATE OR REPLACE VIEW dist_pg_deadlocks AS
WITH RECURSIVE lock_chain (
    waiter_gxid,
    waiter_pid,
    waiter_node,
    blocking_gxid,
    blocking_pid,
    blocking_node_name,
    path,
    is_cycle
) AS (
    SELECT
        locks.gxid,
        locks.pid,
        locks.node_name,
        locks.blocking_gxid,
        locks.blocking_pid,
        locks.blocking_node_name,
        ARRAY[locks.gxid],
        (locks.gxid = locks.blocking_gxid)
    FROM
        dist_pg_locks AS locks
    WHERE
        NOT locks.granted

    UNION ALL

    SELECT
        lc.blocking_gxid,
        lc.blocking_pid,
        lc.blocking_node_name,
        locks.blocking_gxid,
        locks.blocking_pid,
        locks.blocking_node_name,
        lc.path || lc.blocking_gxid,
        lc.blocking_gxid = ANY(lc.path)
    FROM
        lock_chain lc
    JOIN
        dist_pg_locks locks ON lc.blocking_gxid = locks.gxid AND lc.blocking_pid = locks.pid
    WHERE
        NOT locks.granted AND NOT lc.is_cycle
)
SELECT
    c.path || c.blocking_gxid AS deadlock_cycle_gxid,
    l.node_name, l.pid, l.gxid, l.lock_target, l.mode AS lock_mode,
    act.query AS waiter_query,
    l.blocking_node_name, l.blocking_pid, l.blocking_gxid,
    blocker_act.query AS blocking_query
FROM
    lock_chain c
JOIN
    dist_pg_locks l ON c.waiter_gxid = l.gxid AND c.waiter_pid = l.pid
JOIN
    dist_pg_stat_activity act ON l.pid = act.pid AND l.node_name = act.nodename
LEFT JOIN
    dist_pg_stat_activity blocker_act ON l.blocking_pid = blocker_act.pid AND l.blocking_node_name = blocker_act.nodename
WHERE
    c.is_cycle;

/******************************************************************************
 * Section 4: Grant Permissions
 ******************************************************************************/
 
GRANT SELECT ON dist_pg_stat_activity TO PUBLIC;
GRANT SELECT ON dist_pg_stat_activity_cn TO PUBLIC;
GRANT SELECT ON dist_pg_locks_raw TO PUBLIC;
GRANT SELECT ON dist_pg_locks TO PUBLIC;
GRANT SELECT ON dist_pg_locks_all_info TO PUBLIC;

GRANT SELECT ON dist_pg_stat_query_summary TO PUBLIC;
GRANT SELECT ON dist_pg_stat_query_details TO PUBLIC;
GRANT SELECT ON dist_pg_lock_waits_summary TO PUBLIC;
GRANT SELECT ON dist_pg_lock_wait_chains TO PUBLIC;
GRANT SELECT ON dist_pg_deadlocks TO PUBLIC;