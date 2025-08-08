/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause
 * 
 */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "CREATE EXTENSION pg_dist_stat_views" to load this file. \quit

/* Now redefine */
CREATE OR REPLACE FUNCTION dist_pg_stat_get_activity(
    IN sessionid text,
    IN coordonly bool,
    IN localonly bool,

    -- 在第一阶段，你可以保持和原来完全一样，以求功能对等。
    -- 在第二阶段（功能增强），你就可以在这里增加、删除或修改列了！
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
    OUT global_query_id text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- ===================================================================
-- ==                 Distributed Locks Information                 ==
-- ===================================================================

-- 定义新的SRF函数，这是用户查询的入口
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
    OUT gxid text
    -- OUT blocking_pid integer,
    -- OUT blocking_gxid text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'get_dist_pg_locks'
LANGUAGE C STRICT VOLATILE;

-- 你的首要目标是实现视图。等视图功能稳定后，再考虑是否需要实现这些管理功能。
-- 如果要保留，也需要把它们的名字和实现都改成你自己的版本。
/*
CREATE OR REPLACE FUNCTION pg_signal_session(text, integer, bool)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION pg_terminate_session(text)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION pg_cancel_session(text)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C;
*/

CREATE OR REPLACE VIEW dist_pg_stat_activity AS
  SELECT * FROM dist_pg_stat_get_activity(NULL, false, false);

CREATE OR REPLACE VIEW dist_pg_stat_activity_cn AS
  SELECT * FROM dist_pg_stat_get_activity(NULL, true, false);

-- ===================================================================
-- 视图名称: dist_pg_stat_query_summary
-- 视图目标: 提供一个以“全局查询ID (global_query_id)”为核心的聚合摘要视图。
--          每一行代表一个正在运行的完整分布式查询，将所有参与节点的
--          关键信息进行语义化整合，旨在帮助DBA快速定位慢查询、
--          识别性能瓶颈和理解分布式查询的执行模式。
-- ===================================================================
CREATE OR REPLACE VIEW dist_pg_stat_query_summary AS

-- 使用 WITH 子句 (CTE - Common Table Expression) 定义一个基础数据集。
-- 这样做可以避免在后续的多个子查询中重复书写 FROM 和 WHERE 子句，
-- 使代码更简洁，也可能让查询规划器更好地优化。
WITH query_activities AS (
    SELECT *
    FROM dist_pg_stat_activity
    -- 核心过滤条件：只关心那些已经被我们的扩展模块成功打上 GID 标签的活动。
    -- 这会自动排除掉后台进程、空闲的连接池连接等“噪音”。
    WHERE global_query_id IS NOT NULL AND global_query_id != ''
),

-- 2. 【核心】直接、纯粹地拼接字段，创建“复合角色”
activities_with_compound_role AS (
    SELECT
        *,
        -- 使用 COALESCE 确保即使某个字段为 NULL，拼接也能正常工作
        -- 将 role 和 backend_type 直接用 '-' 连接
        (COALESCE(role, 'unknown_role') || '-' || COALESCE(backend_type, 'unknown_type')) 
            AS compound_role
    FROM
        query_activities
)

SELECT
    -- 【1. 核心关联ID】
    -- 这是聚合的唯一键，代表一次完整的分布式查询。
    gid.global_query_id,

    -- 【2. 查询发起者信息】
    -- 通过 FILTER 子句，我们只从角色为 'coordinator' 的记录中提取这些信息，
    -- 因为只有协调节点才能代表查询的“源头”。
    MAX(gid.query) FILTER (WHERE gid.role = 'coordinator') AS top_level_query,
    MAX(gid.usename) FILTER (WHERE gid.role = 'coordinator') AS username,
    MAX(gid.application_name) FILTER (WHERE gid.role = 'coordinator') AS application_name,
    MAX(gid.client_addr) FILTER (WHERE gid.role = 'coordinator') AS client_address,

    -- 【3. 性能与事务指标】
    -- 这个查询从在协调节点上开始，到当前的总耗时。是定位慢查询最直接的指标。
    (NOW() - MIN(gid.query_start))::interval(3) AS total_duration,
    -- 参与本次查询的进程（Process）总数
    COUNT(*) AS involved_processes,
    -- 参与本次查询的独立节点（CN/DN）的总数。
    COUNT(DISTINCT gid.nodename) AS distinct_nodes,
    -- 整个分布式查询的最小 xmin。这个值长时间不推进，可能导致表膨胀。
    -- 【修正】在聚合前，将 xid 类型强制转换为 bigint，先转test字符串
    MIN((gid.backend_xmin::text)::bigint) AS cluster_xmin_horizon,

    /*
    xid (Transaction ID) 是一个PostgreSQL内部专用的、32位的无符号整数类型。
    虽然它本质上是数字，但它有特殊的“回卷 (wraparound)”行为。
    为了防止用户对它进行无意义的数学运算（比如求平均事务ID），
    PostgreSQL 没有为 xid 类型预定义 MIN 或 MAX 这些聚合函数。
    */
    
    /*
     * ===================================================================
     * 【4. 语义化聚合展示区】
     * 通过子查询和字符串/数组聚合函数，将多行明细数据浓缩成高度可读的文本摘要。
     * ===================================================================
     */

    /*
     * 【状态摘要】
     * 将所有参与进程的状态进行分组统计，以 "状态名(数量){节点-进程列表}" 的格式呈现。
     * 示例: "active(7){cn001:pid1,dn001:pid2,dn001:pid3...}"
     */
    (SELECT STRING_AGG(
                state_summary.state || '(' || state_summary.count || '){' || array_to_string(state_summary.node_pids, ',') || '}',
                ', '
            )
     FROM (SELECT 
                state, 
                COUNT(*) as count, 
                -- 将 "nodename:pid" 拼接起来，再聚合成数组
                ARRAY_AGG(nodename || ':' || pid ORDER BY nodename, pid) as node_pids
           FROM query_activities
           WHERE global_query_id = gid.global_query_id
           GROUP BY state
           ORDER BY state
          ) AS state_summary
    ) AS states_summary,

    /*
     * 【等待事件摘要】
     * 将所有正在发生的等待事件进行分组统计，聚合信息中包含具体的PID，格式与状态摘要类似。
     * 示例: "PgSleep(2){dn001:pid1,dn002:pid2}"
     * 这是定位性能瓶颈最关键的字段之一。
     */
    (SELECT STRING_AGG(
                wait_summary.wait_event || '(' || wait_summary.count || '){' || array_to_string(wait_summary.node_pids, ',') || '}',
                ', '
            )
     FROM (SELECT 
                wait_event, 
                COUNT(*) as count, 
                -- 将 "nodename:pid" 拼接起来，再聚合成数组
                ARRAY_AGG(nodename || ':' || pid ORDER BY nodename, pid) as node_pids
           FROM query_activities
           WHERE global_query_id = gid.global_query_id AND wait_event IS NOT NULL
           GROUP BY wait_event
           ORDER BY wait_event
          ) AS wait_summary
    ) AS waits_summary,
    
    /*
     * 【后端参与者摘要】
     * 将分布式角色(role)和后端类型(backend_type)结合，提供最精确的参与者画像。
     * 格式: "角色-类型(数量){PID列表}"
     * 示例: "coordinator-client backend(1){123}, datanode-parallel worker(6){...}"
     * 揭示了查询的并行执行模式。
     */
    (SELECT STRING_AGG(
                -- 直接使用我们拼接好的 compound_role 进行展示
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
           GROUP BY compound_role -- 【只按拼接后的新字段分组！】
           ORDER BY compound_role
          ) AS role_summary
    ) AS backends_summary

FROM
    query_activities AS gid
GROUP BY
    gid.global_query_id
-- 【最终过滤】
-- 使用 HAVING 子句，确保只显示那些【当前至少还有一个进程在 active 状态】的查询。
-- 这解决了因统计延迟而导致的“幽灵摘要”（已结束查询的摘要残留）问题，
-- 使得这个视图成为一个真正的“实时”仪表盘。
HAVING
    COUNT(*) FILTER (WHERE gid.state = 'active') > 0;

-- ===================================================================
-- 【新增】分布式查询活动明细视图 (按 GID 排序)
-- ===================================================================
CREATE OR REPLACE VIEW dist_pg_stat_query_details AS
SELECT
    -- 核心关联与身份信息
    gid.global_query_id,
    gid.sessionid,
    gid.nodename,
    gid.role,
    gid.pid,
    gid.usename,
    gid.datname,
    gid.application_name, -- 【新增】
    gid.client_addr,
    gid.backend_type,     -- 【新增】

    -- 状态与等待信息
    gid.state,
    gid.wait_event_type,
    gid.wait_event,
    
    -- 事务信息
    gid.backend_xid,      -- 【新增】
    gid.backend_xmin,     -- 【新增】
    
    -- 时间戳信息
    gid.query_start,
    gid.xact_start,
    gid.backend_start,
    gid.state_change,
    
    -- 查询与计划文本 (通常比较长，放在后面)
    gid.query,
    gid.planstate
FROM
    dist_pg_stat_activity AS gid
WHERE
    -- 同样，我们只关心那些有 GID 的、真正的分布式查询活动
    gid.global_query_id IS NOT NULL AND gid.global_query_id != ''
ORDER BY
    -- 【核心排序逻辑】
    -- 1. 首先，按照 global_query_id 排序，把同一个查询的所有活动都聚在一起。
    gid.global_query_id,
    
    -- 2. 在同一个 GID 内部，我们进行二次排序，让结果更有条理。
    --    这里用一个 CASE 语句来给节点角色赋予一个排序优先级：
    --    CN (1) > DN (2) > 其他。
    --    这确保了发起查询的 CN 记录总是出现在每个分组的最前面。
    CASE gid.role
        WHEN 'coordinator' THEN 1
        WHEN 'datanode' THEN 2
        ELSE 3
    END,

    -- 3. 最后，按节点名排序，让输出更稳定。
    gid.nodename;

-- 创建最终用户视图：dist_pg_locks
-- 创建一个基础的、只封装C函数的锁视图
CREATE OR REPLACE VIEW dist_pg_locks_raw AS
  SELECT * FROM get_dist_pg_locks(false);

-- 创建最终的、在SQL层推断阻塞关系的 dist_pg_locks 视图
CREATE OR REPLACE VIEW dist_pg_locks AS
WITH 
waiters AS (
    SELECT * FROM dist_pg_locks_raw WHERE NOT granted
),
holders AS (
    SELECT * FROM dist_pg_locks_raw WHERE granted
),
blocking_pairs AS (
    SELECT DISTINCT
        w.node_name AS waiter_node,
        w.pid AS waiter_pid,
        h.node_name AS blocking_node_name,
        h.pid AS blocking_pid,
        h.gxid AS blocking_gxid
    FROM waiters w JOIN holders h 
      ON w.database = h.database AND (
         -- 精确匹配表、页、元组锁
         (w.relation IS NOT NULL AND w.relation = h.relation) OR
         -- 精确匹配事务ID锁
         (w.locktype = 'transactionid' AND w.transactionid = h.transactionid) OR
         -- 精确匹配 advisory lock (假设 classid 和 objid 相同)
         (w.locktype = 'advisory' AND w.classid = h.classid AND w.objid = h.objid)
      )
    WHERE w.pid != h.pid
)
-- 最终整合输出
SELECT 
    -- 核心锁信息
    raw.node_name,
    raw.granted,
    raw.gxid,
    raw.pid,
    raw.locktype,
    raw.mode,
    
    -- 锁定的对象
    CASE 
        WHEN raw.locktype = 'relation' THEN raw.relation::regclass::text 
        WHEN raw.locktype = 'transactionid' THEN raw.transactionid::text
        ELSE pg_describe_object(raw.classid, raw.objid, raw.objsubid)
    END AS lock_target,
    
    -- 【核心】推断出的阻塞者信息
    bp.blocking_node_name,
    bp.blocking_pid,
    bp.blocking_gxid,

    -- 保留一些原始列用于深入分析
    raw.relation,
    raw.transactionid AS local_xid,
    raw.virtualtransaction
FROM 
    dist_pg_locks_raw AS raw
-- 只做一次 LEFT JOIN，关联我们内部计算出的阻塞关系
LEFT JOIN
    blocking_pairs bp ON raw.pid = bp.waiter_pid AND raw.node_name = bp.waiter_node;

-- ===================================================================
-- ==      Advanced Lock Analysis Views (Wait Chains & Deadlocks)   ==
-- ===================================================================

-- in pg_dist_stat_views--1.0.sql

-- 视图 1: 完整的分布式锁等待链 (修正版)
CREATE OR REPLACE VIEW dist_pg_lock_wait_chains AS
WITH RECURSIVE lock_chain AS (
    -- 递归的起点 (Anchor)
    SELECT
        1 AS level,
        locks.gxid AS waiter_gxid,
        locks.pid AS waiter_pid,
        locks.node_name AS waiter_node,
        locks.blocking_gxid,
        locks.blocking_pid,
        locks.blocking_node_name,
        ARRAY[locks.gxid] AS path,
        (locks.node_name || ':' || locks.pid) AS path_detail
    FROM
        dist_pg_locks AS locks
    WHERE
        NOT locks.granted

    UNION ALL

    -- 递归的递推 (Recursive Step)
    SELECT
        lc.level + 1,
        lc.blocking_gxid,
        lc.blocking_pid,
        lc.blocking_node_name,
        locks.blocking_gxid,
        locks.blocking_pid,
        locks.blocking_node_name,
        lc.path || lc.blocking_gxid,
        lc.path_detail || ' -> ' || (locks.node_name || ':' || locks.pid)
    FROM
        lock_chain lc
    JOIN
        dist_pg_locks locks ON lc.blocking_gxid = locks.gxid AND lc.blocking_pid = locks.pid
    WHERE
        NOT locks.granted AND NOT (locks.gxid = ANY(lc.path))
)
-- 最终输出: 将递归结果与锁信息和【活动信息】进行关联
SELECT
    c.level AS wait_level,
    c.path_detail AS wait_chain,
    -- 等待者信息
    c.waiter_node,
    c.waiter_pid,
    c.waiter_gxid,
    -- 【核心修正】从 dist_pg_stat_activity (别名 act) 获取 state 和 query
    act.state AS waiter_state,
    (now() - act.query_start)::interval(3) AS waiter_duration,
    act.query AS waiter_query,
    -- 锁信息 (来自 dist_pg_locks, 别名 l)
    l.lock_target,
    l.mode AS lock_mode,
    -- 阻塞者信息
    c.blocking_node_name,
    c.blocking_pid,
    c.blocking_gxid
FROM
    lock_chain c
-- 关联 dist_pg_locks 以获取锁的详细信息
JOIN
    dist_pg_locks l ON c.waiter_gxid = l.gxid AND c.waiter_pid = l.pid
-- 【核心修正】关联 dist_pg_stat_activity 以获取等待者的活动信息
JOIN
    dist_pg_stat_activity act ON c.waiter_pid = act.pid AND c.waiter_node = act.nodename
ORDER BY
    c.path, c.level;


-- 视图 2: 分布式死锁检测 (最终修正版)
CREATE OR REPLACE VIEW dist_pg_deadlocks AS
WITH RECURSIVE lock_chain ( -- 【核心修正】在这里显式声明所有列名
    waiter_gxid,
    waiter_pid,
    waiter_node,
    blocking_gxid,
    blocking_pid,
    blocking_node_name,
    path,
    is_cycle -- 确保 is_cycle 在这里被声明
) AS (
    -- 递归的起点 (Anchor)
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

    -- 递归的递推 (Recursive Step)
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
        NOT locks.granted AND NOT lc.is_cycle -- 这里的 lc.is_cycle 引用现在是绝对清晰的
)
-- 最终输出
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

-- 授权
GRANT SELECT ON dist_pg_stat_activity TO PUBLIC;
GRANT SELECT ON dist_pg_stat_activity_cn TO PUBLIC;
GRANT SELECT ON dist_pg_stat_query_summary TO PUBLIC;
GRANT SELECT ON dist_pg_stat_query_details TO PUBLIC;
GRANT SELECT ON dist_pg_locks_raw TO PUBLIC;
GRANT SELECT ON dist_pg_locks TO PUBLIC;
GRANT SELECT ON dist_pg_lock_wait_chains TO PUBLIC; -- 授权新视图
GRANT SELECT ON dist_pg_deadlocks TO PUBLIC;      -- 授权新视图