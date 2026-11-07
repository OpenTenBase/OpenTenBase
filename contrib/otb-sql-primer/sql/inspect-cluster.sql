-- ============================================================================
-- inspect-cluster.sql —— OpenTenBase 集群检查 SQL（带注释可运行版）
-- 来源：contrib/otb-health/otb_health.sh（第 42、47、55–57 行）
-- 逐句解释见同目录上级的 ../sql-primer.md
--
-- 运行方式（在能连上 CN 的机器上）：
--   psql -h 127.0.0.1 -p 11003 -U opentenbase -d postgres -f inspect-cluster.sql
--   （端口/用户/库名按你的集群实际修改；参考 otb_health.sh 第 21–24 行的默认值）
--
-- 注意：查询 [1][2] 只读，可放心执行；[3] 是建表冒烟测试，会写数据，
--       不想写库时请跳过 [3]（等价于 bash otb_health.sh --no-write）。
-- ============================================================================

-- [1] 集群节点一览：有哪些节点、各是什么角色、端口和地址
--     （原文见 otb_health.sh 第 42 行）
SELECT node_name, node_type, node_port, node_host
FROM pgxc_node
ORDER BY node_type;

-- [2] 节点分组：建分布表 to group <组名> 时引用的组都列在这里
--     （原文见 otb_health.sh 第 47 行）
SELECT * FROM pgxc_group;

-- [3] 分布式建表冒烟测试（原文见 otb_health.sh 第 55–57 行）
--     DISTRIBUTE BY SHARD(id) = 按 id 哈希分片；TO GROUP = 落到该 DN 组
--     ⚠️ 会真实建表写数据，用完即删；只读检查可注释掉本段。
DROP TABLE IF EXISTS otb_health_check;
CREATE TABLE otb_health_check(id int, name text)
    DISTRIBUTE BY SHARD(id) TO GROUP default_group;
INSERT INTO otb_health_check VALUES (1,'ok'),(2,'good');
SELECT * FROM otb_health_check ORDER BY id;
DROP TABLE otb_health_check;
