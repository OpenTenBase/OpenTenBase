# OpenTenBase 基础性能测试：问题与排障记录

本文记录搭建和运行基础性能测试时实际遇到的三个问题。它们都发生在正式测量之前，并已解决；失败运行产生的数据没有计入 [`REPORT.md`](REPORT.md)中的正式结果。

## 测试版本

```text
Git commit:
b612d77cbfd4d762f20c54c35f7caf09d57ef098

postgres:
PostgreSQL 10.0 @ OpenTenBase_v5.0 (commit: b612d77cb)

psql:
PostgreSQL 10.0 @ OpenTenBase_v5.0 (commit: b612d77cb)

pgbench:
PostgreSQL 10.0 @ OpenTenBase_v5.0 (commit: b612d77cb)
```

## 1. `node_forward_port` 为 0，复制表写入失败

### 现象

运行数据加载脚本：

```bash
psql -f benchmark/sql/01_load_data.sql
```

第一条向复制表 `bench_categories` 写入数据的语句失败：

```text
WARNING: primary datanode connection dn0003 ... is released, the current transaction will be aborted
WARNING: primary datanode connection dn0001 ... is released, the current transaction will be aborted
ERROR: terminating connection due to administrator command
```

虽然错误看起来像管理员主动停止了 DN，但执行测试时并没有执行节点停止命令。

### 诊断

DN 日志提供了更直接的原因：

```text
the conn is not inited nodename cn0001 host 127.0.0.1 forward port 0
because tcp fails to send data, force kill SIGTERM to pid ...
```

查询节点目录：

```sql
SELECT node_name,
       node_type,
       node_host,
       node_port,
       node_forward_port
FROM pgxc_node
ORDER BY node_type, node_name;
```

发现 CN 和 DN 的 `node_forward_port` 为 `0`。与此同时，各节点`postgresql.conf` 中的 Forward Port 已正确配置：

| 节点 | 服务端口 | Pooler 端口 | Forward Port |
| --- | ---: | ---: | ---: |
| cn0001 | 11003 | 11004 | 11005 |
| dn0001 | 11006 | 11007 | 11008 |
| dn0002 | 11009 | 11010 | 11011 |
| dn0003 | 11012 | 11013 | 11014 |

因此，问题是节点目录没有保存相应的 Forward Port。

### 原因判断

根据日志和源码检查，初步判断 `opentenbase_ctl` 构造节点映射 SQL 时遗漏了`FORWARD` 参数。

[`cluster.cpp`](../contrib/opentenbase_ctl/src/cluster/cluster.cpp) 中`build_create_pgxz_node_cmd()` 已能取得每个节点的端口信息，但当时生成的 SQL没有包含已经分配好的 `FORWARD` 值。这是基于当前 commit 的初步定位，需要进一步核对版本设计

### 临时解决方法

为所有 CN 和 DN 补充正确的节点映射，例如：

```sql
ALTER NODE cn0001
WITH (
    HOST = '127.0.0.1',
    PORT = 11003,
    FORWARD = 11005
);

ALTER NODE dn0001
WITH (
    HOST = '127.0.0.1',
    PORT = 11006,
    FORWARD = 11008
);

ALTER NODE dn0002
WITH (
    HOST = '127.0.0.1',
    PORT = 11009,
    FORWARD = 11011
);

ALTER NODE dn0003
WITH (
    HOST = '127.0.0.1',
    PORT = 11012,
    FORWARD = 11014
);

SELECT pgxc_pool_reload();
```

由于 `pgxc_node` 是节点本地系统目录，本次修复最初在旧测试数据库的 CN 和三个 DN 上分别执行。旧的 9,020 行测试数据库后来已经删除；当前 5,000,000 行正式结果使用 `database`。具体 IP 和端口必须以自己的集群配置为准。

修复后再次查询 `pgxc_node`，确认 CN 和三个 DN 的 Forward Port 分别为`11005`、`11008`、`11011`、`11014`。


## 2. 当前版本不接受 `DISTRIBUTE BY HASH`

### 现象
最初使用以下语句建表：

```sql
CREATE TABLE bench_users (
    user_id bigint PRIMARY KEY,
    username text NOT NULL
) DISTRIBUTE BY HASH(user_id);
```

数据库返回：

```text
ERROR: Cannot support distribute type: Hash
```

### 原因判断

OpenTenBase 的表分布语法可能随版本、分支或构建模式不同。本次使用的 commit不接受 `HASH`，但接受 `SHARD`。因此不能只根据其他 PostgreSQL-XC/XL 或OpenTenBase 版本的示例假设当前构建支持相同语法。(这个记录只能证明当前测试版本的实际行为)

### 解决方法

将测试表改为当前版本支持的语法：

```sql
CREATE TABLE bench_users (
    user_id     bigint PRIMARY KEY,
    username    text NOT NULL,
    region_id   integer NOT NULL,
    created_at  timestamp NOT NULL
) DISTRIBUTE BY SHARD(user_id);
```

其他大表也分别使用 `DISTRIBUTE BY SHARD(user_id)`
或 `DISTRIBUTE BY SHARD(payment_id)`。

小型字典表仍使用 `DISTRIBUTE BY REPLICATION`

### 验证

建表完成后查询 `pgxc_class`：

```sql
SELECT c.relname,
       x.pclocatortype,
       x.nodeoids
FROM pgxc_class AS x
JOIN pg_class AS c ON c.oid = x.pcrelid
WHERE c.relname LIKE 'bench_%'
ORDER BY c.relname;
```

结果中：
- `bench_users`、`bench_orders`、`bench_payments` 的定位类型为 `S`；
- `bench_categories` 的定位类型为 `R`；
- 四张表均关联 DN1、DN2、DN3。

## 3. pgbench 提示标准表不存在

### 现象

首次运行自定义点查询：

```bash
pgbench \
  -f benchmark/workloads/point_select.sql \
  -c 1 -j 1 -T 60 -P 10
```

测试开始前出现：

```text
starting vacuum...ERROR: relation "pgbench_branches" does not exist
ERROR: relation "pgbench_tellers" does not exist
ERROR: relation "pgbench_history" does not exist
(ignoring this error and continuing anyway)
```

随后 pgbench 正常输出进度、TPS 和延迟。

### 原因

pgbench 自带一套标准数据模型，包括pgbench_accounts、pgbench_branches、pgbench_tellers 和 pgbench_history。

这些表通常由 `pgbench -i` 创建。本测试没有使用标准数据模型，而是使用`bench_users`、`bench_orders` 等自定义表。pgbench 默认仍尝试在开始测试前维护它自己的标准表，所以报告表不存在。这是 pgbench 的正常行为。

### 解决方法

对所有自定义 workload 增加 `-n`即 `--no-vacuum`，表示测试开始前不要处理标准 pgbench 表。：

```bash
pgbench -n \
  -f benchmark/workloads/point_select.sql \
  -c 1 -j 1 -T 20 -P 5
```
