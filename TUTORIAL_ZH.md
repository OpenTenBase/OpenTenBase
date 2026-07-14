# OpenTenBase 新手导览：从部署到理解分布式架构

本文面向已经完成编译和 `opentenbase_ctl install`、但还不熟悉 OpenTenBase 架构的读者。目标是帮助你建立整体认识，并完成第一次连接、建表和查询。

> 本文命令中的安装路径、IP 和端口仅为示例。请以
> `opentenbase_ctl status -c opentenbase_config.ini` 的实际输出为准。

## 1. 理解 OpenTenBase

OpenTenBase 是兼容 PostgreSQL 生态的分布式关系型数据库：应用连接协调节点（CN）负责将 SQL 分发给保存数据的数据节点（DN），全局事务管理器（GTM）为跨节点事务提供全局事务标识和一致性快照。

一个最小分布式实例通常包含 1 个 GTM、1 个 CN 和 1 个 DN。它具备完整的分布式组件，但只有增加到多个 DN 后，才能真正体现数据分片、容量扩展和并行计算的优势。

## 2. 架构理解图

```text
                         申请全局事务 ID / 快照
                    +----------------------------+
                    |                            v
+----------------+  |  +----------------+   +---------+
| 应用 / psql /  |---->| Coordinator CN |<->|   GTM   |
| JDBC / libpq   | SQL | SQL 入口与协调器 |   | 全局事务 |
+----------------+     +-------+--------+   +---------+
                              |
                   拆分、分发 SQL，汇总结果
                    +---------+---------+
                    |                   |
                    v                   v
              +-----------+       +-----------+
              | DN1       |       | DN2       |
              | 保存分片 A |       | 保存分片 B |
              +-----------+       +-----------+
                    |                   |
                    v                   v
              DN1 slave           DN2 slave
              （可选副本）          （可选副本）
```

一次典型查询的路径如下：

1. 客户端通过 PostgreSQL 协议把 SQL 发给 CN。
2. CN 解析和规划 SQL，并根据表的分布规则确定目标 DN。
3. CN 把查询片段发送到一个或多个 DN。
4. DN 在本地扫描数据、使用索引并计算局部结果。
5. CN 汇总、排序或聚合结果，再返回给客户端。
6. 如果事务跨越多个节点，GTM 为它提供全局事务信息和一致性视图。

## 3. 核心术语表

| 术语 | 面向新手的解释 |
| --- | --- |
| **实例（Instance）** | 由一份 `opentenbase_config.ini` 描述并由 `opentenbase_ctl` 管理的一套数据库。一个分布式实例可以包含 GTM、多个 CN、多个 DN 及其备节点。 |
| **Coordinator（CN）** | 应用访问分布式集群的 SQL 入口。CN 保存元数据，负责解析、规划、分发 SQL 和汇总结果；通常不保存用户表数据。日常业务不要绕过 CN 直接修改 DN。 |
| **DataNode（DN）** | 真正保存用户数据并执行本地扫描、连接、聚合等操作的节点。增加 DN master 可以分散数据量和计算压力。 |
| **GTM** | Global Transaction Manager。为集群事务提供全局事务 ID、快照等全局信息，使不同 CN、DN 能获得一致的事务视图。 |
| **分布式模式（distributed）** | 配置中 `type=distributed`。需要 GTM、CN 和 DN，适合通过多个 DN 进行水平扩展。客户端连接 CN。 |
| **集中式模式（centralized）** | 配置中 `type=centralized`。部署工具会忽略 GTM 和 CN 配置，只部署一组 DataNode，适合不需要分布式拆分的场景。 |
| **Node Group（节点组）** | 一组 DN 的逻辑集合，用于规定表可以放在哪些数据节点上。`opentenbase_ctl install` 会为配置中的 DN 创建默认节点组。 |
| **分布键（Distribution Key）** | 决定一行数据放到哪个 DN 的列。例如按 `user_id` 做 HASH 分布，相同用户的数据会被稳定路由到相应 DN。分布键应尽量均匀，并贴合常用查询和 JOIN。 |
| **数据分片（Sharding）** | 多个 DN master 保存不同的数据子集，用于扩展容量和吞吐量。例如 DN1 保存分片 A，DN2 保存分片 B。 |
| **主节点与备节点（Master/Slave）** | slave 是 master 的数据副本，主要用于高可用；它与分片不同。增加 slave 通常不会增加可写数据容量，增加 DN master 才会增加分片。 |
| **分布式执行计划** | CN 将一个 SQL 转换成多个节点上的执行任务。包含分布键的过滤条件可能触发节点裁剪；跨分片 JOIN、排序和聚合则可能产生节点间数据交换。 |
| **分布式事务** | 一个事务同时访问多个 DN 时，需要保证这些节点上的操作整体成功或整体失败。CN、DN 和 GTM 共同完成全局事务协调。 |
| **opentenbase_ctl** | OpenTenBase 5 的集群部署和运维工具，支持安装、删除、启停、状态查询、远程 shell、SQL 执行和 GUC 参数管理。它是管理工具，不是数据库服务进程本身。 |

## 4. 分布键为什么重要

假设 `orders` 按 `user_id` 分布：

```sql
CREATE TABLE orders (
    user_id    bigint NOT NULL,
    order_id   bigint NOT NULL,
    amount     numeric(12, 2) NOT NULL,
    created_at timestamp NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (user_id, order_id)
) DISTRIBUTE BY HASH(user_id);
```

查询指定用户时，CN 可以根据 `user_id` 推算目标 DN：

```sql
SELECT * FROM orders WHERE user_id = 1001;
```

如果查询条件不包含分布键，CN 可能需要访问保存该表的所有 DN：

```sql
SELECT * FROM orders WHERE amount > 100;
```

选择分布键时，优先考虑：

- 值分布均匀，避免某个 DN 保存大部分数据；
- 经常出现在等值查询条件中；
- 经常作为相关大表的 JOIN 字段；
- 写入后很少修改；
- 不使用性别、状态等取值很少的字段。

如果 `users` 和 `orders` 都按 `user_id` HASH 分布，同一用户的两张表记录更容易位于同一个 DN，JOIN 时可减少跨节点传输：

```sql
CREATE TABLE users (
    user_id  bigint PRIMARY KEY,
    username text NOT NULL
) DISTRIBUTE BY HASH(user_id);
```

对于 HASH 分布表，主键、唯一约束以及相关参照约束通常需要包含分布键，才能在多个 DN 上正确保证约束。

## 5. 快速上手

### 5.1 查看实例状态

在部署目录执行：

```bash
./opentenbase_bin_v5.0/bin/opentenbase_ctl status -c opentenbase_config.ini
```

状态输出会列出每个节点是否为 `Running`，并在 `Master CN Connection Info` 中给出环境变量和 `psql` 连接命令。`Unknown` 通常表示工具无法通过 SSH 获取节点状态，也可能是服务器或 SSH 配置异常。

### 5.2 连接 CN

复制状态输出中的实际连接命令，例如：

```bash
psql -h 127.0.0.1 -p 11003 -U opentenbase -d postgres
```

进入 `psql` 后，可执行以下命令确认连接信息：

```sql
SELECT version();
```

### 5.3 查看节点和节点组

以下查询应在 CN 上执行：

```sql
SELECT node_name, node_type, node_host, node_port
FROM pgxc_node
ORDER BY node_type, node_name;

SELECT * FROM pgxc_group;
```

### 5.4 创建练习数据

```sql
CREATE DATABASE learning;
```

使用 `\c learning` 切换数据库，然后执行：

```sql
CREATE TABLE users (
    user_id  bigint PRIMARY KEY,
    username text NOT NULL
) DISTRIBUTE BY HASH(user_id);

CREATE TABLE orders (
    user_id  bigint NOT NULL,
    order_id bigint NOT NULL,
    amount   numeric(12, 2) NOT NULL,
    PRIMARY KEY (user_id, order_id)
) DISTRIBUTE BY HASH(user_id);

CREATE TABLE order_status (
    status_id   integer PRIMARY KEY,
    status_name text NOT NULL
) DISTRIBUTE BY REPLICATION;

INSERT INTO users VALUES (1, '张三'), (2, '李四');
INSERT INTO orders VALUES
    (1, 10001, 25.50),
    (1, 10002, 99.00),
    (2, 10003, 36.80);

SELECT u.username, sum(o.amount) AS total_amount
FROM users AS u
JOIN orders AS o ON o.user_id = u.user_id
GROUP BY u.username
ORDER BY u.username;
```

### 5.5 观察查询如何分发

对比包含和不包含分布键的查询：

```sql
EXPLAIN VERBOSE
SELECT * FROM orders WHERE user_id = 1;

EXPLAIN VERBOSE
SELECT * FROM orders WHERE amount > 30;
```

重点观察执行计划访问了哪些远端节点。第一条查询包含 HASH 分布键，通常更容易定位目标 DN；第二条查询可能需要扫描全部相关 DN。具体计划受版本、节点组和优化器决策影响。

## 6. `opentenbase_ctl` 常用操作

```bash
# 查看帮助
./opentenbase_bin_v5.0/bin/opentenbase_ctl --help

# 安装实例
./opentenbase_bin_v5.0/bin/opentenbase_ctl install -c opentenbase_config.ini

# 查看整个实例状态
./opentenbase_bin_v5.0/bin/opentenbase_ctl status -c opentenbase_config.ini

# 启动或停止实例
./opentenbase_bin_v5.0/bin/opentenbase_ctl start -c opentenbase_config.ini
./opentenbase_bin_v5.0/bin/opentenbase_ctl stop -c opentenbase_config.ini
```

执行 `delete` 会删除实例数据，不能把它当作普通停止命令。生产环境执行启停、删除、故障切换或节点变更前，应先确认备份和操作范围。

## 7. FAQ

### 为什么应当连接 CN，而不是直接连接 DN？

CN 知道表的分布信息，会把 SQL 正确路由到相关 DN 并汇总结果。直接在某个 DN 修改数据可能只改变一个本地分片，绕过全局路由和协调，造成集群逻辑数据不一致。直接连接 DN 通常只用于受控的诊断或运维。

### 增加 slave 是否等于扩容？

不等于。slave 是 master 的副本，主要解决高可用问题；增加 DN master 并把数据分布到更多 master，才是分片意义上的水平扩容。

### GTM 是否保存业务表数据？

不保存。GTM 管理全局事务 ID、快照等事务相关的全局信息，不承担业务表的数据存储。

### 所有小表都应该使用 REPLICATION 吗？

不是。复制表适合小、读多写少且经常与分布表 JOIN 的表。更新频繁或不断增长的表复制到所有 DN，会放大写入与存储成本。

### OpenTenBase 与 PostgreSQL 的使用体验

OpenTenBase 保留了 PostgreSQL 协议、`psql`、SQL、MVCC、WAL、角色权限和常用驱动等大量能力。但分布式环境还需要考虑分布键、跨节点 JOIN、全局约束、分布式事务、扩缩容和节点故障等问题。应用开发者应当理解这些差异，并在设计表结构和 SQL 时考虑分布式特性。

## 8. 延伸阅读

- 项目中文快速安装说明：[`README_ZH.md`](README_ZH.md)
- `opentenbase_ctl` 使用说明：[`contrib/opentenbase_ctl/README.md`](contrib/opentenbase_ctl/README.md)
- [应用接入指南](https://docs.opentenbase.org/guide/02-access/)
- [基本使用](https://docs.opentenbase.org/guide/03-basic-usage/)
- [高级使用](https://docs.opentenbase.org/guide/04-advanced-use/)

<!--
Copyright (c) 2026 OpenTenBase Contributors
SPDX-License-Identifier: BSD-3-Clause
-->
