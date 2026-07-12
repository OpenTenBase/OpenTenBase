# OpenTenBase 架构术语表与新手导览

本文档面向首次接触 OpenTenBase 的读者，用通俗语言解释核心架构概念。建议先通读一遍，再对照 [README_ZH.md](../README_ZH.md) 的部署步骤实操，理解会更深入。

---

## 一、架构全景图

OpenTenBase 是一个**Shared-Nothing 分布式数据库**，集群由三种角色组成：

```
                          ┌──────────────────────┐
                          │      用户 / 应用       │
                          └──────────┬───────────┘
                                     │ psql / JDBC / ODBC
                                     ▼
             ┌───────────────────────────────────────────┐
             │           CN (Coordinator / 协调节点)       │
             │   ┌──────┐  ┌──────┐       ┌──────┐      │
             │   │ CN001│  │ CN002│  ...  │ CN00N│      │
             │   └──┬───┘  └──┬───┘       └──┬───┘      │
             │      │         │               │          │
             │      │    接收 SQL，生成分布式执行计划       │
             │      │    不存储用户数据（仅元数据）          │
             └──────┼─────────┼───────────────┼──────────┘
                    │         │               │
                    ▼         ▼               ▼
             ┌───────────────────────────────────────────┐
             │           DN (DataNode / 数据节点)          │
             │   ┌──────┐  ┌──────┐       ┌──────┐      │
             │   │ DN001│  │ DN002│  ...  │ DN00N│      │
             │   └──────┘  └──────┘       └──────┘      │
             │      │         │               │          │
             │      │    存储用户数据，执行查询片段           │
             │      │    数据按分片策略分布在各 DN 上        │
             └──────┴─────────┴───────────────┴──────────┘
                                  ▲
                                  │ 全局事务 ID / 快照
                                  │
                          ┌───────┴───────┐
                          │      GTM       │
                          │  (全局事务管理器) │
                          │ ┌─────┐ ┌─────┐│
                          │ │GTM主│ │GTM备││
                          │ └─────┘ └─────┘│
                          │  全局事务 ID 分配 │
                          │  全局快照管理     │
                          └─────────────────┘
```

### 架构角色速查表

| 角色 | 全称 | 一句话职责 | 存储什么 | 类比 |
|------|------|-----------|---------|------|
| **CN** | Coordinator（协调节点） | 接收 SQL，拆分查询，汇总结果 | 仅元数据（表结构、节点路由） | 数据库的"大脑" |
| **DN** | DataNode（数据节点） | 存储数据，执行查询片段 | 全部用户数据 | 数据库的"身体" |
| **GTM** | Global Transaction Manager | 分配全局事务 ID，管理全局快照 | 事务状态 | 数据库的"时钟" |

### 两种工作模式

| 维度 | 分布式模式 (`distributed`) | 集中式模式 (`centralized`) |
|------|--------------------------|---------------------------|
| 节点组成 | GTM + CN + DN | 仅 DN |
| 适用场景 | 多机集群，大数据量，高并发 | 单机开发测试，小规模业务 |
| CN 数量 | ≥ 1（可横向扩展） | 0（无 CN，直接连 DN） |
| GTM | 必须 | 不需要 |
| 配置复杂度 | 较高（需多机 SSH 互通） | 较低（可单机部署） |
| 典型用途 | 生产环境 | 本地开发、CI/CD |

---

## 二、核心术语详解（15 条）

### 1. OpenTenBase

基于 **Postgres-XL** 的企业级分布式数据库，由腾讯开源。它把 PostgreSQL 的单机能力扩展到了多机集群，支持水平扩展、全局事务和高可用。

- 上游项目：[Postgres-XL](https://www.postgres-xl.org/)
- 许可证：BSD 3-Clause
- 官网：[opentenbase.org](https://www.opentenbase.org/)

> **新手提示**：如果你用过单机 PostgreSQL，OpenTenBase 在 SQL 语法上几乎完全兼容。区别在于数据被分散存储在多台机器上，而你只需连接一个 CN 就能访问全部数据。

---

### 2. 集群（Cluster）/ 实例（Instance）

一个 OpenTenBase **实例** 就是一个完整的数据集群，包含一组协同工作的 GTM、CN 和 DN 节点。`config.ini` 中的 `[instance] name` 就是实例名称。

- **分布式实例**：包含 GTM + CN + DN 三类节点，用于多机生产环境
- **集中式实例**：仅包含 DN 节点，用于单机开发或小规模部署

> 实例名只允许字母、数字和下划线（如 `opentenbase01`、`test_cluster`），不能含 `-` 或 `.`。

---

### 3. GTM（Global Transaction Manager / 全局事务管理器）

GTM 是分布式事务的"协调中心"。在分布式数据库中，一个 SQL 可能涉及多个 DN，GTM 负责：

- 分配**全局事务 ID**（GXID），确保所有节点对事务的可见性一致
- 管理**全局快照**（Global Snapshot），决定每个事务能看到哪些数据

GTM 通常配置为 **一主多从**：主 GTM 处理请求，从 GTM 同步状态并在主节点故障时接管。

> **集中式模式下不需要 GTM**，因为所有数据都在一个 DN 上，不存在分布式事务问题。

---

### 4. CN（Coordinator / 协调节点）

CN 是用户与数据库之间的**桥梁**。用户总是连接 CN 执行 SQL，从不直接连接 DN。

CN 的核心工作：
1. **接收 SQL** → 解析查询
2. **生成分布式计划** → 把一条 SQL 拆成多个子查询片段
3. **分发到各 DN** → 每个 DN 执行自己那部分
4. **收集结果** → 汇总、排序、聚合后返回给用户

CN 本身**不存储用户数据**，只存储表结构、节点路由等元数据。

> **节点命名规则**：CN 节点自动命名为 `cn0001`、`cn0002`……`cnNNNN`。

---

### 5. DN（DataNode / 数据节点）

DN 是真正**存储和处理数据**的节点。所有表的实际数据都分布在各个 DN 上。

数据如何分布？
- **分片（Sharding）**：一张表的数据按分片键（sharding key）拆分成多个分片，分布在不同 DN 上
- **复制（Replication）**：某些小表可以在每个 DN 上存一份完整副本（适合维度表）

每个 DN 本质上是一个增强版的 PostgreSQL 实例，有自己的数据目录、WAL 日志和查询执行器。

> **节点命名规则**：DN 节点自动命名为 `dn0001`、`dn0002`……`dnNNNN`。

---

### 6. 分布式模式（Distributed Mode）

这是 OpenTenBase 的**完整形态**，适合多机集群部署：

```
config.ini 中: type=distributed

节点需求:
  [gtm]          — 至少 1 主
  [coordinators] — 至少 1 主
  [datanodes]    — 至少 1 主

典型最小配置: 1 GTM 主 + 1 CN 主 + 2 DN 主
```

特点：
- 数据水平分片，可横向扩展
- CN 层和 DN 层可分别扩容
- 支持全局事务一致性
- 需要多机 SSH 互信

---

### 7. 集中式模式（Centralized Mode）

这是分布式模式的**简化版本**，仅包含 DN 节点：

```
config.ini 中: type=centralized

节点需求:
  [datanodes] — 至少 1 主

无 GTM，无 CN。用户直接连接 DN 执行 SQL。
```

特点：
- 部署简单，适合单机开发测试
- 无需配置 GTM 和 CN
- 不具备水平扩展能力
- SQL 兼容性与分布式模式一致

---

### 8. Node Group（节点组）

节点组是 DN 的**逻辑分组**，用于控制数据分布策略。

- 每个 DN 主节点必须属于至少一个 node group
- 创建表时，可以指定数据存储在哪个 node group 的 DN 上
- 默认节点组 `default_group`：未指定时自动使用

```sql
-- 查看节点组
SELECT * FROM pgxc_group;

-- 创建节点组
CREATE NODE GROUP my_group WITH (dn001, dn002);
```

---

### 9. Sharding Group（分片组）

分片组定义了数据在节点组内的**分片方式**。

- 绑定到某个 node group
- `CREATE sharding group to group <group_name>`
- `CLEAN SHARDING` — 清理并重建分片映射

> 集中式实例只有一个 DN，分片退化为"所有数据都在一个节点上"，但仍需创建 sharding group 才能建表。

---

### 10. 主节点 / 从节点（Master / Slave）

OpenTenBase 支持**主从复制**实现高可用：

| 角色 | 职责 | 命名规则 |
|------|------|---------|
| **Master（主）** | 处理读写请求，生成 WAL 日志 | `cn0001`、`dn0001`、`gtm0001` |
| **Slave（从）** | 同步 WAL 日志，主节点故障时接管 | 与主节点同名，但部署在不同 IP 上 |

> 从节点通过 `pg_basebackup` 从主节点同步初始数据，之后通过流复制保持同步。

---

### 11. opentenbase_ctl

OpenTenBase 的**集群管理命令行工具**。位于安装目录的 `bin/` 下。

常用命令：
| 命令 | 作用 |
|------|------|
| `install -c config.ini` | 按配置文件部署新集群 |
| `status -c config.ini` | 查看所有节点运行状态 |
| `start -c config.ini` | 启动整个集群 |
| `stop -c config.ini` | 停止整个集群 |
| `delete -c config.ini` | 删除集群（清理数据目录） |
| `shell --cmd "..."` | 在所有节点上批量执行 shell |
| `sql --sql "..."` | 在所有节点上批量执行 SQL |
| `guc -k param -v val` | 查看或修改 GUC 参数 |

工具通过 SSH 远程执行命令，因此需要配置 `[server]` 段的 SSH 账号密码。

---

### 12. config.ini（opentenbase_config.ini）

`opentenbase_ctl` 使用的 **INI 格式配置文件**。模板位于仓库 `contrib/opentenbase_ctl/config/config.ini`。

核心配置段：

```
[instance]      — 实例名、类型（distributed/centralized）、安装包路径
[gtm]           — GTM 主/从 IP 列表
[coordinators]  — CN 主/从 IP、每服务器节点数、自定义 postgresql.conf
[datanodes]     — DN 主/从 IP、每服务器节点数、自定义 postgresql.conf
[server]        — SSH 用户名、密码、端口
[log]           — 日志级别（DEBUG/INFO/WARN/ERROR）
```

---

### 13. PG_HOME 与安装路径

编译或解压 OpenTenBase 后，二进制文件所在的根目录。

```bash
# 源码编译方式
PG_HOME=${INSTALL_PATH}/opentenbase_bin_v5.0
# 目录结构: PG_HOME/bin/  PG_HOME/lib/  PG_HOME/share/

# 预编译包方式
PG_HOME=<home>/install/opentenbase/5.21.8
```

需要将此路径加入 `PATH` 和 `LD_LIBRARY_PATH` 才能正常使用 `psql`、`opentenbase_ctl` 等命令。

---

### 14. pgxc_node（节点路由表）

OpenTenBase 的系统目录表，记录集群中所有节点的**路由信息**。

```sql
SELECT * FROM pgxc_node;
-- node_name | node_type | node_port | node_host | ...
-- dn0001    | D         | 11000     | 127.0.0.1 | ...
```

- `node_type`：`C` = Coordinator、`D` = DataNode、`G` = GTM
- `node_host`：节点 IP 地址
- `node_port`：节点端口

> 每个 CN 和 DN 都有一份 `pgxc_node` 的副本，用于节点间通信的路由查找。

---

### 15. Postgres-XL / PG-XC

OpenTenBase 的技术源头：

- **Postgres-XC**（eXtensible Cluster）：最早的 PostgreSQL 分布式集群方案，提供了 GTM/CN/DN 三层架构原型
- **Postgres-XL**（eXtensible, L）：XC 的演进版本，增强了水平扩展能力和 MPP 并行计算

OpenTenBase 在 Postgres-XL 基础上添加了企业级功能：安全管理、审计日志、Oracle 兼容语法、列存引擎等。

---

## 三、新手学习路径

建议按以下顺序了解 OpenTenBase：

1. **通读本术语表**（15 分钟）— 建立架构概念
2. **阅读 [README_ZH.md 概览](../README_ZH.md#概览)** — 了解集群组成
3. **在 WSL 或 Linux 上完成一次单机部署** — 参考[部署验证记录](DEPLOYMENT_VALIDATION_ZH.md)
4. **连接 psql 执行基本操作** — `CREATE TABLE`、`INSERT`、`SELECT`
5. **查看 `pgxc_node` 系统表** — 理解节点路由
6. **尝试分布式模式**（需要 2+ 台机器）— 体验数据分片和水平扩展

### 常见新手困惑速查

| 困惑 | 解答 |
|------|------|
| 为什么连 CN 而不是 DN？ | CN 是入口，知道数据分布在哪，会帮你路由到正确的 DN |
| 集中式和分布式选哪个？ | 单机开发/CI 用集中式；多机生产用分布式 |
| CN 上能查到数据吗？ | CN 只存元数据，`SELECT` 时 CN 会从 DN 拉取数据返回给你 |
| 为什么建表需要 sharding group？ | 需要告诉数据库数据按什么规则分布到各个 DN 上 |
| GTM 挂了怎么办？ | 配 GTM slave，主挂后从接管 |
| 怎么知道数据在哪个 DN 上？ | 看 `pgxc_node` 表，或通过 CN 执行查询，它会自动路由 |

---

## 四、相关文档

- [README_ZH.md](../README_ZH.md) — 部署主文档
- [DEPLOYMENT_VALIDATION_ZH.md](DEPLOYMENT_VALIDATION_ZH.md) — 部署验证记录（含 WSL 实战）
- [AI_USAGE_REPORT_ZH.md](AI_USAGE_REPORT_ZH.md) — AI 使用策略报告
- [OpenTenBase 官网](https://www.opentenbase.org/)
- [Postgres-XL 文档](https://www.postgres-xl.org/documentation/)
