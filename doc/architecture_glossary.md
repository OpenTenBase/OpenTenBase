# OpenTenBase 新手导览、核心架构术语表与 FAQ

本导览旨在帮助初次接触 OpenTenBase 的开发者与新手快速建立对整体架构的认知，帮助新手阅读理解 README 与部署文档。

---

## 1. 新手导览 (Beginner's Quick Start)

OpenTenBase 是腾讯开源的企业级分布式 HTAP（混合事务/分析处理）数据库。新手在开启探索时，建议按照以下顺序学习：

### Step 1：理解角色分工与运行模式
* **架构角色**：OpenTenBase 并非单机数据库，而是由负责连接与计算的 **CN**、负责存储的 **DN** 以及负责事务协调的 **GTM** 共同构成的集群。
* **模式选择**：初学者单机测试或开发调试时，优先使用 **集中式模式 (Centralized Mode)** 或 Docker 部署；生产环境海量数据下，使用 **分布式模式 (Distributed Mode)**。

### Step 2：使用管控工具部署
使用集群运维工具 `opentenbase_ctl`（或 `pgxc_ctl`）完成集群的一键初始化与启动，无需手动启动各个进程：
```bash
# 初始化并启动集群（示例）
opentenbase_ctl init
opentenbase_ctl start
```

### Step 3：体验分布式建表与数据分片
连接 CN 后，通过指定 `DISTRIBUTE BY` 体验数据如何在不同的 DN 节点间存储：
```sql
-- 创建基于 user_id 哈希分片的分布式表
CREATE TABLE user_logs (    
    user_id BIGINT NOT NULL,    
    action_type VARCHAR(32),    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) DISTRIBUTE BY HASH(user_id);

-- 插入数据时，CN 会根据 user_id 的哈希值自动路由到对应的 DN 节点
INSERT INTO user_logs (user_id, action_type) VALUES (10001, 'login');
```

---

## 2. 核心术语表 (Core Terms Glossary)

以下整理了 OpenTenBase 13 个主要技术术语：

1. **Coordinator (CN - 协调节点)**：数据库的业务入口。负责接收客户端 SQL 请求、解析 SQL、生成分布式执行计划，并将子任务路由发给 DN。
2. **DataNode (DN - 数据节点)**：数据库的存储与计算单元。存储业务数据分片（Shard），并执行 CN 下发的局部 SQL 查询与计算任务。
3. **GTM (Global Transaction Manager - 全局事务管理器)**：集群的事务控制部分。统一发放全局事务 ID (GXID) 与全局时间戳 (GTS)，保证分布式环境下的 ACID 特性与跨节点 MVCC（多版本并发控制）。
4. **GTM Proxy (GTM 代理)**：GTM 的高并发性能缓冲层。部署在各物理节点上，批量向 GTM 申请事务资源，降低网络通信与 GTM 单点并发压力。
5. **GTM Standby (GTM 备节点)**：GTM 的高可用容灾节点。通过主备同步机制实时接收 GTM 主节点的状态更新，当 GTM 主节点故障时可提升为主节点（Promote），防止单点失效。
6. **分布式模式 (Distributed Mode)**：OpenTenBase 的完全体形态。CN、DN、GTM 部署在多台物理节点上，数据打散切分存储，支持海量数据的水平扩展与并行计算。
7. **集中式模式 (Centralized Mode)**：OpenTenBase 的轻量形态。把 CN、DN、GTM 所有组件部署在单台机器上，行为类似于传统单机数据库，适合快速开发、测试与功能验证。
8. **Node Group (节点组)**：逻辑上的 DN 节点分队。在集群中将若干 DN 划分到一个组中，便于进行数据隔离、按组动态扩容（Add Node Group）或将特定的表绑定存储。
9. **opentenbase_ctl / pgxc_ctl (集群管控工具)**：OpenTenBase 的自动化运维控制台。提供命令行交互界面，管理员通过输入命令即可自动化完成整个分布式集群的构建、启动、停止与状态监控。
10. **Distribution Key / Sharding (分布键与分片)**：数据分片规则。建表时指定某个字段（如 `user_id`）作为 Distribution Key，系统自动通过 Hash 或 Replicate 方式将数据打散存储到不同的 DN 上。
11. **2PC (Two-Phase Commit - 两阶段提交)**：分布式事务的一致性保证机制。当事务跨多个 DN 时，CN 先向所有 DN 下发准备命令（Prepare），确认均成功后再下发提交命令（Commit），防止数据不一致。
12. **GTS / GXID (全局时间戳与全局事务 ID)**：由 GTM 统一颁发的全局“身份证”，确保分布在不同物理机上的事务在时间维度上有严格一致的先后顺序。
13. **Pooler (连接池模块)**：CN 与 DN 之间的内部高效通信通道。通过长连接复用机制，避免 CN 频繁与各个 DN 建立/销毁网络连接带来的巨大开销。

---

## 3. 架构组件交互与查询全链路

OpenTenBase 分布式查询与事务处理链路如下表所示：

| 步骤 / 组件 | 角色定位 | 是否存数据 | 核心职责与交互链路 |
| :--- | :--- | :---: | :--- |
| **1. 客户端连接** | 请求发起方 | 否 | 发起数据库连接，向任意 **CN** 发送 SQL 语句（如 `SELECT * FROM user_logs;`）。 |
| **2. CN 规划** | 业务入口 | 否 | ① 接收 SQL；② 向 **GTM/GTM Proxy** 申请 GXID 与 GTS；③ 解析生成分布式计划；④ 下发子任务至对应 **DN**。 |
| **3. GTM 协调** | 事务大脑 | 否 | 统一颁发事务时间戳，协调多 DN 间的 2PC（两阶段提交），并通过 **GTM Standby** 保障高可用。 |
| **4. DN 执行** | 存储与计算 | **是** | 在本地磁盘检索数据分片，执行局部计算，并将结果汇总返回给 **CN**。 |
| **5. 管控运维** | 控制工具 | 否 | 由 **opentenbase_ctl** 负责上述 CN、DN、GTM 组件的集群初始化、状态监控与节点扩容。 |

---

## 4. 常见问题解答 (FAQ)

**Q1：没有多台服务器，是否可以运行 OpenTenBase ？**
> **答**：可以。可以使用 **集中式模式 (Centralized Mode)** 或使用项目自带的 Docker/镜像环境，在单台 Linux 虚拟机甚至笔记本上把 CN、DN、GTM 跑在同一个节点上体验。

**Q2：为什么 CN 不可直接存储数据，而需要通过 CN 和 DN 操作？**
> **答**：在分布式架构中，经拆分后，CN 专门处理高并发的 SQL 解析与计划生成（计算节点），DN 专门负责海量数据的存储与并发 IO（存储节点）。当存储不够时只需扩容 DN，计算不够时只需扩容 CN，扩展非常灵活。

**Q3：如何知道数据应存储于哪一个 DN 上？**
> **答**：建表时通过指定 `DISTRIBUTE BY HASH(column)`。例如以 `user_id` 为分布键，系统对 `user_id` 做哈希计算，将不同用户的数据均匀打散存入不同的 DN 分片中。

---

## 5. AI 使用策略自我报告 (AI Usage Report)

1. **使用工具**：Gemini AI。
2. **主要用途**：辅助整理分布式数据库通用术语、架构交互和 md 表格排版，以及对源码文档进行阅读和提炼，以及对学习中所遇到的部分技术和操作进行补充学习和拓展，辅助对项目内容的理解。
