# OpenTenBase 接入社区 PostgreSQL Kubernetes 部署框架调研报告

## 摘要

本文围绕 OpenTenBase GitHub Issue #201，调研 OpenTenBase 当前部署模型与 CloudNativePG 1.30 的 Kubernetes Operator 架构，分析两者在资源抽象、节点生命周期、拓扑管理、高可用、扩缩容、备份恢复和安全模型上的差异。

结论是：CloudNativePG 的声明式 API、状态建模、角色感知生命周期管理、监控与故障信息暴露等理念可以借鉴，但不能直接把 OpenTenBase 当作单一 PostgreSQL 主备集群。OpenTenBase 需要专门处理 GTM、Coordinator、多个 DataNode 分片、逐节点拓扑注册、连接池刷新、节点组与分片组创建，以及未来的分布式故障恢复和数据重分布。

本次调研还产出了一套静态 PoC：`OpenTenBaseCluster` CRD 草案、最小资源样例、部分状态样例、资源中立伪结构、字段证据矩阵和离线验证脚本。该 PoC 只验证 API 与设计表达能力，不代表 OpenTenBase 已能在 Kubernetes 上运行。

## 1. 调研范围与方法

调研分为四部分：

1. 阅读 OpenTenBase 仓库中的 README、`opentenbase_ctl`、GTM、节点目录、连接池等源码，确认 GTM、CN、DN 的职责和当前部署流程。
2. 沿真实调用链梳理分布式和集中式初始化顺序、主备建立、拓扑 SQL 和连接池刷新。
3. 仅使用 CloudNativePG 官方 1.30 文档、API 参考和官方发布说明调研其 Operator 模型。
4. 建立 62 行差异与复用矩阵，将能力分为“可直接借鉴概念”“需适配”“必须新建”“不适用”“未确认”。

所有关键结论均保留仓库路径、函数或官方资料来源；无法确认的行为没有写成事实。

## 2. OpenTenBase 部署模型

### 2.1 三类核心角色

OpenTenBase 分布式模式由 GTM、Coordinator 和 DataNode 组成。

- **GTM**：承担全局事务管理、全局事务 ID 与时间戳相关服务。
- **Coordinator（CN）**：客户端入口，负责 SQL 解析、分布式计划与结果汇总；不保存分布式用户表数据，但包含系统目录、拓扑元数据和运行状态，因此不能简单视为无状态前端。
- **DataNode（DN）**：保存用户数据，数据分布在多个节点或分片中。

集中式示例主要表现为 DataNode 主备；分布式示例则要求 GTM、CN、DN 同时存在。

### 2.2 当前分布式初始化顺序

根据 `opentenbase_ctl` 的调用路径，当前分布式部署顺序为：

1. 准备并分发安装包。
2. 初始化、配置并启动 GTM master。
3. 并行初始化 CN master 和 DN master。
4. 在每一个 CN/DN master 上写入完整的 CN/DN-master 拓扑，执行 `pgxc_pool_reload()`，随后经历停止与重新启动边界。
5. 在 master 阶段全部完成后，并行初始化 CN、DN 与 GTM standby。
6. 通过第一个 CN master 创建默认 DN group、sharding group，并执行相关清理操作。

一个重要事实是：初始拓扑不是只在单一控制节点写入后自动传播，而是在每个 CN/DN master 上分别执行等价的拓扑 SQL。这意味着未来 Operator 必须追踪每个 master 的拓扑应用状态。

### 2.3 主备与远程操作

- CN/DN standby 使用同名 master 配对，并通过 `pg_basebackup` 初始化。
- GTM standby 使用独立的 `initgtm` 与 standby 配置路径。
- 当前工具依赖 IP 列表、远程 shell、`sshpass`、`ssh`、`scp` 以及 trust 认证配置。
- 当前安装流程未发现持续 reconciliation、部分失败回滚或经过证明的幂等重试机制。

这些约束均不适合直接迁移到 Kubernetes，需要拆分为可观察、可重试的控制阶段。

## 3. CloudNativePG 1.30 核心机制

CloudNativePG 是围绕单 PostgreSQL 主备集群设计的 Operator，核心特点包括：

- 使用 `Cluster`、`Backup`、`ScheduledBackup`、`Pooler` 等 CRD。
- 不使用 StatefulSet，而由自定义控制器直接管理数据库 Pod。
- 每个实例主容器内运行 instance manager，作为 PID 1 管理 PostgreSQL 生命周期、探针与本地状态。
- 默认创建 `rw`、`ro`、`r` Service，分别指向 primary、replica 和任意实例。
- 直接管理每实例 PVC，可为 WAL 配置独立存储。
- 状态中记录当前/目标 primary、实例就绪、PVC 健康、phase、reason 和 conditions。
- 故障转移采用两阶段流程；Lease 用作 promotion gate，primary isolation check 承担 fencing，Lease 本身不是 fence。
- 支持角色感知滚动更新、Prometheus 指标与 PodMonitor。
- 推荐通过 CNPG-I 与 Barman Cloud Plugin 实现对象存储备份与 WAL 归档；原生 in-tree Barman 能力在 1.30 中仍为 deprecated，计划于 1.31 移除。

## 4. 关键差异

### 4.1 资源抽象

CloudNativePG 的一个 `Cluster` 表达一个 primary 加多个 streaming replicas。OpenTenBase 一个部署同时包含 GTM、多个 CN、多个 DN shard 及其 standby、node group 和 sharding group，因此需要新的顶层资源模型。

### 4.2 拓扑状态

CloudNativePG 主要追踪主备角色和复制状态；OpenTenBase 还必须追踪：

- 期望的 CN/DN-master 拓扑；
- 每个 master 的已应用拓扑代次；
- `pgxc_pool_reload()` 结果；
- 默认 group 与 sharding group 的创建状态；
- GTM 配置与 `pgxc_node` 路径之外的状态。

### 4.3 服务模型

CNPG 的 `rw/ro/r` 不能直接映射到 OpenTenBase。OpenTenBase 外部客户端通过 Coordinator 接入，GTM 和 DN 主要承担内部服务，因此至少需要：

- Coordinator 客户端 Service；
- GTM 内部发现 Service；
- CN、GTM、每个 DN shard 的稳定内部发现方式。

### 4.4 高可用

CNPG 的 primary failover 无法直接覆盖 GTM、CN、DN 的独立 promotion 语义。OpenTenBase 未来需要分别设计：

- promotion gate；
- fencing；
- promotion 后拓扑、目录、group、pool 与 Service 的收敛；
- 不同角色的恢复边界。

当前证据不足以证明 `opentenbase_ctl` 已提供上述完整能力。

### 4.5 扩缩容

增加 PostgreSQL replica 与增加 DN shard 是不同问题。DN shard 扩容还涉及数据重分布、事务一致性、失败回滚和长任务管理，不能作为普通副本扩容处理。

### 4.6 备份恢复

单 PostgreSQL 物理备份不能证明多 DN 的全局一致性。OpenTenBase 需要进一步回答：

- GTM 是否参与一致性屏障；
- CN 目录与拓扑如何保存；
- 多 DN 如何形成同一一致性时间点；
- 恢复时 GTM、DN、CN 和拓扑元数据的顺序。

## 5. 可复用与必须新建的能力

### 可借鉴概念

- desired spec 与 observed status 分离；
- 数据库角色感知的工作负载生命周期控制；
- 每实例存储归属与健康状态；
- promotion gate 与 fencing 分离；
- Prometheus 与 PodMonitor 集成；
- reconciliation 指标和明确的失败状态。

### 需要适配

- 顶层集群资源；
- instance manager 或本地 agent 的职责；
- 稳定身份与网络发现；
- 有序 bootstrap；
- Service 与存储模型；
- standby 加入与滚动更新；
- 备份编排。

### 必须为 OpenTenBase 新建

- GTM/CN/DN/shard/group 的声明式库存；
- GTM-first 幂等初始化状态机；
- 多 master 拓扑注册、漂移检测和连接池刷新；
- 拓扑代次和逐节点应用状态；
- node group 与 sharding group reconciliation；
- 部分安装恢复；
- 角色特定 promotion、fencing 和收敛；
- shard 扩容与数据重分布；
- 分布式一致性备份恢复；
- Kubernetes 原生凭据、证书和权限边界。

## 6. PoC 结果

静态 PoC 包含：

- `OpenTenBaseCluster` CRD 草案；
- 最小分布式自定义资源样例；
- `RegisteringTopology` 部分状态样例；
- 不选择 Pod/StatefulSet 的资源中立伪结构；
- 字段到证据的追踪矩阵；
- 离线一致性验证脚本和验证记录。

CRD 草案显式区分：

- `metadata.generation` / `status.observedGeneration`：Kubernetes 资源代次；
- `desiredTopologyGeneration` / `observedTopologyGeneration`：OpenTenBase 数据库拓扑收敛代次。

离线验证结果为 **38 passed, 0 failed, 1 warning**。警告是环境中缺少 PyYAML，因此使用了文本级回退检查。Kubernetes API Server admission、structural schema enforcement 和样例 admission 尚未验证。

## 7. 未解决问题

- OpenTenBase 是否能在所有配置与 `pgxc_node` 中稳定使用 DNS 名称替代 IP；
- GTM 与 CN 的精确持久化、恢复和替换需求；
- topology SQL 与 group SQL 的可查询、原子性和幂等重试；
- master 拓扑注册后的停止/启动是否必要；
- GTM、CN、DN 的 readiness 定义；
- 多 CN 接入时的会话和路由语义；
- DN 到 DN 的直接连接需求；
- 各角色适合的最终 workload primitive；
- promotion 后如何完成拓扑与服务收敛；
- 分布式一致性备份和恢复协议。

## 8. 结论

OpenTenBase 可以借鉴成熟 PostgreSQL Operator 的声明式管理理念，但不能直接接入或简单改造现有单 PostgreSQL Operator。更可行的方向是设计 OpenTenBase 专属 Operator，以一个顶层资源统一表达 GTM、CN、DN shard 和 group，通过分阶段 reconciliation 替代当前一次性 SSH 部署流程。

第一阶段应优先完成声明式拓扑、GTM-first bootstrap、逐 master 拓扑应用状态、稳定身份、状态与错误暴露。自动故障转移、在线重分片、分布式一致性备份和生产级升级应作为后续独立阶段推进。

## 参考资料

- OpenTenBase 仓库 README、`contrib/opentenbase_ctl`、`src/backend/access/transam/gtm.c`、`src/backend/pgxc/nodemgr/nodemgr.c`、`src/backend/pgxc/pool/poolutils.c` 等。
- CloudNativePG 1.30 官方文档与 API 参考。
- CloudNativePG v1.30.0 release notes。
- Barman Cloud Plugin 官方文档。
