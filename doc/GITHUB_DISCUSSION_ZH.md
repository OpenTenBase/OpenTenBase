# [Discussion] OpenTenBase Kubernetes Operator 设计方案与风险讨论

> 建议发帖位置：https://github.com/OpenTenBase/OpenTenBase/discussions
> 分类：Ideas / RFC
> 标签：kubernetes, operator, architecture

---

## 背景

OpenTenBase 是基于 Postgres-XL 的分布式数据库，包含 GTM（全局事务管理）、CN（协调节点）、DN（数据节点）三类异构角色。目前社区已存在多个成熟的 PostgreSQL Kubernetes Operator（CloudNativePG、StackGres、Crunchy PGO、Zalando Operator），但它们的 CRD 模型假设"一个 PG 集群 = 一组同构节点（1 主 + N 从）"，无法直接编排 OpenTenBase 的三层异构拓扑。

我调研了以上 4 个 Operator 的架构设计，并结合 OpenTenBase 源码分析，起草了一份 [OpenTenBase Kubernetes Operator 设计方案](doc/K8S_OPERATOR_DESIGN_ZH.md)。以下是核心设计要点和待讨论的风险点。

## 设计方案摘要

### CRD：OpenTenBaseCluster

```yaml
spec:
  mode: distributed          # distributed | centralized
  version: "5.21.8"
  gtm:       { replicas, resources, storage }    # GTM StatefulSet
  coordinators: { replicas, slaveReplicas, ... } # CN StatefulSet
  datanodes:   { replicas, slaveReplicas, ... }  # DN StatefulSet
  backup:    { type, schedule, repository }       # pgBackRest / WAL-G
  monitoring: { enabled, exporterImage }          # Prometheus
```

### 核心编排流程

```
1. GTM StatefulSet → GTM Master Ready
2. CN + DN StatefulSet（并行）→ initdb with GTM address
3. Populate pgxc_node routing table（所有节点）
4. CREATE DEFAULT node group + sharding group
5. Create Slave pods（pg_basebackup）
6. Service exposure（CN read-write / read-only / DN headless）
```

### 关键差异化设计

- **多角色 StatefulSet**：GTM/CN/DN 各一个 StatefulSet，各自的 Pod 模板、存储、资源独立配置
- **初始化依赖链**：通过 init container + 环境变量注入 GTM 地址，保证初始化顺序
- **pgxc_node 路由管理**：Controller 在所有 CN 上执行 `CREATE NODE` / `ALTER NODE` 同步路由表
- **扩容模型**：加 CN = 计算扩容（直接 scale）；加 DN = 存储扩容（需数据再平衡）

## 待讨论问题（欢迎社区反馈）

### 1. GTM 的单点风险
GTM 是全局事务的协调中心。虽然支持主从（GTM Slave），但故障切换可能导致短暂的事务中断。是否应该引入 etcd 加速 GTM 选主？还是现有的 StatefulSet + headless service 足够？

### 2. pgxc_node 的声明式管理
`pgxc_node` 表存储所有节点的路由信息，新增/移除节点时需要在所有节点上同步更新。当前设计中 Controller 通过 SQL 直接更新 `pgxc_node`。是否应该将其抽象为 ConfigMap，通过 init container 注入？

### 3. opentenbase_ctl 的定位
`opentenbase_ctl` 是 OpenTenBase 的集群管理工具（C++ / libssh2）。在 K8s 环境中，是应该完全用 Controller 替换它，还是将其作为底层工具保留（类比 CloudNativePG 仍使用 `pg_ctl`）？

### 4. 数据再平衡的用户体验
扩容 DN 后已有数据不会自动分布到新 DN。是否需要 Operator 自动触发数据重分布？这个过程需要 `ALTER TABLE ... DISTRIBUTE BY` 并可能长时间锁表。

### 5. 集中式模式在 K8s 上的价值
集中式模式只有一个 DN，在 K8s 上的高可用优势不明显（单节点 PG + Patroni 就能做到）。是否应该只支持分布式模式？还是保留集中式用于开发测试环境？

### 6. 社区意向与合作
OpenTenBase 社区是否有计划开发 K8s Operator？如果有，希望采用什么技术栈（Go + Kubebuilder / Rust + kube-rs / Java + Quarkus）？是否有意愿与现有 PG Operator 项目合作？

## 完整设计文档

详见仓库 `doc/K8S_OPERATOR_DESIGN_ZH.md`（分支 `community-deploy`）。

欢迎任何形式的反馈：技术质疑、架构建议、实现经验分享、或者"这个设计有问题"的批评。🙏
