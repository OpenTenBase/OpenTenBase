# [Issue #201] OpenTenBase Kubernetes Operator 接入方案与最小 PoC 设计

## 背景

CloudNativePG 等 PostgreSQL Operator 已经形成了声明式 API、生命周期控制、存储、故障转移、监控和备份等成熟模式。但 OpenTenBase 并不是单一 PostgreSQL primary/replica 集群。一个分布式 OpenTenBase 部署包含 GTM、Coordinator、多个 DataNode shard、主备关系、node group 和 sharding group。

本提案基于 OpenTenBase 仓库中的 `opentenbase_ctl` 初始化路径，并以 CloudNativePG 1.30 为对照，提出一个边界明确的 `OpenTenBaseCluster` 静态 PoC。完整调研和设计分别见 [`research-report.md`](research-report.md) 与 [`operator-design.md`](operator-design.md)。

## 当前部署流程的主要发现

已审查的分布式初始化顺序是：

1. 准备并分发安装包；
2. 初始化并启动 GTM master；
3. 并行初始化 Coordinator master 和 DataNode master；
4. 在每个 Coordinator/DataNode master 上应用完整 master 拓扑、执行 `pgxc_pool_reload()`，并跨过当前 restart 边界；
5. 初始化 Coordinator/DataNode standby 和 GTM standby；
6. 通过第一个 Coordinator master 创建 default node group 和 sharding group。

关键影响是：拓扑会分别应用到每个必需的 CN/DN master。因此，未来控制器需要逐节点拓扑收敛状态，而不能只使用一个集群级“拓扑已创建”标记。

Coordinator 也不应被建模为通用无状态前端。虽然它不存储分布式用户表数据，但持有 catalog/topology 状态，并参与初始化和 standby clone。

## 为什么普通 PostgreSQL Operator 模型不足

CloudNativePG 1.30 提供了可借鉴概念，但其核心形态是一个可写 PostgreSQL primary 加 streaming replica。OpenTenBase 还需要以下领域逻辑：

- GTM、CN、DN shard、standby、node group 和 sharding group 的声明式库存；
- GTM-first 有序初始化；
- 在每个 CN/DN master 上应用拓扑；
- pool reload reconciliation；
- topology generation 与逐节点 applied generation；
- 角色特定的 promotion 与 promotion 后收敛；
- DN shard 扩容和数据重分布；
- 分布式一致性备份与恢复。

可以借鉴的概念包括声明式 spec/status、角色感知生命周期控制、逐实例健康与存储状态、与 fencing 分离的 promotion gate、Prometheus 集成，以及显式 reconciliation 错误。本提案不声称可以直接复用 CloudNativePG 源码。

## 建议的顶层 API

概念资源如下：

```yaml
apiVersion: database.opentenbase.org/v1alpha1
kind: OpenTenBaseCluster
metadata:
  name: demo
spec:
  image: "<OPEN_TENBASE_IMAGE_NOT_SELECTED>"
  gtm:
    primaryCount: 1
    standbyCount: 1
  coordinators:
    primaryCount: 2
    standbyCountPerPrimary: 1
  dataNodes:
    shards:
      - id: "0"
        standbyCount: 1
      - id: "1"
        standbyCount: 1
```

准确 schema 仍是设计草案。Coordinator cardinality 模型是对已审查同名 primary/standby 配对的临时 PoC 表达，不是已经验证的生产 API。

## 建议的 reconciliation 阶段

- `Pending`
- `Validating`
- `ProvisioningGTM`
- `WaitingForGTM`
- `ProvisioningMasters`
- `RegisteringTopology`
- `RestartingMasters`
- `ProvisioningStandbys`
- `CreatingGroups`
- `Ready`
- `Degraded`
- `Failed`

当前 `opentenbase_ctl` 的幂等性尚未得到证明。这些阶段描述的是未来控制器需要实现的属性，包括持久化检查点和安全重试。

## 拓扑状态模型

建议的 status 将 Kubernetes 资源代次与数据库拓扑收敛分开：

- `status.observedGeneration`；
- `status.desiredTopologyGeneration`；
- `status.observedTopologyGeneration`；
- 逐节点 `appliedTopologyGeneration`；
- 逐节点 registration 与 pool reload 状态；
- default node group 与 sharding group 状态；
- conditions、endpoints、last error 和 retry 状态。

只有全部必需 CN/DN master 都应用目标拓扑并完成 pool reload 后，`observedTopologyGeneration` 才应推进。

## Service 与稳定身份

静态 PoC 建模了：

- 一个面向客户端的 Coordinator Service；
- 一个内部 GTM discovery Service；
- 每个角色或 DN shard 的 Headless discovery；
- `demo-gtm-0`、`demo-cn-0`、`demo-dn-s0-0` 等稳定逻辑身份。

运行时 hostname 支持仍未验证。OpenTenBase 当前在多个路径传递和持久化 host/port，因此 DNS 兼容性需要真实实现测试。

## 工作负载管理

PoC 有意不选择直接管理 Pod、StatefulSet 或混合策略。可以借鉴的是数据库感知、角色感知的生命周期控制。最终 primitive 需要在验证角色身份、持久化存储所有权、替换语义、有序生命周期操作和 promotion 行为后再决定。

为降低首个可运行 PoC 的实现复杂度，可以优先评估 GTM、Coordinator 和每个 DataNode shard 分别使用 StatefulSet 配合 Headless Service，以验证稳定 Pod 名称、DNS 和独立存储身份。该建议只是首个可运行 PoC 候选，不是最终生产决策，也不代表 StatefulSet 或 Headless Service 已通过 OpenTenBase 运行时验证；CloudNativePG 的直接 Pod 管理及按角色采用不同 primitive 仍是后续可评估方向。

## 首个 PoC 的明确排除项

静态 PoC 未实现或证明：

- GTM/CN/DN 自动故障转移；
- fencing 或 promotion 后收敛；
- 在线 DN shard 扩容、resharding 或安全移除；
- 分布式一致性备份或生产恢复；
- major-version upgrade；
- 生产级 TLS、凭据和完整监控；
- 生产级工作负载替换语义；
- OpenTenBase 在 Kubernetes 上启动或可提供查询服务。

## 静态 PoC 交付物

PR 中包含：

- [`poc/crd/opentenbasecluster-crd.yaml`](poc/crd/opentenbasecluster-crd.yaml)：`OpenTenBaseCluster` CRD 草案；
- [`poc/samples/minimal-cluster.yaml`](poc/samples/minimal-cluster.yaml)：最小自定义资源样例；
- [`poc/samples/partial-status-example.yaml`](poc/samples/partial-status-example.yaml)：`RegisteringTopology` 部分状态示例；
- [`poc/pseudostructure/generated-resources.yaml`](poc/pseudostructure/generated-resources.yaml)：资源中立生成计划；
- [`poc/traceability/field-evidence-matrix.md`](poc/traceability/field-evidence-matrix.md)：字段/证据追踪矩阵；
- [`poc/validation/validation-results.md`](poc/validation/validation-results.md)：离线验证结果。

离线验证结果：

```text
38 passed, 0 failed, 1 warning
```

警告是环境中缺少 PyYAML，因此使用了文本级回退检查。Kubernetes API Server admission、structural-schema enforcement 和样例 admission 均未验证。

## 风险与开放问题

1. DNS 名称能否安全替代 GTM 设置、initdb 参数、`pgxc_node`、replication 配置和 pool 中的字面 IP？
2. GTM 和 Coordinator 替换与恢复必须持久化哪些状态？
3. topology/group 操作在部分成功后是否可查询并安全重试？
4. 当前 topology 后 restart 是否必需？
5. 哪些 readiness gate 足以判断 GTM、CN 和 DN 可用？
6. 如何在不引入未经证实的会话/路由假设下暴露多个 Coordinator？
7. 需要哪些 DN-to-DN 连接？
8. 角色特定的 promotion、fencing 和 topology convergence 应如何工作？
9. 如何跨 DN shard 创建分布式一致性备份点？
10. 每个角色最安全的 workload primitive 是什么？

## 希望社区讨论的问题

- 单一顶层 `OpenTenBaseCluster` 是否是合适的初始 API 边界？
- topology registration 应继续由 SQL 驱动，还是部分迁移到专用控制 API？
- topology 后 restart 能否移除或改为更精确的操作？
- 应由哪个角色执行 node group 与 sharding group reconciliation？
- GTM 和 Coordinator 预期的持久化/恢复契约是什么？
- 是否正式支持基于 hostname 的 topology，还是 Operator 应解析并 reconciliation IP？
- 第一个可运行 Kubernetes PoC 应采用哪种最小拓扑？

## 建议实施路径

1. 验证 CRD admission、DNS 兼容性和 readiness 定义；
2. 实现最小 GTM primary + CN/DN master reconciliation；
3. 增加逐 master topology 与 group convergence；
4. 增加 standby，但不启用自动 promotion；
5. 增加监控和恢复实验；
6. 将 HA、升级和 shard 数据重分布作为后续独立里程碑。

欢迎社区重点审阅资源边界、拓扑收敛语义、Coordinator 持久化要求和最小可运行验证范围。
