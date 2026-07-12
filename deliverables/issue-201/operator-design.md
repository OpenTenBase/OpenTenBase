# OpenTenBase Kubernetes Operator 可行性与最小 PoC 设计

## 1. 设计目标

本文提出一套面向 OpenTenBase 的 Kubernetes Operator 概念方案，目标是把当前 `opentenbase_ctl` 的一次性、SSH/IP 驱动部署流程拆解为声明式、可观察、可重试的 reconciliation 阶段。

第一版仅设计静态 API 和控制流程，不声称已经实现 Operator，也不声称 OpenTenBase 已可在 Kubernetes 上运行。

## 2. 设计原则

1. GTM、CN、DN 必须作为不同角色管理。
2. DN shard 与 replica 必须分别建模。
3. Kubernetes 对象 Ready 不等于数据库拓扑 Ready。
4. 每个 CN/DN master 的拓扑应用状态必须单独追踪。
5. promotion gate、fencing 与 promotion 后收敛必须分离。
6. 未验证的 DNS、幂等性、存储、故障转移能力必须显式标记。
7. 第一版不选择最终 workload primitive。

## 3. 顶层资源

建议使用：

```yaml
apiVersion: database.opentenbase.org/v1alpha1
kind: OpenTenBaseCluster
```

一个 `OpenTenBaseCluster` 表示一个分布式部署，至少包含：

- 1 个 GTM primary；
- 可选 GTM standby；
- 1 个以上 Coordinator primary；
- 每个 Coordinator primary 可配置 standby 数；
- 1 个以上 DataNode shard；
- 每个 shard 可配置 standby 数；
- 服务、存储意图和 bootstrap 策略；
- 角色、节点、拓扑、group、endpoint 与错误状态。

Coordinator 数量采用：

```text
primaryCount × (1 + standbyCountPerPrimary)
```

这是基于当前同名 master/standby 配对的 PoC 表达，不是最终生产 API 承诺。

## 4. 资源模型

### 4.1 GTM

PoC 表达一个 GTM primary 和可选 standby。需要稳定身份、内部发现和保守的持久化意图。

未决问题包括：GTM 的持久化文件、复制一致性、promotion 和重连机制。

### 4.2 Coordinator

Coordinator 是外部客户端入口。虽然不保存分布式用户表数据，但持有目录和拓扑状态，因此不应作为普通无状态前端处理。

PoC 中定义 Coordinator 客户端 Service，但不引入 `rw/ro/r` 语义，也不假设所有 CN 的会话行为完全等价。

### 4.3 DataNode

每个 DN shard 是独立分布单元。每个 shard 包含一个 primary 和若干 standby。DN 的持久化是明确需求，但 PVC 数量、WAL 分离、替换和重挂载语义仍未确定。

## 5. 稳定身份与网络

临时命名建议：

- `<cluster>-gtm-0`
- `<cluster>-gtm-1`
- `<cluster>-cn-<ordinal>`
- `<cluster>-dn-s<shard>-0`
- `<cluster>-dn-s<shard>-<ordinal>`

概念 Service：

- Coordinator client Service；
- GTM internal discovery Service；
- GTM、CN 和各 DN shard 的 Headless discovery；
- DN 内部发现作为扩展点。

是否可将所有当前 IP 字段替换为 DNS 名称仍需运行验证。

## 6. Bootstrap 状态机

| Phase | 目标 |
|---|---|
| Pending | 接收资源并等待处理 |
| Validating | 校验角色数量、分片、存储意图和不支持操作 |
| ProvisioningGTM | 创建并初始化 GTM primary |
| WaitingForGTM | 等待 GTM 达到可供 CN/DN 初始化的 readiness |
| ProvisioningMasters | 初始化 CN 与 DN master |
| RegisteringTopology | 在每个 CN/DN master 上应用完整拓扑并刷新 pool |
| RestartingMasters | 表达当前流程中的拓扑后重启边界 |
| ProvisioningStandbys | 分别初始化 CN/DN standby 与 GTM standby |
| CreatingGroups | 创建默认 node group 和 sharding group |
| Ready | PoC 范围内状态全部收敛 |
| Degraded | 保留成功检查点的可重试失败 |
| Failed | 无效或非重试状态 |

未来 controller 必须做到按节点保存检查点、只重试未收敛节点，并在执行写操作前查询当前状态。当前 `opentenbase_ctl` 的幂等性尚未证明。

## 7. 拓扑收敛模型

建议状态中包含：

- `observedGeneration`
- `desiredTopologyGeneration`
- `observedTopologyGeneration`
- 每个 master 的 `appliedTopologyGeneration`
- 每个 master 的 registration state
- 每个 master 的 pool reload state
- default group 与 sharding group 的 desired/applied generation
- last error 与 retry state

只有所有要求的 CN/DN master 都应用目标拓扑并完成 pool reload 后，`observedTopologyGeneration` 才能推进。

## 8. Conditions 与状态

建议 conditions：

- `Validated`
- `GTMReady`
- `MastersReady`
- `TopologyApplied`
- `PoolsReloaded`
- `StandbysReady`
- `GroupsReady`
- `EndpointsReady`
- `Ready`
- `Degraded`
- `UnsupportedOperation`

Node status 应包含逻辑身份、角色、shard、期望/观察角色、初始化状态、readiness、拓扑代次和错误。

## 9. 工作负载管理策略

本 PoC 使用资源中立伪结构，不决定直接 Pod、StatefulSet 或混合方式。

可借鉴 CloudNativePG 的是“数据库角色感知的生命周期控制”，而不是直接照搬其 Pod Controller。最终选择必须通过以下验证：

- GTM/CN/DN 的稳定身份与替换语义；
- 存储所有权和重挂载；
- 主备 promotion；
- 有序启动和停止；
- 节点删除与缩容风险。

### 首个可运行 PoC 的候选编排方式

当前静态 PoC 没有选择最终生产 workload primitive。为了降低后续首个可运行 PoC 的实现复杂度，可以优先评估以下候选编排：

- GTM 使用一个独立 StatefulSet；
- Coordinator 使用一个独立 StatefulSet；
- 每个 DataNode shard 使用各自独立的 StatefulSet；
- 每个角色或 DataNode shard 配套 Headless Service。

选择 StatefulSet 作为首个可运行 PoC 候选，主要是为了利用稳定 Pod 名称、稳定 DNS 和独立存储身份，便于映射 GTM、CN、DN、shard 及 primary/standby 身份，并验证当前 `opentenbase_ctl` 所体现的节点身份与有序初始化约束。

这只是首个可运行 PoC 的候选方案，不是最终生产架构决策，也不代表 StatefulSet 已经通过 OpenTenBase 运行时验证。CloudNativePG 的直接 Pod 管理仍是后续可评估方向；GTM、CN、DN 未来也可以采用不同 workload primitive。运行时 hostname 兼容性、存储重挂载、故障转移、节点替换和身份复用语义仍未验证，Headless Service 也尚未证明可被 OpenTenBase 正常使用。

## 10. 存储设计边界

| 角色 | PoC 假设 | 未决问题 |
|---|---|---|
| DN primary | 必须声明持久化意图 | PVC 布局、WAL、替换与恢复 |
| DN standby | 独立可写 clone 目标 | 清理、重试、快照和复用 |
| CN | 保守声明持久化意图 | 精确持久化边界和可重建性 |
| GTM | 保守声明持久化意图 | durable state、备份和 promotion |

## 11. 高可用设计边界

第一版明确不实现自动 failover。未来 HA 控制器需要把以下三件事分开：

1. **Promotion gate**：保证只有一个候选节点进入 promotion。
2. **Fencing**：保证旧 active 节点不能继续提供服务。
3. **Convergence**：更新 GTM/CN/DN 拓扑、目录、group、pool 与 Service。

GTM、CN 和 DN 需要不同的 promotion 与收敛逻辑。

## 12. 扩缩容

### Coordinator

未来可设计增加 primary/standby、注册节点、刷新拓扑、更新 Service 与健康检查。安全移除还需处理会话排空和目录删除。

### DN standby

增加 standby 需要 clone、复制配置、readiness 与 topology 状态。

### DN shard

增加 shard 是独立长任务，需要数据重分布、进度、失败回滚和一致性控制，不应阻塞主 reconcile loop。第一版不支持在线 shard 扩缩容。

## 13. 备份恢复

第一版不实现分布式一致性备份。后续需设计：

- 一致性屏障；
- 每个 DN 的备份代次；
- GTM/CN 是否参与；
- WAL/日志归档；
- 恢复顺序；
- 拓扑重建；
- 部分恢复失败处理。

## 14. 监控

未来至少需要：

- Operator reconcile 次数、错误与耗时；
- GTM availability 与事务服务指标；
- CN readiness、会话与拓扑状态；
- DN shard readiness、复制状态与 lag；
- topology generation 差异；
- pool reload 失败；
- group 创建和扩容进度。

可以借鉴 Prometheus 与 PodMonitor 模式，但指标必须针对 OpenTenBase 新建。

## 15. 安全迁移

必须替换当前 `sshpass`、SSH、静态 IP 和 trust 认证假设。未来设计应使用：

- Kubernetes ServiceAccount 与最小 RBAC；
- Secret 与证书；
- 节点本地 agent 或受控 exec 接口；
- NetworkPolicy；
- 明确的数据库操作权限；
- 日志脱敏。

## 16. PoC 交付与验证

本次 PoC 已产出：

- structural-schema-oriented CRD 草案；
- 最小自定义资源；
- 部分状态示例；
- 资源中立伪结构；
- 字段证据矩阵；
- 离线验证脚本。

离线检查结果：`38 passed, 0 failed, 1 warning`。由于缺少 PyYAML 和可用 API Server，完整 YAML parsing、CRD admission、structural schema enforcement 与样例 admission 尚未验证。

## 17. 分阶段实施建议

### Phase 0：API 与验证

完成 CRD admission、样例 admission、DNS/IP 兼容实验和 readiness 定义。

### Phase 1：最小部署

实现单 GTM primary、CN/DN master、逐节点 topology registration 和 group 创建。

### Phase 2：Standby

实现 CN/DN clone 和 GTM standby，保留手动 promotion。

### Phase 3：监控与备份基础

补充指标、告警和非分布式一致性的节点级备份实验。

### Phase 4：HA 与升级

实现角色特定 promotion gate、fencing、收敛和滚动升级。

### Phase 5：分片扩缩容

实现数据重分布、长任务状态机与回滚。

## 18. 风险

- DNS 名称不能完整替代 IP；
- SQL 与 restart 不具备安全幂等性；
- 部分 master 拓扑收敛导致长期不一致；
- CN/GTM 持久化边界判断错误；
- promotion 后目录和 Service 不一致；
- 分布式备份缺乏全局一致性；
- shard 变更导致长时间数据迁移和回滚困难。

## 19. 结论

建议采用 OpenTenBase 专属 Operator，而不是直接套用 CloudNativePG。最小实现应优先解决声明式角色库存、GTM-first 初始化、逐 master 拓扑收敛和明确状态暴露。生产 HA、备份、重分片和升级应在基础 reconciliation 被真实验证后再实施。
