# OpenTenBase Kubernetes 静态 PoC 材料

> **这些材料不会部署 OpenTenBase，也没有实现 Operator。**

## 目的

本目录包含 Issue #201 的静态、面向 schema 的设计草案，用于表达经过审阅的 PoC 边界：一个包含独立 GTM、Coordinator 和 DataNode shard 的分布式部署，以及有序 bootstrap、逐 master 拓扑进度、group 进度、稳定身份假设和明确排除项。

这些文件仅用于审阅，不包含可运行 OpenTenBase 工作负载、bootstrap 实现、已选运行时镜像、数据库命令、SQL、probe、mount path 或实际运行结论。

## 目录结构

```text
poc/
├── README.md
├── crd/opentenbasecluster-crd.yaml
├── samples/minimal-cluster.yaml
├── samples/partial-status-example.yaml
├── pseudostructure/generated-resources.yaml
├── validation/validate.py
├── validation/validation-results.md
└── traceability/field-evidence-matrix.md
```

## 文件说明

- `crd/opentenbasecluster-crd.yaml`：带 status subresource、字段显式类型化的 `v1alpha1` structural-schema-oriented CRD 草案；API Server acceptance 尚未验证。
- `samples/minimal-cluster.yaml`：使用显式镜像占位符、不可运行的期望状态样例。
- `samples/partial-status-example.yaml`：`RegisteringTopology` 部分状态的文档示例；status 通常由 controller 写入，这不是真实运行结果。
- `pseudostructure/generated-resources.yaml`：表达身份和依赖的资源中立计划；`GeneratedResourcePlan` 不是本 CRD 提供的 Kubernetes API。
- `validation/validate.py`：离线静态检查器；仅在环境已安装 PyYAML 时使用它，否则执行有明确说明的文本级回退检查。
- `validation/validation-results.md`：真实 Stage 4B 验证记录。
- `traceability/field-evidence-matrix.md`：重要字段和边界的分类与证据追踪。

## CRD 与运行时实现的区别

安装 CRD 只会让 Kubernetes API Server 能够存储和校验自定义资源。本草案没有提供 controller，不会观察资源、创建工作负载、初始化数据库、应用拓扑、reload pool、创建 group 或写入 status。

`metadata.generation` 与 `status.observedGeneration` 表示 Kubernetes 期望资源代次；`status.desiredTopologyGeneration` 与 `status.observedTopologyGeneration` 是独立提出的 OpenTenBase 数据库拓扑收敛字段。Kubernetes 不会自动计算或管理这些 topology generation 字段。

工作负载 primitive 有意保持未选择状态。直接 Pod、StatefulSet 和按角色混合管理仍未确认。

Coordinator cardinality 使用明确字段：`primaryCount` 表示 Coordinator primary 数量，`standbyCountPerPrimary` 应用于每个 primary，因此期望 Coordinator 实例总数为 `primaryCount * (1 + standbyCountPerPrimary)`。这是对已审查同名 primary/standby 配对的临时 PoC 表达，不是经过证明的生产 cardinality API。

## 验证命令

从仓库根目录执行：

```sh
python3 deliverables/issue-201/poc/validation/validate.py
git diff --check
```

如果环境中存在 `kubectl`，可以尝试在不应用资源的情况下检查 CRD：

```sh
kubectl apply --dry-run=client -f deliverables/issue-201/poc/crd/opentenbasecluster-crd.yaml
```

在 CRD 注册到 API Server 之前，自定义资源的 client-side validation 可能无法完成。不得将 `--validate=false` 作为 schema 有效的证明。

## 验证限制

静态检查可以验证文件存在性、环境允许时的 YAML parsing、CRD identity、显式类型字段、样例数量边界、唯一 shard ID、伪结构禁止 kind、警告文本和追踪分类。本草案旨在满足 Kubernetes structural-schema 要求，但本次没有完成完整 YAML parsing、API Server CRD admission、structural-schema enforcement 和样例 admission。

静态检查无法证明 OpenTenBase 能够启动、hostname 可用、topology/group 操作成功、操作具有幂等性、数据可用或未来 controller 能正确 reconciliation。

## 明确排除项

- GTM、Coordinator、DataNode 自动故障转移和 promotion；
- fencing 与 promotion 后收敛；
- 分布式一致性备份和生产恢复编排；
- 在线 shard 扩容、resharding、数据重分布或安全移除；
- major-version upgrade 编排；
- 生产 TLS、凭据、安全自动化和完整监控；
- 生产工作负载 primitive、替换、存储重新挂载和可用性证明。

Coordinator 未被建模为无状态：其 catalog/topology 存储需求是临时假设，准确持久化行为仍未确认。DataNode shard 是不同的数据分布单元，不是 replica。

## 推荐阅读顺序

1. [`../operator-design.md`](../operator-design.md)；
2. 本 README；
3. `crd/opentenbasecluster-crd.yaml`；
4. `samples/minimal-cluster.yaml`；
5. `samples/partial-status-example.yaml`；
6. `pseudostructure/generated-resources.yaml`；
7. `traceability/field-evidence-matrix.md`；
8. `validation/validation-results.md`。

## 与已审阅 PoC 范围的关系

这些草案没有扩大已审阅的 PoC 范围。hostname 兼容性、工作负载选择、GTM/Coordinator 持久化、readiness、安全重试、topology drift 检测、运行时权限和路由行为仍是实现验证输入，而不是已经确认的能力。
