# PR 描述

建议标题：`docs: 提交 OpenTenBase Kubernetes Operator 调研与设计方案`

## 背景

关联 Issue #201。本次工作调研 OpenTenBase 分布式部署模型，并以 CloudNativePG 1.30 为官方对照基线，分析社区 PostgreSQL Operator 概念在 GTM、Coordinator、DataNode shard 拓扑中的适用范围。

## 本次提交内容

- OpenTenBase 与 CloudNativePG 1.30 中文调研报告；
- OpenTenBase Kubernetes Operator 可行性与最小 PoC 设计；
- GitHub Discussions 中文草稿；
- AI 使用策略与真实修正记录；
- `OpenTenBaseCluster` CRD 静态草案、样例、资源伪结构、追踪矩阵和验证记录；
- PR 描述与 Issue 评论中文草稿。

## 核心发现

- OpenTenBase 的 GTM/CN/DN 分布式拓扑不能直接套用单 PostgreSQL primary/replica 模型。
- 当前 bootstrap 顺序为 GTM master → CN/DN master → 逐 master topology registration 与 pool reload → restart → standby → group 创建。
- Coordinator 保存 catalog/topology 状态，不能直接按无状态前端处理。
- 声明式 spec/status、角色感知生命周期和逐节点状态可借鉴，但 CloudNativePG 源码直接复用并未得到证明。

## 方案设计

方案使用一个概念性 `OpenTenBaseCluster` 表达角色、DN shard、standby、Service、存储意图和拓扑状态。状态模型区分 Kubernetes generation 与 OpenTenBase topology generation，并记录逐节点 topology application、pool reload、group、endpoint 和错误状态。

后续首个可运行 PoC 可优先评估 GTM、Coordinator 和每个 DataNode shard 分别使用 StatefulSet 配合 Headless Service，以降低稳定身份验证的实现复杂度；该候选不构成最终生产 workload primitive 决策，也尚未通过 OpenTenBase 运行时验证。

## PoC 边界

本 PoC 是静态设计验证，不包含可运行 Operator 或工作负载。未实现自动故障转移、fencing、在线重分片、分布式一致性备份、生产恢复、major-version upgrade、生产安全自动化和完整监控。hostname 兼容性与 Operator reconcile 均未验证。

## 静态验证结果

离线验证结果为 `38 passed, 0 failed, 1 warning`。PyYAML 不可用，因此使用文本级回退检查。Kubernetes API Server CRD admission、structural-schema enforcement 和样例 admission 未完成。

## 未验证事项

- OpenTenBase 在 Kubernetes 中的初始化、启动和查询可用性；
- DNS/hostname 兼容性；
- topology/group 操作幂等性；
- GTM/CN 持久化和替换语义；
- failover、backup/restore、resharding、upgrade 和生产安全。

## 交付物路径

- `deliverables/issue-201/research-report.md`
- `deliverables/issue-201/operator-design.md`
- `deliverables/issue-201/github-discussion-draft.md`
- `deliverables/issue-201/ai-usage-report.md`
- `deliverables/issue-201/poc/`

关联 Issue #201
