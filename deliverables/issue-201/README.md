# OpenTenBase Issue #201 中文交付材料

> **本目录中的 PoC 为静态设计验证，不包含可运行的 OpenTenBase Operator，也不代表 OpenTenBase 已在 Kubernetes 中部署成功。**

## 任务说明

本目录整理 [OpenTenBase Issue #201](https://github.com/OpenTenBase/OpenTenBase/issues/201) 的中文调研、方案设计、社区讨论草稿、AI 使用说明、提交文案和静态 PoC。任务围绕 OpenTenBase 如何借鉴社区 PostgreSQL Kubernetes Operator 架构，并表达 GTM、Coordinator、DataNode shard 组成的分布式拓扑。

## 交付物目录

- [`research-report.md`](research-report.md)：OpenTenBase 部署模型、CloudNativePG 1.30 调研及差异结论。
- [`operator-design.md`](operator-design.md)：顶层资源、状态机、拓扑收敛、Service、存储和实施边界。
- [`github-discussion-draft.md`](github-discussion-draft.md)：可提交到 GitHub Discussions 的中文讨论草稿。
- [`ai-usage-report.md`](ai-usage-report.md)：真实 AI 使用过程、修正记录和验证边界。
- [`submission/pr-description.md`](submission/pr-description.md)：中文 PR 描述草稿。
- [`submission/issue-comment.md`](submission/issue-comment.md)：中文 Issue 评论草稿。
- [`poc/`](poc/)：CRD 草案、样例、资源伪结构、追踪矩阵和离线验证材料。

## 调研对象

外部对照基线为 **CloudNativePG 1.30**。调研使用其官方文档、官方仓库及发布材料，重点关注 CRD、controller、Pod 管理、instance manager、Service、存储、状态、高可用、滚动更新、监控和备份恢复。

## 核心结论

- OpenTenBase 分布式模式包含职责不同的 GTM、Coordinator 和 DataNode，不能直接映射为单一 PostgreSQL primary/replica 集群。
- 当前初始化必须保留 GTM-first、CN/DN master、逐 master topology registration、restart、standby 和 group 创建等阶段边界。
- Coordinator 保存 catalog/topology 状态，不能在缺乏持久化分析时视为通用无状态前端。
- CloudNativePG 的 spec/status、角色感知生命周期、逐实例状态、监控和显式错误等概念可以借鉴，但不代表源码可直接复用。
- DN standby 扩展与 DN shard 扩展、数据重分布是不同操作。

## 设计范围

设计提出一个概念性 `OpenTenBaseCluster` 顶层资源，用于统一表达 GTM、Coordinator、DN shard、standby、Service、存储意图、初始化阶段、逐节点拓扑应用和 group 状态。工作负载 primitive 保持 `Unselected`，没有决定采用 Pod、StatefulSet 或混合方案。

在后续首个可运行 PoC 中，可优先评估 GTM、Coordinator 和每个 DataNode shard 分别使用 StatefulSet 配合 Headless Service，以验证稳定身份、DNS 和存储语义；这不代表最终生产 Operator 的 workload primitive 已确定，也不代表该方式已经通过 OpenTenBase 运行时验证。

## PoC 边界

静态 PoC 未实现自动故障转移、fencing、promotion 后收敛、在线重分片、数据重分布、安全 shard 移除、分布式一致性备份、生产恢复、major-version upgrade、生产级安全自动化或完整监控。

运行时 hostname 兼容性、Operator reconcile、工作负载替换、GTM/CN 精确持久化和数据库操作幂等性均未验证。

## 验证结果

离线验证器的实际结果为：

```text
38 passed, 0 failed, 1 warning
```

警告原因是环境缺少 PyYAML，因此使用文本级回退检查。`kubectl apply --dry-run=client` 因没有可访问的 API Server/OpenAPI 端点而未完成。没有使用 `--validate=false` 将其表述为验证成功。

## 未验证事项

- 完整 YAML parsing；
- Kubernetes API Server CRD admission 与 structural-schema enforcement；
- 样例资源 admission；
- OpenTenBase 进程初始化、启动和查询可用性；
- DNS/hostname 在 GTM、initdb、`pgxc_node`、replication 和 pool 路径中的兼容性；
- 自动故障转移、备份恢复、重分片、升级和生产安全。

## 推荐阅读顺序

1. 本 README；
2. [`research-report.md`](research-report.md)；
3. [`operator-design.md`](operator-design.md)；
4. [`poc/README.md`](poc/README.md)；
5. [`poc/crd/opentenbasecluster-crd.yaml`](poc/crd/opentenbasecluster-crd.yaml) 与样例；
6. [`poc/traceability/field-evidence-matrix.md`](poc/traceability/field-evidence-matrix.md)；
7. [`poc/validation/validation-results.md`](poc/validation/validation-results.md)；
8. [`github-discussion-draft.md`](github-discussion-draft.md)；
9. [`ai-usage-report.md`](ai-usage-report.md)；
10. [`submission/`](submission/) 中的提交文案。
