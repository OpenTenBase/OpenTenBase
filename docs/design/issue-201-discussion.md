<!--
Copyright (c) 2026 OpenTenBase Contributors
Licensed under the BSD 3-Clause License. See LICENSE.txt.
-->

# 提案：以 CloudNativePG 为运行模式基线的 OpenTenBase Kubernetes Operator

## 背景

我针对 Issue #201 调研了 CloudNativePG、StackGres、Crunchy PGO 和 Zalando PostgreSQL Operator，并进一步对照了 OpenTenBase 当前 `opentenbase_ctl` 的源码实现。

核心结论是：OpenTenBase 不能被建模为一个普通 PostgreSQL 主备集群。它的正确状态同时依赖 GTM、多个 Coordinator（CN）、多个 DataNode（DN）、稳定节点身份、CN 节点目录和默认 node group。现有 PostgreSQL Operator 可以复用大量 Kubernetes 运维经验，但不能直接接管这些分布式语义。

## 推荐方案

推荐新建 `OpenTenBaseCluster` 高层 Operator：

- 一个 CR 表达集中式或分布式拓扑。
- GTM 使用一个 StatefulSet；每个逻辑 CN/DN 各使用一个 StatefulSet，同组 ordinal 承载主备实例。
- 使用 headless Service 提供稳定节点 DNS，单独的 CN read-write Service 作为客户端入口。
- 初始化状态机以当前源码为准：`GTM 主 -> CN/DN 主并行 -> 各备节点 -> 节点目录和默认 node group`。
- topology reconcile 使用 Kubernetes Lease 串行化，以规范化拓扑哈希作为 `topologyRevision`，所有 serving CN 都验证一致后才对外 Ready。
- PoC 对 DN groups 的变更 fail closed；不把 StatefulSet 扩缩容误当作数据再平衡。

CloudNativePG 作为设计基线，复用的是：

- CRD/status/conditions 的控制器模式；
- 每实例独立 PVC、shared-nothing 调度和反亲和；
- 基于数据库角色的 Service selector；
- Prometheus/PodMonitor 和自定义 SQL 指标接入；
- 先备后主、受控切换的滚动升级思想；
- 对象存储、凭据、Job 和备份插件的外围模式。

必须新建的是：

- GTM 生命周期和 fencing；
- CN/DN 角色化 initdb 与 instance agent；
- 跨角色 bootstrap 和 topology diff/reconcile；
- DN 再平衡边界；
- 全局一致备份 barrier、manifest 和整集群恢复；
- OpenTenBase 角色感知的升级与故障切换。

不建议直接 fork CNPG。CNPG 1.30 官方边界是 PGDG 支持版本、一个 Primary 加可选 Hot Standby；OpenTenBase 镜像可做 time-boxed 兼容性实验，但不应在未经上游支持和 e2e 证明时作为生产依赖。

## PoC 安全边界

第一版只完成声明式创建、幂等重试、重启恢复、稳定 Service/PVC 和拓扑一致性检查。以下能力默认关闭：

- DN 自动扩容/缩容；
- 未完成 fencing 协议的自动主备切换；
- 把各 DN 独立物理备份描述为全局 PITR；
- 删除 CR 时自动删除 PVC。

目标是先证明控制器在重复 apply、Operator 崩溃、Pod 重建和部分初始化失败后仍能收敛到同一个 topologyRevision，再逐步打开 HA、备份和在线拓扑变更。

## 希望社区确认的问题

1. 是否已有可供 Operator 调用的正式 topology diff/reconcile API？
2. GTM 推荐的选主、fencing 和同步协议是什么？
3. 通用形态是否同意在再平衡 API 成熟前把 DN groups 设为 immutable？
4. CN/DN 主备切换后，节点目录需要在哪些节点上原子更新？
5. 是否已有全局一致备份的事务屏障或快照接口？
6. Operator 更适合放在本仓库 `contrib/`，还是独立仓库？
7. 是否接受“以 CNPG 行为模式为基线，但不承诺 API/二进制兼容”的定位？

完整设计、CRD 草案、示例资源和 AI 使用报告会随对应 PR 提交。欢迎维护者重点审阅初始化顺序、GTM fencing、节点目录幂等性和 DN 扩缩容边界。
