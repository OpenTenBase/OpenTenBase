<!--
Copyright (C) 2019-2025 OpenTenBase Authors. All rights reserved.
SPDX-License-Identifier: BSD-3-Clause
-->

# 社区讨论帖草案

> **说明**：Issue #201 要求在 GitHub Discussions 发帖。经确认，OpenTenBase 仓库当前**未开启 Discussions 功能**（issue #201 下 @EagleBear2002 于 7 月 12 日也提出了同一问题，尚无回复）。
>
> 因此本文以 Markdown 形式随 PR 提交，作为讨论帖的完整草案。一旦仓库开启 Discussions，可直接发布；也可由维护者转为 issue 讨论。

---

## 标题

[RFC] OpenTenBase 接入 Kubernetes：拓扑目录协调是核心难点，而非 Pod 编排

---

## 1. 背景

社区已有成熟的 PostgreSQL Operator（CloudNativePG、StackGres、Crunchy PGO、Zalando）。它们围绕「一主多备」构建，而 OpenTenBase 是包含 GTM、Coordinator、DataNode 三种角色的分布式数据库，不能直接套用。

我调研了 CloudNativePG 的核心抽象，并逐条核对了 OpenTenBase 仓库（基线 `b612d77c`）的源码实现，形成了一份设计方案。

**本帖希望讨论的不是「怎么写 StatefulSet」，而是三个更底层的问题。** 我在核对源码时发现，真正的难点和社区常见讨论的方向有偏差。

---

## 2. 三个源码发现

### 发现 1：GTM 地址是系统表状态，不是配置项

我原本以为 GTM 地址存在 `postgresql.conf` 中，切换时改配置再 reload 即可。但源码显示，`initdb` 收到 `--master_gtm_*` 参数后执行的是一条 SQL：

```c
PG_CMD_PRINTF3("create gtm node %s with (type='gtm', host='%s',port=%s, primary=1);\n\n",
            master_gtm_nodename, master_gtm_ip, master_gtm_port);
```

来源：`src/bin/initdb/initdb.c:2165-2168`

这意味着 **GTM 切换后改配置文件是无效的**，必须通过 SQL 更新系统表。

### 发现 2：已经存在全集群广播的 GTM 更新语法

语法层面提供了正好合适的工具：

```
ALTER GTM NODE nodename WITH (...)          -- 仅当前节点
ALTER CLUSTER GTM NODE nodename WITH (...)  -- 全集群
```

来源：`src/backend/parser/gram.y:12939, 12949`

`cluster` 标志在 utility 层触发广播：

```c
case T_AlterNodeStmt:
    PgxcNodeAlter((AlterNodeStmt *) parsetree);
    if (((AlterNodeStmt *) parsetree)->cluster)
        exec_type = EXEC_ON_ALL_NODES;
```

来源：`src/backend/tcop/utility.c:971-975`

**这可能让 GTM failover 简化为一条 SQL**，而不需要 Operator 逐个进入容器改配置再 reload。

### 发现 3：端口分配依赖 SSH，且每节点占 3 个端口

```c
node_port    = current_port;
pooler_port  = current_port + 1;
forward_port = current_port + 2;
```

来源：`contrib/opentenbase_ctl/src/utils/utils.cpp:39-95`，起始端口 11000

而 `check_port_available()` 的签名带 `username` / `password` / `ssh_port` —— 端口探测是 SSH 远程完成的。Kubernetes 中每个 Pod 有独立网络命名空间，这段逻辑应整体替换为静态端口约定。

---

## 3. 核心结论

**最难的部分不是把三种角色跑起来，而是维持拓扑目录与实际 Pod 地址的一致。**

OpenTenBase 的 CN 和 DN 需要在本地系统表中注册所有对端的 host 与 port。这与普通 PostgreSQL 靠 Service 动态解析的模型根本不同。Pod 一旦漂移，目录就失效。

因此我建议把「拓扑漂移」作为 CRD 的一等状态（`status.phase: TopologyDrifted`）和一等监控指标，而不是只在日志里体现。

可复用的能力大约一半：声明式 CRD 模型、PVC 与存储类管理、单节点流复制、instance manager 模式、Prometheus 集成。

必须新建的核心是：拓扑目录协调器、GTM 角色控制器、多角色引导状态机、node group 与分片管理、免 SSH 初始化路径、分布式一致性备份。

---

## 4. 希望讨论的问题

### Q1：Headless Service 的 FQDN 长度是否安全？

节点名上限已确认为 64（`src/include/pgxc/nodemgr.h:21`），但注册进系统表的 host 字段使用 `NAMEDATALEN`（`nodemgr.c:810-811`）。

Kubernetes 的完整 FQDN 形如：

```
otb-demo-dn-0-0.otb-demo-dn-0-headless.opentenbase.svc.cluster.local
```

这已经接近 64 字符。**请问 host 字段的实际可用长度是多少？** 如果不足，是否建议注册短域名并依赖 search domain，还是有其他推荐做法？

### Q2：`ALTER CLUSTER GTM NODE` 在 GTM 主节点已宕机时能否成功？

发现 2 提供了理想的 failover 路径，但我没有条件验证故障场景。GTM 已经不可用时，这条广播语句本身是否会受影响（例如获取全局事务信息失败而无法执行）？

如果会受影响，推荐的 GTM failover 顺序是什么？

### Q3：在线分片重分布的实际语义是什么？

`ALTER NODE GROUP default_group ADD (dn0003)` 之后，数据重分布：

- 是否阻塞写入？阻塞粒度是表级还是分片级？
- 中断后能否续做，还是必须重来？
- 是否有官方推荐的重分布工具或存储过程？

这是我方案中最大的未验证区域。在明确之前，我倾向于建议 Operator **不自动执行**重分布，只负责节点注册，把数据迁移交给独立的 Kubernetes Job 并允许人工介入。请问这个边界是否合理？

### Q4：StatefulSet 还是直接管理 Pod？

CloudNativePG 刻意不用 StatefulSet，因为其顺序约束会阻碍精细化故障转移。

但 OpenTenBase 把地址写进系统表，稳定 DNS 标识的价值很高，所以我倾向于先用 StatefulSet。**想听听社区对这个权衡的看法** —— 是否有已知的场景会让 StatefulSet 成为障碍？

### Q5：GTM 可导出哪些监控指标？

GTM 不是 PostgreSQL 实例，没有 `pg_stat_*` 视图。`src/gtm/README` 说明了源码结构，但我未确认可采集的统计项。是否已有可用的统计接口，或者需要从头实现 exporter？

---

## 5. 当前交付物的边界

必须明确说明：**这是静态设计方案，不是可运行的 Operator。**

未验证的内容包括：Kubernetes API Server 的 CRD admission 校验、实际 reconcile 行为、自动故障转移、在线分片扩容、分布式一致性备份、生产级节点替换语义。

完整设计文档、CRD 草案、示例与离线校验脚本见 PR。欢迎指出源码理解上的错误 —— 如果我对上述任何一条源码行为的解读有偏差，希望能得到纠正。
