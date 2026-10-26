<!--
Copyright (C) 2019-2025 OpenTenBase Authors. All rights reserved.
SPDX-License-Identifier: BSD-3-Clause
-->

# OpenTenBase 接入 Kubernetes 部署框架：设计方案

关联 Issue：[#201](https://github.com/OpenTenBase/OpenTenBase/issues/201)
仓库基线：`master` 分支 `b612d77c`

---

## 这是什么

一份调研 + 设计文档，回答「OpenTenBase 能否接入 CloudNativePG 这类 Kubernetes PostgreSQL Operator，以及需要新建什么」。

**核心结论**：不能直接复用任何现有 Operator，但约一半的基础设施能力可以复用。必须新建的核心是「拓扑目录协调器」—— 因为 OpenTenBase 把节点地址持久化在系统表里，而不是靠 Kubernetes Service 动态解析。

## 这不是什么

**不是可运行的 Operator。** 没有 Go 代码，没有部署过任何集群，连 CRD 的 API Server admission 都未验证。

所有 Kubernetes 侧的行为都是设计意图。已验证与未验证的边界在 [`poc/validation-results.md`](poc/validation-results.md) 中逐条列明。

---

## 文档导航

| 文档 | 内容 |
| --- | --- |
| [`operator-design.md`](operator-design.md) | **主文档**。CNPG 抽象调研、源码事实、五个关键差异、可复用与新建清单、最小 PoC 设计、扩缩容边界、监控接入、7 项风险 |
| [`poc/opentenbasecluster-crd.yaml`](poc/opentenbasecluster-crd.yaml) | CRD 草案。校验规则从源码约束提取，非凭经验设定 |
| [`poc/sample-distributed.yaml`](poc/sample-distributed.yaml) | 分布式模式示例（2 GTM + 2 CN + 2 分片） |
| [`poc/sample-centralized.yaml`](poc/sample-centralized.yaml) | 集中式模式示例（故意不含 gtm 与 coordinators 段） |
| [`poc/validate.py`](poc/validate.py) | 离线校验脚本。仅标准库，65 项断言 |
| [`poc/validation-results.md`](poc/validation-results.md) | 校验结果，含**明确未执行的校验清单** |
| [`discussion-draft.md`](discussion-draft.md) | 社区讨论帖草案（仓库未开 Discussions，随 PR 提交） |
| [`AI_USAGE_REPORT.md`](AI_USAGE_REPORT.md) | AI 使用策略报告，含 6 处被源码推翻的判断 |

---

## 三个源码发现

本方案的价值集中在这三点，它们都不是通用分布式数据库经验能推出的：

### 1. GTM 地址是系统表状态，不是配置项

`initdb` 收到 `--master_gtm_*` 后执行的是一条 SQL：

```c
PG_CMD_PRINTF3("create gtm node %s with (type='gtm', host='%s',port=%s, primary=1);\n\n",
            master_gtm_nodename, master_gtm_ip, master_gtm_port);
```

依据 `src/bin/initdb/initdb.c:2165-2168`。

**因此 GTM 切换后改 `postgresql.conf` + reload 是无效的。** 这是一个容易犯且后果严重的设计错误。

### 2. 已存在全集群广播的 GTM 更新语法

```
ALTER CLUSTER GTM NODE nodename WITH (...)
```

依据 `src/backend/parser/gram.y:12949`；`cluster` 标志在 `src/backend/tcop/utility.c:973` 触发 `exec_type = EXEC_ON_ALL_NODES`。

**这可能让 GTM failover 简化为一条 SQL**，而不需要逐个进容器改配置。

### 3. 每节点占 3 个连续端口，且端口探测走 SSH

`node_port` / `pooler_port` / `forward_port` 从 11000 起连续分配，探测函数带 SSH 凭据参数。

依据 `contrib/opentenbase_ctl/src/utils/utils.cpp:39-95`。

Kubernetes 中 Pod 网络隔离，应改为静态端口约定，SSH 逻辑整体废弃。

---

## 快速验证

```bash
cd doc/design/issue-201/poc
python3 validate.py
```

预期：`通过 65 / 65`，退出码 0。无需 Kubernetes 集群，无需安装任何第三方包。
