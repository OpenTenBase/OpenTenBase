<!--
Copyright (C) 2019-2025 OpenTenBase Authors. All rights reserved.
SPDX-License-Identifier: BSD-3-Clause
-->

# 校验结果记录

关联 Issue：#201
仓库基线：`b612d77c`
执行时间：2026-07-30

本文记录本次交付物实际执行过的校验，以及**明确未能执行**的校验。目的是让评审者能区分「已验证」与「设计意图」。

---

## 1. 离线结构校验

命令：

```bash
cd doc/design/issue-201/poc
python3 validate.py
```

结果：**通过 65 / 65**，退出码 0。

该脚本只依赖 Python 标准库，无需 Kubernetes 集群，也无需安装 PyYAML，评审者可零环境成本复现。

覆盖的检查类别：

| 类别 | 检查项数 | 说明 |
| --- | --- | --- |
| 文件存在性 | 6 | 全部交付物齐全 |
| CRD 基本结构 | 5 | apiVersion、kind、命名、status 子资源、scope |
| 三种角色齐全 | 3 | gtm、coordinators、datanodes |
| 源码硬约束编码 | 12 | 见下节 |
| 示例一致性 | 9 | 含集中式示例的反向断言 |
| 设计文档证据链 | 7 | 7 个源码文件必须被引用 |
| 风险显式标注 | 9 | R1 至 R7 及边界声明 |
| Markdown 结构 | 3 | 代码围栏配对 |
| 换行符 | 6 | 必须为 LF |

### 1.1 被编码为断言的源码约束

这是本校验脚本与「形式化检查」的区别 —— 断言内容来自源码而非通用规范：

| 断言 | 源码依据 |
| --- | --- |
| 支持 distributed 与 centralized 两种模式 | `config.h:24` |
| 定义 node / pooler / forward 三个端口 | `utils.cpp:39-95` |
| 默认 node 端口为 11000 | `utils.cpp` `START_PORT` |
| 说明 GTM 主节点恒为 1 | `config.h` `ConfigFileGtm::master` |
| 引用节点名长度上限 64 | `nodemgr.h:21` |
| 节点组名 maxLength 不超过 64 | 同上 |
| 默认节点组名为 default_group | `cluster.cpp:220` |
| 引导阶段顺序为 GTM → DN → CN | `cluster.cpp:844-870` |
| 含 poolReloadAfterChange | `cluster.cpp:241` |
| 提供 alterClusterGtmNode 策略 | `gram.y:12949`、`utility.c:973` |
| 集中式示例不含 gtm 段 | `config.h:24` |
| 集中式示例不含 coordinators 段 | `config.h:24` |

其中「引导阶段顺序」的断言方式是比较 `BootstrappingGTM`、`BootstrappingDN`、`BootstrappingCN` 在 CRD 文本中的出现位置，把编排顺序约束固化为结构检查，而不是只写在文档段落里。

---

## 2. YAML 真实解析校验

使用 PyYAML 对三份 YAML 执行真实解析（非文本匹配）：

```
opentenbasecluster-crd.yaml        解析 OK，文档数 = 1
sample-distributed.yaml            解析 OK，文档数 = 1
sample-centralized.yaml            解析 OK，文档数 = 1
```

解析后确认的 CRD 结构：

| 项 | 值 |
| --- | --- |
| group | `opentenbase.org` |
| kind | `OpenTenBaseCluster` |
| version | `v1alpha1` |
| spec required | `['mode', 'image', 'datanodes']` |
| spec 字段 | coordinators, datanodes, gtm, image, imagePullPolicy, mode, monitoring, nodeGroup, nodeNaming, ports, topologyReconcile |
| status 字段 | conditions, coordinators, datanodes, gtm, observedGeneration, phase, topology |
| phase enum | Pending, BootstrappingGTM, BootstrappingDN, BootstrappingCN, RegisteringTopology, Running, TopologyDrifted, Reconciling, Degraded |

两份示例的顶层 spec 键：

| 示例 | mode | spec 键 |
| --- | --- | --- |
| sample-distributed | distributed | coordinators, datanodes, gtm, image, imagePullPolicy, mode, monitoring, nodeGroup, nodeNaming, ports, topologyReconcile |
| sample-centralized | centralized | datanodes, image, mode, monitoring, ports, topologyReconcile |

集中式示例确实不含 `gtm` 与 `coordinators`，与 `config.h:24` 的行为一致。

---

## 3. 示例与 CRD 交叉校验

递归遍历示例的每个字段，确认其在 CRD schema 中均有定义：

```
== sample-distributed.yaml
  apiVersion 匹配 CRD: opentenbase.org/v1alpha1
  kind 匹配: OpenTenBaseCluster
  required 字段齐全: ['mode', 'image', 'datanodes']
== sample-centralized.yaml
  apiVersion 匹配 CRD: opentenbase.org/v1alpha1
  kind 匹配: OpenTenBaseCluster
  required 字段齐全: ['mode', 'image', 'datanodes']

交叉校验结果: ALL OK
```

**零个未定义字段。** 示例不会引用 CRD 中不存在的字段。

---

## 4. 源码引用行号逐条复核

对设计文档中引用的每个行号，读取该行并确认包含预期内容：

| 文件:行 | 预期内容 | 结果 |
| --- | --- | --- |
| `initdb.c:2165-2168` | `create gtm node` | OK |
| `gram.y:12939` | `ALTER GTM NODE` | OK |
| `gram.y:12949` | `ALTER CLUSTER GTM NODE` | OK |
| `utility.c:973` | `->cluster` | OK |
| `nodemgr.h:21` | `PGXC_NODENAME_LENGTH` | OK |
| `nodemgr.c:1471` | `PGXC_NODENAME_LENGTH` | OK |
| `utils.cpp:39` | `get_available_port_pair` | OK |
| `types.h:72` | `forward_port` | OK |
| `config.h:24` | `centralized` | OK |
| `cluster.cpp:220` | `CREATE DEFAULT node group` | OK |
| `cluster.cpp:241` | `pgxc_pool_reload` | OK |
| `cluster.cpp:290` | `CREATE sharding group` | OK |

**复核过程中发现并修正了 3 处行号偏差**（初稿写作时的引用误差）：

| 初稿 | 修正后 | 说明 |
| --- | --- | --- |
| `config.h:23` | `config.h:24` | 第 23 行是 `name` 字段，`type` 字段在第 24 行 |
| `cluster.cpp:239` | `cluster.cpp:241` | `pgxc_pool_reload` 实际在 241 行 |
| `initdb.c:2165` | `initdb.c:2165-2168` | 2165 是函数签名行，SQL 在 2167 行，改为范围引用 |

这三处如果不修正，评审者按行号查看会看到无关内容。

---

## 5. Git 检查

```
git diff --check     通过，无空白错误
换行符               全部文件 CRLF = 0（纯 LF）
```

注意：本仓库文件原始为 LF。Windows 环境下 `core.autocrlf=true` 会导致入库时转为 CRLF，因此提交前显式关闭该设置并做了字节级复核。

---

## 6. 明确未能执行的校验

以下内容**没有验证**，对应设计文档第 9 节的风险项：

| 未执行的校验 | 原因 | 对应风险 |
| --- | --- | --- |
| Kubernetes API Server CRD admission | 无可用集群 | — |
| `kubectl apply` 实际创建资源 | 无可用集群 | — |
| Operator reconcile 行为 | 未实现 Operator | — |
| Headless Service FQDN 长度实测 | 需真实集群执行 `CREATE NODE` | R1 |
| GTM 宕机时 `ALTER CLUSTER GTM NODE` 行为 | 需故障注入环境 | R2 |
| 在线分片重分布的阻塞与恢复语义 | 需真实集群与数据 | R3 |
| StatefulSet 对 failover 的实际影响 | 需 failover 原型 | R4 |
| GTM 可导出指标清单 | 未逐一阅读 `src/gtm/main` 统计实现 | R5 |
| 目录注册中断的恢复效果 | 需故障注入 | R6 |
| 分布式一致性备份可行性 | 未设计 | R7 |

**因此本交付物的性质是：经过静态结构校验与源码依据复核的设计方案，不是已验证可运行的实现。**

---

## 7. 复现方式

```bash
# 离线结构校验（无外部依赖）
cd doc/design/issue-201/poc
python3 validate.py

# 可选：YAML 真实解析（需要 PyYAML）
python3 -c "import yaml,sys; [yaml.safe_load_all(open(f,encoding='utf-8')) and print(f,'OK') for f in ['opentenbasecluster-crd.yaml','sample-distributed.yaml','sample-centralized.yaml']]"
```
