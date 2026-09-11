# Project2：IVFFlat 索引诊断与优化建议系统——需求设计

## 1. 文档定位

本文档定义 **Project2 的需求**。

---

## 2. 要解决的问题

PostgreSQL + pgvector 的 IVFFlat 索引可以通过 `lists`、`probes` 等参数在构建成本、查询延迟和 Recall 之间做权衡，但仅执行 `CREATE INDEX` 无法回答以下问题：

1. **构建过程是否正常**：当前在哪个阶段，主要时间花在哪里，失败发生在哪一步。
2. **构建资源是否合理**：当前配置是否会触发内存硬失败，构建期间 CPU、内存、IO、WAL、等待和并行 worker 的表现如何。
3. **索引结构是否合理**：索引内部 list/page/tuple 如何分布，是否存在空 list、严重倾斜或空间异常。
4. **查询效果是否满足要求**：在明确的数据集、Ground Truth 和 workload 下，Recall、Latency、QPS 是否满足目标。
5. **参数应该如何调整**：如何结合构建成本、资源、索引结构和查询质量，对 `lists`、`probes`、`maintenance_work_mem` 等参数给出有证据、可验证的建议。

Project2 最终形成以下闭环：

```text
构建前检查
   ↓
构建过程与资源观测
   ↓
索引结构检查
   ↓
查询质量评估
   ↓
联合诊断
   ↓
参数建议
   ↓
相同 workload 重跑验证
```

---

## 3. 核心能力

### 3.1 Build Process Observation——构建过程观测

目标：回答 **“索引现在建到哪里、哪里最耗时、是否发生等待或失败”**。

需要具备：

- 识别 IVFFlat 原生构建阶段；
- 在阶段支持可靠进度时显示阶段进度；
- 记录总构建耗时和阶段耗时；
- 记录 leader / parallel worker、wait event；
- 构建失败、取消时保留已采集事实和失败阶段。

### 3.2 Build Resource Diagnosis——构建资源诊断

目标：回答 **“索引能不能建、为什么资源消耗高、为什么构建慢”**。

需要具备：

- 构建前根据 pgvector 源码模型估算受 `maintenance_work_mem` 约束的构建内存；
- 在预计触发 pgvector 内存硬错误时预警或阻止正式构建；
- 构建过程中观测 leader / workers 的 CPU、RSS/PSS、IO、wait 等资源；
- 在 PostgreSQL 能力允许时记录 Buffer、Temp、WAL 等数据；
- 资源证据不足时明确输出 `unavailable/unknown`，不得用 0 或单一指标制造确定结论。

### 3.3 Index Structure Inspection——索引结构检查

目标：回答 **“IVFFlat 最终建成了什么结构”**。

需要具备：

- 获取索引基本信息与总体大小；
- 获取逐 list 的 tuple/page 等原始事实；
- 派生 list 大小分布、empty-list、倾斜度、集中度、覆盖率和空间利用指标；
- 结构指标只描述索引事实，不单独等价为 Recall 好坏。

### 3.4 Workload Quality Evaluation——查询质量评估

目标：回答 **“这个索引在指定 workload 下查得准不准、快不快”**。

需要具备：

- 支持 L2 / Cosine / IP 三类向量距离；
- 基于匹配的数据范围和 Ground Truth 计算 Recall@K；
- 记录 mean、P50/P95/P99、stddev、单客户端 QPS；
- 检查查询是否实际使用目标 IVFFlat 索引；
- 对同一个 `lists` 扫描多个 `probes`，形成 Recall-Latency 权衡数据。

### 3.5 Joint Diagnosis & Parameter Recommendation——联合诊断与参数建议

目标：回答 **“为什么有问题、应该改什么、改完是否有效”**。

需要具备：

- 联合构建时间、资源、结构、质量和用户 SLO；
- 不凭单一指标直接下结论；
- 对候选配置做可行性过滤和 Pareto 比较；
- MVP 至少支持对 `lists`、`probes`、`maintenance_work_mem`、`max_parallel_maintenance_workers` 形成建议；
- 无 SLO 时只给相对候选，不宣称“最佳配置”；
- 未重跑的建议为 `proposed`，使用相同 workload 验证成功后才标记 `validated`。

---

## 4. 输入与输出

### 4.1 输入

| 类别 | 主要输入 |
|---|---|
| 数据库 | DSN、database、schema、table、vector column、index name |
| 数据 | dataset、rows、dimension、metric/opclass |
| 构建候选 | lists、maintenance_work_mem、并行参数 |
| 查询候选 | probes、K、query count、repeat |
| 质量基准 | Ground Truth、数据范围、缓存模式、客户端模式 |
| 目标约束 | Recall SLO、P95/P99、构建时间、内存等，可为空 |
| 环境 | PostgreSQL/pgvector 版本、CPU、内存、存储、关键 GUC |

### 4.2 输出

最终输出分为两层：

**汇总报告**

- Diagnosis 总结；
- Build 报告；
- Quality 报告；
- Structure 报告；
- Finding / Recommendation / Before-After。

**机器证据**

- manifest；
- progress/resource timeline；
- build/quality/structure 原始与聚合数据；
- errors、capability、诊断证据引用。

用户应能仅通过根目录报告理解实验结论；原始 JSON/JSONL 仅用于追查证据和自动处理。

---

## 5. 交付阶段

Project2 的 MVP 由 P0 + P1 完成；P2 为后续增强，不属于本期 MVP。

### P0：可信观测与查询质量基线

P0 目标：**事实采准。**

交付：

- Preflight：数据、对象、配置和采集能力检查；
- pgvector 构建内存硬失败预估；
- IVFFlat 原生阶段、耗时、worker、wait 观测；
- CPU、RSS/PSS、IO、可用 PG 资源数据采集；
- Recall / Latency / single-client QPS 评估；
- 失败、取消、能力降级的结果持久化；
- 人类可直接阅读的 Diagnosis / Build / Quality 报告。

P0 完成后能够回答：

> 怎么建、建到哪里、会不会内存失败、构建资源表现怎样、当前查询质量怎样。

P0 **不负责**逐 list 结构分析和正式参数推荐。

### P1：结构检查与完整诊断闭环

P1 目标：**在 P0 事实基础上，解释索引建成什么样，并形成可验证建议。**

交付：

- pgvector 侧只读 IVFFlat 结构统计接口，作为数据库侧源码增强 PR；
- 逐 list/page/tuple 结构事实及派生结构指标；
- 资源 + 结构 + 质量 + SLO 联合诊断；
- lists × probes 可行性过滤和 Pareto 候选；
- 参数 Recommendation；
- before/after Validation；
- 完整 Diagnosis Report。

P1 完成即达到 Project2 MVP。

### P2：增强能力

P2 不属于本期 MVP，主要包括：

- sampling / k-means / assign / tuplesort / load 的精确内部计时；
- k-means 迭代进度和更精确的内存埋点；
- 过滤查询、iterative scan、数据漂移；
- 冷缓存、并发 workload；
- 多节点聚合和其他索引类型扩展。

---

## 6. 本期明确不交付

- HNSW 等其他索引类型的完整诊断；
- 自动执行 `ALTER SYSTEM`、生产在线调参或自动重建；
- 生产并发容量承诺；
- NUMA、CPU 绑核、内核参数调优；
- 每次 MemoryContext 分配级追踪；
- 过滤条件、分区/部分索引、持续写入漂移的完整诊断；
- PostgreSQL 通用内核改造，除非后续证明 pgvector/外围能力无法满足且存在通用价值。

---

## 7. MVP 验收标准

MVP 完成时，一次 Diagnosis Run 应能够：

1. 对多个 `lists` 候选分别执行可追溯的 Build Run；
2. 在构建前识别数据/配置错误和预计内存硬失败；
3. 保存构建阶段、资源、失败和索引元数据；
4. 对成功索引执行多个 `probes` 的 Quality Run；
5. 获取逐 list/page/tuple 结构事实；
6. 联合结构、资源、质量和 SLO 形成有证据的 Finding；
7. 给出 proposed 参数建议，并支持相同 workload 重跑验证；
8. 至少形成一项数据库侧源码增强 PR，并通过自动测试和 benchmark 验证；
9. 人类报告可以直接说明“发生了什么、证据是什么、建议是什么、是否已验证”。
