# Project2：IVFFlat 索引诊断与优化建议系统——功能设计

## 1. 文档定位

本文档只回答 **“Project2 怎么工作、模块如何拆分、数据如何流转、P0/P1/P2 分别如何实现”**。

需求目标与交付边界见《Project2 需求设计》；每个指标为什么采、如何解释见《Project2 指标说明》。

当前 `benchmark/` 已包含冻结后的 P0/P1 实现；P2 为后续设计，不在本次提交范围。

---

## 2. 总体架构

```text
                         Diagnosis Orchestrator
                    数据集 / lists / probes / SLO
                               │
                          for each lists
                               │
                               v
                         Preflight
                   数据 / 配置 / 能力检查
                               │
                               v
                      Memory Estimator
                 pgvector 内存硬失败预估
                               │
                               v
                         Build Executor
                          CREATE INDEX
                               │
                  ┌────────────┴────────────┐
                  │                         │
                  v                         v
           Build Monitor              Resource Collector
        phase / progress / wait      CPU / RSS / IO / WAL
                  │                         │
                  └────────────┬────────────┘
                               v
                         Build Aggregator
                       Build facts / summary
                               │
                     ┌─────────┴─────────┐
                     │                   │
                     v                   v
             Structure Inspector     Quality Runner
                  （P1）           probes / Recall / Latency
                     │                   │
                     └─────────┬─────────┘
                               v
                       Joint Diagnosis（P1）
                               │
                               v
                 Recommendation / Pareto（P1）
                               │
                               v
                       Validator（P1）
                               │
                               v
                        Human Report
```

系统遵循三个原则：

- **采集与判断分离**：Collector 只产事实，Diagnosis 才下结论；
- **结构与质量分离**：结构异常不能直接等价 Recall 低；
- **建议必须验证**：未重跑只能是 proposed。

---

## 3. 总体控制流

```text
开始 Diagnosis Run
  │
  ├─ 加载 dataset / Ground Truth / 参数 / SLO
  ├─ 探测 capability
  │
  └─ 对每个 rows
       │
       └─ 对每个 lists
            │
            ├─ Preflight
            │    └─ 数据/参数不一致 → Build 失败，不 CREATE INDEX
            │
            ├─ Memory Estimator
            │    └─ 预计 hard failure → 默认 Skip Build
            │
            ├─ CREATE INDEX
            │    └─ 同时采集 phase / worker / CPU / memory / IO / wait
            │
            ├─ Build Aggregate + Report
            │
            ├─ P1：Structure Inspection
            │
            └─ Build 成功后，对每个 probes
                 └─ Quality Run
                      ├─ planner 检查
                      ├─ warmup
                      ├─ repeat 查询
                      └─ Recall / Latency / QPS

所有 lists / probes 完成
  ↓
P1：联合诊断
  ↓
P1：Pareto / Recommendation
  ↓
P1：Validation Run
  ↓
最终报告
```

一个 `lists` 对应一次 Build Run；一个 Build Run 可以关联多个 `probes` Quality Run。

---

## 4. 数据流

```text
输入参数 / 数据集 / Ground Truth
            │
            v
        Preflight facts ───────┐
            │                  │
            v                  │
      Memory estimate          │
            │                  │
            v                  │
       CREATE INDEX            │
            │                  │
      ┌─────┴─────┐            │
      v           v            │
 Progress      Resource        │
 samples       samples         │
      └─────┬─────┘            │
            v                  │
      Build aggregate          │
            │                  │
      ┌─────┴──────────┐       │
      v                v       │
 Structure facts(P1) Quality facts
      │                │       │
      └────────┬───────┴───────┘
               v
         Candidate Facts
               │
               v
        Finding / Evidence
               │
               v
       Recommendation / Pareto
               │
               v
         Validation result
               │
               v
         Markdown + JSON/CSV
```

原始采样始终保留，派生指标和诊断可以回溯到原始证据。

---

## 5. 代码目录与模块职责

### 5.1 当前 P0 代码

```text
benchmark/
├── common/                         # 诊断流程共享基础能力
│   ├── config.py
│   ├── db.py
│   ├── datasets/
│   ├── ground_truth.py
│   ├── metrics.py
│   ├── query_quality.py
│   ├── manifest.py
│   ├── schema.py
│   └── run_store.py
│
└── ivfflat_diagnosis/
    ├── run_matrix.py               # Diagnosis Run 编排
    ├── preflight.py                # 构建前检查 / capability
    ├── memory_estimator.py         # pgvector checked memory 估算
    ├── build.py                    # CREATE INDEX 生命周期
    ├── monitor.py                  # 周期采样调度
    ├── collectors/
    │   └── runtime.py              # PG + /proc 单次资源快照
    ├── aggregate.py                # Build 时序聚合
    ├── ivfflat_query.py            # Quality Run 编排
    ├── report.py                   # 人类报告
    ├── failure_tests.py            # 基础异常场景
    └── validate_recall.py          # 兼容 CLI
```

说明：`benchmark/` 是唯一用户入口；`ivfflat_diagnosis/` 与 `common/` 一并交付。

### 5.2 P1 新增模块

```text
OpenTenBase/contrib/pgvector/
└── src/ivfstats.c                  # IVFFlat 私有页面只读统计

ivfflat_diagnosis/
├── structure.py                    # 调用结构 SQL / 保存原始事实
├── diagnosis/
│   ├── model.py                    # Evidence / Finding 数据结构
│   ├── rules.py                    # 诊断规则
│   └── engine.py                   # 联合诊断
├── recommendation.py               # 可行性过滤 / Pareto / 建议
└── validation.py                   # before / after 重跑验证
```

### 5.3 P2 扩展模块

P2 不预先固化文件，只定义能力边界：pgvector 内部阶段精确埋点、filtered/concurrent workload、漂移和多节点扩展。

---

# 6. P0 功能设计——可信观测基线

## 6.1 P0 功能拆分

| 小功能 | 作用 | 当前主要实现 |
|---|---|---|
| Diagnosis 编排 | 组织 rows × lists × probes 实验 | `run_metric()` |
| Capability Probe | 确认哪些数据源可用 | `probe_capabilities()` |
| Preflight | 确认实验期望与数据库事实一致 | `collect_preflight_snapshot()` |
| Memory Precheck | 预测 pgvector 内存硬失败 | `estimate_pgvector_checked_memory()` |
| Build Lifecycle | 执行一次 CREATE INDEX | `build_index()` |
| Build Monitor | 周期调度采集器 | `monitor_build()` |
| Runtime Snapshot | 采 phase / worker / CPU / RSS / IO / WAL | `collect_runtime_snapshot()` |
| Build Aggregation | 阶段时间、delta、峰值、scope 聚合 | `aggregate_build_samples()` |
| Quality Evaluation | probes 查询、Recall、Latency、QPS | `run_quality_evaluation()` |
| Human Report | 生成根报告和 Build/Quality 报告 | `write_*_report()` |

## 6.2 P0 函数设计

| 函数 | 输入 | 输出 | 核心职责 |
|---|---|---|---|
| `run_metric(args, metric)` | metric、rows、lists、probes 等 | Diagnosis 结果目录 | 顶层串行编排，创建 diagnosis/build/quality ID |
| `probe_capabilities(conn)` | PG 连接、主机上下文 | capability map | 探测 progress、backend IO/WAL、`/proc`、PSS、IO timing 等 |
| `collect_preflight_snapshot(...)` | table/column/lists/requested rows | Preflight snapshot | 获取 actual rows、eligible vectors、dimension、GUC、主机约束；严格校验行数 |
| `estimate_pgvector_checked_memory(...)` | pgvector 版本、lists、dimension、样本规模、内存限额 | memory estimate | 复现 pgvector 0.8.6 受检查的内存计算，预测 hard failure |
| `build_index(...)` | BuildContext、Preflight、capability | BuildSummary | CREATE INDEX、启动/停止 monitor、收集 index metadata、聚合结果 |
| `monitor_build(...)` | leader PID、采样周期、capability | progress/resource JSONL | 周期采集并发现 worker，单项能力失败时降级 |
| `collect_runtime_snapshot(...)` | leader PID、PG stats conn、process registry | ProgressSample + ResourceSample[] | 一次对齐采样：progress/activity/backend stats + `/proc` |
| `aggregate_build_samples(...)` | progress/resource timeline、真实 build window | aggregate | 生成 phase 边界、CPU/IO delta、RSS/PSS peak、wait、build-run PG delta |
| `run_quality_evaluation(...)` | BuildContext、probes、queries、GT | QualityResult | planner 检查、repeat 测量、Recall/Latency/QPS 汇总 |
| `write_build_report()` / `write_quality_report()` / `write_root_reports()` | 聚合后的 Run 结果 | Markdown/CSV | 将机器事实变成人类入口 |

## 6.3 P0 运行状态

P0 当前已经形成以下主链：

```text
Preflight
  → Memory Precheck
  → Build + Runtime Collect
  → Aggregate
  → Quality
  → Human Report
```

P0 的输出是后续 P1 的输入；P0 不读取 IVFFlat 私有 list/page，也不生成正式参数建议。

---

# 7. P1 功能设计——结构检查与诊断闭环

P1 在 P0 的 Build / Resource / Quality 事实之上增加 Structure Facts，并完成：

```text
Structure Facts
    + P0 Build/Resource/Quality
    ↓
Candidate Facts
    ↓
Evidence → Finding
    ↓
SLO Filter → Pareto
    ↓
proposed Recommendation
    ↓
Validation Run
    ↓
validated / validation_failed
```

P1 分四层：

1. **数据库结构观测层**：pgvector 暴露 IVFFlat 原始结构事实。
2. **事实与诊断层**：Python 计算结构派生指标，并与 P0 数据联合产生 Finding。
3. **候选比较与建议层**：对 `lists × probes` 做 SLO 可行性过滤和 Pareto 筛选，生成 proposed 建议。
4. **验证层**：使用相同 workload 重跑，形成 before/after；验证通过后才能标记 validated。

## 7.1 数据库结构观测层

### 7.1.1 `ivfflat_list_stats()`

```sql
ivfflat_list_stats(index regclass, sample_rate real DEFAULT 1.0)
RETURNS TABLE (
    format_version integer,
    dimensions integer,
    declared_lists integer,
    list_id integer,
    list_page bigint,
    list_offset integer,
    start_page bigint,
    insert_page bigint,
    entry_pages bigint,
    physical_tuple_count bigint,
    usable_bytes bigint,
    used_bytes bigint,
    free_bytes bigint
)
```

职责：C 侧遍历 metapage → list page → entry page，按 list 返回原始结构事实。

关键口径：

- `physical_tuple_count`：entry page 当前实际存在的 `IndexTuple` 数；不宣称等于 MVCC live tuple。
- `used/free/usable_bytes`：只统计 entry page；`free_bytes` 使用 PostgreSQL page free-space 语义，`used = usable - free`。
- C 层只返回原始事实，不计算 CV、Gini、Recall 或参数建议。

### 7.1.2 `sample_rate`

抽样单位固定为 **list**：

- `1.0`：扫描全部 list；P1 正式诊断、coverage 和验收只使用全量模式。
- `(0,1)`：按固定 hash/seed 确定性选择 list；被选 list 的 entry chain 仍完整扫描。
- sample 模式允许输出 sample-scope 分布统计，但 `exact indexed tuples`、`coverage_ratio`、`exact empty_ratio` 标记 unavailable。

### 7.1.3 `ivfflat_index_stats()`

```sql
ivfflat_index_stats(index regclass)
RETURNS TABLE (
    format_version integer,
    dimensions integer,
    declared_lists integer,
    list_pages bigint,
    entry_pages bigint,
    physical_indexed_tuples bigint,
    empty_lists bigint,
    usable_bytes bigint,
    used_bytes bigint,
    free_bytes bigint,
    scanned_pages bigint,
    scan_elapsed_ms double precision
)
```

用途：DBA 查询和 SQL 回归交叉校验。正式 Diagnosis Run 优先全量调用 `ivfflat_list_stats()` 后由 Python 汇总，避免重复扫描大型索引。

### 7.1.4 错误、安全与并发语义

以下直接 ERROR：非 index、非 IVFFlat、非法 `sample_rate`、metapage/page type 不匹配、block 越界、page-chain cycle、无权限。

空索引、empty list、少数据多 lists 属于合法结果，计数返回 0。

实现要求：

- relation lock + buffer share lock；
- block/page 类型与 cycle 校验；
- `CHECK_FOR_INTERRUPTS()`；
- 只读，不修改页面、不产生 WAL；
- `SECURITY INVOKER`，建议 `VOLATILE PARALLEL RESTRICTED`；
- MVP benchmark 运行期间不持续 DML，因此不宣称并发写入下的事务级结构快照。

建议内部入口：

```text
ivfflat_list_stats() / ivfflat_index_stats()
              ↓
       IvfflatCollectStats()
        ├─ ValidateMetaPage
        ├─ ScanListPages
        └─ ScanEntryPages
```

## 7.2 事实与诊断层

### 7.2.1 Structure Collector

`structure.py` 新增：

```text
collect_structure_stats(build_id, index_oid, relfilenode, sample_rate)
```

执行前必须确认当前 `index_oid/relfilenode` 与 BuildSummary 一致；不一致则拒绝关联。

输出：

```text
builds/<build_id>/structure/
├── lists.jsonl
├── scan.json
└── summary.json
```

### 7.2.2 Structure Summary

`summarize_structure()` 计算：

| 指标 | 定义 |
|---|---|
| list tuple 分布 | min/mean/P50/P90/P95/P99/max/stddev |
| CV | `stddev / mean` |
| Gini | list tuple count 的 Gini |
| empty ratio | `empty_lists / declared_lists` |
| concentration | max/avg、P95/P50、Top-N tuple share |
| fill ratio | `sum(used_bytes) / sum(usable_bytes)` |
| coverage ratio | `physical_indexed_tuples / eligible_vectors` |
| amplification | `index_bytes / raw_vector_bytes` |

`coverage_ratio` 只在 full scan、无持续 DML、eligible vectors 可靠时作为正式证据。

结构阈值仅作为版本化 heuristic，不视为 pgvector 官方阈值。P1-v1 建议：

```text
moderate imbalance:
CV >= 0.5 AND (Gini >= 0.30 OR P95/P50 >= 2.0)

severe imbalance:
CV >= 1.0 AND Gini >= 0.50

empty-list warning:
eligible_vectors >= declared_lists AND empty_ratio >= 5%
```

若 `eligible_vectors < declared_lists`，empty-list anomaly 为 `not_applicable`。

**结构异常只能产生结构 Finding；不得单独推出 Recall 差或 lists 调整结论。**

### 7.2.3 Candidate / Evidence / Finding

**Candidate**：一个 `lists × probes` 配置；Build/Structure 属于 lists，Quality 属于具体 probes。

最小字段：

```text
Candidate:
  candidate_id, diagnosis_id, build_id, lists, probes
  build_metrics, resource_metrics, structure_metrics, quality_metrics
  validity, artifact_refs

Evidence:
  evidence_id, metric, value, unit, source
  build_id, quality_run_id, availability, artifact_ref

Finding:
  finding_id, rule_id, scope, candidate_ids
  status, evidence_ids, summary, likely_cause
  confidence, limitations
```

规则状态固定为：

```text
triggered        规则成立
clear            规则适用但未触发
unknown          规则适用，但必需证据缺失
not_applicable   当前场景不满足规则前提
```

## 7.3 第一批诊断规则

P1-v1 先实现少量可解释规则：

| Rule | 必需证据 | 结果 |
|---|---|---|
| Memory hard failure | checked memory + `maintenance_work_mem` | candidate infeasible |
| Worker quota | planned/launched workers + worker GUC | worker capacity Finding |
| Probes improve Recall | Recall<SLO + 更高 probes Recall 上升 + index/rows valid | probes increase candidate |
| Probes diminishing returns | 相邻 probes Recall/Latency 变化 | 收益递减 Finding |
| Structure impacts quality | 结构倾斜 + 同 Build quality 异常 + 其他 lists 对照 | medium-confidence joint Finding |

默认 diminishing-return heuristic：`ΔRecall < 0.002` 且 P95 增长 `>=20%`。阈值必须配置化和版本化。

## 7.4 候选比较与建议层

### 7.4.1 SLO 可行性过滤

先过滤不可用 Candidate：

```text
Build failed
predicted memory hard failure
Quality invalid / GT mismatch
未使用目标 IVFFlat
returned_rows 异常
```

若用户提供 SLO，再按已提供的约束过滤：

```text
Recall >= minimum
P95/P99 <= maximum
Build Time <= maximum
Peak Memory <= maximum
Index Size <= maximum
```

未提供的 SLO 不作为 hard reject。

### 7.4.2 Pareto

默认比较维度：

| 维度 | 方向 |
|---|---|
| Recall | maximize |
| P95 latency | minimize |
| Build time | minimize |
| Peak memory | minimize |
| Index size | minimize |

单客户端 QPS 不再单独进入 Pareto，避免与 latency 重复计权。

Peak memory 优先使用 process-group PSS；若只能使用 summed RSS，必须保证候选间 source/口径一致，否则该维度记为 incomparable。

无 SLO 时只输出 Pareto candidates，不宣称“最佳配置”。

### 7.4.3 Recommendation

`build_recommendations()` 输出 proposed 建议，最小字段：

```text
recommendation_id
status = proposed
action
current / recommended
finding_ids / evidence_ids
expected_effect
cost
scope
verification_workload
```

条件参数只有对应 Finding 证据完整时才允许进入建议。

## 7.5 验证层

`validate_recommendation()` 复用已有能力：

```text
P0 build_index()
P0 run_quality_evaluation()
P1 collect_structure_stats()
```

Validation Run 创建新的 `build_id/quality_run_id`，但仍属于原 `diagnosis_id`。

Before/After 必须保持以下条件一致：

```text
dataset / row range
metric / opclass
Ground Truth / query set / K
query_count / repeat
cache mode / client mode / planner mode
PG / pgvector version
机器与 cgroup 资源
```

仅允许 Recommendation 明确指定的参数发生变化。

P1 不声明统计学显著性，只使用 material-change tolerance：

```text
Recall: ±0.002 absolute
Latency / Build Time / Memory: ±5% relative
```

最终状态：

- `validated`：SLO 达成、预期方向改善、guardrail 无明显退化；
- `validation_failed`：建议未达到目标或出现明显退化；
- `not_comparable`：环境/workload 发生不可接受变化。

## 7.6 粒度与版本约束

- 文档中的“逐 list/page/tuple”统一解释为：**逐 list 输出聚合事实；C 内部逐 entry page 扫描并统计 tuple**，P1 不逐 tuple/page 输出明细行。
- 若未来需要逐 page fragmentation 诊断，在 P2 单独增加 page-level 接口。
- 正式数据库侧 PR 必须提供扩展版本升级路径和 upgrade SQL；不得静默修改 0.8.6 接口而不改变扩展版本。

## 7.7 P1 实施顺序

```text
1. 冻结 Structure SQL 与结构数据 schema
2. 实现 pgvector C scanner + SQL regression
3. 实现 structure.py + Structure Summary
4. 冻结 Candidate / Evidence / Finding schema
5. 实现 P1-v1 基础诊断规则
6. 实现 SLO feasibility + Pareto
7. 实现 proposed Recommendation
8. 实现 Validation + before/after + 最终报告
```

每一步先冻结输入/输出契约，再进入下一步；不要并行铺开 Diagnosis、Recommendation 和 Validation。

---

# 8. P2 功能设计——精确埋点与场景扩展

P2 不改变 P0/P1 主架构，只增强事实来源和 workload。

## 8.1 内部阶段精确埋点

目标：把目前 Native Phase 内部不可分的步骤拆成：

```text
sampling → k-means → assign/scan → tuplesort → load
```

实现方向：在 pgvector 构建路径增加结构化时间点/计数器；不得依赖解析人类可读 NOTICE 作为稳定协议。

## 8.2 其他增强

- k-means iteration progress；
- 更精确的内存峰值/MemoryContext 观测；
- filtered query / iterative scan；
- 并发和冷缓存 workload；
- 数据漂移和重建时机；
- 多节点或其他索引类型扩展。

P2 仅在具体需求明确后再进行函数级设计。

---

## 9. 结果目录

P0 当前目录：

```text
results/<dataset-metric>/<diagnosis_id>/
├── README.md
├── summary.md
├── build-summary.csv
├── quality-summary.csv
├── builds/lists-...__<short-id>/
│   ├── report.md
│   └── raw/
├── quality/lists-.../probes-...__<short-id>/
│   ├── report.md
│   └── raw/
└── raw/
```

P1 在此基础上增加：

```text
builds/<build>/structure/
findings.json
candidates.json
pareto.json
recommendations.json
validations/
diagnosis.json
report.md
```

原则：**根目录和 `report.md` 给人看；`raw/` 给程序和证据追查使用。**

---

## 10. 阶段验收

### P0

一次运行可以完整执行：Preflight → Memory Check → Build Observation → Resource Collection → Quality → Human Report；能力不可用时正确降级。

### P1

能够读取逐 list/page/tuple 结构，完成联合诊断、Pareto、Recommendation 和 before/after；同时形成数据库侧源码 PR。

### P2

在不破坏现有数据契约和诊断链路的前提下，提供更精确内部阶段或更多 workload 场景。
