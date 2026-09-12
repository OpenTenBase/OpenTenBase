# Project2：IVFFlat 索引诊断指标说明

## 1. 文档定位

本文档面向具备 PostgreSQL/运维基础的读者，回答：

> **每个指标是什么、为什么要观测、从哪里得到、多个指标组合后能判断什么。**

状态说明：

- **P0**：当前观测基线；
- **P1**：结构检查和联合诊断；
- **P2**：精确内部埋点/扩展场景。

---

# 2. 索引构建过程指标

## 2.1 Native Progress Phase（P0）

这些是 pgvector 通过 PostgreSQL `pg_stat_progress_create_index` 暴露的粗粒度构建阶段。

| 指标/阶段 | 含义 | 为什么观测 | 来源 |
|---|---|---|---|
| `initializing` | 初始化 IVFFlat 构建上下文和必要状态 | 判断构建是否已进入实际工作阶段；发现初始化异常 | PG progress |
| `performing k-means` | 采样并训练 IVFFlat 聚类中心的粗粒度阶段 | 判断聚类训练是否为构建主要成本 | PG progress |
| `assigning tuples` | 扫描表中向量并分配到对应 list，内部还包含后续排序准备 | 判断扫描/距离计算/分桶阶段成本 | PG progress |
| `loading tuples` | 将整理后的 index tuples 写入 IVFFlat 索引页面 | 判断最终索引落盘是否成为主要成本 | PG progress |
| phase elapsed | 某 phase 持续时间 | 找出构建主要耗时阶段 | 轮询 phase 派生 |
| total build time | `CREATE INDEX` 总 wall time | 作为所有阶段和参数配置的总体构建成本 | Build Executor |
| progress percent | 当前阶段内的有效完成比例 | 判断是否持续推进、是否长期停滞 | PG progress；仅单位可靠时 |
| wait event | backend 当前等待类型 | 区分 IO、Lock、LWLock、IPC 等等待 | `pg_stat_activity` |
| leader/worker PID | 构建 leader 和并行 worker | 判断并行构建是否真正启动、资源应归属于谁 | `pg_stat_activity` |
| failure phase | 构建失败时最后观测阶段 | 缩小故障定位范围 | phase timeline + SQL error |

### 进度语义

- `loading tuples`：目标版本确认 `tuples_done / tuples_total` 可靠时显示阶段进度。
- `assigning tuples`：`blocks_done / blocks_total` 当前必须经过目标版本 capability 验证；未验证时只显示 phase/elapsed，不生成虚假百分比。
- 不生成跨 phase 的“总完成百分比”，因为不同 phase 的工作单位和成本不可直接相加。

## 2.2 Internal Build Stage（P2 精确化方向）

这些是 IVFFlat 源码内部更细的实际步骤，不等于 PostgreSQL 原生 phase。

| 内部步骤 | 含义 | 为什么希望观测 |
|---|---|---|
| `sampling` | 从基础向量中取得 k-means 训练样本 | 区分“取样慢”与“真正聚类慢” |
| `k-means` | 根据样本训练 `lists` 个 centroid | 分析 lists 增大后聚类 CPU/内存成本 |
| `assign/scan` | 扫描基础表，计算向量到 centroid 的距离并确定所属 list | 判断表扫描与分桶计算成本 |
| `tuplesort` | 使用 PostgreSQL tuplesort 将待写入 index tuples 按构建需要整理/分组 | 判断排序是否耗时、是否发生 temp spill |
| `load` | 将整理后的 tuples 写入 list/entry pages | 判断索引页面写入和 WAL/IO 侧成本 |

P0 中 `performing k-means` 不能可靠拆成 sampling 与 k-means，`assigning tuples` 也不能精确拆出 tuplesort；P2 才考虑源码结构化埋点。

## 2.3 组合判断

- **某 phase wall time 高 + CPU 高 + IO/wait 低**：更可能是计算主导。
- **loading time 高 + WAL/IO wait 明显**：说明写入/WAL 路径值得重点排查，但仍需 IO/WAL 证据确认。
- **wait event 长期为 Lock/LWLock/IPC**：不能把 wall−CPU 的差值简单判断成 IO。
- **planned worker 多但 active/launched 少**：应检查 worker 配额和并行配置。

---

# 3. Build Resource Diagnosis 指标

## 3.1 内存：必须区分三层

### 第一层：pgvector 源码预测内存

| 指标 | 含义 | 为什么观测 | 来源 |
|---|---|---|---|
| `estimated_pgvector_checked_memory_bytes` | 按 pgvector 0.8.6 源码中受检查的 k-means 相关分配估算的内存 | 在 CREATE INDEX 前预测是否会触发 pgvector 内存硬错误 | P0 Memory Estimator |
| sample count / centers / Elkan bounds 等分项 | 估算值的组成 | 解释为什么 `lists` 增大后内存快速上升 | pgvector 源码公式 |
| `memory_headroom` | `maintenance_work_mem - estimated checked memory` | 判断距离硬限制还有多少余量 | 派生 |
| `predicted_memory_failure` | 是否预计超过 pgvector 检查上限 | 决定 Build 是否默认跳过 | 派生 |

这个估算只回答：

> **会不会撞 pgvector 的 `maintenance_work_mem` 检查。**

它不预测整个 PostgreSQL backend 最终 RSS。

### 第二层：PostgreSQL 内存预算

| 指标 | 含义 | 为什么观测 | 来源 |
|---|---|---|---|
| `maintenance_work_mem` | CREATE INDEX/VACUUM 等维护操作可使用的一类工作内存预算 | 与 pgvector checked-memory 估算比较，判断硬失败 | PG GUC |
| `work_mem` | 普通排序/hash 等操作的工作内存预算 | 在涉及其他排序路径时作为上下文 | PG GUC |
| cgroup/host memory limit | 当前进程实际可用的主机/容器内存边界 | 即使 pgvector 内存检查通过，也可能存在整机内存压力 | OS/cgroup |

`maintenance_work_mem` **不是 PostgreSQL 进程 RSS 上限**。

### 第三层：实际进程物理内存

| 指标 | 含义 | 为什么观测 | 来源 |
|---|---|---|---|
| leader RSS | leader 当前驻留物理页总量 | 看 leader 内存变化和峰值 | `/proc/<pid>/status` |
| worker RSS | worker 当前驻留物理页总量 | 不遗漏并行 worker 的内存压力 | `/proc` |
| process-group summed RSS | 同一时刻 leader + workers RSS 求和 | 看进程组内存压力趋势；可能重复计算共享页 | P0 聚合 |
| PSS | 私有页全算、共享页按共享进程数比例分摊 | 比 RSS 求和更接近进程组实际造成的物理内存压力 | `/proc/<pid>/smaps_rollup` |

**RSS 与 PSS 的关键区别：**

```text
RSS：一个共享页会在每个使用它的进程中完整计数
PSS：共享页会按共享该页的进程数量分摊
```

因此多个 PostgreSQL backend 的 RSS 直接相加可能高估物理内存；PSS 更适合估计进程组实际压力。PSS 无权限时必须标记 unavailable。

## 3.2 CPU / IO / WAL / Temp / Worker / Wait

| 指标 | 作用 | 为什么观测 | 来源/粒度 |
|---|---|---|---|
| user/system CPU seconds | 计算 CPU 消耗 | 判断阶段是否 CPU busy | `/proc`，可按 backend/phase 聚合 |
| effective CPU cores | `CPU seconds / wall seconds` | 粗看并行阶段实际使用了多少 CPU 核 | 派生 |
| process read/write bytes | OS 进程实际 IO 字节数 | 观察构建期间磁盘读写变化 | `/proc/<pid>/io` |
| PG backend IO | PostgreSQL 视角 IO 累积量 | 与 OS IO 交叉验证、了解 PG 层读写 | PG backend stats；当前优先 Build Run scope |
| IO read/write time | PostgreSQL IO 时间 | 判断等待是否真实来自 IO | `track_io_timing=on` 时可用 |
| Buffer hit/read/dirtied/written | Buffer 使用行为 | 分析缓存命中和页面写入 | statement/PG stats；按可靠粒度 |
| temp bytes/files | 临时文件/排序落盘 | 识别 tuplesort spill | PG stats/log |
| WAL bytes/records/FPI | 索引构建产生的 WAL 工作量 | 解释 load/write 路径成本 | backend/statement stats；不强行 phase 归因 |
| planned/launched/active workers | 并行 worker 计划和实际启动情况 | 判断 worker 配额是否限制构建 | PG activity/config |
| wait_event_type / wait_event | backend 等待原因 | 区分 IO、Lock、IPC、WAL 等等待 | `pg_stat_activity` |

### 组合判断

- **CPU 高 + phase wall 占比高 + IO wait 低** → 可支持“计算主导”Finding。
- **IO time/吞吐异常 + IO wait 明显** → 可支持“IO 主导”Finding。
- **temp bytes > 0 + tuplesort 相关阶段耗时高** → 可支持“排序 spill”Finding。
- **planned > launched + worker 配额接近上限** → 可支持“worker 配额不足”Finding。
- **只看到 wall time >> CPU time** → 证据不足，不能直接说 IO 瓶颈。

---

# 4. Index Structure Inspection 指标（P1）

结构数据主要不是从 Build Time/Recall 猜出来，而是由 pgvector C/SQL 接口直接读取 IVFFlat 私有 list/entry page，再由 Python 做统计派生。

## 4.1 原始结构事实

| 指标 | 含义 | 为什么观测 | 来源 |
|---|---|---|---|
| declared lists | 索引声明的 list 数 | 验证结构与构建参数一致 | pgvector metapage |
| dimension | 索引向量维度 | 验证对象和数据身份 | pgvector/PG metadata |
| indexed tuples | 实际存在于索引中的 tuple 数 | 与 eligible vectors 比较索引覆盖情况 | pgvector page scan |
| per-list tuple count | 每个 list 包含多少索引 tuple | 观察聚类分布是否均衡 | `ivfflat_list_stats()` |
| per-list entry pages | 每个 list 使用多少 entry page | 观察 list 大小和空间压力 | pgvector page scan |
| used/free bytes | entry pages 的已用/剩余空间 | 判断页面填充情况 | pgvector page scan |
| index bytes/pages | 索引总体大小 | 观察不同 lists 的空间成本 | PG relation metadata |

## 4.2 派生结构指标

| 指标 | 作用 | 为什么观测 |
|---|---|---|
| min/mean/P50/P90/P95/P99/max list size | 描述 list 大小分布 | 识别长尾和极端 list |
| stddev / CV | 描述相对离散程度 | 不同平均规模下比较不均衡程度 |
| Gini | 描述整体分布不均程度 | 识别 tuple 是否集中在少数 list |
| empty list count/ratio | 有多少 list 没有 tuple | 识别 lists 过细或聚类结果异常的信号 |
| max/avg、P95/P50 | 直观倾斜比 | 快速判断长 list 是否远高于典型 list |
| top-N tuple share | 最大若干 list 占全部 tuple 的比例 | 判断数据是否高度集中 |
| coverage ratio | indexed tuples / eligible vectors | 检查索引覆盖是否完整 |
| page fill ratio | 页面使用率 | 判断页面空间利用情况 |
| index amplification | index bytes / raw vector bytes | 比较索引空间成本；不能单独认定“浪费” |

### 组合判断

结构指标只回答 **“建成什么样”**。

例如：

- `empty ratio` 高、CV/Gini 高 → 说明 list 分布明显不均；
- 但只有再结合同一 Build Run 的 Recall/Latency，才能判断这种结构是否影响业务质量；
- 不允许仅凭 Gini 高直接得出“Recall 低”的结论。

---

# 5. Workload Quality Evaluation 指标

| 指标 | 含义 | 为什么观测 | 来源 |
|---|---|---|---|
| Recall@K | ANN Top-K 与 Ground Truth 的重合比例 | 衡量近似索引查询正确性 | benchmark |
| returned rows | 实际返回结果数 | 防止“少返回结果”被误认为 Recall 结果 | benchmark |
| mean latency | 平均单查询延迟 | 观察总体性能 | benchmark |
| P50 | 中位查询延迟 | 表示典型查询体验 | benchmark |
| P95/P99 | 高分位延迟 | 观察尾延迟和稳定性 | benchmark |
| stddev | 延迟波动 | 判断重复运行是否稳定 | benchmark |
| total seconds | 整批查询 wall time | QPS 的直接计算基础 | benchmark |
| single-client QPS | query count / total seconds | 比较单客户端顺序吞吐；不是并发容量 | benchmark |
| `used_ivfflat` | planner 是否真正用了目标索引 | 防止把 Seq Scan 等结果当 IVFFlat 性能 | EXPLAIN |
| Ground Truth ID | GT 对应 dataset/rows/metric/K | 防止数据范围错配导致 Recall 失真 | benchmark metadata |
| cache/client mode | warm/cold、single/concurrent | 明确性能结果适用范围 | run metadata |

## probes 组合分析

同一个 `lists` 下扫描多个 `probes`：

```text
probes 增大
  → 通常扫描更多 lists
  → Recall 往往提高
  → Latency/QPS 成本也上升
```

因此关注：

- 满足 Recall SLO 的最小 probes；
- Recall 边际提升已经很小但 P95 明显上涨的收益递减点；
- 自然 planner 未使用 IVFFlat 时，该 Quality Run 不进入正式索引诊断。

---

# 6. Joint Diagnosis & Recommendation 指标/结果（P1）

这部分不是新的底层采集指标，而是前述指标的组合结果。

| 结果 | 作用 |
|---|---|
| Evidence Set | 指向支撑某个判断的原始/派生指标 |
| Finding | 描述观察到的问题或现象 |
| Likely Cause | 在现有证据下最可能的原因 |
| Confidence | high / medium / low，反映证据完整性 |
| SLO pass/fail | 当前候选是否满足 Recall/Latency/Memory/Build Time 等约束 |
| Pareto status | 候选是否被其他候选全面支配 |
| Recommendation | 推荐调整的参数/动作及代价 |
| proposed/validated | 建议是否已经经过相同 workload 重跑验证 |

## 典型组合规则

| 现象 | 需要的关键证据 | 可形成的建议方向 |
|---|---|---|
| 预计内存硬失败 | checked memory > maintenance_work_mem | 增大 maintenance_work_mem 或降低 lists |
| k-means 计算主导 | k-means wall/CPU 高、IO wait 低 | 评估降低 lists 或优化训练成本 |
| assign 计算主导 | assign wall/CPU 高、worker 使用证据 | 评估 maintenance workers / lists |
| worker 配额不足 | planned > launched + GUC/active worker | 调整 worker 配额 |
| sort spill | temp bytes/files + 相关阶段耗时/IO | 在安全范围调整工作内存 |
| probes 可提升 Recall | used_ivfflat + returned rows 正常 + Recall 曲线 | 增大 probes 到满足 SLO 的最小值 |
| probes 收益递减 | Recall 增量小、Latency 增量大 | 选择拐点或降低 probes |
| 结构倾斜影响查询 | list 分布异常 + 同 Build 质量/延迟异常 | 调整 lists、重建或进一步检查数据分布 |
| IO 主导 | IO time/吞吐 + IO wait，且排除 Lock/IPC | 检查存储/缓存/IO 并发 |

---

# 7. 最简理解模型

整个指标体系可以压缩成四句话：

```text
Build Process：索引“怎么建、时间花在哪”。
Build Resource：索引“建的时候资源花在哪、能不能建”。
Index Structure：索引“最后建成什么样”。
Quality：索引“查起来准不准、快不快”。
```

最后由 Joint Diagnosis 把四类事实合在一起回答：

> **为什么这样、参数该怎么调、调完是否真的更好。**
