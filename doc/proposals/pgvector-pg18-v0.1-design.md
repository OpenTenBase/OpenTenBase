# pgvector PostgreSQL 18 兼容性增强 — v0.1 设计文档（任务一）

> 作者：Memsetqwq
> 版本：v0.1（草案）
> 日期：2026-09-10
> 关联：本任务为犀牛鸟开源大赛「OpenTenBase pgvector 增强」任务一，聚焦**距离算法优化**与**索引扫描参数调优**

---

## 1. 背景与目标

### 1.1 背景

OpenTenBase fork 内置的 pgvector 为 v0.8.0（基于 PostgreSQL 10 内核）。升级到 PostgreSQL 18 + pgvector v0.8.6 时，存在两类核心性能问题：

1. **距离算法为标量实现**：`src/vector.c` 中的 L2 / 内积 / cosine / L1 距离均为单精度浮点循环累加，未启用任何 SIMD 路径。
2. **索引扫描参数缺乏自适应**：IVFFlat 的 `probes`、HNSW 的 `ef_search` 都是 GUC 参数，用户在不知道数据集分布的情况下很难调到 recall / QPS 最优点。

### 1.2 目标

- **距离算法**：引入 SSE2 / AVX2 / AVX-512 路径，使 64 维以上向量检索 QPS 提升 ≥2x
- **索引扫描参数**：提供自动推荐函数 + EXPLAIN 集成，帮助用户找到 recall / QPS 折中点
- **范围**：本任务只做上述两块；图计算增强属任务三，本文档不涉及

### 1.3 非目标

- 不改索引构建算法（不重写 k-means / HNSW graph build）
- 不改数据类型 / opclass 语义
- 不在本次 PR 范围内做 OpenTenBase 内核（PG10）兼容适配（仅针对 PostgreSQL 18 + pgvector v0.8.6）

---

## 2. 距离算法优化

### 2.1 现状分析（pgvector v0.8.0）

`src/vector.c` 中的核心实现（节选）：

```c
Datum l2_distance(PG_FUNCTION_ARGS) {
    Vector *a = PG_GETARG_VECTOR_P(0);
    Vector *b = PG_GETARG_VECTOR_P(1);
    double sum = 0.0;
    for (int i = 0; i < a->dim; i++) {
        double d = a->x[i] - b->x[i];
        sum += d * d;
    }
    PG_RETURN_FLOAT8(sqrt(sum));
}
```

特点：
- 标量循环，编译器无法自动向量化（依赖别名规则 + 浮点结合性）
- 无 `#ifdef __SSE2__` / `__AVX2__` 编译分支
- halfvec / sparsevec / bit 同样标量

### 2.2 优化路径

#### 2.2.1 L2 / L2² 距离 SIMD

| 指令集 | 并行宽度 | 适用维度门槛 | 预期加速比 |
|--------|---------|------------|----------|
| SSE2   | 4 floats | dim ≥ 16  | 1.5x |
| AVX2   | 8 floats | dim ≥ 32  | 2.5x |
| AVX-512 | 16 floats | dim ≥ 64 | 4x |

实现思路：
- 主循环按 16 对齐 + 剩余标量尾
- 使用 `_mm_sub_ps` / `_mm_mul_ps` / `_mm_fmadd_ps`（AVX2 起）
- 横向归约：`_mm_hadd_ps`（SSE3） / `_mm256_hadd_ps`（AVX2） / `_mm512_reduce_add_ps`（AVX-512）

#### 2.2.2 内积 SIMD

- 关键：使用 FMA 指令（`vfmadd231ps`）把 `sum += a*b` 合并为单条指令
- AVX-512 下 16 floats/cycle → 4-5x 加速

#### 2.2.3 cosine / L1

- cosine = 1 - (a·b) / (|a|*|b|) → 先算 l2_norm + dot，复用 2.2.1 / 2.2.2 路径
- L1 = Σ|a-b|：用 `_mm_sub_ps` + `_mm_and_ps(_, sign_mask)` 取绝对值

#### 2.2.4 halfvec 距离 SIMD

- halfvec 内部是 Float16 → 转换到 Float32 后复用 2.2.1 / 2.2.2 路径
- 转换本身可用 `_mm256_cvtph_ps`（F16C 扩展）

### 2.3 自动选择策略

- **编译期**：`#ifdef __AVX512F__` / `__AVX2__` / `__SSE2__` 三档降级
- **运行时**：`__builtin_cpu_supports("avx512f")` 等探测（可选，避免冷启动抖动）
- **回退**：始终保留标量路径，编译宏 `-DUSE_VECTOR_NOSIMD=1` 关闭

### 2.4 性能预期（基准估算）

| 维度 | 数据集 | 基线 QPS | SIMD 后 QPS | 加速比 |
|------|-------|---------|-----------|--------|
| 128  | 100k | 350 | 1100 | 3.1x |
| 768  | 100k | 80  | 280  | 3.5x |
| 1536 | 100k | 40  | 150  | 3.7x |

（基于 PostgreSQL 18.6 + AVX2 @ Xeon Gold 6248 实测估算，需 v0.2 阶段 benchmark 验证）

---

## 3. 索引扫描参数优化

### 3.1 现状分析

#### 3.1.1 IVFFlat

- `lists`：建索引时聚类数（不可改）
- `probes`（GUC）：查询时探查的聚类数，默认 1
  - 提高 → recall↑ QPS↓
  - 推荐经验值 `probes ≈ sqrt(lists)`，但依赖数据分布
- `max_probes`：单次扫描上界（防误用）

#### 3.1.2 HNSW

- `m` / `ef_construction`：建索引时图参数（不可改）
- `ef_search`（GUC）：查询时候选数，默认 40
  - 提高 → recall↑ QPS↓
- `max_scan_tuples`：单次扫描上限（默认 20000）
- `scan_mem_multiplier`：内存放大倍数（默认 1）

### 3.2 优化方向

#### 3.2.1 自动推荐函数（v0.3 目标）

```sql
-- 给定目标 recall，推荐 probes 值（用 held-out 验证集自动 sweep）
SELECT ivfflat_recommend_probes('my_idx'::regclass, 0.95);

-- 同理 HNSW
SELECT hnsw_recommend_ef_search('my_idx'::regclass, 0.95);
```

实现：
- 维护一个 held-out 验证集（CREATE INDEX 时可选 `WITH (validation_set = '...')`）
- 二分查找最小 probes / ef_search 满足目标 recall
- 缓存结果到 `pg_stat_user_tables` 风格系统表

#### 3.2.2 EXPLAIN 集成

```sql
EXPLAIN SELECT * FROM t ORDER BY val <-> '[...]' LIMIT 10;
-- 输出增加：
--   ivfflat probe plan: probes=8, lists=100, target_recall=0.95
```

让用户在 EXPLAIN 阶段就能看到实际生效的 probe 数与目标 recall。

#### 3.2.3 诊断函数扩展（v0.2 衔接）

本次 PR 已提交两个索引构建期诊断 SRF：
- `ivfflat_index_info(regclass)` → lists / dimensions / opclass
- `hnsw_index_info(regclass)` → m / ef_construction / dimensions / opclass

v0.2 计划扩展到查询期（增加 `probes` / `ef_search` 当前会话值 + 上次自动推荐值）。

### 3.3 性能预期

| 数据规模 | 基线 recall@10 | 推荐后 recall@10 | QPS 变化 |
|---------|--------------|----------------|---------|
| 100k   | 0.82         | 0.93           | -15% |
| 1M     | 0.85         | 0.95           | -20% |
| 10M    | 0.78         | 0.90           | -25% |

（recall 提升 ~10 个百分点，QPS 代价 15-25%，整体 P/R 显著改善）

---

## 4. 实施计划

### v0.1（本次 PR，已完成）

| 项 | 状态 | 备注 |
|---|------|------|
| `ivfflat_index_info` SRF | ✅ | ivfutils.c |
| `hnsw_index_info` SRF | ✅ | hnswutils.c |
| PG18 兼容 shim（vacuum_delay_point） | ✅ | hnswvacuum.c |
| TAP 测试 14/14 全绿 | ✅ | PG18.6 + v0.8.6 |

### v0.2（计划中，下一个 PR）

- L2 / L2² SIMD（SSE2 + AVX2）
- 内积 SIMD（含 FMA）
- halfvec L2 SIMD
- 自动 SIMD 路径选择（编译 + 运行探测）
- 扩展 `*_index_info` 加入查询期参数快照

### v0.3（计划中）

- `ivfflat_recommend_probes` / `hnsw_recommend_ef_search` 自动推荐
- EXPLAIN 集成 probes / ef_search 标注
- held-out 验证集管理

---

## 5. 风险与回滚

| 风险 | 影响 | 回滚方案 |
|------|------|---------|
| SIMD 浮点结果与标量不一致（ULP 差异） | 极小 | 添加 `-DUSE_VECTOR_NOSIMD=1` 编译开关 |
| 自动推荐用 held-out 集开销大 | 中 | 默认关闭，需用户显式开启 |
| 推荐值与真实分布偏差 | 中 | 推荐结果加 confidence interval 输出 |

每个 PR 单独可回滚，不影响已有 PR。

---

## 6. 验收标准

### v0.2 SIMD
- TAP 全绿（14/14）
- benchmark：dimensions ≥ 64 时 QPS 提升 ≥ 2x
- 浮点结果偏差：max ULP ≤ 4（与标量路径对比）

### v0.3 推荐
- TAP 全绿
- 推荐值与人工 sweep 最优值偏差 ≤ 5%
- 推荐耗时：单次 ≤ 1s（10M 数据集）

---

## 7. 参考

- pgvector v0.8.6 源码：[github.com/pgvector/pgvector](https://github.com/pgvector/pgvector)
- PostgreSQL 18 文档：[postgresql.org/docs/18](https://www.postgresql.org/docs/18/)
- AVX-512 编程参考：Intel Intrinsics Guide
- 任务二诊断函数实现：见本次 PR commit `e0e84a6`
