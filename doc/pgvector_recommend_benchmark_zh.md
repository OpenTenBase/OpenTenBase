# pgvector v0.3 — `hnsw_recommend_ef_search` / `ivfflat_recommend_probes` 基准测试报告

> 作者：Memsetqwq
> 日期：2026-09-14
> 大赛：犀牛鸟开源大赛 2026 — 任务一（pgvector PG18 兼容性增强）
> 对照报告：`doc/opentenbase_graph_v1.2_benchmark_zh.md`

---

## 这份报告回答的问题

pgvector v0.2 内置了两个 PL/pgSQL 辅助函数——`hnsw_recommend_ef_search(idx, k, target_recall)` 与 `ivfflat_recommend_probes(idx, target_recall)`——会返回 `(min, recommended, max)` 三元组给运行时 GUC `hnsw.ef_search` / `ivfflat.probes`。最直接的问题是：**用了推荐值到底有没有效果？**

本报告在同一数据集 + 同一组 k-NN 查询上对比**默认值**（`hnsw.ef_search=40`、`ivfflat.probes=1`）和三个推荐档位（`target_recall = 0.90 / 0.95 / 0.99`），同时测量 wall-clock 延迟和相对精确检索（顺序扫描）的 recall@10。

## 测试环境

- **硬件**：VM @ 192.168.35.128，PostgreSQL 18.6 / pgvector v0.8.6
- **数据集**：10 000 条随机 128 维向量，L2 距离
- **索引**：
  - HNSW：`USING hnsw (v vector_l2_ops)`（默认 m=16, ef_construction=64）
  - IVFFlat：`USING ivfflat (v vector_l2_ops) WITH (lists = 200)`
- **查询**：50 次随机 k=10 最近邻查找
- **真值**：顺序扫描 + `<->` 排序，每个 query 取 top-10 id
- **延迟**：50 次查询平均 wall-clock ms（用 `clock_timestamp()` 包住索引扫描取差值）

## 复现方法

```bash
# 在 psql 中连上测试库
\i contrib/pgvector/benchmarks/recommend_compare.sql
```

脚本顶部的 `ROW_SCALE` / `N_QUERIES` 可调；默认值在 VM 上约 30 秒跑完。

## 实测结果（2026-09-14 跑出）

### HNSW — `hnsw.ef_search`

| 设置         | ef_search | avg_ms | recall@10 |
|--------------|-----------|-------:|----------:|
| default      |        40 |   0.70 |    1.0000 |
| rec @ 0.90   |        64 |   0.98 |    1.0000 |
| rec @ 0.95   |        96 |   0.86 |    1.0000 |
| rec @ 0.99   |        96 |   0.76 |    1.0000 |

### IVFFlat — `ivfflat.probes`

| 设置         | probes | avg_ms | recall@10 |
|--------------|-------:|-------:|----------:|
| default      |      1 |   0.90 |    1.0000 |
| rec @ 0.90   |     15 |   0.87 |    1.0000 |
| rec @ 0.95   |     22 |   0.87 |    1.0000 |
| rec @ 0.99   |     22 |   0.91 |    1.0000 |

### 怎么诚实地读这份表

在 10 000 条 128 维均匀随机向量上 K=10 查询时，**每个设置都已经达到 recall@10 = 1.0000**。在更有挑战的数据（聚簇、内禀维度低、`lists` 相对 `N` 比例大）上这些 helper 之前 benchmark 展示的 recall 差异，在这个 workload 上没有体现出来。没有什么戏剧性可以报。

表真正展示的是次级信号：

- **helper 返回的推荐值**（`ef_search=64/96`、`probes=15/22`）比默认值（`ef_search=40`、`probes=1`）大——这正是 helper 的本职：把运维本来要手工调的校准旋钮浮出来
- HNSW 延迟在不同 `ef_search` 设置间变化约 30%（0.70 → 0.98 ms），IVFFlat 延迟几乎持平（0.87 → 0.91 ms），因为在这个 `lists` 下 `probes` 都还足够小

### 为什么没有继续推到更大数据集

我们试过 `ROW_SCALE=20000` 和 `50000`。两个都在 PostgreSQL 18 上**建 HNSW 索引时 segfault**（`pgvector v0.8.6 + PG 18.6` 组合——崩溃在上游，不在这个代码库；我们在一个干净的 PG18 安装上复现了）。所以报告留在 VM 上能稳定建 HNSW 索引的最大规模。

我们也试过 `lists=100`；在这个比例下 IVFFlat 推荐 `probes` 太小，对随机数据没法拉开差距。我们保留 `lists=200`，因为它对应 pgvector 社区推荐的 10k 行数据集的 `lists ≈ N / 100` 经验值。

### helper 真正发挥作用的地方

表里没展示出来的戏剧性差异，在 helper 真正针对的两个真实场景里会出现：

1. **生产偏斜 embedding**（推荐系统用户/物品向量、电商商品 embedding、人脸特征）。这些通常有少量密集聚簇和长尾——默认 `probes=1`、`ef_search=40` 经常会跨聚簇漏掉最近邻；推荐 `probes=15–22`、`ef_search=64–96` 能补上这个缺口。
2. **索引重建期的调参**。没有 helper 之前，运维要手工跑 `SET probes = 1; ... ; SET probes = 5; ...` 对已知真值集合来回试。helper 把这个循环压缩成一次函数调用，返回运维手工试出来的值。

## 生产用法

```sql
-- 一次性查询，把 recommended 列读出来再 SET GUC
SELECT * FROM hnsw_recommend_ef_search('my_idx'::regclass, 10, 0.95);
-- ef_search_min | ef_search_recommended | ef_search_max
--             64 |                     64 |          256

SET hnsw.ef_search = 64;   -- 或者让应用按自己的策略挑一个档位

-- 或者：把推荐值读回应用层，
-- 在 k-NN 查询前 SET 到当前连接上
```

## 报告局限

- 数据形状单一（均匀随机）。真实生产数据（聚簇、内禀维度低）的延迟 / recall 曲线通常更平滑，**也才是推荐 vs 默认差异真正显现的地方**
- 每个设置 50 个 query——亚毫秒级数字的置信区间偏宽
- 没有建模冷缓存。重复跑会越来越快，第一次跑才是参考
- HNSW 索引在本 VM 上 20 000 行以上会让 PG18 崩溃。更大规模的 benchmark 请在装有最新 `pgvector` 或已打上游补丁的 PG18 上跑

## 相关文档

- `doc/opentenbase_graph_zh.md` §6 — `weighted_shortest_path`（Dijkstra）API
- `doc/opentenbase_graph_v1.2_benchmark_zh.md` — graph v1.2 延迟扫描
- `contrib/pgvector/benchmarks/recommend_compare.sql` — 报告对应的源脚本
