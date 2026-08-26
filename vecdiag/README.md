# vecdiag · 向量索引构建期内存预测与诊断

2026 腾讯犀牛鸟开源课题实战训练营 · OpenTenBase 方向二（向量索引构建优化与诊断）。

基线：`REL_18_STABLE`（PostgreSQL **18.6**，commit `4c66f172a09296b08d53526f802ddd2b461bd7e8`）+ pgvector **v0.8.6**（commit `8ee86c96f0fd72390f890aa8a336fda6d3ab4c6c`），集中式，CentOS 7。

## 这个模块解决什么

建 IVFFlat 索引时 pgvector 可能直接报错：

```
ERROR:  memory required is 22 MB, maintenance_work_mem is 1 MB
```

问题在于用户**事后**才知道，而且这个数字既不是表大小也不是索引大小，没法推算该把 `maintenance_work_mem` 调到多少。本模块在建索引**之前**给出：需要多少内存、会在哪个检查点失败、分项占用各是多少、每一项对应哪行源码。

## 快速开始

```bash
# 安装（幂等）
PGHOME=/data/pg18/install PGPORT=5518 bash tools/install.sh

# 回归测试：所有 ok 列必须为 t
$PGHOME/bin/psql -p 5518 -d postgres -X -f tests/test_m1_model.sql

# 20 组验证矩阵：真实建索引 + 逐字比对报错原文
bash tools/validate_memory_model.sh tests/matrix_m1.tsv my-run-id
```

对一张真实表做预测：

```sql
analyze my_table;                      -- relpages 要准
select * from vecdiag.ivfflat_predict_table('my_table'::regclass, 1000);
```

```
 first_hit | predicted_mb | mwm_kb | c1_bytes | c2_bytes | c3_bytes | num_samples | sampled
-----------+--------------+--------+----------+----------+----------+-------------+---------
 C2        |           22 |   1024 |   520024 | 22158808 | 35218832 |       41613 |    2000
```

读法：当前 `maintenance_work_mem` 下，构建会在 **C2** 检查点失败，报错文本里的数字是 **22 MB**。
想让它建成功，`maintenance_work_mem` 要超过 `c3_bytes`（34393 kB）。

## 核心结论：检查点有三个，不是一个

`IvfflatCheckMemoryUsage()` 在一次构建里被调用三次，`memoryUsed` 累积递增，**第一个越界的检查点抛错，报错数字就是它自己的累积值**：

| 检查点 | 源码位置 | 累积到什么 |
|:--:|---|---|
| C1 | `ivfbuild.c:394` | centers |
| C2 | `ivfbuild.c:459` | + samples |
| C3 | `ivfkmeans.c:290` | + k-means 9 项 |

只算完整总量（C3）的模型，在低 `maintenance_work_mem` 或大 `lists` 场景下会系统性对不上报错原文——实测同一张表只改内存参数，报错值会在 **1 / 22 / 34 MB** 之间切换。所以 `ivfflat_predict` 必须返回 `first_hit`。

## 已完成的两个模块

**M1 · IVFFlat 构建内存预测**（`sql/00_schema.sql`、`sql/10_ivfflat_memory_model.sql`）
20 组验证矩阵 **20/20 逐字命中、误差 0**（红线 <5%），覆盖 C1 三组、C2 四组、C3 十一组、
以及两组"预测不报错且真的建成功"。新旧模型对照见 `docs/figs/M1-model-compare.svg`：
旧模型（0.8.0 口径）最大偏 **306 倍**。推导与三个被验证矩阵抓出的模型缺陷记在
`docs/M1-ivfflat-memory-model.md`。

**M2 · HNSW 落盘降级预警**（`sql/30_hnsw_model.sql`）
把降级 NOTICE 当观测量反解每元素图内存，**不需要重编 `-DHNSW_MEMORY` 的 `.so`**。
11 组实测降级点 **全部落在预测区间内**，点预测平均绝对误差 **1.20%**；
外样本（dims=256、m=24 均未参与标定）误差 ≤0.18%。
按模型给的内存下限重建后 NOTICE 消失，且降到建议值一半时 NOTICE 重新出现。
详见 `docs/M2-hnsw-spill-model.md`。

**上游测试基线**（`results/t04-20260826/`）：`make installcheck` 14/14 通过；
`make prove_installcheck` 48 个文件、1250 个测例全部通过。

**上游能力清单**（`results/t05-20260826/`）：pgvector 0.8.6 共注册 7 个 GUC，
**全部是查询/扫描侧，构建侧一个都没有**——这是本项目的立项依据，也是边界声明的一手证据。

## 边界声明（不要越界宣传）

- 大 `lists` 组用的是 `real-threshold` 模式（压低 `maintenance_work_mem` 触发同源检查点）。**只证明后端检查点与预测一致，不证明巨型索引构建成功。**
- 只覆盖 `vector` 类型。`halfvec` / `bit` / `sparsevec` 的 itemsize 不同，未验证前不得套用。
- 模型预测的是 pgvector 的**检查点值**，不是进程 RSS，两者不可混同。
- 并行构建路径（`ivfbuild.c` 的 parallel 分支）未纳入本模型。
- ABI 常数与机器、编译器、`BLCKSZ` 绑定。**换机器必须重跑 `tools/abi_probe.sh`**，不得沿用他机数值。
- `pg_stat_progress_create_index` 与 pgvector 的子阶段上报是**上游已有能力**，本模块没有重新实现它们。
