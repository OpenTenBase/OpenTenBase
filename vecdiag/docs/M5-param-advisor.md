# M5 · 构建参数建议表（T2.7）

回答 M1/M2 不回答的那个问题：**"那我到底该用什么参数"**。

M1 说"会不会失败"，M2 说"会不会降级"，这两个都是**可行性**问题。可行之后还剩一个选择问题：
`m` 取多少、`ef_construction` 取多少、`lists` 取多少。上游 README 只给方向
（"调大 ef_construction 召回更好，代价是构建更慢"），不给量。本模块把方向变成本机的量，
并且明确标注每个数字的来源类型。

## 三类来源，不许混

| source_kind | 含义 | 例子 |
|---|---|---|
| `source-code` | pgvector 源码里的常量或结构事实 | `m` 默认 16、范围 [2,100]（`hnsw.h:54-56`） |
| `upstream-doc` | pgvector README 的口径，只有方向没有量 | "调大 `ef_construction` 召回更好"（README.md:268） |
| `measured` | 本机实测，附 run_id 与产物路径 | `m=16/ef=64` 构建中位 18192 ms（run `t27-20260827`） |

查询入口：

```sql
select source_kind, fact, source_ref from vecdiag.param_advice_provenance order by source_kind;
```

## 实测条件（改任何一条，下面的数字都要重测）

| 项 | 值 |
|---|---|
| 底库 | ANN_SIFT1M 前 100000 条，128 维（`sift_base` 子集） |
| ground truth | **库内顺序扫描重算的 exact top-10** |
| 查询集 | 公开 `sift_query.fvecs` 前 100 条 |
| 查询侧参数 | HNSW `hnsw.ef_search=40`（上游默认）；IVFFlat `ivfflat.probes=10` |
| `maintenance_work_mem` | 512 MB（固定住，保证各配置耗时可比） |
| 重复 | 每配置 3 次，构建耗时报 min/median/max，召回取均值 |
| run_id | `t27-20260827`，产物 `results/t27-20260827/` |

**关于 ground truth 的口径必须说清**：ANN_SIFT1M 自带的 `sift_groundtruth.ivecs` 是针对
**全量 100 万底库**算的，用在 10 万行子集上不成立，所以不能直接引用。本模块在库内用
顺序扫描（`enable_indexscan=off`）重算 exact top-10——这是子集场景下唯一正确的做法。
`results/t27-20260827/explain_hnsw.txt` 留了 EXPLAIN 证明召回查询确实走了索引，
否则"召回 1.0"可能只是顺序扫描的假象。

**召回属于方向一的指标口径。** 这里只把它当作构建参数取舍的质量轴，不作为方向一的交付成果。

## 实测结果

HNSW（100000 行 × 128 维）：

| m | ef_construction | 构建中位 (ms) | 索引 (MB) | recall@10 | 单查询 (ms) | 帕累托 |
|---|---|---|---|---|---|---|
| 8 | 64 | 8 326 | 71.0 | 0.9427 | 0.55 | 前沿 |
| 8 | 200 | 23 679 | 71.0 | 0.9583 | 0.56 | **被支配** |
| 16 | 64 | 18 192 | 79.4 | 0.9790 | 0.80 | 前沿 |
| 16 | 200 | 40 573 | 79.4 | 0.9893 | 0.88 | **被支配** |
| 32 | 64 | 36 751 | 98.1 | 0.9917 | 1.28 | 前沿 |
| 32 | 200 | 70 722 | 98.1 | 0.9943 | 1.31 | 前沿 |

IVFFlat（同一底库，`probes=10`）：

| lists | 构建中位 (ms) | 索引 (MB) | recall@10 | 单查询 (ms) |
|---|---|---|---|---|
| 100（上游 `rows/1000`） | 1 147 | 52.5 | 0.9860 | 6.90 |
| 316（`sqrt(rows)`） | 2 226 | 53.4 | 0.9373 | 1.87 |
| 1000 | 14 311 | 56.3 | 0.8583 | 0.76 |

## 三条能直接用的结论

**1. `ef_construction` 不改变索引体积，`m` 会。** `ef_construction` 只影响建图时候选集的大小，
不进入每元素的邻居槽数（`hnswutils.c:218-227`）。实测 64→200 索引体积一字不变
（71.0 / 79.4 / 98.1 MB 三档都是），而 `m` 从 8 加到 32 体积涨 38%。
所以**磁盘或内存受限时，`ef_construction` 是唯一不增加体积的召回杠杆**。

**2. 但在时间预算下，调大 `ef_construction` 不划算——六个配置里有两个被支配。**
`m=8/ef=200`（23 679 ms, 0.9583）被 `m=16/ef=64`（18 192 ms, 0.9790）支配：更快**且**召回更高。
`m=16/ef=200`（40 573 ms, 0.9893）被 `m=32/ef=64`（36 751 ms, 0.9917）支配。
上游 README 只说"调大 `ef_construction` 召回更好"，这句话本身没错，
但本机标定说明**同样的时间花在调大 `m` 上收益更高**。

范围声明：这条结论限定在 100k×128 的 SIFT 子集、`ef_search=40`、这台机器上。
换数据规模或维度必须重跑 `tools/param_sweep.sh` 重新判定前沿，不要把它当普适规律。

**3. `lists` 调大必须同步调大 `probes`，否则只是把召回换成了速度。**
实测 `lists` 100→1000（10 倍）而 `probes` 固定 10：recall@10 从 0.9860 掉到 0.8583，
单查询从 6.90 ms 降到 0.76 ms，构建从 1 147 ms 涨到 14 311 ms。
这与上游 README:736 的 "increase the number of lists (at the expense of recall)" 一致——
上游给方向，这里给量。上游同时建议 `probes` 起点取 `sqrt(lists)`（README.md:342），
本模块的建议函数会把这个耦合关系直接写在输出里。

## 怎么用

按召回目标要参数：

```sql
select * from vecdiag.hnsw_param_advice(0.98, 100000, 128);
--  m=32 / ef_construction=64，前沿上满足 0.98 的最快配置
```

目标超出标定范围时返回 `applicable=false`，并说明该跑哪个脚本扩标定网格，
**不外插、不猜**：

```sql
select applicable, note from vecdiag.hnsw_param_advice(0.999, 100000, 128);
--  f | 已标定范围内达不到召回 0.999：前沿上最好的是 m=32/ef_construction=200，召回 0.9943 …
```

IVFFlat 的 `lists` 建议会串上 M1 的可行性检查——上游告诉你取多少，M1 告诉你建不建得起来：

```sql
select lists_suggested, feasible, first_hit, need_mwm_mb, probes_suggested
  from vecdiag.ivfflat_param_advice(1000000, 128, 65536);
--  1000 | f | C3 | 222 | 31     ← 上游经验式给 1000，但 64 MB 内存下 C3 会超限
```

## 重标定

```bash
bash tools/param_sweep.sh <run_id> <行数> <重复次数>      # 默认 100000 / 3
bash tools/load_param_facts.sh /data/artifacts/<run_id>/param_sweep.csv
```

`M_LIST` / `EF_LIST` / `LISTS_LIST` 可覆盖扫描网格。加载脚本只接受**逐次原始 CSV**，
中位数在库内用 `percentile_disc` 从原始数据算——取的是真跑过的某一次，不造一个没跑过的数。
