# M1 · IVFFlat 构建内存模型

对齐 pgvector **v0.8.6**。所有行号指向该 tag。

## 1. 模型

设 `L` = lists、`R` = 表实际行数、`P` = `relpages`、`D` = 维度、`I` = itemsize、
`A` = `MAXALIGN(I)`、`H` = `sizeof(VectorArrayData)`、`T` = `MaxHeapTuplesPerPage`。

```text
maxTuples   = P * T                                      ← ivfbuild.c:446，不是 R
numSamples  = 1                                          ← heap == NULL（ambuildempty 路径）
            = max( min( max(L*50, 10000), P*T ), 1 )     ← 普通构建
sampled     = samples->length = min(numSamples, R)        ← 实际采到的条数

C1 = H + L*A                                             ← ivfbuild.c:394（centers）
C2 = C1 + H + numSamples*A                               ← ivfbuild.c:459（+samples）
C3 = C2 + kmeans9                                        ← ivfkmeans.c:277-290
     kmeans9 = (H + L*A)        newCenters      266
             + 4*L*D           agg             267
             + 4*L             centerCounts    268
             + 4*sampled       closestCenters  269
             + 4*sampled*L     lowerBound      270  ← 主导项
             + 4*sampled       upperBound      271
             + 4*L             s               272
             + 4*L*L           halfcdist       273
             + 4*L             newcdist        274

first_hit = 第一个满足 floor(C/1024) > mwm_kb 的检查点        ← ivfutils.c:124
报错 MB   = floor(C_firsthit / 1048576) + 1                  ← ivfutils.c:128
```

`sampled == 0` 时 `IvfflatKmeans` 走 `RandomCenters` 而不是 `ElkanKmeans`
（`ivfkmeans.c:561-565`），**C3 这个检查点不存在**。空表就是这种情况。

## 2. ABI 常数

| 常数 | 本机值 | 来源 |
|---|---|---|
| `MAXALIGN(itemsize)`，dims=128 | 520 | **实测**，C1 隔离法四组差分，残差为零 |
| `sizeof(VectorArrayData)` | 24 | 源码 `ivfflat.h:120-126`；实测只能给上界 <252 |
| `MaxHeapTuplesPerPage` | 291 | `htup_details.h:629-631`，8 kB 块 |
| `MAXIMUM_ALIGNOF` | 8 | x86-64 |

实测方法（`tools/abi_probe.sh`）：C1 检查点只含 centers，表达式干净。用**空表**让
`numSamples` 被压到 1，使 C1/C2 与 C3 差两个数量级；再对 `maintenance_work_mem`
做二分，把 `floor(totalSize/1024)` 定位到 1 kB——比直接读报错的 MB 精一千倍。
四组 `lists`（4096/8192/16384/32768）两两差分即得 `A`：

| lists | floor(C/1024) kB |
|---:|---:|
| 4096 | 2080 |
| 8192 | 4160 |
| 16384 | 8320 |
| 32768 | 16640 |

`(K₂-K₁)*1024/(L₂-L₁)` 三组全部给出 **520.000**。

`H` 在 kB 粒度下测不出来（只能约束到 <252 字节），取源码值 24。它占总量约
0.0001%，不影响任何一位有效数字。**报告里要写"A 实测、H 源码读出并有实测上界"，
不要含糊成两个都实测了。**

## 3. 验证矩阵抓出来的三个真实缺陷

第一轮 20 组跑出 11 PASS / 9 FAIL。九个失败聚成三类，全部是模型缺陷而不是环境问题——
这是"先写 harness 再调模型"的直接收益，也是报告里值得写的过程证据。

**缺陷一：`sum(bigint)` 返回 numeric，把整数除法变成了精确小数。**
命中 S2 / S5 / L3 / L4 四组。C 侧的判定是 `totalSize / 1024 > maintenance_work_mem`，
`Size` 整数除法**截断**；而 SQL 里 `sum(bytes)` 的结果是 `numeric`，
`520024 / 1024` 算出 `507.8359375`，与 507 比较时变成了"向上取整"，
判定边界整体偏 1 kB。表现为：`mwm=507` 时模型说命中 C1 报 1 MB，实际报 22 MB（C2）。
修法是把聚合结果显式转回 `bigint`。**这类偏差只在边界处暴露，不做边界用例根本发现不了。**

**缺陷二：把 `heap == NULL` 当成了"unlogged 表"。**
命中 S6。`ivfbuild.c:442` 的注释写的是 "Skip samples for unlogged table"，
但该分支实际对应 **`ambuildempty`（初始化 fork）**，不是"对 unlogged 表执行 CREATE INDEX"。
实测：unlogged 表 500 行 / 256 维 / `lists=200`，报错 **11 MB**，正是
`numSamples=10000` 的 C2 值；若按 `numSamples=1` 算只有 2 MB。
所以参数名改成 `p_empty_build`，与表的持久化属性解耦。**跟着注释走会错，要跟着调用链走。**

**缺陷三：`reltuples` 是估计值，误差会被主导项放大。**
命中 F3 / F5 / X1，每组差 1 MB。`lowerBound = 4*sampled*L` 是主导项，而 `sampled`
依赖行数；`reltuples` 来自 ANALYZE 抽样，15000 行的表可能记成 15120，
放大后就是几十万字节，恰好跨过一个 MB 边界。
修法：`ivfflat_predict_table` 增加 `p_rows_exact`，验证矩阵传真实行数，
让"统计误差"不混进"模型误差"。生产场景拿不到真值时，这一项误差要写进结论边界。

修完三处后重跑：**20/20 PASS，误差 0**。

## 4. 覆盖情况

| 命中检查点 | 组数 | 用例 |
|:--:|:--:|---|
| C1 | 3 | L1 L2 L5（lists 2048 / 4096 / 32768） |
| C2 | 4 | S2 S5 L3 L4 |
| C3 | 11 | S3 S4 S6 F1–F6 X1 X2 |
| none（真的建成功） | 2 | S1（空表走 RandomCenters）、X3 |

原始 stderr 与 SHA256 在 `results/m1-r2-20260826/`。

第三轮（`results/m1-r3-20260826/`）在结果文件里补记了真实 `relpages`，同样 20/20，
并据此产出新旧对照 CSV 与图。

## 5. 新旧模型对照（T1.6 / T1.7）

旧模型 = pgvector **0.8.0** 的原始公式，实现在 `sql/20_legacy_model.sql`，逐项对照
in-tree 0.8.0 的 `src/ivfkmeans.c:277-299`。三处结构差异：

1. `numSamples = max(lists*50, 10000)`，**没有 `relpages*MaxHeapTuplesPerPage` 上限**。
   0.8.0 的 `ivfbuild.c:414` 就写着 `TODO Ensure within maintenance_work_mem`——
   C1/C2 两个检查点正是后来对这个 TODO 的实现。
2. 只有一个总量，**无法表达"报错数字取决于哪个检查点先越界"**。
3. 阈值按字节比较（`totalSize > mwm * 1024L`），0.8.6 改成 `floor(bytes/1024) > mwm_kb`，
   两者最多差 1023 字节。

对照结果（`results/m1-r3-20260826/model_compare.csv`，图 `docs/figs/M1-model-compare.svg`）：

| 场景 | 旧模型 / 实测 | 根因 |
|---|---:|---|
| 饱和组 F1–F6、S6 | 1.00 | 上限不生效且 C3 先触发，两者本就应当相等 |
| 小表 S2 / S3 / X1 / X2 | 3.2 – 11.7 | `numSamples` 没有 `maxTuples` 上限，samples 项被高估 |
| 大 lists L1–L5 | 21 – **306** | 旧模型只有一个总量，而实测命中的是 C1/C2 |
| 空表 S1 | — | 旧模型预测 6 MB 会失败，**实际构建成功**（走 RandomCenters）：误报 |

最大偏差出现在 `lists=32768`：旧模型 5208 MB，实测 17 MB，**306 倍**。

**一处需要更正原计划的结论。** 原计划要求"至少 2 组高估 + 1 组低估"。实测下来
**旧模型不存在低估**，这不是没找到，而是结构上不可能：

```
legacy   = (H + numSamples₀₈₀·A) + (H + L·A) + kmeans9(sampled)
新模型 C3 = (H + L·A) + (H + numSamples₀₈₆·A) + kmeans9(sampled)
且 numSamples₀₈₀ = max(50L,10000) ≥ numSamples₀₈₆ = min(max(50L,10000), P·T)
```

两式的 `sampled` 相同（都是 `min(numSamples, 实际行数)`，而 `P·T ≥ 行数`），
所以 `legacy ≥ C3 ≥ C2 ≥ C1`，而实测报错值一定是这三者之一。
因此旧模型**只会高估，不会低估**，也不会出现"预测 ok 但实际失败"的危险方向。
它的危害是另一种：把能建成的索引报成建不成（S1 就是），以及给出高到没有参考价值的
内存建议（306 倍）。**报告按这个写，不要为了凑"低估"去造一个不存在的用例。**

## 6. 局限与威胁有效性（T1.8）

**只覆盖 `vector` 类型。** `halfvec`（2 字节/维）、`bit`、`sparsevec` 的 itemsize 规则不同，
`vecdiag.vector_itemsize()` 会算错。未实测前不得宣称支持。

**只在 x86-64 + GCC 11.2.1 + 8 kB 块上验证。** `MAXALIGN` 宽度、
`sizeof(VectorArrayData)`、`MaxHeapTuplesPerPage` 都与这三者绑定。
换机器必须重跑 `tools/abi_probe.sh`，两台机器的结果分开归档。

**`estimate_relpages()` 在高维下不可用。** `vector` 是变长类型，维度大到超过
TOAST 阈值（约 2000 字节，`dims ≳ 498`）时值会被压缩或外置，堆内元组变小、
`relpages` 远小于按 itemsize 的估算。实测 100 行 × 960 维那组：用估算页数会把预测
从 17 MB 抬到 58 MB。`vecdiag.toast_risk(dims)` 会对这种维度返回 true；
**高维场景必须先 `ANALYZE` 用真实 `relpages`**。这是估算函数的局限，不是模型的局限。

**`reltuples` 的统计误差会被放大。** 主导项 `lowerBound = 4·sampled·L` 直接乘行数，
ANALYZE 的抽样误差经放大后可跨越 MB 边界。生产场景拿不到真实行数时，
预测应按"±1 MB"给区间而不是给单值。

**并行构建路径未纳入。** `ivfbuild.c` 的 parallel 分支有独立的共享内存计算，本模型只覆盖串行路径。

**预测的是 pgvector 的检查点值，不是进程 RSS。** 两者不可混同：检查点是事前计算的
理论需求量，进程实际驻留内存还受 palloc 上下文、swap、page cache 影响。

**`real-threshold` 模式的证明边界。** 大 `lists` 组是压低 `maintenance_work_mem`
触发同源检查点得到的，只证明后端检查点与预测一致，**不证明巨型索引真的能建成**。
本机 3788 MB 内存做不了 `lists ≥ 2048` 的真实成功构建（`lists=4096` 时
`lowerBound` 一项就要 3.35 GB）。



