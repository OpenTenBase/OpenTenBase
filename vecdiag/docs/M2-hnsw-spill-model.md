# M2 · HNSW 图内存与落盘降级预警

对齐 pgvector **v0.8.6**。所有行号指向该 tag。

## 1. 上游现状与本模块的边界

| 上游有什么 | 源码位置 | 缺什么 |
|---|---|---|
| 图放不下时打 **NOTICE** 并转磁盘继续建 | `hnswbuild.c:530-549` | 是**事后**通知，且不是 ERROR——构建不会失败，只会变慢，用户只知道"建索引特别慢" |
| `memoryTotal = maintenance_work_mem * 1024` | `hnswbuild.c:724` | — |
| 内存数字 `elog(INFO)` | `hnswbuild.c:307` | 只在 `#ifdef HNSW_MEMORY` 下编译，生产环境拿不到 |
| 构建侧 GUC | — | **一个都没有**（实测 7 个 GUC 全是查询/扫描侧，见 `tests/upstream_inventory.sql`） |

所以本模块填的空白是**事前预测**：建索引之前告诉用户会不会降级、大约在第几行降级、
内存下限该给多少。**表述必须限定在"事前预测"**——上游有 `memoryTotal` 和 `FlushPages`，
不能说"上游没有内存管理"。

## 2. 标定原理：把 NOTICE 当观测量

降级判定是 `memoryUsed + margin >= memoryTotal`，串行构建 `margin = 0`（`hnswbuild.c:507`）。
所以降级那一刻 `memoryUsed ≈ maintenance_work_mem`，于是

```
per_element ≈ maintenance_work_mem_bytes / N      （N = NOTICE 里的 tuples 数）
```

**这样就不需要重编一份 `-DHNSW_MEMORY` 的 `.so`**，省掉了"标定构建与交付构建哈希不同"
的整套麻烦。

这个式子有一个可检验推论：`per_element` 与 `maintenance_work_mem` 无关。实测：

| dims | m | mwm | 降级行数 | per_element |
|---:|---:|---:|---:|---:|
| 128 | 16 | 4 MB | 3420 | 1226.4 B |
| 128 | 16 | 8 MB | 6838 | 1226.8 B |
| 384 | 16 | 8 MB | 3725 | 2252.0 B |
| 384 | 16 | 16 MB | 7454 | 2250.8 B |

同配置换内存档位，`per_element` 相对离散 **0.03% / 0.05%**——推论成立，因此可以用一次
标定去预测其它内存档位。完整 8 组样本在 `results/centos7-20260826/m2-20260826/hnsw_spill.csv`。

## 3. per_element 解析式

```
per_element(dims, m) = 206.4 + 31.89 · m + 4 · dims        字节
```

三项性质不同，**报告里必须分开说**：

- `4 · dims` —— **结构上可识别**。向量本体每维一个 `float`。实测 dims 128→384 每维增量
  4.005 字节，与 `sizeof(float)` 相符。
- `31.89 · m` —— **拟合项**。机制清楚：`HnswInitNeighbors` 为 0..level 每层分配一个邻居
  数组，第 0 层 2m 槽、其余每层 m 槽，level 是几何分布（`hnswutils.c:218-227`）。
  但每槽字节数与期望层数没有做结构推导，系数由 m ∈ {8,16,32} 三点线性拟合得到。
- `206.4` —— 拟合项。`HnswElementData` 本体与指针数组的常数部分（`hnswutils.c:245-267`）。

**`ef_construction` 不进入该式**：实测 ef 64 与 200 的降级点完全相同（都是第 6838 行），
它只影响构建耗时，不影响图内存。

## 4. 为什么给区间而不是给点

串行构建走 `HnswMemoryContextAlloc`（`hnswbuild.c:646-654`）：

```c
memoryUsed = MemoryContextMemAllocated(buildstate->graphCtx, false);
```

**这是"内存上下文已分配块总量"，不是请求量之和。** AllocSet 的块从 8 kB 起倍增、
8 MB 封顶，所以 `memoryUsed` 呈阶梯上升。后果很直接：

```
mwm 4096 kB → 第 3420 行降级
mwm 4608 kB → 第 3422 行降级     ← 内存多给 12.5%，降级点只挪了 2 行
mwm 8192 kB → 第 6838 行降级
```

4096 与 4608 落在同一个块级台阶里。这就是 E2 那组点预测偏 12.22% 的**全部原因**，
不是系数不准。`vecdiag.hnsw_spill_range()` 因此给区间：下端按块台阶
（`allocset_capacity_floor()`），上端按朴素线性外推，两端各放 0.5% 覆盖 `per_element`
的拟合离散。

## 5. 验证结果

`tests/test_m2_spill.sql`，数据在 `results/centos7-20260826/m2-20260826/` 与 `results/centos7-20260826/m2v-20260826/`：

| 指标 | 结果 |
|---|---|
| 实测降级点落在预测区间内 | **11 / 11** |
| 点预测平均绝对误差 | **1.20%** |
| 点预测最大误差 | 12.22%（E2，块台阶，区间已正确覆盖） |
| 11 组中误差 ≤ 0.20% 的 | 10 组 |

**外样本预测**（dims=256 与 m=24 都没参与标定）：V1 −0.11%、V2 −0.04%、V3 −0.18%。

**建议有效性（E 组）**：模型给 6000 行 × 128 维 × m=16 的内存下限建议是 **9 MB**。
按 9 MB 重建，**NOTICE 消失**（E1）。为了让这条证据成立，还做了反向验证：把内存降到
建议值的一半（4.5 MB），**NOTICE 重新出现**（E2）。只有 E1 没有 E2，"建议管用"这句话
是空的——因为无法排除"内存本来就够"。

## 6. 局限

- 标定覆盖 dims ∈ [128, 384]、m ∈ [8, 32]。越界时 `confidence` 返回 `extrapolated`，
  结论只能当量级参考。
- 只覆盖**串行**构建。并行构建走 `HnswSharedMemoryAlloc`，`margin = 1 MB`
  且按请求量精确累加（`hnswbuild.c:658-677`），阶梯效应不同，未验证。
- 只覆盖 `vector` 类型与 L2 距离。
- `per_element` 里的 `slot_coef` 与 `base_bytes` 是拟合值。想把它们变成结构推导值，
  需要 `sizeof(HnswElementData)`、`sizeof(HnswNeighborArray)`、每槽字节数与期望层数
  的完整推导——这是下一步该做的事，做完可以把区间进一步收窄。
- 降级**之后**的耗时代价（慢多少倍）没有量化，本模块只预测"会不会降级、在第几行"。
  耗时结论还受本机 2 GB swap 影响，不可外推。
