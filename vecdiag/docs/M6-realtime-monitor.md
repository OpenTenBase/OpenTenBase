# M6 · 实时构建监控（"正在建索引时能看的东西"）

M3 的离线曲线适合出报告：先采样、再落库、再算。但真正在等索引的人需要的是**现在**就能看到
"到哪了、还要多久、会不会出事"。这个模块把 M1/M2/M3 合到一个零参数函数里。

```sql
select pid, phase, pct, elapsed_s, eta_s, eta_basis, risk from vecdiag.build_monitor();
```

命令行看板（另开一个会话）：

```bash
bash tools/watch_build.sh                          # 每 500 ms 刷一次
INTERVAL_MS=200 LOG=/tmp/live.csv bash tools/watch_build.sh
MWM_KB=81920 bash tools/watch_build.sh             # 构建方的内存设置与本会话不同时必须传
```

## 上游给什么，这里给什么

`pg_stat_progress_create_index` 给的是"现在在哪个阶段"，加上阶段内的
`blocks_done/blocks_total`、`tuples_done`。它不给：跨阶段的百分比、剩余时间、
"按当前内存这次会不会降级落盘"。本模块补的就是这三样，全部在 SQL 层，不改内核、不重编译。

## 三条实测撞出来的硬限制（都写进了实现里）

**1. 构建期间拿不到新索引的 `pg_class` 行。**
非 `CONCURRENTLY` 的 `CREATE INDEX`，新索引的 `pg_class` 行属于**尚未提交的事务**，
别的会话看不见；实测 `index_relid` 在进度视图里干脆是 `0`。
所以不能靠 `join pg_class` 拿索引名和访问方法——第一版就是这么写的，结果函数永远返回 0 行，
而空闲时又看不出问题（循环体不执行）。
现在：索引名 `left join`，查不到就显示 `(building oid=0)`；访问方法改用**推断**。

**2. 访问方法只能从阶段名和计数列推断。** 依据是实测计数：

| 判据 | 结论 | 实测依据 |
|---|---|---|
| phase = `performing k-means` 或 `assigning tuples` | ivfflat | 只有 IVFFlat 有这两个阶段（`ivfflat.h:62-63`） |
| phase = `loading tuples` 且 `tuples_total > 0` | ivfflat | 254/254 个采样点都有 |
| phase = `loading tuples` 且 `tuples_total = 0` | hnsw | 2458 个采样点全为 0 |
| phase = `initializing` | 判不出来 | 返回 `null`，**不给百分比，不猜** |

输出里的 `am_source` 列说明这一行的访问方法是查表得到的还是推断出来的。

**3. 读不到别的后端的 `maintenance_work_mem`。**
降级预警要用**正在构建的那个后端**的值去算，但 PostgreSQL 不提供跨后端读 GUC 的能力
（`pg_stat_activity` 里没有）。默认用监控会话自己的值，两边设置不同时会误报——
实测踩过：构建方 80 MB、监控方 256 MB，预警显示"正常"而实际降级了。
所以监控别人的构建时，把对方的值显式传进来：`vecdiag.build_monitor(81920)`。

## 阶段起点从哪来

进度视图不提供阶段开始时刻，而算"这个阶段跑了多久"必须有它。做法：一张 unlogged 状态表
`vecdiag.monitor_state`，在每次调用监控函数时顺手维护，阶段名变了就推进 `phase_started_at`。
不需要后台进程，代价是**必须有人周期性调用**（`watch_build.sh` 就是干这个的）。

构建总起点优先取 `pg_stat_activity.query_start`，也就是 `CREATE INDEX` 真正开始的时刻；
取不到时才退回"第一次被观测到的时刻"，此时 `elapsed_source` 列会标成
`first-observation（已用时间会偏小）`。

## 单阶段访问方法不需要跨阶段权重

HNSW 只定义了一个真正干活的阶段（`hnsw.h:76` 只有 `LOAD`），所以它**不存在跨阶段加权问题**，
百分比直接等于阶段内视图计数。早先一律要求"有可用实测权重"，导致 HNSW 永远拿不到百分比——
那是把 IVFFlat 的多阶段约束错套到单阶段上。现在 `weight_basis` 会写明
`单阶段访问方法：百分比即阶段内视图计数，不需要跨阶段权重`。

## 实时 ETA 按降级修正

HNSW 的百分比按元组线性推进，而降级后每元组耗时是降级前的 4.031 倍
（标定见 `vecdiag.hnsw_spill_penalty`）。所以实时 ETA 用的是与离线
`vecdiag.hnsw_eta_corrected()` 相同的修正式，`eta_basis` 列写明用了哪个口径：

- `已按降级修正（减速倍数 4.031，预测第 68275 行降级）`
- `朴素线性外推（未修正降级；HNSW 降级后会明显偏低）`

## 实测验证（run `live-20260827`）

120 000 行 × 128 维、`m=16`、`maintenance_work_mem=80MB`（会降级），500 ms 轮询：

| 指标 | 值 |
|---|---|
| 采样点 | 198（另 1 行表头） |
| 百分比非单调点 | **0**，终值 100.00 |
| 构建总时长 | 105.1 s |
| 实时 ETA 平均绝对偏差 | **5.36%** |
| 降级预警 | 正确给出 `⚠ 预测会降级落盘`（预测第 68275 行，实测 NOTICE 第 66727 行，偏差 2.3%） |
| 未修正时的对照 | 同一构建、朴素外推在 t=0.6 s 报剩余 20.9 s，实际还剩 104.5 s |

原始序列：`results/live-20260827/live.csv`（含 `am_source` / `elapsed_source` / `eta_basis` 三列，
每一行都能看出这个数字是观测来的还是推算来的）。
