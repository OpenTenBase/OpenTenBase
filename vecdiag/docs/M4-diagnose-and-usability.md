# M4 · 零参数体检与结论可用性分层

## 1. 为什么要"可用性分层"

M3 标定出来的阶段权重有五组，但只有两组的极差达标。两种极端做法都不行：

- **全留在一张表里给人用** → 使用者不知道哪组能用，`recommend_stage_weights` 也可能
  返回一组不可信的权重；
- **把不达标的删掉** → "为什么必须按规模分档、为什么小规模不可信"这个结论就失去了支撑，
  审查者只能看到结论、看不到依据。

所以采用三层结构：**证据全留，默认只让人用可用的**。

| 对象 | 给谁用 | 内容 |
|---|---|---|
| `vecdiag.stage_weight` | 存档 | 全部五组，原样保留 |
| `vecdiag.stage_weight_audit` | **审查者** | 全部五组，每组附"可用/不可用 + 原因" |
| `vecdiag.stage_weight_usable` | **消费方（人和 AI）** | 只有达标且分过档的组 |

可用性判定的阈值是**极差 ≤ 0.25**。这个数不是拍的：实测可用档的极差是 0.08 与 0.16，
不可用档是 0.51 与 0.72，中间有明显空隙，0.25 落在空隙里。
`vecdiag.stage_weight_dispersion_limit()` 单独成函数，换机器后应重新确认这个空隙还在。

**判定必须按组、不能按行。** 实测 S 档里 `assigning` 的极差是 0.18（达标），
但 `loading` 0.51、`k-means` 0.33 都超限。按行过滤会留下一个缺了两个阶段、
求和不为 1 的残缺权重集——那比整组排除更危险。回归测试第 7 项就是守这条。

当前状态：

```
可用：sift1m / M（30 万行，4 个阶段，最大极差 0.0821）
      sift1m / L（100 万行，4 个阶段，最大极差 0.1584）
不可用但保留：sift1m/S（极差 0.5144）、sift1m/pooled、synthetic/pooled（pooled 无物理意义）
```

## 2. 让人和 AI 都能直接用：`recommend_stage_weights()`

不需要先读文档才知道 S/M/L 怎么分、哪组能用：

```sql
select * from vecdiag.recommend_stage_weights('ivfflat', 300000);
--  applicable | size_class | dataset | phases | max_dispersion | note
--  t          | M          | sift1m  |      4 |         0.0821 | 按行数 300000 命中 M 档；极差 0.0821 达标
```

三种回答，每种都说清了为什么：

| 情况 | 返回 | note 里说什么 |
|---|---|---|
| 命中的档达标 | `applicable=t` | 命中哪档、极差多少 |
| 命中的档不达标 | `applicable=t` + 换档 | 明说"退用极差最小的 X 档，ETA 只能当量级参考，结论里要注明换档" |
| 该数据集下没有任何达标权重 | **`applicable=f`** | 指名该跑 `tools/measure_build_time.sh` 与 `tools/load_stage_weights.sh` 去标定，**不拿别的数据集的权重顶替** |
| 行数超过标定上界 100 万 | `applicable=t` | 标明"属外插，ETA 只能当量级参考" |

第三种是关键：**选不出来时明确拒绝，而不是随便给一组凑数**。HNSW 目前就是这种状态
（只有一个阶段、没有分档标定），调用它会得到 `applicable=f`。

## 3. 零参数体检 `vecdiag.diagnose()`

拿到库就能跑，不需要参数、不需要先读文档：

```sql
select * from vecdiag.diagnose();
```

每一条输出都带齐**四要素**——问题、原因、调整方法、验证方式，
且"调整方法"必须是能直接粘贴执行的语句。实测输出举例：

```
severity | warn
object   | public.sift_base(v)
problem  | 按 m=16 建 HNSW 预计在第 218481 行左右发生落盘降级（图放不下内存）
cause    | 预计图内存 1172.0 MB 超过 maintenance_work_mem=262144 kB。上游此时只打一个
           NOTICE 然后转磁盘继续建（hnswbuild.c:530-549），**构建不会失败，只会显著变慢**
fix      | 执行 SET maintenance_work_mem = '1348MB'; 再建索引；内存确实给不到这么多时
           改用更小的 m（如 m=8，图内存约降 21%）
verify   | SET maintenance_work_mem='1348MB'; 重建后 NOTICE 应当消失；降级点区间可用
           select * from vecdiag.hnsw_spill_range(1000241,128,16,262144) 复算。
           注意 confidence=calibrated
```

当前覆盖的检查项：

| 检查 | 触发条件 | 复用了哪个模块 |
|---|---|---|
| 本机未做 ABI 实测 | `abi_const` 里没有 `source='measured'` 的行 | — |
| 统计信息缺失/过期 | `relpages=0` 或 `reltuples<=0` | — |
| IVFFlat 建不起来 | 按 `lists=rows/1000` 预测会在某检查点越界 | M1 |
| HNSW 落盘降级 | 按 `m=16` 预测图放不下 | M2 |
| 高维 TOAST 风险 | `vecdiag.toast_risk(dims)` 为真 | M1 |

**体检第一次跑就抓到了我自己的一个交付问题**：ABI 常数明明实测过（`MAXALIGN(itemsize)=520`），
但没有写回 `abi_const`，表里全是 `source='source-code'`。体检据此报了 warn。
已把写回动作加进 `tools/abi_probe.sh`（不是手工改数据库），现在实测值随标定自动落库。

## 4. 边界

- 体检的 `possible_*` 类判断都来自**系统目录静态推断**，不是 EXPLAIN 结论。回归测试第 3 项
  用措辞检查守这条：输出里不许出现 "EXPLAIN"、"执行计划"。
- IVFFlat 那条用的是 `lists = rows/1000` 这个经验取值（pgvector 文档的建议），
  不是用户实际会用的参数。它回答的是"按常见取值会不会出事"，不是"你的建索引语句会不会出事"。
  要精确判断请直接调 `vecdiag.ivfflat_predict_table()` 传真实 `lists`。
- HNSW 那条固定按 `m=16`（上游默认值）估算。
- 体检不改任何数据、不建索引，是纯只读函数（`stable`）。
- 所有 `vecdiag` 函数都固定了 `search_path = pg_catalog, pg_temp`，回归测试第 9 项守这条。
