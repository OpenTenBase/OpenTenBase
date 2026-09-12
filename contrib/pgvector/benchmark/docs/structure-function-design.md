# Project2 P1 Structure 功能函数设计

## 1. 主流程

```text
Build success + index_oid/relfilenode
  → collect_structure_stats()
  → 校验当前 catalog identity
  → ivfflat_list_stats(index_oid, sample_rate)
  → lists.jsonl
  → summarize_structure()
  → scan.json + summary.json
```

数据库函数使用 `AccessShareLock` 打开索引、对读取的 buffer 使用 share lock，只读
metapage、list page 和 entry page，不修改页面、不生成 WAL。

## 2. 函数列表

### `ivfflat_list_stats`

位置：
`OpenTenBase/contrib/pgvector/src/ivfstats.c`

作用：
按 list 返回 IVFFlat 原始结构事实；`list_id` 从 0 开始。

调用时机：
Build 成功且索引 identity 校验通过后，由 Structure Collector 调用。

输入：
- `index regclass`：目标 IVFFlat index OID。
- `sample_rate real`：`(0,1]`；1 表示全量 list。

输出/状态变化：
- 每个被选 list 一行。
- 空 list 的 `start_page/insert_page` 可以为 `NULL`。
- tuple 只统计 entry page 上 `ItemIdIsNormal()` 的物理 IndexTuple。
- `free_bytes = PageGetExactFreeSpace()`；`used_bytes = usable_bytes - free_bytes`。
- sample 模式记录固定算法版本 `list_id_multiplicative_hash_v1`。

异常处理：
- 非 index、非 IVFFlat、非法 sample rate、权限不足直接 ERROR。
- metapage、page id、block 边界、page chain cycle、跨 list 重复页异常使用
  `ERRCODE_INDEX_CORRUPTED`。
- 循环中执行 `CHECK_FOR_INTERRUPTS()`。

### `ivfflat_index_stats`

位置：
`OpenTenBase/contrib/pgvector/src/ivfstats.c`

作用：
全量扫描所有 list，返回一行索引级物理结构摘要。

调用时机：
DBA 独立检查或 SQL 回归交叉校验；Diagnosis 默认由 Python 汇总 list rows，避免重复扫描。

输入：
- `index regclass`：目标 IVFFlat index OID。

输出/状态变化：
- list/entry/scanned page 数、物理 tuple、empty list、空间和扫描耗时。
- `scanned_pages = 1 metapage + list_pages + entry_pages`。

异常处理：
- 与 `ivfflat_list_stats()` 使用相同的 relation、权限和页面校验。

### `IvfflatCollectStats`

位置：
`OpenTenBase/contrib/pgvector/src/ivfstats.c`

作用：
两个 SQL 函数共享的内部扫描入口。

调用时机：
SQL 包装函数完成参数和 relation 校验之后。

输入：
- 已锁定的 index relation、sample rate、可选 Tuplestore。

输出/状态变化：
- 填充 `IvfflatIndexStatsData`；list 模式同时写 Tuplestore。

异常处理：
- 发现 list 数与 metapage 声明不一致即 ERROR。

### `IvfflatStatsScanEntryChain`

位置：
`OpenTenBase/contrib/pgvector/src/ivfstats.c`

作用：
完整扫描一个被选 list 的 entry chain。

输入：
- `startPage/insertPage`、relation block 上界、全局 page-kind registry。

输出/状态变化：
- entry page、physical tuple、usable/used/free bytes。

异常处理：
- block 重复声明用于检测本 list cycle 和跨 list page sharing。
- 有效 `insertPage` 不属于 entry chain 时 ERROR。

### `collect_structure_stats`

位置：
`benchmark/ivfflat_diagnosis/structure.py`

作用：
关联 Build、执行 Structure SQL 并持久化原始与汇总事实。

调用时机：
一次 Build 成功且核心 index metadata 可用后。

输入：
- diagnosis/build identity、index OID/relfilenode、sample rate。
- eligible vectors、index bytes、raw vector bytes。
- RunStore 和现有可读 Build 目录 prefix。

输出/状态变化：
- `structure/lists.jsonl`
- `structure/scan.json`
- `structure/summary.json`

异常处理：
- catalog OID/relfilenode 不一致时拒绝关联。
- 非 IVFFlat index 拒绝采集。

### `summarize_structure`

位置：
`benchmark/ivfflat_diagnosis/structure.py`

作用：
计算 tuple 分布、CV、Gini、empty、concentration、fill、coverage 和 amplification。

调用时机：
list rows 完成落盘后。

输入：
- list facts、Build identity、采样范围和外部 Build facts。

输出/状态变化：
- 带 availability/source/aggregation/scope 的版本化指标。

异常处理：
- sample scan 下 exact tuple、empty ratio 和 coverage 输出 unavailable，不用 0 代替。
- 分母为 0 或依赖缺失时对应指标输出 unavailable。

## 3. Structure 数据契约

机器 schema：

- `schemas/structure-list.schema.json`：`lists.jsonl` 每一行。
- `schemas/structure-scan.schema.json`：采集状态和 identity。
- `schemas/structure-summary.schema.json`：派生指标及 availability。

共同身份字段：

```text
schema_version = 1.0
structure_schema_version = 1.0
diagnosis_id
build_id
index_oid
relfilenode
timestamp_utc
source / availability / reason
```

完整 build ID 保存在记录内部；`output_prefix` 应使用现有
`builds/lists-<n>__<short-id>` 可读目录。

## 4. SQL/C 回归测试清单

- [x] 0.8.7 扩展全新安装包含两个函数。
- [x] 0.8.6 可以通过 upgrade SQL 更新到 0.8.7。
- [x] full list scan 返回维度、lists、0-based list ID 和有效页面位置。
- [x] list tuple 汇总与 index-level tuple 总数一致。
- [x] `used_bytes + free_bytes = usable_bytes`。
- [x] `scanned_pages = metapage + list_pages + entry_pages`。
- [x] DELETE 未 VACUUM 时 physical count 不冒充 MVCC live count。
- [x] VACUUM 后物理 tuple count 更新。
- [x] 空索引和 empty list 返回合法 0 计数。
- [x] sample 模式选择结果可重复且为 list 子集。
- [x] `sample_rate <= 0` 和 `> 1` 报错。
- [x] 普通表和非 IVFFlat index 报错。
- [x] NULL 参数遵循 STRICT 语义。
- [x] 无底表 SELECT 权限时报错，授权后可调用。
- [x] OpenTenBase 19beta3 编译无警告。
- [x] pg_regress `ivfflat_stats` 通过。
- [ ] 人工构造损坏页验证 page-id、越界、cycle 和重复引用错误；需要专用 fault-injection fixture，不在普通 SQL 回归中直接改数据文件。

## 5. 暂不包含

- Diagnosis Rule、Candidate/Evidence/Finding。
- Pareto、Recommendation、Validation。
- 逐 page/tuple 明细输出。
- 并发写入下的事务级结构快照承诺。
