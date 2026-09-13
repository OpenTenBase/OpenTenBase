# IVFFlat list-directory 缓存性能报告

[English Report](ivfflat-list-cache-performance.md)

日期：2026-09-11

## 摘要

本实验比较类型感知距离分发基线提交 与 IVFFlat
list-directory 缓存候选版本。候选版本将不可变的 list 聚类中心和 entry
起始页缓存在 backend 本地的 relation cache（`rd_amcache`）中，不缓存查询结果或
heap TID。

在四组测试配置中，三次重复的中位平均延迟下降 0.95%～2.97%，中位 TPS 提升
0.97%～3.06%。全部 12 组 baseline/candidate 对比的 Recall@10、返回结果数、
匹配结果数和失败事务数均保持不变。

## 对比版本

| 版本 | 源码 | vector.so SHA-256 |
| --- | --- | --- |
| Baseline | `0b16fb5bd0` | `c4b17e53576e25958ab152f9d7550c41f534ce1eaf077cd8fcc5c2f2953de67d` |
| Candidate | 尚未提交的 list-directory 缓存候选版本 | `2591f41c656b3f1c4924a3a8fffaefc05e85f6fe3e0d12a1b1500e1449733ef8` |

两个模块使用相同的 PostgreSQL 安装环境和 `COPT=-O2` 参数构建。baseline 与
candidate 复用了同一个物理 IVFFlat 索引。

## 测试负载

| 参数 | 值 |
| --- | --- |
| PostgreSQL | 19beta3 |
| pgvector | 0.8.6 |
| 数据类型 | `vector` |
| 数据行数 | 100,000 |
| 向量维度 | 128 |
| 查询向量数 | 1,000 |
| Recall 查询数 | 1,000 |
| K | 10 |
| Lists | 1,000 |
| Probes | 1、10 |
| Clients | 1、12 |
| Jobs | 1 |
| 每轮预热 | 5 秒 |
| 每轮测量 | 15 秒 |
| 重复次数 | 3 |
| JIT | 关闭 |
| 顺序扫描 | 禁用 |
| work_mem | 64 MB |
| 数据集随机种子 | 20260911 |
| 查询随机种子 | 20260912 |

## 中位数结果

每行数据均为三次重复的中位数。

| Probes | Clients | Recall@10 | Baseline 延迟（ms） | Candidate 延迟（ms） | 延迟变化 | Baseline TPS | Candidate TPS | TPS 变化 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 0.0862 | 0.525 | 0.518 | -1.33% | 1906.16 | 1932.30 | +1.37% |
| 1 | 12 | 0.0862 | 0.797 | 0.782 | -1.88% | 15056.24 | 15336.64 | +1.86% |
| 10 | 1 | 0.4588 | 2.124 | 2.061 | -2.97% | 470.72 | 485.11 | +3.06% |
| 10 | 12 | 0.4588 | 2.830 | 2.803 | -0.95% | 4240.54 | 4281.78 | +0.97% |

## 正确性门槛

全部 12 组重复级对比均通过：

- comparison 状态：12/12 为 `matched`；
- 每行 Recall@10 变化均为 `0.000000`；
- baseline 和 candidate 的失败事务数均为 0；
- baseline 和 candidate 均返回 10,000 个结果；
- baseline 和 candidate 的匹配结果数完全一致。

实现还通过了现有的 IVFFlat vector、halfvec 和 bit SQL 回归测试。在同一数据库
session 的手动验证中，第一次扫描、重复扫描、INSERT 后扫描和 `REINDEX` 后扫描
均返回 100 行。

## 优化原理

原始 `GetScanLists()` 在每次查询时都需要遍历 IVFFlat list-directory pages：

```text
ReadBuffer
→ LockBuffer
→ PageGetItem
→ 读取 center 和 startPage
→ UnlockReleaseBuffer
```

候选版本在第一次扫描时将不可变的 center 和 startPage 复制到索引 relation 的
`rd_amcache`。同一 backend 后续扫描直接遍历本地缓存，不再重复 pin、lock 和解析
list-directory pages。

缓存不保存 `insertPage`、候选 TID 或查询结果，因此不会绕过 heap 可见性检查，也不
需要处理查询结果缓存所涉及的 MVCC 失效问题。relcache 失效或 `REINDEX` 时，
PostgreSQL 会释放 `rd_amcache`，下次扫描重新加载目录。

如果索引存在尚未获得 `startPage` 的空 list，目录仅供本次扫描临时使用，扫描结束后
立即释放，不会跨查询保留可能因后续 INSERT 而过期的 `startPage`。如果缓存大小超过
4 MB，则继续使用原有 page traversal 路径。

## 收益为何较小

对于 1,000 个 lists、128 维向量，list-directory 约占 65～70 个索引页，backend
本地缓存约占 0.5 MiB。该优化消除了对这些目录页的重复 buffer lookup、pin、共享锁、
page item 遍历和解锁操作。

该优化没有减少以下开销：

- list-center 距离计算；
- 候选 entry page 读取；
- 候选向量距离计算；
- TupleTableSlot 构造；
- tuplesort 排序。

随着 probes 增加，候选扫描和排序仍然占据主要成本。因此端到端查询收益处于
1%～3% 范围，与优化机制一致。

## 波动与限制

单次 15 秒结果在延迟回退 6.02% 到提升 8.64% 之间波动。这些极值没有在三次重复中
持续出现，因此报告使用中位数，而不是挑选最佳单次结果。

当前限制包括：

- 仅测试了 L2；
- 测量时长为 15 秒，尚未达到最终 60 秒门槛；
- `probes=1/10` 的 Recall@10 较低，因此本实验主要验证扫描管理开销，不代表高召回
  生产配置；
- 没有单独记录 `GetScanLists` 阶段耗时；
- 尚未测试 cold-cache、1M rows、halfvec 和不同 lists 的扩展性；
- candidate 在测试时还没有正式 commit hash。

## 结论

IVFFlat list-directory 缓存通过了正确性门槛，并在所有测试配置中取得正向的中位数
结果。最稳定且最大的中位收益出现在 `probes=10、clients=1`：延迟下降 2.97%，
TPS 提升 3.06%。

在形成最终性能结论前，应对代表性配置执行 60 秒、三次重复的确认测试，并增加
`probes=20`。如果长时间测试仍保持约 1%～3% 的收益，应将结果描述为“降低
IVFFlat list 选择阶段固定开销”，而不是笼统宣称整个向量查询获得大幅加速。
