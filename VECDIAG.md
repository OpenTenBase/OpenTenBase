# 本仓库的交付内容：向量索引构建期诊断（vecdiag）

这是 2026 腾讯犀牛鸟开源课题实战训练营 · OpenTenBase **方向二**（向量索引构建优化与诊断）的交付仓库。

**上游代码一行未改。** 本仓库在 `REL_18_STABLE` 基线之上只**新增**一个目录 `vecdiag/`，
所以 `git diff` 干净、补丁可独立应用，也不会给上游合并带来负担。

```
基线      OpenTenBase/OpenTenBase  REL_18_STABLE
          commit 4c66f172a09296b08d53526f802ddd2b461bd7e8（PostgreSQL 18.6，纯社区代码）
开发分支  feature/vector-build-diagnostics
交付目录  vecdiag/
```

## 从哪开始看

| 想知道什么 | 看这里 |
|---|---|
| 这个模块解决什么问题、怎么用 | `vecdiag/README.md` |
| **审查用**：怎么独立复核、我自报的薄弱点、门禁判定 | `vecdiag/report/审查报告-20260826.md` |
| 一条命令把所有结论重跑一遍 | `bash vecdiag/reproduce.sh` |
| IVFFlat 构建内存模型的推导与验证 | `vecdiag/docs/M1-ivfflat-memory-model.md` |
| HNSW 落盘降级预警的标定与验证 | `vecdiag/docs/M2-hnsw-spill-model.md` |
| 原始 stderr、CSV、SHA256、环境快照 | `vecdiag/results/` |

## 目录结构

```text
vecdiag/
├── README.md          模块说明与快速开始
├── reproduce.sh       一键复现：搭环境 → 装 SQL → 跑全部验证
├── sql/               按序号加载，每个模块一个文件，可独立装卸
│   ├── 00_schema.sql              schema、ABI 常数（带来源标注）、内存参数解析
│   ├── 10_ivfflat_memory_model.sql  M1：分项 breakdown + 三检查点 first_hit
│   ├── 20_legacy_model.sql        旧模型（0.8.0 口径）对照实现
│   └── 30_hnsw_model.sql          M2：图内存标定系数 + 降级点预测与区间
├── tools/             环境搭建、ABI 实测、验证 harness、绘图（纯标准库）
├── tests/             验证矩阵、回归断言、上游能力清单
├── results/           每轮实测产物：原始 stderr + CSV + SHA256SUMS + env.txt
├── docs/              各模块设计文档与图（figs/ 下为 SVG，可从 CSV 重建）
├── report/            审查报告；项目报告与 PPT 收尾时放这里
└── patches/           若最终提交构建期优化补丁，放这里
```

`sql/` 用数字前缀顺序加载，`tools/install.sh` 会按序执行。新模块只要加一个
`NN_xxx.sql` 就能装进来，不需要改动已有文件——M3、M4 会按这个方式接入。

## 已完成与验证状态

| 模块 | 状态 | 关键数字 |
|---|---|---|
| M1 IVFFlat 构建内存预测 | 完成 | 20 组矩阵**逐字命中、误差 0**；旧模型对照偏 1.00×–**306×** |
| M2 HNSW 落盘降级预警 | 核心完成 | 11 组实测降级点**全部落在预测区间内**，点预测平均误差 **1.20%** |
| 上游测试基线 | 完成 | `installcheck` 14/14；`prove_installcheck` 48 文件 **1250 测例全过** |
| M3 阶段耗时与进度 | 进行中 | — |
| M4 诊断整合 / 项目报告 | 未开始 | — |

## 三条不越界的声明

1. `pg_stat_progress_create_index` 与 pgvector 的子阶段上报是**上游已有能力**，
   本仓库没有重新实现它们。我们做的是上游没有的部分：构建前的内存预测、
   跨阶段加权进度、剩余时间预测。
2. 大 `lists` 场景用的是"压低 `maintenance_work_mem` 触发同源检查点"的间接验证，
   **只证明后端检查点与预测一致，不证明巨型索引真的能建成**。
3. ABI 常数与机器、编译器、`BLCKSZ` 绑定。换机器必须重跑 `tools/abi_probe.sh`，
   不要把 `results/` 里的数字当成新机器上的结论。
