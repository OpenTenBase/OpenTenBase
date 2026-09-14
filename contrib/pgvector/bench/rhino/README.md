# Rhino-Bird IVFFlat 自适应构建复现材料

本目录用于展示本项目在 OpenTenBase / pgvector 中对 IVFFlat 索引构建与诊断能力的增强。

原有 IVFFlat 构建路径主要使用 Elkan K-Means。当其估算内存超过 `maintenance_work_mem` 时，索引构建可能失败。本项目保留原有 Elkan 路径，并在 Elkan 内存不足、Yinyang 可以满足当前内存预算时，自动切换到更低内存的 Yinyang K-Means 构建路径。

同时，本项目增加了 IVFFlat 配置诊断能力，用于辅助识别 `lists`、`probes`、统计信息和构建内存等常见问题。

## 交付文档

本目录提供两份完整的项目说明材料：

- [IVFFlat 内存自适应构建与诊断增强：技术报告与复现指南](docs/IVFFlat_Adaptive_Build_Technical_Report.pdf)  
  介绍项目背景、自适应构建设计、Elkan / Yinyang 选择策略、配置诊断能力、核心实验结果以及最小复现方式。建议首次了解本项目时先阅读此文档。

- [IVFFlat 实验环境与详细实验数据](docs/IVFFlat_Experiment_Environment_and_Results.pdf)  
  记录正式实验环境、数据集来源与 SHA256、版本与 Commit、K-Means 内存估算、构建时间、Recall@10、低内存成功/失败结果以及自动化测试证据，用于进一步核验实验结果。

## 快速复现

在 OpenTenBase 仓库根目录执行：

```bash
bash contrib/pgvector/bench/rhino/reproduce_smoke.sh
```

脚本使用本地确定性小规模数据，不需要下载外部数据。

正常运行时会验证以下内容：

- `automatic Yinyang fallback selected`  
  表示 Elkan 的估算内存超过当前 `maintenance_work_mem`，系统自动切换到 Yinyang 构建路径。

- `CREATE INDEX succeeded`  
  表示在低内存路径下，IVFFlat 索引成功完成构建。

- `planner uses demo_vectors_ivfflat_idx`  
  表示生成的 IVFFlat 索引能够被实际查询计划使用。

- `query returned 5 rows`  
  表示构建出的索引不仅创建成功，而且能够正常完成实际向量查询。

- `lists_high`、`low_recall_risk`、`build_memory`  
  表示诊断接口能够识别 IVFFlat 参数设置和构建内存方面的问题。

脚本最终应输出：

```text
Rhino-Bird IVFFlat reproduction: PASS
```

该脚本用于最小功能复现，主要验证自适应构建、索引可用性和配置诊断，不等同于正式的 SIFT / GloVe / GIST 实验。

如需进一步验证 Elkan 和 Yinyang 均无法满足当前内存预算时的失败提示，可执行：

```bash
bash contrib/pgvector/bench/rhino/reproduce_smoke.sh --extended
```

## 核心思路

原有 IVFFlat 构建使用 Elkan K-Means，速度表现较好，但在数据规模或 `lists` 较大时，构建阶段的内存需求可能明显增加。

本项目引入了低内存 Yinyang K-Means 路径。正式实验中，SIFT、GloVe 和 GIST 三组 workload 的 K-Means 内存估算分别降低约 79%、82% 和 45%。

因此，本项目没有直接替换原有 Elkan，而是采用自动选择策略：

```text
Elkan 可以满足当前内存预算
        |
        v
    使用 Elkan

Elkan 超出预算，但 Yinyang 可以满足
        |
        v
   自动使用 Yinyang

两种路径均无法满足预算
        |
        v
返回 ERROR / DETAIL / HINT
```

也就是说，正常情况下继续使用原有 Elkan；只有在 Elkan 无法满足当前内存预算时，才自动切换到低内存 Yinyang 路径，用户无需手动选择 K-Means 算法。

除自适应构建外，本项目还补充了构建失败反馈和 IVFFlat 配置诊断能力。

## 实验结果概览

| 数据集 | Elkan 估算内存 | Yinyang 估算内存 | 降低比例 | 低内存结果 |
|---|---:|---:|---:|---|
| SIFT-128 | 221.218 MiB | 45.939 MiB | 79.23% | Elkan FAIL / Yinyang SUCCESS |
| GloVe-100 | 258.513 MiB | 46.404 MiB | 82.05% | Elkan FAIL / Yinyang SUCCESS |
| GIST-960 | 389.431 MiB | 214.152 MiB | 45.01% | Elkan FAIL / Yinyang SUCCESS |

以上内存数据指 IVFFlat K-Means 构建内存估算 / `maintenance_work_mem` 可行性估算，不等同于进程 RSS。

构建时间方面：

- SIFT-128：Yinyang 总体构建时间中位数约快 11.58%；
- GloVe-100：Yinyang 构建时间中位数约增加 27.77%；
- GIST-960：Yinyang 构建时间中位数约增加 26.98%。

因此，本项目不假设 Yinyang 在所有 workload 上都更快，其主要定位是内存不足时的低内存 fallback。

完整实验数据请查看：

- [IVFFlat 实验环境与详细实验数据](docs/IVFFlat_Experiment_Environment_and_Results.pdf)

## 测试状态

当前验证结果：

```text
pgvector regression        14/14 PASS
adaptive K-Means TAP       20/20 PASS
IVFFlat diagnostics TAP    29/29 PASS
integration smoke          PASS
```

## 项目范围

当前实现主要面向：

- PostgreSQL 18 / OpenTenBase；
- 集中式部署环境；
- IVFFlat 索引构建。

当前不覆盖：

- HNSW 索引构建；
- 分布式索引构建；
- 对 Yinyang 构建时间普遍更快的假设。

## 目录结构

```text
contrib/pgvector/bench/rhino/
├── README.md
├── reproduce_smoke.sh
└── docs/
    ├── IVFFlat_Adaptive_Build_Technical_Report.pdf
    └── IVFFlat_Experiment_Environment_and_Results.pdf
```

生产代码位于 `contrib/pgvector/` 下。

本目录 `contrib/pgvector/bench/rhino/` 仅用于存放评审所需的复现脚本和项目交付文档。
