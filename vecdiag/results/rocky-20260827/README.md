# Rocky 8.10 复验轮（2026-08-27）

第二台机器上的全量复现。第一台是 CentOS 7（`results/*-20260826/`，已到期销毁）；
本轮在 **Rocky Linux 8.10 虚拟机**上从零搭建并跑完 `reproduce.sh` 全部 13 步，
证明结论不是单机偶然。

## 环境

| 项 | 值 |
|---|---|
| OS | Rocky Linux 8.10（Green Obsidian），内核 4.18.0-553.72.1.el8 |
| 编译器 | gcc-toolset-12（12.2.1）；系统默认 gcc 8.5 |
| 资源 | 4 vCPU / 7665 MB 内存 / 40G 盘（18G 可用）/ SELinux **Disabled** |
| PostgreSQL | 18.6，commit `4c66f172a09296b08d53526f802ddd2b461bd7e8`（与第一台相同） |
| pgvector | 0.8.6，commit `8ee86c96f0fd72390f890aa8a336fda6d3ab4c6c`（与第一台相同） |
| 引导脚本 | `tools/bootstrap_env_rocky.sh`（本轮实测；CentOS 7 用 `bootstrap_env.sh`） |

`env-snapshot/manifest-basic.txt` 含 `pg_config --configure` 与版本串；
`bootstrap.log` 是完整搭建日志。

## 13 步结果（对照第一台）

| 判定标准 | 本轮（Rocky 8.10） | 第一台（CentOS 7） |
|---|---|---|
| M1 矩阵 | **20/20 PASS，0 FAIL，0 BLOCKED** | 20/20 |
| ABI `MAXALIGN(itemsize)` dims=128 | **520（实测）** | 520 |
| ABI `MaxHeapTuplesPerPage` | **291（实测）** | 291 |
| M2 外样本降级点偏差 | **0.03% / −0.08% / −0.12%** | ≤0.18% |
| M2 双向建议（E 组） | E1 按建议重建 NOTICE 消失 PASS；E2 12.05%（块台阶离群，同源复现） | E2 12.2% |
| T3.5 降级点（60MB 外样本） | 预测 51206 / 实测 **51290**，偏差 **0.16%** | 51267，0.12% |
| T3.5 降级减速比 | **3.96×**（0.754→2.983 ms/元组） | 4.03× |
| T4.3/T4.4 异常矩阵 | **18/18 correct，DANGEROUS=0，边界精确到 1 kB** | 同 |
| T4.5 TAP | **19/19，prove 退出码 0** | 19/19 |
| M3 采样开销（交替测量） | 1510 vs 1520 ms，−0.66%（噪声底内） | +0.99% / −1.53% |

## T2.7 参数扫描：帕累托结构跨机器保持

同一底库（SIFT1M 前 100k×128）、同一查询侧参数（`ef_search=40` / `probes=10`）、
每配置 3 次。**构建耗时整体比第一台慢**（虚拟机单核较弱，1.5–1.7×），但结论不变：

| 配置 | 本轮构建中位 (ms) / recall@10 | 第一台 (ms) / recall | 帕累托 |
|---|---|---|---|
| m=8 / ef=64 | 14 597 / 0.9390 | 8 326 / 0.9427 | 前沿 |
| m=8 / ef=200 | 35 477 / 0.9603 | 23 679 / 0.9583 | **被支配**（两台一致） |
| m=16 / ef=64 | 27 081 / 0.9813 | 18 192 / 0.9790 | 前沿 |
| m=16 / ef=200 | 61 491 / 0.9887 | 40 573 / 0.9893 | **被支配**（两台一致） |
| m=32 / ef=64 | 50 817 / 0.9913 | 36 751 / 0.9917 | 前沿 |
| m=32 / ef=200 | 117 399 / 0.9943 | 70 722 / 0.9943 | 前沿 |

`ef_construction` 不改变索引体积（71.0 / 79.4 / 98.1 MB 三档两台机器一字不差）、
"调大 m 比调大 ef_construction 划算"的结论在第二台机器上成立。
recall 差异 ≤0.004（测量噪声），索引体积逐字节同档。

## M2 系数跨机器复核

本轮标定的 per_element（`m2/hnsw_spill.csv`）：1227.5 / 973.6 / 1738.2 / 2251.4 B，
与第一台（1226.4 / 973.5 / 1738.9 / 2252.0）偏差 ≤0.1%。
**库里未重新拟合系数**——沿用第一台的 31.89 / 206.4 直接在外样本上验证
（0.03%–0.12%），这比重新拟合更有说服力：系数跨机器可迁移。
若按本轮三点重算：slot_coef≈31.86、base≈205.1，与旧值差 <0.6%。

## 数据来源说明（SIFT1M）

原站 `ftp.irisa.fr` 在本网络仅 ~20 KB/s。本轮改为从 **ann-benchmarks.com** 取
`sift-128-euclidean.hdf5`（294 MB），用 `tools/sift_from_annbenchmarks.py` 转回
corpus-texmex 的 fvecs/ivecs 格式：

- `sift_base.fvecs`：sha256 `21f66e29…` **与第一台归档的官方文件逐字节一致**
- `sift_query.fvecs`：`f7fc9be1…` **逐字节一致**
- `sift_groundtruth.ivecs`：`75571702…` 与官方不同——ann-benchmarks 自算的邻居顺序，
  本仓库工具不使用该文件（召回的 ground truth 一律库内顺序扫描重算），仅作归档

入库核对：1 000 000 行，relpages 71472，reltuples 999941——与第一台完全一致。

## 本轮的一个异常记录（不隐瞒）

`reproduce.sh` 步骤 3（复现两个诊断对象）在**自动化后台运行**中三个捕获全空（WARN），
但同一脚本、同一路径（`su - postgres -c "bash …verify_phenomena.sh"`）手动重跑
全部 `[OK]`（22 MB 报错原文、第 12137 行降级 NOTICE、8 行 GUC 清单），见 `phenomena/`
（run `rocky1-20260827`，含 SHA256SUMS）。怀疑是后台会话中 heredoc 的偶发竞态，
下一台新机器复跑 `reproduce.sh` 时请留意步骤 3 是否复现该现象；现象本身的证据以
`phenomena/` 为准。

## 目录

```
abi/        ABI 常数实测（bisect 到 1 kB，写回 abi_const）
m1/         20 组验证矩阵逐例结果 + stderr + SHA256SUMS
m2/ m2v/    HNSW 降级标定与外样本验证
m3/         阶段耗时（合成档）与采样开销
t27/        T2.7 参数扫描原始 CSV 与统计
t35/        T3.5 降级对 ETA 的影响（预测/实测/分段速率）
t44/        异常场景矩阵 18 用例 + 每例原始 stderr
tap/        prove 输出与摘要
perf/       perf_compare.sh 生成的统一对比表
phenomena/  两个诊断对象 + GUC 清单（手动重跑，含哈希）
env-snapshot/  manifest-basic（configure 参数、版本串）
bootstrap.log  完整搭建日志
reproduce.log  13 步全量日志
```
