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

## 审查员：拿到仓库后的 10 分钟核验路径

前提：一台 CentOS 7 或 Rocky 8 机器（root 或 sudo），能访问 github.com。
仓库当前为 **private**——审查者需要先被加为 collaborator（Settings → Collaborators），
或等提交窗口公开后匿名访问。

```bash
git clone -b feature/vector-build-diagnostics git@github.com:muzimu217/OpenTenBase.git
cd OpenTenBase
bash vecdiag/reproduce.sh          # 全自动 13 步：编译 PG18.6+pgvector → 逐模块验证 → TAP
```

想先快速看一眼再跑全量（约 2 分钟）：

```bash
bash vecdiag/tools/bootstrap_env_rocky.sh   # 或 bootstrap_env.sh（CentOS 7）；约 15-25 分钟
psql … -c 'select * from vecdiag.diagnose();'   # 零参数体检
bash vecdiag/tools/verify_phenomena.sh quick1   # 两个诊断对象原文复现
```

跑完后对照 `reproduce.sh` 末尾打印的判定标准逐条核对（每条都能指到
`/data/artifacts/<run>/` 下的原始文件）。**环境证明**：`results/rocky-20260827/env-snapshot/`
与 `results/centos7-20260826/env-snapshot-20260826/` 分别是两台实测机器的 `pg_config --configure`、
版本串与配置文件；换机器的所有需重测量在 `docs/00-requirements-and-scope.md` 第五节列表。

## 从哪开始看

| 想知道什么 | 看这里 |
|---|---|
| **拿到库先跑这个**：一条 SQL 看当前库有什么风险 | `select * from vecdiag.diagnose();` |
| **正在建索引，想看进度和剩余时间** | `select * from vecdiag.build_monitor();` 或 `bash vecdiag/tools/watch_build.sh` |
| **要解决什么问题、上游缺什么、哪些没做** | `vecdiag/docs/00-requirements-and-scope.md` |
| **一张表看完所有"改进前 vs 改进后"** | `bash vecdiag/tools/perf_compare.sh` → `perf_compare.md` |
| 这个模块解决什么问题、怎么用 | `vecdiag/README.md` |
| **审查用**：怎么独立复核、我自报的薄弱点、门禁判定 | `vecdiag/report/审查报告-20260826.md` |
| 一条命令把所有结论重跑一遍 | `bash vecdiag/reproduce.sh` |
| IVFFlat 构建内存模型的推导与验证 | `vecdiag/docs/M1-ivfflat-memory-model.md` |
| HNSW 落盘降级预警的标定与验证 | `vecdiag/docs/M2-hnsw-spill-model.md` |
| 阶段耗时、加权进度、ETA 偏差量化 | `vecdiag/docs/M3-progress-and-stage-timing.md` |
| `m` / `ef_construction` / `lists` 该怎么选 | `vecdiag/docs/M5-param-advisor.md` |
| 实时监控能给什么、有哪三条硬限制 | `vecdiag/docs/M6-realtime-monitor.md` |
| 原始 stderr、CSV、SHA256、环境快照 | `vecdiag/results/` |

## 目录结构

```text
vecdiag/
├── README.md            模块说明与快速开始
├── reproduce.sh         一键复现（13 步：搭环境 → 装 SQL → M1-M6 全部验证 → TAP → 对比表）
│
├── sql/                 按**两位编号 = 模块号**排序加载（install.sh 依序执行）
│   ├── 00_schema.sql             基础：schema、ABI 常数（带来源标注）、内存参数解析
│   ├── 10_m1_ivfflat_memory.sql  M1：三检查点内存模型 + 所需内存下界
│   ├── 20_m2_hnsw_spill.sql      M2：图内存系数 + 降级点预测/区间 + 一键重拟合
│   ├── 30_m3_progress.sql        M3：阶段顺序（源码常量）、进度曲线、ETA 偏差、降级修正
│   ├── 40_m4_diagnose.sql        M4：零参数体检 + 结论可用性三层
│   ├── 50_m5_param_advisor.sql   M5：参数建议表（三类溯源）+ 帕累托前沿
│   ├── 60_m6_realtime.sql        M6：实时监控（含降级预警与修正 ETA）
│   └── 90_legacy_compare.sql     对照用：pgvector 0.8.0 旧公式实现（非主线）
│
├── tools/               全部脚本，按用途分类（见 tools/README.md 导览表）
├── tests/               回归断言：test_m1..m6 六个模块各一份 + matrix_m1.tsv
│   └── t/               TAP 用例（prove 可跑，含历史缺陷的回归）
├── results/             证据，**按机器分目录**（见 results/README.md 总索引）
│   ├── centos7-20260826/   第一台机器全部证据（含 index/ 最终态导出、archive/ 中间轮）
│   └── rocky-20260827/     第二台机器：13 步复验 + M2 重拟合（含跨机器对照 README）
├── docs/                M1-M6 模块文档 + 00-需求与范围对照 + figs/（SVG，可从 CSV 重建）
├── report/              最终提交成品：项目报告 PDF + 答辩 PPT（+ 过程审查报告）
└── patches/             预留：若最终提交构建期优化补丁
```

`sql/` 的编号即加载顺序，**十位数与模块号一一对应**（10=M1 … 60=M6），90 是对照实现。
依赖方向单向：后面的模块只引用前面的。新模块加一个 `NN_mX_*.sql` 即可，无需改已有文件。

## 已完成与验证状态

| 模块 | 状态 | 关键数字 |
|---|---|---|
| M1 IVFFlat 构建内存预测 | 完成 | 20 组合成矩阵 + **公开数据集 SIFT1M 4 组抽查全部逐字命中**（含 222 MB / 208 MB 大数）；旧模型对照偏 1.00×–**306×** |
| M2 HNSW 落盘降级预警 | 核心完成 | 11 组实测降级点**全部落在预测区间内**，点预测平均误差 **1.20%** |
| 上游测试基线 | 完成 | `installcheck` 14/14；`prove_installcheck` 48 文件 **1250 测例全过** |
| M3 阶段耗时与加权进度 | 完成 | 两套数据集共 **3919 采样点单调性断言通过**；权重按 S/M/L 分档；**实测证明权重依赖数据分布**（主导阶段从 k-means 46% 变成 loading 41%）|
| M4 零参数体检与可用性分层 | 完成 | `vecdiag.diagnose()` 零参数、每条输出带齐四要素且修复建议可直接执行；权重分三层（存档 / 审计 / 消费） |
| M1 补充：所需内存 ≠ 报错里的 MB | 完成 | 三配置边界**精确到 1 kB**；C1 253 kB 与 C3 63459 kB 差 250 倍 |
| M3 T3.4 ETA 偏差量化 | 完成 | 修掉两个缺陷后 L 档 MAD **42.33% → 29.08%**；`unstable` 窗口 K=1 有实测依据 |
| M3 T3.5 降级对 ETA 的影响 | 完成 | 降级点外样本偏差 **0.12%**；接入 M2 后全程 MAD **59.10% → 9.02%** |
| M5 构建参数建议表（T2.7） | 完成 | 每个数字标 source-code / upstream-doc / measured；6 个 HNSW 配置里 **2 个被支配** |
| M6 实时构建监控 | 完成 | 198 采样点单调、终值 100%，实时 ETA MAD **5.36%**，降级预警正确触发 |
| T4.3 / T4.4 异常场景与保守方向 | 完成 | 18 用例全部判定正确，`DANGEROUS` = **0** |
| T4.5 TAP 回归 | 完成 | `prove` **19/19 通过**，退出码 0 |
| **第二台机器全量复验（Rocky 8.10）** | 完成 | `reproduce.sh` 13 步全过：M1 **20/20**、M2 外样本 **0.03%–0.12%**、T4 **18/18 / 0 DANGEROUS**、TAP **19/19**；ABI 520/291 与首台一致；T2.7 帕累托结构与召回跨机器保持（两个被支配配置不变）；证据 `results/rocky-20260827/` |
| P5 项目报告与 PPT | 脚本已交付 | 讲稿与报告脚本见工作区 `09-讲稿/` |

## 数据集

复现**不依赖任何外部数据文件**：合成向量由脚本现场生成。此外接入了公开数据集
**ANN_SIFT1M**（TEXMEX / INRIA，1,000,000 × 128），用于回答"结论是否只在自造数据上成立"：

```
tools/load_sift1m.sh     下载 → 校验 sha256 → fvecs 流式转换 → COPY 入库（约 3 分钟）
results/centos7-20260826/index/sift_sha256.txt  sift.tar.gz 与 sift_base.fvecs 的校验和，证明用的是原始数据
```

M1 在 SIFT1M 上抽查 4 组全部逐字命中；M3 的阶段权重在 SIFT1M 上重测了一遍，
**与合成数据的结果并列保存、不互相覆盖**（`vecdiag.stage_weight.dataset` 列区分）。

## 结论怎么用（人和 AI 都适用）

阶段权重有五组，只有两组的极差达标。**证据全部保留，但默认只让人用可用的那两组**：

| 对象 | 给谁用 |
|---|---|
| `vecdiag.stage_weight_usable` | **消费方默认走这个视图**：只含达标且分过档的权重 |
| `vecdiag.stage_weight_audit` | 审查者：全部五组，每组附"可用/不可用 + 原因" |
| `vecdiag.stage_weight` | 存档原表 |

不确定该用哪组时不要自己挑，问它：

```sql
select * from vecdiag.recommend_stage_weights('ivfflat', 300000);
```

它会回答命中哪档、极差多少；命中的档不达标就明说换了档；**该数据集下没有任何达标权重时
返回 `applicable=false` 并指名该跑哪个脚本去标定，不会随便给一组凑数**。
详见 `vecdiag/docs/M4-diagnose-and-usability.md`。

## 三条不越界的声明

1. `pg_stat_progress_create_index` 与 pgvector 的子阶段上报是**上游已有能力**，
   本仓库没有重新实现它们。我们做的是上游没有的部分：构建前的内存预测、
   跨阶段加权进度、剩余时间预测。
2. 大 `lists` 场景用的是"压低 `maintenance_work_mem` 触发同源检查点"的间接验证，
   **只证明后端检查点与预测一致，不证明巨型索引真的能建成**。
3. ABI 常数与机器、编译器、`BLCKSZ` 绑定。换机器必须重跑 `tools/abi_probe.sh`，
   不要把 `results/` 里的数字当成新机器上的结论。
4. M5 里的 `recall@10` 属于**方向一的指标口径**，这里只作为构建参数取舍的质量轴，
   **不作为方向一的交付成果**。它的 ground truth 是库内顺序扫描重算的 exact top-10——
   公开数据集自带的 groundtruth 是针对全量 100 万底库的，用在子集上不成立。
5. 实时监控读不到别的后端的 `maintenance_work_mem`（PostgreSQL 不提供该能力），
   默认用监控会话自己的值。监控别人的构建时必须把对方的值传进来，否则降级预警会误报。
