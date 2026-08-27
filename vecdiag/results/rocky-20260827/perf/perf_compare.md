# 性能与能力对比（run repro-20260827-125434-perf）

本表由 `tools/perf_compare.sh` 从库内数据与归档 CSV 重算生成，不含手工填写的数字。

| 组 | 对比项 | 基线 | 基线值 | 改进/本项目 | 改进值 | 单位 | 差异 | 证据 |
|---|---|---|---|---|---|---|---|---|
| M1-内存模型 | 报错MB逐字命中 | 旧公式(0.8.0) | 高估 1.00–306.35 倍 | 本模型 | 18/18 逐字命中（另 2 例预测不报错且实际未报错） | 倍/例 | 平均高估 29.54 倍 | `results/m1-r3-20260826/model_compare.csv` |
| M1-内存模型 | 内存下界 | 报错消息里的MB | 13 | 所需内存(三检查点最大值) | 62 | MB | 差 4.8 倍 | `results/t44-20260827/anomaly_matrix.csv（B* 组）` |
|  |  |  |  |  |  |  |  | `` |
|  |  |  |  |  |  |  |  | `` |
| M2-降级点 | 外样本(mwm=60MB，标定用的是4/8/16MB) | 实测NOTICE | 51267 | 事前预测 | 51206 | 行 | 偏差 0.12% | `results/t35-20260827/prediction.txt 与 spill_notice.txt` |
| T2.7-参数 | HNSW m=8/ef_construction=64 | 构建中位耗时 | 14597 | recall@10 | 0.9390 | ms / 比例 | 在帕累托前沿 | `vecdiag.param_pareto（run repro-20260827-125434-t27）` |
| T2.7-参数 | HNSW m=16/ef_construction=64 | 构建中位耗时 | 27081 | recall@10 | 0.9813 | ms / 比例 | 在帕累托前沿 | `vecdiag.param_pareto（run repro-20260827-125434-t27）` |
| T2.7-参数 | HNSW m=8/ef_construction=200 | 构建中位耗时 | 35477 | recall@10 | 0.9603 | ms / 比例 | 被支配：m=16/ef=64（27081 ms; recall 0.9813） | `vecdiag.param_pareto（run repro-20260827-125434-t27）` |
| T2.7-参数 | HNSW m=32/ef_construction=64 | 构建中位耗时 | 50817 | recall@10 | 0.9913 | ms / 比例 | 在帕累托前沿 | `vecdiag.param_pareto（run repro-20260827-125434-t27）` |
| T2.7-参数 | HNSW m=16/ef_construction=200 | 构建中位耗时 | 61491 | recall@10 | 0.9887 | ms / 比例 | 被支配：m=32/ef=64（50817 ms; recall 0.9913） | `vecdiag.param_pareto（run repro-20260827-125434-t27）` |
| T2.7-参数 | HNSW m=32/ef_construction=200 | 构建中位耗时 | 117399 | recall@10 | 0.9943 | ms / 比例 | 在帕累托前沿 | `vecdiag.param_pareto（run repro-20260827-125434-t27）` |
| T2.7-参数 | IVFFlat lists=100 | 构建中位耗时 | 1662 | recall@10 | 0.9867 | ms / 比例 | 在帕累托前沿 | `vecdiag.param_pareto（run repro-20260827-125434-t27）` |
| T2.7-参数 | IVFFlat lists=316 | 构建中位耗时 | 3889 | recall@10 | 0.9337 | ms / 比例 | 被支配：lists=100（1662 ms; recall 0.9867） | `vecdiag.param_pareto（run repro-20260827-125434-t27）` |
| T2.7-参数 | IVFFlat lists=1000 | 构建中位耗时 | 28698 | recall@10 | 0.8647 | ms / 比例 | 被支配：lists=100（1662 ms; recall 0.9867）; lists=316（3889 ms; recall 0.9337） | `vecdiag.param_pareto（run repro-20260827-125434-t27）` |
| M3-采样开销 | 50ms 轮询进度视图 | 无采样(中位) | 5175 | 有采样(中位) | 5096 | ms | -1.53%（低于本机噪声底，不当成加速） | `results/m3r-sift1m-20260826/build_time_stats.csv` |
| 上游对照 | 构建前能否知道会 OOM | pgvector 0.8.6 | 只在超限时报错，事后 | 本项目 | 事前预测，逐例命中 | - | 上游无事前预测能力 | `tests/upstream_inventory.sql` |
| 上游对照 | 构建侧 GUC 数量 | pgvector 0.8.6 | 0（7 个 GUC 全在查询/扫描侧） | 本项目 | SQL 层 6 个模块 | 个 | 需 LOAD 'vector' 后才可见 | `results/t05-20260826/upstream_inventory.txt` |
| 上游对照 | HNSW 图内存数字 | pgvector 0.8.6 | 仅 #ifdef HNSW_MEMORY 下 elog(INFO) | 本项目 | 用 NOTICE 反解，无需重编译 | - | hnswbuild.c:307 | `docs/M2-hnsw-spill-model.md` |
| 上游对照 | 跨阶段进度百分比 | PostgreSQL+pgvector | 只报当前阶段名 | 本项目 | 加权百分比+ETA+可用性分层 | - | 阶段内计数只有部分阶段有 | `docs/M3-progress-and-stage-timing.md` |
