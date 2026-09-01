# tools/ · 工具导览

每个脚本自包含（`PGHOME/PGPORT/PGDB` 环境变量可覆盖，默认 5518）。
按用途分四类；**标定类换机器必须重跑**，其他类按需。

## ① 复现主链路（reproduce.sh 会自动调用）

| 脚本 | 干什么 | 产出 |
|---|---|---|
| `bootstrap_env.sh` | CentOS 7：编译 PG18.6 + pgvector 0.8.6 全流程 | `/data/pg18/` 实例 |
| `bootstrap_env_rocky.sh` | Rocky 8：同上（gcc-toolset-12 路线，已实测） | 同上 |
| `env_check.sh` | 环境快照（CPU/内存/SELinux/编译器/依赖） | env.txt |
| `install.sh` | 按序号安装 `sql/` 全部模块 | vecdiag schema |
| `verify_phenomena.sh` | 复现两个诊断对象原文（IVFFlat ERROR / HNSW NOTICE） | phenomena/ |
| `abi_probe.sh` | ABI 常数实测（C1 隔离 + 对 mwm 二分到 1 kB，写回 abi_const） | abi/ |
| `validate_memory_model.sh` | M1 20 组矩阵逐例验证（四态 harness） | m1-*/ |
| `compare_models.sh` | M1 新模型 vs pgvector 0.8.0 旧公式对照 | model_compare.csv |
| `hnsw_spill_probe.sh` | M2 降级点标定（8 组） | m2/ |
| `hnsw_validate.sh` | M2 外样本验证（V 组）+ 建议双向验证（E 组） | m2v/ |
| `measure_build_time.sh` | M3 阶段耗时（S/M/L 三档 ×3 次 + 采样开销交替测量） | m3/ |
| `progress_sampler.sh` | 进度视图采样器（50ms 轮询出 CSV；被上面调用） | samples/ |
| `load_stage_weights.sh` | 采样序列 → 阶段权重三层表 | stage_weight 表 |
| `param_sweep.sh` | M5 参数扫描（构建+召回+查询，3 次重复） | t27/ |
| `load_param_facts.sh` | 扫描 CSV → 参数建议表（median 库内重算） | param_measure 表 |
| `hnsw_eta_spill.sh` | T3.5：降级对 ETA 的影响 + 修正对比 | t35/ |
| `anomaly_matrix.sh` | T4.3/T4.4：18 用例异常矩阵（DANGEROUS 判定） | t44/ |
| `perf_compare.sh` | 从库内与归档 CSV 重算统一对比表（21 行） | perf/ |

## ② 标定与重拟合（换机器必跑）

| 脚本 | 干什么 | 产出 |
|---|---|---|
| `hnsw_calib_sweep.sh` | M2 密网格标定（dims×m，m=24 留网格外） | calib_sweep.csv |
| `load_hnsw_calib.sh` | 标定 CSV → hnsw_calib 表 | 表 |
| （SQL）`vecdiag.hnsw_refit(true)` | OLS 重拟合 slot/base，残差>5% 拒写 | hnsw_coef 表 |

## ③ 实时监控

| 脚本 | 干什么 |
|---|---|
| `watch_build.sh` | 建索引时的命令行看板（调 `vecdiag.build_monitor()`，M/Z 注：监控他人构建须传 `MWM_KB`） |

## ④ 数据获取与一次性证据生成（历史轮已用，复现时按需）

| 脚本 | 干什么 |
|---|---|
| `load_sift1m.sh` | 下载/灌入 ANN_SIFT1M（原站慢时用下面的转换器替代获取） |
| `sift_from_annbenchmarks.py` | ann-benchmarks HDF5 → fvecs（转换产物与官方逐字节一致） |
| `checkpoint_verify.sh` | 三检查点隔离复验（t13 证据） |
| `verify_on_real_data.sh` | M1 真实数据抽查（real-20260826 证据） |
| `plot_compare.py` | 纯标准库 SVG 绘图（docs/figs/ 可从 CSV 重建） |

## 写新工具时遵守

1. 数值参数白名单校验（T4.6：恶意输入在调 psql 前被拒）；
2. 长任务放 `nohup` 且**连接预检**（实例悄悄死掉时宁可失败退出，不要产出误导性空结果——踩过）；
3. 产物目录自带 `SHA256SUMS`；
4. 脚本内注释记录踩过的坑，不写"为什么我是对的"。
