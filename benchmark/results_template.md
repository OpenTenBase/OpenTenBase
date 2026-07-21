# OpenTenBase 基准测试数据

本报告基于 2026-07-21(最近一次测试) 的真实执行结果填写。所有性能数字均来自 `benchmark_results_formal_pctl_20260721_092139/` 下的原始日志与汇总文件。

## 1. 测试概览

| 项目 | 实际值 |
| --- | --- |
| 测试日期 | 最近: 2026-07-21 |
| 测试人员 | linnaid 主要负责测试核心和实际测试，AI 辅助脚本与结果整理 |
| 结果目录 | `benchmark_results_formal_pctl_20260721_092139` |
| 是否通过 CN 执行 | 是，统一通过 `172.17.0.2:11003` |
| 备注 | 在正式运行前修复了 CN 与 DN 本地 `pgxc_node.node_forward_port` 不一致问题；否则分布表查询会报 `57P01 terminating connection due to administrator command` |

## 2. 环境信息

| 项目 | 实际值 |
| --- | --- |
| OpenTenBase 版本 | `PostgreSQL 10.0 @ OpenTenBase_v5.0 (commit: 302e1a680) 2026-07-18 12:29:22 on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 9.4.0-1ubuntu1~20.04.2) 9.4.0, 64-bit` |
| 编译参数 | `--prefix=/data/opentenbase/install/opentenbase_bin_v5.0 --enable-user-switch --with-libxml --disable-license --with-openssl --with-ossp-uuid CFLAGS=-g` |
| 操作系统 | `Ubuntu 20.04.6 LTS (Focal Fossa)`，内核 `Linux 6.17.0-40-generic x86_64 GNU/Linux` |
| CPU | `AMD Ryzen 7 8845H w/ Radeon 780M Graphics`，`16 CPUs`，`8 cores / 16 threads`，单 socket |
| 内存 | `27 GiB total`，采集时 `14 GiB used / 1.5 GiB free / 10 GiB buff-cache / 6.2 GiB available`，`Swap 8.0 GiB` |
| 磁盘 | 容器视角 `overlay 98G total / 69G used / 25G available (74%)`；底层块设备包含 `nvme0n1 953.9G` |
| 网络 | 单机 Docker 容器网络，容器 IP `172.17.0.2`；`CN/DN/GTM` 全部位于同一地址 |
| CN 地址 | `172.17.0.2` |
| CN 端口 | `11003` |
| GTM 节点 | `gtm0001: 172.17.0.2:11000` |
| DN 列表 | `dn0001: 172.17.0.2:11006`，`dn0002: 172.17.0.2:11009` |
| 数据库名 | `benchmark` |
| default_group | 已存在并可正常使用 |

## 3. 数据规模

| 表 | 分布方式 | 目标行数 | 实际行数 | 备注 |
| --- | --- | --- | --- | --- |
| `perf_user` | `SHARD(user_id)` | 10,000 | 10,000 | 与目标一致 |
| `perf_order` | `SHARD(user_id)` | 100,000 | 100,000 | 与目标一致 |
| `perf_city` | `REPLICATION` | 100 | 100 | 每个 DN 本地均有完整副本 |
| `perf_event` | `SHARD(user_id)` | 500,000 | 500,000 | 与目标一致 |

## 4. 执行参数

| 项目 | 实际值 |
| --- | --- |
| warmup 并发 | `1,4` |
| warmup 时长 | `10s` |
| 正式测试并发 | `1,4,8` |
| 正式测试时长 | `30s` |
| pgbench jobs | runner 自动取并发数，实际对应 `1/4/8` |
| workload 过滤 | 全量 W1-W8 |

## 5. Workload 结果

| Workload | 并发 | 时长(s) | TPS/QPS | avg(ms) | P50(ms) | P95(ms) | P99(ms) | 原始日志 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| W1 `INSERT perf_order` | `1 / 4 / 8` | `30 / 30 / 30` | `1673.79 / 6704.38 / 10815.94` | `0.598 / 0.597 / 0.740` | `0.561 / 0.567 / 0.710` | `0.803 / 0.776 / 0.966` | `1.003 / 1.111 / 1.492` | `raw/w1_insert_c1/pgbench.log` `raw/w1_insert_c4/pgbench.log` `raw/w1_insert_c8/pgbench.log` |
| W2 分布键点查 | `1 / 4 / 8` | `30 / 30 / 30` | `2411.77 / 8379.15 / 12734.72` | `0.415 / 0.477 / 0.628` | `0.391 / 0.460 / 0.615` | `0.593 / 0.603 / 0.737` | `0.753 / 0.771 / 0.977` | `raw/w2_dist_key_lookup_c1/pgbench.log` `raw/w2_dist_key_lookup_c4/pgbench.log` `raw/w2_dist_key_lookup_c8/pgbench.log` |
| W3 非分布键过滤 | `1 / 4 / 8` | `30 / 30 / 30` | `19.29 / 40.46 / 32.25` | `51.855 / 98.880 / 248.109` | `51.396 / 97.072 / 246.036` | `55.090 / 111.740 / 305.074` | `58.327 / 124.835 / 343.387` | `raw/w3_non_dist_filter_c1/pgbench.log` `raw/w3_non_dist_filter_c4/pgbench.log` `raw/w3_non_dist_filter_c8/pgbench.log` |
| W4 分布键聚合 | `1 / 4 / 8` | `30 / 30 / 30` | `19.39 / 41.67 / 33.58` | `51.583 / 96.014 / 238.284` | `50.978 / 93.870 / 234.535` | `55.938 / 112.304 / 294.739` | `59.663 / 120.958 / 324.509` | `raw/w4_dist_key_aggregate_c1/pgbench.log` `raw/w4_dist_key_aggregate_c4/pgbench.log` `raw/w4_dist_key_aggregate_c8/pgbench.log` |
| W5 非分布键聚合 | `1 / 4 / 8` | `30 / 30 / 30` | `7.86 / 15.64 / 12.31` | `127.305 / 255.731 / 650.272` | `126.539 / 252.250 / 651.280` | `134.541 / 278.853 / 767.098` | `137.497 / 295.296 / 819.157` | `raw/w5_non_dist_aggregate_c1/pgbench.log` `raw/w5_non_dist_aggregate_c4/pgbench.log` `raw/w5_non_dist_aggregate_c8/pgbench.log` |
| W6 同分布 Join | `1 / 4 / 8` | `30 / 30 / 30` | `15.86 / 33.66 / 27.53` | `63.059 / 118.859 / 290.670` | `62.465 / 116.941 / 290.799` | `67.572 / 133.645 / 341.696` | `73.367 / 143.149 / 366.035` | `raw/w6_colocated_join_c1/pgbench.log` `raw/w6_colocated_join_c4/pgbench.log` `raw/w6_colocated_join_c8/pgbench.log` |
| W7 复制表 Join | `1 / 4 / 8` | `30 / 30 / 30` | `424.52 / 1688.96 / 3131.78` | `2.356 / 2.369 / 2.555` | `2.273 / 2.317 / 2.533` | `2.854 / 2.729 / 2.720` | `3.176 / 3.115 / 3.003` | `raw/w7_replication_join_c1/pgbench.log` `raw/w7_replication_join_c4/pgbench.log` `raw/w7_replication_join_c8/pgbench.log` |
| W8 GTM 短事务 | `1 / 4 / 8` | `30 / 30 / 30` | `985.28 / 3720.77 / 6182.81` | `1.015 / 1.075 / 1.294` | `0.932 / 1.029 / 1.259` | `1.395 / 1.357 / 1.519` | `1.641 / 1.707 / 1.991` | `raw/w8_gtm_short_tx_c1/pgbench.log` `raw/w8_gtm_short_tx_c4/pgbench.log` `raw/w8_gtm_short_tx_c8/pgbench.log` |

## 6. 执行计划观察

| Workload | 关键计划特征 | 证据 |
| --- | --- | --- |
| W2 | 只访问必要 DN，符合按 `user_id` 分布后的定点路由预期 | 正式结果中点查吞吐明显最高之一；同类分布键点查模式与 W8 读组件一致，W8 在 `explain.log` 中显示单 DN `Remote Fast Query Execution` |
| W3 | 非分布键过滤需要访问多 DN，延迟显著高于 W2 | TPS 从 `2411/8379/12735` 下降到 `19/40/32`，avg latency 上升到 `51ms/99ms/248ms` |
| W4 | 存在 DN 局部聚合，再由 CN 合并结果 | 吞吐明显低于 W2，且随并发增加延迟快速上升；符合 `GROUP BY user_id` 聚合与合并模式 |
| W5 | 多 DN 聚合和合并开销最大，是当前样本里最慢的 workload | avg latency `127ms -> 256ms -> 650ms`，P99 达到 `819ms` |
| W6 | 同分布 Join 在 DN 本地执行 `Hash Join + HashAggregate`，之后 CN 接收排序结果 | `explain.log` 显示两个 DN 各自执行 `Hash Join` 与 `HashAggregate`，无明显重分布证据 |
| W7 | 复制表 Join 在每个 DN 本地完成，减少跨节点搬运 | `explain.log` 显示 `Remote Subquery Scan on all (dn0001,dn0002)`，`perf_city` 在 DN 上本地 `Seq Scan` 后参与 `Hash Join` |
| W8 | 写入与读组件都走快速远程执行；读组件按分布键只访问一个 DN | `explain.log` 显示 `Remote Fast Query Execution`，并且 `SELECT count(*) ... user_id = 100` 只访问 `dn0002` |

## 7. 分布与倾斜

### 7.1 各 DN 行数

| 表 | DN | 行数 |
| --- | --- | --- |
| `perf_user` | `dn0001` | 5039 |
| `perf_user` | `dn0002` | 4961 |
| `perf_order` | `dn0001` | 50390 |
| `perf_order` | `dn0002` | 49610 |
| `perf_city` | `dn0001` | 100 |
| `perf_city` | `dn0002` | 100 |
| `perf_event` | `dn0001` | 251950 |
| `perf_event` | `dn0002` | 248050 |

### 7.2 倾斜指标

| 表 | max_rows | min_rows | avg_rows | skew_ratio | max_deviation_ratio |
| --- | --- | --- | --- | --- | --- |
| `perf_user` | 5039 | 4961 | 5000.00 | 1.0078 | 0.0156 |
| `perf_order` | 50390 | 49610 | 50000.00 | 1.0078 | 0.0156 |
| `perf_city` | 100 | 100 | 100.00 | 1.0000 | 0.0000 |
| `perf_event` | 251950 | 248050 | 250000.00 | 1.0078 | 0.0156 |

## 8. 系统观察

| 观察点 | 现象 | 证据 |
| --- | --- | --- |
| CN CPU | 未采集 | 本次仅保留了 SQL 与 pgbench 日志 |
| DN CPU | 未采集 | 本次未同步采集 `vmstat/iostat/pidstat` |
| GTM 压力 | 在 `1/4/8` 并发样本下未见明显瓶颈 | W8 TPS 从 `985` 增长到 `3721`、`6183`，结果目录无 `ERROR/FATAL` |
| 磁盘 IO | 未采集 | 本次未记录系统级 IO 指标 |
| 网络 | 单机容器内网络，不代表多机跨网卡场景 | `CN/DN/GTM` 都位于 `172.17.0.2` |
| 错误 / 超时 | 本次正式样本无错误 | `grep -R "ERROR\|FATAL\|aborted" benchmark_results_formal_pctl_20260721_092139` 无输出 |

## 9. 瓶颈判断

| 症状 | 可能瓶颈 | 验证证据 | 初步结论 |
| --- | --- | --- | --- |
| W2 吞吐远高于 W3/W5 | SQL / 分布设计 | 分布键点查 `12734 TPS@c8`，非分布键过滤 `32 TPS@c8`，非分布键聚合 `12 TPS@c8` | 当前主要瓶颈来自非分布键访问模式，而不是分布式链路本身 |
| W5 延迟最高 | CN 合并 + 多 DN 聚合 | `avg 650ms@c8`，`P99 819ms@c8` | 非分布键聚合是当前样本最差 workload |
| W6 明显慢于 W7 | 事实表扫描/聚合开销 | W6 `27.53 TPS@c8`，W7 `3131.78 TPS@c8` | 复制维表策略有效；同分布 Join 仍受大表扫描与聚合影响 |
| W8 随并发增长仍可扩展 | GTM | `985 -> 3721 -> 6183 TPS`，无错误日志 | 在 `c8` 以内未观察到 GTM 成为主要瓶颈 |
| 数据分布均衡 | DN | `skew_ratio` 约 `1.0078` | 暂无明显 DN 倾斜问题 |
| 系统资源是否成为瓶颈 | CN / 磁盘 / 网络 | 未采集系统指标 | 目前无法下最终结论，需要补系统监控日志 |

