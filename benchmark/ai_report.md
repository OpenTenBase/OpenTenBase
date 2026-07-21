# OpenTenBase Benchmark AI 使用策略报告

## 1. 报告目的

本文件用于独立说明本次 OpenTenBase 基准测试中 AI 的使用边界、参与方式、人工验证过程和被拒绝建议。它与结果报告分开存放，避免将 AI 使用说明混在性能结果正文里。

## 2. 使用原则

本次 AI 使用遵循以下原则：

1. AI 负责辅助方案拆分、脚本草拟、日志整理和问题排查思路梳理。
2. AI 不直接生成未经验证的性能结论，不编造 TPS、延迟或执行计划结果。
3. 所有最终结果都必须来自真实运行日志、`EXPLAIN` 输出或系统命令采集结果。
4. 当 AI 建议与实际环境冲突时，以人工验证结果为准。

## 3. AI 参与阶段

### 3.1 方案设计

- 收敛交付物结构，避免把 benchmark 拆成过多文件。
- 明确需要覆盖：
  - 单表写入
  - 分布键点查
  - 非分布键过滤
  - 聚合查询
  - Join 查询
  - GTM 短事务
  - 并发连接

### 3.2 脚本与文档草稿

AI 协助了以下文件初稿：

- `schema.sql`
- `load_data.sql`
- `workload.sql`
- `benchmark_runner.sh`
- `README.md`
- `results_template.md`
- `discussion_post.md`

### 3.3 排错辅助

AI 协助分析了以下真实问题：

- `psql` 不在当前 shell 的 `PATH` 中。
- 容器内 benchmark 目录与宿主机工作区目录不是自动同步的。
- `benchmark_runner.sh` 的相对路径导致 `pgbench -f` 找不到 workload 临时文件。
- CN 与 DN 本地 `pgxc_node.node_forward_port` 不一致，导致分布表查询触发 `57P01 terminating connection due to administrator command`。

### 3.4 结果整理

AI 协助：

- 从 `pgbench.log` 与事务日志中提取 `TPS / avg / P50 / P95 / P99`。
- 总结 `distribution.log` 中的 DN 行数与倾斜指标。
- 结合 `explain.log` 整理分布式执行特征。

## 4. 人工验证内容

以下内容均由人工在真实环境中执行与确认：

### 4.1 环境与连通性

- 启动 Docker 容器和 OpenTenBase 集群。
- 验证 CN / DN / GTM 节点状态。
- 验证 `default_group` 是否存在。

### 4.2 脚本执行

- 手工执行 `setup / load / analyze / warmup / run`。
- 手工检查 `\dt`、表行数、pgbench 结果和日志目录。

### 4.3 问题修复

- 手工确认 `pgxc_node` 在 `11003 / 11006 / 11009` 三个端口上的 `node_forward_port` 值。
- 手工执行 `ALTER NODE ... WITH (FORWARD = ...)` 修复 CN 和两个 DN 本地 catalog。
- 手工重启整个集群并重新验证分布表查询。

### 4.4 结果验收

- 手工确认结果目录无 `ERROR/FATAL/aborted`。
- 手工查看 `explain.log`、`distribution.log` 与 `run_summary.tsv`。

## 5. 被拒绝的 AI 建议

### 5.1 只修 CN 的 `node_forward_port`

- AI 曾建议先在 CN 上修 `pgxc_node.node_forward_port`。
- 人工验证发现 DN 本地 `pgxc_node` 仍然为 `0`，问题未解决。
- 最终结论：必须同时修复 CN 和两个 DN 本地 catalog，然后完整重启集群。

### 5.2 未经重启直接继续压测

- 在 `node_forward_port` 修复后，如果不完整 `stop/start`，旧连接池和旧 backend 状态不会被清掉。
- 人工验证后确认必须完整重启。

## 6. 数据来源说明

本次报告中的结果来源于以下真实执行产物：

- `benchmark_results_formal_pctl_20260721_092139/run_summary.tsv`
- `benchmark_results_formal_pctl_20260721_092139/raw/*.log`
- `benchmark_results_formal_pctl_20260721_092139/raw/explain.log`
- `benchmark_results_formal_pctl_20260721_092139/raw/distribution.log`

## 7. 结论

- AI 在本次任务中主要承担“辅助设计、辅助排错、辅助整理”的角色。
- 所有最终可提交的性能结果都经过了人工执行验证。
- AI 的价值主要体现在：
  - 快速形成可复现交付物结构
  - 快速定位环境问题与脚本问题
  - 减少日志整理和报告撰写的重复劳动

## 8. 后续建议

- 后续扩展轨继续保留相同策略：AI 负责提出方案与脚本初稿，人工负责真实运行与最终验收。
- 如果补跑 `HASH/MODULO`、TPC-H、Scale-Out、Failover，仍应保持日志可追溯，不把 AI 推测值写成真实结果。
