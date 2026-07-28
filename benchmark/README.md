# OpenTenBase 分布式基准测试

本目录为 Issue #202 提供一套可复现、可解释的基础性能测试工具。它覆盖单表写入、简单查询、
聚合查询、Join、短事务和并发连接，并把 pgbench 原始输出、执行计划、节点分布与主机资源
指标关联到同一次运行。

设计依据见 [DESIGN.md](DESIGN.md)。脚本要求 Python 3.8+，只使用标准库。

## 目录

```text
benchmark/
├── run_benchmark.py            # 预检、造数、预热、运行并发矩阵
├── analyze_results.py          # 汇总结果并生成瓶颈假设
├── aggregate_results.py        # 校验多次运行可比性并计算中位数/CV
├── collect_host_metrics.py     # 在 CN/DN/GTM 主机采集 /proc 指标
├── config.example.json         # 无密钥环境配置
├── sql/                        # 建表、造数、预检、计划和分布证据
├── workloads/                  # pgbench 自定义 workload
├── reports/                    # Discussion 发帖模板
├── results/                    # 结果表说明与空模板
└── tests/                      # 无数据库依赖的单元测试
```

## 测试对象

| workload | 分布式特征 | 主要观察点 |
| --- | --- | --- |
| `write` | 事件表按 `account_id` 分片 | 单表写入、序列和 DN 写路径 |
| `point_read` | 按分布键等值查询 | CN 定位单个 DN 的路由能力 |
| `aggregate` | 跨一段分布键聚合 | DN 并行扫描与 CN 合并 |
| `join_colocated` | 两表同按 `account_id` 分片 | DN 本地 Join |
| `join_replicated` | 分片事实表 Join 复制维表 | 小表复制策略 |
| `join_redistributed` | `account_id` 分片表 Join `order_id` 分片表 | 数据重分布代价 |
| `short_tx` | 更新与插入组成短事务 | GTM/事务协调和锁开销 |

## 1. 准备环境清单

复制配置并填写真实值，不要把密码写入文件：

```bash
cp benchmark/config.example.json benchmark/config.local.json
export PGPASSWORD='replace-me'
```

正式报告至少记录：

- OpenTenBase commit/版本和参数差异；
- 每台机器的 CPU、内存、磁盘介质、网络带宽；
- GTM、CN、DN 的数量、主备关系和物理机映射；
- 表行数、预计数据大小、客户端所在机器；
- 客户端矩阵、每个样本的预热时间和正式持续时间。

`config.local.json` 已被仓库的通用 `*.local` 规则之外管理时，请勿提交包含私有地址或账号的
版本。推荐把它放在仓库外，并通过 `--config` 指定。

## 2. 预检与造数

所有连接必须指向 CN。目标数据库、配置中的 node group 及其 sharding map 需要预先存在。
新建测试数据库后，如预检显示 sharding map 为空，请由集群管理员执行：

```sql
CREATE SHARDING GROUP TO GROUP default_group;
```

将 `default_group` 替换为配置中的 `cluster.node_group`。该命令属于集群管理操作，因此基准
脚本只检查、不自动创建。

```bash
python3 benchmark/run_benchmark.py preflight \
  --config benchmark/config.local.json

python3 benchmark/run_benchmark.py setup \
  --config benchmark/config.local.json
```

`setup` 会重建 `otb_bench` schema，请勿把正式业务数据库作为目标。预检输出包括版本、
CN/DN 拓扑、node group 和关键设置。

## 3. 同步采集主机指标

在每台物理机上运行采集器，时间需覆盖预热和正式测试。生产型结果应显式填写磁盘设备和
业务网卡，避免聚合无关设备。

```bash
python3 benchmark/collect_host_metrics.py \
  --host cn01 --role cn --duration 900 --interval 1 \
  --devices nvme0n1 --interfaces eth0 \
  --output cn01.csv
```

DN 和 GTM 分别使用 `--role dn`、`--role gtm`。将 CSV 复制到本次运行目录的
`host_metrics/` 下后重新执行 `analyze`，报告会自动使用这些证据。

## 4. 执行正式矩阵

```bash
python3 benchmark/run_benchmark.py all \
  --config benchmark/config.local.json \
  --output-dir benchmark-results/issue-202-run-01
```

`all` 依次执行：预检、重建测试 schema、造数、计划采集、分布检查、预热、正式矩阵和分析。
若已经造数，可只执行：

```bash
python3 benchmark/run_benchmark.py run \
  --config benchmark/config.local.json \
  --output-dir benchmark-results/issue-202-run-02
```

`write` 和 `short_tx` 会改变测试数据。需要比较多次独立运行时，每次都应使用 `all`，或在
`run` 前重新执行 `setup`；否则后续运行的初始状态不同，不能直接计算中位数或变异系数。

输出目录结构：

```text
issue-202-run-01/
├── environment.json
├── summary.csv
├── report.md
├── distribution.csv
├── raw/
│   ├── preflight.txt
│   ├── explain.txt
│   └── <workload>-c<clients>.txt
└── host_metrics/
```

`summary.csv` 和 `report.md` 是派生数据；`raw/` 才是可审计证据。不要只提交截图。
P50/P95/P99 来自 pgbench 事务日志抽样，抽样率由 `run.latency_sample_rate` 固定并记录在
`environment.json`；低 TPS 场景应提高抽样率，且报告必须同时给出样本数。

## 5. 单独重建报告

```bash
python3 benchmark/analyze_results.py \
  --input-dir benchmark-results/issue-202-run-01 \
  --output benchmark-results/issue-202-run-01/report.md
```

报告中的瓶颈段落是待验证假设。例如“CN CPU 高、DN CPU 低且吞吐饱和”支持 CN 假设，
但仍需结合执行计划、等待事件和复跑对照确认。

## 6. 聚合可比的重复运行

每次都用 `all` 重建相同初始数据后，可以聚合多个运行：

```bash
python3 benchmark/aggregate_results.py \
  --input-dir benchmark-results/run-01 \
  --input-dir benchmark-results/run-02 \
  --input-dir benchmark-results/run-03 \
  --output benchmark-results/aggregate.csv
```

聚合器会比较拓扑描述、连接目标、数据规模、运行矩阵、源码 commit 和工具版本。任一字段
不同或 workload/client 点不完整都会拒绝合并。输出包含 TPS 中位数、样本标准差 CV、延迟
中位数和总失败率。

## 7. 结果发布

1. 将 `results/summary_template.csv` 替换为真实运行的 `summary.csv`，或把完整结果目录作为
   PR 附件/可追溯制品。
2. 基于 `reports/discussion_template.md` 和生成的 `report.md` 填写 GitHub Discussion。
3. 在 Discussion 中同时链接原始日志、配置（去除密码）和 commit SHA。
4. 附上 [AI_USAGE.md](AI_USAGE.md)，明确 AI 做了什么、人工验证了什么。

## 验证脚本本身

```bash
python3 -m unittest discover -s benchmark/tests -v
python3 -m py_compile \
  benchmark/run_benchmark.py \
  benchmark/analyze_results.py \
  benchmark/aggregate_results.py \
  benchmark/collect_host_metrics.py
```
