# OpenTenBase 基准性能测试报告

> 对应 Issue #202。发布前请替换所有 `TODO`，并附上可下载或可审阅的原始证据。

## 摘要

- OpenTenBase 版本/commit：`TODO`
- 拓扑：`TODO GTM + TODO CN + TODO DN`
- 数据规模：account `TODO`、order `TODO`、event `TODO`
- 正式矩阵：`TODO clients × TODO seconds × TODO repetitions`
- 最先出现的性能拐点：`TODO`
- 当前最有证据支持的瓶颈假设：`TODO`

## 测试环境

| 角色/机器 | 数量 | CPU | 内存 | 磁盘 | 网络 | 部署节点 |
| --- | ---: | --- | --- | --- | --- | --- |
| 客户端 | TODO | TODO | TODO | TODO | TODO | pgbench |
| 数据库主机 | TODO | TODO | TODO | TODO | TODO | TODO |

关键参数差异：

```text
TODO: shared_buffers, work_mem, max_connections, synchronous_commit, ...
```

## 方法

工具使用 `psql`、`pgbench` 和仓库 `benchmark/` 下的标准库 Python 脚本。所有连接经过 CN。
每个读 workload 先预热，再按预先固定的并发矩阵运行。结果同时采集请求层、SQL 计划、
数据库节点分布和 Linux 主机资源四层证据。

覆盖场景：

1. 单表写入；
2. 分布键简单查询；
3. 跨分片聚合；
4. 同分布、复制表和重分布 Join；
5. 短事务；
6. 并发连接扩展。

## 结果

将自动生成的 `report.md` 中“性能结果”“数据分布”“主机资源摘要”三节粘贴到这里。

TODO

至少执行三次正式运行，并补充中位数和变异系数：

| workload | clients | TPS/QPS 中位数 | 平均延迟中位数 | CV | 失败率 |
| --- | ---: | ---: | ---: | ---: | ---: |
| TODO | TODO | TODO | TODO | TODO | TODO |

## 分布式特征解释

- 分布键点查：`TODO，引用计划中的目标节点证据`
- 聚合：`TODO，引用 DN 扫描和 CN 合并证据`
- 同分布 Join：`TODO`
- 复制表 Join：`TODO`
- 重分布 Join：`TODO`
- GTM/短事务：`TODO`

## 瓶颈假设与反证

| 假设 | 支持证据 | 反对证据 | 下一次只改变一个变量的实验 | 状态 |
| --- | --- | --- | --- | --- |
| CN | TODO | TODO | 增加 CN 或降低 CN 合并工作 | 待验证 |
| DN | TODO | TODO | 增加 DN/调整并行度 | 待验证 |
| 网络 | TODO | TODO | 同机对照或更高带宽链路 | 待验证 |
| 磁盘 | TODO | TODO | 热缓存/更快介质对照 | 待验证 |
| SQL | TODO | TODO | 索引或等价改写对照 | 待验证 |
| 数据分布 | TODO | TODO | 更换分布键或重分布数据 | 待验证 |

## 改进建议

按“证据强度 × 预期收益 ÷ 实施成本”排序：

1. `TODO`
2. `TODO`
3. `TODO`

## 可复现材料

- 代码 commit/PR：`TODO`
- 去密配置：`TODO`
- `summary.csv`：`TODO`
- pgbench 原始日志和执行计划：`TODO`
- CN/DN/GTM 主机指标：`TODO`
- AI 使用策略报告：`benchmark/AI_USAGE.md`
