# otb-sql-primer —— 带新手的 3 条真实 SQL（活动方向 1）

> 读者：第一次接触 OpenTenBase、看得懂 SQL 语法但不知道"这些表是干嘛的"的同学。
> 目标：把仓库里**真实存在**的 3 条 SQL 逐条讲透——查什么、为什么这么写、结果长什么样。
> 所有示例都取自本仓库 `contrib/otb-health/otb_health.sh`，逐字引用并标注行号，**没有编造的 SQL**。

## 这里有什么

| 文件 | 用途 |
| --- | --- |
| [`sql-primer.md`](./sql-primer.md) | 主交付物：3 条 SQL 的用途、逐列解释、预期结果、新手易错点 |
| [`sql/inspect-cluster.sql`](./sql/inspect-cluster.sql) | 同一批 SQL 的**带注释可运行版**，`psql -f` 直接跑 |
| [`evidence/sql-sources.md`](./evidence/sql-sources.md) | 每条 SQL、每处技术表述的来源文件与行号核对表 |
| [`evidence/ai-usage-log.md`](./evidence/ai-usage-log.md) | AI 辅助过程：AI 做了什么、人工纠正了什么 |
| [`evidence/peer-feedback.md`](./evidence/peer-feedback.md) | 试读反馈记录（模板，现场试读后填写） |

## 三条 SQL 分别是什么

| # | 查什么 | 来源行 |
| --- | --- | --- |
| 1 | `select ... from pgxc_node order by node_type` —— 集群里注册了哪些节点、各是什么角色 | `otb_health.sh:42` |
| 2 | `select * from pgxc_group` —— 节点分组，建分布表时的落点单位 | `otb_health.sh:47` |
| 3 | `create table ... distribute by shard(id) to group default_group` —— 分布式建表 | `otb_health.sh:55–57` |

第 1 条告诉你"集群有哪些节点"，第 2 条告诉你"数据能往哪组放"，第 3 条是"真把表建成分布表"。
三条连起来就是一条完整的理解链：**看见节点 → 看见分组 → 用分组建表**。

## 推荐阅读顺序

1. 先看 [CN / DN / GTM 图解卡片](../otb-viz-docs/README.md)（方向 2）——搞清三个角色的分工；
2. 再读 [`sql-primer.md`](./sql-primer.md) 的第 0 节——搞清 SQL 连的是 CN 而不是 DN；
3. 按第 1 → 2 → 3 条顺序读，读完用 `sql/inspect-cluster.sql` 在真实集群上跑一遍。

图与 SQL 是配套的：**图回答"谁在干什么"，SQL 回答"怎么把集群看清楚"**。

## 关于"预期结果"

本文档所有结果表都标了 **预期结果（未在实例上执行）**，推导依据分别注明。这是有意的：
参赛队当前没有可用的 OpenTenBase 实例，**不把推演结果伪装成实测输出**。
如果你有集群，按下面方式跑一遍即可把"预期"升级为"实测"：

```bash
# 只读检查
psql -h 127.0.0.1 -p 11003 -U opentenbase -d postgres -f sql/inspect-cluster.sql

# 原脚本（[3] 段会写数据，不想写加 --no-write）
bash contrib/otb-health/otb_health.sh --no-write
```

## 素材来源（全部可在仓库内核对）

- SQL 原文：`contrib/otb-health/otb_health.sh` 第 42、47、55–57 行
- 集群分工定义：`README_ZH.md` 第 13、15 行（`README.md` 第 16 行为对应英文表述）
- `node_type` 取值：`src/include/nodes/parsenodes.h` 第 127–132 行
- `pgxc_group` 列结构：`src/include/catalog/pgxc_group.h` 第 28、30、33、38 行

逐条核对记录见 [`evidence/sql-sources.md`](./evidence/sql-sources.md)。

## 写作约定

- 脚本里是小写、无分号（`psql -c` 调用形式）；文档给出**大写 + 分号**的规范化写法，语义等价，已在正文注明。
- "未验证"和"静态推理"一律显式标注，不混入结论。
