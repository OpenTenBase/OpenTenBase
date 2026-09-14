# OpenTenBase 图计算增强 — v0.1 设计文档（任务三）

> 作者：Memsetqwq
> 版本：v0.1（草案）
> 日期：2026-09-10
> 关联：本任务为犀牛鸟开源大赛「OpenTenBase 多模态分析开发挑战赛」任务三，聚焦**图计算能力增强**。
> 与同仓库 `pgvector-pg18-v0.1-design.md`（任务一）配套阅读。

---

## 1. 背景与目标

### 1.1 背景

OpenTenBase 作为企业级分布式 HTAP 数据库，在 AI 与大模型时代面临"多模态融合"诉求：

- **关系数据**：OpenTenBase 核心引擎
- **向量数据**：pgvector（任务一已覆盖）
- **图数据**：当前 fork 内核**无任何图计算扩展**
- **时序/全文/空间/KV**：分别由 TimescaleDB / tsvector / PostGIS / 原生 KV 承担

2025 年 OpenTenBase 多模态分析开发挑战赛已把"多模态插件增强"列为四大赛道之一，明确要求适配 PostgreSQL 生态的 Apache AGE / TimescaleDB 等插件至 OpenTenBase 分布式架构（[来源](https://www.opentenbase.org/news/news-post-38)）。2026-07 北京城市行现场，北京盛舒科技魏波公开演示了 **OpenTenBase + pgvector + Apache AGE + TimescaleDB + PostGIS 七模态单引擎**架构（[来源](https://caijing.chinadaily.com.cn/a/202607/14/WS6a55e0dca310d709c2fbd6d6.html)）。

但**当前 fork contrib 目录里没有 AGE 代码**。本次任务要把这条路打通。

### 1.2 目标

- **v0.1（本文档）**：方案选型 + 路径规划，输出可执行的实施路线图
- **v0.2（下一个 PR）**：在 fork 上落地**最轻量**的图遍历能力（立即可用，零依赖）
- **v0.3**：根据 v0.2 反馈决定是否引入 Apache AGE

### 1.3 非目标

- 不重写 OpenTenBase 内核（fork 是 PG10 内核，重大升级超出本次任务范围）
- 不与 Neo4j / TigerGraph 独立图数据库拼全功能
- 不做分布式图计算（图分片、跨 DN 边遍历属 OpenTenBase 自身架构演进，超出本次任务范围）

---

## 2. 现状分析

### 2.1 OpenTenBase fork 内核现状

```
PACKAGE_VERSION = "10.0 @ OpenTenBase_v$OPENTENBASE_VERSION"  (configure)
```

fork 内核基于 **PostgreSQL 10.x**。本次 PR 期间探查到的 PG API 缺失示例：

| API | PG 版本要求 | fork 状态 |
|-----|-----------|---------|
| `InitMaterializedSRF` | PG 12+ | ❌ 缺失（任务一 SRF 函数受影响） |
| `MAT_SRF_BLESS` | PG 12+ | ❌ 缺失 |
| `CYCLE` 关键字（递归 CTE） | PG 14+ | ❌ 缺失 |
| `MERGE ... ON CREATE/MATCH` | AGE 1.7.0 (PG18) | ❌ 不可用 |

这意味着：

- **任何 PG12+ 才有的图相关 API，fork 上都不能直接用**
- AGE 1.7.0（PG18）/ 1.6.0（PG14-17）**不能直接 backport**
- AGE 1.5.0（PG11-13）是 fork 内核唯一**理论上**能跑的版本（PG11 几乎兼容 PG10 的大多数 API）

### 2.2 OpenTenBase fork contrib 现状

`contrib/` 现有扩展盘点（grep + ls 结果，节选）：

| 类别 | 扩展 | 是否与图相关 |
|------|------|------------|
| 文本/全文 | `pg_trgm`、`fuzzystrmatch` | 否 |
| 类型 | `citext`、`hstore`、`cube` | 否 |
| 索引 | `btree_gin`、`btree_gist` | 否（GiST 是 ltree/AGE 依赖） |
| **层级树** | **`ltree`** ✅ | **是（最轻量图遍历）** |
| FDW | `dblink`、`file_fdw` | 否 |
| 工具 | `adminpack`、`auto_explain`、`oid2name` | 否 |
| 监控 | `audit_test`、`auth_delay` | 否 |
| OpenTenBase 自研 | `opentenbase_ai`、`opentenbase_ctl`、`opentenbase_ora_package_function` | 否 |

**关键发现**：`ltree` 已经在 contrib 里！这是 PostgreSQL 官方的层级树路径类型（`ltree` 数据类型 + GiST 索引），可以**立即**承担"树状层级"场景。

### 2.3 PostgreSQL 上游对图计算的官方态度

2025 年 Hacker News 上对 PostgreSQL 19 feature roadmap 的讨论中，PG 核心开发者 Andres Freund 在邮件列表明确表态（[来源](https://news.ycombinator.com/item?id=44702692)）：

> "None of the people that work on postgres have any interest in adding graph db style stuff."
>
> "There is no proposal or roadmap for adding graph database features."

**结论**：
- PG19 不会原生加图能力
- PG 核心团队**故意**把图能力留给 extension 生态
- 任何在 OpenTenBase 上做图计算的方案，**必须**走"集成第三方扩展"路线

### 2.4 Apache AGE 当前状态

[Apache AGE](https://age.apache.org/) 现状（截至 2026-09）：

| PG 主版本 | 最新 AGE | 发布时间 |
|----------|---------|---------|
| **PG 18** | **1.7.0** | 2026-01（RC0） |
| PG 17 | 1.6.0 | 2025-09 |
| PG 16 / 15 / 14 | 1.6.0 | 2025-09 |
| **PG 13 / 12 / 11** | **1.5.0** | 2024-01（冻结） |
| **PG 10** | ❌ 无 | — |

2025-10 Apache AGE 正式升为 **Apache 顶级项目（TLP）**（[来源](https://www.postgresql.org/about/news/apache-age-reaches-top-level-status-and-adds-postgres-17-support-2948/)）。

**AGE 1.7.0 关键能力**：openCypher 查询、shortest_path / all_shortest_paths、`MERGE ... ON CREATE/ON MATCH`、变量长度边性能优化。

**结论**：
- AGE 1.5.0 是 fork 内核（≈ PG10/11）唯一**理论上**可移植的版本
- 但 AGE 1.5.0 已**冻结**，无新功能（缺 `CYCLE` 关键字、缺 `MERGE ON CREATE/MATCH`）
- 想用 AGE 全功能，必须先把 fork 内核至少升到 PG14+

---

## 3. 候选方案对比

### 方案 A：递归 CTE + ltree 模板（最轻量）

**思路**：fork 已有 `ltree` 扩展。利用 ltree 的层级路径索引 + 递归 CTE 写一组**开箱即用的图遍历模板函数**。

```sql
-- 节点表
CREATE TABLE nodes (id bigserial PRIMARY KEY, label text);

-- 边表（用 bigint id 比 regclass 更通用）
CREATE TABLE edges (
    src bigint REFERENCES nodes,
    dst bigint REFERENCES nodes,
    weight double precision DEFAULT 1.0
);

-- 开箱即用的 BFS 模板（递归 CTE，PG10 兼容）
CREATE FUNCTION bfs(start_id bigint, max_depth int DEFAULT 5)
RETURNS TABLE(depth int, node_id bigint, path bigint[])
AS $$
    WITH RECURSIVE walk(node, depth, path, visited) AS (
        SELECT start_id, 0, ARRAY[start_id], ARRAY[start_id]
        UNION ALL
        SELECT e.dst, w.depth + 1, w.path || e.dst, w.visited || e.dst
        FROM walk w JOIN edges e ON e.src = w.node
        WHERE w.depth < max_depth
          AND NOT (e.dst = ANY(w.visited))   -- PG10 没 CYCLE 关键字，手写防环
    )
    SELECT depth, node, path FROM walk;
$$ LANGUAGE sql STABLE;
```

**优点**：
- ✅ 零依赖（fork 已有 ltree + 递归 CTE 都是 PG10 标配）
- ✅ 5 分钟上手，纯 SQL，SQL 工程师不需要学 Cypher
- ✅ 与 OpenTenBase 分布式架构兼容（节点/边表天然可分片）
- ✅ 可立即在 fork 上 PR 落地

**缺点**：
- ❌ 没有 openCypher 标准化查询语言
- ❌ 防环要手写（PG10 缺 `CYCLE` 关键字）
- ❌ 大图遍历性能远不如 AGE（无 Cypher-to-plan 优化）

**适用场景**：中小规模图（< 100k 节点、深度 ≤ 10）、BI 报表、推荐召回。

### 方案 B：backport Apache AGE 1.5.0 到 fork

**思路**：把 Apache AGE 1.5.0（最后支持 PG11-13 的版本）源代码移植到 fork 内核（≈ PG10）。

**优点**：
- ✅ 用户拿到 openCypher 标准查询语言
- ✅ 现成的最短路径、Cypher 解析器、图存储引擎
- ✅ 与 PostgreSQL 生态兼容（PG 文档、教程都适用）

**缺点**：
- ❌ AGE 1.5.0 已冻结，缺 `CYCLE`、缺 `MERGE ON CREATE/MATCH`
- ❌ backport 工作量大：AGE 用大量 PG12+ API（`InitMaterializedSRF`、`dshash`、`pgstat` 等），需要逐个写 shim
- ❌ OpenTenBase 分布式架构（CN/DN/GTM）下，AGE 的单进程图遍历可能不直接 work，需要分布式改造
- ❌ 后续升级 AGE 版本需要重新做 shim 工作

**适用场景**：需要 openCypher 标准化、需要最短路径等 AGE 特有功能的中大规模图。

### 方案 C：内核升级到 PG14+ 后引入 AGE 1.6+

**思路**：把 OpenTenBase fork 内核至少升级到 PostgreSQL 14，然后直接引入 AGE 1.6.0。

**优点**：
- ✅ AGE 全功能可用（1.6+ 有 `CYCLE` 关键字、`MERGE ON CREATE/MATCH`）
- ✅ 长期可持续升级到 AGE 1.7+（PG18）
- ✅ 同时获得 PG14+ 全部改进（性能、新 SQL 语法、安全修复）

**缺点**：
- ❌ **内核升级是大工程**：OpenTenBase 在 PG10 上做了大量分布式改造（GTM、CN/DN、XC 协议），PG14+ 的存储/复制/WAL 协议多有变化，逐个适配工作量极大
- ❌ 影响范围广：fork 上百个 contrib 扩展都可能要适配
- ❌ 时间成本：估计 6-12 个月起，超出犀牛鸟赛期（数周）

**适用场景**：OpenTenBase 团队层面的长期战略，不在犀牛鸟任务范围内。

### 方案 D：自研轻量图遍历引擎（PL/pgSQL + C 扩展）

**思路**：fork 上自研一套类 Cypher 的图遍历引擎，编译为 C 扩展或 PL/pgSQL。

**优点**：
- ✅ 完全可控
- ✅ 可针对 OpenTenBase 分布式架构优化

**缺点**：
- ❌ 工作量最大，与"小步快跑"的犀牛鸟赛制冲突
- ❌ 自研引擎难以与外部生态（cypher 标准、NetworkX 等）兼容

**结论**：不推荐。

---

## 4. 推荐方案：**A + B 双轨**

**主路径：方案 A 立即落地（v0.2）**

- fork 已有 `ltree`，几乎零成本上线
- PR 短小精悍（预估 1-2 个 commit，新增 1-2 个 SQL 文件 + 模板函数）
- 用户**今天**就能在 OpenTenBase 上跑图查询

**预研路径：方案 B 启动技术调研（v0.3 候选）**

- 启动 AGE 1.5.0 backport 的**预研**，不进入正式编码
- 输出"AGE 1.5.0 → fork 内核 API 差异表" + "OpenTenBase 分布式改造影响范围评估"
- 由 OpenTenBase 团队评审是否纳入下一个开发周期

**不采纳方案 D**：自研引擎与赛制节奏不匹配。

---

## 5. 实施计划

### v0.1（本文档） — 已完成

| 项 | 状态 | 产出 |
|---|------|------|
| 方案选型 | ✅ | 推荐 A+B 双轨 |
| 内核 API 差异盘点 | ✅ | §2.1 表格 |
| AGE 现状调研 | ✅ | §2.4 表格 |
| fork contrib 盘点 | ✅ | §2.2 表格 |

### v0.2（下一个 PR，预计 2 周）— 方案 A 落地

| 项 | 状态 | 备注 |
|---|------|------|
| `contrib/opentenbase_graph/sql/` 新建 | 📋 计划 | 节点/边表模板 + BFS/DFS/最短路径函数 |
| `contrib/opentenbase_graph/expected/` 新建 | 📋 计划 | TAP 测试 expected |
| 文档：`doc/opentenbase_graph.md` | 📋 计划 | 用法示例 + 限制说明 |
| 中文文档：`doc/opentenbase_graph_zh.md` | 📋 计划 | 同上中文版 |
| TAP 全绿 | 📋 计划 | 沿用 fork 的 `make installcheck` 框架 |
| fork 内核编译通过 | 📋 计划 | 验证 PG10 API 兼容 |

**v0.2 设计要点**：

```sql
-- 节点表（应用层任意创建）
CREATE TABLE my_nodes (id bigserial PRIMARY KEY, props jsonb);
-- 边表（应用层任意创建）
CREATE TABLE my_edges (src bigint, dst bigint, weight float8);

-- 用 PL/pgSQL 模板函数包：
CREATE FUNCTION opentenbase_graph.bfs(nodes regclass, edges regclass, src_col text, dst_col text,
                                       start_id anyelement, max_depth int DEFAULT 5)
RETURNS TABLE(depth int, node anyelement, path anyarray);
CREATE FUNCTION opentenbase_graph.shortest_path(nodes regclass, edges regclass, ...);
CREATE FUNCTION opentenbase_graph.degree(nodes regclass, edges regclass, ...);
```

**限制声明**（写入 README）：
- 仅适合中小规模图（< 100k 节点）
- 无并发图遍历优化
- 复杂查询性能不如 AGE
- 升级到 v0.3（AGE 集成）后，本模板作为"轻量备选 API"长期保留

### v0.3（候选，需评审）— 方案 B 启动 backport 调研

| 项 | 状态 | 备注 |
|---|------|------|
| AGE 1.5.0 → fork API 差异表 | 📋 调研 | 输出 markdown 报告 |
| AGE 在 OpenTenBase CN/DN 上的改造方案 | 📋 设计 | 分布式图分片策略 |
| OpenTenBase 团队评审 | 📋 阻塞 | 需团队确认是否纳入路线图 |
| AGE 集成 PoC（如批准） | 📋 待定 | 工作量评估 1-3 个月 |

---

## 6. 风险与回滚

| 风险 | 影响 | 回滚方案 |
|------|------|---------|
| v0.2 PL/pgSQL 函数性能差 | 低 | 不影响内核，单独 contrib，回滚 PR 即可 |
| v0.2 与 fork 现有 ltree 冲突 | 中 | 检查 fork ltree 版本，独立目录命名 |
| v0.3 AGE backport 工作量过大 | 高 | 终止 backport，保留 A 方案 |
| 用户误用 v0.2 模板跑超大规模图 | 低 | README 明确写"中小规模"限制 |
| OpenTenBase 分布式架构下边遍历出错 | 中 | v0.2 不涉及分布式，限定单 DN；分布式问题留给 v0.3 评估 |

每个 PR 单独可回滚，不影响任务一（pgvector）PR。

---

## 7. 验收标准

### v0.2 方案 A 落地

- ✅ TAP 测试全绿（新增 graph 测试套件）
- ✅ fork 主分支 `make installcheck` 通过
- ✅ 文档中英双版同步（参考 `feedback_sync_all_fork_files.md`）
- ✅ 性能基线：10k 节点 / 50k 边 / 深度 5 的 BFS < 100ms（单 DN）

### v0.3 方案 B 调研（如启动）

- ✅ 输出 AGE 1.5.0 → fork API 差异表（≥ 30 条 API 适配点）
- ✅ OpenTenBase 分布式图分片策略 PoC 报告
- ✅ OpenTenBase 团队评审通过后方可进入编码阶段

---

## 8. 参考

### 8.1 OpenTenBase 多模态战略

- [OpenTenBase 30 万奖金池 多模态分析开发挑战赛开启](https://www.opentenbase.org/news/news-post-38) — 官方明确把 AGE 列为推荐扩展
- [OpenTenBase 城市行北京站：当数据库内核遇见 AI 智能体](https://caijing.chinadaily.com.cn/a/202607/14/WS6a55e0dca310d709c2fbd6d6.html) — 魏波公开演示七模态架构

### 8.2 PostgreSQL 上游

- [PostgreSQL 19 features (HN 讨论)](https://news.ycombinator.com/item?id=44702692) — Andres Freund 明确表态不加图
- [PostgreSQL 官方 ltree 文档](https://www.postgresql.org/docs/current/ltree.html)
- [PostgreSQL 递归 CTE 文档](https://www.postgresql.org/docs/current/queries-with.html)

### 8.3 Apache AGE

- [Apache AGE 官网](https://age.apache.org/) — 官方下载与状态
- [Apache AGE 升为 TLP 公告](https://www.postgresql.org/about/news/apache-age-reaches-top-level-status-and-adds-postgres-17-support-2948/)
- [Apache AGE GitHub](https://github.com/apache/age) — 源代码与 issue tracker
- [Apache AGE 性能最佳实践](https://learn.microsoft.com/zh-hk/azure/postgresql/azure-ai/generative-ai-age-performance) — Microsoft Azure 团队实战

### 8.4 内部参考

- `doc/proposals/pgvector-pg18-v0.1-design.md` — 任务一设计文档
- `contrib/ltree/` — fork 内已有 ltree 扩展源码
- `PROGRESS.md` — 本地同步进度（gitignored）

---

*最后更新：2026-09-10*