# opentenbase_graph — 轻量图遍历模板

> 作者：Memsetqwq
> 版本：1.1
> 日期：2026-09-12
> 关联：犀牛鸟开源大赛 2026 — 任务三（OpenTenBase 图计算增强）
> 另见：`doc/proposals/pgvector-pg18-v0.3-graph-design.md`（设计文档）

---

## 概述

`opentenbase_graph` 是为 OpenTenBase 提供的轻量图遍历模板扩展。**纯 PL/pgSQL 实现**（无 C 代码），兼容 **PostgreSQL 10 及以上**，包括 OpenTenBase fork 内核（基于 PG10）。

扩展暴露五个函数（四个返回集合、一个布尔），操作应用层自定义的节点/边表。所有标识符参数都用正则白名单（`^[a-zA-Z_][a-zA-Z0-9_]*$`）校验，杜绝 SQL 注入。

## 安装

```sql
CREATE EXTENSION opentenbase_graph;
```

四个函数将加载到 `opentenbase_graph` schema 下。

> v1.1 新增第五个函数 `_graph_info`：图形状全局诊断（见 §5）。

## API

所有函数标记为 `STABLE`，可被内联到更大查询中。

### 1. `bfs` — 广度优先遍历

```sql
opentenbase_graph.bfs(
    nodes_table regclass,   -- 节点表（必须存在）
    edges_table regclass,   -- 边表
    src_col     text,       -- edges.src 列名
    dst_col     text,       -- edges.dst 列名
    start_id    bigint,     -- 起始节点 id
    max_depth   int DEFAULT 5
)
RETURNS TABLE(depth int, node bigint)
```

返回每个被访问节点的 `(depth, node)`，起始节点本身 depth=0。防环用 `visited[]` 数组实现（fork 内核 PG10 没有 `CYCLE` 关键字）。

**示例：**

```sql
-- 假设：nodes(id bigint), edges(src bigint, dst bigint)
SELECT * FROM opentenbase_graph.bfs('nodes', 'edges', 'src', 'dst', 1, 3);
 depth | node
-------+------
     0 |    1
     1 |    2
     1 |    3
     2 |    4
     2 |    5
     ...
```

### 2. `shortest_path` — 无权最短路径

```sql
opentenbase_graph.shortest_path(
    nodes_table regclass,
    edges_table regclass,
    src_col     text,
    dst_col     text,
    start_id    bigint,
    end_id      bigint,
    max_depth   int DEFAULT 1000
)
RETURNS TABLE(depth int, path bigint[])
```

返回跳数 `depth` 和完整路径 `path bigint[]`。**找不到路径**返回 0 行。

**示例：**

```sql
SELECT * FROM opentenbase_graph.shortest_path('nodes', 'edges', 'src', 'dst', 1, 5, 100);
 depth |     path
-------+---------------
     4 | {1,2,3,4,5}
```

### 3. `degree` — 入度/出度

```sql
opentenbase_graph.degree(
    edges_table regclass,
    src_col     text,
    dst_col     text,
    node_id     bigint
)
RETURNS TABLE(in_degree bigint, out_degree bigint)
```

返回 `dst = node_id` 的边数（入度）和 `src = node_id` 的边数（出度）。

**示例：**

```sql
SELECT * FROM opentenbase_graph.degree('edges', 'src', 'dst', 4);
 in_degree | out_degree
-----------+------------
         2 |          1
```

### 4. `reachable` — 布尔可达性判断

```sql
opentenbase_graph.reachable(
    nodes_table regclass,
    edges_table regclass,
    src_col     text,
    dst_col     text,
    start_id    bigint,
    end_id      bigint,
    max_depth   int DEFAULT 1000
)
RETURNS bool
```

只需要"是否可达"答案时比 `shortest_path` 更便宜。

**示例：**

```sql
SELECT opentenbase_graph.reachable('nodes', 'edges', 'src', 'dst', 1, 5, 100) AS 能到;
 能到
------
 t
```

### 5. `_graph_info` — 图形状全局诊断（v1.1+）

```sql
opentenbase_graph._graph_info(
    edges_table regclass,
    src_col     text,
    dst_col     text
)
RETURNS TABLE(
    node_count        bigint,
    edge_count        bigint,
    max_in_degree     bigint,
    max_out_degree    bigint,
    max_total_degree  bigint,
    density           double precision
)
```

返回单行结果，汇总边表的全局形状。用于"查询慢"调试或决定"数据集是否适合 v1.0 模板 vs 需要 AGE"。

- `node_count` = 在 `src_col` 或 `dst_col` 中出现过的去重节点数
- `edge_count` = 边表行数
- `max_in_degree` / `max_out_degree` = 按 `dst|src` 分组后 `count(*)` 的最大值
- `max_total_degree` = 所有节点 `in_degree + out_degree` 的最大值
- `density` = 有向简单图的 `edge_count / (node_count * (node_count - 1))`；`node_count < 2` 时为 `0`

**示例：**

```sql
SELECT * FROM opentenbase_graph._graph_info('edges', 'src', 'dst');
 node_count | edge_count | max_in_degree | max_out_degree | max_total_degree |      density
------------+------------+---------------+----------------+------------------+--------------------
          7 |         12 |             2 |              2 |                4 | 0.2857142857142857
```

## 表结构约定

本扩展**不**自动建表——表结构由应用层拥有。最小约定：

```sql
CREATE TABLE my_nodes (id bigserial PRIMARY KEY, props jsonb);
CREATE TABLE my_edges (src bigint REFERENCES my_nodes,
                       dst bigint REFERENCES my_nodes,
                       weight float8 DEFAULT 1.0);
```

列名 `src` / `dst` 不强制——通过 `src_col` / `dst_col` 参数可指定任意列名。

## 限制

- 适合**中小规模图**（< 100k 节点，深度 ≤ 10）。超大规模图建议用 Apache AGE（backport 完成后）或专用图数据库。
- 函数通过 `EXECUTE` 执行动态 SQL 以支持任意表/列名，牺牲了部分 planner 优化机会。
- 防环用 `visited[]` 数组，每行 O(depth) 内存开销。
- **不**支持带权最短路径（`weight` 列被忽略；如需 Dijkstra，请另外实现）。
- **不**支持分布式：所有数据必须在同一 database/schema。跨节点图遍历属 OpenTenBase 架构演进范畴，不在本扩展范围内。

## 安全性

所有标识符参数通过正则 `^[a-zA-Z_][a-zA-Z0-9_]*$` 白名单校验。通过表名/列名参数 SQL 注入**不可能**。

## 许可证

与 OpenTenBase 同许可证（见顶层 `LICENSE`）。

## 作者

Memsetqwq — 犀牛鸟开源大赛 2026 参赛作品