---
name: pg-graph-search
description: 操作与扩展 pg_graph_search（PostgreSQL 属性图搜索扩展）的实战手册。覆盖元函数 install/plan/advise/uninstall、生成的 5 个图搜索函数（N 度搜索 / 游标分页 / 最短路径 BFS / 子图 JSON / 分层统计）、spec 参数、性能量级与踩坑清单，以及 sql/ 模板改动后的回归验证闭环。Use when 需要在 PostgreSQL 边表上做 N 度关系搜索、两点最短路径、子图 JSON 绘图数据，想把手写 WITH RECURSIVE 换成可安装函数，或要修改本扩展的 SQL 模板与测试。
---

# pg_graph_search 操作手册

项目位置：`OpenTenBase/contrib/pg_graph_search`（OpenTenBase 源码树内）

一句话：**元函数按你的边表结构生成业务函数**。不再手写那 200 行 `WITH RECURSIVE`，
一次 `install()` 得到 1 张统一边视图 + 5 个可直接调用的函数 + 1 条 registry 记录。

本文只写"怎么用、怎么验、哪里会踩坑"。设计动机与原文对比看 `README.md`，
性能数字看 README 第八节。

---

## 0. 10 秒判断该不该用它

| 你的场景 | 结论 |
|---|---|
| 边就存在一张 PG 表里（`c1` / `c2` / `prop`），只差查询函数 | ✅ 直接 `install()`，5 分钟内出结果 |
| 折线路径、好友 N 度、组织上下级、调用链、依赖扩散 | ✅ 正合适 |
| 要在前端画子图，需要 nodes + edges JSON | ✅ `_subgraph()` 一次到位 |
| 只有几行数据、写一次性的递归 CTE 就够 | ❌ 直接用 CTE，别加依赖 |
| 最难路径、加权最短路（Dijkstra / A*） | ❌ 本扩展的最短路径是**按跳数** BFS，不是加权最短路 |
| 十亿边以上、超内存深遍历 | ❌ 换 Apache AGE / 专用图引擎（README 8.5 有诚实清单） |

---

## 1. 5 分钟上手（以下命令在 PG 18.6 实测通过）

```bash
cd contrib/pg_graph_search      # 相对 OpenTenBase 源码根
make                 # 由 sql/ 片段拼出 pg_graph_search--1.0.0.sql（含 DISTRIBUTE BY REPLICATION）
make install         # 装到 $(DESTDIR)$(pg_config --sharedir)/extension
psql -c "CREATE EXTENSION pg_graph_search;"
```

原生 PostgreSQL 上做功能验证用可移植版（分布子句与 `sql_mode` 都去掉）：

```bash
make portable        # → pg_graph_search--1.0.0-portable.sql + pg_graph_search-portable.control
```

```sql
-- 1) 先体检（只读，不落库）：无向图缺反向索引会退化 2~3 个数量级
SELECT unnest(graph_search.advise(
  '{"table":"public.t_edge","directed":false}'::jsonb));

-- 2) 再预览完整 DDL（只读，返回 11 条语句：1 视图 + 5 函数 + 5 COMMENT）
SELECT unnest(graph_search.plan(
  '{"table":"public.t_edge","target_schema":"graph","prop":"prop"}'::jsonb));

-- 3) 最后安装（写；execute plan() 的全部 DDL 并登记 registry）
SELECT graph_search.install(
  '{"table":"public.t_edge","prop":"prop","target_schema":"graph","prefix":"user"}'::jsonb);

-- 4) 调用（生成物在 target_schema，不在 search_path，务必 schema 限定）
SELECT * FROM graph.user_search(1, 3, 100, 0);          -- N 度搜索
SELECT * FROM graph.user_shortest_path(1, 11, 10);      -- 最短路径
SELECT graph.user_subgraph(1, 2, 100, 0);               -- 绘图 JSON
SELECT * FROM graph.user_stats(1, 3, 100, 0);           -- 分层扇出
```

前置条件：`target_schema` **必须已存在**（`CREATE SCHEMA graph;`），扩展只创建自己的
`graph_search` schema，不替你建业务 schema。PG 12+，纯 SQL 扩展，无编译产物。

---

## 2. 元函数链：规划与执行分离

| 函数 | 性质 | 作用 |
|---|---|---|
| `graph_search.advise(spec)` | 只读 | 安装前体检，返回建议文本数组；有向图返回 0 行 |
| `graph_search.plan(spec)` | 只读 | 返回**将要执行**的 DDL 文本数组，可审计、可 `psql -f` 人工执行 |
| `graph_search.install(spec, p_replace => true)` | 写 | 执行 `plan()` 的 DDL + 写 registry；同名实例默认先卸后装 |
| `graph_search.list_installed()` | 只读 | 列已安装实例（含 api 列，可直接抄函数名） |
| `graph_search.uninstall(schema, prefix, cascade => false)` | 写 | 按依赖反序 DROP，返回实际执行的 DDL 文本 |

`plan()` 与 `install()` 分离是有意的：**落库前先看一眼生成代码**。
排错时先跑 `plan()`，能复现大部分"安装失败"问题且完全无副作用。

```sql
SELECT * FROM graph_search.list_installed();               -- 谁装过、装在哪个 schema
SELECT graph_search.uninstall('graph', 'user');            -- 卸载并清 registry
```

---

## 3. 生成的 5 个函数（精确签名）

设 `node_type` 为生成时的点类型（`int` / `bigint` / `smallint` / `text` / `uuid` / `numeric`，
由 `c1` 列类型推导，`varchar`/`bpchar`/`name` 统一映射成 `text`）。

| 函数 | 签名 | 返回 |
|---|---|---|
| `<prefix>_search` | `(root node_type, depth int = 3, limit bigint = 100, weight float8 = 0)` | `TABLE(o_path node_type[], o_point1, o_point2, o_link_prop jsonb, o_depth int)` |
| `<prefix>_cursor` | `(root node_type, curname name, depth int = 3, limit bigint = 100, weight float8 = 0)` | `refcursor` |
| `<prefix>_shortest_path` | `(p1 node_type, p2 node_type, depth int = 3, limit bigint = 2000000000, weight float8 = 0)` | `TABLE(o_path node_type[], o_depth int, o_link_props jsonb[])` |
| `<prefix>_subgraph` | `(root node_type, depth int = 3, limit bigint = 100, weight float8 = 0)` | `jsonb` |
| `<prefix>_stats` | `(root node_type, depth int = 3, limit bigint = 100, weight float8 = 0)` | `TABLE(o_depth int, o_edges bigint, o_new_nodes bigint, o_avg_path_len numeric)` |

语义要点（踩过坑的地方）：

* **`limit` 是"每层每分支"的上限**，不是总行数。3 度 × 每层 100 最多能回约 300 行量级，
  但扇出大时会显著更多，别拿它当分页。
* **`weight` 是边权阈值过滤（`e.w >= weight`），不是累积代价**。
  `weight => 0.9` = 只看权重 ≥ 0.9 的边。
* **`_search` 每行是一条"根到某点的路径 + 这条边的属性"**，不是一行一个节点。
  想按点去重自己 `DISTINCT`。
* **`_shortest_path` 按跳数最短（BFS）**，同层入边按 `w DESC` 取一条以保证确定性。
  返回含起终点的完整路径；`o_link_props[i]` 是第 i 跳边的 `prop`（已完成与跳数对齐的修复）。
  - 不可达返回 **0 行**；`p1 = p2` 返回 `{p1}`、`depth = 0`。
* **`_subgraph` JSON 结构**：`{root, depth, node_count, edge_count, nodes:[{id,depth}], edges:[{source,target,depth,prop}]}`，
  直接喂 d3 / Cytoscape。
* **`_cursor` 只用于 `BEGIN ... FETCH ... CLOSE`**，事务外 FETCH 会失败。
* 生成函数是 `STABLE` 且内部走动态 SQL：每次调用有一次 SQL 解析开销（实测约 4 ms/次）。

---

## 4. spec 参数

| 键 | 默认 | 说明 |
|---|---|---|
| `table` | **必填** | 边表，`schema.table` 或裸表名；统一渲染成 schema 限定名 |
| `c1` / `c2` | `c1` / `c2` | 两个点列名，不存在直接报错 |
| `prop` | 无 | 边属性列（jsonb）；省略则视图里 `prop` 为 NULL、`w` 恒为 0 |
| `weight_key` | `weight` | 从 `prop` 取权重的键名 |
| `weight_expr` | 无 | 权重表达式**原文**，受字符白名单校验（挡 `;` / `--` / `/*`） |
| `directed` | `true` | `false` 时视图 `UNION ALL` 双向展开 |
| `target_schema` | `public` | 生成对象所在 schema，必须已存在，必须当前用户有权限 |
| `prefix` | `g_<表名>` | 函数名前缀，自动净化成合法标识符 |
| `node_type` | 按 `c1` 推导 | 见第 3 节支持列表 |
| `max_depth_cap` | `64` | **安装时固化**的运行时深度硬上限 |

`max_depth_cap` 是安装期属性：改它必须重装实例（`install` 默认 `p_replace => true`
会先自动卸载旧实例，函数签名变化也不会残留）。

---

## 5. 性能量级（决定你怎么用）

实测环境：PG 18.6，8 vCPU / 15 GB，**952,476 条边 / 88 MB**（20,000 点 × 50 出边）。
详细数字见 README 第八节，这里只留三条会影响你设计的结论：

1. **无向图必须建反向索引**。无 `(c2, c1)` 索引时反向分支退化成 Parallel Seq Scan：
   单次 `src=1` 查询 30.6 ms vs 0.05 ms，**600 倍**。装无向实例前先跑 `advise()`。
2. **最短路径的可达性决定性能量级**。可达（2 跳内）TPS 6,311，随机两点（多数不可达）TPS 62，
   **100 倍差距**——BFS 确认不可达必须扫完整个连通分量，这是算法本质。
   实践做法：应用侧做连通分量预判、把不可达当**可缓存的负结果**。
3. **无向搜索本身约 9 倍于有向**（`UNION ALL` 双分支），能建有向就别用无向。

其他基线：3 度搜索（每层限 100，返回 249 条路径）19.4 ms，其中 UDF 包装约 4 ms；
子图 JSON 3.3 ms；分层统计 2.6 ms。

---

## 6. 踩坑清单 / 报错对照

| 症状 | 原因 | 处理 |
|---|---|---|
| `target schema "x" 不存在` | 没建目标 schema | `CREATE SCHEMA x;` |
| `spec.table 必填` | spec 是 `{}` 或缺 table | 至少给 `{"table":"public.e"}` |
| `列 "c" 在表 x 上不存在` | `c1`/`c2`/`prop` 名字不对 | 显式传列名，或用 `\d` 确认 |
| `i_depth N 超过本实例安装时的 max_depth_cap=64` | 运行时深度超过安装期上限 | 重装并调大 `max_depth_cap` |
| `function g.user_search(...) does not exist` | 函数名/签名猜错 | `SELECT * FROM graph_search.list_installed()` 看 api 列 |
| `relation "g.user_edge" does not exist` | 生成物没在 search_path | 一律 schema 限定调用 |
| 无向图查询突然变慢 | 缺 `(c2, c1)` 反向索引 | `advise()` → 按其提示 `CREATE INDEX` |
| 最短路径偶发 200 ms+ | 目标不可达，扫完整连通分量 | 连通分量预判 + 负结果缓存 |
| `DROP SCHEMA ... CASCADE` 后 registry 仍有记录 | 级联删除绕过 uninstall | 手工 `DELETE FROM graph_search.registry WHERE target_schema='...'` |
| 覆盖安装报函数已存在/签名不符 | 旧实例残留 | `install(..., p_replace => true)`（默认即 true），或先 `uninstall()` |
| `schema "ag_catalog" does not exist`（跑回归时） | 见第 7 节的环境陷阱 | 在已 `CREATE EXTENSION age` 的库里跑测试 |

---

## 7. 测试与验证闭环

```bash
python3 test/lint_sql.py                # 1) SQL 模板静态门禁（4 类高危问题，无数据库）
psql -d <db> -f test/01_setup.sql       # 2) 造 12 点小图 + 装有向/无向实例
psql -d <db> -f test/02_correctness.sql # 3) 15 项正确性（第 14/15 项的 ERROR 是预期抛错）
psql -d <db> -f test/03_regress.sql     # 4) 18 项断言式回归
bash test/verify_all.sh                 # 全链路：make → 门禁 → 建临时库 → 装载 → 2/3/4 → 分布策略
```

`verify_all.sh` 自动识别平台：原生 PG 上直接 `\i` 可移植版 SQL；
OpenTenBase 上要求先 `make install`（全节点）再 `CREATE EXTENSION`。
它只建一个临时库（`DBNAME=gs_verify`），验证完 `DROP`，不碰系统已装扩展；
`KEEP_DB=1` 保留，`SKIP_MAKE=1` 跳过构建。

分布式补充用例（集群上才跑）：

```bash
psql -d <db> -f test/opentenbase/01_setup_distributed.sql  # 复制表/哈希表 × 有向/无向对照
psql -d <db> -f test/opentenbase/02_verify_nodes.sql       # EXECUTE DIRECT 看 DN 侧对象
```

**环境陷阱（本机实测）**：本机 `shared_preload_libraries = age, pg_cron, pg_stat_statements`。
在"age 已预载但当前库没有 `CREATE EXTENSION age`"的库里，`test/03_regress.sql` 的 R10
会在 `TRUNCATE` 临时表时报 `schema "ag_catalog" does not exist`（AGE 的钩子介入）。
这不是扩展的问题：**测试库先 `CREATE EXTENSION age;`**，或换一个没预载 age 的实例。

---

## 8. 改扩展本身的流程（改 `sql/` 模板）

1. `sql/` 六个片段按文件名拼接：`00_header → 10_assert → 20_norm → 30_ddl_basic → 31_ddl_shortest → 40_meta`。
2. **只改片段，别改 `pg_graph_search--1.0.0.sql`**（它是 `make sql` 的产物，交付物）。
3. 改完必跑 `make sql` + `python3 test/lint_sql.py`；静态门禁抓到过真实 bug
   （子查询投影缺 `AS` 别名 → 运行期 `column t.depth does not exist`）。
4. 生成的 PL/pgSQL 是**多层 dollar-quoting**，嵌套时按约定选标记，避免撞名：

   | 标记 | 用途 |
   |---|---|
   | `$pga$` | 本扩展函数的函数体 |
   | `$pgq$` | `format()` 模板串（内含被生成的 DDL 文本） |
   | `$pgg$` | 被生成的 plpgsql 函数体 |
   | `$pgs$` | 被生成函数体内部的动态 SQL 串 |

5. 标识符一律 `format('%I')`、常量一律 `format('%L')`；`weight_expr` 是唯一原文拼接点，
   必须走白名单校验。
6. **改行为就补断言**：新增/修改用例加在 `test/03_regress.sql`（断言失败即非零退出），
   人工可推演的期望值放 `test/02_correctness.sql`；同时更新 `verify_all.sh` 里的项数文案。
7. 模板改动后先在临时库验证（下面第 9 节），确认 18/18 再考虑 `make install`。
8. **只在 OpenTenBase 生效的片段**用 `@DISTRIBUTE_CLAUSE@` 占位符（目前只有注册表一处），
   `make` / `make portable` 各自替换，禁止在片段里直接写死 `DISTRIBUTE BY ...`。

已知的实现细节（改代码前先知道）：

* 生成函数用 `sql/30_ddl_basic.sql` 的 `_cte_nsearch()` 保证 search/cursor/subgraph/stats
  四个函数的递归口径完全一致；只改一处忘了另一处会漂移。
* 防环条件是**两端都查**：`e.src <> ALL(path) AND e.dst <> ALL(path)`（原文只查一端，
  会产出 `{1,2,4,6,8,2}` 这种终点重复的路径）。
* 递归 CTE 的 anchor 与递归项统一包成 `(SELECT ... ORDER BY ... LIMIT n) t`，
  否则 PG 报 `syntax error at or near "UNION"`。

---

## 9. 不改共享实例的验证配方（本机实测可用）

```bash
cd contrib/pg_graph_search
make portable        # 用无分布子句的可移植版，单机加载不会报语法错
# 用独立库验证生成的 SQL，不碰系统 extension 目录
su postgres -c "psql -X -q -d postgres \
  -c 'DROP DATABASE IF EXISTS gs_check;' -c 'CREATE DATABASE gs_check;'"
su postgres -c "psql -X -q -v ON_ERROR_STOP=1 -d gs_check \
  -c 'CREATE EXTENSION age;' -c 'CREATE SCHEMA graph_search;' \
  -f $(pwd)/pg_graph_search--1.0.0-portable.sql"
su postgres -c "psql -X -q -v ON_ERROR_STOP=1 -d gs_check -f test/01_setup.sql"
su postgres -c "psql -X -q -d gs_check -f test/03_regress.sql"   # 期望：全部 18 项回归通过
su postgres -c "psql -X -q -d gs_check -f test/02_correctness.sql"  # 期望：仅第 71/77 行两处预期 ERROR
su postgres -c "psql -X -q -d postgres -c 'DROP DATABASE gs_check;'"
```

为什么手动 `CREATE SCHEMA graph_search` + `\i 生成文件`：这样可以验证最新模板而不覆盖
`$(pg_config --sharedir)/extension/` 下的已装版本，也不影响别的库。
`CREATE EXTENSION age` 只为绕开第 7 节的 R10 环境陷阱。

---

## 10. OpenTenBase 分布式部署（与单机唯一的差异）

| 项 | 单机做法 | OpenTenBase 做法 |
|---|---|---|
| 装载 | `\i portable.sql` 或 `CREATE EXTENSION` | `make install` 到**所有节点**的 `sharedir`，再在 CN 上 `CREATE EXTENSION` |
| 注册表 | 普通表 | 必须 `DISTRIBUTE BY REPLICATION`（`make` 默认注入） |
| 边表 | 随便建 | 中小图用 `DISTRIBUTE BY REPLICATION`（每 DN 全量）；大图用 `HASH(c1)` 但需压测 |
| SQL 模式 | 只有 postgresql | `opentenbase_ora` 模式下也能建扩展（control 里 `sql_mode = 'all'`） |

上线前必查：

```sql
SELECT graph_search.registry_distribution();  -- 期望 REPLICATION
SELECT graph_search.set_registry_replication();  -- 已是 REPLICATION 时幂等返回
```

`registry_distribution()` 返回非 `REPLICATION` 时，`install()` 只发 WARNING 不阻断。
原因：注册表不是 REPLICATION 时，每个 DN 只看到一部分实例清单，
`uninstall()` / `list_installed()` 会出现"装了但查不到"。

完整 Runbook、依据（`check_extension_in_right_mode()`、两阶段 `CREATE EXTENSION`）、
待实测清单见 `doc/opentenbase-port.md`。

---

## 11. 交付前自检

- [ ] `make` 与 `make portable` 都跑过，两个产物都由 `sql/` 片段生成（勿手改）
- [ ] `python3 test/lint_sql.py` 通过
- [ ] `test/03_regress.sql` 全绿（改动行为时断言数同步更新）
- [ ] `test/02_correctness.sql` 只有第 71/77 行两处预期 ERROR
- [ ] `advise()` 对无向实例给出索引结论
- [ ] README 与本文的命令、签名、性能数字没有互相矛盾
