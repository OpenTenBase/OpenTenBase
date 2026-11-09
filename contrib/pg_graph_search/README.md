# pg_graph_search — PostgreSQL 属性图搜索扩展（元函数驱动）

把 digoal's [《PostgreSQL 图式搜索(graph search)实践》](https://github.com/digoal/blog/blob/master/201801/20180102_04.md)里的递归 CTE 方案，做成一个可安装、
可参数化、可复用的 PostgreSQL 扩展。

核心理念：**元函数生成业务函数**。你不需要手写那 200 行 `WITH RECURSIVE`，
而是调一次元函数，让它按你的边表结构生成一整套业务函数。

    SELECT graph_search.install('{"table":"public.a","c1":"c1","c2":"c2","prop":"prop"}');

一条命令，得到 5 个可直接调用的函数 + 1 张统一边视图 + 1 条注册记录。

> **给 AI Agent 的操作手册在 [SKILL.md](SKILL.md)**：触发条件、5 分钟上手、生成 API 的精确签名、
> spec 参数、报错对照表、性能量级，以及"改 `sql/` 模板 → 跑回归"的验证闭环。
> README 讲**为什么这么设计**，SKILL.md 讲**怎么用、怎么验、哪里会踩坑**。

---

## 一、设计要点

### 1. 元函数（meta function）分三层，规划与执行解耦

| 层 | 函数 | 职责 |
|---|---|---|
| 规范化 | `_norm(spec)` | 校验表/列存在、推导点类型、生成 schema 限定的表名、补齐默认值 |
| 体检 | **`advise(spec)`** | 安装前性能预检（索引/驱动列），返回建议文本 |
| 规划 | **`plan(spec)`** | 只产出 DDL 文本数组，**不做任何修改**。可预览、可审计、可人工执行 |
| 执行 | **`install(spec)`** | 执行 `plan()` 的 DDL 并登记到 registry |
| 清理 | **`uninstall(schema, prefix)`** | 按依赖反序删除生成对象，清 registry |
| 盘点 | **`list_installed()`** | 列出所有实例 |

`plan()` 与 `install()` 分离是有意的：生成的代码你可以先看一眼再落库，
也可以直接 `psql -f` 人工执行。

### 2. 统一边视图

所有生成函数都查同一张 `<prefix>_edge(src, dst, prop, w)` 视图：

* 有向（默认）：直接映射源表
* 无向：`UNION ALL` 双向展开，一份数据支持两种图语义

权重 `w` 在视图里算一次（`coalesce((prop->>'weight')::float8, 0)`），
5 个函数复用同一口径，不会出现"搜索按权重排序但最短路径不按权重"这类漂移。

### 3. 生成的 5 个业务函数

| 函数 | 返回 | 用途 |
|---|---|---|
| `<prefix>_search(root, depth, limit, weight)` | `setof record` | N 度搜索，返回完整路径 + 边属性 |
| `<prefix>_cursor(root, curname, depth, limit, weight)` | `refcursor` | 游标版，FETCH 分页，首行毫秒级返回 |
| `<prefix>_shortest_path(p1, p2, depth, limit, weight)` | `setof record` | 最短路径，**BFS 分层提前退出** |
| `<prefix>_subgraph(root, depth, limit, weight)` | `jsonb` | 子图 JSON，直接喂前端绘图 |
| `<prefix>_stats(root, depth, limit, weight)` | `setof record` | 每层边数/新增点数/平均路径长，做扇出分析 |

---

## 二、相对原文的四处修正

### 修正 1：最短路径的返回值漏了终点（原文 UDF3/UDF4 的 bug）

原文递归项是 `path || g.c1`，最终却 `SELECT path as o_path`。
`path` 里存的是"到当前点为止"的节点，终点 `c2` 从未被拼进去。
原文示例输出即证据：

    o_path                | o_point1 | o_point2
    {31208,319951,3199647}|  3199647 | 31957536     <-- 31957536 不在路径里

本实现返回**含起终点的完整路径**，并附带沿途每条边的属性数组 `o_link_props`。

### 修正 2：最短路径不做"提前退出"

原文写法依赖 `RecursiveUnion` 恰好以层为单位流式吐行、且 `LIMIT 1` 能早停。
一旦执行计划里插进 Sort/Materialize，早停失效，退化为"把整个可达子图展开完再过滤"。

本实现是显式的 BFS 循环：维护 `frontier`（当前层）与 `visited`（已访问），
每层只扫当前层，节点访问数是 O(可达节点数) 而非 O(路径条数)，
命中目标当层立即 `RETURN`。

### 修正 3：防环条件漏检终点

原文递归项是 `AND (g.c1 <> ALL(sg.path))`，但输出列是 `path || c2` ——
`c2` 也进了路径，却没被检查。实测会产出 `{1,2,4,6,8,2}` 这种终点重复的路径。

本实现同时检查两端：`e.src <> ALL(path) AND e.dst <> ALL(path)`。

### 修正 4：递归 CTE 的 anchor 项需要子查询包裹

PG 不允许递归 CTE 的 anchor 项直接带 `ORDER BY` / `LIMIT` 后接 `UNION ALL`：

    syntax error at or near "UNION"

本实现把 anchor 与递归项统一包成 `(SELECT ... ORDER BY ... LIMIT n) t`，
既过语法关，也顺带统一了"每层 limit"的语义。

---

## 三、安装

    make
    make install          # 安装到 `pg_config --sharedir` 指向的 extension 目录
    psql -c "CREATE EXTENSION pg_graph_search;"

### 3.1 在 OpenTenBase（本仓库）里构建

本目录就是 OpenTenBase 的 `contrib/pg_graph_search`，跟内核一起编即可：

    cd <OpenTenBase 源码根>
    chmod +x configure configure.in                     # 仓库里没有可执行位
    ./configure --prefix=$PWD/_install --enable-user-switch --with-libxml \
                --disable-license --with-openssl CFLAGS="-g -mcx16"
    make -j8 && make install                            # 别用 -sj，出错会静默
    cd contrib && make -j8 && make install              # 新扩展就在其中

    # 只编这一个扩展（开发期）
    make -C contrib/pg_graph_search && make -C contrib/pg_graph_search install

构建期的三个坑（都是上游仓库的，不是本扩展的）：

1. `src/include/opentenbase_ora/gram_ora.h` 在仓库里是从 macOS 提交的 `XSym` 符号链接占位，
   Linux 上会被当普通头文件编译 → `'OraScanKeywords' undeclared`。删掉它让 bison 重新生成。
2. `CFLAGS` 缺 `-mcx16` 时，`pg_atomic_compare_and_swap_u128()` 里的
   `__sync_val_compare_and_swap` 会引用 `__sync_val_compare_and_swap_16`，
   而 `libatomic` 并不导出该符号 → `collect2: error: ld returned 1`。
3. `configure` 把 zstd/lz4 的路径写死为 `/usr/local/lib/lib*.a`；
   用 `--without-zstd --without-lz4`，或把系统库软链到该路径。

两个构建档位（同一份源码）：

| 目标 | 产物 | 用途 |
|---|---|---|
| `make` | `pg_graph_search--1.0.0.sql`（含 `DISTRIBUTE BY REPLICATION`） | OpenTenBase 部署 |
| `make portable` | `pg_graph_search--1.0.0-portable.sql` + `pg_graph_search-portable.control` | 在原生 PostgreSQL 上做功能验证 |

### 3.2 版本要求

纯 SQL 扩展，无编译产物，无需 `pg_config --includedir-server`。
用到的 SQL 特性最低版本 ≤ 9.6（`jsonb`/`WITH ORDINALITY` 9.4、`jsonb_object_agg`/`ON CONFLICT` 9.5），
因此 **OpenTenBase 的 PG 10 内核基线在特性层面是满足的**；
实测与未实测项见 [doc/opentenbase-port.md](doc/opentenbase-port.md) 的验证矩阵。

---

## 四、使用

### 4.1 安装实例

    -- 最简：列名默认 c1/c2/prop，生成到 public，函数前缀 g_<表名>
    SELECT graph_search.install('{"table":"public.a"}');

    -- 完整：指定列名、目标 schema、前缀、图方向、深度上限
    SELECT graph_search.install(jsonb_build_object(
        'table',          'public.a',
        'c1',             'c1',
        'c2',             'c2',
        'prop',           'prop',
        'weight_key',     'weight',    -- prop 里的权重键名
        'directed',       false,       -- false = 无向图（自动 UNION ALL 双向）
        'target_schema',  'graph',
        'prefix',         'user',      -- 生成 user_search / user_cursor / ...
        'max_depth_cap',  32           -- 运行时深度硬上限，防打爆
    ));

### 4.2 先体检、先预览（都不落库）

    -- 性能预检：无向图缺反向索引会退化 2~3 个数量级
    SELECT unnest(graph_search.advise('{"table":"public.a","directed":false}'));

    -- 看清楚将要执行的完整 DDL
    SELECT unnest(graph_search.plan('{"table":"public.a","target_schema":"graph"}'));

### 4.3 调用

    -- N 度搜索：3 度，每层限 100 条
    SELECT * FROM graph.user_search(root => 31208, depth => 3, limit => 100);

    -- 最短路径（含起终点 + 沿途边属性）
    SELECT * FROM graph.user_shortest_path(31208, 31957536, 5);

    -- 绘图数据：nodes + edges + 计数，前端直接 render
    SELECT graph.user_subgraph(31208, 2, 100);

    -- 扇出分析：每层有多少边、新增多少点
    SELECT * FROM graph.user_stats(31208, 3);

    -- 游标分页
    BEGIN;
    SELECT graph.user_cursor(31208, 'cur1', 3);
    FETCH 100 FROM cur1;
    CLOSE cur1;
    COMMIT;

### 4.4 运维

    SELECT * FROM graph_search.list_installed();
    SELECT graph_search.uninstall('graph', 'user');   -- 返回实际执行的 DROP 文本

`uninstall()` 按"函数 → 视图"依赖反序删除，并清理 registry。
覆盖安装（`install(..., p_replace => true)`）会先自动卸载旧实例，
避免函数签名变化导致 `CREATE OR REPLACE` 失败。

> 注意：`DROP SCHEMA ... CASCADE` 删掉生成对象后 registry 仍有记录。
> 这时应手工执行 `DELETE FROM graph_search.registry WHERE target_schema='...'`。

---

## 五、spec 参数

| 键 | 默认 | 说明 |
|---|---|---|
| `table` | **必填** | 边表。`schema.table` 或裸表名 |
| `c1` / `c2` | `c1` / `c2` | 点 1 / 点 2 的列名，必须存在 |
| `prop` | 无 | 边属性列（jsonb）。缺省则 `w` 恒为 0 |
| `weight_key` | `weight` | 从 `prop` 取权重的键名 |
| `weight_expr` | 无 | 自定义权重表达式**原文**。受字符白名单校验，挡分号/注释 |
| `directed` | `true` | `false` 时生成无向视图 |
| `target_schema` | `public` | 生成对象所在 schema，**必须已存在** |
| `prefix` | `g_<表名>` | 函数名前缀，自动净化成合法标识符 |
| `node_type` | 按 `c1` 列推导 | 支持 int2/int4/int8/text/uuid/numeric |
| `max_depth_cap` | `64` | 运行时深度硬上限 |

---

## 六、安全

* 标识符一律 `format('%I')`，常量一律 `format('%L')`
* 表名统一经 `_qualified()` 渲染为 schema 限定名 —— 生成物不依赖 `search_path`，
  避免跨 schema 同名表踩雷
* `weight_expr` 是原文拼接，仅允许 `[A-Za-z0-9_ .()<>:'"*+\-/,|]`，
  拒绝 `;`、`--`、`/*`
* `install()` 检查目标 schema 存在性与当前用户权限
* 生成函数是 `STABLE` 而非 `VOLATILE`，允许被外层查询优化

---

## 七、测试

    bash test/verify_all.sh          # 全链路：静态门禁 → make install → 建扩展 → 正确性 → 回归

    # 或者单步跑（<db> 换成自己的测试库）
    python3 test/lint_sql.py         # SQL 模板静态门禁（4 类高危问题，不需要数据库）
    psql -d <db> -f test/01_setup.sql        # 造小图 + 装实例
    psql -d <db> -f test/02_correctness.sql  # 15 项人工可推演的正确性验证
    psql -d <db> -f test/03_regress.sql      # 18 项断言式回归

    # 社区标准回归（pg_regress，T1–T15）
    make installcheck

    # OpenTenBase 专有：分布式对照 + 节点级检查
    psql -d <db> -f test/opentenbase/01_setup_distributed.sql   # 复制表/哈希表 × 有向/无向
    psql -d <db> -f test/opentenbase/02_verify_nodes.sql        # EXECUTE DIRECT 查 DN 侧对象

`verify_all.sh` 走环境变量里的连接参数（`PGHOST/PGPORT/PGUSER/PGDATABASE`），
自动识别“原生单机 / OpenTenBase 集群”并选对应装载方式；它会建一个临时库，
验证完删掉（`KEEP_DB=1` 可保留，`VERIFY_CREATE_AGE=1` 可规避下文的 AGE 陷阱）。

> 环境提示：若实例的 `shared_preload_libraries` 里预载了 `age`，在"age 已预载、
> 但当前库未 `CREATE EXTENSION age`"的库里，`03_regress.sql` 的 R10 会在 `TRUNCATE`
> 临时表时报 `schema "ag_catalog" does not exist`（AGE 的钩子介入，与本扩展无关）。
> 测试库先 `CREATE EXTENSION age;`（或设 `VERIFY_CREATE_AGE=1`），或换一个未预载 age 的实例。

静态门禁抓到过真实 bug（子查询投影缺 `AS` 别名导致运行期报
`column t.depth does not exist`），值得在改模板后先跑一遍。

---

## 八、性能实测

**环境**：PostgreSQL 18.6，8 vCPU / 15 GB 内存 / NVMe，本机非生产。
**数据**：20,000 节点 × 50 出边 = **952,476 条边，88 MB**（含 `(c1,c2)` 主键索引）。

### 8.1 单次查询

| 场景 | 耗时 | 说明 |
|---|---|---|
| 3 度搜索，每层限 100 | 19.4 ms | 返回 249 条路径 |
| 同上，直接查视图（不走 UDF） | 15.2 ms | 证明 UDF 包装开销约 4 ms（含一次动态 SQL 解析） |
| 2 度搜索，每层限 100 | 5.6 ms | 返回 149 条 |
| 最短路径，1→2 跳可达点 | 5.3 ms | BFS 扫 2 层 |
| 最短路径，**不可达** | 43.6 ms | **必须扫完整个连通分量才能确认不可达** |
| 分层统计（每层限 50，权重 ≥ 0.9） | 2.6 ms | 返回 3 行 |
| 子图 JSON（3 度，限 50） | 3.3 ms | 11,800 字节 |
| 有向搜索（2 度限 50） | 5.5 ms | |
| 无向搜索（同参数） | 48.3 ms | UNION ALL 双分支，约 9 倍 |

### 8.2 并发压测（pgbench，16 并发）

| 脚本 | 平均延迟 | TPS |
|---|---|---|
| 3 度搜索，每层限 100 | 82.2 ms | **195** |
| 最短路径，**可达** | 2.5 ms | **6,311** |
| 最短路径，**随机两点（多数不可达）** | 257.8 ms | **62** |

### 8.3 关键结论：最短路径的可达性决定性能量级

最短路径 TPS 可达/不可达相差 **100 倍**。原因是 BFS 找不到目标就得
扫完所在连通分量的所有节点 —— 这是算法本质，不是实现缺陷。

**实践建议**：
* 应用侧做**连通分量预判**（同 bucket 群体、预计算分区标签），避免无效 BFS
* 把不可达当**可缓存的负结果**，同一对 (p1,p2) 不重复搜索
* 数据量远超内存时，纯 SQL BFS 不够，需换 Apache AGE / 专用图引擎

### 8.4 无向图必须建反向索引

无向视图 = 正向 `UNION ALL` 反向，反向分支要按 `c2` 过滤。
主键是 `(c1, c2)`，`c2` 不是驱动列：

| 状态 | 执行计划 | Buffers | 单次 src 查询 |
|---|---|---|---|
| 无反向索引 | `Parallel Seq Scan` + `Rows Removed: 317475` | 7,944 | **30.6 ms** |
| 建 `(c2, c1)` 索引 | 双向 `Index Only Scan` | 8 | **0.05 ms** |

600 倍差距。安装前用 `graph_search.advise()` 检查：

    SELECT unnest(graph_search.advise('{"table":"public.a","directed":false}'));

有向图（默认）不受影响 —— `c1` 天然是主键驱动列。

### 8.5 未测项（诚实清单）

* **未测**：10 亿边以上规模。50 亿边会占约 34 GB 且生成耗时以小时计，
  已按磁盘/时间约束放弃。本文档所有数字仅在 95 万边规模成立。
* **未测**：PG 12~17 兼容性。只在 PG 18.6 实测。
* **未测**：并行执行下的最短路径。`max_parallel_workers_per_gather > 0` 时
  BFS 每层的聚合是否可并行收益未验证。
* **未测**：并发写场景。压测期间只有读。

---

## 九、目录结构与配套 SKILL.md

    pg_graph_search/
    ├── SKILL.md                     # 面向 AI Agent 的操作手册：怎么用、怎么验、哪里会踩坑
    ├── README.md                    # 本文：设计动机、相对原文的修正、实测数据、OpenTenBase 适配
    ├── Makefile                     # 拼接 sql/*.sql → pg_graph_search--1.0.0.sql（含双档构建）
    ├── pg_graph_search--1.0.0.sql  # 交付物，由 make 生成，勿手改
    ├── pg_graph_search.control     # 扩展元信息（schema=graph_search / sql_mode=all）
    ├── sql/                         # 唯一需要手改的源，6 个片段按文件名顺序拼接
    │   ├── 00_header.sql            #   文件头注释 + OpenTenBase 适配说明
    │   ├── 10_assert.sql            #   列断言 / schema 限定渲染 / 反向索引体检 / 点类型推导
    │   ├── 20_norm.sql              #   _norm：spec 校验与缺省补齐
    │   ├── 30_ddl_basic.sql         #   边视图 + N 度搜索 + 游标 DDL 构造器
    │   ├── 31_ddl_shortest.sql      #   最短路径（BFS）+ 子图 JSON + 分层统计 DDL 构造器
    │   └── 40_meta.sql              #   registry / plan / advise / install / uninstall / list
    │                                #   + registry_distribution / set_registry_replication
    ├── test/
    │   ├── lint_sql.py              #   SQL 模板静态门禁（4 类高危模式）
    │   ├── 01_setup.sql             #   造小图 + 装有向/无向实例
    │   ├── 02_correctness.sql       #   15 项人工可推演正确性
    │   ├── 03_regress.sql           #   18 项断言式回归
    │   ├── verify_all.sh            #   一键全链路（自动识别单机/集群）
    │   ├── sql/ + expected/         #   pg_regress 用例与期望输出
    │   └── opentenbase/             #   分布式对照 + 节点级检查（EXECUTE DIRECT）
    ├── bench/                       # 95 万边规模压测与数据生成（支持 DIST=replication|hash）
    └── doc/
        └── opentenbase-port.md      # 移植说明、依据、Runbook、验证矩阵、待实测清单

使用顺序：人读 `README.md` 了解设计；Agent 读 `SKILL.md` 执行操作；
改实现只改 `sql/`，然后 `make` + 跑 `test/`。

---

## 十、OpenTenBase 适配

本目录即 OpenTenBase 的 `contrib/pg_graph_search`。适配点少但关键：

| 问题 | 处理 |
|---|---|
| 注册表默认会被 **HASH(target_schema)** 分布，每个 DN 只看到一部分实例清单 | 注册表 DDL 末尾注入 `DISTRIBUTE BY REPLICATION`（构建期占位符，见 `Makefile`） |
| OpenTenBase 的 `check_extension_in_right_mode()` 默认只接受 `postgresql` 模式 | `control` 增加 `sql_mode = 'all'`，`opentenbase_ora` 模式下也能建扩展 |
| `CREATE EXTENSION` 是两阶段（PREPARE/EXECUTE）+ 全节点广播，脚本会在每个节点执行 | 本扩展纯 SQL、无 `.so`、无本地文件依赖，天然符合该协议；官方 `contrib/opentenbase_ai` 已用同样方式在扩展脚本里建复制表 |
| 生成物若依赖 `search_path` 会在 CN/DN 不一致 | 生成物一律 schema 限定（`_qualified()`），不依赖 `search_path` |

新增两个元函数（原生 PostgreSQL 上也能创建，只是没有分布概念）：

    SELECT graph_search.registry_distribution();
    --  OpenTenBase: REPLICATION / HASH / MODULO / DISTRIBUTED / NONE
    --  原生 PostgreSQL: single-node

    SELECT graph_search.set_registry_replication();
    --  把已有注册表改成 REPLICATION（幂等）；单机上明确报错

`install()` 在写入实例前会静默体检：注册表若不是 `REPLICATION`，发一条 WARNING 并给出修复命令。

**边表怎么分布**（使用者决定，直接决定性能）：

    -- 中小图（单机实测 95 万边 / 88 MB 量级完全可行）：复制表，每 DN 全量图
    CREATE TABLE public.t_edge (...) DISTRIBUTE BY REPLICATION;
    CREATE INDEX ON public.t_edge (c2, c1);      -- 无向图必须建反向索引

    -- 大图：哈希分布存储友好，但 CN 侧递归要按 frontier 反复跨节点取数，必须压测
    CREATE TABLE public.t_edge (...) DISTRIBUTE BY shard (c1);

分布函数仍是 `graph_search.<实例名>_n_degree()` 等，返回的是 CN 侧聚合结果；
`EXECUTE DIRECT` 下看不到这些由 `CREATE EXTENSION` 广播出去的 schema 内对象之外的东西。

分布式完整 Runbook（建集群、建扩展、验证 DN 侧对象、故障注入、性能清单）
见 [doc/opentenbase-port.md](doc/opentenbase-port.md)。
