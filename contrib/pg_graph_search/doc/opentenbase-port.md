# pg_graph_search × OpenTenBase 适配说明与验证记录

本文记录把 `pg_graph_search` 从原生 PostgreSQL 移植到 **OpenTenBase**（Postgres-XL 血统的
分布式 HTCC 数据库，内核基线 PG 10，当前发行版 5.21.8）所做的改动、依据、上线步骤与验证状态。

- 结论优先：**扩展本身几乎不需要改**（纯 SQL、无 `.so`、无本地文件依赖），
  真正需要适配的是 **注册表的分布策略**、**control 的 `sql_mode`** 与**测试/验证方式**。
- 平台差异全部集中在一个构建期占位符 `@DISTRIBUTE_CLAUSE@` 和一个运行期探测函数
  `graph_search.registry_distribution()` 上，因此同一份源码仍然可以在原生 PostgreSQL 上跑全量测试。
- **状态：2026-10-19 已在 OpenTenBase 5.21.8 真机集群（1CN + 2DN）上跑通全链路**
  —— 18/18 回归、15/15 正确性、节点级对象一致性、性能对照均已出数，
  详见 §4；真机才暴露的平台约束集中在 §8、集群搭建坑在 §9。

---

## 1. 改了什么

| 文件 | 改动 | 原因 |
|---|---|---|
| `sql/40_meta.sql` | 注册表 DDL 末尾加构建期占位符 `@DISTRIBUTE_CLAUSE@`；新增 `registry_distribution()`、`set_registry_replication()`、`_check_registry_distribution()`；`install()` 前静默体检分布策略 | 注册表是集群级元数据，必须全量复制到所有 DN |
| `sql/00_header.sql` | 头部补充 OpenTenBase 适配说明（a–d 条） | 让后来人知道生成物为什么长这样 |
| `pg_graph_search.control` | 增加 `sql_mode = 'all'` | OpenTenBase 的 `check_extension_in_right_mode()` 只接受 `all` 或与当前模式匹配的值 |
| `Makefile` | 重写：`DATA_built` 驱动生成；`DISTRIBUTE_CLAUSE` 双档；新增 `make portable`、`make lint`；接上 `REGRESS` | 同一份源码产出 OpenTenBase 版与可移植版；pg_regress 可跑 |
| `test/sql/pg_graph_search.sql` + `test/expected/pg_graph_search.out` | 新增 pg_regress 用例（T1–T15） | OpenTenBase 社区标准回归方式；一份期望输出双平台通用 |
| `test/03_regress.sql` | 新增 R17/R18（分布策略可观测、`set_registry_replication()` 行为） | 14→18 项，覆盖新适配点 |
| `test/opentenbase/01_setup_distributed.sql` | 新增：复制表 × 哈希表 × 有向 × 无向 四实例 | 分布式对照实验 |
| `test/opentenbase/02_verify_nodes.sql` | 新增：`EXECUTE DIRECT` 验证 DN 侧对象/注册表/函数签名 | 分布式下最容易出问题的是"某个节点没装上" |
| `test/verify_all.sh` | 重写：连接参数走环境变量、自动识别平台、双档装载 | 原脚本硬编码 `su postgres` + `postgres` 库，分布式下不可用 |
| `bench/gen_big.sh` | 连接参数化 + `DIST=replication\|hash\|none` | 压测要能指定边表分布 |
| `.gitignore` | 忽略 portable 产物与 pg_regress 输出 | 生成物不入库 |

**没有改的**：`sql/10_assert.sql`、`20_norm.sql`、`30_ddl_basic.sql`、`31_ddl_shortest.sql`
（生成器与 5 个业务函数）。原因：生成物一律 schema 限定、标识符 `format('%I')`、
只用标准 SQL 与 plpgsql，天然适配 XL 的"CN 生成、DDL 广播到全节点"模型。

---

## 2. 依据（源码/文档定位）

| 事实 | 位置 |
|---|---|
| `CREATE EXTENSION` 是**两阶段 + 全节点广播**：CN 上 `PrepareExtension()` → 广播 `PREPARE <stmt>` → `ExecuteExtension()` → 广播 `EXECUTE <stmt>` | `src/backend/tcop/utility.c` 的 `T_CreateExtensionStmt` 分支（约 4225–4275 行）、`src/backend/commands/extension.c` 的 `PrepareExtension`（约 3515）/`ExecuteExtension`（约 3901） |
| 扩展脚本会在 CN 与每个 DN 上执行 → 脚本里可以建表，**官方已有先例** | `contrib/opentenbase_ai/opentenbase_ai--1.0.sql`：`CREATE TABLE public.ai_model_list (...) DISTRIBUTE BY REPLICATION;` |
| 无 `DISTRIBUTE BY` 时默认取**主键/唯一约束首列**做 HASH | `doc/src/sgml/ddl.sgml` |
| `CREATE TABLE IF NOT EXISTS ... DISTRIBUTE BY REPLICATION` 语法合法 | `src/backend/parser/gram.y` 的 `CREATE OptTemp TABLE IF_P NOT EXISTS ... OptDistributeBy` 产生式 |
| `ALTER TABLE ... DISTRIBUTE BY ...` 可用（`set_registry_replication()` 的底层） | `gram.y` 的 `AT_DistributeBy`（约 2913 行） |
| 分布策略存在 `pgxc_class.pclocatortype`，取值为 `R/H/M/D/O` | `src/include/catalog/pgxc_class.h`、`src/include/pgxc/locator.h` |
| `CREATE FUNCTION` / `CREATE VIEW` → `EXEC_ON_ALL_NODES`；`ExecUtilityFindNodes()` 默认也是 | `src/backend/tcop/utility.c`（约 1690 / 1989 / 7208 行） |
| `sql_mode` 默认 `postgresql`，须显式写 `all` 才能在两种模式都建扩展 | `src/backend/commands/extension.c`（约 75 / 4438 行） |
| `EXECUTE DIRECT` 限超级用户、单节点、禁 DDL/DML/事务 | `doc/src/sgml/ref/execute_direct.sgml` |
| 内核对 `WITH RECURSIVE` 无额外限制（MERGE 才有） | `src/backend/parser/parse_merge.c` |

---

## 3. 上线步骤（Runbook）

### 3.1 编译与部署

```bash
# 1) 内核与 contrib（与官方 README 一致）
su - opentenbase
export SOURCECODE_PATH=/data/opentenbase/OpenTenBase
export INSTALL_PATH=/data/opentenbase/install
cd ${SOURCECODE_PATH}
./configure --prefix=${INSTALL_PATH}/opentenbase_bin_v5.0 --enable-user-switch \
            --with-libxml --disable-license --with-openssl CFLAGS="-g"
make -sj && make install
cd contrib && make -sj && make install          # 新扩展就在其中

# 2) 只编这一个扩展（开发期）
make -C contrib/pg_graph_search
make -C contrib/pg_graph_search install
```

> 本机实测的三个构建前提（见 §5）：`CFLAGS` 带 `-mcx16`（否则 128 位 CAS 链接失败）、
> 清掉 `src/include/opentenbase_ora/gram_ora.h` 这个从 macOS 提交的 `XSym` 占位文件、
> 以及 zstd/lz4（configure 把这两个库的路径硬编码成 `/usr/local/lib/`）。

### 3.2 建集群、建扩展

```bash
opentenbase_ctl install -c opentenbase_config.ini     # type=distributed 或 centralized
opentenbase_ctl status  -c opentenbase_config.ini
psql -h <CN_IP> -p 11000 -U opentenbase -d postgres
```

```sql
CREATE EXTENSION pg_graph_search;                    -- 各节点同时生效
SELECT graph_search.registry_distribution();         -- 期望 REPLICATION
```

### 3.3 建图与安装实例

> ⚠️ **本版本只支持两种分布策略**（未开 `ENABLE_ALL_TABLE_TYPE`，实测确认）：
> `DISTRIBUTE BY shard (col)` 与 `DISTRIBUTE BY REPLICATION`。
> 写 `DISTRIBUTE BY HASH(c1)` / `DISTRIBUTED BY (c1)` / `DISTRIBUTED RANDOMLY`
> 会在 `utility.c` 的 `T_CreateStmt` 分支被直接拒掉：
> `ERROR: (XX000) Cannot support distribute type: Hash`。
> 详见 §8。
>
> ⚠️ `DISTRIBUTE BY REPLICATION` 需要集群里存在默认 node group，
> 否则报 `ERROR: (42704) default group not defined`。

```sql
-- 先确认集群已有默认 node group（没有就先建，见 §8.3）
SELECT group_name, default_group FROM pgxc_group;

CREATE SCHEMA IF NOT EXISTS graph;

-- 中小图首选：复制表（每个 DN 全量），配合反向索引
CREATE TABLE public.t_edge (
  c1 int NOT NULL, c2 int NOT NULL, prop jsonb,
  PRIMARY KEY (c1, c2)
) DISTRIBUTE BY REPLICATION;
CREATE INDEX ON public.t_edge (c2, c1);

-- 大图用分片表（注意：不是 HASH，是 shard）；CN 侧递归要跨节点取数，务必压测
-- CREATE TABLE ... ) DISTRIBUTE BY shard (c1);

SELECT unnest(graph_search.advise(
  '{"table":"public.t_edge","directed":false}'::jsonb));           -- 安装前体检
SELECT unnest(graph_search.plan(
  '{"table":"public.t_edge","prop":"prop","target_schema":"graph"}'::jsonb));  -- 预览 11 条 DDL
SELECT graph_search.install(
  '{"table":"public.t_edge","prop":"prop","target_schema":"graph","prefix":"user"}'::jsonb);
```

### 3.4 验证

```bash
# 全链路（自动识别平台；分布式档会额外跑 test/opentenbase/01_setup_distributed.sql）
PGHOST=<CN_IP> PGPORT=11000 PGUSER=opentenbase bash test/verify_all.sh

# 社区标准回归（需要扩展已 make install 到实例）
make -C contrib/pg_graph_search installcheck

# 节点级检查：先把 dn0001/dn0002 换成实际节点名
psql -h <CN_IP> -p 11000 -U opentenbase -d postgres -f test/opentenbase/02_verify_nodes.sql
```

---

## 4. 验证矩阵（做了什么 / 在哪做的）

### 4.1 原生 PostgreSQL 侧（回归开发用）

| 项 | 环境 | 结果 |
|---|---|---|
| SQL 模板静态门禁（4 类高危模式） | 任意 | ✅ 通过 |
| 15 项人工可推演正确性（`test/02_correctness.sql`） | 原生 PG 18.6 + 可移植构建 | ✅ 仅第 71/77 行两处**预期**抛错 |
| 18 项断言式回归（`test/03_regress.sql`，含 R17/R18 分布策略） | 原生 PG 18.6 + 可移植构建 | ✅ 18/18 |
| pg_regress 用例（`test/sql/pg_graph_search.sql`，T1–T15） | 原生 PG 18.6 | ✅ `ok 1 - pg_graph_search`，期望输出 284 行 |
| `registry_distribution()` 单机回退 | 原生 PG 18.6 | ✅ 返回 `single-node` |
| `set_registry_replication()` 单机明确报错 | 原生 PG 18.6 | ✅ 报错 + HINT |

### 4.2 OpenTenBase 5.21.8 真机（2026-10-19）

集群：`PostgreSQL 10.0 @ OpenTenBase_v5.0 (commit: b612d77cb)`，
单机 1CN(11000) + 2DN(12001/12002) + GTM(6666)，`zh_CN.utf8`。
搭建脚本见 §9（含 5 个必须绕开的坑）。

| 项 | 结果 | 证据 |
|---|---|---|
| `CREATE EXTENSION pg_graph_search`（两阶段 + 全节点广播） | ✅ | CN/DN 三个节点 `pg_extension` 都有 |
| 注册表落点与分布策略 | ✅ | `registry_distribution()` = `REPLICATION` |
| **注册表行数 CN 与 DN 一致** | ✅ | CN 4 行 / dn0001 4 行（REPLICATION 语义正确） |
| 生成函数广播到 DN 且签名一致 | ✅ | `EXECUTE DIRECT` 查到 5 个 `rep_*`，`pg_get_function_arguments` 与 CN 逐字相同 |
| 生成边视图在 DN 上存在 | ✅ | `gtest.rep_edge` |
| `uninstall()` 的 DROP 传播 | ✅ | `rep_*` 函数、视图在 CN 与 DN 上均清零，registry 行同步删除 |
| 分布式对照用例（`01_setup_distributed.sql`） | ✅ | 复制表 × 分片表 × 有向 × 无向 4 实例安装成功 |
| 断言式回归 18 项 | ✅ | `==== 全部 18 项回归通过 ====` |
| 正确性验证 15 项 | ✅ | 0 失败，2 处预期抛错 |
| `bash test/verify_all.sh` 全链路 | ✅ | `===== 全链路验证通过（平台 opentenbase）=====` |
| 真实分片落点 | ✅ | 100 行 shard 表：dn0001 42 / dn0002 58 |

### 4.3 性能对照（2 DN，20000 节点 / 199k 边，pgbench -t 20 -c 2 -j 2）

| 查询 | REPLICATION | shard(c1) | 备注 |
|---|---|---|---|
| N 度搜索 `b_search(r,3,100,0)` | **8.15 tps** / 245.7 ms | **10.23 tps** / 195.8 ms | 分片快约 25% |
| 最短路径 `b_shortest_path(p1,p2,4)` | **71.8 tps** / 28.2 ms | **35.6 tps** / 56.5 ms | 复制表快约 2 倍 |
| 存储占用 | 50 MB | 25 MB | REPLICATION = 每 DN 全量 |

结论：**分片换存储（减半）但最短路径慢一倍，N 度搜索略快**。
最坏情况是随机两点不可达——那会退化成"扫完 frontier 再判空"，两种分布都远慢于可达场景，
单机基线就有 6,311 TPS vs 62 TPS 的落差，单节点测试量级下不要按可达场景外推。

---

## 5. 开工前踩到的坑（上游仓库，非本扩展问题）

1. **`src/include/opentenbase_ora/gram_ora.h` 是 macOS 符号链接占位**。
   文件内容是 `XSym / 0065 / <md5> / /data/workspace/TBase-V3.0/src/backend/opentenbase_ora/gram_ora.h`，
   Linux 下会被当普通头文件编译，报
   `gram_ora.h:2:1: error: expected '=', ',', ';', 'asm' or '__attribute__' before numeric constant`，
   连带 `'OraScanKeywords' undeclared`。处理：删掉该占位文件，`touch` 语法文件让 bison 重新生成。
2. **缺 `-mcx16` 会链接失败**。`src/include/port/atomics.h` 的
   `pg_atomic_compare_and_swap_u128()` 在非 aarch64 分支直接用
   `__sync_val_compare_and_swap()` 操作 `__int128`；gcc 默认不生成 `cmpxchg16b`，
   而是引用 `__sync_val_compare_and_swap_16`——这个符号 **libatomic 并不导出**
   （`nm -D libatomic.so | grep sync_val` 为空），加 `-latomic` 也无效。
   处理：`CFLAGS` 加 `-mcx16`（需 CPU 支持 `cx16`，`/proc/cpuinfo` 可查）。
   改完 CFLAGS 要 `make clean`，否则旧 `.o` 仍引用旧符号。
3. **configure 把 zstd / lz4 的库路径硬编码为 `/usr/local/lib/lib*.a`**（`configure` 约 12802/12852 行），
   即使系统装了 `libzstd-dev`/`liblz4-dev`（库在 `/usr/lib/x86_64-linux-gnu/`）也会
   `library not found`。处理：`--without-zstd --without-lz4`（本扩展不需要这两种压缩），
   或把系统 `.a` 软链到 `/usr/local/lib/`。
4. **`./configure` 默认没有可执行位**，README 里要求的 `chmod +x configure*` 是必需的。
5. 构建 `make -sj` 是静默模式，出问题只在最后报 `Error 2`；排查时用 `make -j8` 保留编译输出。
6. **`src/fe_utils` 并行构建会自相残杀**：`psqlscan.l` 与 `psqlscan_ora.l` 同一目录同时调 flex，
   第二个 `rm lex.backup` 报 `No such file or directory` → `Error 1`。
   处理：先 `make -C src/fe_utils -j1`，再继续 `make -j8`。

---

## 6. 原风险清单的实测结论（2026-10-19 真机）

| # | 原风险 | 实测结论 |
|---|---|---|
| 1 | 两阶段 `CREATE EXTENSION` 下注册表落点 | ✅ CN 4 行 / dn0001 4 行，REPLICATION 语义正确 |
| 2 | `INSERT ... ON CONFLICT` 在复制表上的行为 | ✅ 4 次 install（幂等重装）全部正确，R13 通过 |
| 3 | `WITH RECURSIVE` 下推能力 | ✅ 递归正常跑通；两种分布都能出正确结果（性能差异见 §4.3） |
| 4 | `refcursor` 的 FETCH 行为 | ✅ FETCH 逐条与 `search()` 完全一致（但断言写法要避开 §8.2 的坑） |
| 5 | `DROP` 的传播范围 | ✅ `DROP FUNCTION` 与 `DROP VIEW` 在各 DN 都生效，registry 行同步删除 |
| 6 | 故障注入（停一个 DN 后回滚） | ✅ `install()` 快失败且回滚干净；`uninstall()` 整单中止、状态不变；DN 恢复后无需修复 |
| 7 | 性能（两种分布对比） | ✅ 已出数，见 §4.3 |
| 8 | `sql_mode='all'` 在 `opentenbase_ora` 模式下 | ⚠️ 可建性已由源码确认，运行语义未实测（需重编译换模式，见下） |

### 6.1 故障注入实测细节

`pg_ctl stop -Z datanode -D <dn2>` 之后：

- **`install()`**：约 1 秒内失败，报
  `ERROR: (53000) Failed to get pooled connections: connect for dn0002(127.0.0.1:12002) failed`，
  且带 `CONTEXT: ... graph_search.install(jsonb,boolean) line 29` —— 死在
  "是否已装过"的前置检查上，还没发任何 DDL。
  回滚后 `graph_search.registry` 里**没有**半截记录，其余 5 个实例完好。
- **`uninstall()`**：第一条 `DROP FUNCTION` 就抛同样的错，整个 `FOREACH ... EXECUTE`
  循环随事务一起回滚 —— `registry` 行仍在、`repu_*` 的 5 个函数仍在，**没有"删一半"的状态**。
- **DN 恢复后**：`SELECT count(*) FROM gbench.b_search(1,3,100,0)` 立刻返回 210 行，
  无需任何人工修复。

运维含义：**必须先把所有 DN 拉起来才能 `uninstall()`**，否则整单不执行；
但好处是不会留下需要手工收拾的半成品。

### 6.2 关于 `opentenbase_ora` 模式

`check_extension_in_right_mode()`（`src/backend/commands/extension.c:72-75` + 同名函数）的判定是：

```c
if (strcmp(control->sql_mode, ALL_EXT_SQL_MODE) == 0 ||       /* "all" */
    (ORA_MODE && strcmp(control->sql_mode, ORA_EXT_SQL_MODE) == 0) ||
    (PG_MODE  && strcmp(control->sql_mode, PG_EXT_SQL_MODE)  == 0))
    return true;
```

本扩展 control 写的是 `sql_mode = 'all'`，**无条件命中第一条分支**，
所以两种模式下 `CREATE EXTENSION` 都能建 —— 这条已由源码确认。

没实测的是**生成函数的运行语义**：Oracle 兼容模式下空串等于 NULL、
隐式类型转换等规则会影响递归查询的结果。`sql_mode` 在本版本是
`PGC_INTERNAL` + `GUC_DISALLOW_IN_FILE`，`pg_database` 里也没有对应列，
**换模式必须重编译**（另一轮约 30–60 分钟）。本扩展生成的 SQL 只用了
标准 SQL + plpgsql + `jsonb` + `ARRAY[]`，没有主动依赖 Oracle 特有语义，
但这属于"未验证"，不要当成已验证项对外引用。

---

## 7. 在原生 PostgreSQL 上做可移植验证（开发期回归）

```bash
cd contrib/pg_graph_search
make USE_PGXS=1            # 生成 OpenTenBase 版 pg_graph_search--1.0.0.sql
make USE_PGXS=1 portable   # 生成 -portable.sql 与 pg_graph_search-portable.control

# 直接把可移植版加载进临时库（不碰系统 extension 目录）
psql -c 'CREATE DATABASE gs_dev' -c '\c gs_dev'
psql -d gs_dev -c 'CREATE SCHEMA graph_search'
psql -d gs_dev -f pg_graph_search--1.0.0-portable.sql
psql -d gs_dev -f test/01_setup.sql
psql -d gs_dev -f test/03_regress.sql      # 期望：全部 18 项回归通过
psql -d gs_dev -f test/02_correctness.sql  # 期望：仅 2 处预期抛错

# 或一把梭
bash test/verify_all.sh
```

`make portable` 额外生成的 `pg_graph_search-portable.control` 去掉了 `sql_mode`
（原生 PG 会把未知 control 参数当 ERROR），它只用于本地验证，不随扩展发布。

---

## 8. OpenTenBase 行为约束（真机实测，2026-10-19）

这一节是本轮适配最硬的产出：**扩展生成的 5 个函数在 OpenTenBase 上是 CN 侧
"pull-up" 函数**，能跑，但有一批 SQL 构造上的硬边界。踩到哪条，报错长什么样，
以及本扩展自己的测试怎么绕开的，都记在这里。

### 8.1 分布策略：只有 `shard` 和 `REPLICATION`

`src/backend/tcop/utility.c` 的 `T_CreateStmt` 分支里有这么一段（未开
`ENABLE_ALL_TABLE_TYPE` 时生效）：

```c
if (stmt_loc->distributeby
    && stmt_loc->distributeby->disttype != DISTTYPE_SHARD
    && stmt_loc->distributeby->disttype != DISTTYPE_REPLICATION)
{
    switch (stmt_loc->distributeby->disttype) {
        case DISTTYPE_HASH: case DISTTYPE_MODULO:
            elog(ERROR, "Cannot support distribute type: Hash");
        case DISTTYPE_ROUNDROBIN:
            elog(ERROR, "Cannot support distribute type: RoundRobin");
    }
}
```

对照 `gram.y` 的 `OptDistributeByInternal`，本编译的合法/非法矩阵是：

| 写法 | disttype | 结果 |
|---|---|---|
| `DISTRIBUTE BY shard (c1)` | `DISTTYPE_SHARD` | ✅ |
| `DISTRIBUTE BY REPLICATION` | `DISTTYPE_REPLICATION` | ✅（需先有默认 node group） |
| `DISTRIBUTE BY hash (c1)` | `DISTTYPE_HASH` | ❌ `Cannot support distribute type: Hash` |
| `DISTRIBUTE BY modulo (c1)` | `DISTTYPE_MODULO` | ❌ 同上 |
| `DISTRIBUTE BY roundrobin` | `DISTTYPE_ROUNDROBIN` | ❌ `...: RoundRobin` |
| `DISTRIBUTED BY (c1)` | `DISTTYPE_HASH` | ❌ 同 Hash |
| `DISTRIBUTED RANDOMLY` | `DISTTYPE_ROUNDROBIN` | ❌ |
| `DISTSTYLE key DISTKEY (c1)` | `DISTTYPE_HASH` | ❌ |
| `DISTSTYLE even` / `DISTSTYLE all` | ROUNDROBIN / REPLICATION | ❌ / ✅ |

**注意"不写 `DISTRIBUTE BY`"也不行**：默认规则取主键首列做 `DISTTYPE_HASH`，
照样被拒。边表必须显式写分布子句。

对应到本扩展：`pclocatortype` 的取值见 `src/include/pgxc/locator.h`，
本扩展 `registry_distribution()` 的映射表已补上 `'S' → 'SHARD'`（原来漏了）。

### 8.2 pull-up 函数的 SQL 边界

`gtest.t_search(...)` 这类函数被 OTB 判定为 "need to be executed in CN"。
实测边界：

| 用法 | 结果 |
|---|---|
| `SELECT count(*) FROM gtest.t_search(1,3,1000,0)` | ✅ 正常 |
| 同一会话里连续调用多次 | ✅ 稳定（9/9/9 行） |
| `SELECT ... f() UNION ALL SELECT ... f()` | ✅ 正常 |
| `INSERT INTO tmp SELECT ... FROM f()` | ❌ `Using pull-up functions in DML statements is not supported` |
| `... WHERE EXISTS (SELECT ... FROM f())` | ❌ `Subplan contains a function runs on CN which is not supported` |
| `f() EXCEPT/INTERSECT 本地临时表` | ⚠️ **不报错但结果错**（实测 9 行变成 3 行） |
| `array_agg(o_path)`，`o_path` 是 `int[]` | ❌ `cannot accumulate arrays of different dimensionality` |

最后一条尤其值得记住：`EXCEPT/INTERSECT` 静默出错比报错危险得多，
测试里千万不要用它来比对 CN 侧函数和本地临时表。

`R10`（游标 vs search 逐行一致）的断言因此改成：两侧各自把 `o_path` 转成文本、
排序后聚成 `text[]` 再比数组。**在原生 PostgreSQL 上同样通过**，两边都验过。

### 8.3 REPLICATION 表依赖默认 node group

没有默认 node group 时，建复制表会报 `ERROR: (42704) default group not defined`
（`src/backend/catalog/heap.c`）。最小闭环：

```sql
CREATE DEFAULT NODE GROUP ng1 WITH (dn0001, dn0002);   -- 语法无 NODE_LIST=
CREATE SHARDING GROUP TO GROUP ng1;                    -- 初始化该组的分片映射
```

漏掉第二条，建 `DISTRIBUTE BY shard (col)` 表时会报
`please initialize group:<oid> sharding map first`。
`pgxc_group` / `pgxc_shard_map` 两张系统表可以直接核对。

---

## 9. 单机搭 OpenTenBase 集群的 5 个必踩坑

用 `opentenbase_ctl` 一键搭会连续撞上下面几个问题（每个都实测过），
最后是绕开 `opentenbase_ctl` 手写 `initdb` 搭起来的。

1. **`opentenbase_ctl` 的 initdb 是静默的**：命令尾巴带 `&>/dev/null`，
   initdb 失败工具照样打 `Successfully initdb`。容器里没有 `zh_CN.utf8` 时
   `initdb --locale=zh_CN.utf8` 直接失败，三个数据目录全是空的，
   后续 `pg_ctl start` 报 "is not a database cluster directory"，工具报 `Success`。
   先 `locale-gen`。
2. **`opentenbase_ctl` 硬编码 `shared_preload_libraries='pageinspect'`**
   （`src/node/node.cpp`），pageinspect 没编进包的话所有节点起不来：
   `FATAL: could not access file "pageinspect"`。先把 `contrib/pageinspect` 编进去。
3. **端口分配会自相撞**：gtm 绑 `0.0.0.0:<cn/dn 首个端口>`，而多 IP 部署时
   每个 IP 都从 11000 开始分配，结果 gtm（`0.0.0.0:11000`）和 dn0002
   （`127.0.0.2:11000`）抢同一个端口，dn0002 起不来，
   连带 `CREATE NODE` 的元数据广播全部丢失。单机构建就把所有节点放在 `127.0.0.1`。
4. **`pooler_port` 全节点默认值都是 6667**，而 pooler 的 unix socket 锁文件
   只按端口命名（`/tmp/.s.PGPOOL.<port>.lock`，`poolcomm.c` 的
   `POOLER_UNIXSOCK_PATH`）。同机多节点共用默认端口时，只有一个节点的 pooler
   能起来，其余的 pooler 进程每秒 `FATAL: lock file ... already exists` 一次，
   CN 的分布式远程连接永远建不起来 —— **表现为任何跨节点语句静默挂死**。
   必须逐节点错开 `pooler_port`。
5. **`node_forward_port` 默认 0**，必须用 `ALTER NODE ... WITH (FORWARD=n)` 显式登记
   （注意选项名是 `FORWARD` 不是 `forward_port`）。不登记的话日志里是
   `the conn is not inited nodename cn0001 host 127.0.0.1 forward port 0`，
   然后 `force kill SIGTERM`。登记完还要重启 DN 才生效。

另外两个不算坑但会咬人的：

- `initdb` 必须带 `--master_gtm_nodename/--master_gtm_ip/--master_gtm_port`，
  否则 `pgxc_node` 里没有 GTM 行，启动后所有语句报
  `GetMasterGtmInfo: can not get master gtm info from pgxc_node`。
- initdb 建出来的 CN 自己那一行（`cn0001`）**不会**自动出现在 DN 的 `pgxc_node` 里，
  要用 `EXECUTE DIRECT ON (dn0001) 'CREATE NODE cn0001 ...'` 补齐，
  否则 DN 校验 `global_session=cn0001_<pid>_<xid>` 时报
  `node "cn0001_..." does not exist`，所有分布式语句失败。
  同一个 `EXECUTE DIRECT` 里只能有一条语句，多条会报
  `EXECUTE DIRECT cannot execute multiple queries`。

**可复现脚本**：`/root/new/tmp/otb_cluster_build.sh`（单机 1CN+2DN，
GTM:6666 / CN:11000 / DN:12001,12002，每节点独立 forward_port + pooler_port，
自动建 node group + sharding map + 分片自检）。
