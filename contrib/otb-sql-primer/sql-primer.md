# OpenTenBase SQL 入门手册（方向 1 交付）

> **读者**：第一次接触 OpenTenBase 的同学。
> **目标**：读懂 3 条真实存在于本仓库的 SQL，理解它们各自"查什么、为什么这么写"。
> **示例来源**：本仓库 `contrib/otb-health/otb_health.sh`（集群健康检查脚本），3 条 SQL 分别位于该文件**第 42 行、第 47 行、第 55–57 行**，本文逐字引用。
> **诚实声明**：本文所有结果表格均为**预期结果（未在 OpenTenBase 实例上执行）**，推导依据已在每条注明。若你在实例上运行，请以实际输出为准并回填实测结果。

---

## 0. 先搞懂：这些 SQL 在哪运行、连的是什么

`otb_health.sh` 用 `psql` 连接集群的 **CN（协调节点）** 执行 SQL（脚本第 30 行）：

```bash
PSQL=(psql -h 127.0.0.1 -p "$CN_PORT" -U "$DB_USER" -d "$DB_NAME")
```

- `-p $CN_PORT`：CN 的端口，默认 `11003`（脚本第 22 行）
- `-d $DB_NAME`：默认连 `postgres` 库（脚本第 24 行）

为什么连 CN？因为 OpenTenBase 是分布式数据库：**集群由协调节点（Coordinator/CN）、数据节点（DataNode/DN）和全局事务管理节点（GTM）组成；用户数据都在 DN 上，CN 只存元数据，GTM 负责全局事务管理**（来源：本仓库根目录 `README.md` 第 16 行，逐字核对）。所以你连上 CN 发 SQL，CN 负责把请求分发到各 DN。

> 提示：脚本对 `pgxc_node`、`pgxc_group` 的查询属于**集群元数据查询**——查的是"集群的组成"，不涉及用户数据，所以在 CN 上直接查即可。

---

## 1. 查看集群里注册了哪些节点

### 原文（`otb_health.sh` 第 42 行，逐字）

```sql
select node_name, node_type, node_port, node_host from pgxc_node order by node_type
```

> 脚本里是小写、末尾无分号（因为通过 `psql -c "..."` 调用，分号可省）。在交互式 `psql` 中手敲时，写成大写加分号 `;` 是等价的常规写法。

### 用途（一句话）

列出当前集群注册的全部节点及各自的角色、端口和地址——相当于拿到一张"集群成员表"。

### 逐列解释

| 列 | 含义 | 说明 |
|---|---|---|
| `node_name` | 节点名称 | 集群内唯一标识，如 `gtm1`、`cn1`、`dn1` 这类命名（以实际输出为准） |
| `node_type` | 节点角色 | 标识该节点是 GTM / 协调节点（CN）/ 数据节点（DN）中的哪一种 |
| `node_port` | 服务端口 | 该节点监听的端口，客户端或内部转发用它建立连接 |
| `node_host` | 主机地址 | 节点所在机器的 IP / 主机名 |

`order by node_type`：按角色排序，让同类型节点（如所有 DN）排在一起，一眼看清集群构成。

**`pgxc_node` 是什么**：OpenTenBase（源自 Postgres-XC 体系）的系统表，记录集群的节点注册信息。

**`node_type` 的取值有源码依据**（`src/include/nodes/parsenodes.h` 第 127–132 行，逐字核对）：

| 常量 | 取值 | 含义 |
| --- | --- | --- |
| `PGXC_NODE_COORDINATOR` | `'C'` | 协调节点（CN） |
| `PGXC_NODE_DATANODE` | `'D'` | 数据节点（DN） |
| `PGXC_NODE_SLAVEDATANODE` | `'S'` | 从数据节点 |
| `PGXC_NODE_GTM` | `'G'` | 全局事务管理节点（GTM） |
| `PGXC_NODE_NONE` | `'N'` | 未指定 |

所以在这条 SQL 的结果里看到 `C` / `D` / `G`，就能对上"协调 / 数据 / 全局事务"三种角色。
**注意**：上表是源码里的**常量定义**，属于静态依据；某个集群实际返回哪些行，仍以你自己执行的结果为准。

### 预期结果（未在实例上执行，格式示意）

按脚本注释里的"单节点最小集群 1 GTM + 1 CN + 1 DN"（第 5–6 行）推演，`order by node_type` 升序下的形状大致如下（`node_name`、端口、地址以实际集群为准，此处只示意 `node_type` 的取值）：

| node_name | node_type | node_port | node_host |
| --- | --- | --- | --- |
| gtm1 | `G` | （以实际为准） | （以实际为准） |
| cn1 | `C` | （以实际为准） | （以实际为准） |
| dn1 | `D` | （以实际为准） | （以实际为准） |

行数 = 集群注册的节点总数。多 DN 集群则相应增多（每个 DN 一行 `D`）。
**这是静态推演，不是实测输出**；若你的结果与上表不同（例如多了 `S` 从节点），以实际输出为准。

---

## 2. 查看节点分组

### 原文（`otb_health.sh` 第 47 行，逐字）

```sql
select * from pgxc_group
```

### 用途（一句话）

查看集群里的**节点分组**——分组是"建分布表时数据往哪些 DN 落"的调度单位。

### 为什么要懂它

第 3 条 SQL 里会出现 `to group default_group`：建分布表时要指定"分发到哪个组"。这条查询就是先看一眼集群里有哪些组、每组包含哪些 DN。**两条 SQL 是配套的**：先查组（本条），再按组建表（第 3 条）。

**`pgxc_group` 的列结构有源码依据**（`src/include/catalog/pgxc_group.h` 第 28–38 行，逐字核对，共 3 列）：

| 列名 | 类型 | 含义 | 源码行 |
| --- | --- | --- | --- |
| `group_name` | `NameData` | 组名（如 `default_group`） | 第 28 行 |
| `default_group` | `int32` | 是否为默认组 | 第 30 行 |
| `group_members` | `oidvector` | 组内成员节点（DN 的 OID 列表） | 第 33 行 |

`Natts_pgxc_group` 定义为 3（第 38 行），即这张表只有这三列。`select *` 正是"先看全貌再细究列含义"的新手友好写法——健康检查脚本选择它，也是这个原因。

### 预期结果（未在实例上执行，格式示意）

| group_name | default_group | group_members |
| --- | --- | --- |
| `default_group` | （`1` 表示默认组，以实际为准） | （若干 DN 的 OID，以实际为准） |

至少包含一个名为 `default_group` 的分组（因为脚本第 57 行建表时引用了它；若该行查询结果为空，第 3 条 SQL 的建表会失败——这是两条 SQL 之间的依赖关系，属于静态代码推理，非实测）。

---

## 3. 分布式建表（新手最难懂的一条）★重点讲解

### 原文（`otb_health.sh` 第 55–57 行，逐字）

```sql
drop table if exists otb_health_check;
create table otb_health_check(id int, name text)
    distribute by shard(id) to group default_group;
```

### 用途（一句话）

建一张**分布表**：数据按 `id` 分片，打散存到 `default_group` 所辖的数据节点（DN）上——这是 OpenTenBase 区别于普通 PostgreSQL 的核心语法。

### 逐段解释

| 片段 | 含义 |
|---|---|
| `drop table if exists otb_health_check;` | 若同名表已存在（如上次运行残留）先删掉，保证脚本可重复执行 |
| `create table otb_health_check(id int, name text)` | 表结构：两列，`id` 整型、`name` 文本，与普通 PostgreSQL 建表完全一样 |
| `distribute by shard(id)` | **OpenTenBase 特有**：按 `id` 列做哈希分片（shard），不同 `id` 的行落到不同 DN |
| `to group default_group` | 分片落点：发到 `default_group` 这一组 DN（即第 2 条 SQL 查到的分组） |

### 新手为什么难懂：改写前 vs 改写后对比

**改写前**（初见者的困惑）：单看 `create table ... distribute by shard(id) to group default_group` 这一行，至少三个疑问——
1. `distribute by shard(id)` 是标准 SQL 吗？——**不是**，普通 PostgreSQL/MySQL 都没有这句；
2. `shard(id)` 里的 `id` 决定了什么？——决定每一行数据"住在哪台 DN"，选分布键 = 选数据摆放规则；
3. `to group default_group` 的 group 是什么？——DN 的分组名，不是库名也不是模式名。

**改写后**（本文的处理）：把一句话拆成"建表（熟悉的 PG 语法）+ 分片声明（OpenTenBase 扩展）+ 落点声明（组）"三段逐个解释，并配套第 2 条 SQL 让读者先亲眼看到 `default_group` 长什么样，再回来看建表语句——困惑点从"三个同时出现"变成"逐个击破"。

### 预期结果（未在实例上执行）

按脚本第 58–60 行的配套语句（`insert` 两行 → `select` 回读 → `drop` 清理），若一切正常：`select * from otb_health_check order by id;` 应返回 2 行（`1,'ok'` 和 `2,'good'`）。**此为静态推理，实测结果以在实例上运行脚本为准**；若你有环境，跑一遍 `bash otb_health.sh` 并把输出贴回本文，即可把"预期"升级为"实测"。

---

## 4. 一页速查

| # | SQL（规范化写法） | 干什么 | 来源行 |
|---|---|---|---|
| 1 | `SELECT node_name, node_type, node_port, node_host FROM pgxc_node ORDER BY node_type;` | 看集群有哪些节点 | `contrib/otb-health/otb_health.sh` 第 42 行 |
| 2 | `SELECT * FROM pgxc_group;` | 看节点分组（建分布表的前提） | 同上，第 47 行 |
| 3 | `CREATE TABLE otb_health_check(id int, name text) DISTRIBUTE BY SHARD(id) TO GROUP default_group;` | 建分布表 | 同上，第 55–57 行 |

> 规范化写法（大写 + 分号）与脚本原文语义等价，仅便于阅读；脚本原文为小写无分号（`psql -c` 调用形式）。

## 5. 本文的核对说明

- 3 条 SQL 的原文与行号，逐行核对自 `contrib/otb-health/otb_health.sh`（核对记录见 `evidence/sql-sources.md`）；
- CN/DN/GTM 分工表述，逐字引自仓库根目录 `README.md` 第 16 行；
- `node_type` 取值（第 1 节）来自 `src/include/nodes/parsenodes.h` 第 127–132 行；`pgxc_group` 列结构（第 2 节）来自 `src/include/catalog/pgxc_group.h` 第 28–38 行；
- 所有结果表均标注**预期结果（未在实例上执行）**；
- 试读反馈见 `evidence/peer-feedback.md`。
