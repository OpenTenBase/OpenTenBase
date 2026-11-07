# SQL 来源核对表（方向 1 依据）

> 核对方法：下载 `contrib/otb-health/otb_health.sh` 原始文件，逐行比对本文档引用的 SQL 与行号。
> 核对基准文件：上游仓库 `OpenTenBase/OpenTenBase` master 分支的 `contrib/otb-health/otb_health.sh`（2361 字节 / 65 行）。

## 逐条核对

| # | 文档中引用 | 脚本原文（逐字） | 脚本行号 | 一致性 |
|---|---|---|---|---|
| 1 | 节点查询 | `select node_name, node_type, node_port, node_host from pgxc_node order by node_type` | **42** | ✅ 逐字一致 |
| 2 | 分组查询 | `select * from pgxc_group` | **47** | ✅ 逐字一致 |
| 3 | 分布式建表 | `drop table if exists otb_health_check;`（55 行）＋ `create table otb_health_check(id int, name text)` ＋ `distribute by shard(id) to group default_group;`（56–57 行） | **55–57** | ✅ 逐字一致 |

## 写法差异说明（不是改动，是格式规范化）

脚本中 SQL 通过 `psql -c "..."` / heredoc 调用，因此为**小写、末尾无分号**；文档为便于阅读给出大写 + 分号的规范化写法，两者语义等价。文档已在相应位置注明此差异。

## 其他引用依据

| 文档中的表述 | 依据位置 | 核对结果 |
|---|---|---|
| "集群由 CN、DN、GTM 组成；用户数据都在 DN，CN 只存元数据，GTM 管全局事务" | 仓库根目录 `README.md` **第 16 行** | ✅ 逐字核对（原文为英文表述，文档为中文转述） |
| psql 连接参数（端口 11003 / 库 postgres） | `otb_health.sh` 第 21–24 行（默认值定义）、第 30 行（psql 调用） | ✅ |
| "单节点最小集群 1 GTM + 1 CN + 1 DN" | `otb_health.sh` 第 5–6 行注释 | ✅ |
| "default_group 依赖关系（若 [2] 查询为空则 [3] 建表失败）" | 静态代码推理：第 57 行 `to group default_group` 引用该组名 | 标注为推理，非实测 |
| `node_type` 取值 `C` / `D` / `S` / `N` / `G` | `src/include/nodes/parsenodes.h` **第 127–132 行** | ✅ 逐字核对 |
| `pgxc_group` 三列 `group_name` / `default_group` / `group_members` | `src/include/catalog/pgxc_group.h` **第 28、30、33 行**，`Natts_pgxc_group 3` 在第 38 行 | ✅ 逐字核对 |

## 明确标注的未验证项

- `pgxc_node` / `pgxc_group` 的**真实返回行**——源码只能证明常量定义与列结构，具体集群返回哪些行、哪些 port/host，**未在实例上执行，文档中已标注"以实际输出为准"**；
- 所有"预期结果"表格——**未在实例上执行**，全部如实标注。

> 说明：早期版本曾把 `node_type` 取值与 `pgxc_group` 列结构一并标为"未验证"。后续在源码中找到了确定依据（见上表最后两行），已从"未验证"升级为"源码可核验"，但不等同于运行验证。
