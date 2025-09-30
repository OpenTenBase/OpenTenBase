# OpenTenBase 分布式状态视图 (`pg_dist_stat_views`)

## 概述 

`pg_dist_stat_views` 是一个专为OpenTenBase分布式数据库集群设计的PostgreSQL扩展。它提供了一套功能强大的分布式状态视图，旨在解决在多节点环境下监控和诊断性能问题的核心挑战。

在像OpenTenBase这样的分布式数据库中，一个用户的查询可能会涉及到跨越多个协调节点（CN）和数据节点（DN）的大量进程。标准的视图，如`pg_stat_activity`和`pg_locks`，仅能提供单个节点的状态快照，这迫使数据库管理员（DBA）和开发人员不得不手动登录多个节点，进行繁琐且极易出错的人工信息关联。

本扩展从根本上解决了这个问题，它提供了一个统一的、覆盖整个集群的视角。通过智能地关联和聚合状态信息，用户可以：

*   **端到端地追踪**分布式查询。
*   **快速定位性能瓶颈**和等待事件。
*   **分析复杂的锁等待链**并调查死锁问题。
*   **显著降低故障排查的时间和复杂度**。

## 核心功能 

本扩展提供了一套“三位一体”的诊断视图系统：

1.  **分布式活动视图:**
    *   `dist_pg_stat_activity`: 一个跨集群的、实时的后端活动视图。
    *   `dist_pg_stat_query_details`: 针对每个分布式事务的、经过排序的详细活动信息。
    *   **`dist_pg_stat_query_summary` (核心视图):** 一个高度聚合的摘要视图，将每个**活跃**分布式事务的状态、等待事件和后端角色浓缩为一行语义化信息。

2.  **分布式锁视图:**
    *   `dist_pg_locks_raw`: 提供从所有节点采集的、带有GXID和阻塞者信息的原始锁数据。
    *   `dist_pg_locks`: 一个干净的基础锁视图，它在原始数据基础上，补全了阻塞者节点名和人类可读的锁对象描述，并**过滤掉了冗余的底层锁**。
    *   `dist_pg_locks_all_info`: 一个将锁信息与等待者、阻塞者的活动信息完全连接的大而全视图。

3.  **高级锁分析视图:**
    *   **`dist_pg_lock_waits_summary` (核心视图):** 将每个**直接**的“等待-阻塞”关系，浓缩为一行摘要，并同时附加上等待双方在“物理现场”（DN）和“逻辑源头”（CN）的完整信息。
    *   **`dist_pg_lock_wait_chains` (核心视图):** 解决深度锁等待问题的终极武器。它能通过递归查询，穿透多层阻塞，直接识别出任何复杂锁竞争的**“核心阻塞者”**。
    *   `dist_pg_deadlocks`: 一个用于审计和分析分布式死锁模式的工具，作为内核内置死锁检测器的有力补充。

## 安装

### 1. 先决条件

在开始之前，请确保您已满足以下所有条件：

*   **一个正在运行的 OpenTenBase 集群：** 您的集群（包括所有协调节点和数据节点）必须已成功安装并处于可连接状态。
*   **OpenTenBase 源码访问权限：** 您必须能够访问用于编译当前集群版本的 OpenTenBase 源码树。本扩展需要从源码进行编译。
*   **编译工具链：** 您的操作系统必须安装了必要的编译工具，如 `make`, `gcc` 等。
*   **PostgreSQL 开发库：** 必须安装与您的 OpenTenBase 版本相匹配的开发头文件。

### 2. 安装步骤

#### 第 1 步：编译并安装扩展文件

此步骤通过 `contrib` 目录的统一编译逻辑，将 `pg_dist_stat_views` 与其他插件一起编译成动态链接库（`.so` 文件）和 SQL 控制文件，并安装到 OpenTenBase 的扩展目录中。

**此操作只需在集群中的任意一台装有完整源码的机器上执行一次。**

1. **进入contrib目录：**
   打开终端，切换到 OpenTenBase 源码树下的 `contrib` 目录。请将 `/path/to/opentenbase/source` 替换为您的实际源码路径。

   ```bash
   cd /path/to/opentenbase/source/contrib
   ```

2. **统一编译所有插件（含 pg_dist_stat_views）：**
   执行 make 命令，系统会自动遍历 SUBDIRS 列表中的所有插件目录（已包含 pg_dist_stat_views）,编译对应的 C 源码。

   ```bash
   make
   ```

3. **统一安装所有插件文件：**
   使用 sudo 执行 make install，此命令会将所有编译好的插件文件（含 pg_dist_stat_views 的 .so 文件、.control 文件和 SQL 脚本）复制到 OpenTenBase 的系统目录下（例如 `/usr/local/pgsql/lib` 和 `/usr/local/pgsql/share/extension`）。

   ```bash
   sudo make install
   ```

   > **提示 1**：如果 `make install` 后，其他节点无法找到扩展，您可能需要手动将生成的文件从这台机器复制到**所有其他协调节点和数据节点**的相应目录中。保持目录结构一致。

    > **提示 2**：若只需单独编译安装 `pg_dist_stat_views` 插件（无需编译其他插件），可直接进入该插件目录执行操作：
    >
    > ```bash
    > # 进入插件源码目录
    > cd /path/to/opentenbase/source/contrib/pg_dist_stat_views
    > 
    > # 单独编译该插件
    > make
    > 
    > # 单独安装该插件
    > sudo make install
    > ```
    >
    > 此方式适用于仅更新 `pg_dist_stat_views` 插件的场景，可节省编译时间。

#### 第 2 步：配置 `postgresql.conf` 文件

为了让 OpenTenBase 在启动时加载我们的扩展库，必须将其添加到 `shared_preload_libraries` 参数中。

**此操作必须在集群的【所有】协调节点（Coordinator）和数据节点（Datanode）上重复执行。**

1. **找到 `postgresql.conf` 文件：**
   如果您不确定文件的位置，可以连接到数据库后执行以下 SQL 命令查找：

   ```sql
   SHOW config_file;
   ```

2. **修改配置参数：**
   使用文本编辑器（如 `vim`）打开 `postgresql.conf` 文件，找到 `shared_preload_libraries` 这一行，将 `'pg_dist_stat_views'` 添加进去。

   ```conf
   # 示例：在 postgresql.conf 文件中
   
   # 如果该参数原先为空或被注释，则修改为：
   shared_preload_libraries = 'pg_dist_stat_views'
   
   # 如果该参数已存在其他库（如 pg_stat_statements），请使用逗号分隔，不要重复写参数名：
   shared_preload_libraries = 'pg_stat_statements,pg_dist_stat_views' 
   ```

   **注意：** 库名称是大小写敏感的，且用单引号包裹。

3. **保存并关闭文件。** 在每一个节点上重复此操作。

#### 第 3 步：重启整个集群

`shared_preload_libraries` 参数的更改只有在数据库服务完全重启后才会生效，仅仅重载配置（reload）是无效的。

使用您的集群管理工具来执行完整的停止和启动操作。

```bash
# 使用 pgxc_ctl 工具的示例
pgxc_ctl stop all
pgxc_ctl start all
```

如果您的集群使用 `systemd` 或其他服务管理器，请确保在所有节点上都执行了 `restart` 操作。

#### 第 4 步：在数据库中创建扩展

最后一步是在您需要使用此扩展的特定数据库中激活它。

1. **连接到目标数据库：**
   使用 `psql` 或其他客户端工具，连接到您希望监控的数据库。例如，连接到名为 `mydatabase` 的数据库。

   ```bash
   psql -d mydatabase
   ```

2. **执行 CREATE EXTENSION 命令：**
   此命令会运行扩展的 SQL 脚本，在数据库中创建所需的视图、函数等对象。

   ```sql
   CREATE EXTENSION pg_dist_stat_views;
   ```

### 3. 验证安装

安装完成后，执行以下检查以确保扩展已成功加载并正常工作：

1. **检查扩展是否已注册：**
   在您创建扩展的数据库中，使用 `\dx` 命令查看已安装的扩展列表。

   ```sql
   \dx pg_dist_stat_views
                                    List of installed extensions
           Name        | Version |   Schema   |                     Description
   --------------------+---------+------------+------------------------------------------------------
    pg_dist_stat_views | 1.0     | public     | distributed statistics views for activities and locks
   (1 row)
   ```

   看到如上输出，表示扩展已成功创建。

2. **测试功能视图：**
   尝试查询由该扩展创建的某个分布式视图，例如 `dist_pg_stat_activity;`。如果能返回数据（即使是空数据），说明扩展已在正常工作。

   ```sql
   SELECT count(*) FROM dist_pg_stat_activity;;
    count
   -------
       20
   (1 row)
   ```

### 4. 卸载扩展

如果您需要卸载此扩展，请按以下顺序操作：

1.  **在数据库中删除扩展：**
    `DROP EXTENSION pg_dist_stat_views;`
2.  **从 `postgresql.conf` 中移除配置：**
    在所有节点的 `postgresql.conf` 文件中，从 `shared_preload_libraries` 参数里删除 `pg_dist_stat_views`。
3.  **重启整个集群：**
    执行一次完整的集群停止和启动。
4.  **(可选) 清理文件：**
    在编译机上进入源码目录，执行 `sudo make uninstall` 来移除已安装的文件。

## 用法与视图参考

### `dist_pg_stat_query_summary`

这是识别慢速或有问题的分布式查询的主要工具。每一行代表一个当前活跃的分布式事务。

**查询示例:**

```sql
SELECT * FROM dist_pg_stat_query_summary;
```

**示例输出：**

```sql
testdb=# select * from dist_pg_stat_query_summary;
 global_query_id           |                  top_level_query                  |  username   | application_name | client_address | total_duration | involved_processes | distinct_nodes | cluster_xmin_horizon |                                       states_summary                                       |                                         waits_summary                                         |                                     backends_summary                                     
---------------------------+---------------------------------------------------+-------------+------------------+----------------+----------------+--------------------+----------------+----------------------+------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------
 cn001-934244-812553358718868 | SELECT COUNT(*), pg_sleep(5)                      +| opentenbase | psql             | 10.132.221.101      | 00:00:02.260   |                  7 |              3 |                  643 | active(7){cn001:7082,dn001:7121,dn001:7134,dn001:7136,dn002:7123,dn002:7174,dn002:7175} | ClientRead(4){dn001:7134,dn001:7136,dn002:7174,dn002:7175}, PgSleep(2){dn001:7121,dn002:7123} | coordinator-client backend(1){7082}, datanode-client backend(6){7121,7123,7134,7136,7174,7175}
                           | FROM (                                            +|             |                  |                |                |                    |                |                      |                                                                                          |                                                                                               | 
                           |     SELECT t1.id, pg_sleep(0.01)                  +|             |                  |                |                |                    |                |                      |                                                                                          |                                                                                               | 
                           |     FROM my_dist_table AS t1                      +|             |                  |                |                |                    |                |                      |                                                                                          |                                                                                               | 
                           |     JOIN my_dist_table AS t2 ON t1.id = t2.id + 1 +|             |                  |                |                |                    |                |                      |                                                                                          |                                                                                               | 
                           |     WHERE t1.id < 1000                            +|             |                  |                |                |                    |                |                      |                                                                                          |                                                                                               | 
                           | ) AS subquery;                                     |             |                  |                |                |                    |                |                      |                                                                                          |                                                                                               | 
 cn001-7088-808973663463129 | select * from dist_pg_stat_query_summary;          | opentenbase | psql             | 10.132.221.101      | 00:00:00       |                  4 |              4 |                  643 | active(4){cn001:7088,cn002:7194,dn001:7155,dn002:7157}                                   |                                                                                               | coordinator-client backend(2){7088,7194}, datanode-client backend(2){7155,7157}
(2 rows)
```

**关键列说明:**

*   `global_query_id`: 全局查询标识。这是关联所有活动的核心键。
*   `top_level_query`: 在协调节点上发起的、当前正在执行的SQL文本。
*   `username`, `client_address`: 发起该事务的用户和客户端信息。
*   application_name: 一个由客户端应用程序在连接时设置的字符串，用于标识连接来源。
*   `total_duration`: 从事务开始到现在的总耗时。
*   `involved_processes`, `distinct_nodes`: 参与此事务的总进程数和节点数。
*   cluster_xmin_horizon: 一个与事务可见性和垃圾回收（VACUUM）相关的集群级指标。
*   `states_summary`: 聚合字符串，展示所有相关进程的状态分布 (例如: `active(3){...}, idle(2){...}`)。
*   `waits_summary`: 聚合字符串，展示当前所有等待事件，是瓶颈分析的关键 (例如: `ClientRead(2){...}, Lock(1){...}`)。
*   `backends_summary`: 聚合字符串，描述所有参与后端的角色 (例如: `coordinator-client backend(1){...}, datanode-parallel worker(4){...}`)。

---

### `dist_pg_lock_waits_summary`

此视图为每个直接的锁等待关系提供一行摘要，并富含源头信息。

**查询示例:**

```sql
SELECT * FROM dist_pg_lock_waits_summary;
```

**示例输出:**

```sql
testdb=# select * from dist_pg_lock_waits_summary;
 waiter_gxid | blocking_gxid | waiter_source_node | waiter_user_source | waiter_client_source |                    waiter_query_source                    | blocker_source_node | blocker_user_source | blocker_client_source |                    blocker_query_source                     |                                                                              wait_scene_details                                                                               |        locks_waited        
-------------+---------------+--------------------+--------------------+----------------------+-----------------------------------------------------------+---------------------+---------------------+-----------------------+-------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------
 0:188:1     | 1:199:7       | cn001              | opentenbase        | 127.0.0.1            | UPDATE my_dist_table SET info = 'also by A' WHERE id = 2; | cn002               | opentenbase         | 127.0.0.1             | UPDATE my_dist_table SET info = 'also by B' WHERE id = 3;   | [{"waiter_pid": 15512, "blocking_pid": 15553, "waiter_state": "active", "blocking_state": "idle in transaction", "wait_happen_node": "dn001", "blocking_node_name": "dn001"}] | ShareLock{transaction 619}
 1:188:1     | 0:199:7       | cn002              | opentenbase        | 127.0.0.1            | UPDATE my_dist_table SET info = 'also by B' WHERE id = 3; | cn001               | opentenbase         | 127.0.0.1             | UPDATE my_dist_table SET info = 'locked by C' WHERE id = 3; | [{"waiter_pid": 15556, "blocking_pid": 15552, "waiter_state": "active", "blocking_state": "idle in transaction", "wait_happen_node": "dn002", "blocking_node_name": "dn002"}] | ShareLock{transaction 619}
(2 rows)
```

**关键列说明:**

*   `waiter_gxid`, `blocking_gxid`: 等待和阻塞事务的GXID。
*   `waiter_source_node`, `waiter_user_source`, `waiter_query_source`: 等待事务的“源头”信息（来自哪个CN，哪个用户，哪个顶层查询）。
*   `blocker_source_node`, `blocker_user_source`, `blocker_query_source`: 阻塞事务的“源头”信息。
*   `wait_scene_details`: 一个JSONB数组，详细描述了锁等待的“现场”（在哪个DN，哪个PID，以及它们的当前状态）。
*   `locks_waited`: 聚合字符串，列出了所有正在等待的具体锁（例如: `RowExclusiveLock{my_table}, ExclusiveLock{row in my_table}`）。

---

### `dist_pg_lock_wait_chains`

分析复杂、多级锁竞争的终极工具。每一行代表一条从最初的等待者到根源阻塞者的完整等待链。

**查询示例:**

```sql
SELECT * FROM dist_pg_lock_wait_chains;
```

**示例输出：**

```sql
testdb=# select * from dist_pg_lock_wait_chains;
 chain_length |     full_wait_chain_gxid      |           full_wait_chain_pids            | chain_head_source_node | chain_head_user | chain_head_gxid | root_blocker_source_node | root_blocker_user | root_blocker_gxid |                     chain_head_query                      |                     root_blocker_query                      
--------------+-------------------------------+-------------------------------------------+------------------------+-----------------+-----------------+--------------------------+-------------------+-------------------+-----------------------------------------------------------+-------------------------------------------------------------
            1 | 0:188:1 -> 1:199:7            | 47650:dn002 -> 47594:dn002                | cn001                  | opentenbase     | 0:188:1         | cn001                    | opentenbase       | 1:199:7           | UPDATE my_dist_table SET info = 'also by B' WHERE id = 4; | UPDATE my_dist_table SET info = 'locked by C' WHERE id = 4;
            2 | 1:198:2 -> 0:188:1 -> 1:199:7 | 47671:dn002 -> 47650:dn002 -> 47594:dn002 | cn002                  | opentenbase     | 1:198:2         | cn001                    | opentenbase       | 1:199:7           | UPDATE my_dist_table SET info = 'also by A' WHERE id = 3; | UPDATE my_dist_table SET info = 'locked by C' WHERE id = 4;
(2 rows)
```

**关键列说明:**

*   `chain_length`: 等待链的深度 (例如: 值为2代表一个3级链 A->B->C)。
*   `full_wait_chain_gxid`: 由GXID表示的、完整的逻辑等待路径。
*   `full_wait_chain_pids`: 由`pid:nodename`表示的、完整的物理等待路径。
*   `chain_head_*` 列: 关于链条“头部”事务的信息。
*   **`root_blocker_*` 列: (最重要)** 关于链条“根源”（导致整个链条阻塞的最终原因）事务的信息。

## 回归测试

本扩展包含一套测试，用于验证其功能和稳定性。测试分为两部分：一套用于语法和基础逻辑的自动化测试，以及一套用于复杂并发场景的手动测试指南。

### 1. 自动化基础测试 (`make check`)

这套测试是验证扩展基本功能是否健全的第一道防线。

在 `contrib/pg_dist_stat_views` 目录下，运行:

```bash
make check
```

**测试流程会自动：**

1.  创建一个临时的、隔离的 OpenTenBase 集群。
2.  运行一系列 `.sql` 脚本，验证所有已创建视图的语法、列定义和在无并发下的基本查询逻辑。
3.  将输出与预期的结果进行比对，确保没有基础层面的错误。

**重要限制：**

`make check` 在一个受控的、单线程的测试环境中运行。因此，它**无法**模拟多事务之间的真实并发冲突，例如锁等待、死锁等场景。专门用于诊断此类问题的视图（如 `dist_pg_lock_waits_summary` 和 `dist_pg_lock_wait_chains`）在 `make check` 中只会进行基本的语法检查，而不会验证其核心的并发诊断逻辑。

要验证这些高级视图，请遵循下面的手动测试指南。

### 2. 手动并发测试 (锁等待链场景)

此测试旨在验证 `dist_pg_lock_wait_chains` 视图能否准确地识别、追踪并展示一个多级锁等待链。

#### 场景描述

我们将手动构造一个三级锁等待链 (A → B → C)，其中：

*   事务 C 持有一个行锁。
*   事务 B 等待事务 C 释放该锁。
*   事务 A 等待事务 B 释放该锁。

我们将使用第四个会话作为观察者，查询视图并验证结果。

#### 准备工作

1. **准备测试表:** 在您的测试数据库中，创建一个简单的表并插入一行数据。

   ```sql
   CREATE TABLE lock_test (id INT PRIMARY KEY, data TEXT);
   INSERT INTO lock_test VALUES (1, 'initial data');
   ```

2. **打开四个终端会话:** 每个终端都使用 `psql` 连接到**一个协调节点**和**同一个数据库**。我们将它们分别标记为 **Session 1 (C)**, **Session 2 (B)**, **Session 3 (A)**, 和 **Session 4 (Observer)**。

#### 测试步骤

**第 1 步: 在 Session 1 (C) 中，持有锁**
这个会话将扮演根阻塞者。

```sql
-- Session 1 (C)
BEGIN;
UPDATE lock_test SET data = 'locked by C' WHERE id = 1;

-- 注意：不要 COMMIT 或 ROLLBACK！
-- 此事务现在持有了 id=1 这一行的排他锁。
```

**第 2 步: 在 Session 2 (B) 中，等待 C**
这个会话将成为等待链的中间环节。

```sql
-- Session 2 (B)
BEGIN;
UPDATE lock_test SET data = 'locked by B' WHERE id = 1;

-- 此命令将会“卡住”，因为它正在等待 Session 1 (C) 释放锁。
```

**第 3 步: 在 Session 3 (A) 中，等待 B**
这个会话是等待链的最前端，是最初的受害者。

```sql
-- Session 3 (A)
BEGIN;
UPDATE lock_test SET data = 'locked by A' WHERE id = 1;

-- 此命令同样会“卡住”，等待队列在 Session 2 (B) 之后。
```

**第 4 步: 在 Session 4 (Observer) 中，观察结果**
现在，锁等待链已经形成。使用观察者会话查询 `dist_pg_lock_wait_chains` 视图。

```sql
-- Session 4 (Observer)
SELECT 
    chain_length,
    full_wait_chain_gxid,
    root_blocker_gxid,
    root_blocker_user_source,
    root_blocker_query_source,
    root_blocker_state
FROM 
    dist_pg_lock_wait_chains;
```

#### 预期输出

您应该会看到类似下面的一行结果（GXID 会有所不同）：

```sql
testdb=# SELECT * FROM dist_pg_lock_wait_chains;
 chain_length |       full_wait_chain_gxid       |             full_wait_chain_pids             | chain_head_source_node | chain_head_user | chain_head_gxid | root_blocker_source_node | root_blocker_user | root_blocker_gxid |                    chain_head_query                     |                   root_blocker_query                    
--------------+----------------------------------+----------------------------------------------+------------------------+-----------------+-----------------+--------------------------+-------------------+-------------------+---------------------------------------------------------+---------------------------------------------------------
            1 | 0:1992:1 -> 0:1995:4             | 464254:dn001 -> 464255:dn001                 | cn001                  | opentenbase     | 0:1992:1        | cn001                    | opentenbase       | 0:1995:4          | UPDATE lock_test SET data = 'locked by B' WHERE id = 1; | UPDATE lock_test SET data = 'locked by C' WHERE id = 1;
            2 | 1:1994:1 -> 0:1992:1 -> 0:1995:4 | 464275:dn001 -> 464254:dn001 -> 464255:dn001 | cn002                  | opentenbase     | 1:1994:1        | cn001                    | opentenbase       | 0:1995:4          | UPDATE lock_test SET data = 'locked by A' WHERE id = 1; | UPDATE lock_test SET data = 'locked by C' WHERE id = 1;
(2 rows)
```

**结果分析:**

*   `chain_length`: 值为 `2`，正确地表示了这是一个包含3个事务、2个等待环节的链条 (A→B, B→C)。
*   `full_wait_chain_gxid`: 清晰地展示了从链头(1:1994:1)到链根(0:1995:4)的完整GXID路径。
*   `root_blocker_gxid`: 准确地识别出 `0:1995:4`（即我们的 Session 1 (C)）是导致整个链条阻塞的**根源**。
*   `root_blocker_*` 列: 提供了根阻塞者的详细信息，包括其来源用户、GXID、执行的查询。

这个输出证明了 `dist_pg_lock_wait_chains` 视图成功地诊断了复杂的多级锁等待问题。

#### 清理

测试完成后，为了释放所有锁，请按顺序在会话中执行 `ROLLBACK`。

1.  **在 Session 1 (C) 中:** `ROLLBACK;` (这会立刻解锁 Session 2)。
2.  **在 Session 2 (B) 和 Session 3 (A) 中:** 它们的 `UPDATE` 命令会执行失败（因为事务回滚），然后执行 `ROLLBACK;` 即可。