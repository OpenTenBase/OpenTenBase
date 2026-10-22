# 分布式事务原理

>[高级使用](04-advanced-use.md) 的「事务使用」一节介绍了 `begin`、`commit`、`rollback`、隔离级别与行锁的**用法**。本节不重复这些操作，而是解释它们在集群内部**是怎么实现的**：全局事务号从哪里来、快照为什么必须是全局的、一个跨数据节点的写事务如何保证原子提交。

## 1、角色分工与问题的由来

### 1.1、三大角色在事务中的职责

OpenTenBase 的三大角色（详见[快速入门](01-quickstart.md)）在事务处理中的分工如下：

| 角色                  | 在事务中承担什么                                             | 不承担什么                                   |
| --------------------- | ------------------------------------------------------------ | -------------------------------------------- |
| CN（协调节点）        | 事务入口；申请全局事务号；为跨节点事务充当两阶段提交的协调者；提交阶段向 GTM 申请全局提交序号并下发给 DN | 不存储业务数据；不单独决定元组对用户的可见性 |
| DN（数据节点）        | 持有所属分片的元组；执行 CN 分发的计划；记录本地事务状态与提交序号 | 不能独立决定分布式事务的最终提交或回滚       |
| GTM（全局事务管理器） | 分配全局事务号 GXID；提供全局快照；分配全局提交时间戳（CSN）；管理序列等全局对象 | 不参与两阶段提交的表决；不写业务数据         |

一个容易产生的误解是「GTM 负责提交事务」。实际上 GTM 只做两件与事务相关的事：**分配编号**（GXID、CSN）和**回答可见性问题**（提供全局快照）。提交动作本身发生在 CN 与 DN 之间。

### 1.2、一个跨节点写事务要解决的三个问题

假设 `UPDATE` 命中了两条分别位于 DN1、DN2 的记录，事务提交时必须同时满足：

1. **原子性**：两个 DN 要么都提交，要么都回滚，不能出现只提交一个的情况；
2. **全局可比的事务标识**：需要一个跨节点唯一、可比较的编号，用来判断「这个事务和我谁先提交」；
3. **一致的可见性判断**：在 DN1 上读到 DN2 的数据时，必须用同一套快照口径判断该元组是否可见。

对应到实现上，就是后面的第 2 节（GXID）、第 3 节（全局快照）、第 4 节（CSN）和第 5 节（两阶段提交）。

## 2、全局事务号（GXID）

### 2.1、为什么需要 GXID

每个 DN 在内核层面都有原生的本地 XID（`TransactionId`），与原生 PostgreSQL 一致，由本地共享内存的计数器分配。但不同 DN 的本地 XID 空间彼此独立，`400` 号在 DN1 和在 DN2 上代表完全不同的两个事务，**无法跨节点比较**。

因此集群需要一个全局唯一的编号，这就是 GXID（`GlobalTransactionId`）。它被用于两处关键场景：

- GTM 维护的全局快照中，进行中的事务以 GXID 列表的形式记录；
- 两阶段提交过程中，标识「同一个事务在各个节点上的 prepare 记录」。

### 2.2、GXID 的分配路径

GXID 由 GTM 单调递增地集中分配，CN 通过 GTM 客户端接口申请：

| 环节                 | 位置                                                         |
| -------------------- | ------------------------------------------------------------ |
| CN 向 GTM 申请事务号 | `BeginTranGTM()`，`src/backend/access/transam/gtm.c`         |
| 客户端接口声明       | `src/include/access/gtm.h`                                   |
| GTM 端受理并分配     | `GTM_BeginTransaction()`、`GTM_GetGlobalTransactionId()`，`src/gtm/main/gtm_txn.c` |

需要注意，申请 GXID 是一次**跨进程的网络交互**。这也是 GTM 会成为事务路径上性能敏感点的原因：事务并发越高，GTM 处的压力越集中。

### 2.3、GXID 与本地 XID 的关系

两者并存，各管一段：

- **本地 XID** 仍然存在，继续负责页级的事务状态、子事务（subtransaction）、clog 记录等原生 PostgreSQL 语义；
- **GXID** 只负责跨节点的协调语义。

一个事务同时拥有这两种身份，二者通过 1:1 的映射关系衔接，因此不能用本地 XID 去替代 GXID 做跨节点判断。

## 3、全局快照与读一致性

### 3.1、快照从哪里来

在原生 PostgreSQL 中，快照由 backend 自己扫描 `ProcArray` 生成。而在 OpenTenBase 中，协调节点需要一份**覆盖全集群**的快照，因此改为向 GTM 申请：

| 环节                     | 位置                                                         |
| ------------------------ | ------------------------------------------------------------ |
| CN 构造快照时向 GTM 申请 | `GetSnapshotData()` 中的 `GetSnapshotGTM()`，`src/backend/storage/ipc/procarray.c` |
| GTM 客户端实现           | `GetSnapshotGTM(gxid, canbe_grouped)`，`src/backend/access/transam/gtm.c` |
| GTM 端生成快照           | `GTM_GetTransactionSnapshot()`，`src/gtm/main/gtm_snap.c`    |

### 3.2、快照的内容

GTM 返回的快照结构与 PostgreSQL 的 `xip` 快照同构，只是编号空间换成了 GXID：

``` c
typedef struct GTM_SnapshotData
{
    GlobalTransactionId     sn_xmin;
    GlobalTransactionId     sn_xmax;
    uint32                  sn_xcnt;
    GlobalTransactionId    *sn_xip;
} GTM_SnapshotData;
```

其中 `sn_xip` 是**申请快照这一刻仍在进行中的 GXID 列表**，`sn_xmin`/`sn_xmax` 界定其范围。有了这三项，任意一个元组的事务号都可以被判断为「已提交」「进行中」或「未来」，从而得到与单机一致的可见性语义。

### 3.3、快照如何到达 DN

DN 执行 CN 下发的计划时，并不各自去 GTM 取一次快照（那样会在同一事务的多条语句间产生口径偏差），而是由 CN 把快照随语句一起下发：

- CN 侧下发逻辑位于 `pgxc_node_send_snapshot()`（`src/backend/pgxc/pool/pgxcnode.c`）；
- 快照来源在代码中区分为 `SNAPSHOT_COORDINATOR`（从 CN 接收）与 `SNAPSHOT_DIRECT`（直接向 GTM 取得）两种。

这一点是理解「为什么在 DN 上手工执行 `select` 和通过 CN 查询结果可能不同」的关键：绕过 CN 直连 DN，等于绕过了快照下发链路。

### 3.4、不同隔离级别的差别

隔离级别决定了快照的**复用范围**：

- **READ COMMITTED**：每条语句开始时取一个新快照，因此同一事务内的两条相同查询可能看到不同结果；
- **REPEATABLE READ**：事务内首次取快照后固定复用，事务全程使用同一份快照，从而保证可重复读。

这与单机行为一致，区别只在于快照的生成者从「本地 ProcArray」换成了「GTM + CN 下发」。可参考[高级使用](04-advanced-use.md)中的 4.4 节查看实际操作示例。

## 4、提交序号（CSN）与 MVCC 可见性

### 4.1、为什么还需要 CSN

仅有 GXID 和快照还不够。判断可见性时要回答的问题是「事务 A 是否早于我的快照提交」。如果各 DN 各自用自己的本地事务状态（clog）来判断，得到的结论在跨节点场景下**不具备可比性**——因为各节点的提交顺序并不一致。

解决方式是给每个已提交的事务分配一个**全局单调递增的提交序号** CSN（Commit Sequence Number），所有可见性判断统一比较 CSN：

- 提交序号记录在 `pg_csnlog` 中（`src/backend/access/transam/csnlog.c`）；
- 可见性判断在 `src/backend/utils/time/tqual.c` 中通过 `CSNLogGetCSNAdjusted()` 取出元组事务的 CSN，再与快照的提交序号比较。

### 4.2、CSN 的分配：由 CN 统一取号

CSN 来自 GTM 的全局提交时间戳，分配时机在两阶段提交的第二阶段、向各节点下发提交命令之前：

| 环节                                    | 位置                                                         |
| --------------------------------------- | ------------------------------------------------------------ |
| CN 在提交阶段向 GTM 取全局提交时间戳    | `GetGlobalTimestampGTMForCommit()`，调用点在 `pgxc_node_remote_finish()`，`src/backend/pgxc/pool/execRemote.c` |
| DN 局部提交时取号并写入 csnlog          | `CSNLogAssignCSN()` → `GetGlobalTimestampGTMForCommit()`，`src/backend/access/transam/csnlog.c` |
| prepare 阶段先写入带 prepare 标记的 CSN | `CSNLogSetCSN(...)` 配合 `MASK_PREPARE_BIT`，`src/backend/access/transam/xact.c` |

也就是说，**一个分布式事务的提交序号由 CN 统一向 GTM 申请，再随提交命令传给各参与节点**，而不是各节点各自取号。这一点保证了同一事务在所有节点上拥有相同的 CSN，从而使跨节点可见性判断自洽。

### 4.3、CSN 与 clog 的关系

社区中有一种说法是「新版本用 csnlog 替代了 clog」。从源码看，**这个说法不准确**，两者是并存的：

| 组件                                              | 记录什么                               | 是否仍在用                                        |
| ------------------------------------------------- | -------------------------------------- | ------------------------------------------------- |
| `clog`（`src/backend/access/transam/clog.c`）     | 事务的最终状态：已提交、已回滚、进行中 | 是，`xact.c` 中仍调用 `TransactionIdCommitTree()` |
| `csnlog`（`src/backend/access/transam/csnlog.c`） | 事务的提交序号，以及子事务的父事务映射 | 是，MVCC 可见性判断与提交顺序比较依赖它           |

更准确的表述是：**clog 回答「事务最终是什么状态」，csnlog 回答「事务在什么位置提交」**。分布式 MVCC 需要后者来做跨节点的顺序比较，因此 csnlog 是分布式事务的必要补充，而不是对 clog 的替换。

## 5、跨节点写事务的提交流程

### 5.1、协调者是 CN

跨节点写事务采用**两阶段提交（2PC）**，协调者是 CN，而不是 GTM。

判断依据很直接：CN 侧的执行模块会向参与节点直接发送标准的 `PREPARE TRANSACTION` 命令，命令文本在 `src/backend/pgxc/pool/execRemote.c` 中拼接：

``` c
sprintf(prepare_cmd, "PREPARE TRANSACTION '%s'", prepareGID);
```

`gid` 即该事务在集群范围内的全局标识，与[pg_clean 使用说明](10-pg_clean.md)中看到的 `gid` 是同一个概念。CN 侧的 2PC 状态机集中在 `g_twophase_state` 中维护，参与节点分为数据节点与协调节点两类（`twophase_add_participant()`）。

### 5.2、第一阶段：PREPARE TRANSACTION

CN 在收到应用的 `COMMIT` 后：

1. 向事务涉及的**每一个** DN（以及需要参与的其他 CN）发送 `PREPARE TRANSACTION '<gid>'`；
2. 参与节点把事务的修改落盘、释放锁之外的资源，并进入 prepared 状态，此时数据已「就绪但未生效」；
3. 各节点返回准备结果。

关于资源位，`max_prepared_transactions` 决定了单个节点上可同时存在的 prepared 事务数量（默认远大于常规并发需求，可在[组件安装及管理](05-component.md)中调整）。该值被占满时，prepare 会失败，进而导致事务回滚。

### 5.3、第二阶段：COMMIT PREPARED

当**全部**参与节点都返回准备成功后，CN 才进入第二阶段：

1. 向 GTM 申请全局提交时间戳，作为本事务的 CSN；
2. 向各参与节点下发 `COMMIT PREPARED`，并携带该 CSN；
3. 各节点把事务标记为已提交、写入 CSN，事务对后续快照可见。

只有在此时，数据才真正对外可见。这也解释了为什么在两阶段提交的中间态上，应用看不到「半个事务」的结果。

### 5.4、异常与残留

任何一步失败都会走回滚路径，而不是部分提交：

- **第一阶段有节点 prepare 失败**：CN 向已 prepare 成功的节点发送 `ROLLBACK PREPARED`，整个事务回滚；
- **第二阶段有节点未收到或提交失败**：事务在部分节点上停留在 prepared 状态，形成 2PC 残留。此时各节点上的数据尚未生效，但资源被占用。

残留记录的排查与清理方法见 [pg_clean 使用说明](10-pg_clean.md)：残留的 prepare 记录会体现在 `pg_2pc` 目录与集群的两阶段事务视图中，并且可以通过全局事务状态与各节点状态逐一比对后清理。

### 5.5、不需要 2PC 的场景

并非所有事务都要付出两阶段提交的代价。若一个写事务只涉及**单个 DN**（例如只命中单个分片，或操作的是未分片的普通表且只落在一个 DN 上），CN 无需协调多节点表决，可以直接提交。

这从侧面说明了两件事：

- 分片键设计会直接影响事务成本。跨分片的事务才需要 2PC，这也是[基本使用](03-basic-use.md)中「分片键选择准则」强调避免分布式事务的原因；
- 在 CN 上可以观察到两类事务的统计区分：代码中分别计数了 `autocommit_2pc` 与 `nonautocommit_2pc`（`src/backend/pgxc/pool/execRemote.c`），可用于判断业务的实际跨节点比例。

## 6、完整时序

下面是一次跨两个 DN 的写事务的完整时序（`GTM` 列表示 GTM 侧承担的动作）：

``` text
应用        CN（协调者）                    GTM                DN1     DN2
 |             |                            |                  |       |
 |  BEGIN      |                            |                  |       |
 |------------>| BeginTranGTM()             |                  |       |
 |             |--------------------------->| 分配 GXID        |       |
 |             |<---------------------------|                  |       |
 |  UPDATE     | GetSnapshotGTM()           |                  |       |
 |------------>|--------------------------->| 全局快照         |       |
 |             |  下发计划 + 快照            |                  |       |
 |             |----------------------------------------------->|       |
 |             |----------------------------------------------->|       |
 |  COMMIT     |                            |                  |       |
 |------------>| 阶段一：PREPARE TRANSACTION |                  |       |
 |             |----------------------------------------------->|       |
 |             |----------------------------------------------->|       |
 |             |<---------------- 全部 prepare 成功 -------------|       |
 |             | GetGlobalTimestampGTMForCommit()               |       |
 |             |--------------------------->| 分配全局 CSN     |       |
 |             | 阶段二：COMMIT PREPARED + CSN                  |       |
 |             |----------------------------------------------->|       |
 |             |----------------------------------------------->|       |
 |<------------| 返回提交成功                |                  |       |
```

## 7、常见问题澄清

| 常见说法                      | 是否准确       | 说明                                                         |
| ----------------------------- | -------------- | ------------------------------------------------------------ |
| 跨 DN 的写事务走两阶段提交    | 准确           | 由 CN 作为协调者，先 `PREPARE TRANSACTION`，全部成功后再 `COMMIT PREPARED` |
| 2PC 的协调者是 GTM            | 不准确         | GTM 只分配编号和提供快照；协调逻辑在 CN 的执行模块中         |
| GTM 负责提交事务              | 不准确         | 提交动作发生在 CN 与 DN 之间，GTM 只回答「谁先提交」这类顺序问题 |
| 全局事务号由 GTM 分配         | 准确           | 通过 `BeginTranGTM()` 申请，GTM 端单调递增分配               |
| 读取时向 GTM 申请全局一致快照 | 准确，但有细节 | CN 向 GTM 取快照，再随语句下发给 DN，DN 通常不各自取快照     |
| csnlog 替代了 clog            | 不准确         | 两者并存：clog 记最终状态，csnlog 记提交序号，用于跨节点可见性比较 |
| 单节点事务也要 2PC            | 不准确         | 只涉及单个 DN 的写事务可以直接提交，无需两阶段表决           |

## 8、对运维与设计的影响

- **GTM 是关键路径节点**。事务开启与提交阶段都需要与 GTM 交互，GTM 需要主备部署并保持低延迟，主备切换方式见[主备切换](06-switching.md)。
- **跨分片写入有额外成本**。涉及多个 DN 的写事务需要两阶段提交，多两次网络往返与一次落盘；对延迟敏感的业务应优先让事务落在单个分片内。
- **注意 prepare 残留**。异常场景下 prepared 事务会占用资源，需要定期用 pg_clean 检查，避免达到 `max_prepared_transactions` 上限后写入受阻。
- **直连 DN 会绕开快照下发**。绕过 CN 直接在 DN 上查询，得到的可见性结论可能与通过 CN 查询不同，不应用于业务判断。

## 9、源码索引

| 主题                                           | 文件                                                         |
| ---------------------------------------------- | ------------------------------------------------------------ |
| GTM 事务管理（GXID 分配、prepare/commit 状态） | `src/gtm/main/gtm_txn.c`                                     |
| GTM 快照生成                                   | `src/gtm/main/gtm_snap.c`                                    |
| CN 侧 GTM 客户端（取号、取快照、取提交时间戳） | `src/backend/access/transam/gtm.c`、`src/include/access/gtm.h` |
| CN 构造快照                                    | `src/backend/storage/ipc/procarray.c`                        |
| 远端语句下发、2PC 协调、快照下发               | `src/backend/pgxc/pool/execRemote.c`、`src/backend/pgxc/pool/pgxcnode.c` |
| 两阶段提交的本体实现（单机语义）               | `src/backend/access/transam/twophase.c`                      |
| 事务提交与 CSN 写入                            | `src/backend/access/transam/xact.c`                          |
| CSN 日志                                       | `src/backend/access/transam/csnlog.c`                        |
| MVCC 可见性判断                                | `src/backend/utils/time/tqual.c`                             |
| 两阶段事务状态结构                             | `src/include/gtm/gtm_c.h`                                    |

## 10、延伸阅读

- [快速入门](01-quickstart.md)：集群架构与三大角色的基本定义
- [基本使用](03-basic-use.md)：分片表与复制表，以及分片键选择准则
- [高级使用](04-advanced-use.md)：事务用法、隔离级别与行锁
- [组件安装及管理](05-component.md)：GTM 主备部署与 `max_prepared_transactions` 等参数
- [pg_clean 使用说明](10-pg_clean.md)：两阶段事务残留的排查与清理
