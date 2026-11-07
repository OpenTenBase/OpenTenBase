<!-- Copyright (C) 2026 OpenTenBase Authors. Licensed under BSD-3-Clause; see ../LICENSE.txt. -->

# 分布式事务说明：验证记录

对应[原理说明](distributed-transactions-zh.md)和[验证脚本](examples/verify_distributed_transactions.py)。

## 基线与环境

- 验证日期：2026-11-07。
- 文档源码核对：`123e2b3e8e2d6d0091ea82646d46432cb7a73682`，分支 `haijun` 从该提交创建。
- 运行环境：Ubuntu 22.04 VM，4 核、约 8 GB 内存，1 GTM + 1 CN + 2 DN，无备节点。
- VM 源码 HEAD：`ac54d240fb7f1ab22b849ced7a8460d20a8e849c`，已有构建兼容修改；不是文档核对提交的重新构建。
- 服务版本：`PostgreSQL 10.0 @ OpenTenBase_v5.0 OpenTenBase V5.21 2026-11-06 17:32:58 on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 11.4.0-1ubuntu1~22.04.3) 11.4.0, 64-bit`。
- 预先部署条件：节点 forward 端口已配置，节点上 `max_parallel_workers_per_gather=0`。本次未修改这些持久配置。
- 本次写入会话显式设置 `enable_2pc_optimization=off`；测试表为普通分片表，不使用临时表。

因此，下列结果是该 VM 上的行为证据；本文机制结论另以指定源码基线为依据，不将它表述成 master 二进制验证。

## 实际输出

```text
distribution dn0001: 492
distribution dn0002: 508
PASS physical row count: 1000
PASS commit count/sum: 1000|501500
PASS rollback sum: 501500
PASS REPEATABLE READ initial: 501500
PASS REPEATABLE READ after writer commit: 501500
PASS READ COMMITTED initial: 502500
PASS READ COMMITTED after writer commit: 503500
PASS all behavioral checks
cleanup: dedicated schema removed
```

脚本退出码为 0。Repeatable Read 与 Read Committed 两轮实验顺序执行，前一轮写者已提交的更新保留给后一轮，因此后一轮初始总和是 502500。分片计数可能随集群布局变化，以至少两个 DN 非空且总数为 1000 为断言，不硬编码 492/508。

## 如何解释结果

逐 DN 的 `EXECUTE DIRECT` 只用于实验读取物理行数；所有测试写入都经 CN 执行。两连接实验在第一次读取完成后才启动写者，并在写者提交成功后才发起第二次读取，不依赖固定 sleep。

这些结果验证了跨 DN 写入、正常提交、显式回滚和两种隔离级别的观察行为。它们没有抓取内部协议、暂停 prepared 阶段或触发故障，不单独证明运行时每个提交都执行了哪些内部函数。

尚未验证：多 CN、主备切换、崩溃与网络分区恢复、aarch64、性能指标、其他编译配置。本文不对这些情况作实验结论。
