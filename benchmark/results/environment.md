# 测试环境

采集日期：2026-07-21

| 项目 | 值 |
| --- | --- |
| Git commit | `b612d77cbfd4d762f20c54c35f7caf09d57ef098` |
| OpenTenBase | PostgreSQL 10.0 @ OpenTenBase_v5.0 |
| pgbench | PostgreSQL 10.0 @ OpenTenBase_v5.0 |
| CPU | Intel Core i5-10500 @ 3.10 GHz |
| CPU 核心 | 6 核、12 逻辑 CPU |
| 内存 | 15 GiB |
| 操作系统 | Ubuntu 20.04 系列，Linux 5.13.0-35-generic |
| 部署方式 | 单机多进程，节点地址均为 127.0.0.1 |
| 数据库 | `database` |

## 拓扑

| 节点 | 类型 | 服务端口 | Forward 端口 |
| --- | --- | ---: | ---: |
| cn0001 | CN | 11003 | 11005 |
| dn0001 | DN | 11006 | 11008 |
| dn0002 | DN | 11009 | 11011 |
| dn0003 | DN | 11012 | 11014 |
| gtm0001 | GTM | 11000 | 不适用 |

```text
pgbench
   |
   v
cn0001
   |
   +-- dn0001
   +-- dn0002
   +-- dn0003
   |
   +-- gtm0001
```

所有组件共享同一台机器的 CPU、内存和磁盘，因此本报告不能测量真实节点间
网络性能，也不能仅凭整机 CPU 精确区分 CN 和 DN 的消耗。

其中，CN 是接收客户端请求并安排执行的节点，DN 是保存和处理数据的节点，GTM
负责协调需要多个节点配合的事务。

## 数据分布
 node_name | node_type | node_host | node_port | node_forward_port
-----------|-----------|-----------|-----------|-------------------
 cn0001    | C         | 127.0.0.1 |     11003 |             11005
 dn0001    | D         | 127.0.0.1 |     11006 |             11008
 dn0002    | D         | 127.0.0.1 |     11009 |             11011
 dn0003    | D         | 127.0.0.1 |     11012 |             11014
 gtm0001   | G         | 127.0.0.1 |     11000 |                 0


  relname      | pclocatortype |     nodeoids
------------------|---------------|-------------------
 bench_categories | R             | 16384 16385 16386
 bench_orders     | S             | 16384 16385 16386
 bench_payments   | S             | 16384 16385 16386
 bench_users      | S             | 16384 16385 16386


 table_name    | rows
------------------|------
 bench_users      | 500000
 bench_orders     | 3000000
 bench_categories |   20
 bench_payments   | 1499980
