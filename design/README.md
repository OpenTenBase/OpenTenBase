# OpenTenBase 接入分布式 PostgreSQL 社区部署框架 — 设计方案

> **Issue**: #201  
> **类型**: 调研 + 设计方案（Discussion + PR）  
> **状态**: Draft

---

## 1. 问题背景

### 1.1 当前部署痛点

OpenTenBase 的当前部署方式（`opentenbase_ctl` + Docker/host 脎脚本）存在以下问题：

| 痛点 | 说明 |
|------|------|
| **手动编排** | `opentenbase_ctl` 基于 SSH 远程执行，无声明式配置，无法与 K8s 生态整合 |
| **启动顺序依赖** | GTM → DN → CN 三阶段必须严格顺序启动，任何阶段失败导致集群不可用 |
| **无自愈能力** | Pod 崩溃后无法自动恢复，节点间注册关系丢失 |
| **无声明式扩缩容** | 添加 DN 需手动执行 `ALTER NODE` + 数据重分布 |
| **无统一备份恢复** | 分布式环境下 DN 间数据一致性难以保障 |
| **无监控标准** | GTM/CN/DN 指标分散，缺少统一的 Prometheus 导出方案 |

### 1.2 为什么需要社区 PG Operator 框架

2024–2026 年，Kubernetes 上的 PostgreSQL 生命周期管理已形成成熟生态：

- **CloudNativePG**（CNPG）— 意大利 Enter 公司主导，2024 年成为 CNCF Sandbox 项目
- **StackGres** — OnGres 公司，Sidecar 模式 + pgBackRest，企业级备份
- **Crunchy PGO** — CrunchyData，美国市场主导，安全合规导向
- **Zalando PGO** — Spotify 旗下，轻量级，欧盟广泛使用

这些 Operator 均基于同一核心范式：

```
声明式 CR → Operator 控制器 → StatefulSet + PVC → Pod 内 PG 进程
```

OpenTenBase 需要判断：**能否直接复用某个 Operator 的核心抽象？还是必须从头设计？**

---

## 2. 社区 PG Operator 深度对比

### 2.1 四大 Operator 核心特征矩阵

| 维度 | CloudNativePG | StackGres | Crunchy PGO | Zalando PGO |
|------|---------------|-----------|-------------|-------------|
| **GitHub Stars** | ~4.5K | ~500 | ~1.3K | ~3.8K |
| **核心 CRD** | `Cluster` | `SGCluster` | `PostgresCluster` | `postgresql` |
| **架构模式** | 直接管理 PG 进程 | Sidecar 焷管理 | 独立 Sidecar | 直接管理 PG 进程 |
| **HA 方案** | PG 内原生流复制 | Patroni + DCS | Patroni + DCS | Patroni + DCS |
| **备份** | pg_dump + WAL archiving | pgBackRest (企业级) | pgBackRest | WAL archiving |
| **监控** | 内置 pg_exporter | Prometheus Sidecar | pgMonitor + Exporter | 简易 exporter |
| **连接池** | PgBouncer 内置 | PgBouncer Sidecar | PgBouncer | 无内置 |
| **升级** | 原地滚动升级 | 转储 + 新集群 | 转储 + 新集群 | 原地升级 |
| **存储** | PVC + StorageClass | PVC + 卷快照 | PVC + 卷快照 | PVC + StorageClass |
| **CNCF 状态** | Sandbox | — | — | — |

### 2.2 关键架构差异图

![架构对比](charts/architecture_comparison.svg)

左列：OpenTenBase 的三角色分布式架构（GTM + CN + DN），每个角色有独立的启动依赖链和节点间注册需求。

右列：标准 PG Operator 的单角色架构（Primary + Replica + HA），无跨角色注册，无全局事务管理。

### 2.3 Operator 框架选择：为什么 CloudNativePG 是最佳起点

基于以下分析，**CloudNativePG（CNPG）** 是 OpenTenBase 接入的最佳框架候选：

| 判断维度 | CNPG | StackGres | Crunchy | Zalando |
|----------|------|-----------|---------|---------|
| **代码侵入性** | 低 — Sidecar 模式可替换为 OTB init | 高 — 强 Sidecar 耦合 | 高 — 专有 Sidecar | 中 |
| **CRD 扩展性** | 好 — `Cluster` spec 结构清晰 | 中 — 字段多但耦合 | 低 — 耦合程度高 | 中 |
| **GTM 兼容性** | 可扩展 — 添加独立 StatefulSet | 困难 — 深度定制 PG 管理 | 困难 | 困难 |
| **社区活跃度** | 最高 — CNCF Sandbox | 中 | 中 | 高 |
| **文档质量** | 优秀 | 良好 | 良好 | 中 |

**核心结论**：CNPG 的 `Cluster` CRD 范式最适合扩展为 `OpenTenBaseCluster`，但需要新增 GTM 等独立编排能力。

---

## 3. OpenTenBase 与 PG Operator 的核心差异分析

### 3.1 差异清单

| # | 差异项 | OpenTenBase | 单机 PG Operator |
|---|--------|-------------|------------------|
| D1 | **角色数量** | 3（GTM/CN/DN） | 1（PG instance） |
| D2 | **全局事务** | GTM 提供全局快照+序列号 | 无，本地 MVCC |
| D3 | **启动依赖** | GTM→DN→CN 严格顺序 | initdb 即可独立启动 |
| D4 | **节点注册** | CREATE NODE / ALTER NODE 跨角色 | 无，本地 pg_hba.conf |
| D5 | **数据分片** | Hash/Modulo/Shard/Replication | 无，本地存储 |
| D6 | **跨节点查询** | Remote SQL/Broadcast/Redistribute | 无，本地执行 |
| D7 | **扩容** | 新 DN + ALTER NODE + 数据重分布 | 新 Replica + 流复制 |
| D8 | **备份一致性** | 全 DN 协调快照 | 单节点 pg_dump |
| D9 | **GTM HA** | GTM Standby + active_host 配置 | 无 |
| D10 | **连接路由** | CN 层统一路由 | PgBouncer 直连 Primary |

### 3.2 这些差异意味着什么

**不能简单复用**：将 CNPG 的 `Cluster` CRD 直接映射到 OpenTenBase 不可行——
- 单个 `Cluster` 只管一个 PG 实例族（Primary + Replica）
- OpenTenBase 需要 **3 个独立的编排单元**，且它们之间有严格的初始化依赖

**可以借鉴**：CNPG 的以下能力可以直接复用或改造——
- `instanceManager` 侧边进程 → 可改造为 OTB 的 init-container
- `PVC` + `StorageClass` 管理 → DN/CN 各自独立 PVC
- `PgBouncer` 集成 → 可复用为 CN 层连接池
- WAL archiving → 每个 DN 独立归档
- `pg_exporter` → 每个 DN/CN/GTM 独立指标导出

---

## 4. 接入方案设计

### 4.1 方案概览：OpenTenBaseCluster Operator

![K8s 提议架构](charts/k8s_proposed_architecture.svg)

核心设计思路：

1. **新增 `OpenTenBaseCluster` CRD** — 顶层编排入口，声明 GTM/CN/DN 三组 StatefulSet
2. **Phase-based 初始化** — Operator 控制器按 `Initializing → GTMReady → DNReady → CNReady → Running` 五阶段推进
3. **独立 StatefulSet 编排** — GTM/CN/DN 各自独立 StatefulSet + PVC
4. **Init-container 注册** — 每个角色的 Pod 使用 init-container 完成节点注册
5. **可复用 CNPG 组件** — PgBouncer、pg_exporter、WAL archiving

### 4.2 启动依赖与初始化流程

![启动序列](charts/startup_sequence.svg)

详细步骤：

```
Phase: Initializing
  └─ Operator 创建 OpenTenBaseCluster CR
  └─ 生成 gtm.conf / postgresql.conf 模板（ConfigMap）

Phase: GTMReady
  └─ 创建 GTM Master StatefulSet (replicas=1)
  └─ Pod init-container: initdb + gtm.conf 写入
  └─ Pod 主进程启动: gtm_main
  └─ (可选) 创建 GTM Standby StatefulSet
  └─ GTM Service (ClusterIP) 注册到 K8s DNS
  └─ Status.phase → "GTMReady"

Phase: DNReady
  └─ 等待 GTM Service 可达（DNS + TCP 6666）
  └─ 创建 DN StatefulSet (replicas=N)
  └─ 每个 DN Pod init-container:
     ├─ initdb
     ├─ postgresql.conf (引用 gtm_host=otbc-gtm-master:6666)
     ├─ pg_ctl start (临时)
     ├─ ALTER NODE dn{i} WITH (HOST='otbc-dn{i}', PORT=5432)
     └─ pg_ctl stop
  └─ DN Services 注册
  └─ Status.phase → "DNReady"

Phase: CNReady
  └─ 等待所有 DN Services 可达（TCP 5432）
  └─ 创建 CN StatefulSet (replicas=1+)
  └─ CN Pod init-container:
     ├─ initdb
     ├─ postgresql.conf (引用 gtm_host + dn_hosts)
     ├─ pg_ctl start (临时)
     ├─ CREATE NODE cn{i} WITH (TYPE='coordinator', ...)
     ├─ CREATE NODE dn{i} WITH (TYPE='datanode', ...)
     └─ pg_ctl stop
  └─ CN Service (LoadBalancer/ClusterIP) 注册
  └─ (可选) 创建 PgBouncer Deployment
  └─ Status.phase → "CNReady"

Phase: Running
  └─ CN 主进程启动
  └─ DN 主进程启动
  └─ Status.phase → "Running"
  └─ connectionInfo 写入 CR status
```

### 4.3 OpenTenBaseCluster CRD 设计

详见 [`samples/opentenbasecluster_crd.yaml`](samples/opentenbasecluster_crd.yaml)

关键设计决策：

| 设计点 | 决策 | 原因 |
|--------|------|------|
| **顶层 CRD** | `OpenTenBaseCluster` (Kind: OTBC) | 单一入口管理全部角色 |
| **GTM 编排** | 独立 `gtm` spec（master + standby） | GTM 是全局事务核心，必须独立生命周期 |
| **CN 编排** | `coordinators` spec + pgbouncer 可选 | CN 是用户入口，PgBouncer 是标配 |
| **DN 编排** | `datanodes` spec + defaultDistribution | DN 是存储核心，分片策略必须声明 |
| **Status.phase** | 5 阶段枚举 | 反映严格的启动依赖 |
| **nodeStatus[]** | 动态数组 | 跟踪每个 Pod 的角色/状态/端口 |
| **connectionInfo** | 结构化字段 | 让应用可以直接从 CR status 读连接信息 |

### 4.4 最小集群实例

详见 [`samples/opentenbasecluster_instance.yaml`](samples/opentenbasecluster_instance.yaml)

```yaml
# 1 CN + 2 DN + 1 GTM + 1 GTM Standby — 最小分布式集群
spec:
  distributedType: distributed
  gtm:
    master: { replicas: 1, storage: { size: 5Gi } }
    standby: { replicas: 1, storage: { size: 5Gi } }
  coordinators:
    replicas: 1
    pgbouncer: { enabled: true, poolMode: transaction }
  datanodes:
    replicas: 2
    defaultDistribution: hash
```

### 4.5 Init-Container 初始化脚本

详见 [`samples/k8s_init_script.sh`](samples/k8s_init_script.sh)

脚本按 `ROLE` 环境变量区分 GTM/DN/CN 初始化流程，核心逻辑：
- GTM：`initdb` → 写 `gtm.conf` → 直接启动
- DN：等待 GTM → `initdb` → 写 `postgresql.conf` → 临时启动 → `ALTER NODE` → 停止
- CN：等待 GTM + DN → `initdb` → 写 `postgresql.conf` → 临时启动 → `CREATE NODE` × N → 停止

---

## 5. 与现有 K8s 生态的集成分析

### 5.1 KubeBlocks 兼容性

OpenTenBase 仓库中已有 `docker/k8s_support/` 目录，包含为 **KubeBlocks** 构建的镜像：

```
docker/k8s_support/Dockerfile  →  纯二进制镜像（无 entrypoint）
docker/k8s_support/docker.mk   →  构建脚本
```

**现状**：仅有镜像构建能力，缺少 Operator/CRD/生命周期管理。

**我们的方案与 KubeBlocks 的关系**：

| 维度 | KubeBlocks | 本方案（OpenTenBaseCluster Operator） |
|------|-----------|--------------------------------------|
| **定位** | 通用数据库编排平台 | OpenTenBase 专有 Operator |
| **CRD** | `ClusterDefinition` + `ClusterVersion` | `OpenTenBaseCluster` |
| **编排粒度** | 多数据库引擎统一抽象 | 专注 OTB 三角色编排 |
| **GTM 管理** | 无（不支持分布式 PG） | 完整 GTM lifecycle |
| **互补性** | 可作为上层统一入口 | 底层 CRD 可被 KubeBlocks 封装 |

**建议路径**：

1. **Phase 1**：独立 `OpenTenBaseCluster` Operator，验证三角色编排可行性
2. **Phase 2**：将 CRD 注册为 KubeBlocks `ClusterDefinition`，接入统一管理面

### 5.2 监控集成

```
GTM Pod:
  ├─ gtm_main (port 6666)
  └─ pg_exporter sidecar (port 9187) → 自定义 GTM 指标

CN Pod:
  ├─ postgres (port 5432)
  └─ pg_exporter sidecar (port 9187) → PG 标准指标

DN Pod:
  ├─ postgres (port 5432)
  └─ pg_exporter sidecar (port 9187) → PG 标准指标 + 分片指标

PgBouncer Pod:
  ├─ pgbouncer (port 6432)
  └─ pgbouncer_exporter sidecar (port 9188)
```

GTM 的 `pg_exporter` 需要自定义查询文件，导出以下关键指标：

| 指标 | 说明 |
|------|------|
| `otb_gtm_tx_count` | 已分配的全局事务数 |
| `otb_gtm_snapshot_count` | 已分配的全局快照数 |
| `otb_gtm_sequence_count` | 已分配的全局序列号 |
| `otb_gtm_active_connections` | GTM 当前连接数 |
| `otb_gtm_status` | GTM 节点状态（master/standby） |

### 5.3 备份策略

分布式环境的备份比单机 PG 复杂得多：

| 策略 | 说明 | 适用场景 |
|------|------|----------|
| **逐 DN pg_dump** | 每个 DN 独立 pg_dump | 小集群、调试 |
| **全 DN 协调快照** | 所有 DN 同一时刻 pg_dump（通过 GTM 全局快照协调） | 中等集群 |
| **WAL 持续归档** | 每个 DN 独立 WAL archiving → S3/MinIO | 生产环境推荐 |
| **pgBackRest** | 复用 StackGres 的 pgBackRest Sidecar | 企业级（需额外开发） |

**MVP 建议**：先实现逐 DN pg_dump + WAL 持续归档，pgBackRest 作为后续迭代目标。

---

## 6. Operator 框架对比总结

![框架对比](charts/operator_comparison.svg)

四象限分析：

- **左上（高扩展性 + 低侵入）**：CNPG — 最佳起点
- **右上（高扩展性 + 高侵入）**：StackGres — 企业级但耦合重
- **左下（低扩展性 + 低侵入）**：Zalando — 轻量但功能不足
- **右下（低扩展性 + 高侵入）**：Crunchy — 耦合程度高

---

## 7. 实施路线图

### 7.1 Phase 1：最小可行 Operator（3–6 月）

| 任务 | 交付物 |
|------|--------|
| CRD 定义 + Operator 控制器骨架 | `OpenTenBaseCluster` CRD + Go controller |
| GTM StatefulSet 编排 | GTM Master/Standby Pod 生命周期 |
| DN StatefulSet 编排 + init-container | DN 初始化 + ALTER NODE 注册 |
| CN StatefulSet 编排 + init-container | CN 初始化 + CREATE NODE 注册 |
| Phase-based status 管理 | 5 阶段 status.phase 推进 |
| 基础 Service 暴露 | CN LoadBalancer/ClusterIP |

### 7.2 Phase 2：生产级增强（6–12 月）

| 任务 | 交付物 |
|------|--------|
| GTM HA failover | GTM Standby 自动切换 |
| DN 扩缩容 + 数据重分布 | DN 添加/移除 + 自动 ALTER NODE |
| PgBouncer 集成 | CN 层连接池 Deployment |
| WAL archiving | DN → S3/MinIO 持续归档 |
| Prometheus 监控 | GTM/DN/CN pg_exporter + ServiceMonitor |
| KubeBlocks 接入 | ClusterDefinition 封装 |

### 7.3 Phase 3：企业级功能（12+ 月）

| 任务 | 交付物 |
|------|--------|
| pgBackRest 备份恢复 | StackGres 备份方案集成 |
| 滚动升级 | 零宕机版本升级 |
| 多集群管理 | 跨 Region 集群联邦 |
| 安全合规 | TLS 加密 + RBAC + 审计日志 |

---

## 8. 风险与挑战

| 风险 | 等级 | 缓解方案 |
|------|------|----------|
| **GTM 单点瓶颈** | 高 | GTM Standby + GTM Proxy 分流 |
| **DN 扩容时数据重分布耗时** | 中 | 后台异步重分布 + 旧 DN 仍可读 |
| **跨 DN 事务一致性备份** | 高 | GTM 全局快照协调 |
| **CNPG 与 OTB 代码兼容性** | 中 | 独立 Operator，仅复用模式不复用代码 |
| **K8s 环境网络延迟** | 中 | DN 间内网 Service + NodeLocal DNS |
| **Pod 重启后节点注册丢失** | 高 | init-container 每次启动重新注册 |

---

## 9. 附录

### 9.1 文件清单

| 文件 | 说明 |
|------|------|
| [`charts/architecture_comparison.svg`](charts/architecture_comparison.svg) | OpenTenBase vs 标准 PG Operator 架构对比 |
| [`charts/k8s_proposed_architecture.svg`](charts/k8s_proposed_architecture.svg) | OpenTenBaseCluster Operator 提议架构 |
| [`charts/startup_sequence.svg`](charts/startup_sequence.svg) | GTM→DN→CN 启动序列流程图 |
| [`charts/operator_comparison.svg`](charts/operator_comparison.svg) | 四大 Operator 框架象限分析 |
| [`samples/opentenbasecluster_crd.yaml`](samples/opentenbasecluster_crd.yaml) | OpenTenBaseCluster CRD 草案（完整定义） |
| [`samples/opentenbasecluster_instance.yaml`](samples/opentenbasecluster_instance.yaml) | 最小集群实例配置 |
| [`samples/k8s_init_script.sh`](samples/k8s_init_script.sh) | K8s init-container 初始化脚本模板 |

### 9.2 参考资料

| 资料 | 链接 |
|------|------|
| CloudNativePG 官方文档 | https://cloudnative-pg.io/documentation/ |
| StackGres Operator 文档 | https://stackgres.io/doc/latest/ |
| Crunchy PGO 文档 | https://access.crunchydata.com/documentation/postgres-operator/ |
| Zalando PGO 文档 | https://postgres-operator.readthedocs.io/ |
| KubeBlocks 项目 | https://kubeblocks.io/ |
| OpenTenBase Docker 部署 | 仓库 `docker/` 目录 |
| OpenTenBase opentenbase_ctl | 仓库 `contrib/opentenbase_ctl/` 目录 |

---

> 本方案为调研+设计阶段产出，核心 CRD 和架构图已设计完成，但 Operator 控制器代码实现属于后续 Phase 1 工作范围。
