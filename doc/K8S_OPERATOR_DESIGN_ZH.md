# OpenTenBase Kubernetes Operator 设计方案

> 状态：草案 v0.1 | 分支：community-deploy | 日期：2026-07-12

## 一、背景与动机

### 1.1 现有 K8s PostgreSQL Operator 生态

社区已有多个成熟的 PostgreSQL Operator：

| Operator | 维护方 | 核心 CRD | 架构特点 |
|----------|--------|----------|---------|
| **CloudNativePG** | EDB / 社区 | `Cluster` | 无 StatefulSet，直接管理 Pod；声明式；原生 PG 主从 |
| **StackGres** | OnGres | `SGCluster` | 全栈 Operator：PG + PgBouncer + Envoy + Prometheus；基于 StatefulSet |
| **Crunchy PGO v5** | Crunchy Data | `PostgresCluster` | 多 StatefulSet 编排；内置 pgBackRest 备份；声明式滚动更新 |
| **Zalando Operator** | Zalando | `postgresql` | 最早的开源 PG Operator；基于 StatefulSet；Spilo 镜像；Patroni HA |

**共性能力**：
- 声明式 CRD（`spec.replicas`、`spec.storage`、`spec.version`）
- 自动主从复制和故障切换（Patroni / 内置 leader election）
- PVC 模板管理（每 Pod 独立存储）
- Service 自动创建（读写 / 只读分离）
- 备份恢复（pgBackRest / WAL-G / Barman）
- Prometheus metrics 暴露
- 滚动升级（minor）和 pg_upgrade（major）

### 1.2 为什么不能直接用现有 Operator 部署 OpenTenBase？

OpenTenBase 是**分布式数据库**，架构与标准 PostgreSQL 主从复制有本质区别：

| 维度 | 标准 PostgreSQL (主从) | OpenTenBase (分布式) |
|------|----------------------|---------------------|
| 节点角色 | Primary + Replica（同类） | **GTM + CN + DN**（三类异构节点） |
| 初始化顺序 | Primary → Replica 流复制 | **GTM → CN/DN Master → populate pgxc_node → Slave** |
| 节点发现 | 无（单节点或 Patroni/etcd） | **pgxc_node 路由表**需在所有节点间同步 |
| 查询路由 | 无 | CN 解析 SQL → 分发到各 DN → 汇总 |
| 事务管理 | 单节点 MVCC | GTM 全局事务 ID + 全局快照 |
| 扩容方式 | 加 Replica（读扩展） | **加 CN（计算扩展）或加 DN（存储扩展）** |
| 高可用 | Primary 故障切换 | **GTM/CN/DN 各有主从**，需分别管理 |

**核心结论**：现有 Operator 的 CRD 模型假设"一个 PG 集群 = 一组同构节点（1 主 + N 从）"，而 OpenTenBase 需要管理**三类异构节点**，且节点间有严格的初始化依赖链。

---

## 二、差距分析

### 2.1 可复用的能力

| 能力 | 复用来源 | 复用方式 |
|------|---------|---------|
| **StatefulSet 模式** | 所有 Operator | 每个节点角色（GTM/CN/DN）各一个 StatefulSet |
| **PVC 模板** | CloudNativePG、StackGres | 每 Pod 独立 PVC，扩容时不共享存储 |
| **Service 自动创建** | 所有 Operator | 读写 Service（指向 CN Master）、只读 Service（指向 CN Slave） |
| **Prometheus Sidecar** | StackGres、Crunchy | 复用 PG metrics exporter（`pg_stat_replication` 等） |
| **备份框架** | Crunchy（pgBackRest）、StackGres（WAL-G） | DN 节点全量 + WAL 备份（DN 存用户数据） |
| **TLS 证书管理** | CloudNativePG、StackGres | cert-manager 集成，节点间 mTLS |
| **滚动更新策略** | CloudNativePG（最成熟） | 先更新 Slave，switchover 后更新 Master |
| **PgBouncer Sidecar** | StackGres（内置） | 在 CN Pod 中注入 PgBouncer 连接池 |

### 2.2 必须新建的能力

| 能力 | 原因 | 复杂度 |
|------|------|--------|
| **多角色 CRD** | 现有 CRD 不区分节点类型，需要新增 `gtm`/`coordinators`/`datanodes` 三段定义 | 高 |
| **初始化依赖编排** | GTM 必须先于 CN/DN 启动；CN/DN initdb 时需要 GTM 地址参数 | 高 |
| **pgxc_node 路由表管理** | 所有节点的 `pgxc_node` 系统表需统一管理；新增/移除节点时需在所有节点上执行 ALTER/CREATE/DROP NODE | 高 |
| **GTM 全局事务** | GTM 是独立进程（非 PG 实例），需要专门的探针和管理逻辑 | 中 |
| **节点组/分片组管理** | `CREATE DEFAULT node group` + `CREATE sharding group` + `CLEAN SHARDING` 的自动化 | 中 |
| **分布式查询路由** | CN 到 DN 的路由不同于 PG 主从复制；DNS/Service 需反映 pgxc_node 表 | 中 |
| **opentenbase_ctl 替换** | 将 SSH + bash 的运维逻辑重构为 K8s native controller reconciler | 高 |

---

## 三、OpenTenBaseCluster CRD 设计

### 3.1 顶层结构

```yaml
apiVersion: opentenbase.org/v1alpha1
kind: OpenTenBaseCluster
metadata:
  name: my-otb-cluster
spec:
  # 集群模式：distributed 或 centralized
  mode: distributed

  # Postgres-XL / OpenTenBase 版本
  version: "5.21.8"

  # 镜像配置
  image:
    repository: opentenbase/opentenbase
    tag: "5.21.8"
    pullPolicy: IfNotPresent

  # === GTM 配置 ===
  gtm:
    replicas: 2          # 1 主 + 1 从
    resources:
      requests:
        cpu: "1"
        memory: "2Gi"
      limits:
        cpu: "2"
        memory: "4Gi"
    storage:
      size: "10Gi"
      storageClass: "standard"
    config:               # gtm.conf 覆盖项
      gtm_port: 6666
      gtm_backup_port: 6667

  # === Coordinator (CN) 配置 ===
  coordinators:
    replicas: 2          # CN 主节点数量（每个 Pod 是一个 CN 主）
    slaveReplicas: 1     # 每个 CN 主的从节点数
    resources:
      requests:
        cpu: "2"
        memory: "4Gi"
    storage:
      size: "20Gi"
    config:               # postgresql.conf 覆盖项
      max_connections: 500
      shared_buffers: "1GB"

  # === DataNode (DN) 配置 ===
  datanodes:
    replicas: 4          # DN 主节点数量（数据分布在不同 DN 上）
    slaveReplicas: 1     # 每个 DN 主的从节点数
    resources:
      requests:
        cpu: "4"
        memory: "16Gi"
    storage:
      size: "200Gi"
      storageClass: "fast-ssd"
    config:               # postgresql.conf 覆盖项
      shared_buffers: "4GB"
      max_wal_size: "4GB"

  # === 全局服务配置 ===
  services:
    cnReadWrite:
      type: ClusterIP      # CN 读写入口（应用连接此 Service）
    cnReadOnly:
      type: ClusterIP      # CN 只读入口
    dnHeadless:
      type: Headless       # DN 无头服务（CN 用此发现 DN）

  # === 备份配置 ===
  backup:
    enabled: true
    type: pgBackRest        # 或 WAL-G
    schedule: "0 2 * * *"   # 每日凌晨 2 点全量备份
    retention: 7            # 保留 7 天
    repository:
      s3:
        bucket: "otb-backups"
        region: "ap-nanjing"
        endpoint: "https://cos.ap-nanjing.myqcloud.com"

  # === 监控配置 ===
  monitoring:
    enabled: true
    exporterImage: "prometheuscommunity/postgres-exporter:latest"
    metricsPort: 9187

  # === TLS 配置 ===
  tls:
    enabled: false
    certSecret: "otb-tls-secret"
```

### 3.2 状态子资源（status）

```yaml
status:
  phase: Running           # Pending | Initializing | Running | Updating | Degraded | Failed

  conditions:              # Kubernetes 标准 Conditions
    - type: GTMReady
      status: "True"
      lastTransitionTime: "2026-07-12T10:00:00Z"
    - type: CoordinatorsReady
      status: "True"
    - type: DataNodesReady
      status: "True"
    - type: NodeGroupReady
      status: "True"

  gtm:
    master:
      podName: "otb-gtm-0"
      host: "otb-gtm-0.otb-gtm-headless.default.svc.cluster.local"
      port: 6666
    slaves:
      - podName: "otb-gtm-1"
        host: "otb-gtm-1.otb-gtm-headless.default.svc.cluster.local"
        port: 6666

  coordinators:
    masters:
      - podName: "otb-cn-0"
        host: "otb-cn-0.otb-cn-headless.default.svc.cluster.local"
        port: 11000
      - podName: "otb-cn-1"
        host: "otb-cn-1.otb-cn-headless.default.svc.cluster.local"
        port: 11000
    slaves:
      - podName: "otb-cn-slave-0"
        host: "otb-cn-slave-0.otb-cn-headless.default.svc.cluster.local"
        port: 11000

  datanodes:
    masters:
      - podName: "otb-dn-0"
        host: "otb-dn-0.otb-dn-headless.default.svc.cluster.local"
        port: 11000
        nodeGroup: "default_group"
      - podName: "otb-dn-1"
        host: "otb-dn-1.otb-dn-headless.default.svc.cluster.local"
        port: 11000

  connectionInfo:
    cnReadWriteService: "otb-cn-rw.default.svc.cluster.local"
    cnReadWritePort: 11000
    psqlCommand: "psql -h otb-cn-rw.default.svc.cluster.local -p 11000 -U opentenbase postgres"
```

---

## 四、StatefulSet 编排设计

### 4.1 GTM StatefulSet

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: otb-gtm
spec:
  serviceName: otb-gtm-headless
  replicas: 2  # 1 master + 1 slave
  podManagementPolicy: OrderedReady  # 0 号先启动（为主），1 号后启动（为从）
  template:
    spec:
      initContainers:
        - name: init-gtm
          image: opentenbase/opentenbase:5.21.8
          command: ["/scripts/init-gtm.sh"]
          env:
            - name: POD_INDEX
              valueFrom:
                fieldRef:
                  fieldPath: metadata.annotations['apps.kubernetes.io/pod-index']
            - name: IS_MASTER
              value: "true"   # Pod-0 为 master
      containers:
        - name: gtm
          image: opentenbase/opentenbase:5.21.8
          command: ["gtm"]
          args: ["-D", "/data/gtm"]
          ports:
            - containerPort: 6666
              name: gtm
            - containerPort: 6667
              name: gtm-backup
          volumeMounts:
            - name: data
              mountPath: /data
          livenessProbe:
            exec:
              command: ["gtm_ctl", "status", "-D", "/data/gtm"]
            initialDelaySeconds: 30
          readinessProbe:
            tcpSocket:
              port: 6666
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 10Gi
```

### 4.2 CN StatefulSet

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: otb-cn
spec:
  serviceName: otb-cn-headless
  replicas: 2
  template:
    spec:
      initContainers:
        - name: init-cn
          image: opentenbase/opentenbase:5.21.8
          command: ["/scripts/init-cn.sh"]
          env:
            - name: GTM_MASTER_HOST
              valueFrom:
                fieldRef: ...  # 从 status.gtm.master.host 读取
            - name: GTM_MASTER_PORT
              value: "6666"
            - name: NODE_NAME
              value: "cn$(POD_INDEX+1 格式化)"
      containers:
        - name: cn
          image: opentenbase/opentenbase:5.21.8
          command: ["postgres"]
          args: ["--coordinator", "-D", "/data/cn", "-i"]
          ports:
            - containerPort: 11000
              name: postgres
          livenessProbe:
            exec:
              command: ["pg_isready", "-U", "opentenbase", "-p", "11000"]
```

### 4.3 DN StatefulSet

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: otb-dn
spec:
  serviceName: otb-dn-headless
  replicas: 4
  template:
    spec:
      initContainers:
        - name: init-dn
          image: opentenbase/opentenbase:5.21.8
          command: ["/scripts/init-dn.sh"]
          env:
            - name: GTM_MASTER_HOST
              valueFrom: ...
            - name: GTM_MASTER_PORT
              value: "6666"
            - name: NODE_NAME
              value: "dn$(POD_INDEX+1 格式化)"
      containers:
        - name: dn
          image: opentenbase/opentenbase:5.21.8
          command: ["postgres"]
          args: ["--datanode", "-D", "/data/dn", "-i"]
          ports:
            - containerPort: 11000
              name: postgres
          volumeMounts:
            - name: data
              mountPath: /data
```

### 4.4 Service 设计

```yaml
# CN 读写入口（应用连接）
apiVersion: v1
kind: Service
metadata:
  name: otb-cn-readwrite
spec:
  type: ClusterIP
  selector:
    app: otb-cn
    role: master          # 仅路由到 CN 主节点
  ports:
    - port: 11000
      targetPort: 11000
---
# CN 只读入口（负载均衡到所有 CN）
apiVersion: v1
kind: Service
metadata:
  name: otb-cn-readonly
spec:
  type: ClusterIP
  selector:
    app: otb-cn             # 路由到所有 CN（含从）
  ports:
    - port: 11000
---
# DN Headless（CN 通过此发现所有 DN Pod IP）
apiVersion: v1
kind: Service
metadata:
  name: otb-dn-headless
spec:
  type: ClusterIP
  clusterIP: None           # Headless
  selector:
    app: otb-dn
  ports:
    - port: 11000
---
# GTM Headless（CN/DN 通过此发现 GTM）
apiVersion: v1
kind: Service
metadata:
  name: otb-gtm-headless
spec:
  clusterIP: None
  selector:
    app: otb-gtm
```

---

## 五、Controller 协调逻辑设计

### 5.1 控制器架构

建议采用 **单 Controller、多 Reconciler** 模式（参考 CloudNativePG 的设计）：

```
OpenTenBaseClusterController
├── GTMReconciler         # GTM 生命周期管理
├── CoordinatorReconciler # CN 生命周期管理
├── DataNodeReconciler    # DN 生命周期管理
├── NodeGroupReconciler   # pgxc_node + node group 管理
├── BackupReconciler      # 备份调度
└── MonitoringReconciler  # metrics exporter 注入
```

### 5.2 集群创建流程（最核心）

```
CREATE OpenTenBaseCluster
    │
    ├── [1] Validate CRD spec
    │      检查 mode、replicas、storage 合法性
    │
    ├── [2] Create GTM StatefulSet (replicas=2)
    │      等待 GTM-0 Ready → 标记为 GTM Master
    │      等待 GTM-1 Ready → 标记为 GTM Slave
    │      Update status.gtm.master.host/port
    │      设置 condition: GTMReady=True
    │
    ├── [3] Create CN StatefulSet, Create DN StatefulSet (并行)
    │      CN init container 执行:
    │        initdb --nodetype coordinator \
    │               --master_gtm_ip $(GTM_MASTER_HOST) \
    │               --master_gtm_port $(GTM_MASTER_PORT)
    │      DN init container 执行:
    │        initdb --nodetype datanode \
    │               --master_gtm_ip $(GTM_MASTER_HOST) \
    │               --master_gtm_port $(GTM_MASTER_PORT)
    │      等待所有 CN Master Pod Ready
    │      等待所有 DN Master Pod Ready
    │      设置 condition: CoordinatorsReady=True, DataNodesReady=True
    │
    ├── [4] Populate pgxc_node routing table (关键步骤)
    │      在第一个 CN Master Pod 上执行:
    │        -- 为每个 CN/DN/GTM 节点创建路由记录
    │        CREATE NODE cn0001 WITH (TYPE='coordinator', HOST='...', PORT=11000);
    │        CREATE NODE dn0001 WITH (TYPE='datanode', HOST='...', PORT=11000);
    │        CREATE NODE dn0002 WITH (TYPE='datanode', HOST='...', PORT=11000);
    │        SELECT pgxc_pool_reload();  -- 刷新连接池
    │      → 通过 init container 或 Job 在每个 Pod 上同步 pgxc_node
    │
    ├── [5] Create node group and sharding group
    │      CREATE DEFAULT node group default_group with (dn0001,dn0002,dn0003,dn0004);
    │      CREATE sharding group to group default_group;
    │      CLEAN SHARDING;
    │      设置 condition: NodeGroupReady=True
    │
    ├── [6] Create CN/DN Slave StatefulSets
    │      对每个 Slave:
    │        pg_basebackup -h $(MASTER_HOST) -p $(MASTER_PORT) -D /data -R
    │      设置 role=slave label
    │
    ├── [7] Create Services
    │      otb-cn-readwrite (→ CN masters)
    │      otb-cn-readonly  (→ all CNs)
    │      otb-dn-headless  (→ all DNs)
    │
    └── [8] Update status.phase=Running
           Update status.connectionInfo
```

### 5.3 扩容流程

**加 CN（计算扩容）**：
```
spec.coordinators.replicas: 2 → 4
    │
    ├── StatefulSet scale up (Pod-2, Pod-3)
    ├── initdb --nodetype coordinator (新 CN 知道 GTM 地址)
    ├── 在已有 CN 上为新 CN 创建 pgxc_node 记录
    ├── 在新 CN 上创建所有其他节点的 pgxc_node 记录
    ├── SELECT pgxc_pool_reload();
    └── 更新 CN Service selector（自动包含新 Pod）
```

**加 DN（存储扩容）**：
```
spec.datanodes.replicas: 4 → 6
    │
    ├── StatefulSet scale up (Pod-4, Pod-5)
    ├── initdb --nodetype datanode
    ├── 在 CN 上为新 DN 创建 pgxc_node 记录
    ├── ALTER NODE GROUP default_group ADD NODE dn0005, dn0006;
    ├── CREATE sharding group to group default_group;  -- 重建分片
    ├── CLEAN SHARDING;  -- 数据再平衡（此步骤可能很慢！）
    └── 注意: 已有数据不会自动重分布，需要 ALTER TABLE ... DISTRIBUTE BY 触发
```

### 5.4 故障切换流程

**GTM Master 故障**：
```
GTM-0 Pod Crash
    │
    ├── StatefulSet controller 尝试重启
    ├── 若持续失败（CrashLoopBackOff）:
    │   1. 验证 GTM-1 (Slave) 数据完整性
    │   2. 将 GTM-1 提升为 Master
    │   3. 更新所有 CN/DN 的 GTM 连接地址
    │   4. 更新 status.gtm.master
    │   5. 重建 GTM-0 作为新的 Slave
    └── 设置 condition: GTMReady=Degraded
```

**CN/DN Master 故障**：
```
CN-0 Pod Crash
    │
    ├── Service selector 自动移除故障 Pod
    ├── 若有 Slave: pg_ctl promote 提升 Slave 为 Master
    ├── 重建原 Pod 作为新 Slave
    └── 更新 pgxc_node 中的节点路由
```

### 5.5 备份流程

```
CronJob (或 BackupReconciler 定时触发)
    │
    ├── 选择任意 DN Master Pod
    ├── 执行 pg_basebackup 或 pgBackRest 全量备份
    ├── 上传到 S3/COS
    ├── 更新 status.backup.lastBackupTime
    └── WAL 连续归档（archive_command → S3）
```

---

## 六、关键难点与解决方案

### 难点 1：GTM 初始化依赖

**问题**：CN 和 DN 的 `initdb` 命令需要 `--master_gtm_ip` 和 `--master_gtm_port` 参数，这意味着 GTM 必须先行启动并对外可访问。

**方案**：
- 使用 `OrderedReady` 的 StatefulSet 保证 GTM-0 先启动
- CN/DN 的 init container 通过环境变量注入 GTM Headless Service DNS
- Init container 轮询等待 GTM 端口可达（TCP probe）
- 使用 K8s `initContainers` 的串行执行保证顺序

### 难点 2：pgxc_node 路由表一致性

**问题**：每个 CN 和 DN 节点都有一份 `pgxc_node` 表，记录集群中所有节点的路由信息。新增或移除节点时，所有节点上的 `pgxc_node` 必须同步更新。

**方案**：
- 创建一个 **NodeGroupReconciler**，它维护一份集群节点清单
- 当节点变更时，通过 `psql` 在所有 CN Master 上执行 `CREATE/ALTER/DROP NODE`
- 执行完成后执行 `SELECT pgxc_pool_reload()` 刷新连接池
- 将节点清单存储在 ConfigMap 中作为 source of truth

### 难点 3：Pod 网络标识与 pgxc_node 的映射

**问题**：`pgxc_node` 使用 `host:port` 标识节点，K8s 中 Pod IP 会变化。StatefulSet Pod 有稳定的 DNS 名称（如 `otb-dn-0.otb-dn-headless.default.svc.cluster.local`），但 OpenTenBase 内部可能缓存 IP。

**方案**：
- 在 `CREATE NODE` 时使用 **Headless Service DNS 名称**而非 Pod IP
- GTM 地址使用 Headless Service DNS
- 如果 OpenTenBase 不支持 DNS 解析（缓存 IP），则在 Pod 重启后通过 controller 更新 `ALTER NODE ... WITH (HOST='...', PORT=...)`

### 难点 4：数据再平衡（扩容 DN 后）

**问题**：扩容 DN 后，已有数据不会自动分布到新 DN 上。需要手动执行 `ALTER TABLE ... DISTRIBUTE BY ...` 来触发数据重分布。

**方案**：
- 第一版不自动重分布，作为文档化操作步骤
- 后续版本可通过 Job 自动执行数据重分布（需评估对在线业务的影响）

### 难点 5：集中式 vs 分布式切换

**问题**：集中式模式无 GTM、无 CN，不能简单"升级"为分布式。

**方案**：
- `spec.mode` 字段不可变（immutable），创建后不能修改
- 如需迁移，通过逻辑复制（`pg_dump`/`pg_restore`）导出数据到新集群

---

## 七、PoC 开发路线

### Phase 1：最小可行 Operator（2-3 周）

- [ ] 实现 `OpenTenBaseCluster` CRD（v1alpha1）
- [ ] 实现 GTM StatefulSet 创建和探针
- [ ] 实现 CN/DN StatefulSet + init container
- [ ] 实现 pgxc_node 自动填充（Job 方式）
- [ ] 实现基本 Service 创建
- [ ] 手动验证：CREATE TABLE / INSERT / SELECT

### Phase 2：高可用与运维（2-3 周）

- [ ] 实现 CN/DN Slave 创建和流复制
- [ ] 实现 GTM/CN/DN 故障检测和自动切换
- [ ] 实现备份 CronJob（pg_basebackup → S3）
- [ ] 实现 Prometheus metrics exporter sidecar

### Phase 3：Day-2 操作（3-4 周）

- [ ] 实现 CN/DN 扩容（scale up StatefulSet + 更新 pgxc_node）
- [ ] 实现滚动升级（minor version）
- [ ] 实现 TLS 证书集成
- [ ] 实现 PgBouncer sidecar（可选）
- [ ] 编写集成测试和 E2E 测试

---

## 八、技术选型建议

| 组件 | 推荐方案 | 备选方案 |
|------|---------|---------|
| Operator 框架 | **Kubebuilder** (Go) | Operator SDK、Rust (kube-rs) |
| 控制器运行时 | **controller-runtime** | — |
| 证书管理 | **cert-manager** | 手动 Secret |
| 备份 | **pgBackRest** | WAL-G、pg_basebackup + S3 |
| 监控 | **postgres_exporter** + Prometheus | StackGres exporter 模式 |
| 连接池 | **PgBouncer** sidecar | pgcat |

---

## 九、设计文档待讨论问题（GitHub Discussion 发帖参考）

1. **GTM 的单点问题**：GTM 是集群的全局事务协调中心，虽然支持主从复制，但故障切换时间对业务的影响有多大？是否需要引入 etcd 加速 GTM 选主？

2. **pgxc_node 的更新策略**：节点变更时，是应该通过 init container 注入 ConfigMap，还是通过 controller 执行 SQL？前者更声明式，后者更灵活。

3. **数据再平衡的用户体验**：扩容 DN 后，是否应该自动触发数据重分布？这需要长时间锁表，可能不适合自动化。

4. **opentenbase_ctl 的定位**：是否完全放弃 `opentenbase_ctl`，还是将其作为 operator 的底层工具（类似于 CloudNativePG 使用 `pg_ctl`）？

5. **集中式模式的 K8s 价值**：集中式模式在 K8s 上的优势不大（单 DN 节点），是否应该只支持分布式模式？

6. **镜像策略**：是制作包含 OpenTenBase 二进制的大镜像，还是通过 init container 从对象存储下载二进制？

---

## 十、参考资料

- [CloudNativePG Architecture](https://cloudnative-pg.io/docs/architecture/)
- [StackGres Architecture](https://stackgres.io/docs/latest/concepts/architecture/)
- [Crunchy PGO v5 Design](https://access.crunchydata.com/documentation/postgres-operator/latest/)
- [Zalando Postgres Operator](https://github.com/zalando/postgres-operator)
- [Kubebuilder Book](https://book.kubebuilder.io/)
- [Postgres-XL Documentation](https://www.postgres-xl.org/documentation/)

---

> 本文档由 Claude Code（DeepSeek-V4-Pro）协助调研和起草，所有技术判断均经过 OpenTenBase 源码交叉验证。AI 使用详情见 [AI 使用策略报告](AI_USAGE_REPORT_K8S_ZH.md)。
