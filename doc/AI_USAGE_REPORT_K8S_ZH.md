# AI 使用策略自我报告 —— K8s Operator 设计任务

## 使用的工具和目的

| 工具 | 用途 |
|------|------|
| Claude Code（DeepSeek-V4-Pro） | 调研 4 个 K8s PostgreSQL Operator 架构；分析 OpenTenBase 源码中的部署逻辑；起草完整设计文档（CRD、StatefulSet、Controller 流程）；撰写 GitHub Discussion 帖子 |
| Claude Code Workflow（多 Agent 编排） | 并行调研 StackGres、CloudNativePG、Crunchy PGO、Zalando Operator；独立分析 OpenTenBase 架构；综合生成差距分析和设计方案 |
| Claude Code 内置 Agent（Explore） | 搜索 `contrib/opentenbase_ctl/src/` 中的节点生成、initdb、配置等源码，提取关键架构细节 |
| Git Bash（Windows） | 创建分支、文件操作、commit、push |

## 对 AI 输出的验证方式

### 调研验证
- **Web 搜索结果交叉核对**：对 AI 搜索到的 Operator CRD 字段、架构描述，手动对比官方文档关键页面（`cloudnative-pg.io`、`stackgres.io`、`access.crunchydata.com`）
- **拒绝未验证的声明**：AI 无法访问部分受登录保护的文档（如 Crunchy Data 的详细 API 文档），这些部分标注为"基于公开博客和 GitHub README 推测"，而非"已验证"

### 架构分析验证
- **OpenTenBase 源码核对**：每条关于 OpenTenBase 节点拓扑、初始化顺序、端口分配、配置路径的断言，均追溯到具体源码：
  - 节点类型常量 → `contrib/opentenbase_ctl/src/types/types.h`（`NODE_TYPE_GTM_MASTER` 等）
  - 节点生成逻辑 → `types.cpp:294`（`generate_nodes` 函数）
  - 初始化命令 → `cluster.cpp:829`（`build_initdb_cmd` 函数）
  - 部署流程 → `cluster.cpp:1009`（`install_distributed_instance`）
  - SSH 执行 → `remote_ssh.cpp`
- **端口分配策略**：验证 `utils.cpp:68`（`assign_ports_for_nodes`），确认从 11000 开始，每节点占用连续 3 端口

### 设计验证
- **CRD 字段完整性**：对照 `config.ini` 模板（`contrib/opentenbase_ctl/config/config.ini`）确保 CRD 的 spec 字段覆盖了所有 INI 配置项（`[instance]`、`[gtm]`、`[coordinators]`、`[datanodes]`、`[server]`、`[log]`）
- **初始化流程**：对照 `install_distributed_instance` 的 6 步流程，确保 Controller 协调逻辑中每一步都有对应
- **pgxc_node 管理**：确认 `cluster.cpp:227` 的 `create_pgxc_node_for_mapp` 和 `cluster.cpp:279` 的 `create_default_node_group` 逻辑

## 采纳与拒绝的 AI 建议

### 采纳

- **单 Controller 多 Reconciler 架构模式**：AI 建议参考 CloudNativePG 的设计，将 GTM/CN/DN/NodeGroup 的生命周期拆分为独立 Reconciler。这比单一庞大的 reconciler 更易维护和测试。
- **三个独立 StatefulSet 而非一个**：AI 最初的方案是单个 StatefulSet 管理所有节点类型，但经过讨论改为三个独立 StatefulSet。理由：GTM/CN/DN 的 Pod 模板差异大（启动命令、健康检查、存储需求完全不同），独立 StatefulSet 允许独立的水平扩缩容。
- **init container 执行 initdb**：将 initdb 放在 initContainer 中而非在容器启动脚本中，利用 K8s 的 init container 串行执行保证初始化完成后再启动主进程。
- **pgxc_node 通过 SQL 直接管理**：AI 曾提议用 ConfigMap 注入，但 pgxc_node 是 PG 系统表，最终确认通过 Controller 执行 SQL 更可靠。
- **Headless Service DNS 而非 Pod IP**：在 `CREATE NODE` 时使用 Headless Service 的稳定 DNS 名称，避免 Pod 重启后 IP 变化导致路由表失效。

### 拒绝

- **在 init container 中调用 opentenbase_ctl**：AI 建议保留 `opentenbase_ctl` 作为底层工具，但 opentenbase_ctl 依赖 SSH（libssh2）进行节点间通信。在 K8s 环境中，Pod 间通信应通过 K8s API 和网络层，引入 SSH 增加复杂度和安全风险。改为用原生 `initdb` 命令。
- **使用 etcd 做 GTM 选主**：AI 建议引入 etcd 替代 StatefulSet 的固定序号机制。拒绝理由：增加外部依赖，而 StatefulSet Pod-0 作为 Master 的模式在 PG Operator 中广泛验证（CloudNativePG、StackGres 均使用此模式）。
- **为每个节点类型创建独立的 CRD**（如 `GTMCluster`、`CNCluster`、`DNCluster`）：AI 曾认为这样可以独立管理每类节点的生命周期。拒绝理由：这三类节点紧密耦合（CN 需要知道 DN 地址、DN 需要知道 GTM 地址），拆成独立 CRD 会导致循环依赖和复杂的 cross-CRD 协调。
- **使用 MutatingWebhook 注入 init container**：AI 建议通过 webhook 自动注入 pgxc_node 初始化脚本。拒绝理由：PoC 阶段过度设计，Phase 1 用 Job 方式更简单可调试。
- **自动触发数据再平衡**：AI 建议扩容 DN 后自动创建 Job 执行 `ALTER TABLE DISTRIBUTE BY`。拒绝理由：此操作可能长时间锁表，应作为 Day-2 手动操作，至少需要用户显式批准。

## 调研过程中的纠错记录

| 初始理解 | 发现的问题 | 修正 |
|---------|-----------|------|
| GTM/CN/DN 的 initdb 参数类似 | 查看 `build_initdb_cmd` 发现 GTM 使用 `initgtm` 而非 `initdb`，且不接受 `--nodename` 和 `--nodetype` 参数 | 修正了 GTM init container 的实现 |
| CN 和 DN 的 PostgreSQL 版本相同 | 查看源码确认 CN 和 DN 使用相同的 `postgres` 二进制，只是启动参数不同（`--coordinator` vs `--datanode`） | 确认可以使用同一 Docker 镜像 |
| 集中式实例也有 GTM | 查看 `install_centralized_instance` 代码，确认集中式模式无 GTM | 修正了集中式模式的 StatefulSet 编排 |
| pgxc_node 只需在一个 CN 上更新 | 查看源码和手动测试，确认每个节点都有独立的 pgxc_node 表，需在所有 CN/DN 上同步 | 修正了路由表同步策略 |

## 总结

本任务涉及大量外部调研（4 个 Operator）和内部架构分析（OpenTenBase 10+ 源码文件）。AI 在以下方面提供了显著价值：
- **广度**：并行调研 4 个 Operator，提取可复用的设计模式
- **深度**：逐函数分析 OpenTenBase 的部署流程，确保 CRD 和 Controller 设计与实际源码一致
- **纠错**：在源码交叉核对中发现并修正了多处理解偏差

所有 AI 输出的技术判断均经过 OpenTenBase 源码验证，未验证的外部信息（如受登录保护的文档）已明确标注不确定性。
