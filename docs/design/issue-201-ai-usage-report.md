<!--
Copyright (c) 2026 OpenTenBase Contributors
Licensed under the BSD 3-Clause License. See LICENSE.txt.
-->

# Issue #201 AI 使用策略自我报告

> 日期：2026-07-28
> 范围：OpenTenBase 接入 PostgreSQL 社区部署框架的调研、方案、CRD 草案和验证

## 1. 使用原则

本任务使用 AI 辅助做信息检索、源码定位、方案对比、文档结构化和机械校验。AI 不被当作事实来源；关键结论必须回到以下一手证据：

1. OpenTenBase 当前仓库的 README、`opentenbase_ctl` 源码和使用文档。
2. CloudNativePG 官方版本化文档和官方 GitHub 仓库。
3. Kubernetes CRD/OpenAPI 的可机器解析结果。

涉及数据安全的能力采用保守原则：没有数据库协议和故障测试证据时，不把“可能可做”写成“已支持”。因此 PoC 明确拒绝 DN 自动缩容、全局 PITR 和未验证的自动 failover。

## 2. AI 参与的工作

| 阶段 | AI 辅助内容 | 人工/证据约束 | 产出 |
| --- | --- | --- | --- |
| 调研 | 提取 Issue 验收项；筛选 CNPG、StackGres、Crunchy PGO、Zalando Operator | 只用项目官方资料确认核心能力和许可证 | 框架选择表 |
| 源码理解 | 搜索 GTM/CN/DN、initdb、pg_basebackup、CREATE/ALTER NODE、node group | 逐段读取当前 commit 对应源码，不依赖二手文章 | 当前安装状态机 |
| 设计 | 从拓扑、身份、存储、路由、一致性不变量推导 Operator 边界 | 每个自动化能力都要给出安全前置条件和失败行为 | 主设计文档 |
| PoC | 生成 CRD 和示例 CR | 用 YAML 解析、结构断言和 CRD/schema 检查验证 | 两个 YAML 文件 |
| 审校 | 检查链接、术语、不可见字符、文档相互引用和 git diff | 命令输出留作 PR checks 说明 | 验证记录 |

未使用 AI 生成或执行生产数据库操作；未向外部服务发送凭据、集群数据或私有代码。

## 3. “调研、比较、验证、修正”记录

### 3.1 调研

- 阅读 Issue #201 的任务要求和评价标准。
- 阅读 CNPG 的 Architecture、Container Image Requirements、Instance Manager、Service、Storage、Bootstrap、Monitoring 和 capability level 文档。
- 阅读 OpenTenBase README 和 `opentenbase_ctl` 的初始化、复制、节点目录与 node group 实现。
- 查看同一 Issue 已出现的公开 PR，避免只做重复的框架罗列，把重点转向可验证的不变量、fail-closed 边界和可执行 CRD。

### 3.2 比较

比较了三种实现路线：

1. fork CNPG；
2. OpenTenBase 高层 Operator 组合多个 CNPG `Cluster`；
3. 独立 Operator 复用 CNPG 的运行模式和生态接口。

评价标准不是“能复用多少代码”，而是“复用后是否仍能表达 OpenTenBase 的正确状态，以及上游是否承诺该用法”。最终推荐第三种；第二种保留为有退出门槛的兼容性实验。

### 3.3 验证与修正

本次至少发生了以下关键修正：

| 初始假设/常见说法 | 验证证据 | 修正后的结论 |
| --- | --- | --- |
| 可以把 CN/DN 当作自定义 PostgreSQL 镜像直接交给 CNPG | CNPG 1.30 官方文档只支持 PGDG 支持版本和 Primary/Hot Standby 架构 | 只能做 time-boxed spike，不能作为受支持方案 |
| OpenTenBase 初始化必须严格 `GTM -> DN -> CN` 串行 | `install_distributed_instance()` 在 GTM 主就绪后并行安装 CN/DN 主 | 硬依赖是 GTM 先就绪；CN/DN 主可并行；拓扑注册必须最后 |
| Pod Running/`pg_isready` 可以作为 Service 就绪条件 | OpenTenBase 还要求 GTM 可达和 CN 节点目录一致 | CN readiness 增加 topologyRevision 校验 |
| 多个 DN 分别做 PostgreSQL 备份即可得到集群 PITR | 分布式事务跨 DN，且恢复还依赖 GTM/拓扑状态 | 只复用备份传输层；全局 barrier/manifest/恢复验证必须新建 |
| DN 扩容就是 StatefulSet replicas 增加 | DN groups 代表数据分片，当前通用形态 expand/shrink 受限 | PoC 把 DN groups 视为 immutable，变更 fail closed |
| StatefulSet ordinal 可直接表示主备角色 | 主备角色可能切换，ordinal 只是稳定实例身份 | Service selector 由数据库确认的角色标签驱动 |

这些修正被同步写入设计、CRD 字段说明和风险表，而不是只保留在调研笔记中。

## 4. 质量与安全控制

- **来源可追溯**：主文档列出版本化官方链接和精确源码行链接。
- **不伪造运行结果**：本次没有可用 Kubernetes 集群，因此不声称部署 e2e 已通过；只验证静态交付物。
- **最小权限**：方案要求 namespace-scoped RBAC、Secret 引用和日志脱敏。
- **破坏性操作默认关闭**：DN 缩容、PVC 删除、自动切换和分布式 PITR 不进入 PoC 的“支持”范围。
- **幂等优先**：数据库实际状态优先于本地 marker，外部副作用验证成功后才更新 status。
- **可证伪门槛**：CNPG 组合方案、自动 failover、在线扩容和备份都有明确的 e2e 退出条件。

## 5. 已知局限与后续人工验证

1. 当前设计是 PoC 级控制面方案，不是已运行的 Operator 实现。
2. CRD 的 `resources`/`affinity` 为草案中的透传字段；正式实现应引用或生成 Kubernetes 结构化类型。
3. GTM 选主/fencing 的正式协议需要维护者确认。
4. topology diff SQL、CN/DN 切换后的元数据同步范围需要数据库专家审阅。
5. 全局备份 barrier 和 restore manifest 需要故障注入与整集群恢复演练。
6. CNPG child `Cluster` 兼容性需要隔离实验，且不能绕过上游支持声明。

## 6. 工具与模型披露

- AI 助手：OpenAI Codex（GPT-5 系列）
- 外部检索：GitHub 公共页面、CloudNativePG 官方文档
- 本地分析：`git`、`rg`、PowerShell、Python 文本/结构断言
- 版本控制：独立 git worktree 和 issue 专用分支，避免混入其他任务改动

最终责任仍由提交者承担：提交前应人工复核技术结论、引用、Discussion 内容和 PR diff，并由项目维护者决定方案是否进入实现阶段。
