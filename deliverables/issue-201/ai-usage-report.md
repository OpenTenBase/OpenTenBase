# AI 使用策略自我报告

## 1. 使用的 AI 工具

本任务实际使用了：

- **Codex CLI**：仓库检索、调用链阅读、证据整理、静态 PoC 文件生成和离线校验。
- **ChatGPT**：任务拆分、阶段验收、结论边界修正、最终调研报告、设计方案、Discussion 草稿和本报告撰写。

未将未实际使用的 Cursor、Claude Code 等工具写入报告。

## 2. AI 的主要用途

### Codex CLI

- 检查仓库状态和已有修改；
- 搜索 GTM、Coordinator、DataNode、`pgxc_node`、`pgxc_pool_reload()` 等实现；
- 追踪 `opentenbase_ctl` 的安装与初始化调用链；
- 整理 CloudNativePG 1.30 官方资料；
- 建立 OpenTenBase 与 CloudNativePG 的差异/复用矩阵；
- 生成 PoC 范围文档、CRD 草案、样例资源、状态样例和资源伪结构；
- 编写并执行离线校验脚本；
- 记录真实工具可用性、命令结果和未验证项。

### ChatGPT

- 将大任务拆分为 Stage 0、1、2、3A、3B、4A、4B、5A；
- 对每个阶段设定范围、禁止项和固定验收格式；
- 审查 Codex 结论是否有证据支持；
- 修正过度推断、版本错误和字段歧义；
- 根据已验收材料撰写最终交付文本。

## 3. 人工参与

人工负责：

- 决定调研目标和阶段边界；
- 在本地仓库运行 Codex CLI；
- 将每个阶段输出提交给 ChatGPT 验收；
- 处理 Codex CLI 更新、网络连接和会话中断；
- 确认不执行提交、推送和破坏性 Git 操作；
- 上传最终归档供人工综合写作；
- 最终审核拟提交内容。

AI 没有直接控制 GitHub 提交或推送。

## 4. 使用边界

本任务采用以下边界：

1. 所有 OpenTenBase 关键结论必须能追溯到仓库源码、官方文档或真实命令输出。
2. CloudNativePG 调研仅使用官方文档、官方仓库和发布说明。
3. 不把搜索命中直接写成结论，必须阅读上下文。
4. 不将“没有发现”写成穷尽证明。
5. 不伪造安装、Kubernetes、数据库或校验成功日志。
6. 不把静态 CRD 和 YAML 写成可运行 Operator。
7. 不执行 SSH、部署、数据库生命周期或破坏性命令。
8. 不执行 commit、push 或创建 PR。
9. 对未确认行为显式标记为 `Unconfirmed` 或超出范围。

## 5. 分阶段工作方式

为避免上下文过长导致错误，任务没有一次性交给 Codex，而是分阶段完成：

- **Stage 0**：工作区检查；
- **Stage 1**：角色与拓扑证据；
- **Stage 2**：部署、初始化和主备调用链；
- **Stage 3A**：CloudNativePG 官方架构调研；
- **Stage 3B**：差异与复用矩阵；
- **Stage 4A**：PoC 范围和设计假设；
- **Stage 4B**：静态 CRD 与 PoC 草案；
- **Stage 5A**：只读归档和导出。

每个阶段结束后停止，由 ChatGPT 验收后才进入下一阶段。

## 6. 真实修正记录

| AI 初步结论或输出 | 发现的问题 | 修正后的处理 |
|---|---|---|
| “GTM 不存用户数据”标为 Verified | 负面检索不是穷尽证明 | 改为“在已审查范围内支持，但未穷尽验证” |
| `pgxc_pool_reload()` 被描述为直接重新加载拓扑元数据 | 表述超过源码证据 | 收窄为拓扑变更后刷新/重载连接池缓存和连接 |
| GTM standby 与 `pgxc_node` 只注册 active GTM 被写成冲突 | 可能是部署层和目录层的正常区别 | 改为待追踪的架构关系 |
| Coordinator 容易被写成无状态 | CN 保存目录和拓扑状态 | 改为“控制面/目录意义上不完全无状态”，PVC 需求仍未确认 |
| CloudNativePG 最新稳定版写为 v1.29.1 | 官方 release 已为 v1.30.0 | 全部资料基线统一到 1.30 |
| Lease 与 fencing 混为一谈 | Lease 本身不是 fence | 区分 promotion gate 与 primary isolation fencing |
| “Direct Pod control”列为直接可复用 | 容易提前决定 workload primitive | 改为“数据库角色感知生命周期控制”可借鉴，具体 primitive 未决 |
| Coordinator `count` + `standbyCount` 语义歧义 | 无法判断 standby 是总数还是每 primary 数 | 改为 `primaryCount` 与 `standbyCountPerPrimary` |
| CRD 被描述为已验证 structural | 未完成 API Server admission | 改为 structural-schema-oriented draft，明确未验证项 |

以上均是任务中真实发生的修正，不是为了丰富报告而补写的虚构案例。

## 7. 验证过程

实际执行或记录的验证包括：

- `git status --short`
- `git diff --check`
- 文件存在性和行数检查
- 证据文件路径与引用检查
- CRD、样例和伪结构的一致性检查
- 禁止资源 Kind 检查
- 自定义 Python 离线验证器
- `kubectl apply --dry-run=client` 尝试

最终离线验证器输出：

```text
SUMMARY: 38 passed, 0 failed, 1 warning
```

警告为 PyYAML 不可用，因此使用文本级回退检查。

`kubectl apply --dry-run=client` 因本机没有可访问的 API Server/OpenAPI 端点而未完成。没有使用 `--validate=false` 将无验证结果伪装成成功。

## 8. 未验证范围

本任务没有验证：

- OpenTenBase 在 Kubernetes 中真正初始化和启动；
- DNS 名称能否替代现有 IP 字段；
- topology/group SQL 的成功和幂等性；
- 自动故障转移、promotion、fencing；
- 分布式一致性备份恢复；
- 在线 shard 扩缩容与数据重分布；
- 生产级 TLS、权限、监控和升级；
- CRD 的 API Server admission 与 structural-schema enforcement。

## 9. AI 带来的效率提升

AI 提高了以下效率：

- 快速定位跨目录源码和调用链；
- 将分散证据整理为可审查表格；
- 自动生成一致的 PoC 文件和校验器；
- 通过固定阶段模板减少遗漏；
- 快速发现字段、版本和术语不一致。

## 10. AI 的局限

- 容易根据通用 PostgreSQL/Kubernetes 常识过度推断 OpenTenBase 行为；
- 容易混用不同版本的官方文档；
- 容易把概念借鉴写成实现选择；
- 长上下文会触发会话压缩和连接错误；
- 静态文件校验不能代替真实部署；
- AI 生成的“人工验证过程”必须由真实命令记录约束，否则可能失真。

## 11. 最终评价

AI 适合承担检索、初稿、结构化整理和静态检查，但不应独立决定分布式数据库的架构事实。通过分阶段执行、逐阶段人工验收、来源约束和真实验证记录，本任务将 AI 输出控制在可追溯、可修正的范围内。
