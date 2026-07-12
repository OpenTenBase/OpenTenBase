# AI 使用策略自我报告

## 使用的工具和目的

| 工具 | 用途 |
| --- | --- |
| Codex（GPT-5） | 阅读现有 README、PPT 提取内容和源码；起草部署说明、排障文档和验证记录。 |
| PowerShell | 检查 Git 工作树、检查 Docker 命令、检索源码和执行文档静态校验。 |
| Web 检索 | 确认 OpenTenBase 官方仓库及公开快速入门资料；任务页无法直接解析时未将搜索摘要当作验收依据。 |

## 对 AI 输出的验证方式

- 对 `opentenbase_ctl` 命令的描述，检查了 `contrib/opentenbase_ctl/src/command/command.cpp` 中实际注册的子命令和 `-c/--config` 参数。
- 对配置项说明，检查了 `contrib/opentenbase_ctl/config/config.ini` 模板和 `src/types/types.cpp` 的校验逻辑；例如 `nodes-per-server` 的最大值为 5。
- 对 README 的配置模板路径和命令路径，使用仓库内真实文件路径和 `PG_HOME` 定义进行交叉核对。
- 对 Markdown 修改执行了 `git diff --check`，并检查新增文档的代码块边界和本地链接。
- 对 Docker 部署不使用模拟结果：实际执行 `docker --version` 后记录了 Docker CLI 缺失，并停止执行后续容器构建命令。

## 采纳与拒绝的 AI 建议

采纳：

- 用仓库现有 `config.ini` 作为 README 的配置起点，替代不存在的 `prepare config` 流程。
- 将工具调用统一为 `"${PG_HOME}/bin/opentenbase_ctl"`，减少工作目录依赖。
- 补充 PATH、LD_LIBRARY_PATH、SSH、端口、配置文件和 CN 连接的可操作排障步骤。

拒绝：

- 拒绝将 Docker 容器构建、编译或集群启动写为“通过”，因为本机没有 Docker CLI。
- 拒绝根据 PPT 猜测 `OpenTenBase-DevEnv` 当前脚本的完整用法，因为任务说明明确其与 PPT 中的脚本存在差异。
- 拒绝保留 README 中的 `prepare config` 指令，因为源码中没有对应子命令。

## 局限与后续复验

AI 可以协助发现不一致并生成候选修复，但不能替代真实 Linux/Docker 环境中的编译、部署和 CN SQL 验证。建议在可联网的 Linux 主机上按 `DEPLOYMENT_VALIDATION_ZH.md` 的复验步骤补充容器日志、`status` 输出和 `select * from pgxc_node;` 结果，再提交 PR。
