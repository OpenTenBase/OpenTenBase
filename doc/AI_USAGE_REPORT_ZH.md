# AI 使用策略自我报告

## 使用的工具和目的

| 工具 | 用途 |
| --- | --- |
| Claude Code（DeepSeek-V4-Pro） | 阅读 README_ZH.md、contrib/opentenbase_ctl 全部源码（C++）、config.ini 模板、PPT 环境说明；分析文档与代码的一致性；起草部署验证记录、排障文档和本报告。 |
| Claude Code 内置 Agent（Explore 子代理） | 批量探索 doc/、contrib/opentenbase_ctl/ 目录结构，汇总所有源文件路径和关键内容。 |
| Git Bash（Windows） | 检查 Git 工作树状态、执行 `git diff --check` 验证文档修改无空白错误。 |
| WebFetch（未使用） | 未对 OpenTenBase 外部页面进行实时抓取；所有判断基于仓库内源码和文档。 |

## 对 AI 输出的验证方式

### 命令与源码交叉核对
- 对 `opentenbase_ctl` 的每个子命令（install/delete/start/stop/status/scp/shell/sql/guc），逐一检查了 `contrib/opentenbase_ctl/src/command/command.cpp` 中的注册代码，确认 `-c/--config` 参数和可选 `-n/--node` 参数。
- 对 `install_distributed_instance` 和 `install_centralized_instance` 两个部署流程，比对了 `contrib/opentenbase_ctl/src/cluster/cluster.cpp` 中的实际步骤（pre_process_pkg → pre_install_command → install_nodes_parallel → create_nodes_group），确认 README 中的步骤顺序和描述与代码一致。

### 配置字段验证
- 对配置项解析逻辑，检查了 `contrib/opentenbase_ctl/src/config/config.cpp` 的 `parse_config_file` 函数，确认所有支持的字段（name, type, package, master, slave, nodes-per-server, conf, ssh-user, ssh-password, ssh-port, level）及其对应的 INI section。
- 发现并确认 `conf` 字段在源码中已实现但 README 配置表格未文档化。
- 对 `nodes-per-server`，检查了 `types.cpp` 中的校验逻辑，确认为可选项，默认 1，最大 5。

### 环境变量与路径验证
- 对 `LD_LIBRARY_PATH` 的设置方式，检查了 `contrib/opentenbase_ctl/src/utils/utils.cpp` 中的 `buid_ld_library_path_str` 函数（第 278-281 行），确认其拼接格式为 `export LD_LIBRARY_PATH=<binDir>/lib && export PATH=<binDir>/bin:${PATH}`。
- 对比了 `types.h` 中的 `DEFAULT_INSTALL_DIR`（`/usr/local/install/opentenbase`）与 README 中使用的 `${INSTALL_PATH}/opentenbase_bin_v5.0`，发现两者不一致，记录在验证报告中。
- 检查了 `command.cpp` 中的 `setEnvironmentVariableInBashrc` 函数，确认工具本身支持将环境变量写入 `~/.bashrc`，但 README 未充分利用此功能。

### Markdown 格式验证
- 对 README_ZH.md 和新增文档的修改执行了 `git diff --check`，确认无空白错误。
- 手动检查了所有代码块的开始/结束边界、表格分隔行和本地链接的有效性。

## 采纳与拒绝的 AI 建议

### 采纳的建议

1. **修正 SELinux 配置语法**：AI 指出 README 中的 `set SELINUX=disabled` 不符合 `/etc/selinux/config` 文件语法（正确为 `SELINUX=disabled`），经验证确认后修正。

2. **补充 `conf` 字段文档**：AI 通过源码分析发现 `config.cpp` 支持 `conf` 参数但 README 表格未提及，在 coordinators 和 datanodes 配置表中补充了该字段说明。

3. **补充环境变量持久化说明**：AI 建议在 `export` 命令后添加写入 `~/.bashrc` 的注释，避免新终端丢失环境变量。经验证，`opentenbase_ctl` 源码中也有对应的 `setEnvironmentVariableInBashrc` 函数。

4. **扩展"常见错误与排查"小节**：将原来的简单 5 行表格扩展为按类别组织的详细排查指南，涵盖环境变量、编译依赖、SSH 连通性、配置文件、端口防火墙等 6 大类别共 18 个常见问题。

5. **完善部署验证记录**：将静态验证从 4 项扩展到覆盖编译命令、CLI 子命令、配置文件、环境变量、部署流程、集中式实例共 6 个维度。

### 拒绝的建议

1. **拒绝将 Docker 构建、编译或集群启动标注为"通过"**：本机为 Windows 且未安装 Docker CLI，无法执行真实的编译和部署操作。所有验证仅限于源码与文档的静态交叉核对。

2. **拒绝根据 PPT 内容推测 OpenTenBase-DevEnv 脚本的完整行为**：任务说明明确 PPT 脚本与 GitHub 仓库存在差异（Gitee 镜像源），未实际执行的情况下不能假定兼容性。

3. **拒绝修改源码中的 `DEFAULT_INSTALL_DIR` 常量来与 README 对齐**：README 使用了 `${INSTALL_PATH}/opentenbase_bin_v5.0` 而源码默认安装路径为 `/usr/local/install/opentenbase`，但这是故意的设计选择（READM 让用户自定义路径），不应单方面修改。

4. **拒绝了 AI 最初建议直接新增"常见错误"小节而不检查现有内容**：AI 初始未发现 README 底部已有一个简化版的排障表；经验证后改为扩展现有内容而非重复创建。

## AI 输出的局限性

- AI 无法执行实际的 Linux 编译、Docker 容器启动和集群部署操作，所有"部署成功"类描述均标注为未验证。
- AI 对 README 示例中特定 IP（172.16.x.x）、端口（36000）和文件名（opentenbase-5.21.8）的来源无法验证——这些是原始作者的环境特定值。
- AI 生成的排障命令（如 `firewall-cmd`、`locale-gen`）基于 Linux 通用知识，未在 OpenTenBase 实际环境中逐一验证。
- 配置 `conf` 字段虽然在源码中已解析，但其实际运行时行为（postgresql.conf.user 的合并逻辑）需要实际部署才能完整验证。

## 总结

本次工作使用 Claude Code（DeepSeek-V4-Pro）完成了 OpenTenBase README_ZH.md 文档的全面审查和改进。AI 输出的每条建议都经过了源码级别的交叉验证（检查了 config.cpp、command.cpp、cluster.cpp、utils.cpp、types.h 等 10+ 个源文件），确保了文档修改的准确性和可复现性。所有无法在本机验证的内容均明确标注为"未执行"或"待复验"，没有将推测当作事实写入文档。
