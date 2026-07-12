# AI 使用策略自我报告 —— 术语表任务

## 使用的工具和目的

| 工具 | 用途 |
|------|------|
| Claude Code（DeepSeek-V4-Pro） | 阅读 README_ZH.md、源码中的架构逻辑（`types.h`、`types.cpp`、`cluster.cpp`、`command.cpp`）、现有部署文档；梳理核心术语；起草术语表和架构图 |
| Git Bash（Windows） | 执行 `git checkout -b` 创建分支、`grep` 搜索源码、`commit` 和 `push` |
| WSL2 Ubuntu 22.04 | 第一阶段部署验证中积累的实战经验，为本术语表的"单机部署注意事项"和"常见新手困惑"部分提供了真实素材 |

## 对 AI 输出的验证方式

- **架构准确性**：每条术语的定义都交叉验证了 `README_ZH.md` 的概览章节和 `contrib/opentenbase_ctl/src/types/types.h` 中的节点类型常量（`NODE_TYPE_CN_MASTER`、`NODE_TYPE_DN_SLAVE`、`INSTANCE_TYPE_CENTRALIZED` 等），确保 CN/DN/GTM 三种角色的职责描述与源码实现一致。
- **节点命名规则**：核对了 `types.cpp` 中的 `generate_nodes` 和 `get_node_name` 函数，确认 CN 的命名格式为 `cnNNNN`、DN 为 `dnNNNN`、GTM 为 `gtmNNN`。
- **配置字段说明**：对比了 `config.cpp` 的 `parse_config_file` 解析逻辑与 README 中的配置表格，确保 `type=distributed|centralized`、`nodes-per-server`、`conf` 等字段的解释与代码一致。
- **分布式/集中式差异**：对照了 `cluster.cpp` 中 `install_distributed_instance` 和 `install_centralized_instance` 两个函数的步骤差异，确认集中式模式下无 GTM 和 CN。
- **新手困惑速查**：基于第一阶段 WSL 部署中实际遇到的 7 个问题（如 CREATE TABLE 失败、数据目录被覆盖等），提炼为 FAQ 式解答。
- 对 Markdown 的 ASCII 架构图做了多平台预览检查（缩进对齐、框线字符兼容性）。

## 采纳与拒绝的 AI 建议

### 采纳

- **15 个术语的选择**：AI 建议从 README 概览、部署配置、系统目录表三个层面选取术语，覆盖了架构角色（CN/DN/GTM）、部署模式（分布式/集中式）、管理工具（opentenbase_ctl）、配置概念（config.ini）和内部机制（pgxc_node、sharding group），构成的术语网络较完整。
- **架构全景图 + 速查表双层呈现**：AI 提议同时用 ASCII 图（展示节点间数据流）和表格（展示角色职责和类比），让不同习惯的读者都能快速理解。
- **"常见新手困惑速查"以 FAQ 呈现**：不按术语表格式排布，而是按"先困惑、后解答"的方式组织，降低阅读负担。
- **术语相互引用**：在说明中交叉引用相关术语（如 DN 词条中引用 node group），帮助读者建立概念之间的联系。

### 拒绝

- **拒绝添加不存在的概念**：AI 曾提议加入 "Docker/容器化部署" 作为独立术语，但由于 README 主流程不涉及 Docker，仅在部署验证文档中作为补充手段提及，故未放入核心术语表。
- **拒绝过度深入 PG-XL 历史**：AI 初始稿对 Postgres-XL 的发展历史（XC→XL→OpenTenBase）用了较长篇幅，与新手导览的定位不符，改为简短说明。
- **拒绝使用 Mermaid 流程图**：AI 建议用 Mermaid 语法绘制架构图，但考虑到 Mermaid 在部分终端和纯文本编辑器中不可渲染，改为 ASCII 框图 + 表格的双重呈现方式。

## 总结

本次术语表的编写以 README 和源码为基础，AI 负责初稿生成和结构组织，人工负责交叉验证和审校。最终产出的术语表在准确性（所有概念均经过源码核实）、可读性（分层呈现、FAQ 补充）和新手友好度（类比法、速查表）三个维度上达到了预期目标。
