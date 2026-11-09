# AI 使用记录

活动鼓励用 AI 辅助，这里记录"AI 做了什么"和"人核对了什么"，便于复查。

## 工具

- Codex（CLI/桌面端），配合 `diagram-design` 技能生成架构图。

## 过程

| 步骤 | AI 做的事 | 人工核对 |
|---|---|---|
| 1 | 在仓库里检索 `CN` / `DN` / `GTM` 的说明，定位到 `README_ZH.md` 概览章节 | 打开 `README_ZH.md:13,15` 确认原文 |
| 2 | 找到节点类型常量与 `pgxc_node` 字段定义 | 打开 `src/include/nodes/parsenodes.h`、`src/include/catalog/pgxc_node.h` 确认 |
| 3 | 找到可直接引用的 `pgxc_node` 查询语句 | 打开 `contrib/otb-health/otb_health.sh` 确认语句与端口默认值 |
| 4 | 按技能规范生成 `cluster-architecture.html`（单文件 + 内联 SVG + 无障碍标题） | 阅读生成的 HTML，逐条比对核对表 |
| 5 | 导出 SVG 与 PNG，运行 `self_check.py` | 确认自检输出 `OK`，确认 PNG 可正常打开 |
| 6 | 起草 README、核对表、本记录 | 逐句检查是否出现仓库里没有的断言 |

## AI 输出中被修改或删掉的内容

- 初稿把 GTM 写成"transaction 主节点"，与仓库表述不符，改为"全局事务管理器"。
- 初稿在图中直接写了执行结果示例，按活动要求（未实际执行的结果必须标"预期"）
  改为不放任何运行输出，只在 README 里给出可自行执行的命令。
- 初稿用了外部超链接，技能的自检规则不允许单文件卡片里出现远程引用，
  改为纯文本 URL + README 中的链接。

## 未使用 AI 的部分

- 图中所有技术结论均来自仓库文件，不是模型记忆。
- 生活类比由人确认"哪些地方会误导"，并在图与 README 中显式标注免责。
