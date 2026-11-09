# 图解表述核对表

对照对象：`contrib/otb-viz-docs/diagram/cluster-architecture.html`
核对方式：逐条回到仓库原文，确认"图中这句话是否能在项目里找到依据"。

## 技术表述

| # | 图中的表述 | 仓库依据 | 核对结果 |
|---|---|---|---|
| 1 | 集群由 CN、DN、GTM 三类角色组成 | `README_ZH.md:13`「一个 OpenTenBase 集群由多个 CoordinateNodes、DataNodes 和 GTM 节点组成」 | 一致 |
| 2 | 用户/客户端只连接 CN | `README_ZH.md:15`「用户总是连接到 CoordinateNodes」 | 一致 |
| 3 | CN 解析 SQL 并把查询拆成片段下发到 DN，再收集结果 | `README_ZH.md:15`「CoordinateNodes 将查询分解为在 DataNodes 中执行的片段，并收集结果」 | 一致 |
| 4 | CN 只保存元数据，不保存用户数据 | `README_ZH.md:13`「CoordinateNode 仅包含元数据」；同句「所有用户数据都存储在 DataNodes 中」 | 一致 |
| 5 | 全部用户数据存放在 DN 上，多个 DN 合起来才是全量 | `README_ZH.md:13`「所有用户数据都存储在 DataNodes 中」 | 一致 |
| 6 | GTM 负责全局事务管理，不存业务数据 | `README_ZH.md:13`「GTM 用于全局事务管理」（该句未提数据存储；"不存业务数据"由第 4 条"所有用户数据都在 DN"反推，属合理推论，已在卡片中避免绝对化措辞） | 一致（推论已标注） |
| 7 | CN 与 DN 共享同一份 schema | `README_ZH.md:13`「CoordinateNodes 和 DataNodes 共享相同的模式」 | 一致 |
| 8 | 节点类型在系统里用 `C` / `D` / `G` 表示 | `src/include/nodes/parsenodes.h:127-132`：`PGXC_NODE_COORDINATOR 'C'`、`PGXC_NODE_DATANODE 'D'`、`PGXC_NODE_GTM 'G'` | 一致 |
| 9 | 节点信息可以通过 `pgxc_node` 查到（`node_name`、`node_type`、`node_port`、`node_host`） | `src/include/catalog/pgxc_node.h:29,35,40,53` 定义上述字段；`contrib/otb-health/otb_health.sh` 第 [2] 项检查使用同一查询 | 一致 |
| 10 | CN 默认端口写作 `:11003` | `contrib/otb-health/otb_health.sh` 中 `CN_PORT=${CN_PORT:-11003}` | 一致（该脚本的可配置默认值，非协议规定） |

## 需要谨慎的两处

1. **"GTM 不存业务数据"**：仓库概览没有直接这句话，是从"所有用户数据都在
   DataNodes"推出的。卡片正文说的是"GTM 不存业务数据、只负责全局事务"，
   已在核对表中标为推论，不作为独立事实引用。
2. **"DN 不对外提供完整 SQL 入口"**：这是对"用户总是连接 CN"的补充解释，
   卡片把它放在"常见误解"里，用"应用应连接 CN"的表述，没有断言 DN 端口
   完全不可连。

## 类比部分

| 类比元素 | 对应概念 | 是否可能误导 | 处理方式 |
|---|---|---|---|
| 顾客 | 客户端 / 应用 | 否 | — |
| 服务员 | CN | 否（但服务员不做菜 ≠ CN 不做计算，需说明） | 卡片标注"CN 不做数据计算" |
| 灶台 | DN | 否 | — |
| 收银台 | GTM | 是：收银台容易让人以为它"管钱/存数据" | 卡片标注"GTM 不参与查询执行、不存用户表" |
| 一桌总单号 | 全局事务号 | 否 | — |

卡片底部已固定一句免责声明：**类比只帮助建立直觉，不是技术定义，边界以项目文档为准。**

## 图面自查

- 节点数 5（客户端、CN、DN-1、DN-2、GTM），未超过 9 个上限。
- 珊瑚色焦点 2 个（CN、GTM），未滥用强调色。
- 所有连接线为水平/垂直或 `r=8` 圆角折线，无斜线。
- 每个箭头两端均从边的垂线方向进出，端口距圆角 ≥ 8px。
- 箭头标签均有不透明遮罩，与线之间留 6–10px 间隙。
- 已运行技能自检：`self_check.py` 返回 `OK`。
