# 结果提交说明

本目录不包含伪造或从其他环境复制的性能数字。`summary_template.csv` 定义了最小结果表结构。
`wsl_smoke_20260728.md` 和同名 CSV 是从当前源码构建的 1 GTM + 1 CN + 2 DN 单机 WSL2
集群的三次真实烟测结果；它们用于验证工具链和分布式路径，不能外推为生产容量。

正式提交需要：

1. 在真实 OpenTenBase 集群运行至少三次固定矩阵；
2. 保留每次运行的 `environment.json`、`summary.csv`、`raw/`、`distribution.csv` 和
   `host_metrics/`；
3. 报告各点中位数和离散程度，不只选择最好的一次；
4. 明确说明单机容器结果不能外推到多物理机生产拓扑；
5. 将真实结果和 Discussion URL 添加到本目录或 PR 描述。

空单元格表示“未测量”，不能填写 0。0 是一个真实测量值，会改变自动判断。
