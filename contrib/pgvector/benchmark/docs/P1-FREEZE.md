# Project2 P1 功能冻结基线

冻结标识：`project2-p1-v1-freeze-20260911`

冻结日期：2026-09-11

## 1. 冻结结论

Project2 P1 已完成《Project2：IVFFlat 索引诊断与优化建议系统——功能设计》
第 7.7 节定义的 1–8 项实施内容，可以进入代码整理、目录迁移和提交准备阶段。

## 2. 已冻结能力

1. `ivfflat_list_stats()` 与 `ivfflat_index_stats()` 只读结构接口。
2. pgvector 0.8.6 → 0.8.7 upgrade SQL 与新安装 SQL。
3. Structure Collector、逐 list JSONL、scan/summary schema 与派生指标。
4. Candidate、Evidence、Finding 数据契约。
5. P1-v1 Memory、Worker、Structure、Probes 和 Structure/Quality 联合规则。
6. 用户显式 SLO 过滤与多目标 Pareto 前沿。
7. evidence-backed proposed Recommendation；无 SLO 时不声明全局最佳配置。
8. 显式开启的 before/after Validation，包含新 Build/Quality ID、环境可比性和
   material-change tolerance。
9. 根 `README.md`、P1 `report.md`、Build/Quality report 与机器 JSON 输出。

## 3. 冻结验收证据

- Python：`make test`，35 项通过。
- pgvector P1 专项：`make installcheck REGRESS=ivfflat_stats`，1 项通过。
- pgvector 全量 SQL：`make installcheck`，15 项通过。
- 数据库端到端 smoke：`diag-p1-smoke-l2`。
  - Recall SLO 触发 `probes=1 → 5` 建议；
  - Recommendation 创建独立 Validation Build、Structure 和 Quality Run；
  - before/after 可比性通过；
  - Recommendation 状态为 `validated`。

## 4. 已知非阻塞边界

- PostgreSQL 未提供可靠 `planned_workers` 证据时，Worker quota 规则输出 `unknown`，
  不做推断。
- Validation 默认关闭，只有显式指定 `--validate-recommendations` 才执行；默认最多
  验证一条建议。
- 专用损坏页 fault-injection fixture 未纳入普通 SQL regression。
- P1 范围固定为 IVFFlat、warm-cache、single-client；并发、冷缓存、过滤查询、漂移和
  精确内部阶段属于 P2。
- 极短 smoke Build 可能因短于采样周期而缺少 P0 phase/resource sample，这不影响 P1
  诊断链路验收。

## 5. 冻结后的变更规则

后续允许：目录迁移、项目改名、import/path 修正、安装脚本、测试入口、CI、文档和
提交清理。

后续不再扩展：P1-v1 指标口径、规则语义、阈值默认值、Recommendation/Validation
状态模型。只有发现 correctness defect 或冻结验收回归失败时才解冻修改，并同步更新
本文件。
