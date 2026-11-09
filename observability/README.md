# OpenTenBase 可观测性接入指南（Prometheus / postgres_exporter / Grafana）

本目录提供一套把 OpenTenBase 分布式集群（CN / DN / GTM）接入 Prometheus 生态监控栈的方案与可直接运行的物料，目标是把集群的运行状态、连接、吞吐与慢查询从"黑盒"变成可观测、可告警、可回溯。

## 为什么需要

OpenTenBase 兼容 PostgreSQL，但作为分布式集群，单机 `pg_stat_*` 视图难以拼出全局视图：CN 上看不到 DN 的细节，跨节点的连接与延迟分布需要额外采集。社区已有成熟的 PostgreSQL 监控方案（`postgres_exporter` + Prometheus + Grafana 面板 9628），本指南给出针对 OpenTenBase 分布式形态的适配要点。

## 架构

```
 CN (协调节点) ──┐
 DN (数据节点) ──┼── postgres_exporter (每节点一个实例) ──> Prometheus ──> Grafana (面板 9628)
 DN (数据节点) ──┘         采集 pg_stat_* / pgxc_* 指标       (file_sd 发现)     可视化/告警
```

- 每个 CN / DN 端点各跑一个 `postgres_exporter`，用最小权限账号连接。
- Prometheus 通过 `file_sd` 动态发现节点（`targets/*.json`），节点扩缩容只需改 targets 文件。
- Grafana 导入面板 **9628（PostgreSQL Database）** 并绑定 Prometheus 数据源。

## 目录内容

| 文件 | 说明 |
| --- | --- |
| `telemetry_user.sql` | 创建最小权限采集账号 `telemetry_user`（仅授予 `pg_monitor`，不碰业务权限） |
| `prometheus.yml` | Prometheus 配置，`file_sd` 按文件发现 CN/DN 端点 |
| `targets/cn.json` | CN 端点登记（示例） |
| `targets/dn.json` | DN 端点登记（示例） |
| `otel-stack.compose.yaml` | docker compose 一键起 Prometheus + postgres-exporter + Grafana |

## 快速开始

1. **创建采集账号**：在 CN 以超级用户执行 `telemetry_user.sql`（先改掉 `CHANGE_ME` 口令）。

   ```sql
   -- 验证采集账号可读系统目录
   SELECT * FROM pg_stat_database LIMIT 1;
   ```

2. **登记节点端点**：修改 `targets/cn.json`、`targets/dn.json` 中的 `HOST:PORT` 为真实 CN/DN 地址；新增节点就加一个 JSON 文件，Prometheus 15 秒内自动发现。

3. **启动监控栈**：

   ```bash
   docker compose -f otel-stack.compose.yaml up -d
   ```

   并把 exporter 的 `DATA_SOURCE_NAME` 改为对应节点的 `telemetry_user` 连接串（每个节点一个 exporter 实例）。

4. **配置可视化**：浏览器打开 `http://<服务器>:3000`，添加 Prometheus 数据源，导入面板 **9628**。

## OpenTenBase 适配要点

- **多节点发现**：CN 与 DN 端口不同、数量可变，用 `file_sd` 而不是静态写死 job。
- **最小权限**：采集账号只给 `pg_monitor`，并固定 `search_path = pg_catalog, pg_monitor`，避免误碰业务 schema。
- **分布式指标**：可基于 OpenTenBase 特有的 `pgxc_class`（分片分布）、`pgxc_node`（节点状态）补充自定义指标，观察分片倾斜与节点健康。
- **建议关注**：连接数、查询吞吐、复制延迟、慢查询、存储 IO；CN 侧重点看路由与聚合开销，DN 侧重点看扫描与锁。

## 追踪延伸（可选）

指标之外，如需链路追踪：应用侧使用 `otelsql`（Go）/ `@opentelemetry/instrumentation-pg`（Node）等注入，经 OTLP 发送到 OpenTelemetry Collector，用 `spanmetrics` 连接器产出延迟直方图，即可在 Tempo/Jaeger 中观察一条跨 CN→DN 查询的 span 与延迟分布。

## 说明

- 本目录物料为方案示例，接生产集群前请替换口令、核对端口与网络策略。
- 欢迎补充 DN 侧自定义指标、告警规则与更多面板适配。
