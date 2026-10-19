/*
 * pg_graph_search 1.0.0 — OpenTenBase 适配版
 * ---------------------
 * 属性图式搜索扩展。元函数 (meta function) 根据一张边表 (c1/c2/prop) 的结构参数，
 * 动态生成一组业务可直接调用的函数：N 度搜索、游标式搜索、最短路径(BFS 提前退出)、
 * 子图 JSON(绘图数据)、分层统计。
 *
 * 设计要点：
 *   1. 所有生成的函数都走一张统一的"边视图" <schema>.<prefix>_edge(src,dst,prop,w)，
 *      有向表直接映射，无向表自动 UNION ALL 双向展开。
 *   2. 标识符一律 format('%I') / 常量一律 format('%L')，杜绝拼接注入。
 *   3. plan() 只产出 DDL 文本不落库，install() 执行 plan() 并登记元数据。
 *   4. 最短路径用真正的 BFS 分层循环，命中即返回；递归 CTE 写法会全量展开后才过滤。
 *
 * OpenTenBase（Postgres-XL）适配：
 *   a. 生成物一律 schema 限定，不依赖 search_path（CN/DN 的 search_path 可能不同）。
 *   b. 本文件由 sql/ 片段拼装，注册表 DDL 的分布子句占位符在构建时替换：
 *      OpenTenBase 版 -> DISTRIBUTE BY REPLICATION；原生 PG 版 -> 空。
 *   c. CREATE EXTENSION 在 OpenTenBase 上是两阶段(PREPARE/EXECUTE)+全节点广播，
 *      本扩展只包含 SQL 对象，无 .so、无本地文件依赖，符合该协议的前提。
 *   d. 生成函数是 STABLE + 动态 SQL，由 CN 调用；动态 DDL 会按 XL 规则广播到所有节点。
 */

-- 使用：CREATE EXTENSION pg_graph_search;
