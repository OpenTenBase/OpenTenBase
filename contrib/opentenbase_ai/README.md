# opentenbase_ai 从 0 到 1

> 基于 OpenTenBase `master` 分支 `contrib/opentenbase_ai`（扩展版本 `1.0`）逐行核对源码与回归测试整理。
> 核对时间：2026-09-19。函数名、参数、默认值均以该版本源码为准。

## 0. 上游状态（认领前必读）

| 对象 | 状态（2026-09-19 查询） | 说明 |
| --- | --- | --- |
| Issue #304「add contrib/opentenbase_ai README.md」 | Open | 任务母 Issue |
| PR #305「docs(contrib): add README for opentenbase_ai」 | Open，未合并 | 611 行，覆盖安装/函数参考/架构/FAQ，已较完整 |
| PR #306「docs: add opentenbase_ai extension README」 | Open，未合并 | 228 行，精简版 |
| `contrib/opentenbase_ai/README.md` | 上游不存在 | 两个 PR 均在补这个文件 |

**本文档的差异化定位**：不重复 PR #305/#306 已覆盖的通用函数参考，重点补充——
① 一条 SQL 跑通小场景（摘要生成 + 文本分类）的最小闭环；
② 基于 httpbin.org 的 mock 验证到底验证了什么（请求构造 / 响应处理，**非真实推理**）；
③ 配合 `contrib/pgvector` 的 embedding 写入 + 相似度检索 RAG 小案例。

---

## 1. 安装前置

| 项 | 要求 | 依据 |
| --- | --- | --- |
| 数据库 | OpenTenBase master（**PG18 基线**，见本组《向量索引构建与诊断增强》技术报告；CN/DN/GTM 分布式形态） | 源码位于 `contrib/` 下 |
| 依赖扩展 | `http`（即 `contrib/pgsql-http`） | `opentenbase_ai.control` 中 `requires = 'http'` |
| 系统依赖 | libcurl（含开发头文件，建议带 SSL） | pgsql-http 需要 |
| 网络 | 所有可能执行 AI 调用的节点（CN，以及涉及该查询的 DN）能访问模型服务端点 | HTTP 请求由数据库进程内发出 |
| SQL 模式 | `sql_mode = 'all'`（PostgreSQL / Oracle 兼容模式均可用） | control 文件 |

> **复用本组已有环境**：犀牛鸟 VM（192.168.35.128）已装 PG 18.6（`/opt/pg18/bin/pg_config`）+ 本组增强版 pgvector v0.8.6。实测前需先确认该 VM 是 OpenTenBase 还是原生 PG18（`SELECT version();`），并按 §8 清单补装 `http` / `opentenbase_ai`。

扩展本体是纯 SQL + plpgsql（仅 `ai.c` 一个小模块用于注册 3 个 GUC），编译很轻量。

## 2. 安装

```bash
# 方式 A：源码树内编译（推荐）
cd ${SOURCECODE_PATH}
make -C contrib/pgsql-http && make -C contrib/pgsql-http install
make -C contrib/opentenbase_ai && make -C contrib/opentenbase_ai install

# 方式 B：PGXS 独立编译
cd contrib/opentenbase_ai && USE_PGXS=1 make && USE_PGXS=1 make install
```

```sql
-- 在 CN 上执行（回归测试同款顺序）
CREATE EXTENSION IF NOT EXISTS http;
CREATE EXTENSION IF NOT EXISTS opentenbase_ai;
-- 或：CREATE EXTENSION opentenbase_ai CASCADE;
```

安装后新增对象（全部来自 `opentenbase_ai--1.0.sql`，**已核对**）：

| 对象 | 位置 | 说明 |
| --- | --- | --- |
| schema `ai` | `ai.*` | 函数命名空间，`GRANT USAGE TO PUBLIC` |
| 表 `ai_model_list` | `public.ai_model_list` | 模型配置表，`DISTRIBUTE BY REPLICATION`（复制表） |
| 视图 `ai.models` | `ai.models` | 模型清单（不含 `request_header`/`json_path`），`GRANT SELECT TO PUBLIC` |
| GUC | `ai.completion_model` / `ai.embedding_model` / `ai.image_model` | 默认模型名，`PGC_USERSET`，未设置时为 NULL（`ai.c`） |

卸载：`DROP EXTENSION opentenbase_ai;`（依赖 `http` 需单独 drop）。

## 3. 模型注册

### 3.1 配置表 `public.ai_model_list` 的表分布设计

```sql
CREATE TABLE public.ai_model_list (...) DISTRIBUTE BY REPLICATION;
```

- **为什么用复制表**：OpenTenBase 是 CN/DN 分布式架构。模型配置（端点、鉴权头、默认参数）需要所有节点一致；复制表在每个 DN 都有完整副本，CN 上注册一次即全局生效，无需逐节点下发，也避免按 `model_name` 分片导致的跨节点查询。
- 权限：表仅 `GRANT SELECT TO PUBLIC`；`ai.*` 函数按 PG 默认向 PUBLIC 授予 EXECUTE，即**任何能连库的用户都能注册/删除模型**。生产环境建议显式 `REVOKE EXECUTE ON FUNCTION ai.add_model(text, http_header[], text, jsonb, text, text, text, text) FROM PUBLIC;` 等并仅授权管理员（扩展 SQL 未做此限制，属已知注意点）。

### 3.2 关键字段

| 字段 | 用途 |
| --- | --- |
| `model_name` | 主键，调用时的句柄 |
| `request_type` / `uri` / `content_type` | HTTP 方法 / 端点 / Content-Type |
| `request_header` | `http_header[]` 数组，通常放 `Authorization: Bearer <token>` |
| `default_args` | JSONB 默认请求体参数，调用时与 `user_args` **浅合并**（`default_args || user_args`，顶层键覆盖） |
| `json_path` | **结果提取模板**，见 3.3 |

### 3.3 `json_path` 的用途（结合源码）

`ai.invoke_model` 拿到 HTTP 响应后执行：

```sql
EXECUTE format(json_path_v, response_content) INTO result;
```

即 `json_path` 是一条 SQL **format 模板**，`%s`/`%L` 处会被整个响应体文本替换，查询结果即模型返回值。内置注册函数写死的模板：

- `add_completion_model`：`SELECT %L::jsonb->'choices'->0->'message'->>'content'`（OpenAI Chat Completions）
- `add_embedding_model`：`SELECT %L::jsonb->'data'->0->'embedding'::TEXT`（OpenAI Embeddings）
- `add_image_model`：同 completion（多模态消息）

换成任意协议时，只需 `ai.add_model(...)` 自定义 `json_path` 指向实际响应结构。

### 3.4 注册函数速查

| 函数 | 场景 |
| --- | --- |
| `ai.add_completion_model(model_name, uri, default_args, token=NULL, provider=NULL)` | OpenAI 兼容对话 |
| `ai.add_embedding_model(...)` | OpenAI 兼容向量 |
| `ai.add_image_model(...)` | OpenAI 兼容视觉 |
| `ai.add_model(...8 参...)` | 任意协议（需自定义 `request_type`/`content_type`/`json_path`） |
| `ai.update_model(name, config, value)` / `ai.delete_model(name)` | 改配置 / 删除 |

## 4. 最小调用示例（场景：摘要生成 + 文本分类）

完整可执行 SQL 见 [`examples/01_minimal_call.sql`](examples/01_minimal_call.sql)。核心三步：

```sql
-- 1. 注册（以 DeepSeek 为例，任何 OpenAI 兼容端点均可）
SELECT ai.add_completion_model(
    'my-chat',
    'https://api.deepseek.com/v1/chat/completions',
    '{"model": "deepseek-chat", "temperature": 0.3}'::jsonb,
    'sk-your-token', 'deepseek');

-- 2. 设默认模型（可选；不设则每次显式传 model_name 参数）
SET ai.completion_model = 'my-chat';

-- 3. 一条 SQL 完成任务
SELECT ai.summarize('（一段长文本）');                    -- 摘要生成
SELECT ai.generate_bool('这条评论是好评吗：东西不错');      -- 文本分类（布尔）
SELECT ai.sentiment('物流太慢了，失望');                   -- 文本分类（情感词）
```

**一条 SQL 的力量**：第 3 步可直接套在表上，例如
`UPDATE feedback SET summary = ai.summarize(content), is_positive = ai.generate_bool('好评返回true：' || content);`
——无需导出数据、无需 ETL，整列文本在库内完成加工。

## 5. mock 验证（httpbin.org）——验证了什么、没验证什么

回归测试 `sql/opentenbase_ai.sql` 用 `https://httpbin.org/post` 作为模型端点。可复现脚本见 [`examples/02_mock_httpbin.sql`](examples/02_mock_httpbin.sql)。

- ✅ 能验证：请求构造是否正确（httpbin 回显收到的 method/headers/body）、`default_args || user_args` 合并行为、`json_path` 模板能否从回显 JSON 中提取字段、错误路径（模型不存在 / http_code ≠ 200）。
- ❌ 不能验证：真实模型推理质量、`json_path` 与真实响应结构的匹配（httpbin 回显结构 ≠ OpenAI 响应结构）、token 鉴权是否被真实服务接受。

结论：mock 通过 ≠ 真实调用通过；接真实模型前务必用真实端点重跑一次 §4。

## 6. 自由延展：pgvector RAG 小案例

见 [`examples/03_rag_pgvector.sql`](examples/03_rag_pgvector.sql)。要点：

1. `ai.add_embedding_model` 注册向量化模型；
2. `ai.embedding('文本')` 返回 JSON 数组文本（如 `[0.01, ...]`），直接 `::vector` 入库；
3. `ORDER BY embedding <=> ai.embedding('查询')::vector LIMIT k` 完成相似度检索。

**叠加本组已有产物**（feat/pgvector-pg18-enhanced 分支的增强函数，v0.8.6）：

4. 建 HNSW / IVFFlat 索引后，用 `hnsw_recommend_ef_search('idx', top_k, target_recall)` / `ivfflat_recommend_probes('idx', target_recall)` 给出检索参数推荐，RAG 检索质量可解释、可调参；
5. 规模控制：本组已复现上游 bug——PG18 + pgvector 0.8.6 在 **2 万行以上建 HNSW 索引段错误**，RAG 演示数据量保持 ≤ 1 万行；VM pgsql_tmp 较小，避免大规模排序/建索引操作。

## 7. 常见问题（FAQ）

| 现象 | 原因 / 解法 |
| --- | --- |
| `Model xxx not found` | `ai_model_list` 无此名；检查注册是否成功、`SET ai.completion_model` 拼写 |
| `Completion model name is not set` | 未传 `model_name` 且 GUC `ai.completion_model` 为 NULL |
| `Failure in http-request. http_code: ...` | 端点/鉴权/网络问题；`ai.raw_invoke_model` 返回完整 `http_response` 便于排查 |
| `Invalid json path for model xxx` | 该行 `json_path` 为 NULL，用 `ai.update_model` 修正 |
| GUC 设置后不生效 | GUC 由 C 模块在 `_PG_init` 注册，需扩展已创建且会话/库已加载该库 |
| 想改请求协议 | 用 8 参 `ai.add_model` 自定义 `request_type`/`content_type`/`json_path` |

## 8. 已实测 / 待验证清单

> 诚实声明：本组当前无运行中的 OpenTenBase 集群，以下为**源码级核对**与**待环境实测**的划分。

| 项 | 状态 |
| --- | --- |
| 函数名/参数/默认值/GUC 名与 master 源码一致 | ✅ 已核对（`opentenbase_ai--1.0.sql`、`ai.c`、回归测试逐文件比对，2026-09-19） |
| 编译安装命令 | ⏳ 待实测（make 目标与 Makefile 一致） |
| `CREATE EXTENSION` 及对象创建 | ⏳ 待实测 |
| 注册函数执行 + `ai_model_list` 写入 | ⏳ 待实测（回归测试已覆盖同等语句） |
| httpbin mock 验证请求构造 | ⏳ 待实测（脚本就绪，见 sql/02） |
| 真实模型端点一次完整调用 | ⏳ 待实测（需有效 API token + 网络） |
| pgvector 相似度检索 | ⏳ 待实测（复用 VM 192.168.35.128 的本组 pgvector v0.8.6；先跑 `SELECT version()` 确认 OpenTenBase/PG18，补装 http + opentenbase_ai；HNSW 索引保持 ≤1 万行） |

## 8.1 交付规范对齐（沿用犀牛鸟 DELIVERY.md 约定）

- 仓库/分支：`Memsetqwq/OpenTenBase`，新建单分支 `feat/opentenbase-ai`（base = OpenTenBase:master，single-branch workflow）
- 文档：README 中英双版，与本组 doc/ 目录现有文档同步维护
- 证据：installcheck 输出 + benchmark/调用结果截图，按 §8 清单逐项回填「已实测」
- PR 说明中引用 Issue #304，并注明与 PR #305/#306 的互补关系

## 9. 大赛提交对齐（评分自查）

详见 [`成果卡.md`](../../成果卡.md)。关键路径：

1. **PR 提交是硬门槛**（代码贡献 20 分中的 PR 提交 10 分，不提交计 0）：`feat/opentenbase-ai` → `OpenTenBase/OpenTenBase:master`。
2. **技术文章截止前发布**（影响力产出 10 分，仅草稿计 0）：围绕本次贡献，附成果截图/链接与验证范围说明。
3. **WorkBuddy 使用证据**（AI 工具深度 10 分）：保留关键操作截图/日志（源码核对、文档生成、共享盘上传、任务协作迭代）。
4. **诚实标注待验证**（正确性与依据 15 分）：§8 清单保持「源码核对」与「环境实测」严格区分。

## 10. 参考

- 源码：`contrib/opentenbase_ai/`（`opentenbase_ai--1.0.sql`、`ai.c`、`Makefile`、`control`）
- 回归测试：`contrib/opentenbase_ai/sql/opentenbase_ai.sql`
- 依赖：`contrib/pgsql-http`
- Issue #304 / PR #305 / PR #306（均为 Open 状态，认领后建议在已有 PR 基础上补充而非另起）
