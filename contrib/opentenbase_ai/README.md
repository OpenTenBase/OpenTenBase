# opentenbase_ai

`opentenbase_ai` 是 OpenTenBase 的**库内 AI 扩展**：它让用户可以直接用 SQL 调用大模型（LLM），
把"文本生成、结构化取值、翻译、摘要、情感分析、问答、文本向量化、图片理解"这些能力
变成和 `SELECT`、`UPDATE` 一样的普通数据库操作。

典型价值：

- **数据不出库就能加工**：对表里的整列文本做翻译、摘要、打标、抽取数值，无需写 ETL、无需导出数据。
- **声明式模型管理**：模型端点、鉴权、默认参数都放在一张配置表里，切模型只是改一行配置。
- **与分布式架构天然契合**：模型配置表是复制表，所有 CN 配置一致、无需逐节点下发。
- **协议无关**：任何兼容 OpenAI Chat Completions / Embeddings / 多模态接口的服务都能接入
  （公有云大模型、私有化部署、vLLM / Ollama 等自建推理服务）。

---

## 文档导航

| 你是 | 建议阅读 |
| --- | --- |
| **DBA / 运维** | [前置条件](#前置条件与依赖) → [安装](#安装) → [快速开始](#快速开始) → [模型管理](#模型管理) → [分布式部署注意事项](#分布式部署注意事项) → [安全与权限](#安全与权限) → [常见问题](#常见问题与排错) |
| **应用 / 数仓开发者** | [快速开始](#快速开始) → [函数参考](#函数参考) → [典型应用场景](#典型应用场景) → [GUC 参数](#guc-参数) |
| **数据库内核开发者** | [实现原理与架构](#实现原理与架构) → [面向内核开发者的说明](#面向内核开发者的说明) → [扩展与二次开发](#扩展与二次开发) |

---

## 前置条件与依赖

| 项 | 说明 |
| --- | --- |
| OpenTenBase | v5.x（PostgreSQL 10 内核，分布式形态：CN / DN / GTM） |
| 必备扩展 | [`pgsql-http`](../pgsql-http)（`contrib/pgsql-http`），本扩展 `requires = 'http'` |
| 系统依赖 | libcurl（由 `pgsql-http` 提供），建议 libcurl ≥ 7.6x 并带 SSL |
| 网络 | **所有 CN 节点**都必须能访问模型服务地址（HTTP/HTTPS，必要时走代理） |
| SQL 模式 | `sql_mode = 'all'`，PostgreSQL 模式与 Oracle 兼容模式（`opentenbase_ora`）下均可用 |

> `opentenbase_ai` 本身是**纯 SQL/PLpgSQL 实现**（除一个只用于注册 GUC 的小型 C 模块外），
> 不引入额外的第三方 C 依赖，编译安装非常轻量。

---

## 安装

### 1. 编译安装

随源码整体编译时，`contrib/Makefile` 已包含本插件；也可以单独编译：

```bash
# 整体编译（推荐）
cd ${SOURCECODE_PATH}
make -sj && make install
cd contrib && make -sj && make install

# 仅编译本插件
cd ${SOURCECODE_PATH}/contrib/opentenbase_ai
make -sj && make install
```

安装后会在 `${INSTALL_PATH}/opentenbase_bin_v5.0/share/extension/` 下看到：

```
opentenbase_ai.control
opentenbase_ai--1.0.sql
```

### 2. 创建扩展

`opentenbase_ai` 依赖 `http` 扩展，需要先创建（或使用 `CASCADE` 自动创建）：

```sql
-- 建议在 CN（协调节点）上执行
CREATE EXTENSION IF NOT EXISTS http;
CREATE EXTENSION IF NOT EXISTS opentenbase_ai;
-- 或者： CREATE EXTENSION opentenbase_ai CASCADE;
```

创建后新增如下对象：

| 对象 | 位置 | 说明 |
| --- | --- | --- |
| `ai` schema | `ai` | 所有函数的命名空间，已 `GRANT USAGE TO PUBLIC` |
| `ai_model_list` 表 | `public.ai_model_list` | 模型配置表，**复制表**（`DISTRIBUTE BY REPLICATION`） |
| `ai.models` 视图 | `ai.models` | 对外暴露的模型清单（**不含**鉴权头），已 `GRANT SELECT TO PUBLIC` |
| 函数 | `ai.*` | 模型管理 / 调用 / 业务封装函数，见[函数参考](#函数参考) |

> 卸载：`DROP EXTENSION opentenbase_ai;`

---

## 快速开始

### 第 1 步：注册一个对话补全模型

以任意兼容 OpenAI 协议的服务为例：

```sql
SELECT ai.add_completion_model(
    'gpt-4o-mini',                                  -- 模型别名（后续引用它）
    'https://api.openai.com/v1/chat/completions',   -- 请求地址
    '{"model": "gpt-4o-mini", "temperature": 0.7}'::jsonb,  -- 默认请求参数
    '<YOUR_API_KEY>',                               -- API Key
    'openai'                                        -- provider 备注（可选）
);
```

### 第 2 步：设定默认模型（可选但推荐）

```sql
SET ai.completion_model = 'gpt-4o-mini';
```

### 第 3 步：调用

```sql
SELECT ai.generate_text('用一句话解释什么是 HTAP 数据库');
```

如果你不想设置 GUC，也可以在每次调用时显式指定模型：

```sql
SELECT ai.generate_text('用一句话解释什么是 HTAP 数据库', 'gpt-4o-mini');
```

查看当前已注册的模型：

```sql
SELECT * FROM ai.models;
```

---

## 模型管理

所有模型配置集中存放在 **`public.ai_model_list`**（复制表，全 CN 一致）。

### 配置表字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `model_name` | `TEXT` | 主键，模型别名，SQL 中引用它 |
| `model_provider` | `TEXT` | 供应商标识，仅作备注/分类 |
| `request_type` | `TEXT` | HTTP 方法，通常为 `POST` |
| `request_header` | `http_header[]` | 请求头数组，鉴权信息（`Authorization: Bearer xxx`）放这里 |
| `uri` | `TEXT` | 模型服务请求地址 |
| `content_type` | `TEXT` | 请求体 Content-Type，通常 `application/json` |
| `default_args` | `JSONB` | **默认请求体参数**，调用时与用户传入的 `config` 合并（后者优先） |
| `json_path` | `TEXT` | **结果抽取 SQL 模板**，用 `%L` 占位响应正文，见下文 |

### 管理函数

| 函数 | 返回 | 说明 |
| --- | --- | --- |
| `ai.add_model(model_name, request_header, uri, default_args, model_provider, request_type, content_type, json_path)` | `boolean` | 通用注册函数，**任意 HTTP 大模型接口**都用它 |
| `ai.add_completion_model(model_name, uri, default_args, token DEFAULT NULL, model_provider DEFAULT NULL)` | `boolean` | 对话补全模型（OpenAI Chat Completions 协议），自动填 `json_path` |
| `ai.add_embedding_model(model_name, uri, default_args, token DEFAULT NULL, model_provider DEFAULT NULL)` | `boolean` | 向量化模型（OpenAI Embeddings 协议），自动填 `json_path` |
| `ai.add_image_model(model_name, uri, default_args, token DEFAULT NULL, model_provider DEFAULT NULL)` | `boolean` | 多模态视觉模型（走 Chat 协议返回文本），自动填 `json_path` |
| `ai.update_model(model_name, config, value)` | `boolean` | 修改某个模型配置的**单个字段**，`config` 为列名，`value` 为文本 |
| `ai.delete_model(model_name)` | `boolean` | 删除模型 |

示例：

```sql
-- 修改默认参数
SELECT ai.update_model('gpt-4o-mini', 'default_args', '{"model":"gpt-4o-mini","temperature":0.2}');

-- 修改请求地址
SELECT ai.update_model('gpt-4o-mini', 'uri', 'https://new-endpoint.example.com/v1/chat/completions');

-- 删除模型
SELECT ai.delete_model('gpt-4o-mini');
```

### 使用通用 `add_model` 接入非标接口

`add_model` 是唯一的"万能入口"，下面这个例子注册一个返回 `{"json":{"prompt":"..."}}` 结构的自定义服务：

```sql
SELECT ai.add_model(
    'my_custom_model',                                     -- model_name
    ARRAY[ROW('Authorization', 'Bearer my-token')::http_header],  -- request_header
    'https://my-llm.internal/v1/generate',                 -- uri
    '{"top_k": 5}'::jsonb,                                 -- default_args
    'internal',                                            -- model_provider
    'POST',                                                -- request_type
    'application/json',                                    -- content_type
    $$SELECT %L::jsonb->'json'->>'prompt'$$                 -- json_path
);
```

> **`json_path` 怎么写？**
> 它是一个**带 `%L` 占位符的 SQL 表达式模板**，执行时 `%L` 会被替换成 HTTP 响应正文（一个字符串字面量），
> 整条语句通过 `EXECUTE` 执行并取回结果。因此你可以用任何 JSON/字符串函数做抽取、拼接、清洗。
>
> - 对话补全：`SELECT %L::jsonb->'choices'->0->'message'->>'content'`
> - 向量化：`SELECT %L::jsonb->'data'->0->'embedding'::TEXT`
>
> ⚠️ 因为 `json_path` 会被动态执行，它属于**受信任配置项**，不要给普通用户开放 `ai_model_list` 的写权限。

---

## 函数参考

以下函数均在 `ai` schema 下，默认从 GUC 取模型名（也可显式传 `model_name`），
`config` 用于覆盖 `default_args`（使用 `default_args || config` 合并，同名键以 `config` 为准）。

### 底层调用

| 函数 | 返回 | 说明 |
| --- | --- | --- |
| `ai.raw_invoke_model(model_name, user_args JSONB)` | `http_response` | 直接发起请求并返回原始响应（`status` / `content_type` / `headers` / `content`），**不校验状态码** |
| `ai.invoke_model(model_name, user_args JSONB)` | `TEXT` | 请求 + 校验 `HTTP 200` + 按 `json_path` 抽取结果；非 200 直接抛异常 |

```sql
-- 排查问题时很有用：看原始响应
SELECT (ai.raw_invoke_model('gpt-4o-mini',
        '{"messages":[{"role":"user","content":"hi"}]}'::jsonb)).*;
```

### 文本生成（按返回类型自动适配提示词与清洗）

| 函数 | 返回 | 说明 |
| --- | --- | --- |
| `ai.generate(prompt, dummy anyelement DEFAULT 'text', model_name DEFAULT NULL, config DEFAULT '{}')` | `anyelement` | 通用入口，通过 `dummy` 的**类型**决定输出类型 |
| `ai.generate_text(prompt, model_name DEFAULT NULL, config DEFAULT '{}')` | `TEXT` | 自由文本 |
| `ai.generate_int(prompt, model_name DEFAULT NULL, config DEFAULT '{}')` | `INTEGER` | 只返回整数（自动剔除多余字符，非法结果兜底为 `0`） |
| `ai.generate_double(prompt, model_name DEFAULT NULL, config DEFAULT '{}')` | `DOUBLE PRECISION` | 只返回数值（支持小数/科学计数法） |
| `ai.generate_bool(prompt, model_name DEFAULT NULL, config DEFAULT '{}')` | `BOOLEAN` | 只返回 `true` / `false`，兼容 `yes/no/1/0/t/f/y/n` |

```sql
SELECT ai.generate_text('把"数据库"翻译成英文');
SELECT ai.generate_int('用户说："这个功能太慢了，严重影响我工作。" 请给紧急程度打分(0-10)，只输出数字');
SELECT ai.generate_bool('判断这句话是否表达了负面情绪："质量很好，非常满意"');
```

### 业务封装函数

| 函数 | 返回 | 说明 |
| --- | --- | --- |
| `ai.translate(text_content, target_language, model_name DEFAULT NULL, config DEFAULT '{}')` | `TEXT` | 翻译到目标语言 |
| `ai.summarize(text_content, model_name DEFAULT NULL, config DEFAULT '{}')` | `TEXT` | 摘要 |
| `ai.sentiment(text_content, model_name DEFAULT NULL, config DEFAULT '{}')` | `TEXT` | 情感倾向，返回 `positive` / `negative` / `neutral` / `mixed` |
| `ai.extract_answer(context, question, model_name DEFAULT NULL, config DEFAULT '{}')` | `TEXT` | **仅依据给定上下文**回答问题（RAG 场景） |
| `ai.embedding(input, model_name DEFAULT NULL, config DEFAULT '{}')` | `TEXT` | 文本向量化，返回形如 `[0.01, -0.02, ...]` 的文本；`input` 为 `NULL` 时返回 `NULL` |
| `ai.image(prompt, image_url TEXT, model_name DEFAULT NULL, config DEFAULT '{}')` | `TEXT` | 图片理解，传入图片 URL 或 data URL |
| `ai.image(prompt, image BYTEA, mime_type DEFAULT 'image/jpeg', model_name DEFAULT NULL, config DEFAULT '{}')` | `TEXT` | 图片理解，直接传二进制（自动 base64）；支持 `image/jpeg` / `image/png` / `image/gif` / `image/webp` |

---

## 典型应用场景

### 1. 对整列数据做翻译 / 摘要（数仓批量加工）

```sql
-- 给还没生成摘要的文章批量生成摘要
UPDATE articles
   SET summary = ai.summarize(content)
 WHERE summary IS NULL;
```

建议按主键分批提交，避免单个事务长时间持有连接（见[性能与稳定性](#性能与稳定性)）。

### 2. 情感分析与打标

```sql
ALTER TABLE feedback ADD COLUMN IF NOT EXISTS sentiment TEXT;

UPDATE feedback
   SET sentiment = ai.sentiment(content)
 WHERE sentiment IS NULL;
```

### 3. 从非结构化文本抽取结构化字段

```sql
SELECT id,
       ai.generate_int('给这条工单的紧急程度打分(0-10)，只输出数字：' || content) AS urgency,
       ai.generate_bool('这条工单是否需要立即人工介入？只回答 true 或 false：' || content) AS need_human,
       ai.generate_double('估算这条工单涉及的业务损失（单位：元），只输出数字：' || content) AS est_loss
  FROM tickets;
```

### 4. 向量检索（配合 [pgvector](../pgvector)）

```sql
CREATE EXTENSION IF NOT EXISTS vector;

SELECT ai.add_embedding_model(
    'text-embedding-3-small',
    'https://api.openai.com/v1/embeddings',
    '{"model": "text-embedding-3-small"}'::jsonb,
    '<YOUR_API_KEY>',
    'openai'
);

ALTER TABLE docs ADD COLUMN IF NOT EXISTS emb vector(1536);

-- 生成向量（ai.embedding 返回文本，可直接转 vector）
UPDATE docs SET emb = ai.embedding(content)::vector WHERE emb IS NULL;

-- 相似检索
SELECT id, content
  FROM docs
 ORDER BY emb <=> (ai.embedding('如何配置 OpenTenBase 集群')::vector)
 LIMIT 5;
```

### 5. 检索增强问答（RAG）

```sql
SELECT ai.extract_answer(
    context := string_agg(content, E'\n' ORDER BY id),
    question := 'OpenTenBase 的 CN 和 DN 分别是什么？'
)
FROM docs
WHERE emb <=> (ai.embedding('CN 和 DN 的区别')::vector) < 0.4;
```

### 6. 图片理解

```sql
-- 方式 A：公网 / 可访问的图片 URL
SELECT ai.image('这张图里有什么？', 'https://example.com/chart.png');

-- 方式 B：库里存二进制
SELECT ai.image('识别这张票据的金额', receipt_img, 'image/png') FROM receipts WHERE id = 1;

-- 方式 C：把服务器上的本地图片读进来
SELECT ai.image('描述这张图', lo_get(lo_import('/data/ai/demo.png')));
```

---

## GUC 参数

扩展定义了三个会话级（`PGC_USERSET`）字符串参数，用于指定默认模型：

| 参数 | 默认值 | 作用范围 |
| --- | --- | --- |
| `ai.completion_model` | 无（`NULL`） | `ai.generate*` / `translate` / `summarize` / `sentiment` / `extract_answer` |
| `ai.embedding_model` | 无（`NULL`） | `ai.embedding` |
| `ai.image_model` | 无（`NULL`） | `ai.image` |

```sql
-- 会话级
SET ai.completion_model = 'gpt-4o-mini';

-- 库级 / 用户级
ALTER DATABASE postgres SET ai.completion_model = 'gpt-4o-mini';
ALTER ROLE app_user  SET ai.completion_model = 'gpt-4o-mini';

-- 全局（需 reload，注意所有 CN 都要配置）
ALTER SYSTEM SET ai.completion_model = 'gpt-4o-mini';
```

> **关于 GUC 的加载**：这三个参数由 C 模块 `ai`（`ai.so` 的 `_PG_init`）注册。
> 若希望它们在集群中长期生效、可被 `SHOW` 查询，请在 `postgresql.conf` 中加入
> `shared_preload_libraries = 'ai'`，或在会话中执行 `LOAD 'ai';` 后再 `SET`。
> 更简单的做法是**不依赖 GUC**，在每次调用时显式传入 `model_name` 参数。

当既没有设置 GUC、又没有传 `model_name` 时，函数会报错：
`Completion model name is not set` / `Embedding model name is not set` / `Image model name is not set`。

---

## 分布式部署注意事项

`opentenbase_ai` 已针对 OpenTenBase 的分布式形态做了适配，但仍有几点需要注意：

1. **模型配置表是复制表**
   `public.ai_model_list` 以 `DISTRIBUTE BY REPLICATION` 创建，配置会复制到所有 CN。
   只要在读写的 CN 上注册/更新模型，其他 CN 也能立即看到一致配置，**无需逐节点操作**。

2. **网络出口是硬约束**
   发起 HTTP 请求的是**当前执行 SQL 的那个 CN 进程**，因此每个可能执行 AI 函数的 CN
   都必须能访问模型服务地址。私有化部署时请注意防火墙与代理设置：

   ```sql
   -- 为当前会话设置代理与超时（与 http 扩展共用）
   SELECT http_set_curlopt('CURLOPT_PROXY', 'http://proxy.internal:8080');
   SELECT http_set_curlopt('CURLOPT_TIMEOUT_MS', '30000');
   -- 用完后复位
   SELECT http_reset_curlopt();
   ```

3. **大模型调用是同步阻塞的**
   一次调用就是一次长耗时的网络往返，会占用 backend 连接和 CN 的 worker 资源。
   建议：
   - 批量加工时**分批 + 限制并发**，不要在一次事务里发起成千上万次调用；
   - 为每批任务设置合理超时（`CURLOPT_TIMEOUT_MS`），避免挂死连接；
   - 重要业务建议把 AI 加工与在线交易负载在 CN 上错峰或隔离。

4. **不要在 DN 上直接建扩展**
   分发 SQL 由 CN 下发；AI 函数调用建议统一从 CN 发起。

---

## 安全与权限

> ⚠️ 以下两点在生产环境请务必评估。

1. **API Key 以明文形式存放在 `ai_model_list.request_header` 中**，
   而安装脚本默认执行了 `GRANT SELECT ON public.ai_model_list TO PUBLIC`。
   这会导致**任何用户都能读到 API Key**。生产环境建议：

   ```sql
   REVOKE ALL ON public.ai_model_list FROM PUBLIC;
   GRANT SELECT ON public.ai_model_list TO dba_role;   -- 仅授权给管理员
   -- 普通业务用户只通过 ai.models 视图查看模型清单（该视图不含 request_header）
   ```

2. **`json_path` 会被动态执行**（`EXECUTE format(json_path, response)`），
   相当于一段受信任的 SQL 模板。仅管理员可写 `ai_model_list` 是基本前提。

3. 扩展函数为 `SECURITY INVOKER`（默认），即**以调用者权限执行**。
   因此普通用户若需要新增模型，必须显式授予 `ai_model_list` 的写权限——通常不建议这样做。

4. 建议为密钥单独规划备份与脱敏策略，避免备份文件泄露密钥。

---

## 实现原理与架构

```
应用 / DBA                    OpenTenBase 集群
┌────────────┐        ┌────────────────────────────────────────────────┐
│ psql / 应用 │        │  CN (Coordinator)                              │
│  SQL 调用    │───────▶│   ai.generate_text() / ai.translate() / ...    │
└────────────┘        │            │                                   │
                      │            ▼                                   │
                      │   ai.invoke_model() ──读取──▶ public.ai_model_list │
                      │            │                  (DISTRIBUTE BY     │
                      │            │                   REPLICATION)      │
                      │            ▼                                   │
                      │   http()  (pgsql-http / libcurl)               │
                      └────────────┼───────────────────────────────────┘
                                   │ HTTPS
                                   ▼
                          大模型服务 (OpenAI 兼容 / 自建 vLLM、Ollama ...)
```

调用链可概括为：

```
业务函数 (generate_text / translate / sentiment / summarize / extract_answer / embedding / image)
    └── ai.generate / 直接调用
        └── ai.invoke_model      ← 校验 HTTP 200、按 json_path 抽取
            └── http(('POST', uri, headers, content_type, body)::http_request)   ← pgsql-http
                └── libcurl
```

### 目录结构

```
contrib/opentenbase_ai/
├── Makefile                    # 构建脚本（EXTENSION / MODULES / REGRESS）
├── ai.c                        # 仅用于注册 ai.completion_model / ai.embedding_model / ai.image_model
├── opentenbase_ai.control      # 扩展控制文件（default_version=1.0, requires='http', sql_mode='all'）
├── opentenbase_ai--1.0.sql     # 全部数据库对象：schema / 表 / 视图 / 函数
├── sql/opentenbase_ai.sql      # 回归测试用例
└── expected/opentenbase_ai.out # 预期输出
```

---

## 面向内核开发者的说明

### 设计要点

1. **极简 C 层**
   `ai.c` 只做一件事：在 `_PG_init` 中通过 `DefineCustomStringVariable` 注册三个
   `PGC_USERSET` 的字符串 GUC，`_PG_fini` 为空。扩展的**所有逻辑都在 PL/pgSQL 中**，
   这使得功能迭代、二次开发（甚至线上热修）都不需要重编译内核或扩展。

2. **纯 SQL 扩展的加载语义**
   由于 SQL 脚本没有绑定任何 `MODULE_PATHNAME` 符号，`ai.so` 不会被"用到即加载"。
   GUC 要想长期生效需要 `shared_preload_libraries = 'ai'` 或显式 `LOAD 'ai'`。
   另外注意：`MODULES = ai` 生成的是 `ai.so`，而控制文件里的
   `module_pathname = '$libdir/opentenbase_ai'` 目前并未被 SQL 引用——二者命名不一致，
   若后续要引入 C 函数，建议统一为 `MODULES = opentenbase_ai` 或将 `module_pathname` 改为 `$libdir/ai`。

3. **参数合并语义**
   请求体由 `default_args || user_args` 生成（jsonb 的 `||` 语义：右侧同名键覆盖左侧）。
   因此调用方的 `config` 可以覆盖模型级默认值（如临时调 `temperature`），
   但无法移除默认参数，只能覆盖。

4. **结果抽取是"模板 + 动态执行"**
   `json_path` 存的是形如 `SELECT %L::jsonb->'choices'->0->'message'->>'content'` 的模板，
   运行时 `EXECUTE format(json_path, response_content)`。
   好处是兼容任意响应结构，代价是把 SQL 注入面绑定到了配置表的写权限上。

5. **错误模型简单直接**
   `invoke_model` 只接受 `status = 200`，其余一律 `RAISE EXCEPTION`，并带上状态码与响应正文；
   没有重试、退避、限流、用量统计。这些需要由上层业务或外部的调度框架补齐。

6. **可观测性**
   关键中间量都通过 `RAISE DEBUG` 输出（`exec_args`、`request_sql`、`response_content`、`result`），
   排查问题时把 `client_min_messages` 调到 `debug1` 即可看到完整的请求与响应。

### 已发现的实现细节 / 待改进项

| 位置 | 现象 | 影响 / 建议 |
| --- | --- | --- |
| `opentenbase_ai.control` | `module_pathname = '$libdir/opentenbase_ai'`，但 Makefile 中 `MODULES = ai` | 当前未被 SQL 引用，暂无影响；后续加 C 函数前需统一 |
| `ai.add_image_model` | 其 `json_path` 与 completion 相同（`choices[0].message.content`） | 只适用于"走 Chat 协议、以文本形式返回结果"的视觉理解模型；若对接 `/v1/images/generations`（返回 `data[0].url` / `b64_json`），请改用 `add_model` 自定义 `json_path` |
| `opentenbase_ai--1.0.sql` | `GRANT SELECT ON public.ai_model_list TO PUBLIC` | 会暴露 `request_header` 中的 API Key，生产环境建议 REVOKE，改用 `ai.models` 视图 |
| `GRANT USAGE ON SCHEMA ai TO PUBLIC` | 所有用户可调用全部 AI 函数 | 会产生费用与外网请求，建议按需 REVOKE，改为按角色授权 |
| `ai.generate` | 数值清洗用正则剔除非法字符，空结果兜底为 `0` | 模型返回异常时可能得到"看似合法"的 0；对精度敏感的场景建议加上下文校验或二次确认 |
| `ai.embedding` | 返回 `TEXT` 而非 `vector` | 与 `pgvector` 组合时需显式 `::vector` 转换 |
| `ai.update_model` | `value` 以文本传入后按目标列类型 `::%s` 转换 | 修改 `default_args` 时需传入合法 JSON 文本 |
| 整体 | 无重试 / 无限流 / 无 token 计量 | 高并发或高可用场景建议在上层引入重试与配额控制 |

---

## 扩展与二次开发

### 新增一个模型 Provider

只要能通过 HTTP 交互，就不需要改代码，用 `add_model` 描述清楚即可。
关键是**找准两个东西**：请求体结构（`default_args`）和响应结构（`json_path`）。

### 新增一个业务意图函数

参照 `ai.summarize` 的写法，在 `opentenbase_ai--1.0.sql` 中追加一个 PL/pgSQL 函数即可：

```sql
CREATE OR REPLACE FUNCTION ai.classify(
    text_content text,
    labels text[],
    model_name text = NULL,
    config jsonb = '{}'::jsonb
) RETURNS text AS $$
BEGIN
    RETURN ai.generate_text(
        format('把下面的文本归类到这些标签之一，只输出标签名：%s\n\n文本：%s',
               array_to_string(labels, ', '), text_content),
        model_name, config);
END;
$$ LANGUAGE plpgsql;
```

> 注意：如果已经安装过旧版本，直接改 `opentenbase_ai--1.0.sql` 不会作用于已有实例。
> 正式发布时应新增一个升级脚本（如 `opentenbase_ai--1.0--1.1.sql`）并提升 `default_version`。

---

## 性能与稳定性

- **同步阻塞**：每次 AI 调用都是完整的一次 HTTP 往返，耗时从几百毫秒到几十秒不等。
  批量场景请分批提交，并考虑用 `LIMIT` + 循环的方式控制单批规模。
- **超时设置**：务必设置 `CURLOPT_TIMEOUT_MS` / `CURLOPT_CONNECTTIMEOUT_MS`，避免连接挂死。
- **幂等与重试**：扩展本身不重试。对重要的批量任务，建议在业务侧实现"写入结果表 + 失败重跑"，
  而不是直接在事务里反复重试。
- **NULL 与空值**：`ai.embedding(NULL)` 直接返回 `NULL`，便于在 `UPDATE ... WHERE col IS NOT NULL` 中使用；
  其他函数未做 NULL 短路，建议在 SQL 里自行过滤。
- **成本控制**：建议为 AI 加工单独规划调用入口与权限，避免全量用户随意调用产生费用。

---

## 测试

扩展自带回归测试（`REGRESS = opentenbase_ai`），用例全部使用 `httpbin.org` 作为模拟端点，
不会真实调用大模型服务：

```bash
cd ${SOURCECODE_PATH}/contrib/opentenbase_ai
make installcheck        # 需要已启动的实例
```

测试覆盖：扩展加载、schema/表创建、`add_model` / `add_completion_model` / `add_embedding_model` /
`add_image_model` 注册、`update_model` 更新、`delete_model` 删除，以及 `ai.models` 视图查询。

---

## 常见问题与排错

**Q1：`ERROR: Model xxx not found`**
模型中未注册该名字。检查 `SELECT * FROM ai.models;`，注意模型名大小写敏感。

**Q2：`ERROR: Completion model name is not set`（或 embedding / image 版本）**
既没有设置 `ai.*` GUC，也没有显式传 `model_name`。任选其一即可。

**Q3：`ERROR: Failure in http-request. http_code: 401, content: ...`**
HTTP 状态码不是 200。常见原因：API Key 错误或过期、额度不足、限流（429）、
模型名不被服务端识别。用 `ai.raw_invoke_model` 看完整响应体定位。

**Q4：请求超时 / 无法连接**
从**执行 SQL 的 CN 节点**上测试 `curl` 该地址，确认网络与代理可达；
然后设置 `CURLOPT_PROXY` 与 `CURLOPT_TIMEOUT_MS`。

**Q5：`ERROR: Invalid json path for model xxx` / 抽取结果为空**
`json_path` 与实际响应结构不匹配。先用 `ai.raw_invoke_model` 打印响应正文，再调整模板。

**Q6：HTTPS 证书校验失败**
可设置 `SELECT http_set_curlopt('CURLOPT_CAINFO', '/path/to/ca.pem');`
（仅在明确知道风险时才考虑 `CURLOPT_SSL_VERIFYPEER = 0`）。

**Q7：想看请求把什么发出去了**
```sql
SET client_min_messages = debug1;
SELECT ai.generate_text('hello');
```

---

## 许可证

本扩展随 OpenTenBase 一起发布，遵循 [BSD-3-Clause](../../LICENSE.txt) 许可证。

## 贡献

欢迎提交 Issue 与 PR，详见 [CONTRIBUTING.md](../../CONTRIBUTING.md) / [CONTRIBUTING_ZH.md](../../CONTRIBUTING_ZH.md)。
