# opentenbase_ai — OpenTenBase AI 调用扩展

`opentenbase_ai` 是 OpenTenBase 的 AI 能力扩展，它把大语言模型（LLM）的调用封装为一组 SQL 函数（`ai` schema），让 DBA 和应用开发者可以直接用 SQL 完成文本生成、翻译、摘要、情感分析、信息抽取、向量嵌入（embedding）与图像理解等任务，无需在数据库外再引入一层应用代码。

模型以"注册制"管理：先通过 `ai.add_*_model` 系列函数把模型端点（URL、鉴权头、默认参数、结果提取路径）登记到 `ai_model_list` 表中，之后即可按模型名调用。HTTP 请求由内置的 [pgsql-http](../pgsql-http/) 扩展发出，天然兼容 OpenAI Chat Completions / Embeddings / Vision 协议的服务端点（如 OpenAI、DeepSeek、Qwen、本地 vLLM/Ollama 网关等）。

本文面向三类读者：

| 读者 | 关注章节 |
|------|----------|
| DBA / 运维 | 前置依赖、编译安装、GUC 参数、注意事项 |
| 应用开发者 | 快速开始、模型注册与管理、函数参考、使用示例 |
| 数据库内核开发者 | 实现说明 |

## 功能特性

- SQL 内直接调用 LLM：`SELECT ai.generate_text('...')` 即可完成一次补全请求
- 兼容 OpenAI 协议的补全、嵌入、视觉（图像理解）三类模型端点
- 内置翻译、摘要、情感分析、问答抽取等常用场景函数
- 类型化生成：`ai.generate_int / generate_double / generate_bool` 直接返回对应标量类型
- 模型注册制管理：`ai_model_list` 表 + `ai.models` 视图，支持增删改查
- 请求参数按 JSONB 顶层浅合并（`default_args || user_args`，同名顶层键整体替换，不递归合并），调用时可按需覆盖默认参数
- 模型登记表按 `DISTRIBUTE BY REPLICATION` 复制到各节点，CN/DN 上均可调用

## 前置依赖

| 依赖 | 说明 |
|------|------|
| pgsql-http 扩展 | `opentenbase_ai` 声明 `requires = 'http'`，必须先安装（见 `contrib/pgsql-http/`，其依赖 libcurl 开发头） |
| 网络出口 | 数据库进程（所有可能执行相关查询的节点，包括 CN 与 DN）需要能够访问模型服务端点 |
| 模型服务 | 任一 OpenAI 协议兼容端点及对应的 API Key |

## 编译安装

### 随源码树编译（推荐）

在 OpenTenBase 源码树 configure 之后：

```bash
make -C contrib/pgsql-http
make -C contrib/pgsql-http install
make -C contrib/opentenbase_ai
make -C contrib/opentenbase_ai install
```

### 使用 PGXS 独立编译

```bash
cd contrib/opentenbase_ai
USE_PGXS=1 make
USE_PGXS=1 make install
```

### 创建扩展

```sql
-- 先装依赖扩展，再装本扩展
CREATE EXTENSION http;
CREATE EXTENSION opentenbase_ai;
```

## 快速开始

三步即可在 SQL 中调用大模型：

```sql
-- 1. 注册一个 OpenAI 兼容的补全模型
SELECT ai.add_completion_model(
    'deepseek-chat',
    'https://api.deepseek.com/v1/chat/completions',
    '{"model": "deepseek-chat", "temperature": 0.7}'::jsonb,
    'sk-your-api-token',        -- 鉴权 token，自动写入 Authorization: Bearer 头
    'deepseek'
);

-- 2. 设置默认补全模型（GUC，见下文"GUC 参数"小节的加载要求）
SET ai.completion_model = 'deepseek-chat';

-- 3. 直接调用
SELECT ai.generate_text('用一句话介绍 OpenTenBase');
```

## 模型注册与管理

### 模型登记表

所有模型登记在 `public.ai_model_list`（复制表）中，核心字段：

| 字段 | 说明 |
|------|------|
| `model_name` | 模型名（主键），调用时的句柄 |
| `request_type` / `uri` / `content_type` | HTTP 方法、端点 URL、请求体类型 |
| `request_header` | `http_header[]` 请求头数组，通常存放 `Authorization` |
| `default_args` | JSONB 默认请求参数，与调用参数顶层浅合并（同名键整体替换） |
| `json_path` | 结果提取模板：一条包含 `%s`（响应体）的 SQL 语句 |

日常查看请使用 `ai.models` 视图，它不暴露 `request_header`：

```sql
SELECT * FROM ai.models;
```

### 注册函数

| 函数 | 适用端点 | 结果提取 |
|------|----------|----------|
| `ai.add_completion_model(name, uri, default_args, token, provider)` | OpenAI Chat Completions 协议 | `choices[0].message.content` |
| `ai.add_embedding_model(name, uri, default_args, token, provider)` | OpenAI Embeddings 协议 | `data[0].embedding` |
| `ai.add_image_model(name, uri, default_args, token, provider)` | OpenAI Vision（chat 协议）视觉模型 | `choices[0].message.content` |
| `ai.add_model(name, header[], uri, default_args, provider, method, content_type, json_path)` | 任意 HTTP 端点 | 自定义 `json_path` |

注意：`add_image_model` 的提取路径与补全模型相同，仅适用于遵循 chat 协议的视觉理解模型（如 gpt-4o、Qwen-VL），不适用于纯文生图端点（如 DALL-E，其响应结构不同）。

### 管理函数

```sql
SELECT ai.update_model('deepseek-chat', 'default_args', '{"model": "deepseek-chat", "temperature": 0.3}');
SELECT ai.delete_model('deepseek-chat');
```

## 函数参考

### 生成类

| 函数 | 返回类型 | 说明 |
|------|----------|------|
| `ai.generate_text(prompt, model_name := NULL, config := '{}')` | `text` | 文本补全 |
| `ai.generate_int(prompt, ...)` | `integer` | 类型化生成，结果清理为整数 |
| `ai.generate_double(prompt, ...)` | `double precision` | 类型化生成，结果清理为数值 |
| `ai.generate_bool(prompt, ...)` | `boolean` | 类型化生成，结果归一化为 true/false |
| `ai.generate(prompt, dummy, model_name, config)` | `anyelement` | 通用入口，按 `dummy` 参数类型分发到上述行为 |

`config` 为 JSONB，会与 `default_args` 合并后作为请求体，可用于临时覆盖 `temperature`、`max_tokens` 等参数。

### 场景函数

| 函数 | 说明 |
|------|------|
| `ai.translate(text, target_language)` | 翻译到目标语言 |
| `ai.summarize(text)` | 摘要 |
| `ai.sentiment(text)` | 情感分析，返回 positive / negative / neutral / mixed |
| `ai.extract_answer(context, question)` | 仅依据 context 回答问题 |

### 嵌入与图像

| 函数 | 说明 |
|------|------|
| `ai.embedding(input)` | 返回嵌入向量的文本形式（JSON 数组，如 `'[0.012,-0.034,...]'`） |
| `ai.image(prompt, image_url)` | 图像理解，图像以 URL 提供 |
| `ai.image(prompt, image bytea, mime_type := 'image/jpeg')` | 图像理解，图像以二进制提供，内部转 base64 data URL；`mime_type` 支持 jpeg / png / gif / webp |

### 底层调用

| 函数 | 说明 |
|------|------|
| `ai.raw_invoke_model(model, user_args)` | 返回原始 `http_response`（含 status / headers / content），供调试 |
| `ai.invoke_model(model, user_args)` | 发起请求并按 `json_path` 提取结果；HTTP 状态码非 200 时抛异常 |

## GUC 参数

`ai.c` 定义了三个用户级参数，用于省略 `model_name` 时确定默认模型：

| 参数 | 作用 |
|------|------|
| `ai.completion_model` | `ai.generate*` / 场景函数的默认补全模型 |
| `ai.embedding_model` | `ai.embedding` 的默认嵌入模型 |
| `ai.image_model` | `ai.image` 的默认视觉模型 |

加载要求：本扩展的 SQL 脚本中没有引用 C 模块的函数，`CREATE EXTENSION` 并不会加载 `ai.so`。要使用上述 GUC，需要显式加载模块：

```sql
LOAD 'ai';
-- 或在 postgresql.conf 中配置后重启：
-- shared_preload_libraries = 'opentenbase_ai'
SET ai.completion_model = 'deepseek-chat';
```

需要说明的是：即使模块未加载，`SET ai.completion_model = ...` 也**不会**报 `unrecognized configuration parameter` 错误——本内核对带前缀（含 `.`）的自定义 GUC 名会先创建占位符变量（placeholder），`current_setting` 同样能读到该值，因此各高层函数在此场景下仍可取到默认模型。但占位符不具备正式定义的参数语义（`SHOW`/`pg_settings` 展示、重启后从配置文件恢复等），生产环境仍建议按上文通过 `LOAD` 或 `shared_preload_libraries` 正式加载模块。若完全不使用 GUC，也可以始终显式传入 `model_name` 参数。

## 使用示例

```sql
-- 类型化生成：直接得到 integer
SELECT ai.generate_int('估算 "hello world" 的字符数');

-- 批量翻译
SELECT id, ai.translate(content, '英文') FROM articles;

-- 情感打标
SELECT ai.sentiment('这个数据库的分布式事务做得真不错');

-- 从工单内容中做问答抽取
SELECT ai.extract_answer(ticket_body, '客户的 SLA 要求是什么？');

-- 嵌入 + pgvector 相似度检索（需另装 contrib/pgvector）
CREATE EXTENSION vector;
SELECT embedding::vector FROM docs ORDER BY embedding::vector <=> ai.embedding('集群容灾')::vector LIMIT 5;

-- 图像理解：URL 或本地文件
SELECT ai.image('描述这张图片', 'https://example.com/photo.jpg');
SELECT ai.image('描述这张图片', pg_read_binary_file('/tmp/photo.png'), 'image/png');
```

## 回归测试

```bash
make -C contrib/opentenbase_ai installcheck   # 需已启动并安装扩展的实例
```

回归测试使用 `httpbin.org` 作为 mock 端点，只验证模型注册/管理函数与表结构，不要求真实的 LLM 服务。

## 注意事项

1. **API Key 明文存储**：`token` 以明文写入 `public.ai_model_list.request_header`，而该表默认 `GRANT SELECT ... TO PUBLIC`，所有用户可读。生产环境建议：通过 revoke 收紧读权限，或让 `uri` 指向内部代理网关、由网关持有真实 Key。
2. **网络出口**：HTTP 请求由数据库进程直接发起，请确保每个可能执行相关查询的节点（CN 与 DN）到模型端点的网络与防火墙放行，并评估外部调用对查询延迟的影响。
3. **错误传播**：HTTP 非 200、模型未注册、`json_path` 提取失败均以异常抛出，调用方需在事务/应用层处理。
4. **自定义端点**：使用非 OpenAI 协议的服务时，用 `ai.add_model` 自行书写 `json_path`，模板中 `%s` 位置为原始响应体文本。

## 实现说明（面向内核开发者）

- **调用链**：`ai.*` 高层函数 → 构造 `messages`/请求体 JSONB → `ai.invoke_model` → `default_args || user_args` 顶层浅合并 → 经 pgsql-http 的 `http()` 发起请求 → `EXECUTE format(json_path, response_content)` 提取 → 高层函数做类型清理后返回。
- **`json_path` 是 SQL 模板**而非 JSON Path 表达式：登记时写入一条含 `%s` 的 `SELECT ...` 语句，运行时以响应体文本替换后 `EXECUTE`，因此提取逻辑可以是任意 SQL 表达式。
- **GUC 加载语义**：三个 GUC 在 `ai.c` 的 `_PG_init` 中以 `PGC_USERSET` 定义；`ai.so` 仅在 `LOAD` 或 `shared_preload_libraries` 时加载（见"GUC 参数"小节）。高层函数通过 `current_setting('...', true)` 读取，未设置时以 "model name is not set" 异常提示。
- **分发策略**：`ai_model_list` 为 `DISTRIBUTE BY REPLICATION` 复制表，保证所有节点的模型登记一致；实际 HTTP 调用发生在执行该查询的节点上。

## 许可证

本项目遵循 OpenTenBase 主仓的 [BSD-3-Clause](../../LICENSE.txt) 许可证（Copyright (C) 2025 OpenTenBase Authors）。
