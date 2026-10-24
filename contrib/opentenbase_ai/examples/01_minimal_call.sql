-- =============================================================
-- 01_minimal_call.sql — opentenbase_ai 最小调用示例
-- 场景：摘要生成 + 文本分类（布尔情感）
-- 基线：OpenTenBase master, contrib/opentenbase_ai v1.0（2026-09-19 源码核对）
-- 状态：语句与回归测试/扩展 SQL 一致【已核对】；真实执行【待实测】
-- =============================================================

-- ---------- 0. 前置：扩展与依赖 ----------
CREATE EXTENSION IF NOT EXISTS http;             -- pgsql-http，opentenbase_ai requires 'http'
CREATE EXTENSION IF NOT EXISTS opentenbase_ai;

-- ---------- 1. 模型注册 ----------
-- 以 DeepSeek 为例；任何 OpenAI Chat Completions 兼容端点均可（OpenAI / Qwen / vLLM / Ollama 网关）
SELECT ai.add_completion_model(
    'my-chat',                                              -- model_name（主键，调用句柄）
    'https://api.deepseek.com/v1/chat/completions',          -- uri
    '{"model": "deepseek-chat", "temperature": 0.3}'::jsonb, -- default_args：默认请求体参数
    'sk-your-api-token',                                     -- token：自动拼成 Authorization: Bearer 头
    'deepseek'                                               -- model_provider（仅标注，可 NULL）
);

-- 注册结果检查（视图不含鉴权头，可放心查询）
SELECT * FROM ai.models WHERE model_name = 'my-chat';

-- ---------- 2. 设置默认模型（可选） ----------
-- 来源：ai.c 中 DefineCustomStringVariable("ai.completion_model", ..., PGC_USERSET)
SET ai.completion_model = 'my-chat';

-- ---------- 3. 最小调用：一条 SQL 完成任务 ----------

-- 3a. 摘要生成（ai.summarize 内部 prompt：'Summarize the following text concisely: ' || 文本）
SELECT ai.summarize(
    'OpenTenBase 是腾讯开源的分布式数据库，基于 PostgreSQL 内核，支持 shared-nothing 架构，
     具备高性能、高可用、线性扩展等特性，广泛应用于金融级场景。'
) AS summary;

-- 3b. 文本分类：布尔型（好评/差评）。generate_bool 用 system prompt 强制只返回 true/false
SELECT ai.generate_bool('这条评论是好评吗：东西不错，还会回购') AS is_positive;
SELECT ai.generate_bool('这条评论是好评吗：物流太慢，差评')     AS is_positive;

-- 3c. 文本分类：情感词（positive/negative/neutral/mixed）
SELECT ai.sentiment('物流太慢了，很失望') AS sentiment;

-- 3d. 批量加工表中整列文本（AI 扩展的核心价值：数据不出库）
-- CREATE TABLE feedback(id int, content text, summary text, is_positive boolean);
-- UPDATE feedback
-- SET summary     = ai.summarize(content),
--     is_positive = ai.generate_bool('好评返回true：' || content);

-- ---------- 4. 错误排查：查看原始 HTTP 响应（不提取 json_path） ----------
-- 返回 http_response 复合类型（status / content / headers），用于定位 http_code <> 200 等问题
SELECT ai.raw_invoke_model('my-chat', '{"messages":[{"role":"user","content":"ping"}]}'::jsonb);

-- ---------- 5. 清理 ----------
-- SELECT ai.delete_model('my-chat');
-- DROP EXTENSION IF EXISTS opentenbase_ai;
