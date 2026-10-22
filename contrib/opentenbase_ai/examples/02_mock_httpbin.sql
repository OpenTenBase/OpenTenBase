-- =============================================================
-- 02_mock_httpbin.sql — 基于 httpbin.org 的 mock 验证
-- 与上游回归测试 contrib/opentenbase_ai/sql/opentenbase_ai.sql 同源
-- 状态：语句已核对；真实执行【待实测】（需数据库可访问外网 httpbin.org）
--
-- ⚠️ mock 验证范围声明：
--   ✅ 验证：请求构造（method/header/body 回显）、default_args||user_args 合并、
--             json_path 模板提取、错误路径（模型不存在 / http_code<>200）
--   ❌ 不验证：真实模型推理质量、json_path 与真实响应结构的匹配、token 被真实服务接受
--   mock 通过 ≠ 真实调用通过，接真实模型前须用真实端点重跑 01_minimal_call.sql
-- =============================================================

CREATE EXTENSION IF NOT EXISTS http;
CREATE EXTENSION IF NOT EXISTS opentenbase_ai;

-- 通用注册：8 参 add_model。json_path 用 %s 占位响应体（invoke_model 内 format(json_path, content)）
-- httpbin /post 回显结构：{"json": {...body...}, "headers": {...}, ...}
SELECT ai.add_model(
    'mock_model',
    ARRAY[ROW('Authorization', 'Bearer test-token')::http_header],
    'https://httpbin.org/post',
    '{"model": "test-model", "temperature": 0.7}'::jsonb,
    'mock-provider',
    'POST',
    'application/json',
    'SELECT json_extract_path_text(''%s''::json, ''json'', ''model'')'   -- 从回显体里提取我们发出去的 model 字段
);

-- 用 raw 调用看完整回显：可核对请求头/请求体构造是否正确
SELECT (ai.raw_invoke_model('mock_model', '{"temperature": 0.9}'::jsonb)).*;

-- 用 invoke 调用：验证 json_path 提取链路（应返回 'test-model'，user_args 覆盖了 default 的 0.7）
SELECT ai.invoke_model('mock_model', '{"temperature": 0.9}'::jsonb) AS extracted_model;

-- 错误路径 1：模型不存在
-- SELECT ai.invoke_model('no_such_model', '{}'::jsonb);
--   预期：ERROR: Model no_such_model not found

-- 错误路径 2：注册时故意写错 uri 制造非 200
-- SELECT ai.add_model('bad_uri_model', '{}'::http_header[], 'https://httpbin.org/status/500',
--                     '{}'::jsonb, 'mock', 'POST', 'application/json', 'SELECT %L');
-- SELECT ai.invoke_model('bad_uri_model', '{}'::jsonb);
--   预期：ERROR: Failure in http-request. http_code: 500, content: ...

-- 清理
SELECT ai.delete_model('mock_model');
-- SELECT ai.delete_model('bad_uri_model');
DROP EXTENSION IF EXISTS opentenbase_ai;
DROP EXTENSION IF EXISTS http;
