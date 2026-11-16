-- =============================================================
-- 03_rag_pgvector.sql — 自由延展：embedding 写入 + 相似度检索（RAG 最小案例）
-- 依赖：contrib/pgvector（vector 类型）+ opentenbase_ai v1.0
-- 状态：函数名/类型转换已核对；真实执行【待实测】（需 pgvector 与有效 embedding 模型）
-- =============================================================

CREATE EXTENSION IF NOT EXISTS http;
CREATE EXTENSION IF NOT EXISTS vector;            -- contrib/pgvector
CREATE EXTENSION IF NOT EXISTS opentenbase_ai;

-- ---------- 1. 注册向量化模型（OpenAI Embeddings 兼容协议） ----------
SELECT ai.add_embedding_model(
    'my-embedding',
    'https://api.deepseek.com/v1/embeddings',               -- 或 text-embedding-ada-002 等
    '{"model": "deepseek-embedding", "encoding_format": "float"}'::jsonb,
    'sk-your-api-token',
    'deepseek'
);
SET ai.embedding_model = 'my-embedding';
-- 内置 json_path（add_embedding_model 写死）：
--   SELECT %L::jsonb->'data'->0->'embedding'::TEXT
-- 即返回 JSON 数组文本 '[0.012, -0.233, ...]'，可直接 ::vector

-- ---------- 2. 建表并写入向量 ----------
CREATE TABLE IF NOT EXISTS kb_doc (
    id        BIGSERIAL PRIMARY KEY,
    content   TEXT NOT NULL,
    embedding VECTOR(1024)                          -- 维度按所选模型调整（ada-002=1536, bge=1024 等）
);

-- 写入时实时向量化（文本少时可接受；生产建议批量预计算 + 异步刷新）
INSERT INTO kb_doc(content, embedding)
SELECT v.content, ai.embedding(v.content)::vector
FROM (VALUES
    ('OpenTenBase 是腾讯开源的分布式数据库，基于 PostgreSQL'),
    ('猫娘 neko 喜欢晒太阳和看恋爱小说'),
    ('分布式数据库通过分片与复制表实现水平扩展')
) AS v(content);

-- 建议：为向量列建 ANN 索引。
-- ⚠️ 规模纪律（本组已复现上游 bug）：PG18 + pgvector 0.8.6 在 2 万行以上建 HNSW 索引段错误，
--    演示数据量保持 ≤ 1 万行；VM pgsql_tmp 小，避免大规模建索引/排序。
-- CREATE INDEX idx_kb_doc_embedding ON kb_doc USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- ---------- 2.1 叠加本组增强 pgvector：检索参数推荐（feat/pgvector-pg18-enhanced） ----------
-- 若已建索引，用推荐函数按目标召回率给出检索参数（本组 v0.8.6 新增，函数已核实）：
-- SELECT * FROM ivfflat_recommend_probes('kb_doc_embedding_idx'::regclass, 0.95);
-- SELECT * FROM hnsw_recommend_ef_search('kb_doc_embedding_idx'::regclass, 10, 0.95);
-- 按推荐值设置会话参数后再查询：
-- SET hnsw.ef_search = 100;  -- 以推荐输出为准

-- ---------- 3. 相似度检索：一条 SQL 完成 RAG 检索 ----------
-- <=> 为余弦距离（越小越相似）；query 同样走 ai.embedding
SELECT id, content,
       embedding <=> ai.embedding('开源分布式数据库有哪些')::vector AS distance
FROM kb_doc
ORDER BY embedding <=> ai.embedding('开源分布式数据库有哪些')::vector
LIMIT 3;

-- ---------- 4.（可选）检索 + 生成 = 完整 RAG ----------
-- SELECT ai.extract_answer(
--     (SELECT string_agg(content, E'\n' ORDER BY distance) FROM (
--         SELECT content, embedding <=> ai.embedding('问题')::vector AS distance
--         FROM kb_doc ORDER BY 2 LIMIT 3) t),
--     'OpenTenBase 是什么？'
-- ) AS answer;

-- ---------- 清理 ----------
-- DROP TABLE IF EXISTS kb_doc;
-- SELECT ai.delete_model('my-embedding');
-- DROP EXTENSION IF EXISTS opentenbase_ai;
-- DROP EXTENSION IF EXISTS vector;
-- DROP EXTENSION IF EXISTS http;
