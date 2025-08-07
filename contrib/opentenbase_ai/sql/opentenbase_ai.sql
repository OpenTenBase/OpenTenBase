-- Test setup
CREATE EXTENSION IF NOT EXISTS http;
CREATE EXTENSION IF NOT EXISTS opentenbase_ai;

-- Test 1: Check if extension is properly loaded
SELECT count(*) > 0 AS extension_loaded FROM pg_extension WHERE extname = 'opentenbase_ai';

-- Test 2: Check if schema and tables are created correctly
SELECT count(*) > 0 AS model_list_exists 
FROM pg_tables 
WHERE schemaname = 'public' AND tablename = 'ai_model_list';

-- Test 3: Add a test model for a mock API using generic add_model function
SELECT ai.add_model(
    'test_model',                                   -- model_name
    ARRAY[                                          -- request_header
        ROW('Content-Type', 'application/json')::http_header,
        ROW('Authorization', 'Bearer test-token')::http_header
    ],
    'https://httpbin.org/post',                     -- uri (using httpbin for testing)
    '{"model": "test-model", "temperature": 0.7}'::jsonb, -- default_args
    'test-provider',                                -- model_provider
    'POST',                                         -- request_type
    'application/json',                             -- content_type
    'SELECT json_extract_path_text(''%s''::json, ''json'', ''prompt'')' -- json_path
);

-- Test 4: List models 
SELECT * FROM ai.models 
WHERE model_name = 'test_model';

-- Test 5: Update model
SELECT ai.update_model('test_model', 'content_type', 'application/json; charset=utf-8');

-- Test 6: Verify update
SELECT content_type 
FROM public.ai_model_list 
WHERE model_name = 'test_model';

-- Test 7: Test add_completion_model function
SELECT ai.add_completion_model(
    'test_completion_model',
    'https://httpbin.org/post',
    '{"model": "gpt-4", "temperature": 0.5, "max_tokens": 100}'::jsonb,
    'fake-token-123',
    'openai'
);

-- Test 8: Verify completion model was added
SELECT * FROM ai_model_list
WHERE model_name = 'test_completion_model';

-- Test 9: Test add_embedding_model function
SELECT ai.add_embedding_model(
    'test_embedding_model',
    'https://httpbin.org/post',
    '{"model": "text-embedding-ada-002", "encoding_format": "float"}'::jsonb,
    'fake-token-456',
    'openai'
);

-- Test 10: Verify embedding model was added with correct json_path
SELECT * FROM ai_model_list
WHERE model_name = 'test_embedding_model';

-- Test 11: Test add_image_model function
SELECT ai.add_image_model(
    'test_image_model',
    'https://httpbin.org/post',
    '{"model": "dall-e-3", "n": 1, "size": "1024x1024"}'::jsonb,
    'fake-token-789',
    'openai'
);

-- Test 12: Verify image model was added
SELECT * FROM ai_model_list
WHERE model_name = 'test_image_model';

-- Test 13: Test updating model with JSON value
SELECT ai.update_model('test_model', 'default_args', '{"model": "test-model-v2", "temperature": 0.3, "top_p": 0.9}');

-- Test 14: Verify JSON update
SELECT default_args
FROM public.ai_model_list
WHERE model_name = 'test_model';

-- Test 15: Test deleting multiple models at once
SELECT ai.delete_model('test_completion_model');
SELECT ai.delete_model('test_embedding_model');
SELECT ai.delete_model('test_image_model');

-- Test 16: Verify deletions
SELECT count(*) AS remaining_models
FROM ai_model_list
WHERE model_name IN ('test_completion_model', 'test_embedding_model', 'test_image_model');

-- Test 17: Delete final test model
SELECT ai.delete_model('test_model');

-- Test 18: Verify all models deleted
SELECT count(*) = 0 AS all_models_deleted
FROM ai_model_list;

DROP EXTENSION IF EXISTS opentenbase_ai;
DROP EXTENSION IF EXISTS http;
