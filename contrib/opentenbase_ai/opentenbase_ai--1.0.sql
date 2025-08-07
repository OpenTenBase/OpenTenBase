/* contrib/opentenbase_ai/opentenbase_ai--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION opentenbase_ai" to load this file. \quit

CREATE SCHEMA ai;
GRANT USAGE ON SCHEMA ai TO PUBLIC;

CREATE TABLE public.ai_model_list (
    model_name TEXT PRIMARY KEY,
    model_provider TEXT,
    request_type TEXT NOT NULL,
    request_header http_header[],
    uri TEXT NOT NULL,
    content_type TEXT NOT NULL,
    default_args JSONB NOT NULL,
    json_path TEXT NOT NULL
) DISTRIBUTE BY REPLICATION;

GRANT SELECT ON public.ai_model_list TO PUBLIC;

CREATE OR REPLACE VIEW ai.models AS
SELECT model_name, model_provider, uri, default_args
FROM public.ai_model_list;

GRANT SELECT ON ai.models TO PUBLIC;

CREATE OR REPLACE FUNCTION ai.raw_invoke_model(model_name_v TEXT, user_args JSONB)
RETURNS http_response AS $$
DECLARE
	uri TEXT;
    model_name TEXT;
	request_header TEXT;
	content_type TEXT;
	request_type TEXT;
	default_args JSONB;
	request_sql TEXT;
	exec_args JSONB;
	result http_response;
BEGIN
	SELECT model_name, request_type, request_header, uri, content_type, default_args
    FROM public.ai_model_list 
    WHERE model_name = model_name_v 
    INTO model_name, request_type, request_header, uri, content_type, default_args;

    IF model_name IS NULL THEN
        RAISE EXCEPTION 'Model % not found', model_name_v;
    END IF;

	exec_args := default_args || user_args;
    RAISE DEBUG 'raw_invoke_model: exec_args: %', exec_args;
	
	request_sql := format('SELECT * FROM http((%L, %L, %L, %L, %L)::http_request);',
								request_type, uri, request_header, content_type, exec_args::TEXT);

    RAISE DEBUG 'raw_invoke_model: request_sql: %', request_sql;
	EXECUTE request_sql INTO result;
	RETURN result;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ai.invoke_model(model_name_v TEXT, user_args JSONB)
RETURNS TEXT AS $$
DECLARE
    uri TEXT;
    model_name TEXT;
    request_header http_header[];
    content_type TEXT;
    request_type TEXT;
    default_args JSONB;
    json_path_v TEXT;
    exec_args JSONB;
    request_sql TEXT;
    http_result http_response;
    http_code INTEGER;
    response_content TEXT;
    result TEXT;
BEGIN
    -- get model info
    SELECT m.model_name, m.request_type, m.request_header, m.uri, m.content_type, m.default_args, m.json_path
    FROM public.ai_model_list m
    WHERE m.model_name = model_name_v
    INTO model_name, request_type, request_header, uri, content_type, default_args, json_path_v;

    -- check model exists
    IF model_name IS NULL THEN
        RAISE EXCEPTION 'Model % not found', model_name_v;
    END IF;

    -- merge args
    exec_args := default_args || user_args;
    RAISE DEBUG 'invoke_model: exec_args: %', exec_args;
    
    -- build and execute http request
    request_sql := format('SELECT * FROM http((%L, %L, %L, %L, %L)::http_request);',
                        request_type, uri, request_header, content_type, exec_args::TEXT);
    
    RAISE DEBUG 'invoke_model: request_sql: %', request_sql;
    EXECUTE request_sql INTO http_result;
    
    -- get http response status and content
    http_code := http_result.status;
    response_content := http_result.content;
    
    -- check http status code
    IF http_code <> 200 THEN
        RAISE EXCEPTION 'Failure in http-request. http_code: %, content: %', http_code, response_content;
    END IF;

    RAISE DEBUG 'invoke_model: response_content: %', response_content;

    -- use json path to extract result
    IF json_path_v IS NULL THEN
        RAISE EXCEPTION 'Invalid json path for model %', model_name_v;
    END IF;

    EXECUTE format(json_path_v, response_content) INTO result;
    RAISE DEBUG 'invoke_model: result: %', result;
    
    RETURN result;
END;
$$ LANGUAGE plpgsql;


-- Internal/advanced function for adding any model
CREATE OR REPLACE FUNCTION ai.add_model(
    model_name text,
    request_header http_header[],
    uri text, 
    default_args jsonb, 
    model_provider text,
    request_type text, 
    content_type text,  
    json_path text 
) RETURNS boolean AS $$
DECLARE
    rows_affected integer;
BEGIN
    INSERT INTO public.ai_model_list VALUES(
        model_name, model_provider, request_type, request_header, uri, 
        content_type, default_args, json_path
    );
    
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    RETURN rows_affected > 0;
END;
$$ LANGUAGE plpgsql;

-- OpenAI Chat Completions compatible API 
CREATE OR REPLACE FUNCTION ai.add_completion_model(
    model_name text,
    uri text, 
    default_args jsonb, 
    token text = NULL,
    model_provider text = NULL
) RETURNS boolean AS $$
DECLARE
    headers http_header[];
BEGIN
    -- Create authorization header from token if provided
    IF token IS NOT NULL THEN
        headers := ARRAY[http_header('Authorization', 'Bearer ' || token)];
    ELSE
        headers := ARRAY[]::http_header[];
    END IF;

    -- Use standard path for completion models
    RETURN ai.add_model(
        model_name, 
        headers, 
        uri, 
        default_args, 
        model_provider, 
        'POST', 
        'application/json', 
        'SELECT %L::jsonb->''choices''->0->''message''->>''content'''
    );
END;
$$ LANGUAGE plpgsql;

-- OpenAI Embeddings compatible API
CREATE OR REPLACE FUNCTION ai.add_embedding_model(
    model_name text,
    uri text, 
    default_args jsonb, 
    token text = NULL,
    model_provider text = NULL
) RETURNS boolean AS $$
DECLARE
    headers http_header[];
BEGIN
    -- Create authorization header from token if provided
    IF token IS NOT NULL THEN
        headers := ARRAY[http_header('Authorization', 'Bearer ' || token)];
    ELSE
        headers := ARRAY[]::http_header[];
    END IF;

    -- Use standard path for embedding models
    RETURN ai.add_model(
        model_name, 
        headers, 
        uri, 
        default_args, 
        model_provider, 
        'POST', 
        'application/json', 
        'SELECT %L::jsonb->''data''->0->''embedding''::TEXT'
    );
END;
$$ LANGUAGE plpgsql;

-- OpenAI Vision/image compatible API
CREATE OR REPLACE FUNCTION ai.add_image_model(
    model_name text,
    uri text, 
    default_args jsonb, 
    token text = NULL,
    model_provider text = NULL
) RETURNS boolean AS $$
DECLARE
    headers http_header[];
BEGIN
    -- Create authorization header from token if provided
    IF token IS NOT NULL THEN
        headers := ARRAY[http_header('Authorization', 'Bearer ' || token)];
    ELSE
        headers := ARRAY[]::http_header[];
    END IF;

    -- Use standard path for image models
    RETURN ai.add_model(
        model_name, 
        headers, 
        uri, 
        default_args, 
        model_provider, 
        'POST', 
        'application/json', 
        'SELECT %L::jsonb->''choices''->0->''message''->>''content'''
    );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ai.delete_model(model_name text) 
RETURNS boolean AS $$
DECLARE
    rows_affected integer;
BEGIN
    EXECUTE format('DELETE FROM public.ai_model_list WHERE model_name = %L', model_name);
    
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    RETURN rows_affected > 0;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ai.update_model(model_name text, config text, value text)
RETURNS boolean AS $$
DECLARE
    rows_affected integer;
    sql_command text;
    column_type text;
BEGIN
    EXECUTE format('SELECT pg_typeof(ai_model_list.%I)::text FROM public.ai_model_list LIMIT 1', config) INTO column_type;

	EXECUTE format('UPDATE public.ai_model_list SET %I = %L::%s WHERE model_name = %L', config, value, column_type, model_name);
	
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    RETURN rows_affected > 0;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ai.generate(
    prompt text,
    dummy anyelement = 'text'::anyelement,
    model_name text = NULL,
    config jsonb = '{}'::jsonb
) RETURNS anyelement AS $$
DECLARE
    model_name_v text;
    text_result text;
    type_name text;
BEGIN
    -- Get model name from parameter or configuration
    IF model_name IS NULL THEN
        model_name_v := current_setting('ai.completion_model', true);
    ELSE
        model_name_v := model_name;
    END IF;

    IF model_name_v IS NULL THEN
        RAISE EXCEPTION 'Completion model name is not set';
    END IF;
    
    -- Get the type name
    type_name := pg_catalog.format_type(pg_typeof(dummy), NULL);
    
    -- Handle different types with specialized prompts
    CASE 
        -- Integer types
        WHEN type_name IN ('integer', 'bigint') THEN
            text_result := ai.invoke_model(model_name_v, jsonb_build_object(
                'messages', jsonb_build_array(
                    jsonb_build_object(
                        'role', 'system', 
                        'content', 'You are a helpful assistant that responds only with a single integer number. Do not include any explanations, units, or text in your response. Only respond with the integer value.'
                    ),
                    jsonb_build_object('role', 'user', 'content', prompt)
                )
            ) || config);
            
            -- Clean up result - remove non-numeric characters
            text_result := regexp_replace(text_result, '[^0-9\-]', '', 'g');
            
            -- Handle empty result
            IF text_result = '' OR text_result IS NULL THEN
                text_result := '0';
            END IF;
            
        -- Floating point types
        WHEN type_name IN ('double precision', 'real') THEN
            text_result := ai.invoke_model(model_name_v, jsonb_build_object(
                'messages', jsonb_build_array(
                    jsonb_build_object(
                        'role', 'system', 
                        'content', 'You are a helpful assistant that responds only with a single number (decimal or integer). Do not include any explanations, units, or text in your response. Only respond with the numeric value.'
                    ),
                    jsonb_build_object('role', 'user', 'content', prompt)
                )
            ) || config);
            
            -- Clean up result - keep only valid numeric characters
            text_result := regexp_replace(text_result, '[^0-9\.\-\+eE]', '', 'g');
            
            -- Handle empty result
            IF text_result = '' OR text_result IS NULL THEN
                text_result := '0';
            END IF;
            
        -- Boolean type
        WHEN type_name = 'boolean' THEN
            text_result := ai.invoke_model(model_name_v, jsonb_build_object(
                'messages', jsonb_build_array(
                    jsonb_build_object(
                        'role', 'system', 
                        'content', 'You are a helpful assistant that responds only with "true" or "false". Do not include any explanations or additional text in your response. Only respond with the boolean value.'
                    ),
                    jsonb_build_object('role', 'user', 'content', prompt)
                )
            ) || config);
            
            -- Normalize boolean values
            text_result := lower(trim(text_result));
            IF text_result IN ('true', 'yes', '1', 't', 'y') THEN
                text_result := 'true';
            ELSE
                text_result := 'false';
            END IF;
            
        -- Text types (default case)
        WHEN type_name = 'text' THEN
            text_result := ai.invoke_model(model_name_v, jsonb_build_object(
                'messages', jsonb_build_array(
                    jsonb_build_object(
                        'role', 'system', 
                        'content', 'You are a helpful assistant that responds with clear, concise text.'
                    ),
                    jsonb_build_object('role', 'user', 'content', prompt)
                )
            ) || config);
        ELSE
            RAISE EXCEPTION 'Unsupported type: %', type_name;
    END CASE;
    
    -- Convert the text result to the requested type
    return text_result;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ai.generate_text(prompt text, model_name text = NULL, config jsonb = '{}'::jsonb)
RETURNS TEXT AS $$
BEGIN
    RETURN ai.generate(prompt, NULL::text, model_name, config);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ai.generate_int(
    prompt text, 
    model_name text = NULL,
    config jsonb = '{}'::jsonb
)
RETURNS INTEGER AS $$
BEGIN
    RETURN ai.generate(prompt, NULL::integer, model_name, config)::integer;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ai.generate_double(
    prompt text, 
    model_name text = NULL,
    config jsonb = '{}'::jsonb
)
RETURNS DOUBLE PRECISION AS $$
BEGIN
    RETURN ai.generate(prompt, NULL::double precision, model_name, config)::double precision;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ai.generate_bool(
    prompt text, 
    model_name text = NULL,
    config jsonb = '{}'::jsonb
)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN ai.generate(prompt, NULL::boolean, model_name, config)::boolean;
END;
$$ LANGUAGE plpgsql;


-- Translation function
CREATE OR REPLACE FUNCTION ai.translate(
    text_content text,
    target_language text,
    model_name text = NULL,
    config jsonb = '{}'::jsonb
) RETURNS text AS $$
DECLARE
    prompt text;
BEGIN
    prompt := format('Translate the following text to %s: %s', target_language, text_content);
    RETURN ai.generate_text(prompt, model_name, config);
END;
$$ LANGUAGE plpgsql;

-- Sentiment analysis function
CREATE OR REPLACE FUNCTION ai.sentiment(
    text_content text,
    model_name text = NULL,
    config jsonb = '{}'::jsonb
) RETURNS text AS $$
DECLARE
    prompt text;
BEGIN
    prompt := 'Analyze the sentiment of the following text and respond with only one word (positive, negative, neutral or mixed): ' || text_content;
    RETURN ai.generate_text(prompt, model_name, config);
END;
$$ LANGUAGE plpgsql;

-- Answer extraction function
CREATE OR REPLACE FUNCTION ai.extract_answer(
    context text,
    question text,
    model_name text = NULL,
    config jsonb = '{}'::jsonb
) RETURNS text AS $$
DECLARE
    prompt text;
BEGIN
    prompt := format('Based only on the following context, answer the question concisely:\n\nContext: %s\n\nQuestion: %s', context, question);
    RETURN ai.generate_text(prompt, model_name, config);
END;
$$ LANGUAGE plpgsql;

-- Text summarization function
CREATE OR REPLACE FUNCTION ai.summarize(
    text_content text,
    model_name text = NULL,
    config jsonb = '{}'::jsonb
) RETURNS text AS $$
DECLARE
    prompt text;
BEGIN
    prompt := 'Summarize the following text concisely: ' || text_content;
    RETURN ai.generate_text(prompt, model_name, config);
END;
$$ LANGUAGE plpgsql;

-- Text embedding function
CREATE OR REPLACE FUNCTION ai.embedding(
    input text,
    model_name text = NULL,
    config jsonb = '{}'::jsonb
) RETURNS text AS $$
DECLARE
    model_name_v text;
    embedding_result text;
BEGIN
    -- Return NULL for NULL input
    IF input IS NULL THEN
        RETURN NULL;
    END IF;

    -- Get model name from parameter or configuration
    IF model_name IS NULL THEN
        model_name_v := current_setting('ai.embedding_model', true);
    ELSE
        model_name_v := model_name;
    END IF;

    IF model_name_v IS NULL THEN
        RAISE EXCEPTION 'Embedding model name is not set';
    END IF;
    
    -- Use the existing invoke_model function which already handles error checking
    embedding_result := ai.invoke_model(model_name_v, jsonb_build_object('input', input) || config);
    
    -- Return the embedding result directly
    RETURN embedding_result;
END;
$$ LANGUAGE plpgsql;

-- Image analysis function with URL
CREATE OR REPLACE FUNCTION ai.image(
    prompt text,
    image_url text,
    model_name text = NULL,
    config jsonb = '{}'::jsonb
) RETURNS text AS $$
DECLARE
    model_name_v text;
    result text;
    messages jsonb;
BEGIN
    -- Get model name from parameter or configuration
    IF model_name IS NULL THEN
        model_name_v := current_setting('ai.image_model', true);
    ELSE
        model_name_v := model_name;
    END IF;

    IF model_name_v IS NULL THEN
        RAISE EXCEPTION 'Image model name is not set';
    END IF;
    
    -- Construct the message array with image
    messages := jsonb_build_array(
        jsonb_build_object(
            'role', 'user',
            'content', jsonb_build_array(
                jsonb_build_object('type', 'text', 'text', prompt),
                jsonb_build_object(
                    'type', 'image_url',
                    'image_url', jsonb_build_object('url', image_url)
                )
            )
        )
    );
    
    -- Invoke the model with the constructed message
    result := ai.invoke_model(model_name_v, jsonb_build_object('messages', messages) || config);
    
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Image analysis function with binary data
CREATE OR REPLACE FUNCTION ai.image(
    prompt text,
    image bytea,
    mime_type text = 'image/jpeg',
    model_name text = NULL,
    config jsonb = '{}'::jsonb
) RETURNS text AS $$
DECLARE
    base64_image text;
BEGIN
    -- Check if the mime_type is valid
    IF mime_type NOT IN ('image/jpeg', 'image/png', 'image/gif', 'image/webp') THEN
        RAISE EXCEPTION 'Unsupported mime_type: %', mime_type;
    END IF;

    -- Convert binary image to base64
    base64_image := encode(image, 'base64');
    
    -- Call the URL version with data URL
    RETURN ai.image(prompt, 'data:' || mime_type || ';base64,' || base64_image, model_name, config);
END;
$$ LANGUAGE plpgsql;
