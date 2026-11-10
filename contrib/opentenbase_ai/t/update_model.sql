-- SPDX-FileCopyrightText: 2026 messere1
-- SPDX-License-Identifier: BSD-3-Clause

-- Run with psql -X -v ON_ERROR_STOP=1 -f this_file.sql in a test database.
BEGIN;
TRUNCATE public.ai_model_list;
DO $$
BEGIN
    IF ai.update_model('missing', 'default_args', '{}') IS DISTINCT FROM false THEN
        RAISE EXCEPTION 'updating an empty registry must return false';
    END IF;
    PERFORM ai.add_model('quote''model', ARRAY[]::http_header[], 'https://unused.invalid',
        '{}', 'test', 'POST', 'application/json', 'SELECT %L');
    IF NOT ai.update_model('quote''model', 'default_args', '{"model":"updated"}') OR
       NOT ai.update_model('quote''model', 'request_header', ARRAY[http_header('X-Test', 'quoted''value')]::text) OR
       NOT ai.update_model('quote''model', 'model_provider', NULL) THEN
        RAISE EXCEPTION 'typed updates did not affect model';
    END IF;
    IF NOT EXISTS (SELECT FROM public.ai_model_list WHERE model_name = 'quote''model'
        AND default_args = '{"model":"updated"}'::jsonb AND model_provider IS NULL
        AND (request_header[1]).value = 'quoted''value') THEN
        RAISE EXCEPTION 'typed or quoted value was not preserved';
    END IF;
    BEGIN
        PERFORM ai.update_model('quote''model', 'default_args', 'invalid json');
        RAISE EXCEPTION 'invalid JSON unexpectedly accepted';
    EXCEPTION WHEN invalid_text_representation THEN NULL;
    END;
END $$;
ROLLBACK;
