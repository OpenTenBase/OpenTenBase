-- SPDX-FileCopyrightText: 2026 messere1
-- SPDX-License-Identifier: BSD-3-Clause

-- Run with psql -X -v ON_ERROR_STOP=1 -f this_file.sql after installing the extension.
-- The transaction-local mock avoids network access and paid API calls.
BEGIN;
CREATE SCHEMA ai_raw_test;
CREATE FUNCTION ai_raw_test.http(http_request) RETURNS http_response
LANGUAGE sql AS $$
    SELECT 200, 'application/json'::varchar, ($1).headers, ($1).content
$$;
SET LOCAL search_path = ai_raw_test, public;
SET LOCAL plpgsql.variable_conflict = error;
SELECT ai.add_model('raw-regression', ARRAY[http_header('X-Test', 'sent')],
    'https://unused.invalid', '{"model":"default","temperature":0.5}',
    'test', 'POST', 'application/json', 'SELECT %L');
DO $$
DECLARE r http_response;
BEGIN
    r := ai.raw_invoke_model('raw-regression', '{"prompt":"hello","temperature":0.2}');
    IF r.status IS DISTINCT FROM 200 OR
       r.content::jsonb IS DISTINCT FROM '{"model":"default","prompt":"hello","temperature":0.2}'::jsonb OR
       (r.headers[1]).value IS DISTINCT FROM 'sent' THEN
        RAISE EXCEPTION 'raw invocation lost model arguments or headers';
    END IF;
    BEGIN
        PERFORM ai.raw_invoke_model('missing-raw-regression', '{}');
        RAISE EXCEPTION 'missing model unexpectedly succeeded';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM <> 'Model missing-raw-regression not found' THEN RAISE; END IF;
    END;
END $$;
ROLLBACK;
