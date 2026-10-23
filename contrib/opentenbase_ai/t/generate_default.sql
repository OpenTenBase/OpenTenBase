-- SPDX-FileCopyrightText: 2026 messere1
-- SPDX-License-Identifier: BSD-3-Clause

-- Run with psql -X -v ON_ERROR_STOP=1 -f this_file.sql after CREATE EXTENSION.
BEGIN;
CREATE OR REPLACE FUNCTION ai.invoke_model(model_name_v text, user_args jsonb)
RETURNS text LANGUAGE sql AS $$ SELECT '42'::text $$;
DO $$
BEGIN
    IF pg_typeof(ai.generate('test', model_name => 'unused')) <> 'text'::regtype OR
       ai.generate('test', model_name => 'unused') IS DISTINCT FROM '42'::text OR
       ai.generate('test', NULL::integer, 'unused') IS DISTINCT FROM 42 OR
       ai.generate_int('test', 'unused') IS DISTINCT FROM 42 OR
       ai.generate_double('test', 'unused') IS DISTINCT FROM 42::double precision THEN
        RAISE EXCEPTION 'default or explicit polymorphic result type is incorrect';
    END IF;
END $$;
ROLLBACK;
