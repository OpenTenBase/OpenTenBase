-- SPDX-FileCopyrightText: 2026 messere1
-- SPDX-License-Identifier: BSD-3-Clause

-- Run with psql -X -v ON_ERROR_STOP=1 -f this_file.sql in a test database.
BEGIN;
-- Capture the URL overload without contacting an image provider.
CREATE OR REPLACE FUNCTION ai.image(prompt text, image_url text, model_name text = NULL, config jsonb = '{}'::jsonb) RETURNS text
LANGUAGE sql AS $$ SELECT $2 $$;
DO $$
DECLARE payload bytea; url text; mime text;
BEGIN
    FOREACH mime IN ARRAY ARRAY['image/jpeg','image/png','image/gif','image/webp'] LOOP
        FOR n IN 0..200 LOOP
            payload := decode(repeat('ab', n), 'hex');
            url := ai.image('test', payload, mime, 'unused', '{}');
            IF url ~ E'[\r\n]' OR
               split_part(url, ',', 1) <> 'data:' || mime || ';base64' OR
               decode(split_part(url, ',', 2), 'base64') IS DISTINCT FROM payload THEN
                RAISE EXCEPTION 'invalid image data URL for % bytes of %', n, mime;
            END IF;
        END LOOP;
    END LOOP;
END $$;
ROLLBACK;
