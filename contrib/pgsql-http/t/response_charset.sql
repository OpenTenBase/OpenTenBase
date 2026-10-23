-- SPDX-FileCopyrightText: 2026 messere1
-- SPDX-License-Identifier: BSD-3-Clause

-- Requires t/charset_server.py on 127.0.0.1:9081.
-- psql -X -v ON_ERROR_STOP=1 -f t/response_charset.sql
BEGIN;
DO $$
DECLARE path text; r http_response;
BEGIN
    FOREACH path IN ARRAY ARRAY['plain','quoted','spaces','mixed','other-quoted'] LOOP
        r := http_get('http://127.0.0.1:9081/' || path);
        IF r.status IS DISTINCT FROM 200 OR r.content IS DISTINCT FROM 'café' THEN
            RAISE EXCEPTION 'response charset was not applied for %', path;
        END IF;
    END LOOP;
END $$;
ROLLBACK;
