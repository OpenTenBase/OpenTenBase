-- SPDX-FileCopyrightText: 2026 messere1
-- SPDX-License-Identifier: BSD-3-Clause

-- Requires the same local echo server as sql/http.sql.
-- psql -X -v ON_ERROR_STOP=1 -f t/bodyless_patch.sql
-- Configure http.server_host before running if the server uses another address.
BEGIN;
SET LOCAL http.server_host = 'http://localhost:9080';
DO $$
DECLARE r http_response;
BEGIN
    r := http(('PATCH', current_setting('http.server_host') || '/anything',
        NULL, NULL, NULL)::http_request);
    IF r.status IS DISTINCT FROM 200 OR r.content::jsonb->>'method' IS DISTINCT FROM 'PATCH' THEN
        RAISE EXCEPTION 'bodyless PATCH used the wrong HTTP method: %', r;
    END IF;
    r := http_patch(current_setting('http.server_host') || '/anything', '', 'text/plain');
    IF r.content::jsonb->>'method' IS DISTINCT FROM 'PATCH' THEN
        RAISE EXCEPTION 'empty-body PATCH used the wrong HTTP method';
    END IF;
    r := http_delete(current_setting('http.server_host') || '/anything');
    IF r.content::jsonb->>'method' IS DISTINCT FROM 'DELETE' THEN
        RAISE EXCEPTION 'bodyless DELETE regressed';
    END IF;
END $$;
ROLLBACK;
