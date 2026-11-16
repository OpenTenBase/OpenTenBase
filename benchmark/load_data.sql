-- OpenTenBase benchmark 的固定造数脚本。
-- 通过固定行数和固定分布规则，保证多次重跑结果可复现。

TRUNCATE TABLE perf_event;
TRUNCATE TABLE perf_order;
TRUNCATE TABLE perf_user;
TRUNCATE TABLE perf_city;

INSERT INTO perf_city (city_id, city_name)
SELECT
    g,
    'city_' || lpad(g::text, 3, '0')
FROM generate_series(1, 100) AS g;

INSERT INTO perf_user (user_id, user_name, age, city_id, created_at)
SELECT
    g,
    'user_' || lpad(g::text, 5, '0'),
    18 + (g % 43),
    ((g - 1) % 100) + 1,
    now() - (((g % 365) || ' days')::interval)
FROM generate_series(1, 10000) AS g;

INSERT INTO perf_order (order_id, user_id, amount, status, created_at)
SELECT
    g::bigint,
    ((g - 1) % 10000) + 1,
    (((g % 500000) + 100)::numeric / 100.0)::numeric(12,2),
    g % 5,
    now() - (((g % 90) || ' days')::interval)
FROM generate_series(1, 100000) AS g;

INSERT INTO perf_event (event_id, user_id, event_type, payload, created_at)
SELECT
    g::bigint,
    ((g - 1) % 10000) + 1,
    g % 8,
    'event_payload_' || g::text,
    now() - (((g % 30) || ' days')::interval)
FROM generate_series(1, 500000) AS g;

SELECT setval('perf_order_id_seq', COALESCE((SELECT max(order_id) FROM perf_order), 1), true);
SELECT setval('perf_event_id_seq', COALESCE((SELECT max(event_id) FROM perf_event), 1), true);

ANALYZE perf_city;
ANALYZE perf_user;
ANALYZE perf_order;
ANALYZE perf_event;

SELECT 'perf_city' AS table_name, count(*) AS row_count FROM perf_city
UNION ALL
SELECT 'perf_user' AS table_name, count(*) AS row_count FROM perf_user
UNION ALL
SELECT 'perf_order' AS table_name, count(*) AS row_count FROM perf_order
UNION ALL
SELECT 'perf_event' AS table_name, count(*) AS row_count FROM perf_event
ORDER BY table_name;
