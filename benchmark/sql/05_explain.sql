\set ON_ERROR_STOP on

EXPLAIN (ANALYZE, VERBOSE, BUFFERS)
SELECT *
FROM bench_users
WHERE user_id = 100;

EXPLAIN (ANALYZE, VERBOSE, BUFFERS)
SELECT *
FROM bench_users
WHERE region_id = 10
LIMIT 100;

EXPLAIN (ANALYZE, VERBOSE, BUFFERS)
SELECT status, count(*), sum(amount)
FROM bench_orders
GROUP BY status;

EXPLAIN (ANALYZE, VERBOSE, BUFFERS)
SELECT u.user_id,
       u.username,
       count(o.order_id),
       sum(o.amount)
FROM bench_users AS u
JOIN bench_orders AS o
  ON o.user_id = u.user_id
WHERE u.user_id BETWEEN 1 AND 100
GROUP BY u.user_id, u.username;

EXPLAIN (ANALYZE, VERBOSE, BUFFERS)
SELECT c.category_name,
       count(*),
       sum(o.amount)
FROM bench_orders AS o
JOIN bench_categories AS c
  ON c.category_id = o.category_id
GROUP BY c.category_name;
