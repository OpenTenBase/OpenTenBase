SELECT node_name,
       node_type,
       node_host,
       node_port,
       node_forward_port
FROM pgxc_node
ORDER BY node_type, node_name;

SELECT c.relname,
       x.pclocatortype,
       x.nodeoids
FROM pgxc_class AS x
JOIN pg_class AS c
  ON c.oid = x.pcrelid
WHERE c.relname LIKE 'bench_%'
ORDER BY c.relname;

SELECT 'bench_users' AS table_name, count(*) AS rows
FROM bench_users
UNION ALL
SELECT 'bench_orders', count(*)
FROM bench_orders
UNION ALL
SELECT 'bench_categories', count(*)
FROM bench_categories
UNION ALL
SELECT 'bench_payments', count(*)
FROM bench_payments;