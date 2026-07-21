SELECT status,
       count(*) AS order_count,
       sum(amount) AS total_amount
FROM bench_orders
GROUP BY status;