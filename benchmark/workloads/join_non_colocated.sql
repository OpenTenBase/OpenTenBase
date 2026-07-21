\set low_order_id random(1, 1499480)
\set high_order_id :low_order_id + 499

SELECT count(*) AS payment_count,
       sum(p.amount) AS payment_amount
FROM bench_orders AS o
JOIN bench_payments AS p
  ON p.user_id = o.user_id
 AND p.order_id = o.order_id
WHERE o.order_id BETWEEN :low_order_id AND :high_order_id;
