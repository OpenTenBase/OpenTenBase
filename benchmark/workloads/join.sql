\set low_id random(1, 499900)
\set high_id :low_id + 99

SELECT u.user_id,
       u.username,
       count(o.order_id) AS order_count,
       sum(o.amount) AS total_amount
FROM bench_users AS u
JOIN bench_orders AS o
  ON o.user_id = u.user_id
WHERE u.user_id BETWEEN :low_id AND :high_id
GROUP BY u.user_id, u.username;
