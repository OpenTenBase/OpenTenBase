\set user_id random(1, 500000)

SELECT user_id,
       username,
       region_id,
       created_at
FROM bench_users
WHERE user_id = :user_id;
