\set region_id random(1, 20)

SELECT user_id,
       username,
       region_id
FROM bench_users
WHERE region_id = :region_id
LIMIT 100;