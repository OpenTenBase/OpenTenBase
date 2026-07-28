-- Copyright (c) 2026 OpenTenBase Authors
-- Licensed under the BSD 3-Clause License.

\set lower_id random(1, :account_count - 1000)
\set upper_id :lower_id + 999
SELECT r.region_name, count(*), sum(o.amount)
FROM otb_bench.customer_order AS o
JOIN otb_bench.region AS r USING (region_id)
WHERE o.account_id BETWEEN :lower_id AND :upper_id
GROUP BY r.region_name;
