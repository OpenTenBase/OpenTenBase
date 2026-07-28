-- Copyright (c) 2026 OpenTenBase Authors
-- Licensed under the BSD 3-Clause License.

\set lower_id random(1, :account_count - 1000)
\set upper_id :lower_id + 999
SELECT count(*)
FROM otb_bench.customer_order AS o
JOIN otb_bench.order_audit AS x USING (order_id)
WHERE o.account_id BETWEEN :lower_id AND :upper_id;
