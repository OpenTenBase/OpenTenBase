-- Copyright (c) 2026 OpenTenBase Authors
-- Licensed under the BSD 3-Clause License.

\set ON_ERROR_STOP on
SET search_path TO otb_bench, public;

SELECT 'account', xc_node_id::text, count(*)::text
FROM account
GROUP BY xc_node_id
UNION ALL
SELECT 'customer_order', xc_node_id::text, count(*)::text
FROM customer_order
GROUP BY xc_node_id
UNION ALL
SELECT 'order_audit', xc_node_id::text, count(*)::text
FROM order_audit
GROUP BY xc_node_id
UNION ALL
SELECT 'account_event', xc_node_id::text, count(*)::text
FROM account_event
GROUP BY xc_node_id
ORDER BY 1, 2;
