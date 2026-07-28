-- Copyright (c) 2026 OpenTenBase Authors
-- Licensed under the BSD 3-Clause License.

\set account_id random(1, :account_count)
SELECT account_id, region_id, status, balance
FROM otb_bench.account
WHERE account_id = :account_id;
