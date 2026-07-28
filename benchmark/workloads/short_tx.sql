-- Copyright (c) 2026 OpenTenBase Authors
-- Licensed under the BSD 3-Clause License.

\set account_id random(1, :account_count)
\set region_id random(1, 64)
BEGIN;
UPDATE otb_bench.account
SET balance = balance + 0.01
WHERE account_id = :account_id;
INSERT INTO otb_bench.account_event
    (event_id, account_id, region_id, event_type, payload, created_at)
VALUES
    (nextval('otb_bench.event_id_seq'), :account_id, :region_id, 1,
     'short-transaction', clock_timestamp());
COMMIT;
