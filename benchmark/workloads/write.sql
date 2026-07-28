-- Copyright (c) 2026 OpenTenBase Authors
-- Licensed under the BSD 3-Clause License.

\set account_id random(1, :account_count)
\set region_id random(1, 64)
\set event_type random(1, 16)
INSERT INTO otb_bench.account_event
    (event_id, account_id, region_id, event_type, payload, created_at)
VALUES
    (nextval('otb_bench.event_id_seq'), :account_id, :region_id, :event_type,
     repeat('x', 64), clock_timestamp());
