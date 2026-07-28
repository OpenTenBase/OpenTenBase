-- Copyright (c) 2026 OpenTenBase Authors
-- Licensed under the BSD 3-Clause License.

\set ON_ERROR_STOP on
\timing on
SET search_path TO otb_bench, public;

INSERT INTO region (region_id, region_name)
SELECT id, 'region-' || id
FROM generate_series(1, 64) AS id;

INSERT INTO account (account_id, region_id, status, balance, created_at)
SELECT
    id,
    ((id - 1) % 64) + 1,
    (id % 4)::smallint,
    ((id * 17) % 100000)::numeric / 100,
    timestamp '2025-01-01 00:00:00' + (id % 31536000) * interval '1 second'
FROM generate_series(1, :account_count) AS id;

INSERT INTO customer_order
    (order_id, account_id, region_id, status, amount, created_at)
SELECT
    id,
    ((id * 7919) % :account_count) + 1,
    ((((id * 7919) % :account_count)) % 64) + 1,
    (id % 8)::smallint,
    (((id * 97) % 100000) + 100)::numeric / 100,
    timestamp '2025-01-01 00:00:00' + (id % 31536000) * interval '1 second'
FROM generate_series(1, :order_count) AS id;

INSERT INTO order_audit (order_id, account_id, action, created_at)
SELECT
    id,
    ((id * 7919) % :account_count) + 1,
    (id % 5)::smallint,
    timestamp '2025-01-01 00:00:00' + (id % 31536000) * interval '1 second'
FROM generate_series(1, :order_count) AS id;

INSERT INTO account_event
    (event_id, account_id, region_id, event_type, payload, created_at)
SELECT
    id,
    ((id * 3571) % :account_count) + 1,
    ((((id * 3571) % :account_count)) % 64) + 1,
    (id % 16)::smallint,
    repeat(chr(97 + (id % 26)::integer), 64),
    timestamp '2025-01-01 00:00:00' + (id % 31536000) * interval '1 second'
FROM generate_series(1, :event_count) AS id;

SELECT setval('event_id_seq', :event_count, true);

ANALYZE region;
ANALYZE account;
ANALYZE customer_order;
ANALYZE order_audit;
ANALYZE account_event;
