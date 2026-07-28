-- Copyright (c) 2026 OpenTenBase Authors
-- Licensed under the BSD 3-Clause License.

\set ON_ERROR_STOP on

DROP SCHEMA IF EXISTS otb_bench CASCADE;
CREATE SCHEMA otb_bench;
SET search_path TO otb_bench, public;

CREATE SEQUENCE event_id_seq;

CREATE TABLE region (
    region_id integer NOT NULL,
    region_name text NOT NULL,
    PRIMARY KEY (region_id)
) DISTRIBUTE BY REPLICATION TO GROUP :group_name;

CREATE TABLE account (
    account_id bigint NOT NULL,
    region_id integer NOT NULL,
    status smallint NOT NULL,
    balance numeric(14, 2) NOT NULL,
    created_at timestamp NOT NULL
) DISTRIBUTE BY SHARD (account_id) TO GROUP :group_name;

CREATE INDEX account_id_idx ON account (account_id);

CREATE TABLE customer_order (
    order_id bigint NOT NULL,
    account_id bigint NOT NULL,
    region_id integer NOT NULL,
    status smallint NOT NULL,
    amount numeric(12, 2) NOT NULL,
    created_at timestamp NOT NULL
) DISTRIBUTE BY SHARD (account_id) TO GROUP :group_name;

CREATE INDEX customer_order_account_idx
    ON customer_order (account_id, order_id);
CREATE INDEX customer_order_region_idx
    ON customer_order (region_id);

-- Deliberately sharded by a different key to expose redistribution cost.
CREATE TABLE order_audit (
    order_id bigint NOT NULL,
    account_id bigint NOT NULL,
    action smallint NOT NULL,
    created_at timestamp NOT NULL
) DISTRIBUTE BY SHARD (order_id) TO GROUP :group_name;

CREATE INDEX order_audit_account_idx ON order_audit (account_id);

CREATE TABLE account_event (
    event_id bigint NOT NULL,
    account_id bigint NOT NULL,
    region_id integer NOT NULL,
    event_type smallint NOT NULL,
    payload text NOT NULL,
    created_at timestamp NOT NULL
) DISTRIBUTE BY SHARD (account_id) TO GROUP :group_name;

CREATE INDEX account_event_account_idx
    ON account_event (account_id, event_id);
