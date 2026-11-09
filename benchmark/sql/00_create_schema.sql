\set ON_ERROR_STOP on

DROP TABLE IF EXISTS bench_payments;
DROP TABLE IF EXISTS bench_orders;
DROP TABLE IF EXISTS bench_users;
DROP TABLE IF EXISTS bench_categories;

CREATE TABLE bench_users (
    user_id     bigint PRIMARY KEY,
    username    text NOT NULL,
    region_id   integer NOT NULL,
    created_at  timestamp NOT NULL
) DISTRIBUTE BY SHARD(user_id);

CREATE TABLE bench_orders (
    user_id     bigint NOT NULL,
    order_id    bigint NOT NULL,
    category_id integer NOT NULL,
    status      integer NOT NULL,
    amount      numeric(12, 2) NOT NULL,
    created_at  timestamp NOT NULL,
    PRIMARY KEY (user_id, order_id)
) DISTRIBUTE BY SHARD(user_id);

CREATE TABLE bench_categories (
    category_id   integer PRIMARY KEY,
    category_name text NOT NULL
) DISTRIBUTE BY REPLICATION;

CREATE TABLE bench_payments (
    payment_id bigint PRIMARY KEY,
    user_id    bigint NOT NULL,
    order_id   bigint NOT NULL,
    amount     numeric(12, 2) NOT NULL
) DISTRIBUTE BY SHARD(payment_id);