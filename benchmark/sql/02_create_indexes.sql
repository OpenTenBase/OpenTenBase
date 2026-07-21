\set ON_ERROR_STOP on
\timing on

CREATE INDEX bench_users_region_idx
    ON bench_users (region_id);

CREATE INDEX bench_orders_status_idx
    ON bench_orders (status);

CREATE INDEX bench_orders_category_idx
    ON bench_orders (category_id);

CREATE INDEX bench_payments_order_idx
    ON bench_payments (user_id, order_id);

\timing off