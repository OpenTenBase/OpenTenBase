\set ON_ERROR_STOP on
\timing on

-- 20 个复制表类别
INSERT INTO bench_categories (
    category_id,
    category_name
)
SELECT
    g,
    'category-' || g
FROM generate_series(1, 20) AS g;

-- 500,000 个用户
INSERT INTO bench_users (
    user_id,
    username,
    region_id,
    created_at
)
SELECT
    g,
    'user-' || g,
    ((g - 1) % 20) + 1,
    timestamp '2025-01-01'
        + ((g - 1) % 365) * interval '1 day'
FROM generate_series(1, 500000) AS g;

-- 3,000,000 个订单，平均每个用户 6 个订单
INSERT INTO bench_orders (
    user_id,
    order_id,
    category_id,
    status,
    amount,
    created_at
)
SELECT
    ((g - 1) % 500000) + 1,
    g,
    ((g - 1) % 20) + 1,
    ((g - 1) % 5) + 1,
    ((g - 1) % 10000 + 1)::numeric / 100,
    timestamp '2025-01-01'
        + ((g - 1) % 365) * interval '1 day'
FROM generate_series(1, 3000000) AS g;

-- 为其中 1,499,980 个订单创建付款记录；四张表合计恰好 5,000,000 行
INSERT INTO bench_payments (
    payment_id,
    user_id,
    order_id,
    amount
)
SELECT
    g,
    ((g - 1) % 500000) + 1,
    g,
    ((g - 1) % 10000 + 1)::numeric / 100
FROM generate_series(1, 1499980) AS g;

\timing off
