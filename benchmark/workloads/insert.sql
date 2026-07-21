\set user_id random(1, 500000)
\set category_id random(1, 20)
\set status random(1, 5)
\set amount random(1, 10000)
\set order_id random(1000000, 2000000000)

INSERT INTO bench_orders (
    user_id,
    order_id,
    category_id,
    status,
    amount,
    created_at
)
VALUES (
    :user_id,
    :order_id,
    :category_id,
    :status,
    :amount,
    current_timestamp
);
