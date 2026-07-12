-- =============================================================================
-- bench_single_insert.sql
-- Single-row INSERT benchmark for pgbench.
-- Inserts one row into bench_transactions per invocation.
-- Uses realistic variable-length values (not all identical rows).
-- =============================================================================
\set from_acc random(1, 100000)
\set to_acc   random(1, 100000)
\set amt_int  random(1, 999999)
\set st       random(1, 100)
\set tp       random(1, 100)

INSERT INTO bench_transactions (from_account, to_account, amount, status, txn_type, ts, description)
VALUES (
    :from_acc,
    :to_acc,
    (:amt_int / 100.0)::double precision,
    CASE WHEN :st <= 94 THEN 0 WHEN :st <= 97 THEN 1 ELSE 2 END,
    CASE WHEN :tp <= 55 THEN 'transfer'
         WHEN :tp <= 75 THEN 'deposit'
         WHEN :tp <= 90 THEN 'withdrawal'
         ELSE 'refund'
    END,
    clock_timestamp(),
    CASE WHEN random() < 0.2 THEN 'desc_' || left(md5(random()::text), 6) ELSE '' END
);
