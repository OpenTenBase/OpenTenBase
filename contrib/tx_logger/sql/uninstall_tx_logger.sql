
-- uninstall_tx_logger.sql
DROP TABLE IF EXISTS tx_logger CASCADE;
DROP FUNCTION IF EXISTS log_transaction_start() CASCADE;
DROP FUNCTION IF EXISTS log_transaction_commit() CASCADE;
DROP FUNCTION IF EXISTS log_transaction_rollback() CASCADE;