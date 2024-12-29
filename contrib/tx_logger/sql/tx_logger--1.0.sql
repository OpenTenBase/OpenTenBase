
-- tx_logger--1.0.sql

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION tx_logger" to load this file. \quit

CREATE TABLE tx_logger (
    xid xid NOT NULL,
    action text NOT NULL,
    timestamp timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE OR REPLACE FUNCTION log_transaction_start()
RETURNS void AS '$libdir/tx_logger', 'log_transaction_start'
LANGUAGE c VOLATILE;

CREATE OR REPLACE FUNCTION log_transaction_commit()
RETURNS void AS '$libdir/tx_logger', 'log_transaction_commit'
LANGUAGE c VOLATILE;

CREATE OR REPLACE FUNCTION log_transaction_rollback()
RETURNS void AS '$libdir/tx_logger', 'log_transaction_rollback'
LANGUAGE c VOLATILE;