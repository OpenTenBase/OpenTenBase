/* contrib/etime/etime--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION etime" to load this file. \quit

-- to get records whose duration_time is >= param t
CREATE OR REPLACE FUNCTION e_gt_dt(t integer)
RETURNS SETOF etime AS $$
BEGIN
    RETURN QUERY
    SELECT * FROM etime WHERE duration_time > t;
END;
$$ LANGUAGE plpgsql;
