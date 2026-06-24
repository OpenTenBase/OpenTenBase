/* src/pl/oraplsql/src/oraplsql--1.0.sql */

/*
 * Currently, all the interesting stuff is done by CREATE LANGUAGE.
 * Later we will probably "dumb down" that command and put more of the
 * knowledge into this script.
 */

CREATE PROCEDURAL LANGUAGE oraplsql;

COMMENT ON PROCEDURAL LANGUAGE oraplsql IS 'PL/SQL procedural language';

