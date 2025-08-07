/* src/pl/plpgsql/src/plpgsql--unpackaged--1.0.sql */

ALTER EXTENSION oraplsql ADD PROCEDURAL LANGUAGE oraplsql;
-- ALTER ADD LANGUAGE doesn't pick up the support functions, so we have to.
ALTER EXTENSION oraplsql ADD FUNCTION oraplsql_call_handler();
ALTER EXTENSION oraplsql ADD FUNCTION oraplsql_inline_handler(internal);
ALTER EXTENSION oraplsql ADD FUNCTION oraplsql_validator(oid);
