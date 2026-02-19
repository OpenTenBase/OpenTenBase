CREATE FUNCTION redis_fdw_handler()
  RETURNS fdw_handler
  AS 'MODULE_PATHNAME'
  LANGUAGE C STRICT;

CREATE FUNCTION redis_fdw_validator(text[], oid)
  RETURNS void
  AS 'MODULE_PATHNAME'
  LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER redis_fdw
  HANDLER redis_fdw_handler
  VALIDATOR redis_fdw_validator;
