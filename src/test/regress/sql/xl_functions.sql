--Immutable, stable, volatile functions and nextval are allowed in DEFAULT clause

-- IMMUTABLE function is allowed
CREATE FUNCTION xl_add(integer, integer) RETURNS integer
    AS 'select $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE TABLE xl_funct2(
	a integer,
	b integer,
	c integer DEFAULT xl_add(10,12 )
) DISTRIBUTE BY HASH(a);

INSERT INTO xl_funct2(a,b) VALUES (1,2);--c should be 22
INSERT INTO xl_funct2(a,b,c) VALUES (3,4,5);-- c should be 5

SELECT * from xl_funct2 order by 1;

--STABLE functions
-- checking STABLE function in DEFAULT of non-distribution column
create function xl_nochange(text) returns text
  as 'select $1 limit 1' language sql stable pushdown;

CREATE TABLE xl_funct3(
	a integer,
	b integer,
	c text DEFAULT xl_nochange('hello')
) DISTRIBUTE BY HASH(a);

INSERT INTO xl_funct3(a,b) VALUES (1,2);--c should be pallavi
INSERT INTO xl_funct3(a,b,c) VALUES (3,4,'qwerty');-- c should be qwerty

SELECT * from xl_funct3 order by 1;

-- checking STABLE function in DEFAULT of distribution column
CREATE TABLE xl_funct4(
	a integer,
	b integer,
	c text DEFAULT xl_nochange('hello')
) DISTRIBUTE BY HASH(c);

INSERT INTO xl_funct4(a,b) VALUES (1,2);--c should be pallavi
INSERT INTO xl_funct4(a,b,c) VALUES (3,4,'qwerty');-- c should be qwerty

SELECT * from xl_funct4 order by 1;

--VOLATILE functions

create or replace function xl_get_curr_decade() returns double precision as
$$ SELECT EXTRACT(DECADE FROM NOW()); $$
language sql volatile pushdown;

CREATE TABLE xl_funct5(
	a integer,
	b integer,
	c double precision DEFAULT xl_get_curr_decade()
) DISTRIBUTE BY HASH(a);

INSERT INTO xl_funct5(a,b) VALUES (1,2);--c should be e.g. 201 for 2015
INSERT INTO xl_funct5(a,b,c) VALUES (3,4,20);-- c should be 20

SELECT * from xl_funct5 order by 1;

--nextval check
SET sequence_range = 1;
CREATE SEQUENCE xl_INSERT_SEQ;

CREATE TABLE xl_funct (
	a integer,
	b INT DEFAULT nextval('xl_insert_seq')
) DISTRIBUTE BY HASH (a);

INSERT INTO xl_funct (a) VALUES (1);
INSERT INTO xl_funct (a) VALUES (2);
INSERT INTO xl_funct (a) VALUES (3);

SELECT * FROM xl_funct order by 1;

CREATE TABLE xl_funct1 (
	a integer DEFAULT nextval('xl_insert_seq'),
	b INT 
) DISTRIBUTE BY HASH (a);

INSERT INTO xl_funct1 (b) VALUES (1);
INSERT INTO xl_funct1 (b) VALUES (2);
INSERT INTO xl_funct1 (b) VALUES (3);

SELECT * FROM xl_funct1 order by a;


DROP TABLE xl_funct;
DROP TABLE xl_funct1;
DROP SEQUENCE xl_INSERT_SEQ;
DROP TABLE xl_funct2;
DROP TABLE xl_funct3;
DROP TABLE xl_funct4;
DROP FUNCTION xl_nochange(text);
DROP TABLE xl_funct5;
DROP FUNCTION xl_get_curr_decade();

CREATE TABLE IF NOT EXISTS t0(c0 TEXT DEFAULT (((((lower((CAST(-1807148075 AS VARCHAR) COLLATE "C")) COLLATE "C"))||(num_nonnulls((((('' COLLATE "C"))||(('f?' COLLATE "C"))) COLLATE "C")))) COLLATE "C")), CHECK(CAST((((t0.c0 COLLATE "C"))LIKE((t0.c0 COLLATE "C"))) AS BOOLEAN))) WITH (autovacuum_vacuum_threshold=1041983328, autovacuum_enabled=1, autovacuum_vacuum_cost_limit=708, autovacuum_vacuum_cost_delay=27, autovacuum_analyze_threshold=2023889437, autovacuum_freeze_min_age=354563129, autovacuum_analyze_scale_factor=1, autovacuum_vacuum_scale_factor=0.01, autovacuum_freeze_max_age=1766099001);

INSERT INTO t0(c0) VALUES(((((((((pg_current_logfile() COLLATE "C"))::VARCHAR COLLATE "C") COLLATE "yi_US.utf8") COLLATE "C"))||(((('?AH4gQ,' COLLATE "C") COLLATE "ar_SD.utf8") COLLATE "C"))) COLLATE "C"));

DROP TABLE t0;
