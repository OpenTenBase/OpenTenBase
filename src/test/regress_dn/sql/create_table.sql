--
-- CREATE_TABLE
--

--
-- CLASS DEFINITIONS
--
CREATE TABLE hobbies_r (
	name		text,
	person 		text
);

CREATE TABLE equipment_r (
	name 		text,
	hobby		text
);

CREATE TABLE onek (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);

CREATE TABLE tenk1 (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
) WITH OIDS;

CREATE TABLE tenk2 (
	unique1 	int4,
	unique2 	int4,
	two 	 	int4,
	four 		int4,
	ten			int4,
	twenty 		int4,
	hundred 	int4,
	thousand 	int4,
	twothousand int4,
	fivethous 	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);


CREATE TABLE person (
	name 		text,
	age			int4,
	location 	point
);


CREATE TABLE emp (
	salary 		int4,
	manager 	name
) INHERITS (person) WITH OIDS;


CREATE TABLE student (
	gpa 		float8
) INHERITS (person);


CREATE TABLE stud_emp (
	percent 	int4
) INHERITS (emp, student);

CREATE TABLE city (
	name		name,
	location 	box,
	budget 		city_budget
);

CREATE TABLE dept (
	dname		name,
	mgrname 	text
);

CREATE TABLE slow_emp4000 (
	home_base	 box
);

CREATE TABLE fast_emp4000 (
	home_base	 box
);

CREATE TABLE road (
	name		text,
	thepath 	path
);

CREATE TABLE ihighway () INHERITS (road);

CREATE TABLE shighway (
	surface		text
) INHERITS (road);

CREATE TABLE real_city (
	pop			int4,
	cname		text,
	outline 	path
);

--
-- test the "star" operators a bit more thoroughly -- this time,
-- throw in lots of NULL fields...
--
-- a is the type root
-- b and c inherit from a (one-level single inheritance)
-- d inherits from b and c (two-level multiple inheritance)
-- e inherits from c (two-level single inheritance)
-- f inherits from e (three-level single inheritance)
--
CREATE TABLE a_star (
	class		char,
	a 			int4
);

CREATE TABLE b_star (
	b 			text
) INHERITS (a_star);

CREATE TABLE c_star (
	c 			name
) INHERITS (a_star);

CREATE TABLE d_star (
	d 			float8
) INHERITS (b_star, c_star);

CREATE TABLE e_star (
	e 			int2
) INHERITS (c_star);

CREATE TABLE f_star (
	f 			polygon
) INHERITS (e_star);

CREATE TABLE aggtest (
	a 			int2,
	b			float4
);

CREATE TABLE hash_i4_heap (
	seqno 		int4,
	random 		int4
) DISTRIBUTE BY REPLICATION;

CREATE TABLE hash_name_heap (
	seqno 		int4,
	random 		name
) DISTRIBUTE BY REPLICATION;

CREATE TABLE hash_txt_heap (
	seqno 		int4,
	random 		text
) DISTRIBUTE BY REPLICATION;

CREATE TABLE hash_f8_heap (
	seqno		int4,
	random 		float8
) DISTRIBUTE BY REPLICATION;

-- don't include the hash_ovfl_heap stuff in the distribution
-- the data set is too large for what it's worth
--
-- CREATE TABLE hash_ovfl_heap (
--	x			int4,
--	y			int4
-- );

CREATE TABLE bt_i4_heap (
	seqno 		int4,
	random 		int4
);

CREATE TABLE bt_name_heap (
	seqno 		name,
	random 		int4
);

CREATE TABLE bt_txt_heap (
	seqno 		text,
	random 		int4
);

CREATE TABLE bt_f8_heap (
	seqno 		float8,
	random 		int4
);

CREATE TABLE array_op_test (
	seqno		int4,
	i			int4[],
	t			text[]
);

CREATE TABLE array_index_op_test (
	seqno		int4,
	i			int4[],
	t			text[]
);

CREATE TABLE testjsonb (
       j jsonb
);

CREATE TABLE unknowntab (
	u unknown    -- fail
);

CREATE TYPE unknown_comptype AS (
	u unknown    -- fail
);

CREATE TABLE IF NOT EXISTS test_tsvector(
	t text,
	a tsvector
);

CREATE TABLE IF NOT EXISTS test_tsvector(
	t text
);

-- invalid: non-lowercase quoted reloptions identifiers
CREATE TABLE tas_case WITH ("Fillfactor" = 10) AS SELECT 1 a;
CREATE TABLE tas_case (a text) WITH ("Oids" = true);

CREATE UNLOGGED TABLE unlogged1 (a int primary key);			-- OK
CREATE TEMPORARY TABLE unlogged2 (a int primary key);			-- OK
SELECT relname, relkind, relpersistence FROM pg_class WHERE relname ~ '^unlogged\d' ORDER BY relname;
REINDEX INDEX unlogged1_pkey;
REINDEX INDEX unlogged2_pkey;
SELECT relname, relkind, relpersistence FROM pg_class WHERE relname ~ '^unlogged\d' ORDER BY relname;
DROP TABLE unlogged2;
INSERT INTO unlogged1 VALUES (42);
CREATE UNLOGGED TABLE public.unlogged2 (a int primary key);		-- also OK
CREATE UNLOGGED TABLE pg_temp.unlogged3 (a int primary key);	-- not OK
CREATE TABLE pg_temp.implicitly_temp (a int primary key);		-- OK
CREATE TEMP TABLE explicitly_temp (a int primary key);			-- also OK
CREATE TEMP TABLE pg_temp.doubly_temp (a int primary key);		-- also OK
CREATE TEMP TABLE public.temp_to_perm (a int primary key);		-- not OK
DROP TABLE unlogged1, public.unlogged2;

CREATE TABLE as_select1 AS SELECT * FROM pg_class WHERE relkind = 'r' and relpartbound is NULL;
CREATE TABLE as_select1 AS SELECT * FROM pg_class WHERE relkind = 'r' and relpartbound is NULL;
CREATE TABLE IF NOT EXISTS as_select1 AS SELECT * FROM pg_class WHERE relkind = 'r';
--CREATE TABLE as_select1 AS SELECT * FROM pg_attribute WHERE attstorage = 'p';
--CREATE TABLE as_select1 AS SELECT * FROM pg_attribute WHERE attstorage = 'p';
--CREATE TABLE IF NOT EXISTS as_select1 AS SELECT * FROM pg_attribute WHERE attstorage = 'p';
DROP TABLE as_select1;

-- create an extra wide table to test for issues related to that
-- (temporarily hide query, to avoid the long CREATE TABLE stmt)
\set ECHO none
SELECT 'CREATE TABLE extra_wide_table(firstc text, '|| array_to_string(array_agg('c'||i||' bool'),',')||', lastc text);'
FROM generate_series(1, 1100) g(i)
\gexec
\set ECHO all
INSERT INTO extra_wide_table(firstc, lastc) VALUES('first col', 'last col');
SELECT firstc, lastc FROM extra_wide_table;

-- check that the oid column is added before the primary key is checked
CREATE TABLE oid_pk (f1 INT, PRIMARY KEY(oid)) WITH OIDS;
DROP TABLE oid_pk;

--
-- Partitioned tables
--

-- cannot combine INHERITS and PARTITION BY (although grammar allows)
CREATE TABLE partitioned (
	a int
) INHERITS (some_table) PARTITION BY LIST (a);

-- cannot use more than 1 column as partition key for list partitioned table
CREATE TABLE partitioned (
	a1 int,
	a2 int
) PARTITION BY LIST (a1, a2);	-- fail

-- unsupported constraint type for partitioned tables
CREATE TABLE pkrel (
	a int PRIMARY KEY
);
CREATE TABLE partitioned (
	a int REFERENCES pkrel(a)
) PARTITION BY RANGE (a);
DROP TABLE pkrel;

CREATE TABLE partitioned (
	a int,
	EXCLUDE USING gist (a WITH &&)
) PARTITION BY RANGE (a);

-- prevent column from being used twice in the partition key
CREATE TABLE partitioned (
	a int
) PARTITION BY RANGE (a, a);

-- prevent using prohibited expressions in the key
CREATE FUNCTION retset (a int) RETURNS SETOF int AS $$ SELECT 1; $$ LANGUAGE SQL IMMUTABLE;
CREATE TABLE partitioned (
	a int
) PARTITION BY RANGE (retset(a));
DROP FUNCTION retset(int);

CREATE TABLE partitioned (
	a int
) PARTITION BY RANGE ((avg(a)));

CREATE TABLE partitioned (
	a int,
	b int
) PARTITION BY RANGE ((avg(a) OVER (PARTITION BY b)));

CREATE TABLE partitioned (
	a int
) PARTITION BY LIST ((a LIKE (SELECT 1)));

CREATE TABLE partitioned (
	a int
) PARTITION BY RANGE (('a'));

CREATE FUNCTION const_func () RETURNS int AS $$ SELECT 1; $$ LANGUAGE SQL IMMUTABLE;
CREATE TABLE partitioned (
	a int
) PARTITION BY RANGE (const_func());
DROP FUNCTION const_func();

-- only accept valid partitioning strategy
CREATE TABLE partitioned (
    a int
) PARTITION BY MAGIC (a);

-- specified column must be present in the table
CREATE TABLE partitioned (
	a int
) PARTITION BY RANGE (b);

-- cannot use system columns in partition key
CREATE TABLE partitioned (
	a int
) PARTITION BY RANGE (xmin);

-- functions in key must be immutable
CREATE FUNCTION immut_func (a int) RETURNS int AS $$ SELECT a + random()::int; $$ LANGUAGE SQL;
CREATE TABLE partitioned (
	a int
) PARTITION BY RANGE (immut_func(a));
DROP FUNCTION immut_func(int);

-- cannot contain whole-row references
CREATE TABLE partitioned (
	a	int
) PARTITION BY RANGE ((partitioned));

-- prevent using columns of unsupported types in key (type must have a btree operator class)
CREATE TABLE partitioned (
	a point
) PARTITION BY LIST (a);
CREATE TABLE partitioned (
	a point
) PARTITION BY LIST (a point_ops);
CREATE TABLE partitioned (
	a point
) PARTITION BY RANGE (a);
CREATE TABLE partitioned (
	a point
) PARTITION BY RANGE (a point_ops);

-- cannot add NO INHERIT constraints to partitioned tables
CREATE TABLE partitioned (
	a int,
	CONSTRAINT check_a CHECK (a > 0) NO INHERIT
) PARTITION BY RANGE (a);

-- some checks after successful creation of a partitioned table
CREATE FUNCTION plusone(a int) RETURNS INT AS $$ SELECT a+1; $$ LANGUAGE SQL;

CREATE TABLE partitioned (
	a int,
	b int,
	c text,
	d text
) PARTITION BY RANGE (a oid_ops, plusone(b), c collate "default", d collate "C");

-- check relkind
SELECT relkind FROM pg_class WHERE relname = 'partitioned';

-- prevent a function referenced in partition key from being dropped
DROP FUNCTION plusone(int);

-- partitioned table cannot participate in regular inheritance
CREATE TABLE partitioned2 (
	a int,
	b text
) PARTITION BY RANGE ((a+1), substr(b, 1, 5));
CREATE TABLE fail () INHERITS (partitioned2);

-- Partition key in describe output
\d partitioned
\d+ partitioned2

INSERT INTO partitioned2 VALUES (1, 'hello');
CREATE TABLE part2_1 PARTITION OF partitioned2 FOR VALUES FROM (-1, 'aaaaa') TO (100, 'ccccc');
\d+ part2_1

DROP TABLE partitioned, partitioned2;

-- check that dependencies of partition columns are handled correctly
create domain intdom1 as int;

create table partitioned (
	a intdom1,
	b text
) partition by range (a);

alter table partitioned drop column a;  -- fail

drop domain intdom1;  -- fail, requires cascade

drop domain intdom1 cascade;

table partitioned;  -- gone

-- likewise for columns used in partition expressions
create domain intdom1 as int;

create table partitioned (
	a intdom1,
	b text
) partition by range (plusone(a));

alter table partitioned drop column a;  -- fail

drop domain intdom1;  -- fail, requires cascade

drop domain intdom1 cascade;

table partitioned;  -- gone


--
-- Partitions
--

-- check partition bound syntax

CREATE TABLE list_parted (
	a int
) PARTITION BY LIST (a);
-- syntax allows only string literal, numeric literal and null to be
-- specified for a partition bound value
CREATE TABLE part_1 PARTITION OF list_parted FOR VALUES IN ('1');
CREATE TABLE part_2 PARTITION OF list_parted FOR VALUES IN (2);
CREATE TABLE part_null PARTITION OF list_parted FOR VALUES IN (null);
CREATE TABLE fail_part PARTITION OF list_parted FOR VALUES IN (int '1');
CREATE TABLE fail_part PARTITION OF list_parted FOR VALUES IN ('1'::int);

-- syntax does not allow empty list of values for list partitions
CREATE TABLE fail_part PARTITION OF list_parted FOR VALUES IN ();
-- trying to specify range for list partitioned table
CREATE TABLE fail_part PARTITION OF list_parted FOR VALUES FROM (1) TO (2);
-- trying to specify modulus and remainder for list partitioned table
CREATE TABLE fail_part PARTITION OF list_parted FOR VALUES WITH (MODULUS 10, REMAINDER 1);

-- check default partition cannot be created more than once
CREATE TABLE part_default PARTITION OF list_parted DEFAULT;
CREATE TABLE fail_default_part PARTITION OF list_parted DEFAULT;

-- specified literal can't be cast to the partition column data type
CREATE TABLE bools (
	a bool
) PARTITION BY LIST (a);
CREATE TABLE bools_true PARTITION OF bools FOR VALUES IN (1);
DROP TABLE bools;

-- specified literal can be cast, but cast isn't immutable
CREATE TABLE moneyp (
	a money
) PARTITION BY LIST (a);
CREATE TABLE moneyp_10 PARTITION OF moneyp FOR VALUES IN (10);
CREATE TABLE moneyp_10 PARTITION OF moneyp FOR VALUES IN ('10');
DROP TABLE moneyp;

-- immutable cast should work, though
CREATE TABLE bigintp (
	a bigint
) PARTITION BY LIST (a);
CREATE TABLE bigintp_10 PARTITION OF bigintp FOR VALUES IN (10);
-- fails due to overlap:
CREATE TABLE bigintp_10_2 PARTITION OF bigintp FOR VALUES IN ('10');
DROP TABLE bigintp;

CREATE TABLE range_parted (
	a date
) PARTITION BY RANGE (a);

-- trying to specify list for range partitioned table
CREATE TABLE fail_part PARTITION OF range_parted FOR VALUES IN ('a');
-- trying to specify modulus and remainder for range partitioned table
CREATE TABLE fail_part PARTITION OF range_parted FOR VALUES WITH (MODULUS 10, REMAINDER 1);
-- each of start and end bounds must have same number of values as the
-- length of the partition key
CREATE TABLE fail_part PARTITION OF range_parted FOR VALUES FROM ('a', 1) TO ('z');
CREATE TABLE fail_part PARTITION OF range_parted FOR VALUES FROM ('a') TO ('z', 1);

-- cannot specify null values in range bounds
CREATE TABLE fail_part PARTITION OF range_parted FOR VALUES FROM (null) TO (maxvalue);

-- trying to specify modulus and remainder for range partitioned table
CREATE TABLE fail_part PARTITION OF range_parted FOR VALUES WITH (MODULUS 10, REMAINDER 1);

-- check partition bound syntax for the hash partition
CREATE TABLE hash_parted (
	a int
) PARTITION BY HASH (a);
CREATE TABLE hpart_1 PARTITION OF hash_parted FOR VALUES WITH (MODULUS 10, REMAINDER 0);
CREATE TABLE hpart_2 PARTITION OF hash_parted FOR VALUES WITH (MODULUS 50, REMAINDER 1);
CREATE TABLE hpart_3 PARTITION OF hash_parted FOR VALUES WITH (MODULUS 200, REMAINDER 2);
-- modulus 25 is factor of modulus of 50 but 10 is not factor of 25.
CREATE TABLE fail_part PARTITION OF hash_parted FOR VALUES WITH (MODULUS 25, REMAINDER 3);
-- previous modulus 50 is factor of 150 but this modulus is not factor of next modulus 200.
CREATE TABLE fail_part PARTITION OF hash_parted FOR VALUES WITH (MODULUS 150, REMAINDER 3);
-- trying to specify range for the hash partitioned table
CREATE TABLE fail_part PARTITION OF hash_parted FOR VALUES FROM ('a', 1) TO ('z');
-- trying to specify list value for the hash partitioned table
CREATE TABLE fail_part PARTITION OF hash_parted FOR VALUES IN (1000);

-- trying to create default partition for the hash partitioned table
CREATE TABLE fail_default_part PARTITION OF hash_parted DEFAULT;

-- check if compatible with the specified parent

-- cannot create as partition of a non-partitioned table
CREATE TABLE unparted (
	a int
);
CREATE TABLE fail_part PARTITION OF unparted FOR VALUES IN ('a');
CREATE TABLE fail_part PARTITION OF unparted FOR VALUES WITH (MODULUS 2, REMAINDER 1);
DROP TABLE unparted;

-- cannot create a permanent rel as partition of a temp rel
CREATE TEMP TABLE temp_parted (
	a int
) PARTITION BY LIST (a);
CREATE TABLE fail_part PARTITION OF temp_parted FOR VALUES IN ('a');
DROP TABLE temp_parted;

-- cannot create a table with oids as partition of table without oids
CREATE TABLE no_oids_parted (
	a int
) PARTITION BY RANGE (a) WITHOUT OIDS;
CREATE TABLE fail_part PARTITION OF no_oids_parted FOR VALUES FROM (1) TO (10) WITH OIDS;
DROP TABLE no_oids_parted;

-- If the partitioned table has oids, then the partition must have them.
-- If the WITHOUT OIDS option is specified for partition, it is overridden.
CREATE TABLE oids_parted (
	a int
) PARTITION BY RANGE (a) WITH OIDS;
CREATE TABLE part_forced_oids PARTITION OF oids_parted FOR VALUES FROM (1) TO (10) WITHOUT OIDS;
\d+ part_forced_oids
DROP TABLE oids_parted, part_forced_oids;

-- check for partition bound overlap and other invalid specifications

CREATE TABLE list_parted2 (
	a varchar
) PARTITION BY LIST (a);
CREATE TABLE part_null_z PARTITION OF list_parted2 FOR VALUES IN (null, 'z');
CREATE TABLE part_ab PARTITION OF list_parted2 FOR VALUES IN ('a', 'b');
CREATE TABLE list_parted2_def PARTITION OF list_parted2 DEFAULT;

CREATE TABLE fail_part PARTITION OF list_parted2 FOR VALUES IN (null);
CREATE TABLE fail_part PARTITION OF list_parted2 FOR VALUES IN ('b', 'c');
-- check default partition overlap
INSERT INTO list_parted2 VALUES('X');
CREATE TABLE fail_part PARTITION OF list_parted2 FOR VALUES IN ('W', 'X', 'Y');

CREATE TABLE range_parted2 (
	a int
) PARTITION BY RANGE (a);

-- trying to create range partition with empty range
CREATE TABLE fail_part PARTITION OF range_parted2 FOR VALUES FROM (1) TO (0);
-- note that the range '[1, 1)' has no elements
CREATE TABLE fail_part PARTITION OF range_parted2 FOR VALUES FROM (1) TO (1);

CREATE TABLE part0 PARTITION OF range_parted2 FOR VALUES FROM (minvalue) TO (1);
CREATE TABLE fail_part PARTITION OF range_parted2 FOR VALUES FROM (minvalue) TO (2);
CREATE TABLE part1 PARTITION OF range_parted2 FOR VALUES FROM (1) TO (10);
CREATE TABLE fail_part PARTITION OF range_parted2 FOR VALUES FROM (9) TO (maxvalue);
CREATE TABLE part2 PARTITION OF range_parted2 FOR VALUES FROM (20) TO (30);
CREATE TABLE part3 PARTITION OF range_parted2 FOR VALUES FROM (30) TO (40);
CREATE TABLE fail_part PARTITION OF range_parted2 FOR VALUES FROM (10) TO (30);
CREATE TABLE fail_part PARTITION OF range_parted2 FOR VALUES FROM (10) TO (50);

-- Create a default partition for range partitioned table
CREATE TABLE range2_default PARTITION OF range_parted2 DEFAULT;

-- More than one default partition is not allowed, so this should give error
CREATE TABLE fail_default_part PARTITION OF range_parted2 DEFAULT;

-- Check if the range for default partitions overlap
INSERT INTO range_parted2 VALUES (85);
CREATE TABLE fail_part PARTITION OF range_parted2 FOR VALUES FROM (80) TO (90);
CREATE TABLE part4 PARTITION OF range_parted2 FOR VALUES FROM (90) TO (100);

-- now check for multi-column range partition key
CREATE TABLE range_parted3 (
	a int,
	b int
) PARTITION BY RANGE (a, (b+1));

CREATE TABLE part00 PARTITION OF range_parted3 FOR VALUES FROM (0, minvalue) TO (0, maxvalue);
CREATE TABLE fail_part PARTITION OF range_parted3 FOR VALUES FROM (0, minvalue) TO (0, 1);

CREATE TABLE part10 PARTITION OF range_parted3 FOR VALUES FROM (1, minvalue) TO (1, 1);
CREATE TABLE part11 PARTITION OF range_parted3 FOR VALUES FROM (1, 1) TO (1, 10);
CREATE TABLE part12 PARTITION OF range_parted3 FOR VALUES FROM (1, 10) TO (1, maxvalue);
CREATE TABLE fail_part PARTITION OF range_parted3 FOR VALUES FROM (1, 10) TO (1, 20);
CREATE TABLE range3_default PARTITION OF range_parted3 DEFAULT;

-- cannot create a partition that says column b is allowed to range
-- from -infinity to +infinity, while there exist partitions that have
-- more specific ranges
CREATE TABLE fail_part PARTITION OF range_parted3 FOR VALUES FROM (1, minvalue) TO (1, maxvalue);

-- check for partition bound overlap and other invalid specifications for the hash partition
CREATE TABLE hash_parted2 (
	a varchar
) PARTITION BY HASH (a);
CREATE TABLE h2part_1 PARTITION OF hash_parted2 FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE h2part_2 PARTITION OF hash_parted2 FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE h2part_3 PARTITION OF hash_parted2 FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE h2part_4 PARTITION OF hash_parted2 FOR VALUES WITH (MODULUS 8, REMAINDER 5);
-- overlap with part_4
CREATE TABLE fail_part PARTITION OF hash_parted2 FOR VALUES WITH (MODULUS 2, REMAINDER 1);
-- modulus must be greater than zero
CREATE TABLE fail_part PARTITION OF hash_parted2 FOR VALUES WITH (MODULUS 0, REMAINDER 1);
-- remainder must be greater than or equal to zero and less than modulus
CREATE TABLE fail_part PARTITION OF hash_parted2 FOR VALUES WITH (MODULUS 8, REMAINDER 8);

-- check schema propagation from parent

CREATE TABLE parted (
	a text,
	b int NOT NULL DEFAULT 0,
	CONSTRAINT check_a CHECK (length(a) > 0)
) PARTITION BY LIST (a);

CREATE TABLE part_a PARTITION OF parted FOR VALUES IN ('a');

-- only inherited attributes (never local ones)
SELECT attname, attislocal, attinhcount FROM pg_attribute
  WHERE attrelid = 'part_a'::regclass and attnum > 0
  ORDER BY attnum;

-- able to specify column default, column constraint, and table constraint

-- first check the "column specified more than once" error
CREATE TABLE part_b PARTITION OF parted (
	b NOT NULL,
	b DEFAULT 1,
	b CHECK (b >= 0),
	CONSTRAINT check_a CHECK (length(a) > 0)
) FOR VALUES IN ('b');

CREATE TABLE part_b PARTITION OF parted (
	b NOT NULL DEFAULT 1 CHECK (b >= 0),
	CONSTRAINT check_a CHECK (length(a) > 0)
) FOR VALUES IN ('b');
-- conislocal should be false for any merged constraints
SELECT conislocal, coninhcount FROM pg_constraint WHERE conrelid = 'part_b'::regclass AND conname = 'check_a';

-- specify PARTITION BY for a partition
CREATE TABLE fail_part_col_not_found PARTITION OF parted FOR VALUES IN ('c') PARTITION BY RANGE (c);
CREATE TABLE part_c PARTITION OF parted (b WITH OPTIONS NOT NULL DEFAULT 0) FOR VALUES IN ('c') PARTITION BY RANGE ((b));

-- create a level-2 partition
CREATE TABLE part_c_1_10 PARTITION OF part_c FOR VALUES FROM (1) TO (10);

-- Partition bound in describe output
\d+ part_b

-- Both partition bound and partition key in describe output
\d+ part_c

-- a level-2 partition's constraint will include the parent's expressions
\d+ part_c_1_10

-- Show partition count in the parent's describe output
-- Tempted to include \d+ output listing partitions with bound info but
-- output could vary depending on the order in which partition oids are
-- returned.
\d parted
\d hash_parted

-- check that we get the expected partition constraints
CREATE TABLE range_parted4 (a int, b int, c int) PARTITION BY RANGE (abs(a), abs(b), c);
CREATE TABLE unbounded_range_part PARTITION OF range_parted4 FOR VALUES FROM (MINVALUE, MINVALUE, MINVALUE) TO (MAXVALUE, MAXVALUE, MAXVALUE);
\d+ unbounded_range_part
DROP TABLE unbounded_range_part;
CREATE TABLE range_parted4_1 PARTITION OF range_parted4 FOR VALUES FROM (MINVALUE, MINVALUE, MINVALUE) TO (1, MAXVALUE, MAXVALUE);
\d+ range_parted4_1
CREATE TABLE range_parted4_2 PARTITION OF range_parted4 FOR VALUES FROM (3, 4, 5) TO (6, 7, MAXVALUE);
\d+ range_parted4_2
CREATE TABLE range_parted4_3 PARTITION OF range_parted4 FOR VALUES FROM (6, 8, MINVALUE) TO (9, MAXVALUE, MAXVALUE);
\d+ range_parted4_3
DROP TABLE range_parted4;

-- cleanup
DROP TABLE parted, list_parted, range_parted, list_parted2, range_parted2, range_parted3;
DROP TABLE hash_parted;
DROP TABLE hash_parted2;

-- comments on partitioned tables columns
CREATE TABLE parted_col_comment (a int, b text) PARTITION BY LIST (a);
COMMENT ON TABLE parted_col_comment IS 'Am partitioned table';
COMMENT ON COLUMN parted_col_comment.a IS 'Partition key';
SELECT obj_description('parted_col_comment'::regclass);
\d+ parted_col_comment
DROP TABLE parted_col_comment;

-- list partitioning on array type column
CREATE TABLE arrlp (a int[]) PARTITION BY LIST (a);
CREATE TABLE arrlp12 PARTITION OF arrlp FOR VALUES IN ('{1}', '{2}');
\d+ arrlp12
DROP TABLE arrlp;

-- partition on boolean column
create table boolspart (a bool) partition by list (a);
create table boolspart_t partition of boolspart for values in (true);
create table boolspart_f partition of boolspart for values in (false);
\d+ boolspart
drop table boolspart;

-- partitions mixing temporary and permanent relations
create table perm_parted (a int) partition by list (a);
create temporary table temp_parted (a int) partition by list (a);
create table perm_part partition of temp_parted default; -- error
create temp table temp_part partition of perm_parted default; -- error
create temp table temp_part partition of temp_parted default; -- ok
drop table perm_parted cascade;
drop table temp_parted cascade;

-- OPENTENBASE
drop function if exists create_multi_tables1(integer, varchar);
CREATE OR REPLACE FUNCTION create_multi_tables1(table_num_in integer, table_sql varchar) RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE
AS $BODY$
declare
    v_idx integer := 0;
    v_strTable varchar :='';
    v_strSql varchar :='';
begin
    while v_idx < table_num_in loop
        v_idx = v_idx+1;
        v_strTable = CONCAT('simple_metadata_query_', v_idx);
        v_strSql = table_sql||' '||v_strTable||'(c1 bigint, c31 smallserial);';
        RAISE NOTICE 'create %', v_strTable;
        BEGIN
            EXECUTE v_strSql;
            EXCEPTION when others then
            raise notice 'ERROR: (%)', SQLERRM;
        end;
    end loop;

    RAISE NOTICE 'finished .....';
end
$BODY$;

drop function if exists del_multi_table1(varchar);
CREATE FUNCTION del_multi_table1(table_sql varchar) RETURNS void AS $$
DECLARE
    tmp VARCHAR(512);
DECLARE names CURSOR FOR
    select tablename from pg_tables where tablename like 'simple_metadata_query_%';
BEGIN
  FOR stmt IN names LOOP
    tmp := table_sql||' '|| quote_ident(stmt.tablename) || ' CASCADE;';
    RAISE NOTICE '%', tmp;
    BEGIN
        EXECUTE tmp;
        EXCEPTION when others then
        raise notice 'ERROR: (%)', SQLERRM;
    end;
  END LOOP;
  RAISE NOTICE 'finished .....';
END
$$ LANGUAGE plpgsql VOLATILE COST 100;

CREATE TABLE simple_metadata_query_3(c1 int, c31 smallserial);
SELECT create_multi_tables1(5, 'create table');
SELECT del_multi_table1('drop table if exists');

DROP FUNCTION create_multi_tables1;
DROP FUNCTION del_multi_table1;

-- 1020421696090509239
drop table if exists t_src;
drop procedure if exists p_test(inout res varchar);
create table t_src
       (empno numeric(4) constraint pk_emp primary key,
        ename varchar(10),
        job varchar(9),
        mgr numeric(4),
        hiredate date,
        sal numeric(7,2),
        comm numeric(7,2),
        deptno numeric(2));
insert into t_src values
(7369,'smith','clerk',7902,to_date('17-12-1980','dd-mm-yyyy'),800,null,20);
insert into t_src values
(7499,'allen','salesman',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,30);
insert into t_src values
(7521,'ward','salesman',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,30);
insert into t_src values
(7566,'jones','manager',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,null,20);
insert into t_src values
(7654,'martin','salesman',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,30);
insert into t_src values
(7698,'blake','manager',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,null,30);
insert into t_src values
(7782,'clark','manager',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,null,10);
insert into t_src values
(7788,'scott','analyst',7566,to_date('19-04-1987','dd-mm-yyyy'),3000,null,20);
insert into t_src values
(7839,'king','president',null,to_date('17-11-1981','dd-mm-yyyy'),5000,null,10);
insert into t_src values
(7844,'turner','salesman',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,30);
insert into t_src values
(7876,'adams','clerk',7788,to_date('23-05-1987', 'dd-mm-yyyy'),1100,null,20);
insert into t_src values
(7900,'james','clerk',7698,to_date('3-12-1981','dd-mm-yyyy'),950,null,30);
insert into t_src values
(7902,'ford','analyst',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,null,20);
insert into t_src values
(7934,'miller','clerk',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,null,10);
create or replace procedure p_test(inout res varchar)
    language plpgsql
    as $$
declare
num numeric ;
v1 t_src.empno%type;
begin
    res:='ok';
    drop table if exists t_dst;
    create table t_dst as select empno,sal from t_src;
    select empno from t_src where empno='7788' into v1;
    raise notice '%',v1;
exception
    when others then
        raise notice '%',sqlerrm;
end;
$$;
call p_test('');
select count(*) from t_src;
select count(*) from t_dst;
drop procedure p_test(inout res varchar);

-- The original snapshot status must be maintained in CTAS process snapshot replacement.
drop function if exists f_ctas_subabort(last_number numeric );
create or replace function f_ctas_subabort(last_number numeric ) returns varchar
    language plpgsql
    as $$
declare
v1 numeric ;
res varchar(20):='ok';
begin
   drop table if exists t_dst_subabort;
   create table t_dst_subabort as select * from t_src;
   RAISE EXCEPTION 'xxx > 0' USING ERRCODE = 'division_by_zero';
   return res;
exception
    when others then
        raise notice '%',sqlerrm;
        return null;
end;
$$;
select f_ctas_subabort(1);
drop function f_ctas_subabort(last_number numeric );
drop table t_src;
drop table t_dst;

-- https://github.com/Tencent/OpenTenBase/issues/83
drop table if exists r;
drop table if exists  r2;
drop table if exists  s;
Drop table if exists  tb1;
drop table if exists  mv_r_s_1;
drop table if exists  mv_r_s_3;
Drop FUNCTION if exists  getr_text(id integer);
Drop FUNCTION if exists  getmaincatalog_tst();
create table r distribute by replication as select i i, i || '_val'::text itxt from generate_series(1,5) i;
create table r2 distribute by replication as select i i, i || '_val'::text itxt from generate_series(1,5) I;
CREATE OR REPLACE FUNCTION getr_text(id integer)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE
 parallel safe
AS $function$
    select itxt from r2 where i = id;
$function$;
create table s (id int primary key, val text) distribute by shard(id) ;
insert into s  select id, id|| '_val' idval from generate_series(1,3) id;
Select count(*) from s;
-- this works
create  table mv_r_s_1 as 
select s.*,r.*, getr_text(r.i),  getr_text(s.id) d2 from
r right outer join s on (getr_text(r.i) = s.val) limit 0;
Select count(*) from mv_r_s_1;

create table tb1(a int);
insert into tb1 select generate_series(1,10);
CREATE OR REPLACE FUNCTION getmaincatalog_tst()
 RETURNS int
 LANGUAGE sql
 immutable PARALLEL SAFE
AS $function$  select a  from tb1;  $function$;
Select count(*) from tb1;
--- However if we use a function that does lookup on some well established table, we get the above error:
create  table mv_r_s_3 as
select s.*,r.*, getr_text(r.i),  getr_text(s.id) d2 from
r right outer join s on (getr_text(r.i) = s.val or 
getmaincatalog_tst() = i % 4);
Select count(*) from mv_r_s_3;

drop table r;
drop table r2;
drop table s;
Drop table tb1;
drop table mv_r_s_1;
drop table mv_r_s_3;
Drop FUNCTION getr_text(id integer);
Drop FUNCTION getmaincatalog_tst();
-- ctas in procedure support params
drop table if exists t_src;
drop table if exists t_dst;
create table t_src(a int, b int);
insert into t_src values(1,1),(2,2),(3,3),(4,4);
select * from t_src order by a;
create table t_dst as select * from t_src where a > 1 and b != 4 - 1;
select * from t_dst order by a;
drop table t_dst;
DROP FUNCTION if exists f1(integer,integer);
create or replace function f1(pa int, pb int)
RETURNS void
language plpgsql
    as $$
declare
	tmp_a int;
	tmp_b int;
	v_sql varchar(2000);
begin
	tmp_a := pa;
	tmp_b := pb -1;

	v_sql := 'drop table if exists t_dst ';
    EXECUTE v_sql;
	
	create table t_dst as select * from t_src where a > tmp_a and b != tmp_b;
exception
    when others then
        raise notice '%',sqlerrm;
end;
$$;
select * from f1(1, 4);
select * from t_dst order by a;
drop table t_src;
drop table t_dst;
DROP FUNCTION f1(integer,integer);

-- comment on column of temp table
CREATE TEMP TABLE comment_t1(c0 int, c1 smallint);
COMMENT ON COLUMN comment_t1.c0 IS NULL;
COMMENT ON COLUMN comment_t1.c0 IS 'comment on temp table';
select description from pg_description, pg_class where objoid = pg_class.oid and relname='comment_t1';
Drop TABLE comment_t1;

-- statistic on temp table
CREATE TEMPORARY TABLE IF NOT EXISTS stat_t0(c0 smallint  UNIQUE NULL,
  c1 bigserial CHECK (((length((((('' COLLATE "C"))||(TRUE)) COLLATE "C")))!=(stat_t0.c1))) NO INHERIT UNIQUE,
  UNIQUE(c1) WITH (autovacuum_vacuum_threshold=1554318410, autovacuum_enabled=1, autovacuum_vacuum_scale_factor=0.5, autovacuum_analyze_threshold=351112419, autovacuum_freeze_min_age=646736889, autovacuum_analyze_scale_factor=0.9, autovacuum_freeze_max_age=975613214))
  ON COMMIT PRESERVE ROWS ;
CREATE STATISTICS IF NOT EXISTS s0 ON c0, c1 FROM stat_t0;
DROP STATISTICS s0;

--
-- test function pg_get_tabledef()
--
CREATE TABLE t1_tbl_def(c1 int, c2 text);
SELECT pg_get_tabledef(oid) FROM pg_class WHERE relname = 't1_tbl_def';
SELECT pg_get_tabledef('t1_tbl_def');
DROP TABLE t1_tbl_def;

--
-- test function pg_get_tabledef of partition table
--
create table pkslow_person_r (age int not null, city varchar not null) partition by RANGE (age);
create table pkslow_person_r1 partition of pkslow_person_r for values from (MINVALUE) to (10);
create table pkslow_person_r2 partition of pkslow_person_r for values from (11) to (20); 
create table pkslow_person_r3 partition of pkslow_person_r for values from (21) to (MAXVALUE); 
select pg_get_tabledef('pkslow_person_r');
select pg_get_tabledef('pkslow_person_r1');
select pg_get_tabledef('pkslow_person_r2');
select pg_get_tabledef('pkslow_person_r3');
create table pkslow_person_l (age int not null, city varchar not null) PARTITION BY list (city); 
create table pkslow_person_l1 PARTITION OF pkslow_person_l FOR VALUES IN ('GZ');  
create table pkslow_person_l2 PARTITION OF pkslow_person_l FOR VALUES IN ('BJ');  
create table pkslow_person_l3 PARTITION OF pkslow_person_l DEFAULT; 
select pg_get_tabledef('pkslow_person_l');
select pg_get_tabledef('pkslow_person_l1');
select pg_get_tabledef('pkslow_person_l2');
select pg_get_tabledef('pkslow_person_l3');
create table pkslow_person_h (age int not null, city varchar not null) partition by hash (city); 
create table pkslow_person_h1 partition of pkslow_person_h for values with (modulus 4, remainder 0);  
create table pkslow_person_h2 partition of pkslow_person_h for values with (modulus 4, remainder 1);  
create table pkslow_person_h3 partition of pkslow_person_h for values with (modulus 4, remainder 2);  
create table pkslow_person_h4 partition of pkslow_person_h for values with (modulus 4, remainder 3); 
select pg_get_tabledef('pkslow_person_h');
select pg_get_tabledef('pkslow_person_h1');
select pg_get_tabledef('pkslow_person_h2');
select pg_get_tabledef('pkslow_person_h3');
select pg_get_tabledef('pkslow_person_h4');
drop table pkslow_person_r;
drop table pkslow_person_l;
drop table pkslow_person_h;

-- check that NOT NULL and default value are inherited correctly
create table parted_notnull_inh_test (a int default 1, b int not null default 0) partition by list (a);
create table parted_notnull_inh_test1 partition of parted_notnull_inh_test (a not null, b default 1) for values in (1);
insert into parted_notnull_inh_test (b) values (null);
-- note that while b's default is overriden, a's default is preserved
\d parted_notnull_inh_test1
drop table parted_notnull_inh_test;

-- check for a conflicting COLLATE clause
create table parted_collate_must_match (a text collate "C", b text collate "C")
  partition by range (a);
-- on the partition key
create table parted_collate_must_match1 partition of parted_collate_must_match
  (a collate "POSIX") for values from ('a') to ('m');
-- on another column
create table parted_collate_must_match2 partition of parted_collate_must_match
  (b collate "POSIX") for values from ('m') to ('z');
drop table parted_collate_must_match;
