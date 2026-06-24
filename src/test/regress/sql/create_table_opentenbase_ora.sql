\c regression_ora
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

-- test: Add testcases for cherry-pick from fc5494d26d3a6b271ec26956c6cdcb0847a0402f in v5

CREATE TABLE rel_test_create_table_20231016_1 (
    id int,
    name varchar2(20),
    nc clob
) pctfree 10 pctused 40 initrans 3 MAXTRANS 255 tablespace pg_default storage(initial 1m next 2m);

CREATE TABLE rel_test_create_table_20231016_2 pctfree 10 PCTUSED 60 initrans 3
as select * from rel_test_create_table_20231016_1;

CREATE TABLE rel_test_create_table_20231016_3(id int, name varchar2(20), nc clob)
pctfree 10 pctused 40.0;
CREATE TABLE rel_test_create_table_20231016_4(id int, name varchar2(20), nc clob)
storage( pctincrease 10.00);
CREATE INDEX rel_test_create_table_20231016_3_IDX ON
rel_test_create_table_20231016_3 (id, name) STORAGE (initial 0.0);
DROP INDEX rel_test_create_table_20231016_3_IDX;
CREATE INDEX rel_test_create_table_20231016_3_IDX ON
rel_test_create_table_20231016_3 (id, name) STORAGE (next 200.00);
DROP INDEX rel_test_create_table_20231016_3_IDX;
CREATE INDEX rel_test_create_table_20231016_3_IDX ON
rel_test_create_table_20231016_3 (id, name) STORAGE (maxsize 200.00);

CREATE INDEX rel_test_create_table_20231016_4_IDX ON
rel_test_create_table_20231016_4(id, name) storage(initial 0 maxsize 10000000000);
DROP INDEX IF EXISTS rel_test_create_table_20231016_4_IDX;
CREATE INDEX rel_test_create_table_20231016_4_IDX ON
rel_test_create_table_20231016_4(id, name) storage(initial   10000000000);
DROP INDEX IF EXISTS rel_test_create_table_20231016_4_IDX;
CREATE INDEX rel_test_create_table_20231016_4_IDX ON
rel_test_create_table_20231016_4 (id, name) storage(next 10000000000);
DROP TABLE rel_test_create_table_20231016_1;
DROP TABLE rel_test_create_table_20231016_2;
DROP TABLE rel_test_create_table_20231016_3;
DROP TABLE rel_test_create_table_20231016_4;

--
--  Fix duplicate index construction of rowid in partition table.
--

drop table RQG_TABLE_20240123_5;

CREATE TABLE "public".RQG_TABLE_20240123_5 (
    C0 INTEGER,
    C1 INTEGER,
    C2 CHARACTER VARYING(2000),
    C3 CHARACTER VARYING(2000),
    C4 DATE,
    C5 DATE,
    C6 TIMESTAMP(6) WITHOUT TIME ZONE,
    C7 TIMESTAMP(6) WITHOUT TIME ZONE,
    C8 TIME WITHOUT TIME ZONE,
    C9 TIME WITHOUT TIME ZONE
)
PARTITION BY HASH (C8)
WITH (checksum='on')
DISTRIBUTE BY HASH (C0);

CREATE TABLE "public".RQG_TABLE_20240123_5_P0 (
    C0 INTEGER,
    C1 INTEGER,
    C2 CHARACTER VARYING(2000),
    C3 CHARACTER VARYING(2000),
    C4 DATE,
    C5 DATE,
    C6 TIMESTAMP(6) WITHOUT TIME ZONE,
    C7 TIMESTAMP(6) WITHOUT TIME ZONE,
    C8 TIME WITHOUT TIME ZONE,
    C9 TIME WITHOUT TIME ZONE
)
WITH (checksum='on');
ALTER TABLE ONLY "public".RQG_TABLE_20240123_5 ATTACH PARTITION "public".RQG_TABLE_20240123_5_P0 FOR VALUES WITH (modulus 4, remainder 0);

drop table RQG_TABLE_20240123_5;
