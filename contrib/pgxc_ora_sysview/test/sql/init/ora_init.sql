/*
 * Must be executed before all regression test.
 */
-- CREATE EXTENSION pgxc_ora_sysview;
-- Create all objects used in the regression tests
CREATE SCHEMA jynnn;
CREATE SCHEMA zjaaa;

CREATE USER jynnuser;  -- owner of schema jynnn
CREATE USER zjaauser;  -- Only able to access some objects under the schema jynnn. 

ALTER SCHEMA jynnn OWNER TO jynnuser;
ALTER SCHEMA zjaaa OWNER TO zjaauser;

----
-- Name: t_ora_zjtab; Type: TABLE; Schema: zjaaa; Owner: zjaauser
----
CREATE TABLE zjaaa.t_ora_zjtab (
    a serial PRIMARY KEY,   -- dependent sequence test
    b int NOT NULL,
    c varchar(20) DEFAULT 'def',
    d int
);  -- Test automatic DISTRIBUTE BY SHARD,default a.

COMMENT ON TABLE zjaaa.t_ora_zjtab IS 'This is a comment for the table.';
COMMENT ON COLUMN zjaaa.t_ora_zjtab.a IS 'This is a comment for the column.';
COMMENT ON COLUMN zjaaa.t_ora_zjtab.b IS 'This is a comment for the column.';

ALTER TABLE zjaaa.t_ora_zjtab OWNER TO zjaauser;

----
-- Name: t_ora_zjpart; Type: Partition Table; Schema: zjaaa; Owner: zjaauser
----
CREATE TABLE zjaaa.t_ora_zjpart
(
    a int NOT NULL,
    b timestamp NOT NULL,
    c varchar(20),
    PRIMARY KEY(a)
) 
PARTITION BY RANGE (b) 
BEGIN (timestamp without time zone '2024-01-01 0:0:0') 
STEP (interval '1 month') PARTITIONS (3) 
DISTRIBUTE BY SHARD(a) 
TO GROUP default_group;

COMMENT ON TABLE zjaaa.t_ora_zjpart IS 'This is a comment for the table.';
COMMENT ON COLUMN zjaaa.t_ora_zjpart.a IS 'This is a comment for the column.';
COMMENT ON COLUMN zjaaa.t_ora_zjpart.b IS 'This is a comment for the column.';

ALTER TABLE zjaaa.t_ora_zjpart OWNER TO zjaauser;

----
-- Name: t_ora_jyinhp; Type: TABLE; Schema: jynnn; Owner: jynnuser
----
CREATE TABLE jynnn.t_ora_jyinhp (
    a serial PRIMARY KEY,   -- dependent sequence test
    b int NOT NULL,
    c varchar(20) DEFAULT 'def',
    d int
) WITH (
    autovacuum_enabled = false,            
    fillfactor = 70,            
    autovacuum_vacuum_scale_factor = 0.2,  
    autovacuum_analyze_scale_factor = 0.1  
)DISTRIBUTE BY SHARD (a);

COMMENT ON TABLE jynnn.t_ora_jyinhp IS 'This is a comment for the table.';
COMMENT ON COLUMN jynnn.t_ora_jyinhp.a IS 'This is a comment for the column.';
COMMENT ON COLUMN jynnn.t_ora_jyinhp.b IS 'This is a comment for the column.';

CREATE INDEX perf_index ON jynnn.t_ora_jyinhp (c); -- dependent index test

ALTER TABLE jynnn.t_ora_jyinhp OWNER TO jynnuser;

----
-- Name: t_ora_jyinhp_child; Type: TABLE; Schema: jynnn; Owner: jynnuser
----
CREATE TABLE jynnn.t_ora_jyinhp_child (
    id int,
    CONSTRAINT "Fk_customer" FOREIGN KEY (id) REFERENCES jynnn.t_ora_jyinhp (a) -- dependent constranit test
)DISTRIBUTE BY SHARD (id);

ALTER TABLE jynnn.t_ora_jyinhp_child ADD CONSTRAINT child_uniq UNIQUE(id);

ALTER TABLE jynnn.t_ora_jyinhp_child OWNER TO jynnuser;

----
-- Name: t_ora_sales; Type: Partition Table; Schema: jynnn; Owner: jynnuser
----
CREATE TABLE IF NOT EXISTS jynnn.t_ora_sales  -- test inherits
(
    sale_id integer NOT NULL,
    "Sale date" date NOT NULL,
    amount numeric,
    CONSTRAINT t_ora_sales_pkey PRIMARY KEY (sale_id, "Sale date")
) PARTITION BY RANGE ("Sale date");

CREATE TABLE jynnn.t_ora_sales_february PARTITION OF jynnn.t_ora_sales
    FOR VALUES FROM ('2023-02-01') TO ('2023-03-01');

CREATE TABLE jynnn.t_ora_sales_january PARTITION OF jynnn.t_ora_sales
    FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');

ALTER TABLE jynnn.t_ora_sales OWNER TO jynnuser;

----
-- Name: "Sample unlogged table"; Type: Unlogged Table; Schema: jynnn; Owner: jynnuser
----
CREATE UNLOGGED TABLE jynnn."Sample unlogged table" (
    id serial PRIMARY KEY,
    "name of customer" varchar(255),    -- quote ident test
    "Age" integer,
    "Birth_Date" DATE,
    CONSTRAINT "check" CHECK ("Birth_Date" > '1900-01-01')
);

COMMENT ON COLUMN jynnn."Sample unlogged table"."name of customer" IS 'This is a comment for the column.';

CREATE INDEX "Gin_Index_Name" ON jynnn."Sample unlogged table" USING spgist ("name of customer");

ALTER TABLE jynnn."Sample unlogged table" ADD CONSTRAINT "Unique age" UNIQUE(id,"Age"); 

ALTER TABLE jynnn."Sample unlogged table" OWNER TO jynnuser;

----
-- Name: t_ora_view1; Type: VIEW; Schema: jynnn; Owner: jynnuser
----
CREATE OR REPLACE VIEW jynnn.t_ora_view1 AS
SELECT
    t_ora_jyinhp.a
FROM
    jynnn.t_ora_jyinhp;

ALTER VIEW jynnn.t_ora_view1 OWNER TO jynnuser;

----
-- Name: "Global fines"; Type: VIEW; Schema: jynnn; Owner: jynnuser
----
CREATE OR REPLACE VIEW jynnn."Global fines" AS
SELECT
    "Sample unlogged table"."name of customer"
FROM
    jynnn."Sample unlogged table";

ALTER VIEW jynnn."Global fines" OWNER TO jynnuser;

----
-- Name: t_ora_mview; Type: MATERIALIZED VIEW; Schema: jynnn; Owner: jynnuser
----

CREATE MATERIALIZED VIEW jynnn.t_ora_mview AS 
SELECT
    t_ora_jyinhp.a
FROM
    jynnn.t_ora_jyinhp;

ALTER MATERIALIZED VIEW jynnn.t_ora_mview OWNER TO jynnuser;

----
-- Name: t_ora_double_salary; Type: FUNCTION; Schema: jynnn; Owner: jynnuser
----
CREATE OR REPLACE FUNCTION jynnn.t_ora_double_salary ()
    RETURNS TRIGGER
    AS $$
BEGIN
    NEW.b = NEW.b * 2;
    RETURN NEW;
END;
$$
LANGUAGE plpgsql;

ALTER FUNCTION jynnn.t_ora_double_salary OWNER TO jynnuser;

----
-- Name: "t_ora_order"; Type: TRIGGER; Schema: jynnn; Owner: jynnuser
----
CREATE TRIGGER "t_ora_order"
    BEFORE INSERT ON jynnn.t_ora_jyinhp
    FOR EACH ROW
    EXECUTE PROCEDURE jynnn.t_ora_double_salary();

----
-- Name: t_ora_edit_text; Type: FUNCTION; Schema: jynnn; Owner: jynnuser
----
CREATE OR REPLACE FUNCTION jynnn.t_ora_edit_text()
RETURNS TRIGGER AS $$
BEGIN
    NEW.c = NEW.c || 'edit';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION jynnn.t_ora_edit_text OWNER TO jynnuser;

----
-- Name: t_tri_edit_text; Type: TRIGGER; Schema: jynnn; Owner: jynnuser
----
CREATE TRIGGER t_tri_edit_text
BEFORE INSERT ON jynnn.t_ora_jyinhp
FOR EACH ROW
EXECUTE PROCEDURE jynnn.t_ora_edit_text();

----
-- Name: "attach Line"; Type: Sequence; Schema: jynnn; Owner: jynnuser
----
CREATE SEQUENCE IF NOT EXISTS jynnn."attach Line"
INCREMENT 1 START 1
MINVALUE 1
MAXVALUE 9223372036854775807
CACHE 1;

ALTER SEQUENCE jynnn."attach Line" OWNER TO jynnuser;

----
-- Name: t_ora_location; Type: Type; Schema: jynnn; Owner: jynnuser
----
CREATE TYPE jynnn.t_ora_location AS (
    lat double precision,
    lon double precision
);

ALTER TYPE jynnn.t_ora_location OWNER TO jynnuser;

----
-- Name: "T_Ora_Address"; Type: Type; Schema: jynnn; Owner: jynnuser
----
CREATE TYPE jynnn."T_Ora_Address" AS (
    "City" character varying,
    loc jynnn.t_ora_location,
    pin bigint
);

ALTER TYPE jynnn."T_Ora_Address" OWNER TO jynnuser;

----
-- Name: "T Merge objects"; Type: FUNCTION; Schema: jynnn; Owner: jynnuser
----
CREATE OR REPLACE FUNCTION jynnn."T Merge objects" ("First_val" text, actual_finder jynnn."T_Ora_Address")
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100 VOLATILE PARALLEL UNSAFE
    AS $BODY$
BEGIN
    RETURN "First_val" || actual_finder."City";
END
$BODY$;


ALTER FUNCTION jynnn."T Merge objects" OWNER TO jynnuser;

----
-- GRANT
----
GRANT USAGE ON SCHEMA jynnn TO zjaauser;
GRANT SELECT ON TABLE jynnn.t_ora_jyinhp_child TO zjaauser;
GRANT EXECUTE ON FUNCTION jynnn.t_ora_edit_text() TO zjaauser;
