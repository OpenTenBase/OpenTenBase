\c opentenbase_ora_package_function_regression_ora
set client_min_messages TO error;
CREATE EXTENSION IF NOT EXISTS opentenbase_ora_package_function;
set client_min_messages TO default;

-- case 1: target list
CREATE TYPE ProductType_20240708 AS
(
	id          NUMBER,
	name        VARCHAR2(15),
	description VARCHAR2(22),
	price       NUMBER(5, 2),
	days_valid  NUMBER
)
/

CREATE TABLE object_products_20240708 OF ProductType_20240708
/

INSERT INTO object_products_20240708 (
	id, name, description, price, days_valid
) VALUES (
	2, 'AAA', 'BBB', 2.99, 5
);

select * from object_products_20240708;
SELECT VALUE(op) FROM object_products_20240708 op;

drop table object_products_20240708;
drop type ProductType_20240708;

-- case 3: select into
call dbms_output.serveroutput(true);
CREATE TYPE value_udt_20240806 AS (
	id NUMBER,
	name VARCHAR2(100),
	dept_id number
);
CREATE TABLE value_table_udt_20240806 OF value_udt_20240806;
INSERT INTO value_table_udt_20240806(id,name,dept_id) VALUES (1,'n1',1000);
INSERT INTO value_table_udt_20240806(id,name,dept_id) VALUES (2,'n2',1000);

declare
	vRow value_udt_20240806;
	vNumber number;
begin
	call dbms_output.put_line('________');
	vRow.id = 1;
	vRow.name = 'n1';
	vRow.dept_id = 1000;
	select id into vNumber from value_table_udt_20240806 p where value(p) = vRow;
	call dbms_output.put_line('Matched:'||vNumber);

	select value(p) into vRow  from value_table_udt_20240806 p  where p.id = 1;
	call dbms_output.put_line('Matched2:'|| vRow.id);
end;
/
drop table value_table_udt_20240806 cascade;
drop type value_udt_20240806 cascade;

-- case 4: insert ... select value
CREATE TYPE value_udt_20240806 AS (
	id NUMBER,
	name VARCHAR2(100),
	dept_id number,
	salary number
);
CREATE TABLE value_table_udt_20240806 OF value_udt_20240806;
INSERT INTO value_table_udt_20240806(id,name,dept_id,salary) VALUES (1,'n1',1000,10000);
INSERT INTO value_table_udt_20240806(id,name,dept_id,salary) VALUES (2,'n2',1000,20000);
insert into value_table_udt_20240806 select value_udt_20240806(id, name, dept_id, salary) from value_table_udt_20240806 p where id = 1;
insert into value_table_udt_20240806 select value(p) from value_table_udt_20240806 p where id = 1;
drop table value_table_udt_20240806 cascade;
drop type value_udt_20240806 cascade;

-- case 5: view
CREATE  TYPE value_udt_all_types_20240808 AS (
	id NUMBER,
	pid number,
	p1_char char(100),
	p2_varchar varchar2(100),
	p3_nchar nchar(100),
	p4_nvarchar2 nvarchar2(100),
	p5_float float,
	p6_date date,
	p7_timestamp timestamp,
	p8_timestamp_local_timezone timestamp with time zone,
	p9_interval_year_to_month interval year to month,
	p10_interval_day_to_second interval day to second
);
CREATE TABLE value_table_udt_all_types_20240808 OF value_udt_all_types_20240808;
create view v_value_func_test_20240808 as select value(p) col from value_table_udt_all_types_20240808 p;
\d v_value_func_test_20240808;
drop view v_value_func_test_20240808;
drop table value_table_udt_all_types_20240808;
drop type value_udt_all_types_20240808;
