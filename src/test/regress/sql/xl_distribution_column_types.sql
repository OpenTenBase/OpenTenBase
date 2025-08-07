--Supported types for distribution column types - INT8, INT2, OID, INT4, BOOL, INT2VECTOR, OIDVECTOR, CHAR, NAME, TEXT, BPCHAR, BYTEA, VARCHAR, NUMERIC, MONEY, ABSTIME, RELTIME, DATE, TIME, TIMESTAMP, TIMESTAMPTZ, INTERVAL, and TIMETZ

--INT8, 

CREATE TABLE xl_dc (
    product_no integer,
    product_id INT8 PRIMARY KEY,
    name text,
    price numeric
) DISTRIBUTE BY HASH (product_id);

--INT2, 


CREATE TABLE xl_dc1 (
    product_no integer,
    product_id INT2 PRIMARY KEY,
    name text,
    price numeric
) DISTRIBUTE BY HASH (product_id);

--OID, 
CREATE TABLE xl_dc2 (
    product_no integer,
    product_id OID PRIMARY KEY,
    name text,
    price numeric
) DISTRIBUTE BY HASH (product_id);

--INT4, 
CREATE TABLE xl_dc3 (
    product_no integer,
    product_id INT4 PRIMARY KEY,
    name text,
    price numeric
) DISTRIBUTE BY HASH (product_id);

--BOOL, 

CREATE TABLE xl_dc4 (
    product_no integer,
    is_available BOOL PRIMARY KEY,
    name text,
    price numeric
) DISTRIBUTE BY HASH (is_available);

--OIDVECTOR, 
CREATE TABLE xl_dc6 (
    product_no integer,
    product_id integer,
    sub_product_ids OIDVECTOR PRIMARY KEY, 
    name text,
    price numeric
) DISTRIBUTE BY HASH (sub_product_ids);

--CHAR, 
CREATE TABLE xl_dc7 (
    product_no integer,
    product_group CHAR PRIMARY KEY,
    name text,
    price numeric
) DISTRIBUTE BY HASH (product_group);

--NAME, 
CREATE TABLE xl_dc8 (
    product_no integer,
    product_name NAME PRIMARY KEY,
    name text,
    price numeric
) DISTRIBUTE BY HASH (product_name);

--TEXT, 
CREATE TABLE xl_dc9 (
    product_no integer,
    product_name TEXT PRIMARY KEY,
    name text,
    price numeric
) DISTRIBUTE BY HASH (product_name);


--BPCHAR - blank padded char, 
CREATE TABLE xl_dc10 (
    product_no integer,
    product_group BPCHAR PRIMARY KEY,
    name text,
    price numeric
) DISTRIBUTE BY HASH (product_group);

--BYTEA - variable length binary string, 
CREATE TABLE xl_dc11 (
    product_no integer,
    product_group BYTEA PRIMARY KEY,
    name text,
    price numeric
) DISTRIBUTE BY HASH (product_group);

--VARCHAR, 
CREATE TABLE xl_dc12 (
    product_no integer,
    product_group VARCHAR PRIMARY KEY,
    name text,
    price numeric
) DISTRIBUTE BY HASH (product_group);

--NUMERIC, 
CREATE TABLE xl_dc15 (
    product_no integer,
    product_id NUMERIC PRIMARY KEY,
    name text,
    price numeric
) DISTRIBUTE BY HASH (product_id);

--MONEY - String datatype, 
CREATE TABLE xl_dc16 (
    product_no integer,
    product_id NUMERIC ,
    name MONEY PRIMARY KEY,
    price numeric
) DISTRIBUTE BY HASH (name);

--ABSTIME, 
CREATE TABLE xl_dc17 (
    product_no integer,
    product_id NUMERIC ,
    purchase_date ABSTIME PRIMARY KEY,
    price numeric
) DISTRIBUTE BY HASH (purchase_date);

--RELTIME, 
CREATE TABLE xl_dc18 (
    product_no integer,
    product_id NUMERIC ,
    name MONEY,
    purchase_date RELTIME PRIMARY KEY,
    price numeric
) DISTRIBUTE BY HASH (purchase_date);


--DATE, 
CREATE TABLE xl_dc19 (
    product_no integer,
    product_id NUMERIC ,
    name MONEY,
    purchase_date DATE PRIMARY KEY,
    price numeric
) DISTRIBUTE BY HASH (purchase_date);

--TIME, 
CREATE TABLE xl_dc20 (
    product_no integer,
    product_id NUMERIC ,
    name MONEY,
    purchase_date TIME PRIMARY KEY,
    price numeric
) DISTRIBUTE BY HASH (purchase_date);

--TIMESTAMP,

CREATE TABLE xl_dc21 (
    product_no integer,
    product_id NUMERIC ,
    name MONEY,
    purchase_date TIMESTAMP PRIMARY KEY,
    price numeric
) DISTRIBUTE BY HASH (purchase_date); 

--TIMESTAMPTZ, 

CREATE TABLE xl_dc22 (
    product_no integer,
    product_id NUMERIC ,
    name MONEY,
    purchase_date TIMESTAMPTZ PRIMARY KEY,
    price numeric
) DISTRIBUTE BY HASH (purchase_date); 

--INTERVAL, 
CREATE TABLE xl_dc23 (
    product_no integer,
    product_id NUMERIC ,
    name MONEY,
    purchase_date INTERVAL PRIMARY KEY,
    price numeric
) DISTRIBUTE BY HASH (purchase_date); 

--and TIMETZ - time along with time zone
CREATE TABLE xl_dc24 (
    product_no integer,
    product_id NUMERIC ,
    name MONEY,
    purchase_date TIMETZ PRIMARY KEY,
    price numeric
) DISTRIBUTE BY HASH (purchase_date); 

--Distribution strategy can specify on a single column
CREATE TABLE xl_dc25 (
    product_no integer,
    product_id NUMERIC ,
    name MONEY,
    purchase_date TIMETZ,
    price numeric,
    primary key(product_no, product_id)
) DISTRIBUTE BY HASH (product_no, product_id); --fail

-- Distribution column value cannot be updated
-- default distributed on HASH by primary key column, i.e. city
CREATE TABLE xl_dc_weather (
    city            varchar(80) PRIMARY KEY,
    temp_lo         int,           -- low temperature
    temp_hi         int,           -- high temperature
    prcp            real,          -- precipitation
    date            date
);

INSERT INTO xl_dc_weather VALUES ('San Francisco', 46, 50, 0.25, '1994-11-27');

UPDATE xl_dc_weather SET city = 'SFO' where temp_lo=46 and temp_hi=50; -- fail


DROP TABLE xl_dc;
DROP TABLE xl_dc1;
DROP TABLE xl_dc2;
DROP TABLE xl_dc3;
DROP TABLE xl_dc4;
DROP TABLE xl_dc6;
DROP TABLE xl_dc7;
DROP TABLE xl_dc8;
DROP TABLE xl_dc9;
DROP TABLE xl_dc10;
DROP TABLE xl_dc11;
DROP TABLE xl_dc12;
DROP TABLE xl_dc15;
DROP TABLE xl_dc16;
DROP TABLE xl_dc17;
DROP TABLE xl_dc18;
DROP TABLE xl_dc19;
DROP TABLE xl_dc20;
DROP TABLE xl_dc21;
DROP TABLE xl_dc22;
DROP TABLE xl_dc23;
DROP TABLE xl_dc24;
DROP TABLE xl_dc25;
DROP TABLE xl_dc_weather;

CREATE TABLE xl_dc26(c1 int, c2 clob) distribute by shard(c2);
INSERT INTO xl_dc26 VALUES(0, 'xxx'::clob), (1, 'xxx'::clob);
SELECT * FROM xl_dc26 ORDER BY 1;
UPDATE xl_dc26 SET c2 = 'yyy'::clob WHERE c1 = 1;
SELECT * FROM xl_dc26 ORDER BY 1;
DROP TABLE xl_dc26;

CREATE TABLE xl_dc26(c1 int, c2 clob) distribute by hash(c2);
INSERT INTO xl_dc26 VALUES(0, 'xxx'::clob), (1, 'xxx'::clob);
SELECT * FROM xl_dc26 ORDER BY 1;
UPDATE xl_dc26 SET c2 = 'yyy'::clob WHERE c1 = 1;
SELECT * FROM xl_dc26 ORDER BY 1;
DROP TABLE xl_dc26;

CREATE TABLE xl_dc26(c1 int, c2 clob) distribute by modulo(c2);
INSERT INTO xl_dc26 VALUES(0, 'xxx'::clob), (1, 'xxx'::clob);
SELECT * FROM xl_dc26 ORDER BY 1;
UPDATE xl_dc26 SET c2 = 'yyy'::clob WHERE c1 = 1;
SELECT * FROM xl_dc26 ORDER BY 1;
DROP TABLE xl_dc26;

-- Fix the not stable hash value for expr which type is not same as distributed key.
-- TAPD: https://tapd.woa.com/tapd_fe/20385652/bug/detail/1020385652136788353
-- postgresql mode
set enable_fast_query_shipping to on;
create table r_dis_typ_20250109(id int, address varchar(100) default '南山' collate "C", dt date default to_date('2099-08-15', 'yyyy-mm-dd')) with(oids) distribute by shard(dt);
create table r2_dis_typ_20250109(id int, address varchar(100) default '南山' collate "C", dt date default to_date('2099-08-15', 'yyyy-mm-dd')) distribute by shard(dt);
insert into r_dis_typ_20250109 select lv, (array['深圳','北京','上海','广州','重庆','武汉','成都','杭州','厦门','珠海','云南','新疆'])[((lv%12)+1)], (to_date('2022-08-15 00:00:00','yyyy-mm-dd HH24:mi:SS')+(lv%700)*interval '1' day) from generate_series(1, 1000) lv;
insert into r2_dis_typ_20250109 select lv, (array['深圳','北京','上海','广州','重庆','武汉','成都','杭州','厦门','珠海','云南','新疆'])[((lv%12)+1)], (to_date('2022-08-15 00:00:00','yyyy-mm-dd HH24:mi:SS')+(lv%700)*interval '1' day) from generate_series(1, 1000) lv;
create or replace function f_dis_typ_20250109(vdt timestamp without time zone) returns varchar
as $$
declare
    vaddress varchar;
begin
    select distinct address into vaddress from r_dis_typ_20250109 where dt=vdt order by 1 limit 1;
    return vaddress;
end; $$ language plpgsql;
select count(*) from r2_dis_typ_20250109 where f_dis_typ_20250109(dt)<>address;
drop function f_dis_typ_20250109;
drop table r_dis_typ_20250109;
drop table r2_dis_typ_20250109;
reset enable_fast_query_shipping;
-- opentenbase_ora mode
\c regression_ora
set enable_fast_query_shipping to on;
create table r_dis_typ_20250109(id int, address varchar2(100) default '南山' collate "C", dt date default to_date('2099-08-15', 'yyyy-mm-dd')) with(oids) distribute by shard(dt);
create table r2_dis_typ_20250109(id int, address varchar2(100) default '南山' collate "C", dt date default to_date('2099-08-15', 'yyyy-mm-dd')) distribute by shard(dt);
insert into r_dis_typ_20250109 select lv, (array['深圳','北京','上海','广州','重庆','武汉','成都','杭州','厦门','珠海','云南','新疆'])[((lv%12)+1)], (to_date('2022-08-15 00:00:00','yyyy-mm-dd HH24:mi:SS')+(lv%700)*interval '1' day) from generate_series(1, 1000) lv;
insert into r2_dis_typ_20250109 select lv, (array['深圳','北京','上海','广州','重庆','武汉','成都','杭州','厦门','珠海','云南','新疆'])[((lv%12)+1)], (to_date('2022-08-15 00:00:00','yyyy-mm-dd HH24:mi:SS')+(lv%700)*interval '1' day) from generate_series(1, 1000) lv;
create or replace function f_dis_typ_20250109(vdt timestamp without time zone) return varchar
as
    vaddress varchar;
begin
    select distinct address into vaddress from r_dis_typ_20250109 where dt=vdt order by 1 limit 1;
    return vaddress;
end;
/
select count(*) from r2_dis_typ_20250109 where f_dis_typ_20250109(dt)<>address;
drop function f_dis_typ_20250109;
drop table r_dis_typ_20250109;
drop table r2_dis_typ_20250109;
reset enable_fast_query_shipping;
