/*
 * test cases for urowid.
 * 1. create table
 *  1.1 create btree index
 *  1.2 create hash index
 *  1.3 search using btree/hash index/scan seq
 *  1.4 create table with urowid(n)
 * 2. rowid/text can be saved in urowid
 *  2.1 search urowid = rowid
 * 3. using in plsql - select rowid into urowid
 * 4. min/max
 */
create database urowid_test sql mode opentenbase_ora;
\c urowid_test
-- 1. create table
create table urid_tbl(k int, k2 urowid);
insert into urid_tbl values(1, 'urowid');
select * from urid_tbl;
drop table urid_tbl;
-- urowid as disk
create table urid_tbl(k urowid, k2 urowid);
insert into urid_tbl values('urowid', 'urowid');
select * from urid_tbl;
select * from urid_tbl where k2 = 'urowid';
drop table urid_tbl;

set enable_seqscan to off;
set enable_bitmapscan to off;

--  1.1 create btree index
create table ur_t(k int, k2 urowid);
create index ik2 on ur_t using btree(k2);
insert into ur_t values(1, '123');
insert into ur_t values(1, '123');
explain (costs off) select * from ur_t where k2 = '123';
select * from ur_t where k2 = '123';
drop table ur_t;

--  1.2 create hash index
create table ur_t(k int, k2 urowid);
create index ik2 on ur_t using hash(k2);
insert into ur_t values(1, '123');
insert into ur_t values(1, '123');
explain (costs off) select * from ur_t where k2 = '123';
select * from ur_t where k2 = '123';
drop table ur_t;

--  1.3 search using btree/hash index/scan seq
-- tested above
--  1.4 create table with urowid(n)
create table rt(a int, b urowid(2));
insert into rt values(1, '12');
insert into rt values(1, '123');
insert into rt values(1, '123'::urowid(2));
drop table rt;
-- 2. rowid/text can be saved in urowid
create table rtbl(a int)  ;
insert into rtbl values(1);
create table utbl(a int, b urowid(20));
insert into utbl select a, '12345678901234567890' from rtbl;

select a, substr(b::text, 17, 12) from utbl;
select substr(b::rowid::text, 17, 12) from utbl;

-- 4. min/max
select substr(min(b)::text, 17, 12), substr(max(b)::text, 17, 12) from utbl;

-- 5. cast to urowid
create or replace function test_cast_to_urid_f(u urowid) returns urowid
as $$
begin
return u;
end;
$$ language default_plsql;
select test_cast_to_urid_f('111'::text);
select test_cast_to_urid_f('ASDF'::varchar);
select test_cast_to_urid_f('===F'::varchar2);
select test_cast_to_urid_f('===F'::nvarchar2);
select test_cast_to_urid_f('FoeOwQ==P0AAAA==AgAAAAAAAAA='::rowid);

drop function test_cast_to_urid_f;

drop table rtbl;
drop table utbl;

\c postgres
clean connection to all for database urowid_test;
select pg_sleep(3);
drop database urowid_test(force);
