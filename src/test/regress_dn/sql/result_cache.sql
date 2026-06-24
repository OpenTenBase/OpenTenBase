drop table if exists result_cache_20230912_t1;
drop table if exists result_cache_20230912_t3;
drop table if exists result_cache_20230912_t2;
drop table if exists result_cache_20230912_t5;
drop table if exists result_cache_20230912_t6;


create table result_cache_20230912_t1(v text);
create table result_cache_20230912_t3(v int);
create table result_cache_20230912_t2(v int, w text);
insert into result_cache_20230912_t1 values('a'), ('a');
insert into result_cache_20230912_t3 values(generate_series(1,5));
insert into result_cache_20230912_t2(v) values(1) , (2);
insert into result_cache_20230912_t2 values(1, 'a');
insert into result_cache_20230912_t2(v) values(1) , (2);

create table result_cache_20230912_t5(c1 int, c2 text, c3 char, c4 bool, c5 varchar);
create table result_cache_20230912_t6(c1 int, c2 text, c3 char, c4 bool, c5 varchar);
insert into result_cache_20230912_t5 values(1, 'a', 'b', true, 'c');
insert into result_cache_20230912_t5 values(2, 'b', 'c', false, 'd');
insert into result_cache_20230912_t5 values(1, 'a', 'b', true, 'c');
insert into result_cache_20230912_t5 values(2, 'b', 'c', false, 'd');
insert into result_cache_20230912_t6 values(1, 'a', 'b', true, 'c');
insert into result_cache_20230912_t6 values(2, 'b', 'c', false, 'd');
insert into result_cache_20230912_t6 values(3, 'b', 'c', false, 'd');
insert into result_cache_20230912_t6(c1) values(1);
insert into result_cache_20230912_t6(c1) values(2);
insert into result_cache_20230912_t6 values(1, 'a', 'b', true, 'c');
insert into result_cache_20230912_t6 values(2, 'b', 'c', false, 'd');
insert into result_cache_20230912_t6 values(3, 'a', 'b', true, 'c');

create or replace function result_cache_20230912_f1(input text) returns int as $$
declare
m int;
begin
select count(v) into m from result_cache_20230912_t1 where v = input;
return m;
end;                                
$$ language plpgsql result_cache;

create or replace function result_cache_20230912_f3(input int) returns int as $$
declare
m int;
begin
select count(v) into m from result_cache_20230912_t3 where v = input;
return m;
end;
$$ language plpgsql pushdown result_cache;

create or replace function result_cache_20230912_f6(f2 text, f3 char, f4 bool, f5 varchar) returns int as $$
declare
m int;
begin
select count(c1) into m from result_cache_20230912_t5 where result_cache_20230912_t5.c2 = f2 and result_cache_20230912_t5.c3 = f3 and result_cache_20230912_t5.c4 = f4 and result_cache_20230912_t5.c5 = f5;
return m;
end;
$$ language plpgsql pushdown result_cache;

-- no args
create or replace function result_cache_20230912_f4() returns int as $$
declare
m int;
begin
select 10 into m;
return m;
end;
$$ language plpgsql result_cache;

-- nargs > 4
create or replace function result_cache_20230912_f5(f1 int, f2 text, f3 char, f4 bool, f5 varchar) returns int as $$
declare
m int;
begin
select count(c1) into m from result_cache_20230912_t5 where result_cache_20230912_t5.c1 = f1 and result_cache_20230912_t5.c2 = f2 and result_cache_20230912_t5.c3 = f3 and result_cache_20230912_t5.c4 = f4 and result_cache_20230912_t5.c5 = f5;
return m;
end;
$$ language plpgsql result_cache;

create or replace function result_cache_20230912_f5(f1 int, f2 text, f3 char, f4 bool, f5 varchar) returns int as $$
declare
m int;
begin
select count(c1) into m from result_cache_20230912_t5 where result_cache_20230912_t5.c1 = f1 and result_cache_20230912_t5.c2 = f2 and result_cache_20230912_t5.c3 = f3 and result_cache_20230912_t5.c4 = f4 and result_cache_20230912_t5.c5 = f5;
return m;
end;
$$ language plpgsql;

-- no return: return void
create or replace function result_cache_20230912_f2(input text) returns void as $$                                             
declare
m int;
begin
select count(v) into m from result_cache_20230912_t1 where v = input;                                                                  
end;
$$ language plpgsql;


set enable_scalar_subquery_cache = true;
select result_cache_20230912_f1(w), result_cache_20230912_f3(v) from result_cache_20230912_t2 order by 1, 2;
select result_cache_20230912_f1(w), result_cache_20230912_f4() from result_cache_20230912_t2 order by 1, 2;
select result_cache_20230912_f5(c1,c2,c3,c4,c5) from result_cache_20230912_t6 order by 1;
select result_cache_20230912_f1(c2), result_cache_20230912_f4(), result_cache_20230912_f6(c2,c3,c4,c5) from result_cache_20230912_t6 order by 1, 2, 3;
select result_cache_20230912_f1(c2), result_cache_20230912_f4(), result_cache_20230912_f5(c1,c2,c3,c4,c5) from result_cache_20230912_t6 order by 1, 2, 3;
select result_cache_20230912_f1(c2), result_cache_20230912_f4(), result_cache_20230912_f5(c1,c2,c3,c4,c5), result_cache_20230912_f2(c2) from result_cache_20230912_t6 order by 1,2,3;
select result_cache_20230912_f1(w), result_cache_20230912_f4(), result_cache_20230912_f2(w) from result_cache_20230912_t2 order by 1, 2;

-- sublink
select (select v from result_cache_20230912_t1 where v = result_cache_20230912_t6.c2 limit 1) from result_cache_20230912_t6 order by 1;
select (select v from result_cache_20230912_t1 where v = result_cache_20230912_t6.c2 limit 1), (select count(v) from result_cache_20230912_t3 where v = result_cache_20230912_t6.c1) from result_cache_20230912_t6 order by 1, 2;
select (select count(c1) from result_cache_20230912_t5 where c1 = result_cache_20230912_t6.c1 and c2 = result_cache_20230912_t6.c2 and c3 = result_cache_20230912_t6.c3 and c4 = result_cache_20230912_t6.c4 and c5 = result_cache_20230912_t6.c5) from result_cache_20230912_t6 order by 1;
select (select v from result_cache_20230912_t1 where v = result_cache_20230912_t6.c2 limit 1), (select count(v) from result_cache_20230912_t3 where v = result_cache_20230912_t6.c1), (select count(c1) from result_cache_20230912_t5 where c1 = result_cache_20230912_t6.c1 and c2 = result_cache_20230912_t6.c2 and c3 = result_cache_20230912_t6.c3 and c4 = result_cache_20230912_t6.c4 and c5 = result_cache_20230912_t6.c5) from result_cache_20230912_t6 order by 1, 2, 3;

-- udf + sublink
select result_cache_20230912_f1(c2), result_cache_20230912_f4(), result_cache_20230912_f5(c1,c2,c3,c4,c5), result_cache_20230912_f2(c2), result_cache_20230912_f6(c2,c3,c4,c5), (select v from result_cache_20230912_t1 where v = result_cache_20230912_t6.c2 limit 1), (select count(v) from result_cache_20230912_t3 where v = result_cache_20230912_t6.c1), (select count(c1) from result_cache_20230912_t5 where c1 = result_cache_20230912_t6.c1 and c2 = result_cache_20230912_t6.c2 and c3 = result_cache_20230912_t6.c3 and c4 = result_cache_20230912_t6.c4 and c5 = result_cache_20230912_t6.c5) from result_cache_20230912_t6 order by 1, 2, 3, 5, 6, 7;
select w, (select result_cache_20230912_f3(v)) from result_cache_20230912_t2; -- subquery call udf

set max_result_cache_memory = 128;
select result_cache_20230912_f1(w), result_cache_20230912_f3(v) from result_cache_20230912_t2 order by 1, 2;
select result_cache_20230912_f1(w), result_cache_20230912_f4() from result_cache_20230912_t2 order by 1, 2;
select result_cache_20230912_f5(c1,c2,c3,c4,c5) from result_cache_20230912_t6 order by 1;
select result_cache_20230912_f1(c2), result_cache_20230912_f4(), result_cache_20230912_f5(c1,c2,c3,c4,c5) from result_cache_20230912_t6 order by 1, 2, 3;
select result_cache_20230912_f1(c2), result_cache_20230912_f4(), result_cache_20230912_f5(c1,c2,c3,c4,c5), result_cache_20230912_f2(c2) from result_cache_20230912_t6 order by 1,2,3;
select result_cache_20230912_f1(w), result_cache_20230912_f4(), result_cache_20230912_f2(w) from result_cache_20230912_t2 order by 1, 2;

-- sublink
select (select v from result_cache_20230912_t1 where v = result_cache_20230912_t6.c2 limit 1) from result_cache_20230912_t6 order by 1;
select (select v from result_cache_20230912_t1 where v = result_cache_20230912_t6.c2 limit 1), (select count(v) from result_cache_20230912_t3 where v = result_cache_20230912_t6.c1) from result_cache_20230912_t6 order by 1, 2;
select (select count(c1) from result_cache_20230912_t5 where c1 = result_cache_20230912_t6.c1 and c2 = result_cache_20230912_t6.c2 and c3 = result_cache_20230912_t6.c3 and c4 = result_cache_20230912_t6.c4 and c5 = result_cache_20230912_t6.c5) from result_cache_20230912_t6 order by 1;
select (select v from result_cache_20230912_t1 where v = result_cache_20230912_t6.c2 limit 1), (select count(v) from result_cache_20230912_t3 where v = result_cache_20230912_t6.c1), (select count(c1) from result_cache_20230912_t5 where c1 = result_cache_20230912_t6.c1 and c2 = result_cache_20230912_t6.c2 and c3 = result_cache_20230912_t6.c3 and c4 = result_cache_20230912_t6.c4 and c5 = result_cache_20230912_t6.c5) from result_cache_20230912_t6 order by 1, 2, 3;

-- udf + sublink
select result_cache_20230912_f1(c2), result_cache_20230912_f4(), result_cache_20230912_f5(c1,c2,c3,c4,c5), result_cache_20230912_f2(c2), (select v from result_cache_20230912_t1 where v = result_cache_20230912_t6.c2 limit 1), (select count(v) from result_cache_20230912_t3 where v = result_cache_20230912_t6.c1), (select count(c1) from result_cache_20230912_t5 where c1 = result_cache_20230912_t6.c1 and c2 = result_cache_20230912_t6.c2 and c3 = result_cache_20230912_t6.c3 and c4 = result_cache_20230912_t6.c4 and c5 = result_cache_20230912_t6.c5) from result_cache_20230912_t6 order by 1, 2, 3, 5, 6;
select w, (select result_cache_20230912_f3(v)) from result_cache_20230912_t2; -- subquery call udf

reset max_result_cache_memory;
reset enable_scalar_subquery_cache;
