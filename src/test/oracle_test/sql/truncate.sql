-- test [alter table t1 truncate partition (t2)]

drop table if exists t_human cascade;
drop table if exists t_man cascade;
drop table if exists t_woman cascade;
drop table if exists t_other cascade;

truncate table t_human;

create table t_human (
	sex			varchar(10)
) PARTITION BY LIST (sex);

create table t_man
partition of t_human for values in ('man');

create table t_woman
partition of t_human for values in ('woman');

create table t_other
partition of t_human default;

-- insert one data into each of three tables
insert into t_human
values ('man'), ('woman'), ('other');

-- truncate table which is not partition of other table
alter table t_man truncate partition (t_human);
alter table t_man truncate partition (woman);
-- it should be 3
select count(*) from t_human;
-- it should be 1
select count(*) from t_man;

-- truncate table t_man which is parititon of t_human
alter table t_human truncate partition (t_man);
-- it should be 2
select count(*) from t_human;
-- it should be 0
select count(*) from t_man;

drop table t_human cascade;

