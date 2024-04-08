-- test [alter table t1 truncate partition (t2)]

drop table t_human cascade;
drop table t_man cascade;
drop table t_woman cascade;
drop table t_other cascade;

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

select count(*) from t_man;

-- truncate table
alter table t_human truncate partition (t_man);

select count(*) from t_man;
