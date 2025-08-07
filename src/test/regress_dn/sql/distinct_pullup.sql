create table tt1_20240508
(
c1 integer not null,
c2 integer not null,
c3 integer,
c4 char(20)
)
DISTRIBUTE BY SHARD (c1);
create table tt2_20240508
(
c1 integer not null,
c2 integer not null,
c3 integer,
c4 char(20)
)
DISTRIBUTE BY SHARD (c1);
create table tt3_20240508
(
c1 integer not null,
c2 integer not null,
c3 integer,
c4 char(20)
)
DISTRIBUTE BY SHARD (c1);

insert into tt2_20240508 select t, t, t+1 from generate_series(1, 100) t;
insert into tt1_20240508 select t, t, t+1 from generate_series(1, 1000) t;
analyze;

-- select distinct targetlist
set enable_pullup_expr_distinct = off;
explain (costs off) select tt1_20240508.c2,  (select distinct tt2_20240508.c2 from tt2_20240508 where tt2_20240508.c1 = tt1_20240508.c1) as x from tt1_20240508 ;
set enable_pullup_expr_distinct = on;
explain (costs off) select tt1_20240508.c2,  (select distinct tt2_20240508.c2 from tt2_20240508 where tt2_20240508.c1 = tt1_20240508.c1) as x from tt1_20240508 ;

-- update distinct targetlist
set enable_pullup_expr_distinct = off;
explain (costs off) update tt1_20240508 set (c2,c3) = (select distinct c2,c3 from tt2_20240508 where tt1_20240508.c4 = tt2_20240508.c4);
set enable_pullup_expr_distinct = on;
explain (costs off) update tt1_20240508 set (c2,c3) = (select distinct c2,c3 from tt2_20240508 where tt1_20240508.c4 = tt2_20240508.c4);

drop table tt1_20240508;
drop table tt2_20240508;
drop table tt3_20240508;