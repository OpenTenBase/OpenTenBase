set enable_filter_statistic to off;
set max_parallel_workers to 0;
set max_parallel_workers_per_gather to 0;
set enable_hashjoin to off;
set enable_mergejoin to off;
set enable_bitmapscan to off;
set enable_material TO off;


create table if not exists pushpred_t1(c1 int, c2 int, c3 int, c4 int);
create table if not exists pushpred_t2(c1 int, c2 int, c3 int, c4 int);
create table if not exists pushpred_t3(c1 int, c2 int, c3 int, c4 int);

insert into pushpred_t1 select i,i,i,i from generate_series(1,100000) i;
insert into pushpred_t2 select i,i,i,i from generate_series(1,100000) i;
insert into pushpred_t3 select i,i,i,i from generate_series(1,100000) i;

create index pushpred_t1_c1_idx on pushpred_t1(c1);
create index pushpred_t2_c1_idx on pushpred_t2(c1);
create index pushpred_t3_c1_idx on pushpred_t3(c1);

analyze pushpred_t1;
analyze pushpred_t2;
analyze pushpred_t3;

set enable_push_pred to off;
explain select * from pushpred_t1 t1, (select c1, sum(c2) from pushpred_t2 group by c1) st2 where t1.c1 = st2.c1;
explain select * from pushpred_t1 t1, (select c1, sum(c2) from pushpred_t2 group by c1 union all select c1, sum(c2) from pushpred_t3 group by c1) st2 where t1.c1 = st2.c1;

set enable_push_pred to on;
explain select * from pushpred_t1 t1, (select c1, sum(c2) from pushpred_t2 group by c1) st2 where t1.c1 = st2.c1;
explain select * from pushpred_t1 t1, (select c1, sum(c2) from pushpred_t2 group by c1 union all select c1, sum(c2) from pushpred_t3 group by c1) st2 where t1.c1 = st2.c1;

explain select * from pushpred_t1 t1, (select c1,c2, sum(c3) from pushpred_t2 group by c1,c2) st2 where t1.c1 = st2.c1 and t1.c2 = st2.c2;
explain select * from pushpred_t1 t1, (select c1,c2, sum(c4) from pushpred_t2 group by c1,c2,c3) st2 where t1.c1 = st2.c1 and t1.c2 = st2.c2;

explain select * from pushpred_t1 t1, (select c1, sum(c2) from pushpred_t2 group by c1 offset 500) st2 where t1.c1 = st2.c1;
explain select * from pushpred_t1 t1, (select c1, sum(c2) from pushpred_t2 group by c1 limit 1) st2 where t1.c1 = st2.c1;
explain select * from pushpred_t1 t1, (select c1, sum(c2) from pushpred_t2 group by c1 union all select c1, sum(c2) from pushpred_t3 group by c1 offset 500) st2 where t1.c1 = st2.c1;

explain select * from pushpred_t1 t1, (select distinct c1 from pushpred_t2) st2 where t1.c1 = st2.c1;

explain select * from pushpred_t1 t1, (select c1, sum(c2*random()) from pushpred_t2 group by c1) st2 where t1.c1 = st2.c1;

create view pushpred_view as
select c1, c2 from pushpred_t1 where not exists (select 1 from pushpred_t3 where pushpred_t3.c1 = pushpred_t1.c2)
union
select c1, c2 from pushpred_t2;

set enable_push_pred to off;
explain (costs off, nodes off) select p2.c1, p2.c2 from pushpred_t3 p, pushpred_view p2 where p.c1 = 10 and p.c2 = p2.c2;

explain (costs off, nodes off) select /*+ pushpred(p2) */ p2.c1, p2.c2 from pushpred_t3 p, pushpred_view p2 where p.c1 = 10 and p.c2 = p2.c2;

set enable_push_pred to on;
explain (costs off, nodes off) select p2.c1, p2.c2 from pushpred_t3 p, pushpred_view p2 where p.c1 = 10 and p.c2 = p2.c2;

-- syntax error
explain (costs off, nodes off) select /*+ pushpred(p2 p) */ p2.c1, p2.c2 from pushpred_t3 p, pushpred_view p2 where p.c1 = 10 and p.c2 = p2.c2;

drop view pushpred_view;
drop table if exists pushpred_t1;
drop table if exists pushpred_t2;
drop table if exists pushpred_t3;


create table pushpred_cap (c1 date not null, c2 numeric(10,0), c3 numeric(10,0), c4 numeric(10,0), c5 numeric(10,0), c6 date);
create unique index pp_cap_u_idx on pushpred_cap(c1,c2,c3,c4);
create index pp_cap_c3 on pushpred_cap(c3);
create index pp_cap_c4 on pushpred_cap(c4);
create index pp_cap_c2_c1 on pushpred_cap(c2,c1);
create index pp_cap_c2 on pushpred_cap(c2);
set enable_push_pred to on;
select 1 from pushpred_cap, (select c1 from pushpred_cap where c1 = (select max(c1) from pushpred_cap) group by c1) s2 where s2.c1 = pushpred_cap.c1;
drop table if exists pushpred_cap;

set enable_push_pred = on;
explain (costs off, nodes off) select * from generate_series(1, 10) i where i in (values(i));

create table pushpred_fix_test1
(   c11 integer,
    c12 integer,
    c13 integer,
    c14 integer,
    c15 integer
);

create table pushpred_fix_test2
(   c21 integer,
    c22 integer,
    c23 integer,
    c24 integer,
    c25 integer
);

create table pushpred_fix_test3
(   c31 integer,
    c32 integer,
    c33 integer,
    c34 integer,
    c35 integer
);

set enable_push_pred = on;

explain (costs off, nodes off) select c11 from pushpred_fix_test1
      where case
            when pushpred_fix_test1.c11=1 THEN 1
            when pushpred_fix_test1.c11=2 THEN 2
            when c11=1 or exists (select pushpred_fix_test2.c23, pushpred_fix_test2.c24
                                      from pushpred_fix_test2 where pushpred_fix_test1.c12=pushpred_fix_test2.c22)
                       or c11 in (select c21 from pushpred_fix_test2
                                       where pushpred_fix_test1.c12=pushpred_fix_test2.c22) THEN pushpred_fix_test1.c11
            END = 1;


explain (costs off, nodes off) select c11 from pushpred_fix_test1
      where  exists (select pushpred_fix_test2.c23, pushpred_fix_test2.c24
                     from pushpred_fix_test2 where pushpred_fix_test1.c12=pushpred_fix_test2.c22)
             or c14 in (select c34 from pushpred_fix_test3
                         where pushpred_fix_test1.c14=pushpred_fix_test3.c34);


explain (costs off, nodes off) select c11 from pushpred_fix_test1
      where  c11=1
             or c14 in (select c34 from pushpred_fix_test3
                         where pushpred_fix_test1.c14=pushpred_fix_test3.c34);

reset enable_filter_statistic;
reset max_parallel_workers;
reset max_parallel_workers_per_gather;
reset enable_hashjoin;
reset enable_mergejoin;
reset enable_bitmapscan;
reset enable_material;
reset enable_push_pred;

drop table pushpred_fix_test1;
drop table pushpred_fix_test2;
drop table pushpred_fix_test3;