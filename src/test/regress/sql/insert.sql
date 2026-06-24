--
-- insert with DEFAULT in the target_list
--
create table inserttest (col1 int4, col2 int4 NOT NULL, col3 text default 'testing');
insert into inserttest (col1, col2, col3) values (DEFAULT, DEFAULT, DEFAULT);
insert into inserttest (col2, col3) values (3, DEFAULT);
insert into inserttest (col1, col2, col3) values (DEFAULT, 5, DEFAULT);
insert into inserttest values (DEFAULT, 5, 'test');
insert into inserttest values (DEFAULT, 7);

select * from inserttest;

--
-- insert with similar expression / target_list values (all fail)
--
insert into inserttest (col1, col2, col3) values (DEFAULT, DEFAULT);
insert into inserttest (col1, col2, col3) values (1, 2);
insert into inserttest (col1) values (1, 2);
insert into inserttest (col1) values (DEFAULT, DEFAULT);

select * from inserttest;

--
-- VALUES test
--
insert into inserttest values(10, 20, '40'), (-1, 2, DEFAULT),
    ((select 2), (select i from (values(3)) as foo (i)), 'values are fun!');

select * from inserttest order by 1,2,3;

--
-- TOASTed value test
--
insert into inserttest values(30, 50, repeat('x', 10000));

select col1, col2, char_length(col3) from inserttest order by 1,2,3;

drop table inserttest;

--
-- check indirection (field/array assignment), cf bug #14265
--
-- these tests are aware that transformInsertStmt has 3 separate code paths
--

create type insert_test_type as (if1 int, if2 text[]);

create table inserttest (f1 int, f2 int[],
                         f3 insert_test_type, f4 insert_test_type[]);

insert into inserttest (f2[1], f2[2]) values (1,2);
insert into inserttest (f2[1], f2[2]) values (3,4), (5,6);
insert into inserttest (f2[1], f2[2]) select 7,8;
insert into inserttest (f2[1], f2[2]) values (1,default);  -- not supported

insert into inserttest (f3.if1, f3.if2) values (1,array['foo']);
insert into inserttest (f3.if1, f3.if2) values (1,'{foo}'), (2,'{bar}');
insert into inserttest (f3.if1, f3.if2) select 3, '{baz,quux}';
insert into inserttest (f3.if1, f3.if2) values (1,default);  -- not supported

insert into inserttest (f3.if2[1], f3.if2[2]) values ('foo', 'bar');
insert into inserttest (f3.if2[1], f3.if2[2]) values ('foo', 'bar'), ('baz', 'quux');
insert into inserttest (f3.if2[1], f3.if2[2]) select 'bear', 'beer';

insert into inserttest (f4[1].if2[1], f4[1].if2[2]) values ('foo', 'bar');
insert into inserttest (f4[1].if2[1], f4[1].if2[2]) values ('foo', 'bar'), ('baz', 'quux');
insert into inserttest (f4[1].if2[1], f4[1].if2[2]) select 'bear', 'beer';

select * from inserttest;

-- also check reverse-listing
create table inserttest2 (f1 bigint, f2 text);
create rule irule1 as on insert to inserttest2 do also
  insert into inserttest (f3.if2[1], f3.if2[2])
  values (new.f1,new.f2);
create rule irule2 as on insert to inserttest2 do also
  insert into inserttest (f4[1].if1, f4[1].if2[2])
  values (1,'fool'),(new.f1,new.f2);
create rule irule3 as on insert to inserttest2 do also
  insert into inserttest (f4[1].if1, f4[1].if2[2])
  select new.f1, new.f2;
\d+ inserttest2

drop table inserttest2;
drop table inserttest;
drop type insert_test_type;

-- direct partition inserts should check partition bound constraint
create table range_parted (
	a text,
	b int
) partition by range (a, (b+0));

-- no partitions, so fail
insert into range_parted values ('a', 11);

create table part1 partition of range_parted for values from ('a', 1) to ('a', 10);
create table part2 partition of range_parted for values from ('a', 10) to ('a', 20);
create table part3 partition of range_parted for values from ('b', 1) to ('b', 10);
create table part4 partition of range_parted for values from ('b', 10) to ('b', 20);

-- fail
insert into part1 values ('a', 11);
insert into part1 values ('b', 1);
-- ok
insert into part1 values ('a', 1);
-- fail
insert into part4 values ('b', 21);
insert into part4 values ('a', 10);
-- ok
insert into part4 values ('b', 10);

-- fail (partition key a has a NOT NULL constraint)
insert into part1 values (null);
-- fail (expression key (b+0) cannot be null either)
insert into part1 values (1);

create table list_parted (
	a text,
	b int
) partition by list (lower(a));
create table part_aa_bb partition of list_parted FOR VALUES IN ('aa', 'bb');
create table part_cc_dd partition of list_parted FOR VALUES IN ('cc', 'dd');
create table part_null partition of list_parted FOR VALUES IN (null);

-- fail
insert into part_aa_bb values ('cc', 1);
insert into part_aa_bb values ('AAa', 1);
insert into part_aa_bb values (null);
-- ok
insert into part_cc_dd values ('cC', 1);
insert into part_null values (null, 0);

-- check in case of multi-level partitioned table
create table part_ee_ff partition of list_parted for values in ('ee', 'ff') partition by range (b);
create table part_ee_ff1 partition of part_ee_ff for values from (1) to (10);
create table part_ee_ff2 partition of part_ee_ff for values from (10) to (20);

-- test default partition
create table part_default partition of list_parted default;
-- Negative test: a row, which would fit in other partition, does not fit
-- default partition, even when inserted directly
insert into part_default values ('aa', 2);
insert into part_default values (null, 2);
-- ok
insert into part_default values ('Zz', 2);
-- test if default partition works as expected for multi-level partitioned
-- table as well as when default partition itself is further partitioned
drop table part_default;
create table part_xx_yy partition of list_parted for values in ('xx', 'yy') partition by list (a);
create table part_xx_yy_p1 partition of part_xx_yy for values in ('xx');
create table part_xx_yy_defpart partition of part_xx_yy default;
create table part_default partition of list_parted default partition by range(b);
create table part_default_p1 partition of part_default for values from (20) to (30);
create table part_default_p2 partition of part_default for values from (30) to (40);

-- fail
insert into part_ee_ff1 values ('EE', 11);
insert into part_default_p2 values ('gg', 43);
-- fail (even the parent's, ie, part_ee_ff's partition constraint applies)
insert into part_ee_ff1 values ('cc', 1);
insert into part_default values ('gg', 43);
-- ok
insert into part_ee_ff1 values ('ff', 1);
insert into part_ee_ff2 values ('ff', 11);
insert into part_default_p1 values ('cd', 25);
insert into part_default_p2 values ('de', 35);
insert into list_parted values ('ab', 21);
insert into list_parted values ('xx', 1);
insert into list_parted values ('yy', 2);
select tableoid::regclass, * from list_parted order by 1,2,3;

-- Check tuple routing for partitioned tables

-- fail
insert into range_parted values ('a', 0);
-- ok
insert into range_parted values ('a', 1);
insert into range_parted values ('a', 10);
-- fail
insert into range_parted values ('a', 20);
-- ok
insert into range_parted values ('b', 1);
insert into range_parted values ('b', 10);
-- fail (partition key (b+0) is null)
insert into range_parted values ('a');
-- Check default partition
create table part_def partition of range_parted default;
-- fail
insert into part_def values ('b', 10);
-- ok
insert into part_def values ('c', 10);
insert into range_parted values (null, null);
insert into range_parted values ('a', null);
insert into range_parted values (null, 19);
insert into range_parted values ('b', 20);

select tableoid::regclass, * from range_parted order by 1, 2, 3;
-- ok
insert into list_parted values (null, 1);
insert into list_parted (a) values ('aA');
-- fail (partition of part_ee_ff not found in both cases)
insert into list_parted values ('EE', 0);
insert into part_ee_ff values ('EE', 0);
-- ok
insert into list_parted values ('EE', 1);
insert into part_ee_ff values ('EE', 10);
select tableoid::regclass, * from list_parted order by 1,2,3;

-- some more tests to exercise tuple-routing with multi-level partitioning
create table part_gg partition of list_parted for values in ('gg') partition by range (b);
create table part_gg1 partition of part_gg for values from (minvalue) to (1);
create table part_gg2 partition of part_gg for values from (1) to (10) partition by range (b);
create table part_gg2_1 partition of part_gg2 for values from (1) to (5);
create table part_gg2_2 partition of part_gg2 for values from (5) to (10);

create table part_ee_ff3 partition of part_ee_ff for values from (20) to (30) partition by range (b);
create table part_ee_ff3_1 partition of part_ee_ff3 for values from (20) to (25);
create table part_ee_ff3_2 partition of part_ee_ff3 for values from (25) to (30);

truncate list_parted;
insert into list_parted values ('aa'), ('cc');
insert into list_parted select 'Ff', s.a from generate_series(1, 29) s(a);
insert into list_parted select 'gg', s.a from generate_series(1, 9) s(a);
insert into list_parted (b) values (1);
select tableoid::regclass::text, a, min(b) as min_b, max(b) as max_b from list_parted group by 1, 2 order by 1;

-- direct partition inserts should check hash partition bound constraint

-- create custom operator class and hash function, for the same reason
-- explained in alter_table.sql
create or replace function dummy_hashint4(a int4, seed int8) returns int8 as
$$ begin return (a + seed); end; $$ language 'plpgsql' immutable;
create operator class custom_opclass for type int4 using hash as
operator 1 = , function 2 dummy_hashint4(int4, int8);

create table hash_parted (
	a int
) partition by hash (a custom_opclass);
create table hpart0 partition of hash_parted for values with (modulus 4, remainder 0);
create table hpart1 partition of hash_parted for values with (modulus 4, remainder 1);
create table hpart2 partition of hash_parted for values with (modulus 4, remainder 2);
create table hpart3 partition of hash_parted for values with (modulus 4, remainder 3);

insert into hash_parted values(generate_series(1,10));

-- direct insert of values divisible by 4 - ok;
insert into hpart0 values(12),(16);
-- fail;
insert into hpart0 values(11);
-- 11 % 4 -> 3 remainder i.e. valid data for hpart3 partition
insert into hpart3 values(11);

-- view data
select tableoid::regclass as part, a, a%4 as "remainder = a % 4"
from hash_parted order by part,a;

-- test \d+ output on a table which has both partitioned and unpartitioned
-- partitions
\d+ list_parted

-- cleanup
drop table range_parted, list_parted;
drop table hash_parted;
drop operator class custom_opclass using hash;
drop function dummy_hashint4(a int4, seed int8);

-- test that a default partition added as the first partition accepts any value
-- including null
create table list_parted (a int) partition by list (a);
create table part_default partition of list_parted default;
\d+ part_default
insert into part_default values (null);
insert into part_default values (1);
insert into part_default values (-1);
select tableoid::regclass, a from list_parted;
-- cleanup
drop table list_parted;

-- more tests for certain multi-level partitioning scenarios
create table mlparted (a int, b int) partition by range (a, b);
create table mlparted1 (a int not null, b int not null) partition by range ((b+0));
create table mlparted11 (like mlparted1);
-- attnum for key attribute 'a' is different in mlparted, mlparted1, and mlparted11
select attrelid::regclass, attname, attnum
from pg_attribute
where attname = 'a'
 and (attrelid = 'mlparted'::regclass
   or attrelid = 'mlparted1'::regclass
   or attrelid = 'mlparted11'::regclass)
order by attrelid::regclass::text;

alter table mlparted1 attach partition mlparted11 for values from (2) to (5);
alter table mlparted attach partition mlparted1 for values from (1, 2) to (1, 10);

-- check that "(1, 2)" is correctly routed to mlparted11.
insert into mlparted values (1, 2);
select tableoid::regclass, * from mlparted;

-- check that proper message is shown after failure to route through mlparted1
insert into mlparted (a, b) values (1, 5);

truncate mlparted;
alter table mlparted add constraint check_b check (b = 3);

-- have a BR trigger modify the row such that the check_b is violated
create function mlparted11_trig_fn()
returns trigger AS
$$
begin
  NEW.b := 4;
  return NEW;
end;
$$
language plpgsql;
create trigger mlparted11_trig before insert ON mlparted11
  for each row execute procedure mlparted11_trig_fn();

-- check that the correct row is shown when constraint check_b fails after
-- "(1, 2)" is routed to mlparted11 (actually "(1, 4)" would be shown due
-- to the BR trigger mlparted11_trig_fn)
-- XXX since trigger are not supported in XL, "(1, 2)" would be shown
insert into mlparted values (1, 2);
drop trigger mlparted11_trig on mlparted11;
drop function mlparted11_trig_fn();

-- check that inserting into an internal partition successfully results in
-- checking its partition constraint before inserting into the leaf partition
-- selected by tuple-routing
insert into mlparted1 (a, b) values (2, 3);

-- check routing error through a list partitioned table when the key is null
create table lparted_nonullpart (a int, b char) partition by list (b);
create table lparted_nonullpart_a partition of lparted_nonullpart for values in ('a');
insert into lparted_nonullpart values (1);
drop table lparted_nonullpart;

-- check that RETURNING works correctly with tuple-routing
alter table mlparted drop constraint check_b;
create table mlparted12 partition of mlparted1 for values from (5) to (10);
create table mlparted2 (a int not null, b int not null);
alter table mlparted attach partition mlparted2 for values from (1, 10) to (1, 20);
create table mlparted3 partition of mlparted for values from (1, 20) to (1, 30);
create table mlparted4 (like mlparted);
-- alter table mlparted4 drop a;
-- alter table mlparted4 add a int not null;
alter table mlparted attach partition mlparted4 for values from (1, 30) to (1, 40);
insert into mlparted (b, a) select s.a, 1 from generate_series(2, 39) s(a) returning tableoid::regclass, *;

alter table mlparted add c text;
create table mlparted5 (a int not null, b int not null, c text) partition by list (c);
create table mlparted5a (a int not null, b int not null, c text);
alter table mlparted5 attach partition mlparted5a for values in ('a');
alter table mlparted attach partition mlparted5 for values from (1, 40) to (1, 50);
alter table mlparted add constraint check_b check (a = 1 and b < 45);
insert into mlparted values (1, 45, 'a');
create function mlparted5abrtrig_func() returns trigger as $$ begin new.c = 'b'; return new; end; $$ language plpgsql;
create trigger mlparted5abrtrig before insert on mlparted5a for each row execute procedure mlparted5abrtrig_func();
insert into mlparted5 (a, b, c) values (1, 40, 'a');
drop table mlparted5;
alter table mlparted drop constraint check_b;

-- Check multi-level default partition
create table mlparted_def partition of mlparted default partition by range(a);
create table mlparted_def1 partition of mlparted_def for values from (40) to (50);
create table mlparted_def2 partition of mlparted_def for values from (50) to (60);
insert into mlparted values (40, 100);
insert into mlparted_def1 values (42, 100);
insert into mlparted_def2 values (54, 50);
-- fail
insert into mlparted values (70, 100);
insert into mlparted_def1 values (52, 50);
insert into mlparted_def2 values (34, 50);
-- ok
create table mlparted_defd partition of mlparted_def default;
insert into mlparted values (70, 100);

select tableoid::regclass, * from mlparted_def order by 1;

-- check that message shown after failure to find a partition shows the
-- appropriate key description (or none) in various situations
create table key_desc (a int, b int) partition by list ((a+0));
create table key_desc_1 partition of key_desc for values in (1) partition by range (b);

create user someone_else;
grant select (a) on key_desc_1 to someone_else;
grant insert on key_desc to someone_else;

set role someone_else;
-- no key description is shown
insert into key_desc values (1, 1);

reset role;
grant select (b) on key_desc_1 to someone_else;
set role someone_else;
-- key description (b)=(1) is now shown
insert into key_desc values (1, 1);

-- key description is not shown if key contains expression
insert into key_desc values (2, 1);
reset role;
revoke all on key_desc from someone_else;
revoke all on key_desc_1 from someone_else;
drop role someone_else;
drop table key_desc, key_desc_1;

-- test minvalue/maxvalue restrictions
create table mcrparted (a int, b int, c int) partition by range (a, abs(b), c);
create table mcrparted0 partition of mcrparted for values from (minvalue, 0, 0) to (1, maxvalue, maxvalue);
create table mcrparted2 partition of mcrparted for values from (10, 6, minvalue) to (10, maxvalue, minvalue);
create table mcrparted4 partition of mcrparted for values from (21, minvalue, 0) to (30, 20, minvalue);

-- check multi-column range partitioning expression enforces the same
-- constraint as what tuple-routing would determine it to be
create table mcrparted0 partition of mcrparted for values from (minvalue, minvalue, minvalue) to (1, maxvalue, maxvalue);
create table mcrparted1 partition of mcrparted for values from (2, 1, minvalue) to (10, 5, 10);
create table mcrparted2 partition of mcrparted for values from (10, 6, minvalue) to (10, maxvalue, maxvalue);
create table mcrparted3 partition of mcrparted for values from (11, 1, 1) to (20, 10, 10);
create table mcrparted4 partition of mcrparted for values from (21, minvalue, minvalue) to (30, 20, maxvalue);
create table mcrparted5 partition of mcrparted for values from (30, 21, 20) to (maxvalue, maxvalue, maxvalue);

-- null not allowed in range partition
insert into mcrparted values (null, null, null);

-- routed to mcrparted0
insert into mcrparted values (0, 1, 1);
insert into mcrparted0 values (0, 1, 1);

-- routed to mcparted1
insert into mcrparted values (9, 1000, 1);
insert into mcrparted1 values (9, 1000, 1);
insert into mcrparted values (10, 5, -1);
insert into mcrparted1 values (10, 5, -1);
insert into mcrparted values (2, 1, 0);
insert into mcrparted1 values (2, 1, 0);

-- routed to mcparted2
insert into mcrparted values (10, 6, 1000);
insert into mcrparted2 values (10, 6, 1000);
insert into mcrparted values (10, 1000, 1000);
insert into mcrparted2 values (10, 1000, 1000);

-- no partition exists, nor does mcrparted3 accept it
insert into mcrparted values (11, 1, -1);
insert into mcrparted3 values (11, 1, -1);

-- routed to mcrparted5
insert into mcrparted values (30, 21, 20);
insert into mcrparted5 values (30, 21, 20);
insert into mcrparted4 values (30, 21, 20);	-- error

-- check rows
select tableoid::regclass::text, * from mcrparted order by 1, 2, 3, 4;

-- cleanup
drop table mcrparted;

-- check that a BR constraint can't make partition contain violating rows
create table brtrigpartcon (a int, b text) partition by list (a);
create table brtrigpartcon1 partition of brtrigpartcon for values in (1);
create or replace function brtrigpartcon1trigf() returns trigger as $$begin new.a := 2; return new; end$$ language plpgsql;
create trigger brtrigpartcon1trig before insert on brtrigpartcon1 for each row execute procedure brtrigpartcon1trigf();
insert into brtrigpartcon values (1, 'hi there');
insert into brtrigpartcon1 values (1, 'hi there');

-- check that the message shows the appropriate column description in a
-- situation where the partitioned table is not the primary ModifyTable node
create table inserttest3 (f1 text default 'foo', f2 text default 'bar', f3 int);
create role regress_coldesc_role;
grant insert on inserttest3 to regress_coldesc_role;
grant insert on brtrigpartcon to regress_coldesc_role;
revoke select on brtrigpartcon from regress_coldesc_role;
set role regress_coldesc_role;
with result as (insert into brtrigpartcon values (1, 'hi there') returning 1)
  insert into inserttest3 (f3) select * from result;
reset role;

-- cleanup
revoke all on inserttest3 from regress_coldesc_role;
revoke all on brtrigpartcon from regress_coldesc_role;
drop role regress_coldesc_role;
drop table inserttest3;
drop table brtrigpartcon;
drop function brtrigpartcon1trigf();

-- check that "do nothing" BR triggers work with tuple-routing (this checks
-- that estate->es_result_relation_info is appropriately set/reset for each
-- routed tuple)
create table donothingbrtrig_test (a int, b text) partition by list (a);
create table donothingbrtrig_test1 (b text, a int);
create table donothingbrtrig_test2 (c text, b text, a int);
alter table donothingbrtrig_test2 drop column c;
create or replace function donothingbrtrig_func() returns trigger as $$begin raise notice 'b: %', new.b; return NULL; end$$ language plpgsql;
create trigger donothingbrtrig1 before insert on donothingbrtrig_test1 for each row execute procedure donothingbrtrig_func();
create trigger donothingbrtrig2 before insert on donothingbrtrig_test2 for each row execute procedure donothingbrtrig_func();
alter table donothingbrtrig_test attach partition donothingbrtrig_test1 for values in (1);
alter table donothingbrtrig_test attach partition donothingbrtrig_test2 for values in (2);
insert into donothingbrtrig_test values (1, 'foo'), (2, 'bar');
copy donothingbrtrig_test from stdout;
1	baz
2	qux
\.
select tableoid::regclass, * from donothingbrtrig_test;

-- cleanup
drop table donothingbrtrig_test;
drop function donothingbrtrig_func();

-- check multi-column range partitioning with minvalue/maxvalue constraints
create table mcrparted (a text, b int) partition by range(a, b);
create table mcrparted1_lt_b partition of mcrparted for values from (minvalue, minvalue) to ('b', minvalue);
create table mcrparted2_b partition of mcrparted for values from ('b', minvalue) to ('c', minvalue);
create table mcrparted3_c_to_common partition of mcrparted for values from ('c', minvalue) to ('common', minvalue);
create table mcrparted4_common_lt_0 partition of mcrparted for values from ('common', minvalue) to ('common', 0);
create table mcrparted5_common_0_to_10 partition of mcrparted for values from ('common', 0) to ('common', 10);
create table mcrparted6_common_ge_10 partition of mcrparted for values from ('common', 10) to ('common', maxvalue);
create table mcrparted7_gt_common_lt_d partition of mcrparted for values from ('common', maxvalue) to ('d', minvalue);
create table mcrparted8_ge_d partition of mcrparted for values from ('d', minvalue) to (maxvalue, maxvalue);

\d+ mcrparted
\d+ mcrparted1_lt_b
\d+ mcrparted2_b
\d+ mcrparted3_c_to_common
\d+ mcrparted4_common_lt_0
\d+ mcrparted5_common_0_to_10
\d+ mcrparted6_common_ge_10
\d+ mcrparted7_gt_common_lt_d
\d+ mcrparted8_ge_d

insert into mcrparted values ('aaa', 0), ('b', 0), ('bz', 10), ('c', -10),
    ('comm', -10), ('common', -10), ('common', 0), ('common', 10),
    ('commons', 0), ('d', -10), ('e', 0);
select tableoid::regclass, * from mcrparted order by a, b;
drop table mcrparted;

-- check that wholerow vars in the RETURNING list work with partitioned tables
create table returningwrtest (a int) partition by list (a);
create table returningwrtest1 partition of returningwrtest for values in (1);
insert into returningwrtest values (1) returning returningwrtest;

-- check also that the wholerow vars in RETURNING list are converted as needed
alter table returningwrtest add b text;
create table returningwrtest2 (a int, b text, c int);
alter table returningwrtest2 drop c;
alter table returningwrtest attach partition returningwrtest2 for values in (2);
insert into returningwrtest values (2, 'foo') returning returningwrtest;
drop table returningwrtest;


-- OPENTENBASE
create table test1 (
    c11 character varying(2) NOT NULL,
    c12 varchar(2),
    c13 varchar(2)
)
DISTRIBUTE BY SHARD (c11) to GROUP default_group;
create table test2 (
    c21 character varying(4) NOT NULL,
    c22 varchar(4),
    c23 varchar(4)
)
DISTRIBUTE BY SHARD (c21) to GROUP default_group;

explain (costs off) insert into test2 select * from test1;
explain (costs off) insert into test1 select * from test2;

insert into test1 values('11','22','33');
insert into test1 values('22','22','33');
insert into test1 values('33','22','33');
insert into test1 values('44','22','33');

insert into test2 select * from test1;
select c21 from test2 order by c21;

delete from test1;
delete from test2;

insert into test2 values('11','22','33');
insert into test2 values('22','22','33');
insert into test2 values('33','22','33');
insert into test2 values('44','22','33');

insert into test1 select * from test2;
select c11 from test1 order by c11;

delete from test1;
delete from test2;

insert into test2 values('11','22','33');
insert into test2 values('22','22','33');
insert into test2 values('3333','22','33');
insert into test2 values('4444','22','33');

insert into test1 select * from test2;
select c11 from test1 order by c11;

drop table test1;
drop table test2;


create table ls_wd_zzdjzt(ZZDAH varchar(20), ZZTYBM varchar(3000)) distribute by shard(ZZDAH);
create table cte1(ZZDAH character varying(20) ,rn bigint) distribute by shard(ZZDAH);

explain (costs off) insert into cte1 
select ZZDAH, count(*) OVER(PARTITION BY ZZTYBM) AS RN
from ls_wd_zzdjzt where ZZDAH='1';

drop table ls_wd_zzdjzt, cte1;

drop table if EXISTS t1 cascade;
create table t1(c1 VARCHAR(100) default '', c2 bigserial) distribute by replication;
explain (costs off, nodes off) insert into t1(c1) select c1 from t1;
drop table t1;

CREATE TABLE t222(c0 VARCHAR(492));
INSERT INTO t222(c0) VALUES(((((((FALSE) IN (FALSE, FALSE))!=((CAST(TRUE AS INT))::BOOLEAN)))||'1') COLLATE "C"));
CREATE TABLE  t333(c0 boolean);
INSERT INTO t333(c0) VALUES(NOT ((((1753667088) IN (948663811, 654512609, -874682764))OR((((164228616) NOT IN (-1815854605, 434827675))OR(NOT (TRUE)))))));
drop table t222,t333;

-- Test Table with rowid 1600 columns with null values
\c regression_ora

-- TestPoint :一次插入1600列
drop table if exists  t1;
create table  t1(
 col1 int 
,col2 int
,col3 text default 1234
,col4 text default 1234
,col5 text default 1234
,col6 text default 1234
,col7 text default 1234
,col8 text default 1234
,col9 text default 1234
,col10 text default 1234
,col11 text default 1234
,col12 text default 1234
,col13 text default 1234
,col14 text default 1234
,col15 text default 1234
,col16 text default 1234
,col17 text default 1234
,col18 text default 1234
,col19 text default 1234
,col20 text default 1234
,col21 text default 1234
,col22 text default 1234
,col23 text default 1234
,col24 text default 1234
,col25 text default 1234
,col26 text default 1234
,col27 text default 1234
,col28 text default 1234
,col29 text default 1234
,col30 text default 1234
,col31 text default 1234
,col32 text default 1234
,col33 text default 1234
,col34 text default 1234
,col35 text default 1234
,col36 text default 1234
,col37 text default 1234
,col38 text default 1234
,col39 text default 1234
,col40 text default 1234
,col41 text default 1234
,col42 text default 1234
,col43 text default 1234
,col44 text default 1234
,col45 text default 1234
,col46 text default 1234
,col47 text default 1234
,col48 text default 1234
,col49 text default 1234
,col50 text default 1234
,col51 text default 1234
,col52 text default 1234
,col53 text default 1234
,col54 text default 1234
,col55 text default 1234
,col56 text default 1234
,col57 text default 1234
,col58 text default 1234
,col59 text default 1234
,col60 text default 1234
,col61 text default 1234
,col62 text default 1234
,col63 text default 1234
,col64 text default 1234
,col65 text default 1234
,col66 text default 1234
,col67 text default 1234
,col68 text default 1234
,col69 text default 1234
,col70 text default 1234
,col71 text default 1234
,col72 text default 1234
,col73 text default 1234
,col74 text default 1234
,col75 text default 1234
,col76 text default 1234
,col77 text default 1234
,col78 text default 1234
,col79 text default 1234
,col80 text default 1234
,col81 text default 1234
,col82 text default 1234
,col83 text default 1234
,col84 text default 1234
,col85 text default 1234
,col86 text default 1234
,col87 text default 1234
,col88 text default 1234
,col89 text default 1234
,col90 text default 1234
,col91 text default 1234
,col92 text default 1234
,col93 text default 1234
,col94 text default 1234
,col95 text default 1234
,col96 text default 1234
,col97 text default 1234
,col98 text default 1234
,col99 text default 1234
,col100 text default 1234
,col101 text default 1234
,col102 text default 1234
,col103 text default 1234
,col104 text default 1234
,col105 text default 1234
,col106 text default 1234
,col107 text default 1234
,col108 text default 1234
,col109 text default 1234
,col110 text default 1234
,col111 text default 1234
,col112 text default 1234
,col113 text default 1234
,col114 text default 1234
,col115 text default 1234
,col116 text default 1234
,col117 text default 1234
,col118 text default 1234
,col119 text default 1234
,col120 text default 1234
,col121 text default 1234
,col122 text default 1234
,col123 text default 1234
,col124 text default 1234
,col125 text default 1234
,col126 text default 1234
,col127 text default 1234
,col128 text default 1234
,col129 text default 1234
,col130 text default 1234
,col131 text default 1234
,col132 text default 1234
,col133 text default 1234
,col134 text default 1234
,col135 text default 1234
,col136 text default 1234
,col137 text default 1234
,col138 text default 1234
,col139 text default 1234
,col140 text default 1234
,col141 text default 1234
,col142 text default 1234
,col143 text default 1234
,col144 text default 1234
,col145 text default 1234
,col146 text default 1234
,col147 text default 1234
,col148 text default 1234
,col149 text default 1234
,col150 text default 1234
,col151 text default 1234
,col152 text default 1234
,col153 text default 1234
,col154 text default 1234
,col155 text default 1234
,col156 text default 1234
,col157 text default 1234
,col158 text default 1234
,col159 text default 1234
,col160 text default 1234
,col161 text default 1234
,col162 text default 1234
,col163 text default 1234
,col164 text default 1234
,col165 text default 1234
,col166 text default 1234
,col167 text default 1234
,col168 text default 1234
,col169 text default 1234
,col170 text default 1234
,col171 text default 1234
,col172 text default 1234
,col173 text default 1234
,col174 text default 1234
,col175 text default 1234
,col176 text default 1234
,col177 text default 1234
,col178 text default 1234
,col179 text default 1234
,col180 text default 1234
,col181 text default 1234
,col182 text default 1234
,col183 text default 1234
,col184 text default 1234
,col185 text default 1234
,col186 text default 1234
,col187 text default 1234
,col188 text default 1234
,col189 text default 1234
,col190 text default 1234
,col191 text default 1234
,col192 text default 1234
,col193 text default 1234
,col194 text default 1234
,col195 text default 1234
,col196 text default 1234
,col197 text default 1234
,col198 text default 1234
,col199 text default 1234
,col200 text default 1234
,col201 text default 1234
,col202 text default 1234
,col203 text default 1234
,col204 text default 1234
,col205 text default 1234
,col206 text default 1234
,col207 text default 1234
,col208 text default 1234
,col209 text default 1234
,col210 text default 1234
,col211 text default 1234
,col212 text default 1234
,col213 text default 1234
,col214 text default 1234
,col215 text default 1234
,col216 text default 1234
,col217 text default 1234
,col218 text default 1234
,col219 text default 1234
,col220 text default 1234
,col221 text default 1234
,col222 text default 1234
,col223 text default 1234
,col224 text default 1234
,col225 text default 1234
,col226 text default 1234
,col227 text default 1234
,col228 text default 1234
,col229 text default 1234
,col230 text default 1234
,col231 text default 1234
,col232 text default 1234
,col233 text default 1234
,col234 text default 1234
,col235 text default 1234
,col236 text default 1234
,col237 text default 1234
,col238 text default 1234
,col239 text default 1234
,col240 text default 1234
,col241 text default 1234
,col242 text default 1234
,col243 text default 1234
,col244 text default 1234
,col245 text default 1234
,col246 text default 1234
,col247 text default 1234
,col248 text default 1234
,col249 text default 1234
,col250 text default 1234
,col251 text default 1234
,col252 text default 1234
,col253 text default 1234
,col254 text default 1234
,col255 text default 1234
,col256 text default 1234
,col257 text default 1234
,col258 text default 1234
,col259 text default 1234
,col260 text default 1234
,col261 text default 1234
,col262 text default 1234
,col263 text default 1234
,col264 text default 1234
,col265 text default 1234
,col266 text default 1234
,col267 text default 1234
,col268 text default 1234
,col269 text default 1234
,col270 text default 1234
,col271 text default 1234
,col272 text default 1234
,col273 text default 1234
,col274 text default 1234
,col275 text default 1234
,col276 text default 1234
,col277 text default 1234
,col278 text default 1234
,col279 text default 1234
,col280 text default 1234
,col281 text default 1234
,col282 text default 1234
,col283 text default 1234
,col284 text default 1234
,col285 text default 1234
,col286 text default 1234
,col287 text default 1234
,col288 text default 1234
,col289 text default 1234
,col290 text default 1234
,col291 text default 1234
,col292 text default 1234
,col293 text default 1234
,col294 text default 1234
,col295 text default 1234
,col296 text default 1234
,col297 text default 1234
,col298 text default 1234
,col299 text default 1234
,col300 text default 1234
,col301 text default 1234
,col302 text default 1234
,col303 text default 1234
,col304 text default 1234
,col305 text default 1234
,col306 text default 1234
,col307 text default 1234
,col308 text default 1234
,col309 text default 1234
,col310 text default 1234
,col311 text default 1234
,col312 text default 1234
,col313 text default 1234
,col314 text default 1234
,col315 text default 1234
,col316 text default 1234
,col317 text default 1234
,col318 text default 1234
,col319 text default 1234
,col320 text default 1234
,col321 text default 1234
,col322 text default 1234
,col323 text default 1234
,col324 text default 1234
,col325 text default 1234
,col326 text default 1234
,col327 text default 1234
,col328 text default 1234
,col329 text default 1234
,col330 text default 1234
,col331 text default 1234
,col332 text default 1234
,col333 text default 1234
,col334 text default 1234
,col335 text default 1234
,col336 text default 1234
,col337 text default 1234
,col338 text default 1234
,col339 text default 1234
,col340 text default 1234
,col341 text default 1234
,col342 text default 1234
,col343 text default 1234
,col344 text default 1234
,col345 text default 1234
,col346 text default 1234
,col347 text default 1234
,col348 text default 1234
,col349 text default 1234
,col350 text default 1234
,col351 text default 1234
,col352 text default 1234
,col353 text default 1234
,col354 text default 1234
,col355 text default 1234
,col356 text default 1234
,col357 text default 1234
,col358 text default 1234
,col359 text default 1234
,col360 text default 1234
,col361 text default 1234
,col362 text default 1234
,col363 text default 1234
,col364 text default 1234
,col365 text default 1234
,col366 text default 1234
,col367 text default 1234
,col368 text default 1234
,col369 text default 1234
,col370 text default 1234
,col371 text default 1234
,col372 text default 1234
,col373 text default 1234
,col374 text default 1234
,col375 text default 1234
,col376 text default 1234
,col377 text default 1234
,col378 text default 1234
,col379 text default 1234
,col380 text default 1234
,col381 text default 1234
,col382 text default 1234
,col383 text default 1234
,col384 text default 1234
,col385 text default 1234
,col386 text default 1234
,col387 text default 1234
,col388 text default 1234
,col389 text default 1234
,col390 text default 1234
,col391 text default 1234
,col392 text default 1234
,col393 text default 1234
,col394 text default 1234
,col395 text default 1234
,col396 text default 1234
,col397 text default 1234
,col398 text default 1234
,col399 text default 1234
,col400 text default 1234
,col401 text default 1234
,col402 text default 1234
,col403 text default 1234
,col404 text default 1234
,col405 text default 1234
,col406 text default 1234
,col407 text default 1234
,col408 text default 1234
,col409 text default 1234
,col410 text default 1234
,col411 text default 1234
,col412 text default 1234
,col413 text default 1234
,col414 text default 1234
,col415 text default 1234
,col416 text default 1234
,col417 text default 1234
,col418 text default 1234
,col419 text default 1234
,col420 text default 1234
,col421 text default 1234
,col422 text default 1234
,col423 text default 1234
,col424 text default 1234
,col425 text default 1234
,col426 text default 1234
,col427 text default 1234
,col428 text default 1234
,col429 text default 1234
,col430 text default 1234
,col431 text default 1234
,col432 text default 1234
,col433 text default 1234
,col434 text default 1234
,col435 text default 1234
,col436 text default 1234
,col437 text default 1234
,col438 text default 1234
,col439 text default 1234
,col440 text default 1234
,col441 text default 1234
,col442 text default 1234
,col443 text default 1234
,col444 text default 1234
,col445 text default 1234
,col446 text default 1234
,col447 text default 1234
,col448 text default 1234
,col449 text default 1234
,col450 text default 1234
,col451 text default 1234
,col452 text default 1234
,col453 text default 1234
,col454 text default 1234
,col455 text default 1234
,col456 text default 1234
,col457 text default 1234
,col458 text default 1234
,col459 text default 1234
,col460 text default 1234
,col461 text default 1234
,col462 text default 1234
,col463 text default 1234
,col464 text default 1234
,col465 text default 1234
,col466 text default 1234
,col467 text default 1234
,col468 text default 1234
,col469 text default 1234
,col470 text default 1234
,col471 text default 1234
,col472 text default 1234
,col473 text default 1234
,col474 text default 1234
,col475 text default 1234
,col476 text default 1234
,col477 text default 1234
,col478 text default 1234
,col479 text default 1234
,col480 text default 1234
,col481 text default 1234
,col482 text default 1234
,col483 text default 1234
,col484 text default 1234
,col485 text default 1234
,col486 text default 1234
,col487 text default 1234
,col488 text default 1234
,col489 text default 1234
,col490 text default 1234
,col491 text default 1234
,col492 text default 1234
,col493 text default 1234
,col494 text default 1234
,col495 text default 1234
,col496 text default 1234
,col497 text default 1234
,col498 text default 1234
,col499 text default 1234
,col500 text default 1234
,col501 text default 1234
,col502 text default 1234
,col503 text default 1234
,col504 text default 1234
,col505 text default 1234
,col506 text default 1234
,col507 text default 1234
,col508 text default 1234
,col509 text default 1234
,col510 text default 1234
,col511 text default 1234
,col512 text default 1234
,col513 text default 1234
,col514 text default 1234
,col515 text default 1234
,col516 text default 1234
,col517 text default 1234
,col518 text default 1234
,col519 text default 1234
,col520 text default 1234
,col521 text default 1234
,col522 text default 1234
,col523 text default 1234
,col524 text default 1234
,col525 text default 1234
,col526 text default 1234
,col527 text default 1234
,col528 text default 1234
,col529 text default 1234
,col530 text default 1234
,col531 text default 1234
,col532 text default 1234
,col533 text default 1234
,col534 text default 1234
,col535 text default 1234
,col536 text default 1234
,col537 text default 1234
,col538 text default 1234
,col539 text default 1234
,col540 text default 1234
,col541 text default 1234
,col542 text default 1234
,col543 text default 1234
,col544 text default 1234
,col545 text default 1234
,col546 text default 1234
,col547 text default 1234
,col548 text default 1234
,col549 text default 1234
,col550 text default 1234
,col551 text default 1234
,col552 text default 1234
,col553 text default 1234
,col554 text default 1234
,col555 text default 1234
,col556 text default 1234
,col557 text default 1234
,col558 text default 1234
,col559 text default 1234
,col560 text default 1234
,col561 text default 1234
,col562 text default 1234
,col563 text default 1234
,col564 text default 1234
,col565 text default 1234
,col566 text default 1234
,col567 text default 1234
,col568 text default 1234
,col569 text default 1234
,col570 text default 1234
,col571 text default 1234
,col572 text default 1234
,col573 text default 1234
,col574 text default 1234
,col575 text default 1234
,col576 text default 1234
,col577 text default 1234
,col578 text default 1234
,col579 text default 1234
,col580 text default 1234
,col581 text default 1234
,col582 text default 1234
,col583 text default 1234
,col584 text default 1234
,col585 text default 1234
,col586 text default 1234
,col587 text default 1234
,col588 text default 1234
,col589 text default 1234
,col590 text default 1234
,col591 text default 1234
,col592 text default 1234
,col593 text default 1234
,col594 text default 1234
,col595 text default 1234
,col596 text default 1234
,col597 text default 1234
,col598 text default 1234
,col599 text default 1234
,col600 text default 1234
,col601 text default 1234
,col602 text default 1234
,col603 text default 1234
,col604 text default 1234
,col605 text default 1234
,col606 text default 1234
,col607 text default 1234
,col608 text default 1234
,col609 text default 1234
,col610 text default 1234
,col611 text default 1234
,col612 text default 1234
,col613 text default 1234
,col614 text default 1234
,col615 text default 1234
,col616 text default 1234
,col617 text default 1234
,col618 text default 1234
,col619 text default 1234
,col620 text default 1234
,col621 text default 1234
,col622 text default 1234
,col623 text default 1234
,col624 text default 1234
,col625 text default 1234
,col626 text default 1234
,col627 text default 1234
,col628 text default 1234
,col629 text default 1234
,col630 text default 1234
,col631 text default 1234
,col632 text default 1234
,col633 text default 1234
,col634 text default 1234
,col635 text default 1234
,col636 text default 1234
,col637 text default 1234
,col638 text default 1234
,col639 text default 1234
,col640 text default 1234
,col641 text default 1234
,col642 text default 1234
,col643 text default 1234
,col644 text default 1234
,col645 text default 1234
,col646 text default 1234
,col647 text default 1234
,col648 text default 1234
,col649 text default 1234
,col650 text default 1234
,col651 text default 1234
,col652 text default 1234
,col653 text default 1234
,col654 text default 1234
,col655 text default 1234
,col656 text default 1234
,col657 text default 1234
,col658 text default 1234
,col659 text default 1234
,col660 text default 1234
,col661 text default 1234
,col662 text default 1234
,col663 text default 1234
,col664 text default 1234
,col665 text default 1234
,col666 text default 1234
,col667 text default 1234
,col668 text default 1234
,col669 text default 1234
,col670 text default 1234
,col671 text default 1234
,col672 text default 1234
,col673 text default 1234
,col674 text default 1234
,col675 text default 1234
,col676 text default 1234
,col677 text default 1234
,col678 text default 1234
,col679 text default 1234
,col680 text default 1234
,col681 text default 1234
,col682 text default 1234
,col683 text default 1234
,col684 text default 1234
,col685 text default 1234
,col686 text default 1234
,col687 text default 1234
,col688 text default 1234
,col689 text default 1234
,col690 text default 1234
,col691 text default 1234
,col692 text default 1234
,col693 text default 1234
,col694 text default 1234
,col695 text default 1234
,col696 text default 1234
,col697 text default 1234
,col698 text default 1234
,col699 text default 1234
,col700 text default 1234
,col701 text default 1234
,col702 text default 1234
,col703 text default 1234
,col704 text default 1234
,col705 text default 1234
,col706 text default 1234
,col707 text default 1234
,col708 text default 1234
,col709 text default 1234
,col710 text default 1234
,col711 text default 1234
,col712 text default 1234
,col713 text default 1234
,col714 text default 1234
,col715 text default 1234
,col716 text default 1234
,col717 text default 1234
,col718 text default 1234
,col719 text default 1234
,col720 text default 1234
,col721 text default 1234
,col722 text default 1234
,col723 text default 1234
,col724 text default 1234
,col725 text default 1234
,col726 text default 1234
,col727 text default 1234
,col728 text default 1234
,col729 text default 1234
,col730 text default 1234
,col731 text default 1234
,col732 text default 1234
,col733 text default 1234
,col734 text default 1234
,col735 text default 1234
,col736 text default 1234
,col737 text default 1234
,col738 text default 1234
,col739 text default 1234
,col740 text default 1234
,col741 text default 1234
,col742 text default 1234
,col743 text default 1234
,col744 text default 1234
,col745 text default 1234
,col746 text default 1234
,col747 text default 1234
,col748 text default 1234
,col749 text default 1234
,col750 text default 1234
,col751 text default 1234
,col752 text default 1234
,col753 text default 1234
,col754 text default 1234
,col755 text default 1234
,col756 text default 1234
,col757 text default 1234
,col758 text default 1234
,col759 text default 1234
,col760 text default 1234
,col761 text default 1234
,col762 text default 1234
,col763 text default 1234
,col764 text default 1234
,col765 text default 1234
,col766 text default 1234
,col767 text default 1234
,col768 text default 1234
,col769 text default 1234
,col770 text default 1234
,col771 text default 1234
,col772 text default 1234
,col773 text default 1234
,col774 text default 1234
,col775 text default 1234
,col776 text default 1234
,col777 text default 1234
,col778 text default 1234
,col779 text default 1234
,col780 text default 1234
,col781 text default 1234
,col782 text default 1234
,col783 text default 1234
,col784 text default 1234
,col785 text default 1234
,col786 text default 1234
,col787 text default 1234
,col788 text default 1234
,col789 text default 1234
,col790 text default 1234
,col791 text default 1234
,col792 text default 1234
,col793 text default 1234
,col794 text default 1234
,col795 text default 1234
,col796 text default 1234
,col797 text default 1234
,col798 text default 1234
,col799 text default 1234
,col800 text default 1234
,col801 text default 1234
,col802 text default 1234
,col803 text default 1234
,col804 text default 1234
,col805 text default 1234
,col806 text default 1234
,col807 text default 1234
,col808 text default 1234
,col809 text default 1234
,col810 text default 1234
,col811 text default 1234
,col812 text default 1234
,col813 text default 1234
,col814 text default 1234
,col815 text default 1234
,col816 text default 1234
,col817 text default 1234
,col818 text default 1234
,col819 text default 1234
,col820 text default 1234
,col821 text default 1234
,col822 text default 1234
,col823 text default 1234
,col824 text default 1234
,col825 text default 1234
,col826 text default 1234
,col827 text default 1234
,col828 text default 1234
,col829 text default 1234
,col830 text default 1234
,col831 text default 1234
,col832 text default 1234
,col833 text default 1234
,col834 text default 1234
,col835 text default 1234
,col836 text default 1234
,col837 text default 1234
,col838 text default 1234
,col839 text default 1234
,col840 text default 1234
,col841 text default 1234
,col842 text default 1234
,col843 text default 1234
,col844 text default 1234
,col845 text default 1234
,col846 text default 1234
,col847 text default 1234
,col848 text default 1234
,col849 text default 1234
,col850 text default 1234
,col851 text default 1234
,col852 text default 1234
,col853 text default 1234
,col854 text default 1234
,col855 text default 1234
,col856 text default 1234
,col857 text default 1234
,col858 text default 1234
,col859 text default 1234
,col860 text default 1234
,col861 text default 1234
,col862 text default 1234
,col863 text default 1234
,col864 text default 1234
,col865 text default 1234
,col866 text default 1234
,col867 text default 1234
,col868 text default 1234
,col869 text default 1234
,col870 text default 1234
,col871 text default 1234
,col872 text default 1234
,col873 text default 1234
,col874 text default 1234
,col875 text default 1234
,col876 text default 1234
,col877 text default 1234
,col878 text default 1234
,col879 text default 1234
,col880 text default 1234
,col881 text default 1234
,col882 text default 1234
,col883 text default 1234
,col884 text default 1234
,col885 text default 1234
,col886 text default 1234
,col887 text default 1234
,col888 text default 1234
,col889 text default 1234
,col890 text default 1234
,col891 text default 1234
,col892 text default 1234
,col893 text default 1234
,col894 text default 1234
,col895 text default 1234
,col896 text default 1234
,col897 text default 1234
,col898 text default 1234
,col899 text default 1234
,col900 text default 1234
,col901 text default 1234
,col902 text default 1234
,col903 text default 1234
,col904 text default 1234
,col905 text default 1234
,col906 text default 1234
,col907 text default 1234
,col908 text default 1234
,col909 text default 1234
,col910 text default 1234
,col911 text default 1234
,col912 text default 1234
,col913 text default 1234
,col914 text default 1234
,col915 text default 1234
,col916 text default 1234
,col917 text default 1234
,col918 text default 1234
,col919 text default 1234
,col920 text default 1234
,col921 text default 1234
,col922 text default 1234
,col923 text default 1234
,col924 text default 1234
,col925 text default 1234
,col926 text default 1234
,col927 text default 1234
,col928 text default 1234
,col929 text default 1234
,col930 text default 1234
,col931 text default 1234
,col932 text default 1234
,col933 text default 1234
,col934 text default 1234
,col935 text default 1234
,col936 text default 1234
,col937 text default 1234
,col938 text default 1234
,col939 text default 1234
,col940 text default 1234
,col941 text default 1234
,col942 text default 1234
,col943 text default 1234
,col944 text default 1234
,col945 text default 1234
,col946 text default 1234
,col947 text default 1234
,col948 text default 1234
,col949 text default 1234
,col950 text default 1234
,col951 text default 1234
,col952 text default 1234
,col953 text default 1234
,col954 text default 1234
,col955 text default 1234
,col956 text default 1234
,col957 text default 1234
,col958 text default 1234
,col959 text default 1234
,col960 text default 1234
,col961 text default 1234
,col962 text default 1234
,col963 text default 1234
,col964 text default 1234
,col965 text default 1234
,col966 text default 1234
,col967 text default 1234
,col968 text default 1234
,col969 text default 1234
,col970 text default 1234
,col971 text default 1234
,col972 text default 1234
,col973 text default 1234
,col974 text default 1234
,col975 text default 1234
,col976 text default 1234
,col977 text default 1234
,col978 text default 1234
,col979 text default 1234
,col980 text default 1234
,col981 text default 1234
,col982 text default 1234
,col983 text default 1234
,col984 text default 1234
,col985 text default 1234
,col986 text default 1234
,col987 text default 1234
,col988 text default 1234
,col989 text default 1234
,col990 text default 1234
,col991 text default 1234
,col992 text default 1234
,col993 text default 1234
,col994 text default 1234
,col995 text default 1234
,col996 text default 1234
,col997 text default 1234
,col998 text default 1234
,col999 text default 1234
,col1000 text default 1332
,col1001 text default 1332
,col1002 text default 1332
,col1003 text default 1332
,col1004 text default 1332
,col1005 text default 1332
,col1006 text default 1332
,col1007 text default 1332
,col1008 text default 1332
,col1009 text default 1332
,col1010 text default 1332
,col1011 text default 1332
,col1012 text default 1332
,col1013 text default 1332
,col1014 text default 1332
,col1015 text default 1332
,col1016 text default 1332
,col1017 text default 1332
,col1018 text default 1332
,col1019 text default 1332
,col1020 text default 1332
,col1021 text default 1332
,col1022 text default 1332
,col1023 text default 1332
,col1024 text default 1332
,col1025 text default 1332
,col1026 text default 1332
,col1027 text default 1332
,col1028 text default 1332
,col1029 text default 1332
,col1030 text default 1332
,col1031 text default 1332
,col1032 text default 1332
,col1033 text default 1332
,col1034 text default 1332
,col1035 text default 1332
,col1036 text default 1332
,col1037 text default 1332
,col1038 text default 1332
,col1039 text default 1332
,col1040 text default 1332
,col1041 text default 1332
,col1042 text default 1332
,col1043 text default 1332
,col1044 text default 1332
,col1045 text default 1332
,col1046 text default 1332
,col1047 text default 1332
,col1048 text default 1332
,col1049 text default 1332
,col1050 text default 1332
,col1051 text default 1332
,col1052 text default 1332
,col1053 text default 1332
,col1054 text default 1332
,col1055 text default 1332
,col1056 text default 1332
,col1057 text default 1332
,col1058 text default 1332
,col1059 text default 1332
,col1060 text default 1332
,col1061 text default 1332
,col1062 text default 1332
,col1063 text default 1332
,col1064 text default 1332
,col1065 text default 1332
,col1066 text default 1332
,col1067 text default 1332
,col1068 text default 1332
,col1069 text default 1332
,col1070 text default 1332
,col1071 text default 1332
,col1072 text default 1332
,col1073 text default 1332
,col1074 text default 1332
,col1075 text default 1332
,col1076 text default 1332
,col1077 text default 1332
,col1078 text default 1332
,col1079 text default 1332
,col1080 text default 1332
,col1081 text default 1332
,col1082 text default 1332
,col1083 text default 1332
,col1084 text default 1332
,col1085 text default 1332
,col1086 text default 1332
,col1087 text default 1332
,col1088 text default 1332
,col1089 text default 1332
,col1090 text default 1332
,col1091 text default 1332
,col1092 text default 1332
,col1093 text default 1332
,col1094 text default 1332
,col1095 text default 1332
,col1096 text default 1332
,col1097 text default 1332
,col1098 text default 1332
,col1099 text default 1332
,col1100 text default 1332
,col1101 text default 1332
,col1102 text default 1332
,col1103 text default 1332
,col1104 text default 1332
,col1105 text default 1332
,col1106 text default 1332
,col1107 text default 1332
,col1108 text default 1332
,col1109 text default 1332
,col1110 text default 1332
,col1111 text default 1332
,col1112 text default 1332
,col1113 text default 1332
,col1114 text default 1332
,col1115 text default 1332
,col1116 text default 1332
,col1117 text default 1332
,col1118 text default 1332
,col1119 text default 1332
,col1120 text default 1332
,col1121 text default 1332
,col1122 text default 1332
,col1123 text default 1332
,col1124 text default 1332
,col1125 text default 1332
,col1126 text default 1332
,col1127 text default 1332
,col1128 text default 1332
,col1129 text default 1332
,col1130 text default 1332
,col1131 text default 1332
,col1132 text default 1332
,col1133 text default 1332
,col1134 text default 1332
,col1135 text default 1332
,col1136 text default 1332
,col1137 text default 1332
,col1138 text default 1332
,col1139 text default 1332
,col1140 text default 1332
,col1141 text default 1332
,col1142 text default 1332
,col1143 text default 1332
,col1144 text default 1332
,col1145 text default 1332
,col1146 text default 1332
,col1147 text default 1332
,col1148 text default 1332
,col1149 text default 1332
,col1150 text default 1332
,col1151 text default 1332
,col1152 text default 1332
,col1153 text default 1332
,col1154 text default 1332
,col1155 text default 1332
,col1156 text default 1332
,col1157 text default 1332
,col1158 text default 1332
,col1159 text default 1332
,col1160 text default 1332
,col1161 text default 1332
,col1162 text default 1332
,col1163 text default 1332
,col1164 text default 1332
,col1165 text default 1332
,col1166 text default 1332
,col1167 text default 1332
,col1168 text default 1332
,col1169 text default 1332
,col1170 text default 1332
,col1171 text default 1332
,col1172 text default 1332
,col1173 text default 1332
,col1174 text default 1332
,col1175 text default 1332
,col1176 text default 1332
,col1177 text default 1332
,col1178 text default 1332
,col1179 text default 1332
,col1180 text default 1332
,col1181 text default 1332
,col1182 text default 1332
,col1183 text default 1332
,col1184 text default 1332
,col1185 text default 1332
,col1186 text default 1332
,col1187 text default 1332
,col1188 text default 1332
,col1189 text default 1332
,col1190 text default 1332
,col1191 text default 1332
,col1192 text default 1332
,col1193 text default 1332
,col1194 text default 1332
,col1195 text default 1332
,col1196 text default 1332
,col1197 text default 1332
,col1198 text default 1332
,col1199 text default 1332
,col1200 text default 1332
,col1201 text default 1332
,col1202 text default 1332
,col1203 text default 1332
,col1204 text default 1332
,col1205 text default 1332
,col1206 text default 1332
,col1207 text default 1332
,col1208 text default 1332
,col1209 text default 1332
,col1210 text default 1332
,col1211 text default 1332
,col1212 text default 1332
,col1213 text default 1332
,col1214 text default 1332
,col1215 text default 1332
,col1216 text default 1332
,col1217 text default 1332
,col1218 text default 1332
,col1219 text default 1332
,col1220 text default 1332
,col1221 text default 1332
,col1222 text default 1332
,col1223 text default 1332
,col1224 text default 1332
,col1225 text default 1332
,col1226 text default 1332
,col1227 text default 1332
,col1228 text default 1332
,col1229 text default 1332
,col1230 text default 1332
,col1231 text default 1332
,col1232 text default 1332
,col1233 text default 1332
,col1234 text default 1332
,col1235 text default 1332
,col1236 text default 1332
,col1237 text default 1332
,col1238 text default 1332
,col1239 text default 1332
,col1240 text default 1332
,col1241 text default 1332
,col1242 text default 1332
,col1243 text default 1332
,col1244 text default 1332
,col1245 text default 1332
,col1246 text default 1332
,col1247 text default 1332
,col1248 text default 1332
,col1249 text default 1332
,col1250 text default 1332
,col1251 text default 1332
,col1252 text default 1332
,col1253 text default 1332
,col1254 text default 1332
,col1255 text default 1332
,col1256 text default 1332
,col1257 text default 1332
,col1258 text default 1332
,col1259 text default 1332
,col1260 text default 1332
,col1261 text default 1332
,col1262 text default 1332
,col1263 text default 1332
,col1264 text default 1332
,col1265 text default 1332
,col1266 text default 1332
,col1267 text default 1332
,col1268 text default 1332
,col1269 text default 1332
,col1270 text default 1332
,col1271 text default 1332
,col1272 text default 1332
,col1273 text default 1332
,col1274 text default 1332
,col1275 text default 1332
,col1276 text default 1332
,col1277 text default 1332
,col1278 text default 1332
,col1279 text default 1332
,col1280 text default 1332
,col1281 text default 1332
,col1282 text default 1332
,col1283 text default 1332
,col1284 text default 1332
,col1285 text default 1332
,col1286 text default 1332
,col1287 text default 1332
,col1288 text default 1332
,col1289 text default 1332
,col1290 text default 1332
,col1291 text default 1332
,col1292 text default 1332
,col1293 text default 1332
,col1294 text default 1332
,col1295 text default 1332
,col1296 text default 1332
,col1297 text default 1332
,col1298 text default 1332
,col1299 text default 1332
,col1300 text default 1332
,col1301 text default 1332
,col1302 text default 1332
,col1303 text default 1332
,col1304 text default 1332
,col1305 text default 1332
,col1306 text default 1332
,col1307 text default 1332
,col1308 text default 1332
,col1309 text default 1332
,col1310 text default 1332
,col1311 text default 1332
,col1312 text default 1332
,col1313 text default 1332
,col1314 text default 1332
,col1315 text default 1332
,col1316 text default 1332
,col1317 text default 1332
,col1318 text default 1332
,col1319 text default 1332
,col1320 text default 1332
,col1321 text default 1332
,col1322 text default 1332
,col1323 text default 1332
,col1324 text default 1332
,col1325 text default 1332
,col1326 text default 1332
,col1327 text default 1332
,col1328 text default 1332
,col1329 text default 1332
,col1330 text default 1332
,col1331 text default 1332
,col1332 text default 1332
,col1333 text default 1332
,col1334 text default 1332
,col1335 text default 1332
,col1336 text default 1332
,col1337 text default 1332
,col1338 text default 1332
,col1339 text default 1332
,col1340 text default 1332
,col1341 text default 1332
,col1342 text default 1332
,col1343 text default 1332
,col1344 text default 1332
,col1345 text default 1332
,col1346 text default 1332
,col1347 text default 1332
,col1348 text default 1332
,col1349 text default 1332
,col1350 text default 1332
,col1351 text default 1332
,col1352 text default 1332
,col1353 text default 1332
,col1354 text default 1332
,col1355 text default 1332
,col1356 text default 1332
,col1357 text default 1332
,col1358 text default 1332
,col1359 text default 1332
,col1360 text default 1332
,col1361 text default 1332
,col1362 text default 1332
,col1363 text default 1332
,col1364 text default 1332
,col1365 text default 1332
,col1366 text default 1332
,col1367 text default 1332
,col1368 text default 1332
,col1369 text default 1332
,col1370 text default 1332
,col1371 text default 1332
,col1372 text default 1332
,col1373 text default 1332
,col1374 text default 1332
,col1375 text default 1332
,col1376 text default 1332
,col1377 text default 1332
,col1378 text default 1332
,col1379 text default 1332
,col1380 text default 1332
,col1381 text default 1332
,col1382 text default 1332
,col1383 text default 1332
,col1384 text default 1332
,col1385 text default 1332
,col1386 text default 1332
,col1387 text default 1332
,col1388 text default 1332
,col1389 text default 1332
,col1390 text default 1332
,col1391 text default 1332
,col1392 text default 1332
,col1393 text default 1332
,col1394 text default 1332
,col1395 text default 1332
,col1396 text default 1332
,col1397 text default 1332
,col1398 text default 1332
,col1399 text default 1332
,col1400 text default 1332
,col1401 text default 1332
,col1402 text default 1332
,col1403 text default 1332
,col1404 text default 1332
,col1405 text default 1332
,col1406 text default 1332
,col1407 text default 1332
,col1408 text default 1332
,col1409 text default 1332
,col1410 text default 1332
,col1411 text default 1332
,col1412 text default 1332
,col1413 text default 1332
,col1414 text default 1332
,col1415 text default 1332
,col1416 text default 1332
,col1417 text default 1332
,col1418 text default 1332
,col1419 text default 1332
,col1420 text default 1332
,col1421 text default 1332
,col1422 text default 1332
,col1423 text default 1332
,col1424 text default 1332
,col1425 text default 1332
,col1426 text default 1332
,col1427 text default 1332
,col1428 text default 1332
,col1429 text default 1332
,col1430 text default 1332
,col1431 text default 1332
,col1432 text default 1332
,col1433 text default 1332
,col1434 text default 1332
,col1435 text default 1332
,col1436 text default 1332
,col1437 text default 1332
,col1438 text default 1332
,col1439 text default 1332
,col1440 text default 1332
,col1441 text default 1332
,col1442 text default 1332
,col1443 text default 1332
,col1444 text default 1332
,col1445 text default 1332
,col1446 text default 1332
,col1447 text default 1332
,col1448 text default 1332
,col1449 text default 1332
,col1450 text default 1332
,col1451 text default 1332
,col1452 text default 1332
,col1453 text default 1332
,col1454 text default 1332
,col1455 text default 1332
,col1456 text default 1332
,col1457 text default 1332
,col1458 text default 1332
,col1459 text default 1332
,col1460 text default 1332
,col1461 text default 1332
,col1462 text default 1332
,col1463 text default 1332
,col1464 text default 1332
,col1465 text default 1332
,col1466 text default 1332
,col1467 text default 1332
,col1468 text default 1332
,col1469 text default 1332
,col1470 text default 1332
,col1471 text default 1332
,col1472 text default 1332
,col1473 text default 1332
,col1474 text default 1332
,col1475 text default 1332
,col1476 text default 1332
,col1477 text default 1332
,col1478 text default 1332
,col1479 text default 1332
,col1480 text default 1332
,col1481 text default 1332
,col1482 text default 1332
,col1483 text default 1332
,col1484 text default 1332
,col1485 text default 1332
,col1486 text default 1332
,col1487 text default 1332
,col1488 text default 1332
,col1489 text default 1332
,col1490 text default 1332
,col1491 text default 1332
,col1492 text default 1332
,col1493 text default 1332
,col1494 text default 1332
,col1495 text default 1332
,col1496 text default 1332
,col1497 text default 1332
,col1498 text default 1332
,col1499 text default 1332
,col1500 text default 1332
,col1501 text default 1332
,col1502 text default 1332
,col1503 text default 1332
,col1504 text default 1332
,col1505 text default 1332
,col1506 text default 1332
,col1507 text default 1332
,col1508 text default 1332
,col1509 text default 1332
,col1510 text default 1332
,col1511 text default 1332
,col1512 text default 1332
,col1513 text default 1332
,col1514 text default 1332
,col1515 text default 1332
,col1516 text default 1332
,col1517 text default 1332
,col1518 text default 1332
,col1519 text default 1332
,col1520 text default 1332
,col1521 text default 1332
,col1522 text default 1332
,col1523 text default 1332
,col1524 text default 1332
,col1525 text default 1332
,col1526 text default 1332
,col1527 text default 1332
,col1528 text default 1332
,col1529 text default 1332
,col1530 text default 1332
,col1531 text default 1332
,col1532 text default 1332
,col1533 text default 1332
,col1534 text default 1332
,col1535 text default 1332
,col1536 text default 1332
,col1537 text default 1332
,col1538 text default 1332
,col1539 text default 1332
,col1540 text default 1332
,col1541 text default 1332
,col1542 text default 1332
,col1543 text default 1332
,col1544 text default 1332
,col1545 text default 1332
,col1546 text default 1332
,col1547 text default 1332
,col1548 text default 1332
,col1549 text default 1332
,col1550 text default 1332
,col1551 text default 1332
,col1552 text default 1332
,col1553 text default 1332
,col1554 text default 1332
,col1555 text default 1332
,col1556 text default 1332
,col1557 text default 1332
,col1558 text default 1332
,col1559 text default 1332
,col1560 text default 1332
,col1561 text default 1332
,col1562 text default 1332
,col1563 text default 1332
,col1564 text default 1332
,col1565 text default 1332
,col1566 text default 1332
,col1567 text default 1332
,col1568 text default 1332
,col1569 text default 1332
,col1570 text default 1332
,col1571 text default 1332
,col1572 text default 1332
,col1573 text default 1332
,col1574 text default 1332
,col1575 text default 1332
,col1576 text default 1332
,col1577 text default 1332
,col1578 text default 1332
,col1579 text default 1332
,col1580 text default 1332
,col1581 text default 1332
,col1582 text default 1332
,col1583 text default 1332
,col1584 text default 1332
,col1585 text default 1332
,col1586 text default 1332
,col1587 text default 1332
,col1588 text default 1332
,col1589 text default 1332
,col1590 text default 1332
,col1591 text default 1332
,col1592 text default 1332
,col1593 text default 1332
,col1594 text default 1332
,col1595 text default 1332
,col1596 text default 1332
,col1597 text default 1332
,col1598 text default 1332
,col1599 text default 1332
,col1600 text default 1332
);

insert into  t1 values(generate_series(1000, 2000),generate_series(1000, 2000));
create table t2 as select * from t1 where col1%2=1;

--批量删除列，只保留3个列
create or replace function delete_column(tablename varchar2) return int 
as 
vsql varchar2(200);
v1 int:=3;
begin 
  while (v1<=1600)
  loop 
    vsql:='alter table '||tablename||' drop column col'||v1;
    execute  vsql;
    v1=v1+1;
  end loop;
  return 1;
end;
/

select delete_column('t2');
select delete_column('t1');

create view v2 as select * from t2;

-- 下面语句会core
merge into t2
using t1 as t1
on t1.col2=t2.col1
when matched then
update set t2.col2=t1.col2
when not matched then 
insert values(t1.col1);

drop view v2 cascade;
drop table t1;
