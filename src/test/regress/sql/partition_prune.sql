--
-- Test partitioning planner code
--
create table lp (a char) partition by list (a);
create table lp_default partition of lp default;
create table lp_ef partition of lp for values in ('e', 'f');
create table lp_ad partition of lp for values in ('a', 'd');
create table lp_bc partition of lp for values in ('b', 'c');
create table lp_g partition of lp for values in ('g');
create table lp_null partition of lp for values in (null);
explain (costs off, verbose on) select * from lp;
explain (costs off, verbose on) select * from lp where a > 'a' and a < 'd';
explain (costs off, verbose on) select * from lp where a > 'a' and a <= 'd';
explain (costs off, verbose on) select * from lp where a = 'a';
explain (costs off, verbose on) select * from lp where 'a' = a;	/* commuted */
explain (costs off, verbose on) select * from lp where a is not null;
explain (costs off, verbose on) select * from lp where a is null;
explain (costs off, verbose on) select * from lp where a = 'a' or a = 'c';
explain (costs off, verbose on) select * from lp where a is not null and (a = 'a' or a = 'c');
explain (costs off, verbose on) select * from lp where a <> 'g';
explain (costs off, verbose on) select * from lp where a <> 'a' and a <> 'd';
explain (costs off, verbose on) select * from lp where a not in ('a', 'd');

-- collation matches the partitioning collation, pruning works
create table coll_pruning (a text collate "C") partition by list (a);
create table coll_pruning_a partition of coll_pruning for values in ('a');
create table coll_pruning_b partition of coll_pruning for values in ('b');
create table coll_pruning_def partition of coll_pruning default;
explain (costs off, verbose on) select * from coll_pruning where a collate "C" = 'a' collate "C";
-- collation doesn't match the partitioning collation, no pruning occurs
explain (costs off, verbose on) select * from coll_pruning where a collate "POSIX" = 'a' collate "POSIX";

create table rlp (a int, b varchar) partition by range (a);
create table rlp_default partition of rlp default partition by list (a);
create table rlp_default_default partition of rlp_default default;
create table rlp_default_10 partition of rlp_default for values in (10);
create table rlp_default_30 partition of rlp_default for values in (30);
create table rlp_default_null partition of rlp_default for values in (null);
create table rlp1 partition of rlp for values from (minvalue) to (1);
create table rlp2 partition of rlp for values from (1) to (10);

create table rlp3 (a int, b varchar) partition by list (b varchar_ops);
create table rlp3_default partition of rlp3 default;
create table rlp3abcd partition of rlp3 for values in ('ab', 'cd');
create table rlp3efgh partition of rlp3 for values in ('ef', 'gh');
create table rlp3nullxy partition of rlp3 for values in (null, 'xy');
alter table rlp attach partition rlp3 for values from (15) to (20);

create table rlp4 partition of rlp for values from (20) to (30) partition by range (a);
create table rlp4_default partition of rlp4 default;
create table rlp4_1 partition of rlp4 for values from (20) to (25);
create table rlp4_2 partition of rlp4 for values from (25) to (29);

create table rlp5 partition of rlp for values from (31) to (maxvalue) partition by range (a);
create table rlp5_default partition of rlp5 default;
create table rlp5_1 partition of rlp5 for values from (31) to (40);

explain (costs off, verbose on) select * from rlp where a < 1;
explain (costs off, verbose on) select * from rlp where 1 > a;	/* commuted */
explain (costs off, verbose on) select * from rlp where a <= 1;
explain (costs off, verbose on) select * from rlp where a = 1;
explain (costs off, verbose on) select * from rlp where a = 1::bigint;		/* same as above */
explain (costs off, verbose on) select * from rlp where a = 1::numeric;	/* no pruning */
explain (costs off, verbose on) select * from rlp where a <= 10;
explain (costs off, verbose on) select * from rlp where a > 10;
explain (costs off, verbose on) select * from rlp where a < 15;
explain (costs off, verbose on) select * from rlp where a <= 15;
explain (costs off, verbose on) select * from rlp where a > 15 and b = 'ab';
explain (costs off, verbose on) select * from rlp where a = 16;
explain (costs off, verbose on) select * from rlp where a = 16 and b in ('not', 'in', 'here');
explain (costs off, verbose on) select * from rlp where a = 16 and b < 'ab';
explain (costs off, verbose on) select * from rlp where a = 16 and b <= 'ab';
explain (costs off, verbose on) select * from rlp where a = 16 and b is null;
explain (costs off, verbose on) select * from rlp where a = 16 and b is not null;
explain (costs off, verbose on) select * from rlp where a is null;
explain (costs off, verbose on) select * from rlp where a is not null;
explain (costs off, verbose on) select * from rlp where a > 30;
explain (costs off, verbose on) select * from rlp where a = 30;	/* only default is scanned */
explain (costs off, verbose on) select * from rlp where a <= 31;
explain (costs off, verbose on) select * from rlp where a = 1 or a = 7;
explain (costs off, verbose on) select * from rlp where a = 1 or b = 'ab';

explain (costs off, verbose on) select * from rlp where a > 20 and a < 27;
explain (costs off, verbose on) select * from rlp where a = 29;
explain (costs off, verbose on) select * from rlp where a >= 29;
explain (costs off, verbose on) select * from rlp where a < 1 or (a > 20 and a < 25);

-- redundant clauses are eliminated
explain (costs off, verbose on) select * from rlp where a > 1 and a = 10;	/* only default */
explain (costs off, verbose on) select * from rlp where a > 1 and a >=15;	/* rlp3 onwards, including default */
explain (costs off, verbose on) select * from rlp where a = 1 and a = 3;	/* empty */
explain (costs off, verbose on) select * from rlp where (a = 1 and a = 3) or (a > 1 and a = 15);

-- multi-column keys
create table mc3p (a int, b int, c int) partition by range (a, abs(b), c);
create table mc3p_default partition of mc3p default;
create table mc3p0 partition of mc3p for values from (minvalue, minvalue, minvalue) to (1, 1, 1);
create table mc3p1 partition of mc3p for values from (1, 1, 1) to (10, 5, 10);
create table mc3p2 partition of mc3p for values from (10, 5, 10) to (10, 10, 10);
create table mc3p3 partition of mc3p for values from (10, 10, 10) to (10, 10, 20);
create table mc3p4 partition of mc3p for values from (10, 10, 20) to (10, maxvalue, maxvalue);
create table mc3p5 partition of mc3p for values from (11, 1, 1) to (20, 10, 10);
create table mc3p6 partition of mc3p for values from (20, 10, 10) to (20, 20, 20);
create table mc3p7 partition of mc3p for values from (20, 20, 20) to (maxvalue, maxvalue, maxvalue);

explain (costs off, verbose on) select * from mc3p where a = 1;
explain (costs off, verbose on) select * from mc3p where a = 1 and abs(b) < 1;
explain (costs off, verbose on) select * from mc3p where a = 1 and abs(b) = 1;
explain (costs off, verbose on) select * from mc3p where a = 1 and abs(b) = 1 and c < 8;
explain (costs off, verbose on) select * from mc3p where a = 10 and abs(b) between 5 and 35;
explain (costs off, verbose on) select * from mc3p where a > 10;
explain (costs off, verbose on) select * from mc3p where a >= 10;
explain (costs off, verbose on) select * from mc3p where a < 10;
explain (costs off, verbose on) select * from mc3p where a <= 10 and abs(b) < 10;
explain (costs off, verbose on) select * from mc3p where a = 11 and abs(b) = 0;
explain (costs off, verbose on) select * from mc3p where a = 20 and abs(b) = 10 and c = 100;
explain (costs off, verbose on) select * from mc3p where a > 20;
explain (costs off, verbose on) select * from mc3p where a >= 20;
explain (costs off, verbose on) select * from mc3p where (a = 1 and abs(b) = 1 and c = 1) or (a = 10 and abs(b) = 5 and c = 10) or (a > 11 and a < 20);
explain (costs off, verbose on) select * from mc3p where (a = 1 and abs(b) = 1 and c = 1) or (a = 10 and abs(b) = 5 and c = 10) or (a > 11 and a < 20) or a < 1;
explain (costs off, verbose on) select * from mc3p where (a = 1 and abs(b) = 1 and c = 1) or (a = 10 and abs(b) = 5 and c = 10) or (a > 11 and a < 20) or a < 1 or a = 1;
explain (costs off, verbose on) select * from mc3p where a = 1 or abs(b) = 1 or c = 1;
explain (costs off, verbose on) select * from mc3p where (a = 1 and abs(b) = 1) or (a = 10 and abs(b) = 10);
explain (costs off, verbose on) select * from mc3p where (a = 1 and abs(b) = 1) or (a = 10 and abs(b) = 9);

-- a simpler multi-column keys case
create table mc2p (a int, b int) partition by range (a, b);
create table mc2p_default partition of mc2p default;
create table mc2p0 partition of mc2p for values from (minvalue, minvalue) to (1, minvalue);
create table mc2p1 partition of mc2p for values from (1, minvalue) to (1, 1);
create table mc2p2 partition of mc2p for values from (1, 1) to (2, minvalue);
create table mc2p3 partition of mc2p for values from (2, minvalue) to (2, 1);
create table mc2p4 partition of mc2p for values from (2, 1) to (2, maxvalue);
create table mc2p5 partition of mc2p for values from (2, maxvalue) to (maxvalue, maxvalue);

explain (costs off, verbose on) select * from mc2p where a < 2;
explain (costs off, verbose on) select * from mc2p where a = 2 and b < 1;
explain (costs off, verbose on) select * from mc2p where a > 1;
explain (costs off, verbose on) select * from mc2p where a = 1 and b > 1;

-- all partitions but the default one should be pruned
explain (costs off, verbose on) select * from mc2p where a = 1 and b is null;
explain (costs off, verbose on) select * from mc2p where a is null and b is null;
explain (costs off, verbose on) select * from mc2p where a is null and b = 1;
explain (costs off, verbose on) select * from mc2p where a is null;
explain (costs off, verbose on) select * from mc2p where b is null;

-- boolean partitioning
create table boolpart (a bool) partition by list (a);
create table boolpart_default partition of boolpart default;
create table boolpart_t partition of boolpart for values in ('true');
create table boolpart_f partition of boolpart for values in ('false');

explain (costs off, verbose on) select * from boolpart where a in (true, false);
explain (costs off, verbose on) select * from boolpart where a = false;
explain (costs off, verbose on) select * from boolpart where not a = false;
explain (costs off, verbose on) select * from boolpart where a is true or a is not true;
explain (costs off, verbose on) select * from boolpart where a is not true;
explain (costs off, verbose on) select * from boolpart where a is not true and a is not false;
explain (costs off, verbose on) select * from boolpart where a is unknown;
explain (costs off, verbose on) select * from boolpart where a is not unknown;

create table boolrangep (a bool, b bool, c int) partition by range (a,b,c);
create table boolrangep_tf partition of boolrangep for values from ('true', 'false', 0) to ('true', 'false', 100);
create table boolrangep_ft partition of boolrangep for values from ('false', 'true', 0) to ('false', 'true', 100);
create table boolrangep_ff1 partition of boolrangep for values from ('false', 'false', 0) to ('false', 'false', 50);
create table boolrangep_ff2 partition of boolrangep for values from ('false', 'false', 50) to ('false', 'false', 100);

-- try a more complex case that's been known to trip up pruning in the past
explain (costs off, verbose on)  select * from boolrangep where not a and not b and c = 25;

-- test scalar-to-array operators
create table coercepart (a varchar) partition by list (a);
create table coercepart_ab partition of coercepart for values in ('ab');
create table coercepart_bc partition of coercepart for values in ('bc');
create table coercepart_cd partition of coercepart for values in ('cd');

explain (costs off, verbose on) select * from coercepart where a in ('ab', to_char(125, '999'));
explain (costs off, verbose on) select * from coercepart where a ~ any ('{ab}');
explain (costs off, verbose on) select * from coercepart where a !~ all ('{ab}');
explain (costs off, verbose on) select * from coercepart where a ~ any ('{ab,bc}');
explain (costs off, verbose on) select * from coercepart where a !~ all ('{ab,bc}');
explain (costs off) select * from coercepart where a = any ('{ab,bc}');
explain (costs off) select * from coercepart where a = any ('{ab,null}');
explain (costs off) select * from coercepart where a = any (null::text[]);
explain (costs off) select * from coercepart where a = all ('{ab}');
explain (costs off) select * from coercepart where a = all ('{ab,bc}');
explain (costs off) select * from coercepart where a = all ('{ab,null}');
explain (costs off) select * from coercepart where a = all (null::text[]);

drop table coercepart;

CREATE TABLE part (a INT, b INT) PARTITION BY LIST (a);
CREATE TABLE part_p1 PARTITION OF part FOR VALUES IN (-2,-1,0,1,2);
CREATE TABLE part_p2 PARTITION OF part DEFAULT PARTITION BY RANGE(a);
CREATE TABLE part_p2_p1 PARTITION OF part_p2 DEFAULT;
INSERT INTO part VALUES (-1,-1), (1,1), (2,NULL), (NULL,-2),(NULL,NULL);
explain (costs off, verbose on) SELECT tableoid::regclass as part, a, b FROM part WHERE a IS NULL ORDER BY 1, 2, 3;

--
-- some more cases
--

--
-- pruning for partitioned table appearing inside a sub-query
--
-- pruning won't work for mc3p, because some keys are Params
explain (costs off, verbose on) select * from mc2p t1, lateral (select count(*) from mc3p t2 where t2.a = t1.b and abs(t2.b) = 1 and t2.c = 1) s where t1.a = 1;

-- pruning should work fine, because values for a prefix of keys (a, b) are
-- available
explain (costs off, verbose on) select * from mc2p t1, lateral (select count(*) from mc3p t2 where t2.c = t1.b and abs(t2.b) = 1 and t2.a = 1) s where t1.a = 1;

-- also here, because values for all keys are provided
explain (costs off, verbose on) select * from mc2p t1, lateral (select count(*) from mc3p t2 where t2.a = 1 and abs(t2.b) = 1 and t2.c = 1) s where t1.a = 1;

--
-- pruning with clauses containing <> operator
--

-- doesn't prune range partitions
create table rp (a int) partition by range (a);
create table rp0 partition of rp for values from (minvalue) to (1);
create table rp1 partition of rp for values from (1) to (2);
create table rp2 partition of rp for values from (2) to (maxvalue);

explain (costs off, verbose on) select * from rp where a <> 1;
explain (costs off, verbose on) select * from rp where a <> 1 and a <> 2;

-- null partition should be eliminated due to strict <> clause.
explain (costs off, verbose on) select * from lp where a <> 'a';

-- ensure we detect contradictions in clauses; a can't be NULL and NOT NULL.
explain (costs off, verbose on) select * from lp where a <> 'a' and a is null;
explain (costs off, verbose on) select * from lp where (a <> 'a' and a <> 'd') or a is null;

-- check that it also works for a partitioned table that's not root,
-- which in this case are partitions of rlp that are themselves
-- list-partitioned on b
explain (costs off, verbose on) select * from rlp where a = 15 and b <> 'ab' and b <> 'cd' and b <> 'xy' and b is not null;

--
-- different collations for different keys with same expression
--
create table coll_pruning_multi (a text) partition by range (substr(a, 1) collate "POSIX", substr(a, 1) collate "C");
create table coll_pruning_multi1 partition of coll_pruning_multi for values from ('a', 'a') to ('a', 'e');
create table coll_pruning_multi2 partition of coll_pruning_multi for values from ('a', 'e') to ('a', 'z');
create table coll_pruning_multi3 partition of coll_pruning_multi for values from ('b', 'a') to ('b', 'e');

-- no pruning, because no value for the leading key
explain (costs off, verbose on) select * from coll_pruning_multi where substr(a, 1) = 'e' collate "C";

-- pruning, with a value provided for the leading key
explain (costs off, verbose on) select * from coll_pruning_multi where substr(a, 1) = 'a' collate "POSIX";

-- pruning, with values provided for both keys
explain (costs off, verbose on) select * from coll_pruning_multi where substr(a, 1) = 'e' collate "C" and substr(a, 1) = 'a' collate "POSIX";

--
-- LIKE operators don't prune
--
create table like_op_noprune (a text) partition by list (a);
create table like_op_noprune1 partition of like_op_noprune for values in ('ABC');
create table like_op_noprune2 partition of like_op_noprune for values in ('BCD');
explain (costs off, verbose on) select * from like_op_noprune where a like '%BC';

--
-- tests wherein clause value requires a cross-type comparison function
--
create table lparted_by_int2 (a smallint) partition by list (a);
create table lparted_by_int2_1 partition of lparted_by_int2 for values in (1);
create table lparted_by_int2_16384 partition of lparted_by_int2 for values in (16384);
explain (costs off, verbose on) select * from lparted_by_int2 where a = 100000000000000;

create table rparted_by_int2 (a smallint) partition by range (a);
create table rparted_by_int2_1 partition of rparted_by_int2 for values from (1) to (10);
create table rparted_by_int2_16384 partition of rparted_by_int2 for values from (10) to (16384);
-- all partitions pruned
explain (costs off, verbose on) select * from rparted_by_int2 where a > 100000000000000;
create table rparted_by_int2_maxvalue partition of rparted_by_int2 for values from (16384) to (maxvalue);
-- all partitions but rparted_by_int2_maxvalue pruned
explain (costs off, verbose on) select * from rparted_by_int2 where a > 100000000000000;

drop table lp, coll_pruning, rlp, mc3p, mc2p, boolpart, boolrangep, rp, coll_pruning_multi, like_op_noprune, lparted_by_int2, rparted_by_int2;

--
-- Test Partition pruning for HASH partitioning
--
-- Use hand-rolled hash functions and operator classes to get predictable
-- result on different machines.  See the definitions of
-- part_part_test_int4_ops and part_test_text_ops in insert.sql.
--

create or replace function part_hashint4_noop(value int4, seed int8)
returns int8 as $$
select value + seed;
$$ language sql immutable;

create operator class part_test_int4_ops
for type int4
using hash as
operator 1 =,
function 2 part_hashint4_noop(int4, int8);

create or replace function part_hashtext_length(value text, seed int8)
RETURNS int8 AS $$
select length(coalesce(value, ''))::int8
$$ language sql immutable;

create operator class part_test_text_ops
for type text
using hash as
operator 1 =,
function 2 part_hashtext_length(text, int8);

create table hp (a int, b text, c int)
  partition by hash (a part_test_int4_ops, b part_test_text_ops);
create table hp0 partition of hp for values with (modulus 4, remainder 0);
create table hp3 partition of hp for values with (modulus 4, remainder 3);
create table hp1 partition of hp for values with (modulus 4, remainder 1);
create table hp2 partition of hp for values with (modulus 4, remainder 2);

insert into hp values (null, null, 0);
insert into hp values (1, null, 1);
insert into hp values (1, 'xxx', 2);
insert into hp values (null, 'xxx', 3);
insert into hp values (2, 'xxx', 4);
insert into hp values (1, 'abcde', 5);
select tableoid::regclass, * from hp order by c;

-- test pruning when not all the partitions exist
drop table hp1;
drop table hp3;
explain (costs off) select * from hp where a = 1 and b = 'abcde';
select * from hp where a = 1 and b = 'abcde';
explain (costs off) select * from hp where a = 1 and b = 'abcde' and
  (c = 2 or c = 3);
select * from hp where a = 1 and b = 'abcde' and
  (c = 2 or c = 3);
drop table hp2;
explain (costs off) select * from hp where a = 1 and b = 'abcde' and
  (c = 2 or c = 3);
select * from hp where a = 1 and b = 'abcde' and
  (c = 2 or c = 3);
select tableoid::regclass, * from hp order by c;

drop table hp;
drop function part_hashtext_length CASCADE;
drop function part_hashint4_noop CASCADE;

show enable_fast_query_shipping;
set enable_fast_query_shipping to off;
--
-- Test runtime partition pruning
--
create table ab (a int not null, b int not null) partition by list (a);
create table ab_a2 partition of ab for values in(2) partition by list (b);
create table ab_a2_b1 partition of ab_a2 for values in (1);
create table ab_a2_b2 partition of ab_a2 for values in (2);
create table ab_a2_b3 partition of ab_a2 for values in (3);
create table ab_a1 partition of ab for values in(1) partition by list (b);
create table ab_a1_b1 partition of ab_a1 for values in (1);
create table ab_a1_b2 partition of ab_a1 for values in (2);
create table ab_a1_b3 partition of ab_a1 for values in (3);
create table ab_a3 partition of ab for values in(3) partition by list (b);
create table ab_a3_b1 partition of ab_a3 for values in (1);
create table ab_a3_b2 partition of ab_a3 for values in (2);
create table ab_a3_b3 partition of ab_a3 for values in (3);

prepare ab_q1 (int, int, int) as
select * from ab where a between $1 and $2 and b <= $3;

-- Execute query 5 times to allow choose_custom_plan
-- to start considering a generic plan.
execute ab_q1 (1, 8, 3);
execute ab_q1 (1, 8, 3);
execute ab_q1 (1, 8, 3);
execute ab_q1 (1, 8, 3);
execute ab_q1 (1, 8, 3);

explain (analyze, costs off, summary off, timing off, nodes off) execute ab_q1 (2, 2, 3);
explain (analyze, costs off, summary off, timing off, nodes off, verbose) execute ab_q1 (1, 2, 3);

deallocate ab_q1;

-- Runtime pruning after optimizer pruning
prepare ab_q1 (int, int) as
select a from ab where a between $1 and $2 and b < 3;

-- Execute query 5 times to allow choose_custom_plan
-- to start considering a generic plan.
execute ab_q1 (1, 8);
execute ab_q1 (1, 8);
execute ab_q1 (1, 8);
execute ab_q1 (1, 8);
execute ab_q1 (1, 8);

explain (analyze, costs off, summary off, timing off, nodes off, verbose) execute ab_q1 (2, 2);
explain (analyze, costs off, summary off, timing off, nodes off) execute ab_q1 (2, 4);

-- Ensure a mix of external and exec params work together at different
-- levels of partitioning.
prepare ab_q2 (int, int) as
select a from ab where a between $1 and $2 and b < (select 3);

execute ab_q2 (1, 8);
execute ab_q2 (1, 8);
execute ab_q2 (1, 8);
execute ab_q2 (1, 8);
execute ab_q2 (1, 8);

explain (analyze, costs off, summary off, timing off, nodes off) execute ab_q2 (2, 2);

-- As above, but with swap the exec param to the first partition level
prepare ab_q3 (int, int) as
select a from ab where b between $1 and $2 and a < (select 3);

execute ab_q3 (1, 8);
execute ab_q3 (1, 8);
execute ab_q3 (1, 8);
execute ab_q3 (1, 8);
execute ab_q3 (1, 8);

explain (analyze, costs off, summary off, timing off, nodes off) execute ab_q3 (2, 2);

-- Test a backwards Append scan
create table list_part (a int) partition by list (a);
create table list_part1 partition of list_part for values in (1);
create table list_part2 partition of list_part for values in (2);
create table list_part3 partition of list_part for values in (3);
create table list_part4 partition of list_part for values in (4);

insert into list_part select generate_series(1,4);

begin;

-- Don't select an actual value out of the table as the order of the Append's
-- subnodes may not be stable.
declare cur SCROLL CURSOR for select 1 from list_part where a > (select 1) and a < (select 4);

-- move beyond the final row
move 3 from cur;

-- Ensure we get two rows.
fetch backward all from cur;

commit;

drop table list_part;

-- Parallel append
prepare ab_q4 (int, int) as
select avg(a) from ab where a between $1 and $2 and b < 4;

-- Encourage use of parallel plans
set parallel_setup_cost = 0;
set parallel_tuple_cost = 0;
set min_parallel_table_scan_size = 0;
set max_parallel_workers_per_gather = 2;

-- Execute query 5 times to allow choose_custom_plan
-- to start considering a generic plan.
execute ab_q4 (1, 8);
execute ab_q4 (1, 8);
execute ab_q4 (1, 8);
execute ab_q4 (1, 8);
execute ab_q4 (1, 8);

explain (analyze, costs off, summary off, timing off, nodes off) execute ab_q4 (2, 2);

-- Test run-time pruning with IN lists.
prepare ab_q5 (int, int, int) as
select avg(a) from ab where a in($1,$2,$3) and b < 4;

-- Execute query 5 times to allow choose_custom_plan
-- to start considering a generic plan.
execute ab_q5 (1, 2, 3);
execute ab_q5 (1, 2, 3);
execute ab_q5 (1, 2, 3);
execute ab_q5 (1, 2, 3);
execute ab_q5 (1, 2, 3);

select explain_filter('explain (analyze, costs off, summary off, timing off, nodes off) execute ab_q5 (1, 1, 1);');
select explain_filter('explain (analyze, costs off, summary off, timing off, nodes off) execute ab_q5 (2, 3, 3);');

-- Test parallel Append with IN list and parameterized nested loops
create table lprt_a (a int not null);
-- Insert some values we won't find in ab
insert into lprt_a select 0 from generate_series(1,100);

-- and insert some values that we should find.
insert into lprt_a values(1),(1);

analyze lprt_a;

create index ab_a2_b1_a_idx on ab_a2_b1 (a);
create index ab_a2_b2_a_idx on ab_a2_b2 (a);
create index ab_a2_b3_a_idx on ab_a2_b3 (a);
create index ab_a1_b1_a_idx on ab_a1_b1 (a);
create index ab_a1_b2_a_idx on ab_a1_b2 (a);
create index ab_a1_b3_a_idx on ab_a1_b3 (a);
create index ab_a3_b1_a_idx on ab_a3_b1 (a);
create index ab_a3_b2_a_idx on ab_a3_b2 (a);
create index ab_a3_b3_a_idx on ab_a3_b3 (a);

set enable_hashjoin = 0;
set enable_mergejoin = 0;

prepare ab_q6 (int, int, int) as
select avg(ab.a) from ab inner join lprt_a a on ab.a = a.a where a.a in($1,$2,$3);
execute ab_q6 (1, 2, 3);
execute ab_q6 (1, 2, 3);
execute ab_q6 (1, 2, 3);
execute ab_q6 (1, 2, 3);
execute ab_q6 (1, 2, 3);

select explain_filter('explain (analyze, costs off, summary off, timing off, nodes off) execute ab_q6 (0, 0, 1);');

insert into lprt_a values(3),(3);

select explain_filter('explain (analyze, costs off, summary off, timing off, nodes off) execute ab_q6 (1, 0, 3);');
select explain_filter('explain (analyze, costs off, summary off, timing off, nodes off) execute ab_q6 (1, 0, 0);');

delete from lprt_a where a = 1;

select explain_filter('explain (analyze, costs off, summary off, timing off, nodes off) execute ab_q6 (1, 0, 0);');

reset enable_hashjoin;
reset enable_mergejoin;
reset parallel_setup_cost;
reset parallel_tuple_cost;
reset min_parallel_table_scan_size;
reset max_parallel_workers_per_gather;

-- Test run-time partition pruning with an initplan
explain (analyze, costs off, summary off, timing off, nodes off)
select * from ab where a = (select max(a) from lprt_a) and b = (select max(a)-1 from lprt_a);

-- A case containing a UNION ALL with a non-partitioned child.
explain (analyze, costs off, summary off, timing off)
select * from (select * from ab where a = 1 union all (values(10,5)) union all select * from ab) ab where b = (select 1);

deallocate ab_q1;
deallocate ab_q2;
deallocate ab_q3;
deallocate ab_q4;
deallocate ab_q5;
deallocate ab_q6;

-- UPDATE on a partition subtree has been seen to have problems.
insert into ab values (1,2);
explain (analyze, costs off, summary off, timing off)
update ab_a1 set b = 3 from ab where ab.a = 1 and ab.a = ab_a1.a;
table ab;

-- Test UPDATE where source relation has run-time pruning enabled
truncate ab;
insert into ab values (1, 1), (1, 2), (1, 3), (2, 1);
explain (analyze, costs off, summary off, timing off)
update ab_a1 set b = 3 from ab_a2 where ab_a2.b = (select 1);
select tableoid::regclass, * from ab order by a;

drop table ab, lprt_a;

-- Join
create table tbl1(col1 int);
insert into tbl1 values (501), (505);

-- Basic table
create table tprt (col1 int) partition by range (col1);
create table tprt_1 partition of tprt for values from (1) to (501);
create table tprt_2 partition of tprt for values from (501) to (1001);
create table tprt_3 partition of tprt for values from (1001) to (2001);
create table tprt_4 partition of tprt for values from (2001) to (3001);
create table tprt_5 partition of tprt for values from (3001) to (4001);
create table tprt_6 partition of tprt for values from (4001) to (5001);

create index tprt1_idx on tprt_1 (col1);
create index tprt2_idx on tprt_2 (col1);
create index tprt3_idx on tprt_3 (col1);
create index tprt4_idx on tprt_4 (col1);
create index tprt5_idx on tprt_5 (col1);
create index tprt6_idx on tprt_6 (col1);

insert into tprt values (10), (20), (501), (502), (505), (1001), (4500);

set enable_hashjoin = off;
set enable_mergejoin = off;

select explain_filter('explain (analyze, costs off, summary off, timing off, nodes off)
select * from tbl1 join tprt on tbl1.col1 > tprt.col1;');

select explain_filter('explain (analyze, costs off, summary off, timing off, nodes off)
select * from tbl1 join tprt on tbl1.col1 = tprt.col1;');

select tbl1.col1, tprt.col1 from tbl1
inner join tprt on tbl1.col1 > tprt.col1
order by tbl1.col1, tprt.col1;

select tbl1.col1, tprt.col1 from tbl1
inner join tprt on tbl1.col1 = tprt.col1
order by tbl1.col1, tprt.col1;

-- Multiple partitions
insert into tbl1 values (1001), (1010), (1011);
select explain_filter('explain (analyze, costs off, summary off, timing off, nodes off)
select * from tbl1 inner join tprt on tbl1.col1 > tprt.col1;');

select explain_filter('explain (analyze, costs off, summary off, timing off, nodes off)
select * from tbl1 inner join tprt on tbl1.col1 = tprt.col1;');

select tbl1.col1, tprt.col1 from tbl1
inner join tprt on tbl1.col1 > tprt.col1
order by tbl1.col1, tprt.col1;

select tbl1.col1, tprt.col1 from tbl1
inner join tprt on tbl1.col1 = tprt.col1
order by tbl1.col1, tprt.col1;

-- Last partition
delete from tbl1;
insert into tbl1 values (4400);
select explain_filter('explain (analyze, costs off, summary off, timing off, nodes off)
select * from tbl1 join tprt on tbl1.col1 < tprt.col1;');

select tbl1.col1, tprt.col1 from tbl1
inner join tprt on tbl1.col1 < tprt.col1
order by tbl1.col1, tprt.col1;

-- No matching partition
delete from tbl1;
insert into tbl1 values (10000);
select explain_filter('explain (analyze, costs off, summary off, timing off, nodes off)
select * from tbl1 join tprt on tbl1.col1 = tprt.col1;');

select tbl1.col1, tprt.col1 from tbl1
inner join tprt on tbl1.col1 = tprt.col1
order by tbl1.col1, tprt.col1;

drop table tbl1, tprt;

-- Test with columns defined in varying orders between each level
create table part_abc (a int not null, b int not null, c int not null) partition by list (a);
create table part_bac (b int not null, a int not null, c int not null) partition by list (b);
create table part_cab (c int not null, a int not null, b int not null) partition by list (c);
create table part_abc_p1 (a int not null, b int not null, c int not null);

alter table part_abc attach partition part_bac for values in(1);
alter table part_bac attach partition part_cab for values in(2);
alter table part_cab attach partition part_abc_p1 for values in(3);

prepare part_abc_q1 (int, int, int) as
select * from part_abc where a = $1 and b = $2 and c = $3;

-- Execute query 5 times to allow choose_custom_plan
-- to start considering a generic plan.
execute part_abc_q1 (1, 2, 3);
execute part_abc_q1 (1, 2, 3);
execute part_abc_q1 (1, 2, 3);
execute part_abc_q1 (1, 2, 3);
execute part_abc_q1 (1, 2, 3);

-- Single partition should be scanned.
explain (analyze, costs off, summary off, timing off, nodes off) execute part_abc_q1 (1, 2, 3);

deallocate part_abc_q1;

drop table part_abc;

-- Ensure that an Append node properly handles a sub-partitioned table
-- matching without any of its leaf partitions matching the clause.
create table listp (a int, b int) partition by list (a);
create table listp_1 partition of listp for values in(1) partition by list (b);
create table listp_1_1 partition of listp_1 for values in(1);
create table listp_2 partition of listp for values in(2) partition by list (b);
create table listp_2_1 partition of listp_2 for values in(2);
select * from listp where b = 1;

-- Ensure that an Append node properly can handle selection of all first level
-- partitions before finally detecting the correct set of 2nd level partitions
-- which match the given parameter.
prepare q1 (int,int) as select * from listp where b in ($1,$2);

execute q1 (1,2);
execute q1 (1,2);
execute q1 (1,2);
execute q1 (1,2);
execute q1 (1,2);

explain (analyze, costs off, summary off, timing off, nodes off, verbose)  execute q1 (1,1);

explain (analyze, costs off, summary off, timing off, nodes off, verbose)  execute q1 (2,2);

-- Try with no matching partitions. One subplan should remain in this case,
-- but it shouldn't be executed.
explain (analyze, costs off, summary off, timing off, nodes off, verbose)  execute q1 (0,0);

deallocate q1;

-- Test more complex cases where a not-equal condition further eliminates partitions.
prepare q1 (int,int,int,int) as select * from listp where b in($1,$2) and $3 <> b and $4 <> b;

execute q1 (1,2,3,4);
execute q1 (1,2,3,4);
execute q1 (1,2,3,4);
execute q1 (1,2,3,4);
execute q1 (1,2,3,4);

-- Both partitions allowed by IN clause, but one disallowed by <> clause
explain (analyze, costs off, summary off, timing off, nodes off)  execute q1 (1,2,2,0);

-- Both partitions allowed by IN clause, then both excluded again by <> clauses.
-- One subplan will remain in this case, but it should not be executed.
explain (analyze, costs off, summary off, timing off, nodes off)  execute q1 (1,2,2,1);

-- Ensure Params that evaluate to NULL properly prune away all partitions
explain (analyze, costs off, summary off, timing off)
select * from listp where a = (select null::int);

drop table listp;

--
-- Check that pruning with composite range partitioning works correctly when
-- it must ignore clauses for trailing keys once it has seen a clause with
-- non-inclusive operator for an earlier key
--
create table mc3p (a int, b int, c int) partition by range (a, abs(b), c);
create table mc3p0 partition of mc3p
  for values from (0, 0, 0) to (0, maxvalue, maxvalue);
create table mc3p1 partition of mc3p
  for values from (1, 1, 1) to (2, minvalue, minvalue);
create table mc3p2 partition of mc3p
  for values from (2, minvalue, minvalue) to (3, maxvalue, maxvalue);
insert into mc3p values (0, 1, 1), (1, 1, 1), (2, 1, 1);

explain (analyze, costs off, summary off, timing off)
select * from mc3p where a < 3 and abs(b) = 1;

drop table mc3p;

--
-- check that stable query clauses are only used in run-time pruning
--
create table stable_qual_pruning (a timestamp) partition by range (a);
create table stable_qual_pruning1 partition of stable_qual_pruning
  for values from ('2000-01-01') to ('2000-02-01');
create table stable_qual_pruning2 partition of stable_qual_pruning
  for values from ('2000-02-01') to ('2000-03-01');
create table stable_qual_pruning3 partition of stable_qual_pruning
  for values from ('3000-02-01') to ('3000-03-01');

-- comparison against a stable value requires run-time pruning
explain (analyze, verbose, costs off, summary off, timing off)
select * from stable_qual_pruning where a < localtimestamp;

-- timestamp < timestamptz comparison is only stable, not immutable
explain (analyze, verbose, costs off, summary off, timing off)
select * from stable_qual_pruning where a < '2000-02-01'::timestamptz;

-- check ScalarArrayOp cases
explain (analyze, verbose, costs off, summary off, timing off)
select * from stable_qual_pruning
  where a = any(array['2010-02-01', '2020-01-01']::timestamp[]);
explain (analyze, verbose, costs off, summary off, timing off)
select * from stable_qual_pruning
  where a = any(array['2000-02-01', '2010-01-01']::timestamp[]);
explain (analyze, verbose, costs off, summary off, timing off)
select * from stable_qual_pruning
  where a = any(array['2000-02-01', localtimestamp]::timestamp[]);
explain (analyze, verbose, costs off, summary off, timing off)
select * from stable_qual_pruning
  where a = any(array['2010-02-01', '2020-01-01']::timestamptz[]);
explain (analyze, verbose, costs off, summary off, timing off)
select * from stable_qual_pruning
  where a = any(array['2000-02-01', '2010-01-01']::timestamptz[]);

drop table stable_qual_pruning;

-- Ensure runtime pruning works with initplans params with boolean types
create table boolvalues (value bool not null);
insert into boolvalues values('t'),('f');

create table boolp (a bool) partition by list (a);
create table boolp_t partition of boolp for values in('t');
create table boolp_f partition of boolp for values in('f');

explain (analyze, costs off, summary off, timing off, nodes off)
select * from boolp where a = (select value from boolvalues where value);

explain (analyze, costs off, summary off, timing off, nodes off)
select * from boolp where a = (select value from boolvalues where not value);

drop table boolp;

reset enable_fast_query_shipping;

--
-- check that pruning works properly when the partition key is of a
-- pseudotype
--

-- array type list partition key
create table pp_arrpart (a int[]) partition by list (a);
create table pp_arrpart1 partition of pp_arrpart for values in ('{1}');
create table pp_arrpart2 partition of pp_arrpart for values in ('{2, 3}', '{4, 5}');
explain (costs off, nodes off) select * from pp_arrpart where a = '{1}';
explain (costs off, nodes off) select * from pp_arrpart where a = '{1, 2}';
explain (costs off, nodes off) select * from pp_arrpart where a in ('{4, 5}', '{1}');
drop table pp_arrpart;

-- array type hash partition key
create table pph_arrpart (a int[]) partition by hash (a);
create table pph_arrpart1 partition of pph_arrpart for values with (modulus 2, remainder 0);
create table pph_arrpart2 partition of pph_arrpart for values with (modulus 2, remainder 1);
insert into pph_arrpart values ('{1}'), ('{1, 2}'), ('{4, 5}');
select tableoid::regclass, * from pph_arrpart order by 1;
explain (costs off, nodes off) select * from pph_arrpart where a = '{1}';
explain (costs off, nodes off) select * from pph_arrpart where a = '{1, 2}';
explain (costs off, nodes off) select * from pph_arrpart where a in ('{4, 5}', '{1}');
drop table pph_arrpart;

-- enum type list partition key
create type pp_colors as enum ('green', 'blue', 'black');
create table pp_enumpart (a pp_colors) partition by list (a);
create table pp_enumpart_green partition of pp_enumpart for values in ('green');
create table pp_enumpart_blue partition of pp_enumpart for values in ('blue');
explain (costs off, nodes off) select * from pp_enumpart where a = 'blue';
explain (costs off, nodes off) select * from pp_enumpart where a = 'black';
drop table pp_enumpart;
drop type pp_colors;

-- record type as partition key
create type pp_rectype as (a int, b int);
create table pp_recpart (a pp_rectype) partition by list (a);
create table pp_recpart_11 partition of pp_recpart for values in ('(1,1)');
create table pp_recpart_23 partition of pp_recpart for values in ('(2,3)');
explain (costs off, nodes off) select * from pp_recpart where a = '(1,1)'::pp_rectype;
explain (costs off, nodes off) select * from pp_recpart where a = '(1,2)'::pp_rectype;
drop table pp_recpart;
drop type pp_rectype;

-- range type partition key
create table pp_intrangepart (a int4range) partition by list (a);
create table pp_intrangepart12 partition of pp_intrangepart for values in ('[1,2]');
create table pp_intrangepart2inf partition of pp_intrangepart for values in ('[2,)');
explain (costs off, nodes off) select * from pp_intrangepart where a = '[1,2]'::int4range;
explain (costs off, nodes off) select * from pp_intrangepart where a = '(1,2)'::int4range;
drop table pp_intrangepart;

--
-- Ensure the enable_partition_prune GUC properly disables partition pruning.
--

create table pp_lp (a int, value int) partition by list (a);
create table pp_lp1 partition of pp_lp for values in(1);
create table pp_lp2 partition of pp_lp for values in(2);

explain (costs off, verbose on) select * from pp_lp where a = 1;
explain (costs off, verbose on) update pp_lp set value = 10 where a = 1;
explain (costs off, verbose on) delete from pp_lp where a = 1;

set enable_partition_pruning = off;

set constraint_exclusion = 'partition'; -- this should not affect the result.

explain (costs off, verbose on) select * from pp_lp where a = 1;
explain (costs off, verbose on) update pp_lp set value = 10 where a = 1;
explain (costs off, verbose on) delete from pp_lp where a = 1;

set constraint_exclusion = 'off'; -- this should not affect the result.

explain (costs off, verbose on) select * from pp_lp where a = 1;
explain (costs off, verbose on) update pp_lp set value = 10 where a = 1;
explain (costs off, verbose on) delete from pp_lp where a = 1;

drop table pp_lp;

-- Ensure enable_partition_prune does not affect non-partitioned tables.

create table inh_lp (a int, value int);
create table inh_lp1 (a int, value int, check(a = 1)) inherits (inh_lp);
create table inh_lp2 (a int, value int, check(a = 2)) inherits (inh_lp);

set constraint_exclusion = 'partition';

-- inh_lp2 should be removed in the following 3 cases.
explain (costs off, verbose on) select * from inh_lp where a = 1;
explain (costs off, verbose on) update inh_lp set value = 10 where a = 1;
explain (costs off, verbose on) delete from inh_lp where a = 1;

-- Ensure we don't exclude normal relations when we only expect to exclude
-- inheritance children
explain (costs off, verbose on) update inh_lp1 set value = 10 where a = 2;

\set VERBOSITY terse	\\ -- suppress cascade details
drop table inh_lp cascade;
\set VERBOSITY default

reset enable_partition_pruning;
reset constraint_exclusion;

-- Check pruning for a partition tree containing only temporary relations
create temp table pp_temp_parent (a int) partition by list (a);
create temp table pp_temp_part_1 partition of pp_temp_parent for values in (1);
create temp table pp_temp_part_def partition of pp_temp_parent default;
explain (costs off) select * from pp_temp_parent where true;
explain (costs off) select * from pp_temp_parent where a = 2;
drop table pp_temp_parent;

-- Stress run-time partition pruning a bit more, per bug reports
create temp table p (a int, b int, c int) partition by list (a);
create temp table p1 partition of p for values in (1);
create temp table p2 partition of p for values in (2);
create temp table q (a int, b int, c int) partition by list (a);
create temp table q1 partition of q for values in (1) partition by list (b);
create temp table q11 partition of q1 for values in (1) partition by list (c);
create temp table q111 partition of q11 for values in (1);
create temp table q2 partition of q for values in (2) partition by list (b);
create temp table q21 partition of q2 for values in (1);
create temp table q22 partition of q2 for values in (2);

insert into q22 values (2, 2, 3);

explain (costs off)
select *
from (
      select * from p
      union all
      select * from q1
      union all
      select 1, 1, 1
     ) s(a, b, c)
where s.a = 1 and s.b = 1 and s.c = (select 1);

select *
from (
      select * from p
      union all
      select * from q1
      union all
      select 1, 1, 1
     ) s(a, b, c)
where s.a = 1 and s.b = 1 and s.c = (select 1);

prepare q (int, int) as
select *
from (
      select * from p
      union all
      select * from q1
      union all
      select 1, 1, 1
     ) s(a, b, c)
where s.a = $1 and s.b = $2 and s.c = (select 1);

explain (costs off) execute q (1, 1);
execute q (1, 1);

drop table p, q;

-- Ensure run-time pruning works correctly when we match a partitioned table
-- on the first level but find no matching partitions on the second level.
create table listp (a int, b int) partition by list (a);
create table listp1 partition of listp for values in(1);
create table listp2 partition of listp for values in(2) partition by list(b);
create table listp2_10 partition of listp2 for values in (10);

explain (costs off, summary off, timing off)
select * from listp where a = (select 2) and b <> 10;
select * from listp where a = (select 2) and b <> 10;

--
-- check that a partition directly accessed in a query is excluded with
-- constraint_exclusion = on
--

-- turn off partition pruning, so that it doesn't interfere
set enable_partition_pruning to off;

-- setting constraint_exclusion to 'partition' disables exclusion
set constraint_exclusion to 'partition';
explain (costs off, verbose on) select * from listp1 where a = 2;
explain (costs off, verbose on) update listp1 set a = 1 where a = 2;
-- constraint exclusion enabled
set constraint_exclusion to 'on';
explain (costs off, verbose on) select * from listp1 where a = 2;
explain (costs off, verbose on) update listp1 set a = 1 where a = 2;

reset constraint_exclusion;
reset enable_partition_pruning;

drop table listp;



-- support multi partition tables
create table T_TA_CONFIRM
(
  busi_date            varchar(8),
  custno               numeric(10),
  d_cdate              varchar(8)
) partition by range (d_cdate) 
;
create table P_TACONFIRM_2006 partition of T_TA_CONFIRM for values from ('20000101') to ('20070101');

create table T_LOF_CONFIRM
(
  busi_date            varchar(8),
  custno               numeric(10),
  d_cdate              varchar(8)
) partition by range (d_cdate) 
;

create table P_LOFCONFIRM_2014 partition of T_LOF_CONFIRM for values from ('20010101') to ('20150101');

create table T_LOF_CONFIRM1
(
  busi_date            varchar(8),
  custno               numeric(10),
  d_cdate              varchar(8)
) 
;
create or replace view v_dc_confirm
as
select * from t_ta_confirm a
union all
select * from t_lof_confirm b
union all
select * from t_lof_confirm1 c;

select * from v_dc_confirm where custno=123;

select * from (
    select * from t_ta_confirm a
    union all
    select * from t_lof_confirm b
    union all
    select * from t_lof_confirm1 c
) a where a.custno=123;

drop table T_TA_CONFIRM cascade;
drop table T_LOF_CONFIRM cascade;
drop table T_LOF_CONFIRM1 cascade;

-- Partition table union to fix the plan distribution error
drop table if exists t_ab, t_cd;
create table t_cd (a int not null, b int not null) partition by list (a);
create table t_cd_a2 partition of t_cd for values in(2) partition by list (b);
create table t_cd_a2_b1 partition of t_cd_a2 for values in (1);
create table t_cd_a2_b2 partition of t_cd_a2 for values in (2);
create table t_cd_a2_b3 partition of t_cd_a2 for values in (3);
create table t_cd_a1 partition of t_cd for values in(1) partition by list (b);
create table t_cd_a1_b1 partition of t_cd_a1 for values in (1);
create table t_cd_a1_b2 partition of t_cd_a1 for values in (2);
create table t_cd_a1_b3 partition of t_cd_a1 for values in (3);
create table t_cd_a3 partition of t_cd for values in(3) partition by list (b);
create table t_cd_a3_b1 partition of t_cd_a3 for values in (1);
create table t_cd_a3_b2 partition of t_cd_a3 for values in (2);
create table t_cd_a3_b3 partition of t_cd_a3 for values in (3);
insert into t_cd values (1, 1), (1, 2), (1, 3), (2, 1), (3,1);
create table t_ab (a int not null, b int not null) partition by list (a);
create table t_ab_a2 partition of t_ab for values in(2) partition by list (b);
create table t_ab_a2_b1 partition of t_ab_a2 for values in (1);
create table t_ab_a2_b2 partition of t_ab_a2 for values in (2);
create table t_ab_a2_b3 partition of t_ab_a2 for values in (3);
create table t_ab_a1 partition of t_ab for values in(1) partition by list (b);
create table t_ab_a1_b1 partition of t_ab_a1 for values in (1);
create table t_ab_a1_b2 partition of t_ab_a1 for values in (2);
create table t_ab_a1_b3 partition of t_ab_a1 for values in (3);
create table t_ab_a3 partition of t_ab for values in(3) partition by list (b);
create table t_ab_a3_b1 partition of t_ab_a3 for values in (1);
create table t_ab_a3_b2 partition of t_ab_a3 for values in (2);
create table t_ab_a3_b3 partition of t_ab_a3 for values in (3);
insert into t_ab values (1, 1), (1, 2), (1, 3), (2, 1), (3,1);
explain (costs off) 
select * from t_ab where a = 1  union all select * from t_cd;
select * from t_ab where a = 1  union all select * from t_cd order by 1,2;
explain (costs off) 
select * from t_cd union all select * from t_ab where a = 1;
select * from t_cd union all select * from t_ab where a = 1 order by 1,2;
drop table t_ab, t_cd;

-- OPENTENBASE
-- Improve run-time partition pruning to handle any stable expression.
drop table if exists part_range_t2 cascade;
create table public.part_range_t2                                                                                   
(
f1 int not null,
f2 date not null default now(),
f3 varchar(50),
f4 integer) 
partition by range (f2);

--创建子分区：
create table part_range_t2_part_0 partition of public.part_range_t2 for values from ('2020-06-01') to ('2020-07-01');
create table part_range_t2_part_1 partition of part_range_t2 for values from ('2020-07-01') to ('2020-08-01');
create table part_range_t2_part_2 partition of part_range_t2 for values from ('2020-08-01') to ('2020-09-01');
insert into part_range_t2(f1,f2,f3,f4) values(1,'2020-06-01','one',2);
insert into part_range_t2(f1,f2,f3,f4) values(2,'2020-07-01','two',3);
insert into part_range_t2(f1,f2,f3,f4) values(3,'2020-08-01','three',4);
insert into part_range_t2(f1,f2,f3,f4) values(4,'2020-08-21','four',4);

explain(costs off, summary off, timing off, nodes off, verbose)
select * from part_range_t2 where f2 > to_date('2020-07-02','yyyy-mm-dd')+1 and f2 < to_date('2020-07-28','yyyy-mm-dd');
select * from part_range_t2 where f2 > to_date('2020-07-02','yyyy-mm-dd')+1 and f2 < to_date('2020-07-28','yyyy-mm-dd');
explain(costs off, summary off, timing off, nodes off, verbose)
select * from part_range_t2 where f2 > to_date('2020-08-02','yyyy-mm-dd')+1 and f2 < to_date('2020-09-28','yyyy-mm-dd');
select * from part_range_t2 where f2 > to_date('2020-08-02','yyyy-mm-dd')+1 and f2 < to_date('2020-09-28','yyyy-mm-dd');

drop table part_range_t2 cascade;

-- Parttion table with null array
drop table if exists t_stable_qual_pruning_20221219 cascade;
create table t_stable_qual_pruning_20221219 (a int, b varchar(20)) partition by range (a);
create table t_stable_qual_pruning_20221219_1 partition of t_stable_qual_pruning_20221219
 for values from (1) to (10);
create table t_stable_qual_pruning_20221219_2 partition of t_stable_qual_pruning_20221219
 for values from (11) to (20);
create table t_stable_qual_pruning_20221219_3 partition of t_stable_qual_pruning_20221219
 for values from (21) to (30);
insert into t_stable_qual_pruning_20221219 values(2,'part1');
insert into t_stable_qual_pruning_20221219 values(12,'part2');
insert into t_stable_qual_pruning_20221219 values(22,'part3');
select * from t_stable_qual_pruning_20221219 where a = any(null::int[]);
select * from t_stable_qual_pruning_20221219 where a = some(null::int[]);
select * from t_stable_qual_pruning_20221219 where a = some(null::int[]) or b='part2';
select * from t_stable_qual_pruning_20221219 where a = some('{1,2,3}'::int[]);
drop table if exists t_stable_qual_pruning_20221219 cascade;
create table t_stable_qual_pruning_20221219 (a timestamp) partition by range (a);
create table t_stable_qual_pruning_20221219_1 partition of t_stable_qual_pruning_20221219
for values from ('2000-01-01') to ('2000-02-01');
create table t_stable_qual_pruning_20221219_2 partition of t_stable_qual_pruning_20221219
for values from ('2000-02-01') to ('2000-03-01');
create table t_stable_qual_pruning_20221219_3 partition of t_stable_qual_pruning_20221219
for values from ('3000-02-01') to ('3000-03-01');
explain select * from t_stable_qual_pruning_20221219 where a = any(null::timestamptz[]);
drop table t_stable_qual_pruning_20221219;
drop table if exists rqg_table1;
drop table if exists rqg_table2;
drop table if exists rqg_table3;
drop table if exists rqg_table6;

CREATE TABLE rqg_table1 (
c0 int,
c1 int,
c2 text,
c3 text,
c4 date,
c5 date,
c6 timestamp,
c7 timestamp,
c8 numeric,
c9 numeric)   ;
alter table rqg_table1 alter column c0 drop not null;
CREATE INDEX idx_rqg_table1_c1 ON rqg_table1(c1);
CREATE INDEX idx_rqg_table1_c3 ON rqg_table1(c3);
CREATE INDEX idx_rqg_table1_c5 ON rqg_table1(c5);
CREATE INDEX idx_rqg_table1_c7 ON rqg_table1(c7);
CREATE INDEX idx_rqg_table1_c9 ON rqg_table1(c9);
INSERT INTO rqg_table1 VALUES  (0, 7, NULL, 'grtbgropc', '2035-05-04 01:32:53', '1994-08-11', '1984-02-17 00:11:30.027457', '1995-05-12 02:34:35.042767', -1.23456789123457e-09, -1.23456789123457e+39) ,  (7, 0, NULL, 'foo', '2014-03-07', '2011-02-03 23:39:34', NULL, '1980-03-23 13:00:15.056082', -7.16466405719304e+18, 1.23456789123457e-09) ,  (7, 3, NULL, 'foo', '2015-01-05 10:54:04', '1982-09-27 21:55:32', '2011-10-10', '1976-01-26 21:58:12.013563', -8.39080810546875e+80, -1.98749481055394e+18) ,  (7, 6, 'k', 'bar', '2022-08-23 15:52:51', '2027-03-01 00:51:59', '2000-08-20 07:44:44.041735', '2019-10-14 02:21:41.038361', -1.23456789123457e+25, -1.23456789123457e+30) ,  (2, NULL, 'foo', 'bar', '1992-07-12 10:53:44', '1975-10-21', '1982-11-20 19:52:27.051969', '2008-06-13 19:15:00.055581', -2.00790404474228e+124, 1.23456789123457e+43) ,  (NULL, 0, 'bar', 'bar', '1993-09-15 11:58:10', '2031-06-16 11:15:08', '1977-06-04 12:40:57.026076', '1996-06-05', NULL, 5.44428899953751e+18) ,  (3, 1, 'bar', 'rtbgropczv', '2002-08-24', '1994-08-27', '2002-03-10 05:46:29', '1992-08-12 18:26:46.044733', 927531008, -3.77349852892975e+124) ,  (5, 4, NULL, 'foo', '2004-09-04 16:17:01', '2021-08-20 22:27:23', '2032-01-19', '1998-08-22 03:16:09.028737', 4.530029296875e+80, 1.23456789123457e+30) ,  (4, 0, 'tbgro', 'bar', '2030-09-10', '2019-07-21', '1992-01-06 15:08:45.062014', NULL, -1.23456789123457e+43, -1.23456789123457e+39) ,  (1, 1, NULL, 'foo', '1976-05-20 12:13:56', '1984-12-25', '2008-05-20 06:32:37.019074', '2021-11-08 22:12:49.045972', -0.123456789123457, 1.23456789123457e+30) ;
CREATE TABLE rqg_table2 (
c0 int,
c1 int,
c2 text,
c3 text,
c4 date,
c5 date,
c6 timestamp,
c7 timestamp,
c8 numeric,
c9 numeric)   distribute by replication;
alter table rqg_table2 alter column c0 drop not null;
CREATE INDEX idx_rqg_table2_c1 ON rqg_table2(c1);
CREATE INDEX idx_rqg_table2_c3 ON rqg_table2(c3);
CREATE INDEX idx_rqg_table2_c5 ON rqg_table2(c5);
CREATE INDEX idx_rqg_table2_c7 ON rqg_table2(c7);
CREATE INDEX idx_rqg_table2_c9 ON rqg_table2(c9);
INSERT INTO rqg_table2 VALUES  (5, 6, 'bar', 'bar', '2033-10-09', '2031-05-21', '2006-12-22 00:42:22.054501', '2002-03-14', 0.123456789123457, -7.10358398724683e+18) ,  (NULL, 4, 'bar', 'q', '2034-12-23 18:13:57', '2010-03-20', '1971-12-10 21:29:50', NULL, -1.42059326171875e+80, -1.23456789123457e+43) ,  (3, 4, 'j', 'bgrop', '1978-11-03', '2011-12-23', '1985-09-20', '1994-08-05 12:12:00.055930', -0.123456789123457, -1.23456789123457e-09) ,  (NULL, NULL, 'foo', 'p', '2024-11-04 11:49:59', '2032-04-11', '1988-03-26 06:06:18.056077', '1971-05-18 16:17:58.028951', -8.0774874066657e+18, 0.123456789123457) ,  (NULL, 8, NULL, 'u', '2021-09-27 02:53:41', '1998-06-25 01:49:57', '1992-09-04', '1976-12-21 16:40:06.029110', 1.23456789123457e-09, -1.23456789123457e+39) ,  (NULL, 0, 'gr', 'ropczvgdcu', '1987-03-24 11:52:34', '1999-06-12', '1983-10-27 13:16:42.032106', '1985-03-28 05:46:56.035012', -3.55438232421875e+80, -1.23456789123457e+39) ,  (2, 4, NULL, 'o', '2029-04-25', '2031-07-07 03:20:01', '2019-10-15 21:32:53.047638', '2006-12-02 12:48:09.009854', -5.94650268149338e+124, 0.123456789123457) ,  (9, NULL, 'pczv', 'foo', '1982-11-04 02:02:05', '1985-10-18', NULL, '1989-02-20', -1.23456789123457e-09, -0.123456789123457) ,  (7, NULL, 'foo', 'bar', '2024-04-27', '2026-03-17 19:05:08', '1994-04-25 12:53:41.048158', '2010-04-21 06:12:22.055281', 0.123456789123457, -9.53074271142281e+17) ,  (5, 8, 'z', 'foo', '2034-12-28 17:39:46', '1971-08-19', '1997-10-21 09:14:55.047253', '1979-11-06 07:03:33.045679', -1.23456789123457e+30, 0.514114379882812) ;
CREATE TABLE rqg_table3 (
c0 int,
c1 int,
c2 text,
c3 text,
c4 date,
c5 date,
c6 timestamp,
c7 timestamp,
c8 numeric,
c9 numeric)    PARTITION BY hash( c4) ;
create TABLE rqg_table3_p0 partition of rqg_table3 for values with(modulus 2,remainder 0);
create TABLE rqg_table3_p1 partition of rqg_table3 for values with(modulus 2,remainder 1);
alter table rqg_table3 alter column c0 drop not null;
CREATE INDEX idx_rqg_table3_c1 ON rqg_table3(c1);
CREATE INDEX idx_rqg_table3_c3 ON rqg_table3(c3);
CREATE INDEX idx_rqg_table3_c5 ON rqg_table3(c5);
CREATE INDEX idx_rqg_table3_c7 ON rqg_table3(c7);
CREATE INDEX idx_rqg_table3_c9 ON rqg_table3(c9);
INSERT INTO rqg_table3 VALUES  (NULL, 4, 'bar', NULL, '1995-05-05 04:48:57', '2015-12-27', '1975-07-18 06:53:25.042898', '2010-04-10 23:51:57.007429', -1.23456789123457e+43, -1.23456789123457e+39) ,  (NULL, 9, NULL, 'bar', '1986-09-23 04:49:01', '1990-03-20', '1981-07-09 21:40:37.026251', '2032-05-28 14:05:34.030576', -0.123456789123457, -1.23456789123457e+43) ,  (NULL, 2, 'y', 'foo', '1988-04-21 10:52:14', '1979-01-01 02:13:52', NULL, '2018-09-27 09:22:47.006555', 1768620032, NULL) ,  (9, 0, 'bar', 'bar', '2034-12-28', '1977-03-19', NULL, '1997-09-06 23:07:45', 0.812881469726562, -388169728) ,  (2, NULL, 'czvgdcuq', 'o', '2025-03-26', '2029-08-14 02:34:20', '1985-07-01 07:09:51.031974', '2015-04-06 07:39:00', -1.23456789123457e+25, -7.41531371811844e+124) ,  (9, 7, 'i', NULL, '1994-11-21 16:19:13', '2018-08-26', '2031-04-08', '2025-04-19', -1.23456789123457e+44, -1.23456789123457e+25) ,  (NULL, NULL, 'zvgdcu', 'vgdcuqdw', '2002-10-18', '1992-02-01', '1971-10-13 04:18:50.058294', '2032-03-23 10:06:10', 1.23456789123457e+39, 1.23456789123457e+43) ,  (NULL, NULL, 'bar', 'bar', '2019-03-01 04:33:48', '2005-01-07', '2007-04-05', '1995-01-19 23:06:55.048886', 1.23456789123457e+39, NULL) ,  (4, 7, 'u', NULL, '2011-01-01', '1979-01-27 03:51:15', '1999-12-25', '1971-05-04 11:46:30', 1.23456789123457e-09, 0.805313110351562) ,  (7, 6, 'bar', 'foo', '2032-12-12', '1971-07-20 15:55:11', '2012-04-19 20:34:42.010272', '1989-04-03 03:21:42.002114', 0.123456789123457, -1.23456789123457e+30) ;
CREATE TABLE rqg_table6 (
c0 int,
c1 int,
c2 text,
c3 text,
c4 date,
c5 date,
c6 timestamp,
c7 timestamp,
c8 numeric,
c9 numeric)    PARTITION BY hash( c2) distribute by replication;
create TABLE rqg_table6_p0 partition of rqg_table6 for values with(modulus 4,remainder 0);
create TABLE rqg_table6_p1 partition of rqg_table6 for values with(modulus 4,remainder 1);
create TABLE rqg_table6_p2 partition of rqg_table6 for values with(modulus 4,remainder 2);
create TABLE rqg_table6_p3 partition of rqg_table6 for values with(modulus 4,remainder 3);
alter table rqg_table6 alter column c0 drop not null;
CREATE INDEX idx_rqg_table6_c1 ON rqg_table6(c1);
CREATE INDEX idx_rqg_table6_c3 ON rqg_table6(c3);
CREATE INDEX idx_rqg_table6_c5 ON rqg_table6(c5);
CREATE INDEX idx_rqg_table6_c7 ON rqg_table6(c7);
CREATE INDEX idx_rqg_table6_c9 ON rqg_table6(c9);
INSERT INTO rqg_table6 VALUES  (NULL, NULL, 'cgt', NULL, '2013-08-15', '2000-09-11', '1973-09-27 18:30:41', NULL, 1.23456789123457e+25, 1.23456789123457e+25) ,  (3, 8, 'bar', NULL, '1975-10-13 13:41:48', '1990-07-12', '1973-02-19 17:29:19.053501', '2017-07-04 00:37:45', 1.23456789123457e-09, -1.23456789123457e+44) ,  (1, 1, 'foo', 'gtna', '1980-06-08 01:10:28', '1975-07-01', '1992-06-10 14:37:29.001894', NULL, -0.123456789123457, 1.23456789123457e+39) ,  (9, 7, 'foo', 'bar', '1991-08-09', '1971-08-19', '1984-01-19 10:39:53.015122', '1990-04-19 01:18:59.028717', 5.53955078125e+80, -5.64526213290892e+18) ,  (1, 6, 'tnaz', 'naz', '2001-01-23 01:08:32', '2003-01-02 08:18:49', '2001-12-01 08:43:51.026882', '1991-01-24 08:46:59.002827', -1.23456789123457e+44, -6.48056029921494e+124) ,  (6, 3, NULL, NULL, '1995-03-02 03:55:50', '2017-04-27', '2033-04-17 19:44:54.015496', '2022-06-01 03:45:39', -1.23456789123457e+43, -1.23456789123457e-09) ,  (NULL, 7, 'az', 'foo', '1982-02-09', '1975-03-06 20:22:42', '2011-12-02 08:16:43', '2009-05-15 13:56:13.054081', 1.23456789123457e+39, 1.23456789123457e-09) ,  (5, 3, 'foo', 'zxmkmwzq', '1999-03-28', '2024-11-09 21:29:35', '2024-04-26', '1995-09-12', -1.23456789123457e+44, 1.23456789123457e+30) ,  (1, 5, 'bar', 'x', '2032-11-05', '2001-12-15', '2031-11-06', '2003-08-05 21:10:17.055214', -1.23456789123457e+30, 0.500625610351562) ,  (3, 2, 'mk', 'foo', '1987-06-04 01:35:37', '1985-04-22 18:27:11', '1986-03-17 12:12:41', NULL, 7.35465966647273e+18, -1.23456789123457e-09) ;

set enable_lightweight_ora_syntax to on;
explain (costs off, nodes off)
 select t1.c0,t2.c1 from ( select c0,c1 from (select t1.c1, t2.c0 from (select c1 from ( SELECT a1.c0, a2.c1, a2.c3, a1.c2, a1.c4, a2.c5, a2.c6, a1.c7, a1.c8, a2.c9 FROM ( rqg_table2 a1 RIGHT JOIN rqg_table6 a2 ON ( ( ( ( ( ( NOT ( a1.c9 IN ( -0.123456789123456789123456789123456789123456789, 123456789123456789123456789123456789123456789,0.0000000012345678912345678912345678912345678912345678 ) ) AND ( a1.c0 = a2.c0 +1 ) ) OR ( a1.c7 > '1997-07-26 12:53:46.058938' ) AND ( a1.c0 = a2.c0 ) ) OR ( a1.c3 IN ( 'qaeucvcxv','aeucvcxvpif' ) ) AND ( a1.c0 = a2.c0 ) ) AND ( a1.c0 IS NOT NULL ) ) OR ( a2.c2 not LIKE 'N9%' ) AND ( a1.c0 = a2.c0 ) ) OR ( a2.c7 NOT BETWEEN '2000-05-27 18:50:35.016957' AND TO_timestamp( '2000-05-27 18:50:35.016957','YYYY-MM-DD HH24:MI:SS.US') + '1 day'::interval ) AND ( a1.c0 = a2.c0 ) ) ) INNER JOIN rqg_table1 a3 ON ( NOT ( a3.c0 = 7 ) AND ( a1.c0 = a3.c0 +1 ) ) WHERE a1.c4 NOT BETWEEN '1982-01-17 08:01:39' AND TO_DATE( '1982-01-17 08:01:39','YYYY-MM-DD HH24:MI:SS') + 1 AND a1.c7 NOT BETWEEN '2000-05-27 18:50:35.016957' AND TO_timestamp( '2000-05-27 18:50:35.016957','YYYY-MM-DD HH24:MI:SS.US') + '1 day'::interval )) t1, (select c0 from ( SELECT a1.c0, a2.c1, a2.c3, a1.c2, a1.c4, a2.c5, a1.c6, a2.c7, a1.c8, a1.c9 FROM rqg_table2 a1 LEFT JOIN rqg_table2 a2 ON ( ( a2.c2 IS NULL AND ( a1.c0 = a2.c0 ) ) AND ( a2.c6 IS NOT NULL ) ) WHERE NOT ( a1.c1 = 7 ) AND a2.c1 IS NOT NULL )) t2)) t1 left join ( select c1 from rqg_table3) t2 on t1.c0=t2.c1 left join (select c1 c2 from rqg_table3 ) t3 on t1.c1=t3.c2;

 select t1.c0,t2.c1 from ( select c0,c1 from (select t1.c1, t2.c0 from (select c1 from ( SELECT a1.c0, a2.c1, a2.c3, a1.c2, a1.c4, a2.c5, a2.c6, a1.c7, a1.c8, a2.c9 FROM ( rqg_table2 a1 RIGHT JOIN rqg_table6 a2 ON ( ( ( ( ( ( NOT ( a1.c9 IN ( -0.123456789123456789123456789123456789123456789, 123456789123456789123456789123456789123456789,0.0000000012345678912345678912345678912345678912345678 ) ) AND ( a1.c0 = a2.c0 +1 ) ) OR ( a1.c7 > '1997-07-26 12:53:46.058938' ) AND ( a1.c0 = a2.c0 ) ) OR ( a1.c3 IN ( 'qaeucvcxv','aeucvcxvpif' ) ) AND ( a1.c0 = a2.c0 ) ) AND ( a1.c0 IS NOT NULL ) ) OR ( a2.c2 not LIKE 'N9%' ) AND ( a1.c0 = a2.c0 ) ) OR ( a2.c7 NOT BETWEEN '2000-05-27 18:50:35.016957' AND TO_timestamp( '2000-05-27 18:50:35.016957','YYYY-MM-DD HH24:MI:SS.US') + '1 day'::interval ) AND ( a1.c0 = a2.c0 ) ) ) INNER JOIN rqg_table1 a3 ON ( NOT ( a3.c0 = 7 ) AND ( a1.c0 = a3.c0 +1 ) ) WHERE a1.c4 NOT BETWEEN '1982-01-17 08:01:39' AND TO_DATE( '1982-01-17 08:01:39','YYYY-MM-DD HH24:MI:SS') + 1 AND a1.c7 NOT BETWEEN '2000-05-27 18:50:35.016957' AND TO_timestamp( '2000-05-27 18:50:35.016957','YYYY-MM-DD HH24:MI:SS.US') + '1 day'::interval )) t1, (select c0 from ( SELECT a1.c0, a2.c1, a2.c3, a1.c2, a1.c4, a2.c5, a1.c6, a2.c7, a1.c8, a1.c9 FROM rqg_table2 a1 LEFT JOIN rqg_table2 a2 ON ( ( a2.c2 IS NULL AND ( a1.c0 = a2.c0 ) ) AND ( a2.c6 IS NOT NULL ) ) WHERE NOT ( a1.c1 = 7 ) AND a2.c1 IS NOT NULL )) t2)) t1 left join ( select c1 from rqg_table3) t2 on t1.c0=t2.c1 left join (select c1 c2 from rqg_table3 ) t3 on t1.c1=t3.c2;
set enable_lightweight_ora_syntax to off;

drop table if exists rqg_table1;
drop table if exists rqg_table2;
drop table if exists rqg_table3;
drop table if exists rqg_table6;

drop table pt1;
create table pt1(c0 int, c1 int, c2 int, c3 int) partition by range(c1,c2);
create table pt1_p1 partition of pt1 for values from (0,0) to (100000, 100000);
create table pt1_p2 partition of pt1 for values from (100000,100000) to (200000, 200000) partition by range(c3);
create table pt1_p2_p1 partition of pt1_p2 for values from (0) to (50000);
create table pt1_p2_p2 partition of pt1_p2 for values from (50000) to (100000);
create table pt1_p2_p3 partition of pt1_p2 for values from (100000) to (200000);
create table pt1_p3 partition of pt1 for values from (200000,200000) to (500000, 500000);

insert into pt1 select n, n % 10, n % 200, n from generate_series(1, 100000) n;
insert into pt1 select n, 100000 + n % 10, 100000 + n % 20, n %10 from generate_series(1, 50000) n;
insert into pt1 select n, 100000 + n % 10, 100000 + n % 20, n %10 + 50000 from generate_series(50000, 100000) n;
insert into pt1 select n, 100000 + n % 10, 100000 + n % 20, n %100 + 100000 from generate_series(100000, 200000) n;
insert into pt1 select n, 200000 + n % 300000, 200000 + n % 300000, n from generate_series(1, 500000) n;

analyze pt1;
set network_byte_cost to 0.01;
explain (costs off)select sum(c0) from pt1 group by c1;
explain (costs off)select sum(c0) from pt1 group by c1, c2;
explain (costs off)select sum(c0) from pt1 where c1 > 100000 and c1 < 200000 and c2 > 100000 and c2 < 200000 group by c1;
explain (costs off)select sum(c0) from pt1 where c1 > 100000 and c1 < 200000 and c2 > 100000 and c2 < 200000 group by c1, c2;
explain (costs off)select sum(c0) from pt1 where c1 > 100000 and c1 < 200000 and c2 > 100000 and c2 < 200000 group by c1, c2, c3;
explain (costs off, verbose)select sum(c0) from pt1 where c1 > 100000 and c1 < 200000 and c2 > 100000 and c2 < 200000 and c3 < 50000 group by c1;
explain (costs off, verbose)select sum(c0) from pt1 where c1 > 100000 and c1 < 200000 and c2 > 100000 and c2 < 200000 and c3 < 50000  group by c1, c2;
explain (costs off, verbose)select sum(c0) from pt1 where c1 > 100000 and c1 < 200000 and c2 > 100000 and c2 < 200000 and c3 < 50000  group by c1, c2, c3;
explain (costs off)select sum(c0) from pt1 where c1 < 200000 and c2 < 200000 group by c1, c2;
explain (costs off)select sum(c0) from pt1_p2 group by c1, c2;
explain (costs off)select sum(c0) from pt1 where c1 > 100000 and c1 < 200000 and c2 > 100000 and c2 < 200000 and c3 < 100000 group by c1, c2;
explain (costs off)select sum(c0) from pt1 where c1 > 100000 and c1 < 200000 and c2 > 100000 and c2 < 200000 and c3 < 100000 group by c1, c2, c3;
explain (costs off, verbose)select sum(c0) from pt1 where c1 > 100000 and c1 < 200000 and c2 > 100000 and c2 < 200000 and c3 < 50000 group by c1, c2, c3;
explain (costs off, verbose)select sum(c0) from pt1_p2 where c3 < 50000 group by c1, c2;
explain (costs off)select sum(c0) from pt1_p2 where c3 < 50000 group by c1, c2, c3;
reset network_byte_cost;
drop table pt1;
-- heap table 
drop table pt1 cascade;
create table pt1(c0 int, c1 int, c2 int, c3 int, c4 int) PARTITION BY RANGE (c1);
CREATE TABLE pt1_p1 PARTITION OF pt1 FOR VALUES FROM (0) TO (10);
CREATE TABLE pt1_p2 PARTITION OF pt1 FOR VALUES FROM (10) TO (20);
CREATE TABLE pt1_p3 PARTITION OF pt1 FOR VALUES FROM (20) TO (30);
CREATE TABLE pt1_p4 PARTITION OF pt1 FOR VALUES FROM (30) TO (40);
CREATE TABLE pt1_p5 PARTITION OF pt1 FOR VALUES FROM (40) TO (50);
CREATE TABLE pt1_p6 PARTITION OF pt1 FOR VALUES FROM (50) TO (60);

drop table pt2 cascade;
create table pt2(c0 int, c1 int, c2 int, c3 int, c4 int) PARTITION BY RANGE (c1);
CREATE TABLE pt2_p1 PARTITION OF pt2 FOR VALUES FROM (0) TO (10);
CREATE TABLE pt2_p2 PARTITION OF pt2 FOR VALUES FROM (10) TO (20);
CREATE TABLE pt2_p3 PARTITION OF pt2 FOR VALUES FROM (20) TO (30);
CREATE TABLE pt2_p4 PARTITION OF pt2 FOR VALUES FROM (30) TO (40);
CREATE TABLE pt2_p5 PARTITION OF pt2 FOR VALUES FROM (40) TO (50);
CREATE TABLE pt2_p6 PARTITION OF pt2 FOR VALUES FROM (50) TO (60);

explain (costs off, nodes off, verbose)
insert into pt1 select n, n % 60 , n, n from generate_series(1, 10000)n;
insert into pt1 select n, n % 60 , n, n from generate_series(1, 10000)n;
select count(1) from pt1;

explain (costs off, verbose)
insert into pt2 select * from pt1;
insert into pt2 select * from pt1;
select count(1) from pt2;

explain (costs off, verbose)
delete from pt2 where c2 < 60;
delete from pt2 where c2 < 60;
select * from pt2 where c2 < 60;

explain (costs off, verbose)
delete from pt2 using pt1 where pt1.c3 = pt2.c3 and pt1.c2 < 120;
delete from pt2 using pt1 where pt1.c3 = pt2.c3 and pt1.c2 < 120;
select * from pt2 where c2 < 120;

explain (costs off, verbose)
update pt2 set c4 = 100 from pt1 where pt1.c3=pt2.c3 and pt1.c2 < 180;
update pt2 set c4 = 100 from pt1 where pt1.c3=pt2.c3 and pt1.c2 < 180;
select avg(c4) from pt2 where c2 < 180;

analyze pt1;
analyze pt2;
create index pt1_c1 on pt1(c1);
create index pt2_c1 on pt2(c1);
set enable_seqscan TO off;
explain (costs off, verbose)
delete from pt1 where c1 < 10;
explain (costs off, verbose)
delete from pt1 where c1 < 100;
delete from pt1 where c1 < 100;

explain (costs off, verbose)
merge into pt1 using pt2 on  pt1.c2 = pt2.c2 and pt1.c1 < 120 when matched then update set pt1.c3=pt2.c3 / 10 when not matched then insert values (c0, c1, c2, c3, c4);
merge into pt1 using pt2 on  pt1.c2 = pt2.c2 and pt1.c1 < 120 when matched then update set pt1.c3=pt2.c3 / 10 when not matched then insert values (c0, c1, c2, c3, c4);

drop table pt1 cascade;
drop table pt2 cascade;

reset enable_seqscan;

--多级分区表
drop table if exits t_par_select_math_20220607;
create table t_par_select_math_20220607(f1 bigserial not null,f2 integer,f3 varchar(20),f4 varchar(20), f5 NUMERIC) partition by list ( f3 ) distribute by shard(f1) to group default_group;
--二级
create table t_par_select_math_20220607_gd partition of t_par_select_math_20220607 for values in ('gd') partition by range(f5);
create table t_par_select_math_20220607_bj partition of t_par_select_math_20220607 for values in ('bj') partition by range(f5);
create table t_par_select_math_20220607_12 partition of t_par_select_math_20220607 for values in ('12');
--三级
create table t_par_select_math_20220607_gd_201701 partition of t_par_select_math_20220607_gd(f1,f2,f3,f4,f5) for values from (minvalue) to (0);
create table t_par_select_math_20220607_gd_201702 partition of t_par_select_math_20220607_gd(f1,f2,f3,f4,f5) for values from (0) to (20);
create table t_par_select_math_20220607_bj_201701 partition of t_par_select_math_20220607_bj(f1,f2,f3,f4,f5) for values from (1) to (20);
create table t_par_select_math_20220607_bj_201702 partition of t_par_select_math_20220607_bj(f1,f2,f3,f4,f5) for values from (20) to (maxvalue);

--插入数据
insert into t_par_select_math_20220607 values (1,1,'gd','a',-1);
insert into t_par_select_math_20220607 values (2,2,'gd','a',19);
insert into t_par_select_math_20220607 values (1,1,'bj','a',1);
insert into t_par_select_math_20220607 values (4,4,'12','a',99);
explain (verbose, costs off, nodes off)
select sum(f2) over() as sum1 from t_par_select_math_20220607 where f3 in (to_char('gd'),to_char('bj'));
select sum(f2) over() as sum1 from t_par_select_math_20220607 where f3 in (to_char('gd'),to_char('bj'));

drop table if exists t_par_select_math_20220607;

drop table if exists part_runtime_prune;
create table part_runtime_prune(c0 date, c1 int, c2 int) partition by range (c0);
create table part_runtime_prune1 partition of part_runtime_prune for values from (minvalue) to ('2022-05-18');
create table part_runtime_prune2 partition of part_runtime_prune for values from ('2022-05-18') to ('2022-08-18');
drop table if exists part_runtime_prune;
