--
-- EXPLAIN
--
-- There are many test cases elsewhere that use EXPLAIN as a vehicle for
-- checking something else (usually planner behavior).  This file is
-- concerned with testing EXPLAIN in its own right.
--

-- To produce stable regression test output, it's usually necessary to
-- ignore details such as exact costs or row counts.  These filter
-- functions replace changeable output details with fixed strings.

create function explain_filter(text) returns setof text
language plpgsql as
$$
declare
    ln text;
begin
    for ln in execute $1
    loop
        -- Replace any numeric word with just 'N'
        ln := regexp_replace(ln, '\m\d+\M', 'N', 'g');
        -- In sort output, the above won't match units-suffixed numbers
        ln := regexp_replace(ln, '\m\d+kB', 'NkB', 'g');
        -- For query_mem
        ln := regexp_replace(ln, '\m\d+MB', 'NMB', 'g');
        -- Text-mode buffers output varies depending on the system state
        ln := regexp_replace(ln, '^( +Buffers: shared)( hit=N)?( read=N)?', '\1 [read]');
        return next ln;
    end loop;
end;
$$;

-- To produce valid JSON output, replace numbers with "0" or "0.0" not "N"
create function explain_filter_to_json(text) returns jsonb
language plpgsql as
$$
declare
    data text := '';
    ln text;
begin
    for ln in execute $1
    loop
        -- Replace any numeric word with just '0'
        ln := regexp_replace(ln, '\m\d+\M', '0', 'g');
        data := data || ln;
    end loop;
    return data::jsonb;
end;
$$;

-- Simple cases

select explain_filter('explain select * from int8_tbl i8');
select explain_filter('explain (analyze) select * from int8_tbl i8');
select explain_filter('explain (analyze, verbose) select * from int8_tbl i8');
select explain_filter('explain (analyze, buffers, format text) select * from int8_tbl i8');
select explain_filter('explain (analyze, buffers, format json) select * from int8_tbl i8');
select explain_filter('explain (analyze, buffers, format xml) select * from int8_tbl i8');
select explain_filter('explain (analyze, buffers, format yaml) select * from int8_tbl i8');
select explain_filter('explain (buffers, format json) select * from int8_tbl i8');

--
-- Test production of per-worker data
--
-- Unfortunately, because we don't know how many worker processes we'll
-- actually get (maybe none at all), we can't examine the "Workers" output
-- in any detail.  We can check that it parses correctly as JSON, and then
-- remove it from the displayed results.

-- Serializable isolation would disable parallel query, so explicitly use an
-- arbitrary other level.
begin isolation level repeatable read;
-- encourage use of parallel plans
set parallel_setup_cost=0;
set parallel_tuple_cost=0;
set min_parallel_table_scan_size=0;
set max_parallel_workers_per_gather=4;

select jsonb_pretty(
  explain_filter_to_json('explain (analyze, verbose, buffers, format json)
                         select * from tenk1 order by tenthous')
  -- remove "Workers" node of the Seq Scan plan node
  #- '{0,Plan,Plans,0,Plans,0,Workers}'
  -- remove "Workers" node of the Sort plan node
  #- '{0,Plan,Plans,0,Workers}'
  -- Also remove its sort-type fields, as those aren't 100% stable
  #- '{0,Plan,Plans,0,Sort Method}'
  #- '{0,Plan,Plans,0,Sort Space Type}'
);

rollback;

-- OPENTENBASE
-- explain with query_mem setting
set work_mem = '2MB';
set query_mem = '64MB';
set enable_indexonlyscan to off;
select explain_filter('explain (summary) select * from tenk1 order by tenthous');
select explain_filter('explain (analyze) select * from tenk1 order by tenthous');

set work_mem = '1MB';
set max_query_mem = '32MB';
show query_mem;
select explain_filter('explain (summary) select tenthous,count(*) from tenk1 group by tenthous');
select explain_filter('explain (analyze) select tenthous,count(*) from tenk1 group by tenthous');

reset work_mem;
select explain_filter('explain (summary) select tenthous,count(*) from tenk1 group by tenthous');
select explain_filter('explain (analyze) select tenthous,count(*) from tenk1 group by tenthous');
reset max_query_mem;
reset query_mem;
reset enable_indexonlyscan;

-- 1020385652099083441
set max_parallel_workers_per_gather=2;
set parallel_tuple_cost to 0;
set parallel_setup_cost to 0;
set min_parallel_table_scan_size to 0;

-- Subquery with InitPlan
CREATE TABLE subquery_with_initplan_base_tbl (a int PRIMARY KEY, b text DEFAULT 'Unspecified');
INSERT INTO subquery_with_initplan_base_tbl SELECT i, 'Row ' || i FROM generate_series(-2, 2) g(i);
CREATE VIEW subquery_with_initplan_view AS SELECT 1 FROM subquery_with_initplan_base_tbl HAVING max(a) > 0;
explain (costs off) select * from subquery_with_initplan_view;
select * from subquery_with_initplan_view;
drop table subquery_with_initplan_base_tbl cascade;

/* test FQS explain output for json and yaml format */
set enable_fast_query_shipping = on;
DROP TABLE IF EXISTS test_fqs_output;
CREATE TABLE test_fqs_output(a INT, b INT);
EXPLAIN (format json, costs off) SELECT * FROM test_fqs_output;
EXPLAIN (format yaml, costs off) SELECT * FROM test_fqs_output;
DROP TABLE test_fqs_output;
reset enable_fast_query_shipping;
