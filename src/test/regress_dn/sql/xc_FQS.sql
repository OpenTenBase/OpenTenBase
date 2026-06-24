\c regression

-- A function to create table on specified nodes 
create or replace function cr_table(tab_schema varchar, nodenums int[], distribution varchar)
returns void language plpgsql as $$
declare
	cr_command	varchar;
	nodes		varchar[];
	nodename	varchar;
	nodenames_query varchar;
	nodenames 	varchar;
	node 		int;
	sep			varchar;
	tmp_node	int;
	num_nodes	int;
begin
	nodenames_query := 'SELECT node_name FROM pgxc_node WHERE node_type = ''D'''; 
	cr_command := 'CREATE TABLE ' || tab_schema || ' DISTRIBUTE BY ' || distribution || ' TO NODE (';
	for nodename in execute nodenames_query loop
		nodes := array_append(nodes, nodename);
	end loop;
	nodenames := '';
	sep := '';
	num_nodes := array_length(nodes, 1);
	foreach node in array nodenums loop
		tmp_node := node;
		if (tmp_node < 1 or tmp_node > num_nodes) then
			tmp_node := tmp_node % num_nodes;
			if (tmp_node < 1) then
				tmp_node := num_nodes; 
			end if;
		end if;
		nodenames := nodenames || sep || nodes[tmp_node];
		sep := ', ';
	end loop;
	cr_command := cr_command || nodenames;
	cr_command := cr_command || ')';

	execute cr_command;
end;
$$;
-- This file contains tests for Fast Query Shipping (FQS) for queries involving
-- a single table

-- Testset 1 for distributed table (by roundrobin)
select create_table_nodes('tab1_rr(val int, val2 int)', '{1, 2, 3}'::int[], 'roundrobin', NULL);
insert into tab1_rr values (1, 2);
insert into tab1_rr values (2, 4);
insert into tab1_rr values (5, 3);
insert into tab1_rr values (7, 8);
insert into tab1_rr values (9, 2);
explain (verbose on, nodes off, num_nodes on, costs off) insert into tab1_rr values (9, 2);
-- simple select
-- should get FQSed
select val, val2 + 2, case val when val2 then 'val and val2 are same' else 'val and val2 are not same' end from tab1_rr where val2 = 4;
explain (verbose on, nodes off, costs off) select val, val2 + 2, case val when val2 then 'val and val2 are same' else 'val and val2 are not same' end from tab1_rr where val2 = 4;
-- should not get FQSed because of aggregates
select sum(val), avg(val), count(*) from tab1_rr;
explain (verbose on, nodes off, costs off) select sum(val), avg(val), count(*) from tab1_rr;
-- should not get FQSed because of window functions
select first_value(val) over (partition by val2 order by val) from tab1_rr order by 1;
explain (verbose on, nodes off, costs off) select first_value(val) over (partition by val2 order by val) from tab1_rr;
-- should not get FQSed because of LIMIT clause
select * from tab1_rr where val2 = 3 limit 1;
explain (verbose on, nodes off, costs off) select * from tab1_rr where val2 = 3 limit 1;
-- should not FQSed because of OFFSET clause
select * from tab1_rr where val2 = 4 offset 1;
explain (verbose on, nodes off, costs off) select * from tab1_rr where val2 = 4 offset 1;
-- should not get FQSed because of SORT clause
select * from tab1_rr order by val;
explain (verbose on, nodes off, costs off) select * from tab1_rr order by val;
-- should not get FQSed because of DISTINCT clause
select distinct val, val2 from tab1_rr where val2 = 8;
explain (verbose on, nodes off, costs off) select distinct val, val2 from tab1_rr where val2 = 8;
-- should not get FQSed because of GROUP clause
select val, val2 from tab1_rr where val2 = 8 group by val, val2;
explain (verbose on, nodes off, costs off) select val, val2 from tab1_rr where val2 = 8 group by val, val2;
-- should not get FQSed because of HAVING clause
select sum(val) from tab1_rr where val2 = 2 group by val2 having sum(val) > 1;
explain (verbose on, nodes off, costs off) select sum(val) from tab1_rr where val2 = 2 group by val2 having sum(val) > 1;

-- tests for node reduction by application of quals, for roundrobin node
-- reduction is not applicable. Having query not FQSed because of existence of ORDER BY,
-- implies that nodes did not get reduced.
select * from tab1_rr where val = 7;
explain (verbose on, nodes off, costs off) select * from tab1_rr where val = 7;
select * from tab1_rr where val = 7 or val = 2 order by val;
explain (verbose on, nodes off, costs off) select * from tab1_rr where val = 7 or val = 2 order by val;
select * from tab1_rr where val = 7 and val2 = 8;
explain (verbose on, nodes off, costs off) select * from tab1_rr where val = 7 and val2 = 8 order by val;
select * from tab1_rr where val = 3 + 4 and val2 = 8 order by val;
explain (verbose on, nodes off, costs off) select * from tab1_rr where val = 3 + 4 order by val;
select * from tab1_rr where val = char_length('len')+4 order by val;
explain (verbose on, nodes off, costs off) select * from tab1_rr where val = char_length('len')+4 order by val;
-- insert some more values 
insert into tab1_rr values (7, 2); 
select avg(val) from tab1_rr where val = 7;
explain (verbose on, nodes off, costs off) select avg(val) from tab1_rr where val = 7;
select val, val2 from tab1_rr where val = 7 order by val2;
explain (verbose on, nodes off, costs off) select val, val2 from tab1_rr where val = 7 order by val2;
select distinct val2 from tab1_rr where val = 7 order by val2;
explain (verbose on, nodes off, costs off) select distinct val2 from tab1_rr where val = 7 order by val2;
-- DMLs
update tab1_rr set val2 = 1000 where val = 7; 
explain (verbose on, nodes off, costs off) update tab1_rr set val2 = 1000 where val = 7; 
select * from tab1_rr where val = 7;
delete from tab1_rr where val = 7; 
explain (verbose on, costs off) delete from tab1_rr where val = 7; 
select * from tab1_rr where val = 7;

-- Testset 2 for distributed tables (by hash)
select cr_table('tab1_hash(val int, val2 int)', '{1, 2, 3}'::int[], 'hash(val)');
insert into tab1_hash values (1, 2);
insert into tab1_hash values (2, 4);
insert into tab1_hash values (5, 3);
insert into tab1_hash values (7, 8);
insert into tab1_hash values (9, 2);
explain (verbose on, costs off) insert into tab1_hash values (9, 2);
-- simple select
-- should get FQSed
select val, val2 + 2, case val when val2 then 'val and val2 are same' else 'val and val2 are not same' end from tab1_hash where val2 = 4;
explain (verbose on, nodes off, costs off) select val, val2 + 2, case val when val2 then 'val and val2 are same' else 'val and val2 are not same' end from tab1_hash where val2 = 2;
-- should not get FQSed because of aggregates
select sum(val), avg(val), count(*) from tab1_hash;
explain (verbose on, nodes off, costs off) select sum(val), avg(val), count(*) from tab1_hash;
-- should not get FQSed because of window functions
select first_value(val) over (partition by val2 order by val) from tab1_hash order by 1;
explain (verbose on, nodes off, costs off) select first_value(val) over (partition by val2 order by val) from tab1_hash;
-- should not get FQSed because of LIMIT clause
select * from tab1_hash where val2 = 3 limit 1;
explain (verbose on, nodes off, costs off) select * from tab1_hash where val2 = 3 limit 1;
-- should not FQSed because of OFFSET clause
select * from tab1_hash where val2 = 4 offset 1;
explain (verbose on, nodes off, costs off) select * from tab1_hash where val2 = 4 offset 1;
-- should not get FQSed because of SORT clause
select * from tab1_hash order by val;
explain (verbose on, nodes off, costs off) select * from tab1_hash order by val;
-- should get FQSed because DISTINCT clause contains distkey
select distinct val, val2 from tab1_hash where val2 = 8;
explain (verbose on, nodes off, costs off) select distinct val, val2 from tab1_hash where val2 = 8;
-- should get FQSed because GROUP BY clause uses distkey
select val, val2 from tab1_hash where val2 = 8 group by val, val2;
explain (verbose on, nodes off, costs off) select val, val2 from tab1_hash where val2 = 8 group by val, val2;
-- should not get FQSed because of HAVING clause
select sum(val) from tab1_hash where val2 = 2 group by val2 having sum(val) > 1;
explain (verbose on, nodes off, costs off) select sum(val) from tab1_hash where val2 = 2 group by val2 having sum(val) > 1;

-- tests for node reduction by application of quals. Having query FQSed because of
-- existence of ORDER BY, implies that nodes got reduced.
select * from tab1_hash where val = 7;
explain (verbose on, nodes off, costs off, num_nodes on) select * from tab1_hash where val = 7;
select * from tab1_hash where val = 7 or val = 2 order by val;
explain (verbose on, nodes off, costs off) select * from tab1_hash where val = 7 or val = 2 order by val;
select * from tab1_hash where val = 7 and val2 = 8;
explain (verbose on, nodes off, costs off, num_nodes on) select * from tab1_hash where val = 7 and val2 = 8;
select * from tab1_hash where val = 3 + 4 and val2 = 8;
explain (verbose on, nodes off, costs off, num_nodes on) select * from tab1_hash where val = 3 + 4;
select * from tab1_hash where val = char_length('len')+4;
explain (verbose on, nodes off, costs off, num_nodes on) select * from tab1_hash where val = char_length('len')+4;
-- insert some more values 
insert into tab1_hash values (7, 2); 
select avg(val) from tab1_hash where val = 7;
explain (verbose on, nodes off, costs off, num_nodes on) select avg(val) from tab1_hash where val = 7;
select val, val2 from tab1_hash where val = 7 order by val2;
explain (verbose on, nodes off, costs off, num_nodes on) select val, val2 from tab1_hash where val = 7 order by val2;
select distinct val2 from tab1_hash where val = 7 order by val2;
explain (verbose on, nodes off, costs off, num_nodes on) select distinct val2 from tab1_hash where val = 7 order by val2;
-- DMLs
update tab1_hash set val2 = 1000 where val = 7; 
explain (verbose on, nodes off, costs off) update tab1_hash set val2 = 1000 where val = 7; 
select * from tab1_hash where val = 7;
delete from tab1_hash where val = 7; 
explain (verbose on, costs off) delete from tab1_hash where val = 7; 
select * from tab1_hash where val = 7;

-- Testset 3 for distributed tables (by modulo)
select cr_table('tab1_modulo(val int, val2 int)', '{1, 2, 3}'::int[], 'modulo(val)');
insert into tab1_modulo values (1, 2);
insert into tab1_modulo values (2, 4);
insert into tab1_modulo values (5, 3);
insert into tab1_modulo values (7, 8);
insert into tab1_modulo values (9, 2);
explain (verbose on, costs off) insert into tab1_modulo values (9, 2);
-- simple select
-- should get FQSed
select val, val2 + 2, case val when val2 then 'val and val2 are same' else 'val and val2 are not same' end from tab1_modulo where val2 = 4;
explain (verbose on, nodes off, costs off) select val, val2 + 2, case val when val2 then 'val and val2 are same' else 'val and val2 are not same' end from tab1_modulo where val2 = 4;
-- should not get FQSed because of aggregates
select sum(val), avg(val), count(*) from tab1_modulo;
explain (verbose on, nodes off, costs off) select sum(val), avg(val), count(*) from tab1_modulo;
-- should not get FQSed because of window functions
select first_value(val) over (partition by val2 order by val) from tab1_modulo order by 1;
explain (verbose on, nodes off, costs off) select first_value(val) over (partition by val2 order by val) from tab1_modulo;
-- should not get FQSed because of LIMIT clause
select * from tab1_modulo where val2 = 3 limit 1;
explain (verbose on, nodes off, costs off) select * from tab1_modulo where val2 = 3 limit 1;
-- should not FQSed because of OFFSET clause
select * from tab1_modulo where val2 = 4 offset 1;
explain (verbose on, nodes off, costs off) select * from tab1_modulo where val2 = 4 offset 1;
-- should not get FQSed because of SORT clause
select * from tab1_modulo order by val;
explain (verbose on, nodes off, costs off) select * from tab1_modulo order by val;
-- should get FQSed because DISTINCT clause contains distkey
select distinct val, val2 from tab1_modulo where val2 = 8;
explain (verbose on, nodes off, costs off) select distinct val, val2 from tab1_modulo where val2 = 8;
-- should get FQSed because GROUP BY clause uses distkey
select val, val2 from tab1_modulo where val2 = 8 group by val, val2;
explain (verbose on, nodes off, costs off) select val, val2 from tab1_modulo where val2 = 8 group by val, val2;
-- should not get FQSed because of HAVING clause
select sum(val) from tab1_modulo where val2 = 2 group by val2 having sum(val) > 1;
explain (verbose on, nodes off, costs off) select sum(val) from tab1_modulo where val2 = 2 group by val2 having sum(val) > 1;

-- tests for node reduction by application of quals. Having query FQSed because of
-- existence of ORDER BY, implies that nodes got reduced.
select * from tab1_modulo where val = 7;
explain (verbose on, nodes off, costs off, num_nodes on) select * from tab1_modulo where val = 7;
select * from tab1_modulo where val = 7 or val = 2 order by val;
explain (verbose on, nodes off, costs off) select * from tab1_modulo where val = 7 or val = 2 order by val;
select * from tab1_modulo where val = 7 and val2 = 8;
explain (verbose on, nodes off, costs off, num_nodes on) select * from tab1_modulo where val = 7 and val2 = 8;
select * from tab1_modulo where val = 3 + 4 and val2 = 8;
explain (verbose on, nodes off, costs off, num_nodes on) select * from tab1_modulo where val = 3 + 4;
select * from tab1_modulo where val = char_length('len')+4;
explain (verbose on, nodes off, costs off, num_nodes on) select * from tab1_modulo where val = char_length('len')+4;
-- insert some more values 
insert into tab1_modulo values (7, 2); 
select avg(val) from tab1_modulo where val = 7;
explain (verbose on, nodes off, costs off, num_nodes on) select avg(val) from tab1_modulo where val = 7;
select val, val2 from tab1_modulo where val = 7 order by val2;
explain (verbose on, nodes off, costs off, num_nodes on) select val, val2 from tab1_modulo where val = 7 order by val2;
select distinct val2 from tab1_modulo where val = 7;
explain (verbose on, nodes off, costs off, num_nodes on) select distinct val2 from tab1_modulo where val = 7;
-- DMLs
update tab1_modulo set val2 = 1000 where val = 7; 
explain (verbose on, nodes off, costs off) update tab1_modulo set val2 = 1000 where val = 7; 
select * from tab1_modulo where val = 7;
delete from tab1_modulo where val = 7; 
explain (verbose on, costs off) delete from tab1_modulo where val = 7; 
select * from tab1_modulo where val = 7;

-- Testset 4 for replicated tables, for replicated tables, unless the expression
-- is itself unshippable, any query involving a single replicated table is shippable
select cr_table('tab1_replicated(val int, val2 int)', '{1, 2, 3}'::int[], 'replication');
insert into tab1_replicated values (1, 2);
insert into tab1_replicated values (2, 4);
insert into tab1_replicated values (5, 3);
insert into tab1_replicated values (7, 8);
insert into tab1_replicated values (9, 2);
explain (verbose on, nodes off, costs off) insert into tab1_replicated values (9, 2);
-- simple select
select * from tab1_replicated order by val;
explain (num_nodes on, verbose on, nodes off, costs off) select * from tab1_replicated;
select sum(val), avg(val), count(*) from tab1_replicated;
explain (num_nodes on, verbose on, nodes off, costs off) select sum(val), avg(val), count(*) from tab1_replicated;
select first_value(val) over (partition by val2 order by val) from tab1_replicated;
explain (num_nodes on, verbose on, nodes off, costs off) select first_value(val) over (partition by val2 order by val) from tab1_replicated;
select * from tab1_replicated where val2 = 2 limit 2;
explain (num_nodes on, verbose on, nodes off, costs off) select * from tab1_replicated where val2 = 2 limit 2;
select * from tab1_replicated where val2 = 4 offset 1;
explain (num_nodes on, verbose on, nodes off, costs off) select * from tab1_replicated where val2 = 4 offset 1;
select * from tab1_replicated order by val;
explain (num_nodes on, verbose on, nodes off, costs off) select * from tab1_replicated order by val;
select distinct val, val2 from tab1_replicated order by 1, 2;
explain (num_nodes on, verbose on, nodes off, costs off) select distinct val, val2 from tab1_replicated order by 1, 2;
explain (num_nodes on, verbose on, nodes off, costs off) select distinct val, val2 from tab1_replicated;
select val, val2 from tab1_replicated group by val, val2 order by 1, 2;
explain (num_nodes on, verbose on, nodes off, costs off) select val, val2 from tab1_replicated group by val, val2 order by 1, 2;
explain (num_nodes on, verbose on, nodes off, costs off) select val, val2 from tab1_replicated group by val, val2;
select sum(val) from tab1_replicated group by val2 having sum(val) > 1 order by 1;
explain (num_nodes on, verbose on, nodes off, costs off) select sum(val) from tab1_replicated group by val2 having sum(val) > 1 order by 1;
explain (num_nodes on, verbose on, nodes off, costs off) select sum(val) from tab1_replicated group by val2 having sum(val) > 1;
-- DMLs
update tab1_replicated set val2 = 1000 where val = 7; 
explain (verbose on, nodes off, costs off) update tab1_replicated set val2 = 1000 where val = 7; 
select * from tab1_replicated where val = 7;
delete from tab1_replicated where val = 7; 
explain (verbose on, costs off) delete from tab1_replicated where val = 7; 
select * from tab1_replicated where val = 7;

-- Constant subquery
create table subquery_fqs(id int, a varchar, c int);
insert into subquery_fqs values(1,'gd', 2);
insert into subquery_fqs values(1,'zj', 2);
insert into subquery_fqs values(1,'sz', 2);
explain (verbose on, costs off) select * from subquery_fqs t join (select 1 id, 'gd' a, 2 c from dual union select 1 id, 'sz' a, 2 c union select 1 id, 'zj' a, 2 c from dual) t2 ON (t.id = t2.id and t.a = t2.a);
select * from subquery_fqs t join (select 1 id, 'gd' a, 2 c from dual union select 1 id, 'sz' a, 2 c union select 1 id, 'zj' a, 2 c from dual) t2 ON (t.id = t2.id and t.a = t2.a);

-- Support subquery FQS only if subquery distributed on same DN with main query(only 1 DN node)
explain (verbose on, costs off)  select * from subquery_fqs t1 where t1.id = 1 and t1.c IN (select c from subquery_fqs t2 where t2.id=1);
select * from subquery_fqs t1 where t1.id = 1 and t1.c IN (select c from subquery_fqs t2 where t2.id=1);
explain (verbose on, costs off)  select * from subquery_fqs t1 where t1.id = 1 and exists (select c from subquery_fqs t2 where t2.id=1);
select * from subquery_fqs t1 where t1.id = 1 and exists (select c from subquery_fqs t2 where t2.id=1);
explain (verbose on, costs off)  select * from subquery_fqs t1 where t1.id = 1 and t1.c = (select c from subquery_fqs t2 where t2.id=1 order by c limit 1);
select * from subquery_fqs t1 where t1.id = 1 and t1.c = (select c from subquery_fqs t2 where t2.id=1 order by c limit 1);
explain (verbose on, costs off) select * from subquery_fqs t1 where t1.id = 1 and t1.c = (select max(c) from subquery_fqs t2 where t2.id=1);
select * from subquery_fqs t1 where t1.id = 1 and t1.c = (select max(c) from subquery_fqs t2 where t2.id=1);
explain (verbose on, costs off) select (select 1) from subquery_fqs;
select (select 1) from subquery_fqs;
explain (verbose on, costs off) select (select 1) from int4_tbl;
select (select 1) from subquery_fqs;

-- Failed for FQS
-- subquery but in different nodes
explain select * from subquery_fqs t1 where t1.id = 1 and t1.c IN (select c from subquery_fqs t2 where t2.id=3);
select * from subquery_fqs t1 where t1.id = 1 and t1.c IN (select c from subquery_fqs t2 where t2.id=3);

drop table tab1_rr;
drop table tab1_hash;
drop table tab1_modulo;
drop table tab1_replicated;
drop table subquery_fqs;
drop function cr_table(varchar, int[], varchar); 

-- Enhance FQS (Fast Query Shipping) to support more sublink scenarios.
drop table if exists t_fqs1;
drop table if exists t_fqs2;
create table t_fqs1(c1 int, c2 int, c3 int);
create table t_fqs2(c1 int, c2 int, c3 int) distribute by replication;
explain (costs off)
select count(*) from t_fqs1 where c1 = 1 and exists(select 1 from t_fqs2 where t_fqs1.c2=t_fqs2.c2);
drop table t_fqs1;
drop table t_fqs2;

\c regression_ora

CREATE TABLE certificate_type_mapping (
    old_certificate_type character varying(20) NOT NULL,
    new_certificate_type character varying(20) NOT NULL,
    mode_group character varying(20) NOT NULL,
    ext1 character varying(20),
    ext2 character varying(20),
    ext3 character varying(20),
    ext4 character varying(20)
)
DISTRIBUTE BY SHARD (old_certificate_type) to GROUP default_group;

CREATE TABLE cfg_operation (
    opt_code character varying(8) NOT NULL,
    opt_name character varying(60),
    opt_desc character varying(255),
    opt_class_name character varying(60),
    opt_group character varying(8),
    opt_group_name character varying(60),
    opt_class character varying(8),
    income_type numeric(1,0),
    id_check_type character varying(8) NOT NULL,
    log_flag character(1) NOT NULL,
    state character(1) NOT NULL,
    state_date date NOT NULL,
    report_flag character(1),
    rule_flag character(1),
    span_flag character(1),
    con_msg_flag character(1),
    fee_msg_flag character(1),
    stadn1 character varying(60),
    stadn2 character varying(100),
    remark character varying(255),
    reverse_opt_code character varying(8)
)
DISTRIBUTE BY SHARD (opt_code) to GROUP default_group;

CREATE TABLE st_busistat_f_202110 (
    operator_code character varying(20) NOT NULL,
    organize_id numeric(12,0) NOT NULL,
    region_id character varying(6) NOT NULL,
    stat_date date,
    opt_code character varying(8) NOT NULL,
    income_flag character(1),
    qty numeric(8,0),
    fee numeric(12,0),
    oth_fee numeric(12,0),
    favor_qty numeric(8,0),
    favor numeric(12,0),
    penalty numeric(12,0),
    derate_late_fee numeric(12,0),
    reimburse_cash numeric(12,0),
    reimburse_cheque numeric(12,0),
    to_deposit numeric(14,0),
    to_pledge numeric(14,0),
    opt_date date,
    remark character varying(256),
    ext1 numeric(12,0),
    ext2 numeric(12,0),
    ext3 numeric(12,0),
    ext4 character varying(32),
    ext5 character varying(32),
    ext6 character varying(32),
    brand_id numeric(12,0),
    offer_id numeric(12,0),
    payed_path character(1),
    certificate_type character varying(20),
    create_date date
)
DISTRIBUTE BY SHARD (opt_code) to GROUP default_group;

CREATE TABLE st_busistat_f_202111 (
    operator_code character varying(20) NOT NULL,
    organize_id numeric(12,0) NOT NULL,
    region_id character varying(6) NOT NULL,
    stat_date date,
    opt_code character varying(8) NOT NULL,
    income_flag character(1),
    qty numeric(8,0),
    fee numeric(12,0),
    oth_fee numeric(12,0),
    favor_qty numeric(8,0),
    favor numeric(12,0),
    penalty numeric(12,0),
    derate_late_fee numeric(12,0),
    reimburse_cash numeric(12,0),
    reimburse_cheque numeric(12,0),
    to_deposit numeric(14,0),
    to_pledge numeric(14,0),
    opt_date date,
    remark character varying(256),
    ext1 numeric(12,0),
    ext2 numeric(12,0),
    ext3 numeric(12,0),
    ext4 character varying(32),
    ext5 character varying(32),
    ext6 character varying(32),
    brand_id numeric(12,0),
    offer_id numeric(12,0),
    payed_path character(1),
    certificate_type character varying(20),
    create_date date
)
DISTRIBUTE BY SHARD (operator_code) to GROUP default_group;

drop table certificate_type_mapping;
drop table cfg_operation;
drop table st_busistat_f_202110;
drop table st_busistat_f_202111;

--[Performance]support function clipping in fqs for update statements.
drop table if exists TXN_PROC_RESULT;
create table TXN_PROC_RESULT(
ACTIVITY_CODE VARCHAR2(8),
REQ_TRANS_ID VARCHAR2(32),
TRADE_SESSION VARCHAR2(32),
RESULT_CODE int,
RESULT_DESC VARCHAR2(256),
OLD_TRADE_SESSION VARCHAR2(32),
NOTIFY_TYPE VARCHAR2(2),
CentreID VARCHAR2(4))  with (fillfactor=70) distribute by shard(TRADE_SESSION);

create index idx_TRADE_SESSION on TXN_PROC_RESULT(TRADE_SESSION);
create index  idx_REQ_TRANS_ID on TXN_PROC_RESULT(REQ_TRANS_ID);

prepare fun1(varchar) as UPDATE  TXN_PROC_RESULT
SET                                                     
RESULT_CODE = RESULT_CODE+1,                                
RESULT_DESC = '用户状态正常222'                            
WHERE                                                   
        TRADE_SESSION = $1;
explain(costs off) execute fun1('599');
drop table TXN_PROC_RESULT;

-- fix core in GetRelationNodesByQuals
drop table if exists dzfp_ypyw_fpckznx_yrz, ckts_js_ckznx;
create table dzfp_ypyw_fpckznx_yrz (
    znxzmbh character varying(50) not null,
    gfsbh character varying(40) not null,
    tslsh character varying(100)
)
distribute by shard (gfsbh);
create table ckts_js_ckznx (
	fpdmhm character varying(30) not null,
    tslsh character varying(32) not null,
    no character varying(50) not null,
    xf_nsrsbh character varying(20) not null,
    gf_nsrsbh character varying(20) not null
)
distribute by shard (fpdmhm);
insert into dzfp_ypyw_fpckznx_yrz values ('34', '111', '67');
insert into dzfp_ypyw_fpckznx_yrz values ('341', '111', '671');
insert into dzfp_ypyw_fpckznx_yrz values ('342', '672', '672');
insert into dzfp_ypyw_fpckznx_yrz values ('343', '111', '673');
insert into ckts_js_ckznx values ('12','23','34','45','67');
insert into ckts_js_ckznx values ('121','231','341','451','671');
insert into ckts_js_ckznx values ('122','232','342','452','672');
insert into ckts_js_ckznx values ('123','233','343','453','673');
select * from ckts_js_ckznx order by 1;

declare
  var_a varchar(20);
begin
	var_a = '232';
	update ckts_js_ckznx t1 set xf_nsrsbh = '789'
        where t1.tslsh = var_a
			and not exists (select 1
                          from dzfp_ypyw_fpckznx_yrz t2
                          where t2.znxzmbh = t1.no
                            and t2.gfsbh = t1.gf_nsrsbh);
end
/
select * from ckts_js_ckznx order by 1;
delete from dzfp_ypyw_fpckznx_yrz where znxzmbh='342' and tslsh='672';

declare
  var_a varchar(20);
begin
	var_a = '232';
	update ckts_js_ckznx t1 set xf_nsrsbh = '789'
        where t1.tslsh = var_a
			and not exists (select 1
                          from dzfp_ypyw_fpckznx_yrz t2
                          where t2.znxzmbh = t1.no
                            and t2.gfsbh = t1.gf_nsrsbh);
end
/
select * from ckts_js_ckznx order by 1;

drop table dzfp_ypyw_fpckznx_yrz, ckts_js_ckznx;

-- fix coredump in transformValuesClause
set enable_fast_query_shipping to on;

SELECT
    "__Alias_853__".column1 AS c0,
    "__Alias_853__".column2 AS c2,
    "__Alias_853__".column3 AS c3,
    "__Alias_853__".column4 AS c4,
    "__Alias_853__".column5 AS c5,
    "__Alias_853__".column6 AS c6,
    "__Alias_853__".column7 AS c7,
    "__Alias_853__".column8 AS c8,
    "__Alias_853__".column9 AS c9
FROM
    (
        VALUES
            (
                1,
                NULL :: text,
                NULL :: text,
                NULL :: text,
                NULL :: text,
                NULL :: text,
                NULL :: text,
                NULL :: text,
                NULL :: text
            ),
            (
                NULL :: integer,
                NULL :: text,
                NULL :: text,
                NULL :: text,
                NULL :: text,
                NULL :: text,
                NULL :: text,
                NULL :: text,
                NULL :: text
            )
    ) "__Alias_853__";


-- Enhance FQS (Fast Query Shipping) to support more sublink scenarios.
drop table if exists t_fqs1;
drop table if exists t_fqs2;
create table t_fqs1(c1 int, c2 int, c3 int);
create table t_fqs2(c1 int, c2 int, c3 int) distribute by replication;
explain (costs off)
select count(*) from t_fqs1 where c1 = 1 and exists(select 1 from t_fqs2 where t_fqs1.c2=t_fqs2.c2);
drop table t_fqs1;
drop table t_fqs2;

--[Performance]support function clipping in fqs for update statements.
drop table if exists TXN_PROC_RESULT;
create table TXN_PROC_RESULT(
ACTIVITY_CODE char(8),
REQ_TRANS_ID char(32),
TRADE_SESSION char(32),
RESULT_CODE int,
RESULT_DESC char(256),
OLD_TRADE_SESSION char(32),
NOTIFY_TYPE char(2),
CentreID char(4))  with (fillfactor=70) distribute by shard(TRADE_SESSION);

create index idx_TRADE_SESSION on TXN_PROC_RESULT(TRADE_SESSION);
create index  idx_REQ_TRANS_ID on TXN_PROC_RESULT(REQ_TRANS_ID);

prepare fun1(varchar) as UPDATE  TXN_PROC_RESULT
SET                                                     
RESULT_CODE = RESULT_CODE+1,                                
RESULT_DESC = '用户状态正常222'                            
WHERE                                                   
        TRADE_SESSION = $1;
explain(costs off) execute fun1('599');
drop table TXN_PROC_RESULT;

-- fix core in GetRelationNodesByQuals
drop table if exists dzfp_ypyw_fpckznx_yrz, ckts_js_ckznx;
create table dzfp_ypyw_fpckznx_yrz (
    znxzmbh character varying(50) not null,
    gfsbh character varying(40) not null,
    tslsh character varying(100)
)
distribute by shard (gfsbh);
create table ckts_js_ckznx (
	fpdmhm character varying(30) not null,
    tslsh character varying(32) not null,
    no character varying(50) not null,
    xf_nsrsbh character varying(20) not null,
    gf_nsrsbh character varying(20) not null
)
distribute by shard (fpdmhm);
insert into dzfp_ypyw_fpckznx_yrz values ('34', '111', '67');
insert into dzfp_ypyw_fpckznx_yrz values ('341', '111', '671');
insert into dzfp_ypyw_fpckznx_yrz values ('342', '672', '672');
insert into dzfp_ypyw_fpckznx_yrz values ('343', '111', '673');
insert into ckts_js_ckznx values ('12','23','34','45','67');
insert into ckts_js_ckznx values ('121','231','341','451','671');
insert into ckts_js_ckznx values ('122','232','342','452','672');
insert into ckts_js_ckznx values ('123','233','343','453','673');
select * from ckts_js_ckznx order by 1;

DO $$
DECLARE
  var_a CHAR(20);
BEGIN
  var_a := '232';
  UPDATE ckts_js_ckznx t1
  SET xf_nsrsbh = '789'
  WHERE t1.tslsh = var_a
    AND NOT EXISTS (
      SELECT 1
      FROM dzfp_ypyw_fpckznx_yrz t2
      WHERE t2.znxzmbh = t1.no
        AND t2.gfsbh = t1.gf_nsrsbh
    );
END;
$$;

SELECT * FROM ckts_js_ckznx ORDER BY 1;
DELETE FROM dzfp_ypyw_fpckznx_yrz WHERE znxzmbh = '342' AND tslsh = '672';

DO $$
DECLARE
  var_a CHAR(20);
BEGIN
  var_a := '232';
  UPDATE ckts_js_ckznx t1
  SET xf_nsrsbh = '789'
  WHERE t1.tslsh = var_a
    AND NOT EXISTS (
      SELECT 1
      FROM dzfp_ypyw_fpckznx_yrz t2
      WHERE t2.znxzmbh = t1.no
        AND t2.gfsbh = t1.gf_nsrsbh
    );
END;
$$ LANGUAGE default_plsql;
select * from ckts_js_ckznx order by 1;
drop table dzfp_ypyw_fpckznx_yrz, ckts_js_ckznx;
reset enable_fast_query_shipping;
-- Joining the S distribution with the R distribution without using FQS to solve the issue of duplicate data returned.
drop table if exists rqg_table1 cascade;
drop table if exists rqg_table4 cascade;
drop table if exists rqg_table6 cascade;
CREATE TABLE rqg_table1 (
c0 int,
c1 int default null,
c2 char (2000),
c3 char (2000) default null,
c4 char(100),
c5 char(100) default null,
c6 char(100),
c7 char(100) default null,
c8 char(100),
c9 char(100) default null) Distribute By SHARD(C0);
CREATE OR REPLACE VIEW view_rqg_table1_1 AS SELECT * FROM rqg_table1;
INSERT INTO rqg_table1 VALUES  (NULL, -474021888, 'bar', 'wnznrriz', '2019-03-10 21:32:00.049726', '1994-09-25', NULL, '1988-11-14 20:26:34.007149', '07:20:19.002787', '17:15:12.047025') ,  (NULL, NULL, 'nznrrizp', 'znrrizpf', '1979-12-25 15:33:21.017792', '2034-01-16 08:16:51.047742', '1973-08-03', '2016-06-08', '10:33:28.038513', '09:41:11.001950') ,  (1, NULL, 'n', 'rr', '1979-05-15 04:34:17.026511', NULL, NULL, '1987-09-27 18:24:44.049052', '13:28:27.053467', '23:05:09.047913') ,  (2, NULL, 'rizpfkxce', 'izpf', '1973-08-27 17:01:35.039391', '2011-12-06', '1977-08-15 21:22:47.043234', '1981-07-14 13:09:10.008739', '18:19:40.062049', '20:45:51.041707') ,  (NULL, -517472256, 'zpfkxc', 'bar', '2028-12-21 02:04:29.026800', '2000-08-02 01:48:51.060042', '2029-03-22 13:33:45.030320', '1988-01-03 21:10:10.010138', '23:07:09.047149', '14:13:52.000632') ,  (148242432, NULL, 'bar', 'pfkx', '1983-03-20 02:18:28.038254', '2021-11-05', '1972-03-06', '2033-08-10 23:31:29.017251', '04:41:16.038240', '07:27:40.004168') ,  (NULL, 0, 'bar', 'bar', NULL, '1998-01-14', '1998-07-16', '2012-07-12', '05:52:43.010380', '05:58:54.016602') ,  (1075183616, -198246400, 'fkxce', 'bar', NULL, '2021-03-28 12:03:20.002370', '2033-05-04', '2002-10-04 02:05:45.044331', '05:02:02.001813', '20:26:26.043173') ,  (NULL, -1553399808, 'bar', 'kxceks', '1986-05-06', NULL, NULL, '2010-03-14 21:29:56.045321', '00:41:58.049195', '14:29:37.030503') ,  (NULL, 1907556352, 'bar', 'x', '1973-10-04', NULL, '1986-01-02', '1972-07-21 07:33:26.022720', '16:10:54.034446', '21:06:03.057652') ;
CREATE TABLE rqg_table4 (
c0 int,
c1 int default null,
c2 char (2000),
c3 char (2000) default null,
c4 char(100),
c5 char(100) default null,
c6 char(100),
c7 char(100) default null,
c8 char(100),
c9 char(100) default null)    PARTITION BY hash( c3) distribute by replication;
create TABLE rqg_table4_p0 partition of rqg_table4 for values with(modulus 2,remainder 0);
create TABLE rqg_table4_p1 partition of rqg_table4 for values with(modulus 2,remainder 1);
CREATE OR REPLACE VIEW view_rqg_table4_1 AS SELECT * FROM rqg_table4;
INSERT INTO rqg_table4 VALUES  (NULL, NULL, 'jjvvkymal', 'jv', '2003-05-13 00:09:55.009711', '1982-01-28 01:01:25.035783', '1985-06-19', '2014-03-21 20:32:19.002547', '04:18:56.049954', '04:25:48.003597') ,  (NULL, -1607991296, 'bar', 'vvkymal', NULL, '2004-12-17 21:47:57.031968', '1999-02-09 01:04:58.008615', '1998-02-07', '02:59:31.001254', '22:50:06.052457') ,  (1, 2084831232, 'bar', 'bar', '2010-08-06 12:31:05.012924', '1972-10-13 05:35:05.053912', '2023-07-21 22:27:07.001772', '2035-03-22', '07:12:39.050276', '08:05:43.030224') ,  (NULL, NULL, 'bar', 'bar', '2000-11-03', '2032-09-02 14:18:01.048136', '1986-08-10 04:10:04.048182', '2032-12-23 13:59:29.061691', '21:31:56.027938', NULL) ,  (1, NULL, 'bar', 'vkymaluk', '1998-10-26 18:55:42.055527', '2000-05-21', '2004-01-11', '2034-04-23 18:29:07.007017', '14:53:41.051367', '22:16:47.019207') ,  (9, 0, 'kymaluk', 'bar', NULL, NULL, NULL, '1979-04-15 10:37:25.035136', '17:23:06.036252', '13:46:00.010627') ,  (3, 8, 'bar', 'ymaluk', '1981-03-12 15:50:52.058095', '1991-06-09 14:50:33.019978', '1983-02-14', '1980-11-17 18:38:45.051510', '13:00:14.063694', '20:56:56.051530') ,  (734003200, NULL, 'm', 'bar', '2013-04-01', '2023-04-15 06:44:52.023488', '2006-10-09 01:55:08.052664', '2031-08-12 10:38:44.052510', NULL, NULL) ,  (425263104, NULL, 'alukquk', 'lu', NULL, '1998-09-16', '2008-02-14 06:51:42.045782', '1981-12-16 07:38:47.015426', '07:41:55.063370', '19:56:21.016713') ,  (1170145280, 305922048, 'ukqukk', 'kqukkoe', '1992-09-17 00:09:05.031045', '2007-03-23 16:49:54.033371', NULL, NULL, '10:48:46.019902', '19:26:57.063604') ;

CREATE TABLE rqg_table6 (
c0 int,
c1 int default null,
c2 char (2000),
c3 char (2000) default null,
c4 char(100),
c5 char(100) default null,
c6 char(100),
c7 char(100) default null,
c8 char(100),
c9 char(100) default null)    PARTITION BY hash( c8) distribute by replication;
create TABLE rqg_table6_p0 partition of rqg_table6 for values with(modulus 4,remainder 0);
create TABLE rqg_table6_p1 partition of rqg_table6 for values with(modulus 4,remainder 1);
create TABLE rqg_table6_p2 partition of rqg_table6 for values with(modulus 4,remainder 2);
create TABLE rqg_table6_p3 partition of rqg_table6 for values with(modulus 4,remainder 3);
CREATE OR REPLACE VIEW view_rqg_table6_1 AS SELECT * FROM rqg_table6;
INSERT INTO rqg_table6 VALUES  (-1606483968, NULL, 'bar', 'ei', NULL, '2034-02-09', '1980-10-19', '2009-11-24 04:16:35.007736', '18:14:13.009635', '05:04:47.025828'),(-1606483968, NULL, 'bar', 'ei', NULL, '2034-02-09', '1980-10-19', '2009-11-24 04:16:35.007736', '18:14:13.009635', '05:04:47.025828'),(-1606483968, NULL, 'bar', 'ei', NULL, '2034-02-09', '1980-10-19', '2009-11-24 04:16:35.007736', '18:14:13.009635', '05:04:47.025828') ;

explain (costs off)
SELECT a1.c0, a1.c2, a1.c3, a1.c4, a1.c5, a1.c6, a1.c7, a1.c8, a1.c9 FROM rqg_table1 AS a1 GROUP BY a1.c0, a1.c2, a1.c3, a1.c4, a1.c5, a1.c6, a1.c7, a1.c8, a1.c9 HAVING NOT EXISTS ( SELECT c3 FROM ( SELECT a1.c0, a1.c2, a1.c3, a1.c4, a1.c5, a1.c6, a1.c7, a1.c8, a1.c9 FROM rqg_table4 AS a1 ) )
UNION ALL
SELECT a1.c0, a1.c2, a1.c3, a1.c4, a1.c5, a1.c6, a1.c7, a1.c8, a1.c9 FROM rqg_table6 AS a1 WHERE ( 4 + 6 ) > a1.c0;

SELECT a1.c0, a1.c2, a1.c3, a1.c4, a1.c5, a1.c6, a1.c7, a1.c8, a1.c9 FROM rqg_table1 AS a1 GROUP BY a1.c0, a1.c2, a1.c3, a1.c4, a1.c5, a1.c6, a1.c7, a1.c8, a1.c9 HAVING NOT EXISTS ( SELECT c3 FROM ( SELECT a1.c0, a1.c2, a1.c3, a1.c4, a1.c5, a1.c6, a1.c7, a1.c8, a1.c9 FROM rqg_table4 AS a1 ) )
UNION ALL
SELECT a1.c0, a1.c2, a1.c3, a1.c4, a1.c5, a1.c6, a1.c7, a1.c8, a1.c9 FROM rqg_table6 AS a1 WHERE ( 4 + 6 ) > a1.c0;

drop table rqg_table1 cascade;
drop table rqg_table4 cascade;
drop table rqg_table6 cascade;

-- test 'ERROR:  column A.C_PARTNERACCO does not exist'
drop table if exists T_TA_SHARECURRENTS cascade;
drop table if exists T_KDTA_SHARECURRENTS cascade;
drop view if exists v_all_sharecurrents cascade;
drop table if exists T_DS_ACCO cascade;
drop table if exists T_MDM_FUNDINFO_GA cascade;
create table T_TA_SHARECURRENTS (
  busi_date        varchar(8),
  recordid         numeric(10),
  custno           numeric(10),
  d_cdate          varchar(8),
  c_cserialno      varchar(20),
  c_businflag      varchar(3),
  d_requestdate    varchar(8),
  c_requestno      varchar(24),
  c_fundacco       varchar(20),
  c_tradeacco      varchar(24),
  c_fundcode       varchar(8),
  c_sharetype      varchar(1),
  c_agencyno       varchar(9),
  c_netno          varchar(9),
  f_occurshares    numeric(16,2),
  f_occurbalance   numeric(16,2),
  f_lastshares     numeric(16,2),
  d_sharevaliddate varchar(8),
  d_transtime      timestamp(0) default sysdate_orcl()
)
distribute by shard (custno) ;

create table T_KDTA_SHARECURRENTS
(
  busi_date        varchar(8),
  recordid         numeric(10),
  custno           numeric(10),
  d_cdate          varchar(8),
  c_cserialno      varchar(20),
  c_businflag      varchar(3),
  d_requestdate    varchar(8),
  c_requestno      varchar(40),
  c_fundacco       varchar(12),
  c_tradeacco      varchar(17),
  c_fundcode       varchar(6),
  c_sharetype      varchar(1),
  c_agencyno       varchar(9),
  c_netno          varchar(9),
  f_occurshares    numeric(16,2),
  f_occurbalance   numeric(16,2),
  f_lastshares     numeric(16,2),
  d_sharevaliddate varchar(8),
  d_transtime      timestamp(0) default sysdate_orcl()
)
distribute by shard (custno) ;

create or replace view v_all_sharecurrents
(tacode, busi_date, recordid, custno, d_cdate, c_cserialno, c_businflag, d_requestdate, c_requestno, c_fundacco, c_tradeacco, c_fundcode, c_sharetype, c_agencyno, c_netno, f_occurshares, f_occurbalance, f_lastshares, d_sharevaliddate, c_partnerid, c_partneracco)
as
select
   '11' TACODE,   a.BUSI_DATE,a.RECORDID,a.CUSTNO,a.D_CDATE,a.C_CSERIALNO,a.C_BUSINFLAG,a.D_REQUESTDATE,a.C_REQUESTNO,a.C_FUNDACCO,a.C_TRADEACCO,a.C_FUNDCODE,a.C_SHARETYPE,a.C_AGENCYNO,a.C_NETNO,a.F_OCCURSHARES,a.F_OCCURBALANCE,a.F_LASTSHARES,a.D_SHAREVALIDDATE,null,null
from
  t_ta_sharecurrents a
  where 1=1
union all
select
   'AA' TACODE,   e1.BUSI_DATE,e1.RECORDID,e1.CUSTNO,e1.D_CDATE,e1.C_CSERIALNO,e1.C_BUSINFLAG,e1.D_REQUESTDATE,e1.C_REQUESTNO,e1.C_FUNDACCO,e1.C_TRADEACCO,e1.C_FUNDCODE,e1.C_SHARETYPE,e1.C_AGENCYNO,e1.C_NETNO,e1.F_OCCURSHARES,e1.F_OCCURBALANCE,e1.F_LASTSHARES,e1.D_SHAREVALIDDATE,null,null
from
  t_kdta_sharecurrents e1
;

create table T_DS_ACCO
(
  busi_date                varchar(8) not null,
  recordid                 numeric(10),
  custno                   numeric(10),
  vc_custno                varchar(12),
  vc_tradecontact          varchar(20),
  vc_tradeacco             varchar(17) not null
) distribute by shard (custno);

create table T_XJB_ACCOINFO
(
  custno               numeric(10) not null,
  vc_tradeacco         varchar(24) not null,
  vc_custno            varchar(20),
  vc_partneracco       varchar(64) not null,
  vc_partnerid         varchar(30) not null
) distribute by shard (custno);

create table T_MDM_FUNDINFO_GA
(
  id                       numeric not null,
  s_fundcode               varchar(10),
  s_webtype                varchar(100)
) distribute by replication;

explain (costs off, nodes off)
select
	a.d_cdate as cDate,
	a.d_sharevaliddate as shareValidDate,
	a.c_fundacco as fundAcco,
	a.c_tradeacco as tradeAcco,
	a.c_fundcode as fundCode,
	a.c_agencyno as agencyNo ,
	a.tacode as taCode,
	a.c_sharetype as shareType,
	nvl(a.f_lastshares,
	0) as lastShares
from
	v_all_sharecurrents a
left join (
	select
		vc_tradeacco,
		max(vc_tradecontact) vc_tradecontact,
		custno
	from
		t_ds_acco
	where
		custno = 1012307758
	group by
		custno,
        vc_tradeacco
		) b on
	nvl(a.c_partneracco,
	a.c_tradeacco) = b.vc_tradeacco
	and a.custno = b.custno
left join (
	select
		vc_tradeacco,
		max(vc_partnerid) vc_partnerid ,
		custno
	from
		t_xjb_accoinfo
	where
		custno = 1012307758
	group by
        custno,
		vc_tradeacco
		) c on
	a.c_tradeacco = c.vc_tradeacco
	and a.custno = c.custno
where
	a.d_sharevaliddate > '20000101'
	and a.d_sharevaliddate > a.d_cdate
	and a.custno = 1012307758
	and (a.tacode <> '98'
		or substr(a.c_fundacco,
		1,
		2) in ('98', '99'))
	and a.c_fundcode not like '1193%'
	and (a.tacode <> '50'
		or a.c_fundcode in  (
		select
			S_FUNDCODE
		from
			T_MDM_FUNDINFO_GA
		where
			S_WEBTYPE in ('2', 'A')));

drop view v_all_sharecurrents cascade;
drop table T_TA_SHARECURRENTS cascade;
drop table T_KDTA_SHARECURRENTS cascade;
drop table T_DS_ACCO cascade;
drop table T_MDM_FUNDINFO_GA cascade;