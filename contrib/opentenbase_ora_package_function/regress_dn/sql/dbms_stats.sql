\c opentenbase_ora_package_function_regression_ora
SET client_min_messages = error;
DROP USER IF EXISTS godlike_dbms_stats;
CREATE user godlike_dbms_stats superuser;
create user joe;
create user no_privilege;

\c - GODLIKE_DBMS_STATS
grant usage on schema dbms_stats to no_privilege;
grant usage on schema dbms_stats to joe;

\c - JOE
-- table joe_t
create table joe_t (id integer not null PRIMARY KEY, test integer);
create index joe_t_test_idx on joe_t(test);
insert into joe_t SELECT generate_series(1,300) as key, 100::integer;
insert into joe_t SELECT generate_series(301,600) as key, 200::integer;
insert into joe_t SELECT generate_series(601,1000) as key, 300::integer;
-- table joe_t_p
create table joe_t_p (id integer not null PRIMARY KEY, test integer) distribute by shard(id);
create index joe_t_p_test_idx on joe_t_p(test);
insert into joe_t_p SELECT generate_series(1,300) as key, 100::integer;
insert into joe_t_p SELECT generate_series(301,600) as key, 200::integer;
insert into joe_t_p SELECT generate_series(601,1000) as key, 300::integer;

\c - NO_PRIVILEGE
exec dbms_stats.gather_table_stats(ownname => 'joe',tabname => 'joe_t');
exec dbms_stats.get_table_stats(ownname => 'joe',tabname => 'joe_t');
exec dbms_stats.get_column_stats('joe', 'joe_t', 'test');
exec dbms_stats.get_index_stats('joe', 'joe_t_test_idx');
exec dbms_stats.get_index_stats('joe', 'joe_t_p_test_idx', 'joe_t_p_test_idx_part_0');

\c - JOE
exec dbms_stats.gather_table_stats(ownname => 'joe',tabname => 'joe_t');
exec dbms_stats.get_table_stats(ownname => 'joe',tabname => 'joe_t');
exec dbms_stats.get_column_stats('joe', 'joe_t', 'test');
declare
numrows numeric;
numlblks numeric;
numdist numeric;
avglblk numeric;
avgdblk numeric;
clstfct numeric;
indlevel numeric;
cachedblk numeric;
cachehit numeric;
begin
exec dbms_stats.get_index_stats('joe', 'joe_t_p_test_idx', numrows=>numrows, numlblks=>numlblks, numdist=>numdist, avglblk=>avglblk, avgdblk=>avgdblk, clstfct=>clstfct, indlevel=>indlevel, cachedblk=>cachedblk, cachehit=>cachehit);
--raise notice '%', numrows; 统计信息更新不及时
end;
/
exec dbms_stats.get_index_stats('joe', 'joe_t_pkey');

exec dbms_stats.gather_database_stats();
exec dbms_stats.get_table_stats(ownname => 'joe',tabname => 'joe_t_p');
exec dbms_stats.get_column_stats('joe', 'joe_t_p', 'test');
declare
numrows numeric;
numlblks numeric;
numdist numeric;
avglblk numeric;
avgdblk numeric;
clstfct numeric;
indlevel numeric;
cachedblk numeric;
cachehit numeric;
begin
exec dbms_stats.get_index_stats('joe', 'joe_t_p_test_idx', numrows=>numrows, numlblks=>numlblks, numdist=>numdist, avglblk=>avglblk, avgdblk=>avgdblk, clstfct=>clstfct, indlevel=>indlevel, cachedblk=>cachedblk, cachehit=>cachehit);
raise notice '%', numrows;
end;
/
exec dbms_stats.get_table_stats('joe', 'joe_t_p', 'joe_t_p_part_0');
exec dbms_stats.get_index_stats('joe', 'joe_t_p_test_idx', 'joe_t_p_test_idx_part_0');

-- clean
\c - GODLIKE_DBMS_STATS
drop table joe_t;
drop table joe_t_p;
REVOKE usage ON schema dbms_stats FROM joe;
REVOKE usage ON schema dbms_stats FROM no_privilege;

-- test identifier in double quotes
\c - GODLIKE_DBMS_STATS
create table "t_ident_in_d_quotes_20240124"("id" int);
insert into "t_ident_in_d_quotes_20240124" SELECT generate_series(1,300);
exec dbms_stats.gather_table_stats(ownname => 'GODLIKE_DBMS_STATS',tabname => 't_ident_in_d_quotes_20240124');
exec dbms_stats.gather_table_stats(ownname => 'GODLIKE_DBMS_STATS',tabname => '"t_ident_in_d_quotes_20240124"');
exec dbms_stats.get_table_stats(ownname => 'GODLIKE_DBMS_STATS',tabname => 't_ident_in_d_quotes_20240124');
exec dbms_stats.get_table_stats(ownname => 'GODLIKE_DBMS_STATS',tabname => '"t_ident_in_d_quotes_20240124"');
exec dbms_stats.get_column_stats('GODLIKE_DBMS_STATS', '"t_ident_in_d_quotes_20240124"', 'id');
exec dbms_stats.get_column_stats('GODLIKE_DBMS_STATS', '"t_ident_in_d_quotes_20240124"', '"id"');
create index "i_ident_in_d_quotes_20240124" on "t_ident_in_d_quotes_20240124"("id");
exec dbms_stats.get_index_stats('GODLIKE_DBMS_STATS', '"t_ident_in_d_quotes_20240124"', 'i_ident_in_d_quotes_20240124');
exec dbms_stats.get_index_stats('GODLIKE_DBMS_STATS', '"t_ident_in_d_quotes_20240124"', '"i_ident_in_d_quotes_20240124"');
create user "r_ident_in_dq_20240124" with superuser;
\c - r_ident_in_dq_20240124
create table t_ident_in_d_quotes_20240125(id int);
insert into t_ident_in_d_quotes_20240125 SELECT generate_series(1,300);
create index i_ident_in_d_quotes_20240125 on t_ident_in_d_quotes_20240125(id);
exec dbms_stats.gather_table_stats(ownname => 'r_ident_in_dq_20240124',tabname => 'T_IDENT_IN_D_QUOTES_20240125');
exec dbms_stats.gather_table_stats(ownname => '"r_ident_in_dq_20240124"',tabname => 't_ident_In_D_quotes_20240125');
exec dbms_stats.get_table_stats(ownname => 'r_ident_in_dq_20240124',tabname => 'T_IDENT_IN_D_QUOTES_20240125');
exec dbms_stats.get_table_stats(ownname => '"r_ident_in_dq_20240124"',tabname => 't_ident_in_d_quotes_20240125');
exec dbms_stats.get_column_stats('r_ident_in_dq_20240124', 'T_IDENT_IN_D_QUOTES_20240125', 'ID');
exec dbms_stats.get_column_stats('"r_ident_in_dq_20240124"', 't_ident_in_d_quotes_20240125', 'id');
exec dbms_stats.get_index_stats('r_ident_in_dq_20240124', 'T_IDENT_IN_D_QUOTES_20240125', 'I_IDENT_IN_D_QUOTES_20240125');
exec dbms_stats.get_index_stats('"r_ident_in_dq_20240124"', 't_ident_in_d_quotes_20240125', 'i_ident_in_d_quotes_20240125');
\c - GODLIKE_DBMS_STATS
drop table t_ident_in_d_quotes_20240125;
drop user "r_ident_in_dq_20240124";
drop table "t_ident_in_d_quotes_20240124";
