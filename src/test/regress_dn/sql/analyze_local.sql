create table if not exists tbl_20231124(a int) with(autovacuum_enabled=off);

explain select * from tbl_20231124;
explain select * from tbl_20231124 where a > 10;
explain select * from tbl_20231124 where a <= 10;

begin;
insert into tbl_20231124 select generate_series(1, 10000);
analyze (local) tbl_20231124;
select count(*) from pg_locks where pid = pg_backend_pid() and mode = 'ShareUpdateExclusiveLock';
explain select * from tbl_20231124;
explain select * from tbl_20231124 where a > 10;
explain select * from tbl_20231124 where a <= 10;
rollback;

explain select * from tbl_20231124;
explain select * from tbl_20231124 where a > 10;
explain select * from tbl_20231124 where a <= 10;

drop table tbl_20231124;

\c regression_ora
create global temp table gtt_20231124(a int) with(autovacuum_enabled=off);
begin;
insert into gtt_20231124 select generate_series(1, 10000);
analyze (local) gtt_20231124;
select count(*) from pg_locks where pid = pg_backend_pid() and mode = 'ShareUpdateExclusiveLock';
explain select * from gtt_20231124;
explain select * from gtt_20231124 where a > 10;
explain select * from gtt_20231124 where a <= 10;
commit;

drop table gtt_20231124;