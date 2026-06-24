set enable_opentenbase_ora_compatible to on;
set enable_seqscan = off;
set enable_indexscan = on;
set enable_bitmapscan = on;
create table tbl1 (id int, v1 opentenbase_ora.date);
insert into tbl1 values (generate_series(1, 100000), current_timestamp::opentenbase_ora.date);
create index idx1 on tbl1 using btree (v1);
explain select * from tbl1 where v1 > sysdate - 1;
drop table if exists tbl1;
