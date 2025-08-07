-- test multi-threads elog
drop table if exists tbl_elog_test11;
create table tbl_elog_test11(f1 int, f2 int);
insert into tbl_elog_test11 values (generate_series(11,20), generate_series(11,20));

set temp_file_limit='100';
show temp_file_limit;
insert into tbl_elog_test11 values (generate_series(1000001,2000000), generate_series(1000001,2000000));
reset temp_file_limit;
drop table tbl_elog_test11;
