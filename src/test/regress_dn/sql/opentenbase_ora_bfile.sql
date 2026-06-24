\c regression_ora
-- test bfile
create table test_20231019(c1 int,c2 bfile);
insert into test_20231019 values(1);
/*
execute direct on (coord1) $$select * from test_20231019$$;
execute direct on (coord2) $$select * from test_20231019$$;
*/
drop table test_20231019;

create directory bfiledir as '/tmp';
select DIRECTORY_NAME,NODENAME from pg_directory;
drop directory bfiledir;
select '11111'::bytea::bfile from dual;
select '11111'::clob::bfile from dual;
