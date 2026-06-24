CREATE EXTENSION test_csnlog_compress;
create table csnlog_compress_test(i int);
insert into csnlog_compress_test (i) values(generate_series(1,100));
insert into csnlog_compress_test (i) values(generate_series(1,100));
insert into csnlog_compress_test (i) values(generate_series(1,100));
insert into csnlog_compress_test (i) values(generate_series(1,100));
insert into csnlog_compress_test (i) values(generate_series(1,100));
insert into csnlog_compress_test (i) values(generate_series(1,100));
insert into csnlog_compress_test (i) values(generate_series(1,100));
insert into csnlog_compress_test (i) values(generate_series(1,100));

do $$
declare
    id INTEGER :=1;
begin
    WHILE id < 32769 LOOP
	  id := id+1;
	  begin
             PERFORM txid_current();
             commit;
	  end;
    END LOOP;
END; $$;


vacuum;
checkpoint;
SELECT test_compress_csnlog_file();

insert into csnlog_compress_test (i) values(generate_series(1,100));
insert into csnlog_compress_test (i) values(generate_series(1,100));
insert into csnlog_compress_test (i) values(generate_series(1,100));
insert into csnlog_compress_test (i) values(generate_series(1,100));

select count(*) from csnlog_compress_test where i=10;

do $$
declare
    id INTEGER :=1;
begin
    WHILE id < 32769 LOOP
	  id := id+1;
	  begin
             PERFORM txid_current();
             commit;
	  end;
    END LOOP;
END; $$;
vacuum;
checkpoint;
SELECT test_rename_csnlog_file();
drop table csnlog_compress_test;
