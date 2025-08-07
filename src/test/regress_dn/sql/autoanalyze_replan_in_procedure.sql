\c regression_ora
set xact_autoanalyze = on;

-- test dml mode
set xact_autoanalyze_mode = 'dml';

drop table if exists t1_20231215;
create table t1_20231215(id numeric(22), name varchar(100));
create index on t1_20231215(id);
drop table if exists t2_20231215;
create table t2_20231215(a int);

begin;

DECLARE
    explain_result TEXT;
    line TEXT;
    index_scan_count int;
    index_only_scan_count int;
    seq_scan_count int;
    auto_analyze_count int;
BEGIN
    index_scan_count := 0;
    index_only_scan_count := 0;
    seq_scan_count := 0;
    auto_analyze_count := 0;

	for i in 1..300 loop
		insert into t1_20231215 values (i, md5(i::text));
		insert into t2_20231215 select count(1) from t1_20231215 where id > 290;

	    -- Use regular expressions to count the scan methods
	    SELECT count(*) INTO index_scan_count FROM regexp_matches(explain_result, 'Index Scan', 'g');
	    SELECT count(*) INTO index_only_scan_count FROM regexp_matches(explain_result, 'Index Only Scan', 'g');
	    SELECT count(*) INTO seq_scan_count FROM regexp_matches(explain_result, 'Seq Scan', 'g');
	    SELECT ANALYZE_TIMES INTO auto_analyze_count from pg_xact_relstat_info where lower(relname) = 't1_20231215';
    end loop;

	RAISE NOTICE 'Index Scan Count: %', index_scan_count;
	RAISE NOTICE 'Index Only Scan Count: %', index_only_scan_count;
    RAISE NOTICE 'Seq Scan Count: %', seq_scan_count;
    RAISE NOTICE 'Auto Analyze Count: %', auto_analyze_count;

END;
/

commit;

-- test use mode
set xact_autoanalyze_mode = 'use';

drop table if exists t1_20231215;
create table t1_20231215(id numeric(22), name varchar(100));
create index on t1_20231215(id);
drop table if exists t2_20231215;
create table t2_20231215(a int);

begin;

DECLARE
    explain_result TEXT;
    line TEXT;
    index_scan_count int;
    index_only_scan_count int;
    seq_scan_count int;
    auto_analyze_count int;
BEGIN
    index_scan_count := 0;
    index_only_scan_count := 0;
    seq_scan_count := 0;
    auto_analyze_count := 0;

	for i in 1..300 loop
		insert into t1_20231215 values (i, md5(i::text));
		insert into t2_20231215 select count(1) from t1_20231215 where id > 290;
		
	    -- Use regular expressions to count the scan methods
	    SELECT count(*) INTO index_scan_count FROM regexp_matches(explain_result, 'Index Scan', 'g');
	    SELECT count(*) INTO index_only_scan_count FROM regexp_matches(explain_result, 'Index Only Scan', 'g');
	    SELECT count(*) INTO seq_scan_count FROM regexp_matches(explain_result, 'Seq Scan', 'g');
	    SELECT ANALYZE_TIMES INTO auto_analyze_count from pg_xact_relstat_info where lower(relname) = 't1_20231215';
    end loop;

	RAISE NOTICE 'Index Scan Count: %', index_scan_count;
	RAISE NOTICE 'Index Only Scan Count: %', index_only_scan_count;
    RAISE NOTICE 'Seq Scan Count: %', seq_scan_count;
    RAISE NOTICE 'Auto Analyze Count: %', auto_analyze_count;

END;
/

commit;

-- test mix mode
set xact_autoanalyze_mode = 'mix';

drop table if exists t1_20231215;
create table t1_20231215(id numeric(22), name varchar(100));
create index on t1_20231215(id);
drop table if exists t2_20231215;
create table t2_20231215(a int);

begin;

DECLARE
    explain_result TEXT;
    line TEXT;
    index_scan_count int;
    index_only_scan_count int;
    seq_scan_count int;
    auto_analyze_count int;
BEGIN
    index_scan_count := 0;
    index_only_scan_count := 0;
    seq_scan_count := 0;
    auto_analyze_count := 0;

	for i in 1..300 loop
		insert into t1_20231215 values (i, md5(i::text));
		insert into t2_20231215 select count(1) from t1_20231215 where id > 290;
		
	    -- Use regular expressions to count the scan methods
	    SELECT count(*) INTO index_scan_count FROM regexp_matches(explain_result, 'Index Scan', 'g');
	    SELECT count(*) INTO index_only_scan_count FROM regexp_matches(explain_result, 'Index Only Scan', 'g');
	    SELECT count(*) INTO seq_scan_count FROM regexp_matches(explain_result, 'Seq Scan', 'g');
	    SELECT ANALYZE_TIMES INTO auto_analyze_count from pg_xact_relstat_info where lower(relname) = 't1_20231215';
    end loop;

	RAISE NOTICE 'Index Scan Count: %', index_scan_count;
	RAISE NOTICE 'Index Only Scan Count: %', index_only_scan_count;
    RAISE NOTICE 'Seq Scan Count: %', seq_scan_count;
    RAISE NOTICE 'Auto Analyze Count: %', auto_analyze_count;

END;
/

commit;

drop table t1_20231215;
drop table t2_20231215;