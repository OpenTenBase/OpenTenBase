SELECT size, pg_size_pretty(size), pg_size_pretty(-1 * size) FROM
    (VALUES (10::bigint), (1000::bigint), (1000000::bigint),
            (1000000000::bigint), (1000000000000::bigint),
            (1000000000000000::bigint)) x(size);

SELECT size, pg_size_pretty(size), pg_size_pretty(-1 * size) FROM
    (VALUES (10::numeric), (1000::numeric), (1000000::numeric),
            (1000000000::numeric), (1000000000000::numeric),
            (1000000000000000::numeric),
            (10.5::numeric), (1000.5::numeric), (1000000.5::numeric),
            (1000000000.5::numeric), (1000000000000.5::numeric),
            (1000000000000000.5::numeric)) x(size);

SELECT size, pg_size_bytes(size) FROM
    (VALUES ('1'), ('123bytes'), ('1kB'), ('1MB'), (' 1 GB'), ('1.5 GB '),
            ('1TB'), ('3000 TB'), ('1e6 MB')) x(size);

-- case-insensitive units are supported
SELECT size, pg_size_bytes(size) FROM
    (VALUES ('1'), ('123bYteS'), ('1kb'), ('1mb'), (' 1 Gb'), ('1.5 gB '),
            ('1tb'), ('3000 tb'), ('1e6 mb')) x(size);

-- negative numbers are supported
SELECT size, pg_size_bytes(size) FROM
    (VALUES ('-1'), ('-123bytes'), ('-1kb'), ('-1mb'), (' -1 Gb'), ('-1.5 gB '),
            ('-1tb'), ('-3000 TB'), ('-10e-1 MB')) x(size);

-- different cases with allowed points
SELECT size, pg_size_bytes(size) FROM
     (VALUES ('-1.'), ('-1.kb'), ('-1. kb'), ('-0. gb'),
             ('-.1'), ('-.1kb'), ('-.1 kb'), ('-.0 gb')) x(size);

-- invalid inputs
SELECT pg_size_bytes('1 AB');
SELECT pg_size_bytes('1 AB A');
SELECT pg_size_bytes('1 AB A    ');
SELECT pg_size_bytes('9223372036854775807.9');
SELECT pg_size_bytes('1e100');
SELECT pg_size_bytes('1e1000000000000000000');
SELECT pg_size_bytes('1 byte');  -- the singular "byte" is not supported
SELECT pg_size_bytes('');

SELECT pg_size_bytes('kb');
SELECT pg_size_bytes('..');
SELECT pg_size_bytes('-.');
SELECT pg_size_bytes('-.kb');
SELECT pg_size_bytes('-. kb');

SELECT pg_size_bytes('.+912');
SELECT pg_size_bytes('+912+ kB');
SELECT pg_size_bytes('++123 kB');

-- test pg_database_size (temp disable unstable cases)
--CREATE DATABASE test_dbsize;
--\c test_dbsize
--SELECT pg_database_size('test_dbsize');
--CREATE TABLE rowtbl (i int, j text);
--SELECT pg_database_size('test_dbsize');
--INSERT INTO rowtbl VALUES (generate_series(1,10), 'hello word');
--SELECT pg_database_size('test_dbsize');
--SELECT pg_database_size('test_dbsize');
--SELECT pg_database_size('test_dbsize');
--\c regression
--DROP DATABASE test_dbsize;

-- Similar to PostgreSQL, returning null for an invalid OID.
select pg_relation_size(0);
select pg_relation_size(888);

-- test index table size
create table testtable5(f1 int primary key,f2 int) distribute by shard(f1);
insert into testtable5 select i, i+10 from generate_series(1, 100000) i;
select (pg_table_size('testtable5_pkey') > 8192) as opentenbase;
select (pg_relation_size('testtable5_pkey') > 8192) as opentenbase;
select (pg_total_relation_size('testtable5_pkey') > 8192)  as opentenbase;
select (pg_allocated_table_size('testtable5_pkey') > 8192) as opentenbase;
select (pg_allocated_total_relation_size('testtable5_pkey') > 8192)  as opentenbase;

drop table t_col_no, testtable5;
