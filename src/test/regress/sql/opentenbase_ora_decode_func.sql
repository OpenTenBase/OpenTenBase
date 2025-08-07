\c regression_ora
-- Test ora_hash
SELECT ORA_HASH('opentenbase', 4294967295, 2) FROM DUAL;
SELECT ORA_HASH('opentenbase', 4294967295) FROM DUAL;
SELECT ORA_HASH('opentenbase') FROM DUAL;

-- Test dump
SELECT DUMP('abc', 16, 0, 1) FROM DUAL;
SELECT DUMP('abc', 16) FROM DUAL;
SELECT DUMP('数据库', 16) FROM DUAL;

-- Test vsize
SELECT VSIZE('abc') FROM DUAL;
SELECT VSIZE(123.456) FROM DUAL;
SELECT VSIZE(1) FROM DUAL;
SELECT VSIZE(sysdate) FROM DUAL;
SELECT VSIZE('1'::clob) FROM DUAL;

-- Demo decode
DROP TABLE IF EXISTS tbl_decode_demo;
CREATE TABLE tbl_decode_demo(product_id int, product_name varchar2(20), warehouse_id int);
INSERT INTO tbl_decode_demo VALUES(1320, 'Product A', 1);
INSERT INTO tbl_decode_demo VALUES(2475, 'Product B', 2);
INSERT INTO tbl_decode_demo VALUES(3344, 'Product C', 3);
INSERT INTO tbl_decode_demo VALUES(4227, 'Product D', 4);
INSERT INTO tbl_decode_demo VALUES(5894, 'Product E', 5);
SELECT product_id, product_name,
       DECODE (warehouse_id, 1, 'Southlake',
                             2, 'San Francisco',
                             3, 'New Jersey',
                             4, 'Seattle',
                                'Non domestic') "Location"
  FROM tbl_decode_demo
  ORDER BY product_id, "Location";
DROP TABLE tbl_decode_demo;

-- Demo dump
SELECT DUMP('abc', 16) FROM DUAL;
SELECT DUMP('abc', 1016) FROM DUAL;
SELECT DUMP('abc', 1016, 1, 1) FROM DUAL;
SELECT DUMP(12345, 16) FROM DUAL;
SELECT DUMP(12345, 1016) FROM DUAL;
SELECT DUMP(12345, 1016, 1, 1) FROM DUAL;

-- Demo ora_hash
SELECT ORA_HASH('demodata', 4294967295, 10) FROM DUAL;
SELECT ORA_HASH('demodata', 4294967295) FROM DUAL;
SELECT ORA_HASH('demodata') FROM DUAL;

-- Support the judge whether the PARAM is null in PREPARE stmt
create table ttest_20230607 (a int, b int);
insert into ttest_20230607 values(1,1);
insert into ttest_20230607 values('',1);

prepare s as select * from ttest_20230607 where $1 is null;
execute s('1');
execute s(1);
execute s('');
execute s(NULL);
deallocate s;

prepare s as
select $1 is null from ttest_20230607;
execute s(1);
execute s('');
execute s(NULL);
deallocate s;

prepare s as select decode(a,$1,'first','default') from ttest_20230607;
execute s(1);
execute s(2);
execute s('');
execute s(NULL);
deallocate s;

prepare s as select decode(a,$1,'first',$2,'second') from ttest_20230607;
execute s(-1,2);
execute s(2,1);
execute s('',2);
execute s(1,'');
execute s('','');
execute s(NULL,NULL);
deallocate s;

prepare s as select decode(a,$1 + $2,'first','default') from ttest_20230607;
execute s(1,0);
execute s(2,1);
execute s(1+'',1);
execute s('','');
execute s(NULL,NULL);
deallocate s;

prepare s as select decode(a,$1 + 2,'first','default') from ttest_20230607;
execute s(-1);
execute s(0);
execute s(1+'');
execute s(1+NULL);
execute s('');
execute s(NULL);
deallocate s;

prepare s as
select case when $1 is null then 'first' else 'else' end from ttest_20230607;
execute s('');
execute s(NULL);
execute s(1);
deallocate s;

prepare s as
select case when $1 is null then 'first'
            when $2 is not null then 'second'
            else 'else' end from ttest_20230607;
execute s('',1);
execute s(1,1);
execute s(1,'');
execute s('','');
execute s(NULL,1);
execute s(1,NULL);
execute s(NULL,NULL);
deallocate s;

-- failed
prepare s as
select $1 is distinct from null as "not null" from ttest_20230607;

drop table ttest_20230607;
