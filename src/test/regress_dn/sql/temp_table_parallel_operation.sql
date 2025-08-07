\c regression_ora

set enable_parallel_insert to on;
set enable_parallel_update to on;
set parallel_setup_cost to 0;
set parallel_tuple_cost to 0;
set min_parallel_table_scan_size to 0;
set max_parallel_workers_per_gather=2;

-- create normal table
drop table if exists baseTable;
create table baseTable(col1 int primary key,
                       col2 varchar2(200),
                       col3 int,
                       parent int,
                       varchar_col VARCHAR2(255),
                       nvarchar_col NVARCHAR2(255),
                       char_col CHAR(10),
                       nchar_col NCHAR(10),
                       number_col NUMBER,
                       integer_col INTEGER,
                       float_col FLOAT,
                       date_col DATE
                      );

-- insert data to normal table;
insert into baseTable
select n,
       'col2_'||n,
       n,
       n%100+1,
       'Test VARCHAR2 - ',
       N'Test NVARCHAR2',
       'Test CHAR',
       N'Test NCHAR',
       123.45,
       42,
       3.1415,
       TO_DATE('2024-03-08', 'YYYY-MM-DD')
from generate_series(1,1000) n;

-- global temp table operation tests
drop table if exists gttTable;
CREATE GLOBAL TEMPORARY TABLE gttTable (col1 int primary key,
                                        col2 varchar2(200),
                                        col3 int,
                                        parent int,
                                        varchar_col VARCHAR2(255),
                                        nvarchar_col NVARCHAR2(255),
                                        char_col CHAR(10),
                                        nchar_col NCHAR(10),
                                        number_col NUMBER,
                                        integer_col INTEGER,
                                        float_col FLOAT,
                                        date_col DATE
                                       ) ON COMMIT delete ROWS;

begin;

explain (costs off)  insert into gttTable select * from baseTable;
insert into gttTable select * from baseTable;

explain (costs off) select count(*)  from gttTable;
select count(*)  from gttTable;

explain (costs off) select col3 from gttTable where col3 < 100;

explain (costs off) update gttTable set col2 = col2 || ' - 2' where col1 < 1001;
update gttTable set col2 = col2 || ' - 2' where col1 < 1001;
select col2 from gttTable  where col1 <= 5 ;

commit ;
select count(*)  from gttTable;
drop table gttTable;

-- temp table parallel operation tests
drop table if exists tempTable;
CREATE TEMPORARY TABLE tempTable (col1 int primary key,
                                  col2 varchar2(200),
                                  col3 int,
                                  parent int,
                                  varchar_col VARCHAR2(255),
                                  nvarchar_col NVARCHAR2(255),
                                  char_col CHAR(10),
                                  nchar_col NCHAR(10),
                                  number_col NUMBER,
                                  integer_col INTEGER,
                                  float_col FLOAT,
                                  date_col DATE
                                 )     ON COMMIT delete ROWS;

begin;

explain (costs off)  insert into tempTable select * from baseTable;
insert into tempTable select * from baseTable;

explain (costs off) select count(*)  from tempTable;
select count(*)  from tempTable;

explain (costs off) select col3 from tempTable where col3 < 100;

explain (costs off) update tempTable set col2 = col2 || ' - 2' where col1 < 1001;
update tempTable set col2 = col2 || ' - 2' where col1 < 1001;
select col2 from tempTable  where col1 <= 5 ;

commit ;
select count(*)  from tempTable;
drop table tempTable;

