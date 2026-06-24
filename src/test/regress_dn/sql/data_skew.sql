DROP TABLE IF EXISTS lineitem;

CREATE TABLE lineitem ( l_orderkey    bigint not null,
                        l_partkey     integer not null,
                        l_suppkey     integer not null,
                        l_linenumber  integer not null,
                        l_quantity    decimal(15,2) not null,
                        l_extendedprice  decimal(15,2) not null,
                        l_discount    decimal(15,2) not null,
                        l_tax         decimal(15,2) not null,
                        l_returnflag  char(1) not null,
                        l_linestatus  char(1) not null,
                        l_shipdate    date not null,
                        l_commitdate  date not null,
                        l_receiptdate date not null,
                        l_shipinstruct char(25) not null,
                        l_shipmode     char(10) not null,
                        l_comment      varchar(44) not null)
                        distribute by shard(l_orderkey);

set data_skew_option=3;

-- Test prefer distribute clause with const in current query block
EXPLAIN (COSTS OFF)
SELECT l1.l_partkey, l1.l_partkey
  FROM (SELECT MIN(l_suppkey) AS l_suppkey, 
               MIN(l_linenumber) AS l_linenumber,
               MIN(l_partkey) as l_partkey
         FROM  lineitem
         GROUP by l_quantity) as l1
  LEFT OUTER JOIN
       (SELECT MIN(l_suppkey) as l_suppkey, 
               MIN(l_linenumber) as l_linenumber,
               MIN(l_partkey) as l_partkey 
         FROM  lineitem 
         GROUP BY l_quantity) as l2
   ON  l1.l_partkey = l2.l_suppkey
   AND l1.l_suppkey = l2.l_linenumber
   AND l1.l_linenumber = l2.l_partkey
 WHERE l1.l_partkey = 10;

-- Test prefer distribute clause with const in subquery block
EXPLAIN (COSTS OFF)
SELECT l1.l_partkey, l1.l_partkey
  FROM (SELECT MIN(l_suppkey) AS l_suppkey, 
               MIN(l_linenumber) AS l_linenumber,
               l_partkey
         FROM  lineitem
         WHERE l_partkey = 10
         GROUP by l_partkey) as l1
  LEFT OUTER JOIN
       (SELECT MIN(l_suppkey) as l_suppkey, 
               MIN(l_linenumber) as l_linenumber,
               MIN(l_partkey) as l_partkey 
         FROM  lineitem 
         GROUP BY l_quantity) as l2
   ON  l1.l_partkey = l2.l_suppkey
   AND l1.l_suppkey = l2.l_linenumber
   AND l1.l_linenumber = l2.l_partkey;

-- Test roundrobin for only distribute clause with const
EXPLAIN (COSTS OFF)
SELECT l1.l_partkey, l1.l_partkey
  FROM (SELECT MIN(l_suppkey) AS l_suppkey, 
               MIN(l_linenumber) AS l_linenumber,
               MIN(l_partkey) as l_partkey
         FROM  lineitem
         GROUP by l_quantity) as l1
  LEFT OUTER JOIN
       (SELECT MIN(l_suppkey) as l_suppkey, 
               MIN(l_linenumber) as l_linenumber,
               MIN(l_partkey) as l_partkey 
         FROM  lineitem 
         GROUP BY l_quantity) as l2
   ON  l1.l_partkey = l2.l_suppkey
 WHERE l1.l_partkey = 10;

DROP TABLE lineitem;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (                                         
    c11 integer,                                           
    c12 integer,                                                 
    c13 integer,                                                  
    c14 integer,                                                     
    c15 integer                                                       
) distribute by shard(c11);

CREATE TABLE t2 (                                         
    c21 integer,                                           
    c22 integer,                                                 
    c23 integer,                                                  
    c24 integer,                                                     
    c25 integer                                                       
) distribute by shard(c21);

insert into t1              
    select  i,i,i,i,i
    from generate_series(2, 9) as i;

insert into t1              
    select  mod(i,2)+1,mod(i,2)+1,mod(i,9)+1,mod(i,2)+1,mod(i,2)+1
    from generate_series(1, 20000) as i;

insert into t2              
    select  i,i,i,i,i
    from generate_series(1, 7) as i;

insert into t2              
    select  mod(i,2)+8,mod(i,2)+8,mod(i,9)+1,mod(i,2)+8,mod(i,2)+8
    from generate_series(1, 20000) as i;

ANALYZE t1;
ANALYZE t2;

-- Test MCV with filter
/*+ SEQSCAN(1.1) SEQSCAN(1.2) 
    Parallel(1.1 0 hard) Parallel(1.2 0 hard) 
    HASHJOIN(1.1 1.2) 
    LEADING((1.1 1.2 ))
    Distribution(Left(1.1 ) Right( 1.2 )) */
EXPLAIN (COSTS OFF)
select * from t1,t2
where t1.c12 = t2.c22
  and t1.c12 < 4;

-- Test choosing flatting column as distribution key
/*+ SEQSCAN(1.1) SEQSCAN(1.2) 
    Parallel(1.1 0 hard) Parallel(1.2 0 hard) 
    HASHJOIN(1.1 1.2) 
    LEADING((1.1 1.2 ))
    Distribution(Left(1.1 ) Right( 1.2 )) */
EXPLAIN (COSTS OFF)
select * from t1,t2
where t1.c12 = t2.c22
  and t1.c13 = t2.c23;

DROP TABLE t1;
DROP TABLE t2;

set data_skew_option=0;
