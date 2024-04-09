## OpenTenBase Database Deplpoy
参考OpenTenBase部署文档，我们在192.168.56.104、192.168.56.106两台机器上部署了集群服务，系统版本为Centos 7，python版本为2.7。

服务部署参考项目部署文档，除了下述两点细节，其他不再细述。
1. 服务部署的用户名为opentenbase，用户目录为/home/opentenbase，后续的所有操作，均在opentenbase用户目录下操作。
2. OpenTenBase项目编译需要加 `--with-python` 和 `--with-libxml` 参数。

## Madlib编译并安装到OpenTenBase
由于madlib在PostgreSQL上的安装，依赖于PostgreSQL的版本，这里OpenTenBase中PostgreSQL的版本为10.0，因此，我们选择的madlib版本为v1.15
```
[opentenbase@localhost madlib]$ psql -V
psql (PostgreSQL) 10.0 OpenTenBase V2
```

### madlib v1.15源码编译
```
[opentenbase@localhost madlib]$ ./configure --install-prefix=/home/opentenbase/madlib/build -DPYXB_TAR_SOURCE=/home/opentenbase/madlib/3rd/PyXB-1.2.6.tar.gz -DEIGEN_TAR_SOURCE=/home/opentenbase/madlib/3rd/eigen-branches-3.2.tar.gz -DBOOST_TAR_SOURCE=/home/opentenbase/madlib/3rd/boost_1_61_0.tar.gz -DPOSTGRESQL_EXECUTABLE=$PGHOME/bin/ -DPOSTGRESQL_10_EXECUTABLE=$PGHOME/bin
make
make install
```
**Notice: 这里预先下载了madlib依赖的第三方库文件，直接通过指定第三方库源码路径安装，madlib编译完成后，安装在/usr/local/madlib目录。**

### madlib安装到OpenTenBase
```
[opentenbase@localhost ~]$ /usr/local/madlib/bin/madpack -p postgres -c opentenbase@192.168.56.106:30004/postgres install
```
这里会出现如下错误：
```
...
madpack.py: INFO : Installing MADlib:
madpack.py: ERROR : Failed executing /tmp/madlib.QmOzey/madlib_install.sql
madpack.py: ERROR : Check the log at /tmp/madlib.QmOzey/madlib_install.sql.log
madpack.py: INFO : MADlib install unsuccessful.
...
```

查看/tmp/madlib.QmOzey/madlib_install.sql.log错误日志：
```
...
psql:/tmp/madlib.QmOzey/madlib_install.sql:24: ERROR:  syntax error at or near "__DBMS_ARCHITECTURE__"
LINE 1: __DBMS_ARCHITECTURE__=
...
```
解决方法：
修改madlib项目cmake/Utils.cmake文件，这里相关参数为空，可以定义相关参数解决，这里我们直接删除如下代码，并重新编译madlib
```
...
"__DBMS_ARCHITECTURE__=${${DBMS_UC}_ARCHITECTURE}"
...
```

### 解决上述问题后，再次重新安装madlib到OpenTenBase
```
[opentenbase@localhost ~]$ /usr/local/madlib/bin/madpack -p postgres -c opentenbase@192.168.56.106:30004/postgres install
```
这里会出现新的错误，查看错误日志如下：
```
...
CREATE OR REPLACE FUNCTION madlib.array_add(x anyarray, y anyarray) RETURNS anyarray
AS '/usr/local/madlib/Versions/1.15/ports/postgres/10/lib/libmadlib.so', 'array_add'
LANGUAGE C IMMUTABLE
;
psql:/tmp/madlib.smKoda/madlib_install.sql:23: ERROR:  node:cn002, backend_pid:10072, nodename:cn002,backend_pid:10072,message:could not load library "/usr/local/madlib/Versions/1.15/ports/postgres/10/lib/libmadlib.so": /usr/local/madlib/Versions/1.15/ports/postgres/10/lib/libmadlib.so: undefined symbol: heap_form_tuple
...
```
这个问题的原因是由于madlib没有在cn002节点所在机器上安装，这里我们将madlib安装到所有机器上，解决该问题。

### 解决上述问题后，再次重新安装madlib到OpenTenBase
```
[opentenbase@localhost ~]$ /usr/local/madlib/bin/madpack -p postgres -c opentenbase@192.168.56.106:30004/postgres install
```
这里会出现新的错误，查看错误日志如下：
```
...
psql:/tmp/madlib.kZ7b9K/madlib_install.sql:13936: ERROR:  syntax error at or near "DOUBLE"
LINE 2:     number DOUBLE PRECISION
                   ^
...
```
排查后发现，上述SQL语句实在madlib中`src/ports/postgres/modules/utilities/utilities.sql_in`文件中定义的：
```
CREATE OR REPLACE FUNCTION MADLIB_SCHEMA.isnan(
    number DOUBLE PRECISION
) RETURNS BOOLEAN
AS $$
    SELECT $1 = 'NaN'::DOUBLE PRECISION;
$$
LANGUAGE sql
```
这个问题的原因，是由于出现了语法错误，怀疑可能number字段与某些关键字冲突，我们在utilities.sql_in文件做如下修改，解决该问题：
```
CREATE OR REPLACE FUNCTION MADLIB_SCHEMA.isnan(
    num DOUBLE PRECISION
) RETURNS BOOLEAN
AS $$
    SELECT $1 = 'NaN'::DOUBLE PRECISION;
$$
LANGUAGE sql
```

### 解决上述问题后，再次重新安装madlib到OpenTenBase
```
[opentenbase@localhost ~]$ /usr/local/madlib/bin/madpack -p postgres -c opentenbase@192.168.56.106:30004/postgres install
```
这里可以看到madlib安装到OpenTenBase成功的信息：
```
...
madpack.py: INFO : Installing MADlib:
madpack.py: INFO : > Created madlib schema
madpack.py: INFO : > Created madlib.MigrationHistory table
madpack.py: INFO : > Wrote version info in MigrationHistory table
madpack.py: INFO : MADlib 1.15 installed successfully in madlib schema.
...
```

## Madlib安装到OpenTenBase用例测试
### 单元测试正常
```
[opentenbase@localhost ~]$ /usr/local/madlib/bin/madpack -p postgres -c opentenbase@192.168.56.106:30004/postgres unit-test
madpack.py: INFO : Detected PostgreSQL version 10.0.
TEST CASE RESULT|Module: utilities|test_minibatch_preprocessing.py|PASS|Time: 155 milliseconds
TEST CASE RESULT|Module: utilities|test_transform_vec_cols.py|PASS|Time: 137 milliseconds
TEST CASE RESULT|Module: utilities|test_validate_args.py|PASS|Time: 59 milliseconds
TEST CASE RESULT|Module: utilities|test_utilities.py|PASS|Time: 142 milliseconds
TEST CASE RESULT|Module: convex|test_mlp_igd.py|PASS|Time: 171 milliseconds
TEST CASE RESULT|Module: recursive_partitioning|test_random_forest.py|PASS|Time: 101 milliseconds
```

### install-check执行用例检查
执行如下操作，检查安装结果，观察所有用例是否正常
```
[opentenbase@localhost ~]$ /usr/local/madlib/bin/madpack -p postgres -c opentenbase@192.168.56.106:30004/postgres install-check
madpack.py: INFO : Detected PostgreSQL version 10.0.
TEST CASE RESULT|Module: bayes|bayes.ic.sql_in|FAIL|Time: 171 milliseconds
madpack.py: ERROR : Failed executing /tmp/madlib.FWPlbQ/bayes/bayes.ic.sql_in.tmp
madpack.py: ERROR : Check the log at /tmp/madlib.FWPlbQ/bayes/bayes.ic.sql_in.log
TEST CASE RESULT|Module: crf|crf_test_small.ic.sql_in|FAIL|Time: 747 milliseconds
madpack.py: ERROR : Failed executing /tmp/madlib.FWPlbQ/crf/crf_test_small.ic.sql_in.tmp
madpack.py: ERROR : Check the log at /tmp/madlib.FWPlbQ/crf/crf_test_small.ic.sql_in.log
...
```

这里问题会比较多，我们逐一解决，先看第一个错误日志
```
[opentenbase@localhost ~]$ tail /tmp/madlib.FWPlbQ/bayes/bayes.ic.sql_in.log
$$ language plpgsql;
CREATE FUNCTION
---------------------------------------------------------------------------
-- Test:
---------------------------------------------------------------------------
SELECT install_test_1();
psql:/tmp/madlib.FWPlbQ/bayes/bayes.ic.sql_in.tmp:111: ERROR:  DML has a subquery contains a function runs on CN
HINT:  You might need to push that function down to DN.
CONTEXT:  SQL statement "INSERT INTO data_1 SELECT 1, ARRAY[fill_feature(id,0.3,num1),fill_feature(id,0.8,num1)] FROM generate_series(1,num1) as id"
PL/pgSQL function install_test_1() line 14 at SQL statement
```

这个问题的原因是由于测试的DML中包含了subquery，这里我们修改OpenTenBase代码让OpenTenBase临时支持该能力。
vim src/backend/optimizer/path/allpaths.c，注释如下代码
```
/*
 if (subquery->hasCoordFuncs &&
     (parse->commandType == CMD_UPDATE ||
      parse->commandType == CMD_INSERT ||
      parse->commandType == CMD_DELETE)
     )
 {
         ereport(ERROR,
                 (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("DML has a subquery contains a function runs on CN"),
                         errhint("You might need to push that function down to DN.")));
 }
 */
```
**Notice: OpenTenBase这里暂时屏蔽了该功能，这里只是临时开启该功能，用于madlib的安装check**

### 修改上述代码后，重新编译部署OpenTenBase，再次执行install-check
```
[opentenbase@localhost ~]$ /usr/local/madlib/bin/madpack -p postgres -c opentenbase@192.168.56.106:30004/postgres install-check
madpack.py: INFO : Detected PostgreSQL version 10.0.
TEST CASE RESULT|Module: bayes|bayes.ic.sql_in|FAIL|Time: 485 milliseconds
madpack.py: ERROR : Failed executing /tmp/madlib.sLECgl/bayes/bayes.ic.sql_in.tmp
madpack.py: ERROR : Check the log at /tmp/madlib.sLECgl/bayes/bayes.ic.sql_in.log
TEST CASE RESULT|Module: crf|crf_test_small.ic.sql_in|FAIL|Time: 737 milliseconds
madpack.py: ERROR : Failed executing /tmp/madlib.sLECgl/crf/crf_test_small.ic.sql_in.tmp
madpack.py: ERROR : Check the log at /tmp/madlib.sLECgl/crf/crf_test_small.ic.sql_in.log
```

查看错误日志如下
```
[opentenbase@localhost ~]$ tail /tmp/madlib.sLECgl/crf/crf_test_small.ic.sql_in.log
        (4,3,'.',4);
COPY 61
        -- extract features for tokens stored in segmenttbl
        SELECT crf_test_fgen('test_segmenttbl','crf_dictionary','test_crf_label','crf_regex','crf_feature_test','viterbi_mtbl','viterbi_rtbl');
psql:/tmp/madlib.sLECgl/crf/crf_test_small.ic.sql_in.tmp:136: ERROR:  spiexceptions.InternalError: Cannot support distribute type: RoundRobin
CONTEXT:  Traceback (most recent call last):
  PL/Python function "crf_test_fgen", line 19, in <module>
    return crf_feature_gen.generate_test_features(**globals())
  PL/Python function "crf_test_fgen", line 223, in generate_test_features
PL/Python function "crf_test_fgen"
```

该问题的原因是由于没有开启OpenTenBase所有distribute table支持，通过在编译时加上如下参数开启：
```
--enable-alltype-distri
```

### 解决上述问题后，再次执行install-check
```
[opentenbase@localhost ~]$ tail /tmp/madlib.eGIK_1/crf/crf_train_small.ic.sql_in.log
        (19,2,'freefall',11,26),   (20,2,'in',5,26),        (21,2,'sterling',11,26),  (22,2,'over',5,26),
        (23,2,'the',2,26),         (24,2,'past',6,26),      (25,2,'week',11,26),      (26,2,'.',43,26);
COPY 64
        SELECT crf_train_fgen('train_segmenttbl', 'train_regex', 'crf_label', 'train_dictionary', 'train_featuretbl','train_featureset');
psql:/tmp/madlib.eGIK_1/crf/crf_train_small.ic.sql_in.tmp:158: ERROR:  spiexceptions.DuplicateTable: node:dn002, backend_pid:2447, nodename:dn001,backend_pid:2645,message:relation "_madlib_temp_table" already exists
```
该问题主要原因怀疑是madlib插件的某些drop表语句没有生效，这里我们找到madlib中相应的sql语句，临时在create table前强制drop表。

修改madlib中`src/ports/postgres/modules/utilities/utilities.sql_in`文件如下代码：
```
CREATE OR REPLACE FUNCTION MADLIB_SCHEMA.create_schema_pg_temp()
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
AS $$
BEGIN
    -- pg_my_temp_schema() is a built-in function
    IF pg_my_temp_schema() = 0 THEN
        -- The pg_temp schema does not exist, yet. Creating a temporary table
        -- will create it. Note: There is *no* race condition here, because
        -- every session has its own temp schema.

        -- fix issue： drop table _madlib_temp_table
        EXECUTE 'DROP TABLE IF EXISTS pg_temp._madlib_temp_table CASCADE; CREATE TEMPORARY TABLE _madlib_temp_table AS SELECT 1;
            DROP TABLE pg_temp._madlib_temp_table CASCADE;';
    END IF;
END;
```

### 解决上述问题后，重新isntall madlib到OpenTenBase，并且再次执行install-check
```
[opentenbase@localhost ~]$ /usr/local/madlib/bin/madpack -p postgres -c opentenbase@192.168.56.106:30004/postgres install-check
madpack.py: INFO : Detected PostgreSQL version 10.0.
TEST CASE RESULT|Module: bayes|bayes.ic.sql_in|FAIL|Time: 293 milliseconds
madpack.py: ERROR : Failed executing /tmp/madlib.H2qMQw/bayes/bayes.ic.sql_in.tmp
madpack.py: ERROR : Check the log at /tmp/madlib.H2qMQw/bayes/bayes.ic.sql_in.log
TEST CASE RESULT|Module: crf|crf_test_small.ic.sql_in|PASS|Time: 1050 milliseconds
TEST CASE RESULT|Module: crf|crf_train_small.ic.sql_in|FAIL|Time: 497 milliseconds
madpack.py: ERROR : Failed executing /tmp/madlib.H2qMQw/crf/crf_train_small.ic.sql_in.tmp
madpack.py: ERROR : Check the log at /tmp/madlib.H2qMQw/crf/crf_train_small.ic.sql_in.log
TEST CASE RESULT|Module: elastic_net|elastic_net.ic.sql_in|FAIL|Time: 293 milliseconds
madpack.py: ERROR : Failed executing /tmp/madlib.H2qMQw/elastic_net/elastic_net.ic.sql_in.tmp
madpack.py: ERROR : Check the log at /tmp/madlib.H2qMQw/elastic_net/elastic_net.ic.sql_in.log
SQL command failed:
SQL: DROP SCHEMA IF EXISTS madlib_installcheck_elastic_net CASCADE;
psql: FATAL:  the database system is in recovery mode
: ERROR : False
SQL command failed
```

查看错误日志，发现主要的问题如下：

a. table "_madlib_nb_attr_values" does not exist
```
[opentenbase@localhost ~]$ tail /tmp/madlib.H2qMQw/bayes/bayes.ic.sql_in.log
...
SELECT install_test_1();
psql:/tmp/madlib.H2qMQw/bayes/bayes.ic.sql_in.tmp:111: ERROR:  spiexceptions.UndefinedTable: node:dn001, backend_pid:15530, nodename:dn002,backend_pid:3899,message:table "_madlib_nb_attr_values" does not exist
...
```
b. relation "_madlib_tmp_segcount_tbl" already exists
```
...
[opentenbase@localhost ~]$ tail /tmp/madlib.H2qMQw/crf/crf_train_small.ic.sql_in.log
psql:/tmp/madlib.H2qMQw/crf/crf_train_small.ic.sql_in.tmp:158: ERROR:  spiexceptions.DuplicateTable: node:dn002, backend_pid:3897, nodename:dn001,backend_pid:15528,message:relation "_madlib_tmp_segcount_tbl" already exists
...
```

### TODO
由于时间有限，因此，这里暂未将所有用例跑通，并且部分用例测试问题的解决，也是用的临时方案，正式方案还需要考虑的更加全面。

前述两类问题，怀疑原因跟相关表drop或create语句在OpenTenBase上执行失败有关，导致预期创建的表，如_madlib_nb_attr_values表，没有被创建，或者预期删除的表，如_madlib_tmp_segcount_tbl表，没有被删除

由于postgresql v10.0上安装madlib完全正常，因此排查的方向，主要是OpenTenBase上执行madlib中相关drop或create表是否有异常，由于OpenTenBase日志中没有更多异常信息（已设置日志级别为DEBUG5），因此可以考虑添加相关详细日志，逐步排查。


## 其他问题
### 在对OpenTenBase进行madlib的install操作时，有遇到如下问题，后面再次install时，这个问题未再出现，这里做下记录，用于参考
`undefined reference to heap_form_tuple`

这个问题的原因，是由于OpenTenBase中修改了heap_form_tuple相关代码，将heap_form_tuple定义为了宏
```
#define heap_form_tuple(tupleDescriptor, values, isnull) \
    heap_form_tuple_shard(tupleDescriptor, values, isnull, SetFlag_NoShard, InvalidAttrNumber, InvalidAttrNumber, InvalidOid, InvalidShardID)
```

这个问题，通过将宏定义修改为函数定义的方式解决了，后续再次操作用于记录文档时，未再出现，比较奇怪。

a. src/include/access/htup_details.h中添加函数声明：
```
extern HeapTuple heap_form_tuple(TupleDesc tupleDescriptor, Datum *values, bool *isnull);
```
b. src/backend/access/common/heaptuple.c中添加函数定义：
```
#ifdef _SHARDING_

HeapTuple heap_form_tuple(TupleDesc tupleDescriptor, Datum *values, bool *isnull)
{
    return heap_form_tuple_shard(tupleDescriptor, values, isnull, SetFlag_NoShard, InvalidAttrNumber, InvalidAttrNumber, InvalidOid, InvalidShardID);
}

#endif
```
