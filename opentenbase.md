# Design a test case for distributed transactions. #13
目标：设计一个分布式事务的测试用例，包括常见的事物操作和事物控制。

## 准备工作
### 集群准备
按照Opentenbase开源项目中的README.md完成配置，本地集群共四个节点：一个GTM节点，一个Coordinator节点，两个DataNode节点（注意都不进行备份）
四个节点均使用Ubuntu 20.04，按照要求创建用户opentenbase，并实现虚拟机之间的互联
### 数据库准备
按照README.md文件中的sql指令创建分片组：
    postgres=# create default node group default_group with (dn001,dn002);
    postgres=# create sharding group to group default_group;

进行测试：
    postgres=# create table foo(id bigint, str text) distribute by shard(id);
    postgres=# insert into foo values(1, 'hello OpenTenBase');
    postgres=# select * from foo;

结果无误。

## 测试设计
### 基本的事务操作测试
1、**跨节点INSERT**：在多个节点上插入数据
    创建测试表 test_table，该表的设计为：(id bigint, name text, num int)按照id进行hash存储
    
    插入的数据为：(1, 'A', 100),(2, 'B', 110),(3, 'C', 120),(4, 'D', 130),(5, 'E', 140),(6, 'F', 150),(7, 'G', 160),(8, 'H', 170),(9, 'I', 180),(10, 'J', 190)
    
    插入完成后利用进行`跨节点SELECT`,检查插入的十条数据，print_result输出查询结果。
    此时预期结果应为：
    ID      name    num
    ------------------------
    1       A       100
    2       B       110
    3       C       120
    4       D       130
    5       E       140
    6       F       150
    7       G       160
    8       H       170
    9       I       180
    10      J       190

    再分别对两个节点上存储的该表的数据进行检查，输出查询结果。
    此时对于dn001预期的结果应为：
    ID      name    num
    ------------------------
    1       A       100
    2       B       110
    5       E       140
    6       F       150
    8       H       170
    9       I       180

    对于dn002预期的结果应为：
    ID      name    num
    ------------------------
    3       C       120
    4       D       130
    7       G       160
    10      J       190

2、**跨节点UPDATE**：更新分布在多个节点上的数据
    将num<130的记录更新为num=129

    更新完成后进行`跨节点SELECT`,检查更新后的表的内容，print_result输出查询结果。
    此时预期的结果应为数据(1, 'A', 100),(2, 'B', 110),(3, 'C', 120)变为(1, 'A', 129),(2, 'B', 129),(3, 'C', 129)：
    ID      name    num
    ------------------------
    1       A       129
    2       B       129
    3       C       129
    4       D       130
    5       E       140
    6       F       150
    7       G       160
    8       H       170
    9       I       180
    10      J       190

3、**跨节点DELETE**：删除涉及多个节点的数据
    将num<130的数据删除

    删除完成后利用进行`跨节点SELECT`,检查删除记录后的表的内容，输print_result出查询结果。
    此时预期的结果应为只包含num>=130的记录：
    ID      name    num
    ------------------------
    4       D       130
    5       E       140
    6       F       150
    7       G       160
    8       H       170
    9       I       180
    10      J       190

4、**跨节点SELECT**：查询涉及多个节点的关联数据
    通过EXECUTE DIRECT ON (节点)实现单个节点存储数据的查询，通过SELECT * FROM test_table ORDER BY id实现该表所有内容的查询

### 基本的事务控制测试
    从BEGIN开始事务->在事务中插入数据(1, 'A', 100)->更新id=4的num，对应的记录应变为(4, 'D', -70)->不符合条件，ROLLBACK
    此时预期的结果应为没有改变：
    ID      name    num
    ------------------------
    4       D       130
    5       E       140
    6       F       150
    7       G       160
    8       H       170
    9       I       180
    10      J       190

    接下来测试保存点的功能：
    BEGIN开始事务->在事务中插入数据(2, 'B', 200)->创建保存点my_savepoint->插入数据(3, 'C', 300)->查询表中内容
    此时预期的结果应为插入了(2, 'B', 200)，(3, 'C', 300)两条数据：
    ID      name    num
    ------------------------
    2       B       200
    3       C       300
    4       D       130
    5       E       140
    6       F       150
    7       G       160
    8       H       170
    9       I       180
    10      J       190

    回滚到保存点my_savepoint->查询表中内容->提交
    此时预期的结果应为插入的数据只有(2, 'B', 200)一条数据，(3, 'C', 300)不存在：
    ID      name    num
    ------------------------
    2       B       200
    4       D       130
    5       E       140
    6       F       150
    7       G       160
    8       H       170
    9       I       180
    10      J       190
### 事务隔离级别控制测试
**创建两个连接用于两个事务**
1、脏读(test_dirty_read)：注意PostgreSQL 确实允许设置 READ UNCOMMITTED 隔离级别，但实际上它的行为与 READ COMMITTED 完全相同，所以应该不存在脏读的问题。
    事务1 BEGIN->设置隔离级别为：READ UNCOMMITTED->更新id=2的num为0，但未提交->事务2 BEGIN-> 设置事务2隔离级别为：READ UNCOMMITTED->事务2 读取id=2的信息
    此时预期的结果应为未改变的值，即id=2的num为200：
    ID      name    num
    ------------------------
    2       B       200

    ->事务1 ROLLBACK->事务2 读取id=2的信息
    此时预期的结果应为未改变的值，即id=2的num为200：
    ID      name    num
    ------------------------
    2       B       200
2、不可重复读(test_nonrepeatable_read)：
    事务1 BEGIN-> 设置隔离级别为：READ COMMITTED->事务1 读取test_table的所有数据
    此时预期结果应为未改变的值：
    ID      name    num
    ------------------------
    2       B       200
    4       D       130
    5       E       140
    6       F       150
    7       G       160
    8       H       170
    9       I       180
    10      J       190

    ->事务2 BEGIN -> 设置隔离级别为：READ COMMITTED->事务2 修改数据(2, 'B', 200)为(2, 'B', 0)->事务2 COMMITED->事务1 读取test_table的所有数据
    此时预期结果应为原来的(2, 'B', 200)变为(2, 'B', 0)：
    ID      name    num
    ------------------------
    2       B       0
    4       D       130
    5       E       140
    6       F       150
    7       G       160
    8       H       170
    9       I       180
    10      J       190

3、幻读(test_phantom_read):
    [事务1 BEGIN]->[设置隔离级别为：REPEATABLE READ]->[事务1 读取id > 8的数据]
    此时预期结果应为：
    ID      name    num
    ------------------------
    9       I       180
    10      J       190

    ->[事务2 BEGIN]->[设置隔离级别为：REPEATABLE READ]->[事务2 插入数据(11, 'K', 200)]->[事务2 COMMIT]->[事务1 再次读取id > 8的数据]
    此时虽然添加数据，且隔离级别为REPEATABLE READ，但PGSQL这个级别也可以防止幻读，所以预期结果应该和之前读到的结果一样，应为：
    ID      name    num
    ------------------------
    9       I       180
    10      J       190
### end