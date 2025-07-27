#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>

void exit_nicely(PGconn *conn) {
    PQfinish(conn);
    exit(1);
}

void print_result(PGresult *res) {
    int rows = PQntuples(res);
    printf("ID\tname\tnum\n");
    printf("------------------------\n");
    
    for (int i = 0; i < rows; i++) {
        printf("%s\t%s\t%s\n", 
               PQgetvalue(res, i, 0), 
               PQgetvalue(res, i, 1),
               PQgetvalue(res, i, 2)); 
    }
}

void set_isolation_level(PGconn *conn, const char *level) {
    char query[100];
    PGresult *res;
    
    // 构建设置隔离级别的SQL命令
    sprintf(query, "SET TRANSACTION ISOLATION LEVEL %s", level);
    
    // 执行命令
    res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "设置隔离级别失败: %s", PQerrorMessage(conn));
        PQclear(res);
        return;
    }
    PQclear(res);
    printf("已设置事务隔离级别为: %s\n", level);

    res = PQexec(conn, "SHOW transaction_isolation");
    if (PQresultStatus(res) == PGRES_TUPLES_OK) {
    printf("现在实际的隔离级别: %s\n", PQgetvalue(res, 0, 0));
    }
    PQclear(res);
    
}

void test_dirty_read(PGconn *conn1, PGconn *conn2) {
    printf("\n测试脏读\n");
    
    // 连接1：更新数据但不提交
    PGresult *res = PQexec(conn1, "BEGIN");
    PQclear(res);
    set_isolation_level(conn1, "READ UNCOMMITTED");

    res = PQexec(conn1, "UPDATE test_table SET num = 0 WHERE id = 2");
    PQclear(res);
    printf("连接1更新了id=2的num为0，但未提交\n");

    // 连接2：尝试读取未提交数据
    res = PQexec(conn2, "BEGIN");
    PQclear(res);
    set_isolation_level(conn2, "READ UNCOMMITTED");
    
    res = PQexec(conn2, "SELECT * FROM test_table WHERE id = 2");
    printf("连接2读取结果:\n");
    print_result(res);
    PQclear(res);
    
    // 连接1回滚
    res = PQexec(conn1, "ROLLBACK");
    PQclear(res);
    
    // 连接2再次读取
    res = PQexec(conn2, "SELECT * FROM test_table WHERE id = 2");
    printf("连接1回滚后，连接2读取结果:\n");
    print_result(res);
    PQclear(res);
    
    res = PQexec(conn2, "COMMIT");
    PQclear(res);
}

void test_nonrepeatable_read(PGconn *conn1, PGconn *conn2) {
    printf("\n测试不可重复读\n");

    // 连接1开始并读取数据，连接2开始，修改数据，提交，连接1再次读取
    PGresult *res = PQexec(conn1, "BEGIN");
    PQclear(res);
    set_isolation_level(conn1, "READ COMMITTED");

    res = PQexec(conn1, "SELECT * FROM test_table ORDER BY id");
    printf("连接1读取结果:\n");
    print_result(res);
    PQclear(res);

    // 连接2：修改数据
    res = PQexec(conn2, "BEGIN");
    PQclear(res);
    set_isolation_level(conn2, "READ COMMITTED");
    
    res = PQexec(conn1, "UPDATE test_table SET num = 0 WHERE id = 2");
    PQclear(res);
    //连接2：提交
    res = PQexec(conn2, "COMMIT");
    PQclear(res);

    // 连接1再次读取
    res = PQexec(conn1, "SELECT * FROM test_table ORDER BY id");
    printf("连接1再次读取结果:\n");
    print_result(res);
    PQclear(res);

    res = PQexec(conn1, "COMMIT");
    PQclear(res);
}

void test_phantom_read(PGconn *conn1, PGconn *conn2) {
    printf("\n测试幻读\n");

    // 连接1开始
    PGresult *res = PQexec(conn1, "BEGIN");
    PQclear(res);
    set_isolation_level(conn1, "REPEATABLE READ");
    
    res = PQexec(conn1, "SELECT * FROM test_table WHERE id > 8 ORDER BY id");
    printf("连接1读取结果:\n");
    print_result(res);
    PQclear(res);

    // 连接2：插入数据
    res = PQexec(conn2, "BEGIN");
    PQclear(res);
    set_isolation_level(conn2, "REPEATABLE READ");

    res = PQexec(conn2, "INSERT INTO test_table VALUES (11, 'K', 200)");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "插入数据失败: %s", PQerrorMessage(conn2));
        PQclear(res);
        exit_nicely(conn2);
    }
    PQclear(res);
    //连接2：提交
    res = PQexec(conn2, "COMMIT");
    PQclear(res);

    // 连接1再次读取
    res = PQexec(conn1, "SELECT * FROM test_table WHERE id > 8 ORDER BY id");
    printf("连接1再次读取结果:\n");
    print_result(res);
    PQclear(res);

    res = PQexec(conn1, "COMMIT");
    PQclear(res);
}

int main() {
    const char *conninfo;
    PGconn *conn;
    PGresult *res;
    int rows;
    
    // 设置连接信息
    conninfo = "host=192.168.10.142 port=30004 dbname=postgres user=opentenbase";
    
    // 连接到数据库
    conn = PQconnectdb(conninfo);
    
    // 检查连接状态
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "连接到数据库失败: %s", 
                PQerrorMessage(conn));
        exit_nicely(conn);
    }
    
    printf("成功连接到OpenTenBase数据库！\n");
    
    /*
        注意在数据库中已经使用README.md中提供的SQL脚本创建了节点组default_group且将数据节点dn001和dn002加入此组，
        并为default_group节点组创建了分片组，当前数据库中仅包含名为foo的分布式表。
    */

    printf("\n开始事务操作测试...\n");

    // 创建测试表
    res = PQexec(conn, "DROP TABLE IF EXISTS test_table");
    PQclear(res);
    
    res = PQexec(conn, "CREATE TABLE test_table(id bigint, name text, num int) DISTRIBUTE BY SHARD(id)");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "创建表失败: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    printf("成功创建表test_table\n");
    PQclear(res);
    
    // 插入数据
    // res = PQexec(conn, "INSERT INTO test_table VALUES (1, 'A', 100)");

    //实现多条数据插入
    res = PQexec(conn, 
    "INSERT INTO test_table VALUES "
    "(1, 'A', 100),"
    "(2, 'B', 110),"
    "(3, 'C', 120),"
    "(4, 'D', 130),"
    "(5, 'E', 140),"
    "(6, 'F', 150),"
    "(7, 'G', 160),"
    "(8, 'H', 170),"
    "(9, 'I', 180),"
    "(10, 'J', 190)");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "插入数据失败: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);
    
    printf("成功插入测试数据\n");
    
    // 查询该表所有数据
    res = PQexec(conn, "SELECT * FROM test_table ORDER BY id");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "查询数据失败: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    printf("\n查询结果：(插入十条数据)\n");
    print_result(res); 
    PQclear(res);

    //查询该表分布在各个节点上的数据 (还需要更普适)
    res = PQexec(conn, "EXECUTE DIRECT ON (dn001) 'SELECT * FROM test_table'");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "查询数据失败: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    printf("\n查询结果：（分布在dn001上的数据）\n");
    print_result(res); 
    PQclear(res);

    res = PQexec(conn, "EXECUTE DIRECT ON (dn002) 'SELECT * FROM test_table'");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "查询数据失败: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    printf("\n查询结果：（分布在dn002上的数据）\n");
    print_result(res); 
    PQclear(res);

    // 更新数据
    res = PQexec(conn, "UPDATE test_table SET num = 129 WHERE num < 130");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "更新数据失败: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    printf("\n成功更新num小于150的记录为150\n");
    PQclear(res);
    
    // 再次查询数据
    res = PQexec(conn, "SELECT * FROM test_table ORDER BY id");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "查询数据失败: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    printf("\n查询结果：(更新num<130的数据为num=129)\n");
    print_result(res); 
    PQclear(res);
    
    // 删除数据
    res = PQexec(conn, "DELETE FROM test_table WHERE num < 130");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "删除数据失败: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    printf("\n成功删除num<130的数据\n");
    PQclear(res);
    
    // 检查结果
    res = PQexec(conn, "SELECT * FROM test_table ORDER BY id");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "查询数据失败: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    printf("\n查询结果：(删除num<130的数据)\n");
    print_result(res); 
    PQclear(res);
    
    /*
    接下来开始事务控制的测试
    */

    printf("\n开始事务控制测试...\n");

    res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "BEGIN失败: %s\n", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);
    printf("事务开始\n");

    // 在事务中插入数据
    res = PQexec(conn, "INSERT INTO test_table VALUES (1, 'A', 100)");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "事务中插入数据失败: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQexec(conn, "ROLLBACK");
        exit_nicely(conn);
    }
    PQclear(res);
    printf("事务中插入数据成功\n");

    // 在事务中更新数据（模拟转账行为）
    res = PQexec(conn, "UPDATE test_table SET num = num - 200 WHERE id = 4");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "更新id=4的num失败: %s\n", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);

    // 获得更新后的数据
    res = PQexec(conn, "SELECT num FROM test_table WHERE id = 4");
    int num_1 = atoi(PQgetvalue(res, 0, 0));
    PQclear(res);
    
    if (num_1 < 0) {
        printf("num小于0，事务回滚\n");
        res = PQexec(conn, "ROLLBACK");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "ROLLBACK失败: %s\n", PQerrorMessage(conn));
            PQclear(res);
            exit_nicely(conn);
        }
        PQclear(res);
        res = PQexec(conn, "BEGIN");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "重新开始事务失败: %s\n", PQerrorMessage(conn));
            PQclear(res);
            exit_nicely(conn);
        }
        PQclear(res);
    } else {
        // 若满足条件，则继续更新其他数据
        res = PQexec(conn, "UPDATE test_table SET num = num + 200 WHERE id = 6");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "更新id=6的num失败: %s\n", PQerrorMessage(conn));
            PQclear(res);
            exit_nicely(conn);
        }
        PQclear(res);
    }

     // 提交事务
    res = PQexec(conn, "COMMIT");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "COMMIT失败: %s\n", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);
    printf("事务已提交\n");


    // 查询当前数据
    res = PQexec(conn, "SELECT * FROM test_table ORDER BY id");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "查询数据失败: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    printf("\n查询结果：(因为rollback了所以应该没有任何改变)\n");
    print_result(res); 
    PQclear(res);

    printf("\n测试保存点功能...\n");

    res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "BEGIN失败: %s\n", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);

    //插入数据
    res = PQexec(conn, "INSERT INTO test_table VALUES (2, 'B', 200)");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "插入数据失败: %s\n", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);
    printf("已插入数据(2, 'B', 200)\n");

    // 创建保存点
    res = PQexec(conn, "SAVEPOINT my_savepoint");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "创建保存点失败: %s\n", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);   
    printf("已创建保存点my_savepoint\n");

    // 插入数据
    res = PQexec(conn, "INSERT INTO test_table VALUES (3, 'C', 300)");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "插入数据失败: %s\n", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);
    printf("已插入数据(3, 'C', 300)\n");

    // 查询数据
    res = PQexec(conn, "SELECT * FROM test_table ORDER BY id");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "查询数据失败: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    // 打印更新后的查询结果
    printf("\n查询结果：(插入了两条数据：(2, 'B', 200)和(3, 'C', 300))\n");
    print_result(res); 
    PQclear(res);

    // 回滚到保存点
    printf("\n开始回滚到保存点my_savepoint...\n");
    res = PQexec(conn, "ROLLBACK TO SAVEPOINT my_savepoint");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "回滚到保存点失败: %s\n", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);
    printf("已回滚到保存点my_savepoint\n");

    // 再次查询数据
    res = PQexec(conn, "SELECT * FROM test_table ORDER BY id");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "查询数据失败: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    printf("\n查询结果：(插入的数据只有(2, 'B', 200))\n");
    print_result(res); 
    PQclear(res);

    res = PQexec(conn, "COMMIT");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "COMMIT失败: %s\n", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);
    printf("事务已提交\n");

    // 关闭连接
    PQfinish(conn);
    printf("\n成功断开与OpenTenBase的连接0\n");
    
    /*
    开始事务隔离级别测试：分为三种并发问题的测试
    1、脏读:两个事务同时读取同一数据，且一个事务修改了数据，另一个事务读取到未提交的数据。
    2、不可重复读:一个事务在读取数据时，另一个事务对该数据进行了修改，导致前者读取到不同的值。
    3、幻读:一个事务在读取数据时，另一个事务插入了新数据，导致前者读取到的结果集发生变化。
    */

    // 创建两个连接模拟并发事务
    PGconn *conn1 = PQconnectdb(conninfo);
    PGconn *conn2 = PQconnectdb(conninfo);
    
    if (PQstatus(conn1) != CONNECTION_OK || PQstatus(conn2) != CONNECTION_OK) {
        fprintf(stderr, "连接失败: %s / %s", 
                PQerrorMessage(conn1), PQerrorMessage(conn2));
        exit_nicely(conn1);
    }
    
    // 测试不同隔离级别
    test_dirty_read(conn1, conn2);
    test_nonrepeatable_read(conn1, conn2);
    test_phantom_read(conn1, conn2);
    
    PQfinish(conn1);
    PQfinish(conn2);
    printf("\n成功断开与OpenTenBase的连接1\n");
    printf("\n成功断开与OpenTenBase的连接2\n");
    return 0;
}