-- 创建测试表
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    age INT,
    email TEXT
);

-- 插入一些数据
INSERT INTO test_table (first_name, last_name, age, email)
VALUES 
    ('John', 'Doe', 30, 'john.doe@example.com'),
    ('Alice', 'Smith', 25, 'alice.smith@example.com'),
    ('Bob', 'Johnson', 35, 'bob.johnson@example.com');

-- 设置预期时间为 1 毫秒
SELECT set_expected_time(1000);

-- 执行一个查询，预期不超时
SELECT * FROM test_table;

-- 插入大量数据
INSERT INTO test_table (first_name, last_name, age, email)
SELECT 
    'FirstName' || generate_series(1, 100),
    'LastName' || generate_series(1, 100),
    (random() * 100)::INT,
    'email' || generate_series(1, 100) || '@example.com';

-- 执行一个查询，预期超时
SELECT * FROM test_table;

LOG:  Query (ID: 21352) exceeded expected time: 1000 microseconds (actual: 3000 microseconds)





