-- 创建测试表
CREATE TABLE test_data (
    id SERIAL PRIMARY KEY,
    info TEXT
);

-- 插入测试数据
INSERT INTO test_data (info) VALUES
('Data 1'),
('Data 2'),
('Data 3'),
('Data 4'),
('Data 5'),
('Data 6'),
('Data 7'),
('Data 8'),
('Data 9'),
('Data 10');

SELECT set_config('slow_sql.threshold', '3000', true);

-- 1. 快速查询示例（应该不会触发慢查询日志）
SELECT count(*) FROM test_data;

-- 2. 故意制造慢查询（这里使用pg_sleep模拟长时间运行的查询）
-- pg_sleep函数使进程休眠指定的秒数，这里设置为6秒
SELECT pg_sleep(6);

-- 3. 另一个查询，用于确认慢查询阈值恢复后的行为
SELECT count(*) FROM test_data WHERE id > 5;

