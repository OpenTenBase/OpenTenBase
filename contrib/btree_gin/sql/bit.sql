-- 关闭顺序扫描
set enable_seqscan=off;

-- 创建测试表
CREATE TABLE test_bit (
    i bit(3)
);

-- 插入数据
INSERT INTO test_bit VALUES ('001'),('010'),('011'),('100'),('101'),('110');

-- 创建位图索引
CREATE INDEX idx_bit ON test_bit USING gin (i);

-- 查询小于 '100' 的数据，并按照 i 字段排序
SELECT * FROM test_bit WHERE i<'100'::bit(3) ORDER BY i;

-- 查询小于等于 '100' 的数据，并按照 i 字段排序
SELECT * FROM test_bit WHERE i<='100'::bit(3) ORDER BY i;

-- 查询等于 '100' 的数据，并按照 i 字段排序
SELECT * FROM test_bit WHERE i='100'::bit(3) ORDER BY i;

-- 查询大于等于 '100' 的数据，并按照 i 字段排序
SELECT * FROM test_bit WHERE i>='100'::bit(3) ORDER BY i;

-- 查询大于 '100' 的数据，并按照 i 字段排序
SELECT * FROM test_bit WHERE i>'100'::bit(3) ORDER BY i;