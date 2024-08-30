-- 关闭顺序扫描
set enable_seqscan=off;

-- 确保测试输出的一致性，无论默认的 bytea 格式如何
SET bytea_output TO escape;

-- 创建测试表
CREATE TABLE test_bytea (
    i bytea
);

-- 插入数据
INSERT INTO test_bytea VALUES ('a'),('ab'),('abc'),('abb'),('axy'),('xyz');

-- 创建 GIN 索引
CREATE INDEX idx_bytea ON test_bytea USING gin (i);

-- 查询小于 'abc' 的数据，并按照 i 字段排序
SELECT * FROM test_bytea WHERE i<'abc'::bytea ORDER BY i;

-- 查询小于等于 'abc' 的数据，并按照 i 字段排序
SELECT * FROM test_bytea WHERE i<='abc'::bytea ORDER BY i;

-- 查询等于 'abc' 的数据，并按照 i 字段排序
SELECT * FROM test_bytea WHERE i='abc'::bytea ORDER BY i;

-- 查询大于等于 'abc' 的数据，并按照 i 字段排序
SELECT * FROM test_bytea WHERE i>='abc'::bytea ORDER BY i;

-- 查询大于 'abc' 的数据，并按照 i 字段排序
SELECT * FROM test_bytea WHERE i>'abc'::bytea ORDER BY i;