-- 创建一个包含timestamptz列的表
CREATE TABLE timestamptztmp (a timestamptz);

-- 从指定路径导入数据到表中
\copy timestamptztmp from 'data/timestamptz.data'

-- 设置启用顺序扫描
SET enable_seqscan=on;

-- 查询小于给定时间戳 '2018-12-18 10:59:54 GMT+3' 的记录数
SELECT count(*) FROM timestamptztmp WHERE a <  '2018-12-18 10:59:54 GMT+3';

-- 查询小于等于给定时间戳 '2018-12-18 10:59:54 GMT+3' 的记录数
SELECT count(*) FROM timestamptztmp WHERE a <= '2018-12-18 10:59:54 GMT+3';

-- 查询等于给定时间戳 '2018-12-18 10:59:54 GMT+3' 的记录数
SELECT count(*) FROM timestamptztmp WHERE a  = '2018-12-18 10:59:54 GMT+3';

-- 查询大于等于给定时间戳 '2018-12-18 10:59:54 GMT+3' 的记录数
SELECT count(*) FROM timestamptztmp WHERE a >= '2018-12-18 10:59:54 GMT+3';

-- 查询大于给定时间戳 '2018-12-18 10:59:54 GMT+3' 的记录数
SELECT count(*) FROM timestamptztmp WHERE a >  '2018-12-18 10:59:54 GMT+3';


-- 重复以上查询，但将比较的时区改为 GMT+2 和 GMT+4

-- 创建索引以加速查询
CREATE INDEX timestamptzidx ON timestamptztmp USING gist ( a );

-- 关闭顺序扫描
SET enable_seqscan=off;

-- 使用timestamptz类型进行比较，以确保正确性

-- 解释查询计划（不显示成本）
EXPLAIN (COSTS OFF)
SELECT a, a <-> '2018-12-18 10:59:54 GMT+2' FROM timestamptztmp ORDER BY a <-> '2018-12-18 10:59:54 GMT+2' LIMIT 3;
