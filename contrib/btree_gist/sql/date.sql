-- 创建一个名为 datetmp 的表，其中包含一个日期类型的字段
CREATE TABLE datetmp (a date);

-- 从 'data/date.data' 文件中拷贝数据到 datetmp 表中
\copy datetmp from 'data/date.data'

-- 开启顺序扫描
SET enable_seqscan=on;

-- 查询比 '2001-02-13' 小的日期记录数
SELECT count(*) FROM datetmp WHERE a <  '2001-02-13';

-- 查询比 '2001-02-13' 小等于的日期记录数
SELECT count(*) FROM datetmp WHERE a <= '2001-02-13';

-- 查询等于 '2001-02-13' 的日期记录数
SELECT count(*) FROM datetmp WHERE a  = '2001-02-13';

-- 查询比 '2001-02-13' 大等于的日期记录数
SELECT count(*) FROM datetmp WHERE a >= '2001-02-13';

-- 查询比 '2001-02-13' 大的日期记录数
SELECT count(*) FROM datetmp WHERE a >  '2001-02-13';

-- 查询最接近 '2001-02-13' 的三条日期记录及其与目标日期的距离
SELECT a, a <-> '2001-02-13' FROM datetmp ORDER BY a <-> '2001-02-13' LIMIT 3;

-- 在 datetmp 表的 a 字段上创建 GIST 索引
CREATE INDEX dateidx ON datetmp USING gist ( a );

-- 关闭顺序扫描
SET enable_seqscan=off;

-- 使用索引进行查询，查询比 '2001-02-13' 小的日期记录数
SELECT count(*) FROM datetmp WHERE a <  '2001-02-13'::date;

-- 使用索引进行查询，查询比 '2001-02-13' 小等于的日期记录数
SELECT count(*) FROM datetmp WHERE a <= '2001-02-13'::date;

-- 使用索引进行查询，查询等于 '2001-02-13' 的日期记录数
SELECT count(*) FROM datetmp WHERE a  = '2001-02-13'::date;

-- 使用索引进行查询，查询比 '2001-02-13' 大等于的日期记录数
SELECT count(*) FROM datetmp WHERE a >= '2001-02-13'::date;

-- 使用索引进行查询，查询比 '2001-02-13' 大的日期记录数
SELECT count(*) FROM datetmp WHERE a >  '2001-02-13'::date;

-- 解释查询计划，选择 a 字段从 datetmp 表中 ORDER BY 子句中的 a 字段与目标日期 '2001-02-13' 的距离，然后按距离升序排列，最后取前三条记录
EXPLAIN (COSTS OFF)
SELECT a, a <-> '2001-02-13' FROM datetmp ORDER BY a <-> '2001-02-13' LIMIT 3;
