-- 创建一个名为 macaddr8tmp 的表，其中包含一个 MAC 地址类型的字段
CREATE TABLE macaddr8tmp (a macaddr8);

-- 从 'data/macaddr.data' 文件中拷贝数据到 macaddr8tmp 表中
\copy macaddr8tmp from 'data/macaddr.data'

-- 开启顺序扫描
SET enable_seqscan=on;

-- 查询比 '22:00:5c:e5:9b:0d' 小的 MAC 地址记录数
SELECT count(*) FROM macaddr8tmp WHERE a <  '22:00:5c:e5:9b:0d';

-- 查询比 '22:00:5c:e5:9b:0d' 小等于的 MAC 地址记录数
SELECT count(*) FROM macaddr8tmp WHERE a <= '22:00:5c:e5:9b:0d';

-- 查询等于 '22:00:5c:e5:9b:0d' 的 MAC 地址记录数
SELECT count(*) FROM macaddr8tmp WHERE a  = '22:00:5c:e5:9b:0d';

-- 查询比 '22:00:5c:e5:9b:0d' 大等于的 MAC 地址记录数
SELECT count(*) FROM macaddr8tmp WHERE a >= '22:00:5c:e5:9b:0d';

-- 查询比 '22:00:5c:e5:9b:0d' 大的 MAC 地址记录数
SELECT count(*) FROM macaddr8tmp WHERE a >  '22:00:5c:e5:9b:0d';

-- 在 macaddr8tmp 表的 a 字段上创建 GIST 索引
CREATE INDEX macaddr8idx ON macaddr8tmp USING gist ( a );

-- 关闭顺序扫描
SET enable_seqscan=off;

-- 使用索引进行查询，查询比 '22:00:5c:e5:9b:0d' 小的 MAC 地址记录数
SELECT count(*) FROM macaddr8tmp WHERE a <  '22:00:5c:e5:9b:0d'::macaddr8;

-- 使用索引进行查询，查询比 '22:00:5c:e5:9b:0d' 小等于的 MAC 地址记录数
SELECT count(*) FROM macaddr8tmp WHERE a <= '22:00:5c:e5:9b:0d'::macaddr8;

-- 使用索引进行查询，查询等于 '22:00:5c:e5:9b:0d' 的 MAC 地址记录数
SELECT count(*) FROM macaddr8tmp WHERE a  = '22:00:5c:e5:9b:0d'::macaddr8;

-- 使用索引进行查询，查询比 '22:00:5c:e5:9b:0d' 大等于的 MAC 地址记录数
SELECT count(*) FROM macaddr8tmp WHERE a >= '22:00:5c:e5:9b:0d'::macaddr8;

-- 使用索引进行查询，查询比 '22:00:5c:e5:9b:0d' 大的 MAC 地址记录数
SELECT count(*) FROM macaddr8tmp WHERE a >  '22:00:5c:e5:9b:0d'::macaddr8;

-- 测试索引仅扫描
-- 关闭位图扫描
SET enable_bitmapscan=off;
-- 解释查询计划，选择所有字段从 macaddr8tmp 表中 WHERE 子句中的 a 字段小于目标 MAC 地址 '02:03:04:05:06:07'，然后按 a 字段升序排列
EXPLAIN (COSTS OFF)
SELECT * FROM macaddr8tmp WHERE a < '02:03:04:05:06:07'::macaddr8 ORDER BY a;
-- 查询所有字段从 macaddr8tmp 表中 WHERE 子句中的 a 字段小于目标 MAC 地址 '02:03:04:05:06:07'，然后按 a 字段升序排列
SELECT * FROM macaddr8tmp WHERE a < '02:03:04:05:06:07'::macaddr8 ORDER BY a;
