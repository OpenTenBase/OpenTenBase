-- 创建 btree_gin 扩展
CREATE EXTENSION btree_gin;

-- Check whether any of our opclasses fail amvalidate
-- 检查是否有任何 opclasses 失败 amvalidate
SELECT amname, opcname
FROM pg_opclass opc LEFT JOIN pg_am am ON am.oid = opcmethod
WHERE opc.oid >= 16384 AND NOT amvalidate(opc.oid);
