-- minimal test, basically just verifying that amcheck

-- 创建测试表 bttest_a 和 bttest_b
CREATE TABLE bttest_a(id int8);
CREATE TABLE bttest_b(id int8);

-- 向 bttest_a 和 bttest_b 表插入数据
INSERT INTO bttest_a SELECT * FROM generate_series(1, 100000);
INSERT INTO bttest_b SELECT * FROM generate_series(100000, 1, -1);

-- 在 bttest_a 和 bttest_b 表上创建 B-tree 索引
CREATE INDEX bttest_a_idx ON bttest_a USING btree (id);
CREATE INDEX bttest_b_idx ON bttest_b USING btree (id);

-- 创建角色 bttest_role

CREATE ROLE bttest_role;

-- 验证权限是否被检查（由于函数不可调用而出错）
SET ROLE bttest_role;
SELECT bt_index_check('bttest_a_idx'::regclass);
SELECT bt_index_parent_check('bttest_a_idx'::regclass);
RESET ROLE;

-- 故意不检查关系权限 - 用受限制的账户在整个集群中运行是有用的，
-- 如上述测试所示，必须显式授予权限。
GRANT EXECUTE ON FUNCTION bt_index_check(regclass) TO bttest_role;
GRANT EXECUTE ON FUNCTION bt_index_parent_check(regclass) TO bttest_role;
SET ROLE bttest_role;
SELECT bt_index_check('bttest_a_idx');
SELECT bt_index_parent_check('bttest_a_idx');
RESET ROLE;

-- 验证普通表被拒绝（出错）
SELECT bt_index_check('bttest_a');
SELECT bt_index_parent_check('bttest_a');

-- 验证不存在的索引被拒绝（出错）
SELECT bt_index_check(17);
SELECT bt_index_parent_check(17);

-- 验证错误的索引类型被拒绝（出错）
BEGIN;
CREATE INDEX bttest_a_brin_idx ON bttest_a USING brin(id);
SELECT bt_index_parent_check('bttest_a_brin_idx');
ROLLBACK;

-- 在事务之外进行正常检查
SELECT bt_index_check('bttest_a_idx');
-- 更广泛的测试
SELECT bt_index_parent_check('bttest_b_idx');

BEGIN;
SELECT bt_index_check('bttest_a_idx');
SELECT bt_index_parent_check('bttest_b_idx');
-- 确保我们没有任何残留的锁定
SELECT * FROM pg_locks
WHERE relation = ANY(ARRAY['bttest_a', 'bttest_a_idx', 'bttest_b', 'bttest_b_idx']::regclass[])
    AND pid = pg_backend_pid();
COMMIT;

-- 清理
DROP TABLE bttest_a;
DROP TABLE bttest_b;
DROP OWNED BY bttest_role; -- permissions
DROP ROLE bttest_role;
