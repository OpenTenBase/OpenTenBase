/* contrib/ltree/ltreetest.sql */

-- Adjust this setting to control where the objects get created.
-- 设置搜索路径为 public
SET search_path = public;

-- 创建名为 test 的表，包含一个 ltree 类型的列 path
CREATE TABLE test (path ltree);

-- 向 test 表插入数据
INSERT INTO test VALUES ('Top');
INSERT INTO test VALUES ('Top.Science');
INSERT INTO test VALUES ('Top.Science.Astronomy');
INSERT INTO test VALUES ('Top.Science.Astronomy.Astrophysics');
INSERT INTO test VALUES ('Top.Science.Astronomy.Cosmology');
INSERT INTO test VALUES ('Top.Hobbies');
INSERT INTO test VALUES ('Top.Hobbies.Amateurs_Astronomy');
INSERT INTO test VALUES ('Top.Collections');
INSERT INTO test VALUES ('Top.Collections.Pictures');
INSERT INTO test VALUES ('Top.Collections.Pictures.Astronomy');
INSERT INTO test VALUES ('Top.Collections.Pictures.Astronomy.Stars');
INSERT INTO test VALUES ('Top.Collections.Pictures.Astronomy.Galaxies');
INSERT INTO test VALUES ('Top.Collections.Pictures.Astronomy.Astronauts');

-- 使用 GiST 索引在 path 列上创建索引
CREATE INDEX path_gist_idx ON test USING gist(path);

-- 使用 B-tree 索引在 path 列上创建索引
CREATE INDEX path_idx ON test USING btree(path);
