
--Column of types “citext”, “ltree” cannot be used as distribution column
--ltree - labels of data stored in a hierarchical tree-like structure
-- 创建名为 xl_dc27 的表，定义表结构并设置分布方式为哈希分布
CREATE TABLE xl_dc27 (
    product_no integer,
    product_id ltree PRIMARY KEY,
    name MONEY,
    purchase_date TIMETZ,
    price numeric
) DISTRIBUTE BY HASH (product_id); --fail
 -- 这里设置以 product_id 列为哈希分布键




