-- 添加一对键值到 hstore
CREATE OR REPLACE FUNCTION hstore_add(hs hstore, key text, value text)
RETURNS hstore AS $$
BEGIN
    -- 添加键值对
    RETURN hs || hstore(key, value);
END;
$$ LANGUAGE plpgsql;

-- 更新 hstore 里的一个键的值(不存在则添加)
CREATE OR REPLACE FUNCTION hstore_update(hs hstore, key text, value text)
RETURNS hstore AS $$
BEGIN
    -- 如果键已存在，则更新
    IF hs ? key THEN
        RETURN hs #- ARRAY[key] || hstore(key, value); -- 移除旧的键值对并添加新的
    ELSE
        -- 如果键不存在，则添加
        RETURN hs || hstore(key, value);
    END IF;
END;
$$ LANGUAGE plpgsql;

-- 从 hstore 删除一个键（及其对应值）
CREATE OR REPLACE FUNCTION hstore_delete_key(hs hstore, key text)
RETURNS hstore AS $$
BEGIN
    RETURN delete(hs, key); -- 使用 hstore 自带的 delete 函数
END;
$$ LANGUAGE plpgsql IMMUTABLE;
