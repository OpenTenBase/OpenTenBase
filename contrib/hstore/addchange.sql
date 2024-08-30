-- hstore 转换为 VARCHAR 文本，通过键名进行查询，如果不存在则返回空字符串
CREATE OR REPLACE FUNCTION hstore_to_varchar(hs hstore, key text)
RETURNS VARCHAR AS $$
BEGIN
    RETURN COALESCE(hs -> key, '');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- 数字数组转 hstore（假设数组索引即为键名）
CREATE OR REPLACE FUNCTION array_to_hstore(arr_key text[], arr_value integer[])
RETURNS hstore AS $$
DECLARE
    result hstore := '';
    i INTEGER;
BEGIN
    FOR i IN array_lower(arr_key, 1) .. array_upper(arr_key, 1) LOOP
       result := result || hstore(arr_key[i], arr_value[i]::text);
    END LOOP;
    RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
