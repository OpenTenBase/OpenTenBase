
-- 创建测试函数：判断数字正负零，并处理 NULL
CREATE OR REPLACE FUNCTION classify_number(n INT)
RETURNS TEXT AS $$
DECLARE
    result TEXT;
BEGIN
    IF n IS NULL THEN
        RAISE EXCEPTION 'Input cannot be NULL';
    ELSIF n < 0 THEN
        result := 'Negative';
    ELSIF n = 0 THEN
        result := 'Zero';
    ELSE
        result := 'Positive';
    END IF;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- 测试用例
SELECT classify_number(-10);   -- Negative
SELECT classify_number(0);     -- Zero
SELECT classify_number(25);    -- Positive
SELECT classify_number(NULL);  -- 应该触发异常
