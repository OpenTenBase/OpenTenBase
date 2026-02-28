--
--设置-添加opentenbase，创建父表，创建初始子表，
--创建触发器和初始触发器函数。
--

--我们需要opentenbase，所以如果它还不可用，请在数据库中创建它。
CREATE LANGUAGE opentenbase;

--创建一个表作为父表，其中包含数据的相应列。
CREATE TABLE my_schema.my_data (name varchar(24), create_date timestamp);

--创建一个初始子表，以便创建函数/触发器时不会出错
CREATE TABLE my_schema.my_data_202404 
    (CHECK (create_date >= '2024-04-01' AND create_date < '2024-05-01')) 
    INHERITS (my_schema.my_data);
--向子表添加索引。
CREATE INDEX idx_my_data_201001 ON my_schema.my_data_201001 (create_date);

--创建初始函数以处理对子表的插入
CREATE OR REPLACE FUNCTION my_schema.my_data_insert_trigger_function()
RETURNS TRIGGER AS $$
BEGIN
    IF ( NEW.create_date >= '2024-04-01' AND NEW.create_date < '2024-05-01' ) THEN
        INSERT INTO my_schema.my_data_201001 VALUES (NEW.*);
    ELSE
        RAISE EXCEPTION '日期超出范围。修复parent_insert_trigger_function（）！';
    END IF;
    RETURN NULL;
END;
$$
LANGUAGE plpgsql;
 
--创建一个触发器以在插入之前调用函数。
CREATE TRIGGER my_data_insert_trigger
    BEFORE INSERT ON my_schema.my_data
    FOR EACH ROW EXECUTE PROCEDURE my_schema.my_data_insert_trigger_function();

