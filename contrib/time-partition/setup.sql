--
-- Setup - Add plpgsql, create a parent table, create an initial child table,
--         create the trigger and the initial trigger function. 
--

-- We'll need plpgsql, so create it in your db if it's not already available.
CREATE LANGUAGE plpgsql;

-- Create a table to act as parent with the appropriate columns for your data.
CREATE TABLE my_schema.my_data (name varchar(24), create_date timestamp);

-- Create an initial child table so that you can create the function/trigger without errors
CREATE TABLE my_schema.my_data_201001 
    (CHECK (create_date >= '2010-01-01' AND create_date < '2010-02-01')) 
    INHERITS (my_schema.my_data);
-- Add an index to your child tables.
CREATE INDEX idx_my_data_201001 ON my_schema.my_data_201001 (create_date);

-- Create the initial function to handle inserting to child tables
CREATE OR REPLACE FUNCTION my_schema.my_data_insert_trigger_function()
RETURNS TRIGGER AS $$
BEGIN
    IF ( NEW.create_date >= '2010-01-01' AND NEW.create_date < '2010-02-01' ) THEN
        INSERT INTO my_schema.my_data_201001 VALUES (NEW.*);
    ELSE
        RAISE EXCEPTION 'Date out of range.  Fix parent_insert_trigger_function()!';
    END IF;
    RETURN NULL;
END;
$$
LANGUAGE plpgsql;
 
-- Create a trigger to call the function before insert.
CREATE TRIGGER my_data_insert_trigger
    BEFORE INSERT ON my_schema.my_data
    FOR EACH ROW EXECUTE PROCEDURE my_schema.my_data_insert_trigger_function();


--
-- Notice that: INSERT INTO my_schema.my_data VALUES ('somename','2010-01-10');
--     inserts into the my_data_201001 table.
-- Notice that: INSERT INTO my_schema.my_data VALUES ('somename','2010-02-10');
--     raises an error, and does not insert anything.
-- Notice that: SELECT * FROM my_schema.my_data;
--     returns records from the child tables.
--