
CREATE OR REPLACE FUNCTION public.create_date_partitions_for_table(begin_time timestamp without time zone,
																   primary_table regclass,
																   date_expression text,
																   spacing interval,
																   fill_child_tables boolean,
																   truncate_parent_table boolean)
RETURNS integer AS
$BODY$
DECLARE startTime timestamp;
DECLARE endTime timestamp;
DECLARE intervalTime timestamp;
DECLARE createStmts text;
DECLARE insertStmts text;
DECLARE createTrigger text;
DECLARE fullTablename text;
DECLARE triggerName text;
DECLARE createdTables integer;
DECLARE intervalEpoch integer;
DECLARE dateFormat text;
DECLARE dateColumnName text;
DECLARE tableOwner text;
DECLARE primary_table_name text;
DECLARE schema_name text;
 
BEGIN
-- determine if the date_expression is a valid identifier
dateColumnName := CASE WHEN date_expression ~* '^[a-z0-9_$]+$' THEN date_expression ELSE 'date' END;

-- determine the date format for the given interval
intervalEpoch := EXTRACT(EPOCH FROM spacing);
dateFormat := CASE WHEN intervalEpoch < EXTRACT(EPOCH FROM interval '1 day') THEN 'error'
		   WHEN intervalEpoch < EXTRACT(EPOCH FROM interval '1 week') THEN 'YYYYDDD'
		   WHEN intervalEpoch < EXTRACT(EPOCH FROM interval '1 month') THEN 'IYYYIW'
		   WHEN intervalEpoch < EXTRACT(EPOCH FROM interval '1 year') THEN 'YYYYMM'
		   ELSE 'YYYY'
	       END;
IF dateFormat = 'error' THEN
  RAISE EXCEPTION 'Interval must be greater than 1 day';
END IF;

SELECT c.relname, n.nspname, a.rolname INTO primary_table_name, schema_name, tableOwner
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON c.relnamespace = n.oid
JOIN pg_catalog.pg_authid AS a ON c.relowner = a.oid
WHERE c.oid = primary_table::oid;


startTime := to_timestamp(to_char(begin_time, dateFormat), dateFormat);
endTime := to_timestamp(to_char(now() + spacing, dateFormat), dateFormat);
createdTables := 0;  
     
WHILE (startTime <= endTime) LOOP
 
   fullTablename := primary_table_name||'_'||to_char(startTime, dateFormat);
   intervalTime := startTime + spacing;

   IF NOT EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = schema_name AND table_name = fullTablename) THEN
     createStmts := 'CREATE TABLE '||schema_name||'.'||fullTablename||' (
              CHECK ('||date_expression||' >= '''||startTime||''' AND '||date_expression||' < '''||intervalTime||''')
              ) INHERITS ('||schema_name||'.'||primary_table_name||')';    
 
     EXECUTE createStmts;
    
     createStmts := 'ALTER TABLE '||schema_name||'.'||fullTablename||' OWNER TO "'||tableOwner||'";';
     EXECUTE createStmts;     
  
     createStmts := 'CREATE INDEX idx_'||fullTablename||'_'||dateColumnName||' ON '||schema_name||'.'||fullTablename||' ('||date_expression||');';
     EXECUTE createStmts;
     
     RAISE NOTICE 'Child table %.% created',schema_name,fullTablename;
    
     IF (fill_child_tables) THEN
        RAISE NOTICE 'Filling child table %.%',schema_name,fullTablename;
        insertStmts := 'INSERT INTO '||schema_name||'.'||fullTablename||' (
            SELECT * FROM '||schema_name||'.'||primary_table_name||' 
            WHERE '||date_expression||' >= '''||startTime||''' AND 
                  '||date_expression||' < '''||intervalTime||'''
              );';
        EXECUTE insertStmts;
     END IF;

     createdTables := createdTables+1;
   END IF;
   
   startTime := intervalTime;
      
END LOOP;

createTrigger := 'CREATE OR REPLACE FUNCTION '||schema_name||'.trf_'||primary_table_name||'_insert_trigger_function()
    RETURNS TRIGGER AS $$
    DECLARE startTime timestamp;
    DECLARE intervalTime timestamp;
    DECLARE fullTablename text;
    DECLARE insertStatment text;
    DECLARE createTableStatment text;
	DECLARE formatDate text;
    BEGIN
	SELECT to_char('||date_expression||','''||dateFormat||''') INTO formatDate FROM (SELECT NEW.*) AS t;
    fullTablename  := '''||primary_table_name||'_''||'||'formatDate;
    insertStatment := ''INSERT INTO '||schema_name||'.'''||'||fullTablename||'' VALUES ($1.*)'';
    BEGIN
        --Try insert on appropiatte child table if exists
        EXECUTE insertStatment using NEW;
        --When child tables not exists, generate it on the fly
        EXCEPTION WHEN UNDEFINED_TABLE THEN
	    startTime := to_timestamp(formatDate, '''||dateFormat||''');
            intervalTime  := startTime + '''||spacing||'''::interval;  

            createTableStatment := ''CREATE TABLE IF NOT EXISTS '||schema_name||'.''||fullTablename||'' (
                  CHECK ('||replace(date_expression, '''', '''''')||' >= ''''''||startTime||'''''' AND '||replace(date_expression, '''', '''''')||' < ''''''||intervalTime||'''''')
                  ) INHERITS ('||schema_name||'.'||primary_table_name||')'';    
            EXECUTE createTableStatment;

            createTableStatment := ''ALTER TABLE '||schema_name||'.''||fullTablename||'' OWNER TO "'||tableOwner||'";'';
            EXECUTE createTableStatment;

            createTableStatment := ''CREATE INDEX idx_''||fullTablename||''_'||dateColumnName||' ON '||schema_name||'.''||fullTablename||'' ('||replace(date_expression, '''', '''''')||');'';
            EXECUTE createTableStatment;    

            --Try the insert again, now the table exists
            EXECUTE insertStatment using NEW;
        WHEN OTHERS THEN        
            RAISE EXCEPTION ''Error in trigger'';
            RETURN NULL;
    END;
    RETURN NULL;
    END;
    $$
    LANGUAGE plpgsql;';

EXECUTE createTrigger;

triggerName := 'tr_'||primary_table_name||'_insert_trigger'; 
IF NOT EXISTS(SELECT * FROM information_schema.triggers WHERE trigger_name = triggerName) THEN
  createTrigger:='CREATE TRIGGER tr_'||primary_table_name||'_insert_trigger
                  BEFORE INSERT ON '||schema_name||'.'||primary_table_name||' 
                  FOR EACH ROW EXECUTE PROCEDURE '||schema_name||'.trf_'||primary_table_name||'_insert_trigger_function();';
  EXECUTE createTrigger;
END IF;

IF (truncate_parent_table) THEN
    RAISE NOTICE 'Truncate ONLY parent table %.%',schema_name,primary_table_name;
    insertStmts := 'TRUNCATE TABLE ONLY '||schema_name||'.'||primary_table_name||';';
    EXECUTE insertStmts;
END IF;

RETURN createdTables;
END;
$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;
ALTER FUNCTION public.create_date_partitions_for_table(timestamp without time zone, regclass, text, interval, boolean, boolean)
  OWNER TO postgres;
COMMENT ON FUNCTION public.create_date_partitions_for_table(timestamp without time zone, regclass, text, interval, boolean, boolean) IS 'The function is created in the public schema and is owned by user postgres.
The function takes params:
begin_time          - Type: timestamp - Desc: time of your earliest data. This allows for backfilling and for reducing trigger function overhead by avoiding legacy date logic.
primary_table  		- Type: regclass      - Desc: name of the parent table. This is used to generate monthly tables ([primary_table_name]_YYYYMM) and an unknown table ([primary_table_name]_unknowns). It is also used in the trigger and trigger function names.
date_expression         - Type: text      - Desc: an expression that returns a date is used for check constraints and insert trigger function.
spacing                - Type: interval      - Desc: an interval which determines the timespan for child tables
fill_child_tables   - Type: boolean   - Desc: if you want to load data from parent table to each child tables.
truncate_parent_table   - Type: boolean   - Desc: if you want to delete table of the parent table

Considerations:

- The insert trigger function is recreated everytime you run this function.

- If child tables already exist, the function simply updates the trigger 
function and moves to the next table in the series.

- This function does not raise exceptions when errant data is encountered.
The trigger captures the UNDEFINED_TABLE exception when any data that does 
not have a matching child table and it automatically generates 
the appropiate child table and insert the row that generated the exeception.

- The function returns the number of tables that it created.

- The fill_child_tables and truncate_parent_table must be used carefully you may
respald your parent table data before
';
