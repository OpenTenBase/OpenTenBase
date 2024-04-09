--
-- Update_partitions - Takes a begin time, schema name, primary (parent) table name,
--                     table owner, the name of the date column,
--                     and if we want 'week'ly or 'month'ly partitions.
--                     The number of created tables is returned.
--                     ex: SELECT public.update_partitions('2010-02-01','my_schema','my_data','postgres','create_date','week')
--


-- Function: public.update_partitions(timestamp without time zone, text, text, text, text, text)

-- DROP FUNCTION public.update_partitions(timestamp without time zone, text, text, text, text, text);

CREATE OR REPLACE FUNCTION public.update_partitions(begin_time timestamp without time zone, schema_name text, primary_table_name text, table_owner text, date_column text, plan text)
  RETURNS integer AS
$BODY$
declare startTime timestamp;
declare endTime timestamp;
declare intervalTime timestamp;
declare createStmts text;
declare createTrigger text;
declare fullTablename text;
declare unknownTablename text;
declare triggerName text;
declare createdTables integer;
declare dateFormat text;
declare planInterval interval;

BEGIN
dateFormat:=CASE WHEN plan='month' THEN 'YYYYMM'
                 WHEN plan='week'  THEN 'IYYYIW'
                 WHEN plan='day'   THEN 'YYYYDDD'
		 WHEN plan='year'  THEN 'YYYY'
                 ELSE 'error'
            END;
IF dateFormat='error' THEN
  RAISE EXCEPTION 'Non valid plan --> %', plan;
END IF;
-- Store the incoming begin_time, and set the endTime to one month/week/day in the future 
--     (this allows use of a cronjob at any time during the month/week/day to generate next month/week/day's table)
startTime:=(date_trunc(plan,begin_time));
planInterval:=('1 '||plan)::interval;
endTime:=(date_trunc(plan,(current_timestamp + planInterval)));
createdTables:=0;

-- Begin creating the trigger function, we're going to generate it backwards.
createTrigger:=' 
        ELSE
            INSERT INTO '||schema_name||'.'||primary_table_name||'_unknowns VALUES (NEW.*);
        END IF;
        RETURN NULL;
    END;
    $$
    LANGUAGE plpgsql;';
            
while (startTime <= endTime) loop

   fullTablename:=primary_table_name||'_'||to_char(startTime,dateFormat);
   intervalTime:= startTime + planInterval;
   
   -- The table creation sql statement
   if not exists(select * from information_schema.tables where table_schema = schema_name AND table_name = fullTablename) then
     createStmts:='CREATE TABLE '||schema_name||'.'||fullTablename||' (
              CHECK ('||date_column||' >= '''||startTime||''' AND '||date_column||' < '''||intervalTime||''')
              ) INHERITS ('||schema_name||'.'||primary_table_name||')';    

     -- Run the table creation
     EXECUTE createStmts;

     -- Set the table owner
     createStmts :='ALTER TABLE '||schema_name||'.'||fullTablename||' OWNER TO '||table_owner||';';
     EXECUTE createStmts;
     
     -- Create an index on the timestamp
     createStmts:='CREATE INDEX idx_'||fullTablename||'_'||date_column||' ON '||schema_name||'.'||fullTablename||' ('||date_column||');';
     EXECUTE createStmts;
     
     -- Track how many tables we are creating (should likely be 1, except for initial run and backfilling efforts).
     createdTables:=createdTables+1;
   end if;
   
   -- Add case for this table to trigger creation sql statement.
   createTrigger:='( NEW.'||date_column||' >= TIMESTAMP '''||startTime||''' AND NEW.'||date_column||' < TIMESTAMP '''||intervalTime||''' ) THEN
              INSERT INTO '||schema_name||'.'||fullTablename||' VALUES (NEW.*); '||createTrigger;
   
   startTime:=intervalTime;
   
   if (startTime <= endTime) 
   then
      createTrigger:=' 
        ELSEIF '||createTrigger;
   end if;
   
end loop;

-- CREATE UNKNOWN HOLDER IF IT DOES NOT EXIST, unknowns table handles possible 
--      inserts for which there is not an appropriate table partition
--      This is often more desirable than simply raising an error.
unknownTablename:=primary_table_name||'_unknowns';
IF NOT EXISTS(SELECT * FROM information_schema.tables 
                WHERE table_schema = schema_name 
                AND table_name = unknownTablename) 
  THEN
    createStmts :='CREATE TABLE '||schema_name||'.'||primary_table_name||'_unknowns (
                ) INHERITS ('||schema_name||'.'||primary_table_name||');'; 
      
    -- Execute the unknown table creation
    EXECUTE createStmts;

    -- Set the table owner
    createStmts:='ALTER TABLE '||schema_name||'.'||primary_table_name||'_unknowns OWNER TO '||table_owner||';';
    EXECUTE createStmts;

END IF;

-- Finish creating the trigger function (at the beginning).
createTrigger:='CREATE OR REPLACE FUNCTION '||schema_name||'.'||primary_table_name||'_insert_trigger_function()
                RETURNS TRIGGER AS $$
    BEGIN
        IF '||createTrigger;

-- Run the trigger replacement;
EXECUTE createTrigger;

-- Create the trigger that uses the trigger function, if it isn't already created
triggerName:=primary_table_name||'_insert_trigger';
if not exists(select * from information_schema.triggers where trigger_name = triggerName) then
  createTrigger:='CREATE TRIGGER '||primary_table_name||'_insert_trigger
                  BEFORE INSERT ON '||schema_name||'.'||primary_table_name||' 
                  FOR EACH ROW EXECUTE PROCEDURE '||schema_name||'.'||primary_table_name||'_insert_trigger_function();';
  EXECUTE createTrigger;
END if;
return createdTables;
END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE
  COST 100;
ALTER FUNCTION public.update_partitions(timestamp without time zone, text, text, text, text, text) OWNER TO postgres;