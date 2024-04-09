-- Function: public.create_date_partitions_for_table(timestamp without time zone, text, regclass, text, interval, boolean, boolean, text)
-- DROP FUNCTION public.create_date_partitions_for_table(timestamp without time zone, text, regclass, text, interval, boolean, boolean, text);

CREATE OR REPLACE FUNCTION public.create_date_partitions_for_table (
	begin_time TIMESTAMP WITHOUT TIME ZONE,
	timezone TEXT,
	primary_table regclass,
	date_expression TEXT,
	spacing INTERVAL,
	fill_child_tables BOOLEAN,
	truncate_parent_table BOOLEAN
) RETURNS INTEGER AS $BODY$

DECLARE start_time TIMESTAMP;
DECLARE end_time TIMESTAMP;
DECLARE interval_time TIMESTAMP;
DECLARE create_stmt TEXT;
DECLARE insert_stmt TEXT;
DECLARE create_trigger TEXT;
DECLARE full_table_name TEXT;
DECLARE my_trigger_name TEXT;
DECLARE created_tables INTEGER;
DECLARE interval_epoch INTEGER;
DECLARE date_format TEXT;
DECLARE date_column_name TEXT;
DECLARE table_owner TEXT;
DECLARE primary_table_name TEXT;
DECLARE my_schema_name TEXT;


BEGIN


-- determine if the date_expression is a valid identifier
CASE
	WHEN date_expression ~* '^[a-z0-9_$]+$' THEN
		date_column_name := date_expression;
	ELSE
		date_column_name := 'date';
END CASE;

-- determine the date format for the given interval
interval_epoch := EXTRACT (EPOCH FROM spacing);
CASE
	WHEN interval_epoch < EXTRACT (EPOCH FROM INTERVAL '1 day') THEN
		date_format := 'error';
	WHEN interval_epoch < EXTRACT (EPOCH FROM INTERVAL '1 week') THEN
		date_format := 'YYYYDDD';
	WHEN interval_epoch < EXTRACT (EPOCH FROM INTERVAL '1 month') THEN
		date_format := 'IYYYIW';
	WHEN interval_epoch < EXTRACT (EPOCH FROM INTERVAL '1 year') THEN
		date_format := 'YYYYMM';
	ELSE
		date_format := 'YYYY';
END CASE;

IF date_format = 'error' THEN
	RAISE EXCEPTION 'Interval must be greater than 1 day' ;
END IF;

-- get the table name, schema and owner
SELECT
	c.relname,
	n.nspname,
	a.rolname INTO primary_table_name,
	my_schema_name,
	table_owner
FROM
	pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON c.relnamespace = n.oid
JOIN pg_catalog.pg_authid AS a ON c.relowner = a.oid
WHERE
	c.oid = primary_table :: oid;

-- Store the incoming begin_time, and set the end_time to one month/week/day in the future 
-- (this allows use of a cronjob at any time during the month/week/day to generate next month/week/day's table)
start_time := to_timestamp(to_char(begin_time, date_format), date_format);
end_time := to_timestamp(to_char(now() + spacing, date_format),	date_format);
created_tables := 0;

WHILE (start_time <= end_time) LOOP

	full_table_name := primary_table_name || '_' || to_char(start_time, date_format);
	interval_time := start_time + spacing;

	-- The table creation sql statement
	IF NOT EXISTS (SELECT	*	FROM information_schema.tables WHERE table_schema = my_schema_name	AND TABLE_NAME = full_table_name) THEN
		create_stmt := 
				'CREATE TABLE ' || my_schema_name || '.' || full_table_name || ' (
					CHECK (' || date_expression || ' >= TIMESTAMP ''' || start_time || ''' AT TIME ZONE ''' || timezone || ''' 
					AND ' || date_expression || ' < TIMESTAMP ''' || interval_time || ''' AT TIME ZONE ''' || timezone || ''')
				) INHERITS (' || my_schema_name || '.' || primary_table_name || ')';
		
		-- Run the table creation
		EXECUTE create_stmt;

		-- Set the table owner     
		create_stmt := 'ALTER TABLE ' || my_schema_name || '.' || full_table_name || ' OWNER TO "' || table_owner || '";';
		EXECUTE create_stmt;

		-- Create an index on the timestamp     
		create_stmt := 'CREATE INDEX idx_' || full_table_name || '_' || date_column_name || ' ON ' || my_schema_name || '.' || full_table_name || ' (' || date_expression || ');';
		EXECUTE create_stmt;
		RAISE NOTICE 'Child table %.% created', my_schema_name, full_table_name;

		--if fill_child_tables is true then we fill the child table with the parent's table data that satisfies the child's table check constraint
		IF (fill_child_tables) THEN
			RAISE NOTICE 'Filling child table %.%', my_schema_name, full_table_name;
			insert_stmt := 
					'INSERT INTO ' || my_schema_name || '.' || full_table_name || ' (
						SELECT * FROM ' || my_schema_name || '.' || primary_table_name || ' 
						WHERE ' || date_expression || ' AT TIME ZONE ''' || timezone || ''' >= ''' || start_time || ''' 
						AND ' || date_expression || ' AT TIME ZONE ''' || timezone || ''' < ''' || interval_time || '''
					);';
			EXECUTE insert_stmt ;
		END IF;

		-- Track how many tables we are creating (should likely be 1, except for initial run and backfilling efforts).
		created_tables := created_tables + 1;
	END IF;

	start_time := interval_time;

END LOOP;

-- The UNDEFINED_TABLE exception is captured on child table is created 'on-the-fly' when new data arrives and 
-- no partition is created to match this data criteria
create_trigger := 
		'CREATE OR REPLACE FUNCTION ' || my_schema_name || '.trf_' || primary_table_name || '_insert_trigger_function()	RETURNS TRIGGER AS $$

		DECLARE start_time timestamp;
		DECLARE timezone text;
		DECLARE interval_time timestamp;
		DECLARE full_table_name text;
		DECLARE insertStatment text;
		DECLARE createTableStatment text;
		DECLARE formatDate text;
	
		BEGIN

			SELECT to_char(' || date_expression || ',''' || date_format || ''') INTO formatDate FROM (SELECT NEW.*) AS t;
			full_table_name := ''' || primary_table_name || '_''||' || 'formatDate;
			insertStatment := ''INSERT INTO ' || my_schema_name || '.''' || '||full_table_name||'' VALUES ($1.*)'';
			timezone := ''' || timezone || ''';
			
			BEGIN

				--Try insert on appropiatte child table if exists
				EXECUTE insertStatment using NEW;

				--When child tables not exists, generate it on the fly
				EXCEPTION WHEN UNDEFINED_TABLE THEN
					start_time := to_timestamp(formatDate, ''' || date_format || ''');
					interval_time := start_time + ''' || spacing || '''::interval; 

					createTableStatment := ''CREATE TABLE IF NOT EXISTS ' || my_schema_name || '.''||full_table_name||'' (
							CHECK (' || REPLACE (date_expression,	'''',	'''''') || ' >= TIMESTAMP ''''''||start_time||'''''' AT TIME ZONE ''''''||timezone||'''''' 
							AND ' || REPLACE (date_expression, '''', '''''') || ' < TIMESTAMP ''''''||interval_time||'''''' AT TIME ZONE ''''''||timezone||'''''')) 
							INHERITS (' || my_schema_name || '.' || primary_table_name || ')'';
					EXECUTE createTableStatment;
					
					createTableStatment := ''ALTER TABLE ' || my_schema_name || '.''||full_table_name||'' OWNER TO "' || table_owner || '";'';
					EXECUTE createTableStatment;

					createTableStatment := ''CREATE INDEX idx_''||full_table_name||''_' || date_column_name || ' ON ' || my_schema_name || '.''||full_table_name||'' 
							(' || REPLACE (date_expression, '''', '''''') || ');'';
					EXECUTE createTableStatment;

					--Try the insert again, now the table exists
					EXECUTE insertStatment using NEW;

					WHEN OTHERS THEN
						RAISE EXCEPTION ''Error in trigger'';
					
					RETURN NULL;
					
			END;
				
			RETURN NULL;

		END;

		$$ LANGUAGE plpgsql;';
EXECUTE create_trigger ;

-- Create the trigger that uses the trigger function, if it isn't already created
my_trigger_name := 'tr_' || primary_table_name || '_insert_trigger';

IF NOT EXISTS (SELECT * FROM information_schema.triggers WHERE TRIGGER_NAME = my_trigger_name) THEN
	create_trigger := 
			'CREATE TRIGGER tr_' || primary_table_name || '_insert_trigger 
			BEFORE INSERT ON ' || my_schema_name || '.' || primary_table_name || ' 
			FOR EACH ROW EXECUTE PROCEDURE ' || my_schema_name || '.trf_' || primary_table_name || '_insert_trigger_function();';
	EXECUTE create_trigger;
END IF;

-- If truncate_parent_table parameter is true, we truncate only the parent table data AS this data is in child tables
IF (truncate_parent_table) THEN
	RAISE NOTICE 'Truncate ONLY parent table %.%', my_schema_name,	primary_table_name;
	insert_stmt := 'TRUNCATE TABLE ONLY ' || my_schema_name || '.' || primary_table_name || ';';
	EXECUTE insert_stmt;
END IF;

RETURN created_tables;


END;


$BODY$ LANGUAGE plpgsql VOLATILE COST 100;

ALTER FUNCTION public.create_date_partitions_for_table (
	TIMESTAMP WITHOUT TIME ZONE,
	TEXT,
	regclass,
	TEXT,
	INTERVAL,
	BOOLEAN,
	BOOLEAN
) OWNER TO postgres;

COMMENT ON FUNCTION public.create_date_partitions_for_table (
	TIMESTAMP WITHOUT TIME ZONE,
	TEXT,
	regclass,
	TEXT,
	INTERVAL,
	BOOLEAN,
	BOOLEAN
) IS 
'The function is created in the public schema and is owned by user postgres.
The function takes params:
begin_time
	- Type: timestamp
	- Desc: time of your earliest data. This allows for backfilling and for reducing trigger function overhead by avoiding legacy date logic.
timezone
	- Type: text
	- Desc: time zone for check constraints. Available time zones in pg_timezone_names table.
primary_table
	- Type: regclass
	- Desc: name of the parent table. This is used to generate monthly tables ([primary_table_name]_YYYYMM) and an unknown table ([primary_table_name]_unknowns). It is also used in the trigger and trigger function names.
date_expression
	- Type: text
	- Desc: an expression that returns a date is used for check constraints and insert trigger function.
spacing
	- Type: interval
	- Desc: an interval which determines the timespan for child tables.
fill_child_tables
	- Type: boolean
	- Desc: if you want to load data from parent table to each child tables.
truncate_parent_table
	- Type: boolean
	- Desc: if you want to delete table of the parent table.

Considerations:

- The insert trigger function is recreated everytime you run this function.

- If child tables already exist, the function simply updates the trigger function and moves to the next table in the series.

- This function does not raise exceptions when errant data is encountered. The trigger captures the UNDEFINED_TABLE exception when any data that does not have a matching child table and it automatically generates the appropiate child table and insert the row that generated the exeception.

- The function returns the number of tables that it created.

- The fill_child_tables and truncate_parent_table must be used carefully you may respald your parent table data before.';
