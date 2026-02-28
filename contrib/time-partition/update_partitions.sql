--
--Update_partitions-获取开始时间、架构名称、主（父）表名称，
--表所有者、日期列的名称，
--如果我们想要“周”或“月”分区。
--将返回已创建的表数。
--


--函数：public.update_partitions（不带时区的时间戳，text，text，text，text）

--DROP FUNCTION public.update_partitions（不带时区的时间戳，text，text，text，text）；

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
--存储传入的begin_time，并将endTime设置为将来的一个月/周/天
--（这允许在月/周/天的任何时间使用cronjob来生成下个月/周的表）
startTime:=(date_trunc(plan,begin_time));
planInterval:=('1 '||plan)::interval;
endTime:=(date_trunc(plan,(current_timestamp + planInterval)));
createdTables:=0;

--开始创建触发器函数，我们将向后生成它。
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
   
--表创建sql语句
   if not exists(select * from information_schema.tables where table_schema = schema_name AND table_name = fullTablename) then
     createStmts:='CREATE TABLE '||schema_name||'.'||fullTablename||' (
              CHECK ('||date_column||' >= '''||startTime||''' AND '||date_column||' < '''||intervalTime||''')
              ) INHERITS ('||schema_name||'.'||primary_table_name||')';    

--运行表创建
     EXECUTE createStmts;

--设置表所有者
     createStmts :='ALTER TABLE '||schema_name||'.'||fullTablename||' OWNER TO '||table_owner||';';
     EXECUTE createStmts;
     
--在时间戳上创建索引
     createStmts:='CREATE INDEX idx_'||fullTablename||'_'||date_column||' ON '||schema_name||'.'||fullTablename||' ('||date_column||');';
     EXECUTE createStmts;
     
--跟踪我们正在创建的表的数量（除了初始运行和回填工作外，应该是1个）。
     createdTables:=createdTables+1;
   end if;
   
--为该表添加case以触发创建sql语句。
   createTrigger:='( NEW.'||date_column||' >= TIMESTAMP '''||startTime||''' AND NEW.'||date_column||' < TIMESTAMP '''||intervalTime||''' ) THEN
              INSERT INTO '||schema_name||'.'||fullTablename||' VALUES (NEW.*); '||createTrigger;
   
   startTime:=intervalTime;
   
   if (startTime <= endTime) 
   then
      createTrigger:=' 
        ELSEIF '||createTrigger;
   end if;
   
end loop;

--创建未知的HOLDER如果它不存在，则可能有未知的表句柄
--没有适当表分区的插入
--这通常比简单地提出错误更可取。
unknownTablename:=primary_table_name||'_unknowns';
IF NOT EXISTS(SELECT * FROM information_schema.tables 
                WHERE table_schema = schema_name 
                AND table_name = unknownTablename) 
  THEN
    createStmts :='CREATE TABLE '||schema_name||'.'||primary_table_name||'_unknowns (
                ) INHERITS ('||schema_name||'.'||primary_table_name||');'; 
      
--执行未知的表创建
    EXECUTE createStmts;

--设置表所有者
    createStmts:='ALTER TABLE '||schema_name||'.'||primary_table_name||'_unknowns OWNER TO '||table_owner||';';
    EXECUTE createStmts;

END IF;

--完成触发器函数的创建（在开始处）。
createTrigger:='CREATE OR REPLACE FUNCTION '||schema_name||'.'||primary_table_name||'_insert_trigger_function()
                RETURNS TRIGGER AS $$
    BEGIN
        IF '||createTrigger;

--运行触发器更换；
EXECUTE createTrigger;

--创建使用触发器函数的触发器（如果尚未创建）
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