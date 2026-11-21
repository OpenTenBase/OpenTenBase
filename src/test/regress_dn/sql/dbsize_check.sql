-- 测试并行统计数据库大小和串行统计数据库大小返回的结果是否一致
do $$
declare
    v_parallel int;
    v_serial   int;
    v_database text;
    v_sql      text;
begin
    for v_database in select datname from pg_database order by datname
    loop
        set calculate_db_size_worker_number = 10;
        v_sql := 'SELECT pg_database_size($1)';
        EXECUTE v_sql INTO v_parallel USING v_database;

        set calculate_db_size_worker_number = 1;
        v_sql := 'SELECT pg_database_size($1)';
        EXECUTE v_sql INTO v_serial USING v_database;

        if v_parallel != v_serial then
            raise notice 'databases % size is different: % != %', v_database, v_parallel, v_serial;
        else
            raise notice 'databases % size is same', v_database;
        end if;

    end loop;
	reset calculate_db_size_worker_number;
end;
$$;
