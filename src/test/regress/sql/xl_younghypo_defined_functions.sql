-- Test custom-defined functions in OpenTenBase distributed environment
-- Data operations: insert, update, select, delete
-- Node information: xl_nodename_from_id1
-- Error handling: raise exception

create table test_distributed_ops (
    id serial,
    name varchar(50),
    value integer,
    created_at timestamp default now()
);

create function test_data_operations(p_operation text) returns text as $$
declare
    v_result text;
    v_count integer;
    v_record record;
begin
    case upper(p_operation)
        when 'INSERT' then
            -- batch insert test
            v_count := 0;
            for i in 1..10 loop
                insert into test_distributed_ops (name, value) 
                values ('test_' || i, i * 100);
                v_count := v_count + 1;
            end loop;
            v_result := 'Inserted ' || v_count || ' rows';
            
        when 'UPDATE' then
            -- condition update test
            update test_distributed_ops 
            set value = value * 2, name = name || '_updated'
            where value < 500;
            get diagnostics v_count = row_count;
            v_result := 'Updated ' || v_count || ' rows';
            
        when 'SELECT' then
            -- complex query test
            select count(*) as cnt, avg(value) as avg_val
            into v_record
            from test_distributed_ops;
            v_result := format('Count: %s, Avg: %s', v_record.cnt, v_record.avg_val);
            
        when 'DELETE' then
            -- condition delete test
            delete from test_distributed_ops where value > 800;
            get diagnostics v_count = row_count;
            v_result := 'Deleted ' || v_count || ' rows';
            
        else
            raise exception 'Unknown operation: %', p_operation;
    end case;
    
    return v_result;
end;
$$ language plpgsql;

create function xl_nodename_from_id1(integer) returns name as $$
declare 
	n name;
BEGIN
	select node_name into n from pgxc_node where node_id = $1;
	RETURN n;
END;$$ language plpgsql;

select xl_nodename_from_id1(xc_node_id), name, value from test_distributed_ops order by id;

select test_data_operations('INSERT');

select test_data_operations('UPDATE');

select test_data_operations('SELECT');

select test_data_operations('DELETE');

select test_data_operations('UNKNOWN');

drop table if exists test_distributed_ops;
drop function if exists test_data_operations;
drop function if exists xl_nodename_from_id1;