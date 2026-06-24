\c regression
create or replace procedure ttt20231124() as $$
begin 
create table if not exists tb_ttt20231124(a int); 
insert into tb_ttt20231124 select random()*100;
end;
$$ language plpgsql;

create or replace function tmp_func_20231124() returns text IMMUTABLE as $$
begin 
call ttt20231124();
return 'ok';
end;
$$ language plpgsql;

select tmp_func_20231124();
drop procedure ttt20231124;
drop function tmp_func_20231124;
drop table tb_ttt20231124;

\c regression_ora
create or replace procedure ttt20231124() as $$
begin 
create table if not exists tb_ttt20231124(a int); 
insert into tb_ttt20231124 select random()*100;
end;
$$ language plpgsql;

create or replace function tmp_func_20231124() returns text IMMUTABLE as $$
begin 
call ttt20231124();
return 'ok';
end;
$$ language plpgsql;

select tmp_func_20231124();
drop procedure ttt20231124;
drop function tmp_func_20231124;
drop table tb_ttt20231124;