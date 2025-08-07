--
-- Test cases for COPY (INSERT/UPDATE/DELETE) TO
--
create table copydml_test (id serial, t text);
insert into copydml_test (t) values ('a');
insert into copydml_test (t) values ('b');
insert into copydml_test (t) values ('c');
insert into copydml_test (t) values ('d');
insert into copydml_test (t) values ('e');

--
-- Test COPY (insert/update/delete ...)
--
copy (insert into copydml_test (t) values ('f') returning id) to stdout;
copy (update copydml_test set t = 'g' where t = 'f' returning id) to stdout;
copy (delete from copydml_test where t = 'g' returning id) to stdout;

--
-- Test \copy (insert/update/delete ...)
--
\copy (insert into copydml_test (t) values ('f') returning id) to stdout;
\copy (update copydml_test set t = 'g' where t = 'f' returning id) to stdout;
\copy (delete from copydml_test where t = 'g' returning id) to stdout;

-- Error cases
copy (insert into copydml_test default values) to stdout;
copy (update copydml_test set t = 'g') to stdout;
copy (delete from copydml_test) to stdout;

create rule qqq as on insert to copydml_test do instead nothing;
copy (insert into copydml_test default values) to stdout;
drop rule qqq on copydml_test;
create rule qqq as on insert to copydml_test do also delete from copydml_test;
copy (insert into copydml_test default values) to stdout;
drop rule qqq on copydml_test;
create rule qqq as on insert to copydml_test do instead (delete from copydml_test; delete from copydml_test);
copy (insert into copydml_test default values) to stdout;
drop rule qqq on copydml_test;
create rule qqq as on insert to copydml_test where new.t <> 'f' do instead delete from copydml_test;
copy (insert into copydml_test default values) to stdout;
drop rule qqq on copydml_test;

create rule qqq as on update to copydml_test do instead nothing;
copy (update copydml_test set t = 'f') to stdout;
drop rule qqq on copydml_test;
create rule qqq as on update to copydml_test do also delete from copydml_test;
copy (update copydml_test set t = 'f') to stdout;
drop rule qqq on copydml_test;
create rule qqq as on update to copydml_test do instead (delete from copydml_test; delete from copydml_test);
copy (update copydml_test set t = 'f') to stdout;
drop rule qqq on copydml_test;
create rule qqq as on update to copydml_test where new.t <> 'f' do instead delete from copydml_test;
copy (update copydml_test set t = 'f') to stdout;
drop rule qqq on copydml_test;

create rule qqq as on delete to copydml_test do instead nothing;
copy (delete from copydml_test) to stdout;
drop rule qqq on copydml_test;
create rule qqq as on delete to copydml_test do also insert into copydml_test default values;
copy (delete from copydml_test) to stdout;
drop rule qqq on copydml_test;
create rule qqq as on delete to copydml_test do instead (insert into copydml_test default values; insert into copydml_test default values);
copy (delete from copydml_test) to stdout;
drop rule qqq on copydml_test;
create rule qqq as on delete to copydml_test where old.t <> 'f' do instead insert into copydml_test default values;
copy (delete from copydml_test) to stdout;
drop rule qqq on copydml_test;

-- triggers
create function qqq_trig() returns trigger as $$
begin
if tg_op in ('INSERT', 'UPDATE') then
    raise notice '% %', tg_op, new.id;
    return new;
else
    raise notice '% %', tg_op, old.id;
    return old;
end if;
end
$$ language plpgsql;
create trigger qqqbef before insert or update or delete on copydml_test
    for each row execute procedure qqq_trig();
create trigger qqqaf after insert or update or delete on copydml_test
    for each row execute procedure qqq_trig();

copy (insert into copydml_test (t) values ('f') returning id) to stdout;
copy (update copydml_test set t = 'g' where t = 'f' returning id) to stdout;
copy (delete from copydml_test where t = 'g' returning id) to stdout;

drop table copydml_test;
drop function qqq_trig();

\c regression_ora
set transform_insert_to_copy TO on;
drop table if exists t_insert_success;
CREATE TABLE t_insert_success (
    col1 integer,
    col2 integer,
    col3 varchar2(3),
    col4 int,
    col5 date
) distribute by shard(col1);
delete from t_insert_success;
prepare p1_3(int)  as insert into t_insert_success  values(1,2,'a',1,'2011-01-01'),(2,2,2,3,'2020-11-01'),(3,3,3,3,'2022-11-01') ;
execute p1_3(1);
select * from t_insert_success order by 1,2,3,4,5;
select count(*) from t_insert_success;
drop table if exists t_insert_success;
reset transform_insert_to_copy;
