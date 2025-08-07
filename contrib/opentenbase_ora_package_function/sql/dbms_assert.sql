\c opentenbase_ora_package_function_regression_ora
select dbms_assert.enquote_literal('some text '' some text') from dual;
select dbms_assert.enquote_name('''"AAA') from dual;
select dbms_assert.enquote_name('''"AAA', false) from dual;
select dbms_assert.enquote_name('''"AAA', true) from dual;
select dbms_assert.noop('some string') from dual;
select dbms_assert.qualified_sql_name('aaa.bbb.ccc."aaaa""aaa"') from dual;
select dbms_assert.qualified_sql_name('aaa.bbb.cc%c."aaaa""aaa"') from dual;
select dbms_assert.schema_name('dbms_assert') from dual;
select dbms_assert.schema_name('jabadabado') from dual;
select dbms_assert.simple_sql_name('"Aaa dghh shsh"') from dual;
select dbms_assert.simple_sql_name('ajajaj -- ajaj') from dual;
select dbms_assert.sql_object_name('pg_catalog.pg_class') from dual;
select dbms_assert.sql_object_name('dbms_assert.fooo') form dual;

select dbms_assert.enquote_literal(NULL) from dual;
select dbms_assert.enquote_name(NULL) from dual;
select dbms_assert.enquote_name(NULL, false) form dual;
select dbms_assert.noop(NULL) from dual;
select dbms_assert.qualified_sql_name(NULL) from dual;
select dbms_assert.qualified_sql_name(NULL) from dual;
select dbms_assert.schema_name(NULL) from dual;
select dbms_assert.schema_name(NULL) from dual;
select dbms_assert.simple_sql_name(NULL) from dual;
select dbms_assert.simple_sql_name(NULL) from dual;
select dbms_assert.sql_object_name(NULL) from dual;
select dbms_assert.sql_object_name(NULL) from dual;

------------------------------------------------------------------------------------------------------------------------
-----dbms_assert.enquote_literal
select dbms_assert.enquote_literal('opentenbase no quotes') from dual;
select dbms_assert.enquote_literal('''') from dual;
select dbms_assert.enquote_literal('''''''') from dual;
select dbms_assert.enquote_literal('''''') from dual;
select dbms_assert.enquote_literal('''''''''') from dual;
select dbms_assert.enquote_literal(''' h2 opentenbase') from dual;
select dbms_assert.enquote_literal('''''h4 opentenbase') from dual;
select dbms_assert.enquote_literal('''''''h6 opentenbase') from dual;
select dbms_assert.enquote_literal('''''''''h8 opentenbase') from dual;
select dbms_assert.enquote_literal('opentenbase t4''''') from dual;
select dbms_assert.enquote_literal('opentenbase t2''') from dual;
select dbms_assert.enquote_literal('opentenbase t6''''''') from dual;
select dbms_assert.enquote_literal('opentenbase t8''''''''') from dual;
select dbms_assert.enquote_literal('m4''''opentenbase') from dual;
select dbms_assert.enquote_literal('m8''''''''opentenbase') from dual;
select dbms_assert.enquote_literal('m2''opentenbase') from dual;
select dbms_assert.enquote_literal('m6''''''opentenbase') from dual;
select dbms_assert.enquote_literal('''h2t2 opentenbase''') from dual;
select dbms_assert.enquote_literal('''h2t6 opentenbase''''''') from dual;
select dbms_assert.enquote_literal('''h2t4 opentenbase''''') from dual;
select dbms_assert.enquote_literal('''h2t8 opentenbase''''''''') from dual;
select dbms_assert.enquote_literal('''''h4t2 opentenbase''') from dual;
select dbms_assert.enquote_literal('''''h4t4 opentenbase''''') from dual;
select dbms_assert.enquote_literal('''''h4t6 opentenbase''''''') from dual;
select dbms_assert.enquote_literal('''''h4t8 opentenbase''''''''') from dual;
select dbms_assert.enquote_literal('''''''h6t6 opentenbase''''''') from dual;
select dbms_assert.enquote_literal('''''''h6t2 opentenbase''') from dual;
select dbms_assert.enquote_literal('''''''h6t4 opentenbase''''') from dual;
select dbms_assert.enquote_literal('''''''h6t8 opentenbase''''''''') from dual;
select dbms_assert.enquote_literal('''''''''h8t8 opentenbase''''''''') from dual;
select dbms_assert.enquote_literal('''''''''h8t6 opentenbase''''''') from dual;
select dbms_assert.enquote_literal('''''''''h8t4 opentenbase''''') from dual;
select dbms_assert.enquote_literal('''''''''h8t2 opentenbase''') from dual;
select dbms_assert.enquote_literal('h''''h4t4'''' opentenbase''''t') from dual;
select dbms_assert.enquote_literal('h''''h4''t4'' opentenbase''''t') from dual;
select dbms_assert.enquote_literal('h''''h4''t4'''' opentenbase''''t') from dual;
select dbms_assert.enquote_literal('h''''h4''''t4'''' opentenbase''''t') from dual;

-----dbms_assert.simple_sql_name
select dbms_assert.simple_sql_name('abc$') from dual;
select dbms_assert.simple_sql_name('1abc') from dual;
select dbms_assert.simple_sql_name('abc¡ã?1t') from dual;

-----dbms_assert.enquote_name
select dbms_assert.enquote_name('_a321bcde') from dual;
select dbms_assert.enquote_name('a321bcde') from dual;
select dbms_assert.enquote_name('a321bcde', true) from dual;
select dbms_assert.enquote_name('abcde') from dual;
 
create sequence dbms_assert_demo_seq_abc_20231224; 
create or replace procedure dbms_assert_demo_proc(seed int) as
$$
begin
end;
$$LANGUAGE default_plsql;

create table "dbms_assert_demo_table?D1¨²"(id integer);

create table dbms_assert_demo_hint_t7_20231224(f1 integer,f2 integer) ;
create index dbms_assert_demo_hint_t7_f1_index_20231224 on dbms_assert_demo_hint_t7_20231224(f1);

----dbms_assert.sql_object_name
select dbms_assert.sql_object_name('dbms_assert_demo_proc') from dual;
select dbms_assert.sql_object_name('dbms_assert_demo_seq_abc_20231224') from dual;
select dbms_assert.sql_object_name('dbms_assert_demo_table?D1¨²') from dual;
select dbms_assert.sql_object_name('dbms_assert_demo_hint_t7_20231224') from dual;
select dbms_assert.sql_object_name('dbms_assert_demo_hint_t7_f1_index_20231224') from dual;

----dbms_assert.qualified_sql_name
select dbms_assert.qualified_sql_name('opentenbase$.a.b') from dual;
select dbms_assert.qualified_sql_name('opentenbase.a._b') from dual;
select dbms_assert.qualified_sql_name('1opentenbase.1a.1b') from dual;
select dbms_assert.qualified_sql_name('opentenbase$.a.ab1t') from dual;

drop sequence dbms_assert_demo_seq_abc_20231224;
drop procedure dbms_assert_demo_proc;
drop table dbms_assert_demo_hint_t7_20231224;
