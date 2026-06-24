create database regression_ora_ext sql mode opentenbase_ora;
\c regression_ora_ext
create extension opentenbase_subscription;
select a.relname, b.attname from pg_class a, pg_attribute b where a.oid=b.attrelid 
and( a.relname like 'opentenbase_subscription%' or  a.relname like 'OPENTENBASE_SUBSCRIPTION%') order by 1,2;

select * from opentenbase_subscription;
select * from opentenbase_subscription_parallel;
drop extension opentenbase_subscription;

\c postgres
create extension opentenbase_subscription;
select a.relname, b.attname from pg_class a, pg_attribute b where a.oid=b.attrelid 
and( a.relname like 'opentenbase_subscription%' or  a.relname like 'OPENTENBASE_SUBSCRIPTION%') order by 1,2;
select * from opentenbase_subscription;
select * from opentenbase_subscription_parallel;
drop extension opentenbase_subscription;
