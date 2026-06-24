--
-- test \dn in opentenbase_ora mode, filter out package and type object created scheam.
--

create database db_ora_test_dn_20240229 with sql mode opentenbase_ora;
\c db_ora_test_dn_20240229;

\dn;
\dn+;

create or replace package pkg_test_20240229 is
a int;
end;
/

create or replace type typobj_20240229 as object
(
f int,
e int
);
/

select nspname from pg_namespace where nspname not like '%temp%' order by 1;

\dn;
\dn+;

\c regression_ora;

drop database db_ora_test_dn_20240229 with(force);
