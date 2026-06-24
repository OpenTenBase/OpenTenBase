--support != . <> . <=. >= with blanks
--test cases 
\c regression_ora
create table test_cmp_syntax(i int, j int);
insert into test_cmp_syntax values (1,2), (3,4),(5,6);
--test less eq syntax
select * from test_cmp_syntax where i < = 3 order by 1;
select * from test_cmp_syntax where i <=  3 order by 1;
select * from test_cmp_syntax where i </*comment*/ = 3 order by 1;
select * from test_cmp_syntax where i <--comment
= 3 order by 1;
select * from test_cmp_syntax where i<  =3 order by 1;

--test greater eq syntax
select * from test_cmp_syntax where i > = 3 order by 1;
select * from test_cmp_syntax where i >=  3 order by 1;
select * from test_cmp_syntax where i >/*comment*/ = 3 order by 1;
select * from test_cmp_syntax where i >--comment
                                        = 3 order by 1;
select * from test_cmp_syntax where i>  =3 order by 1;

--test not eq syntax
select * from test_cmp_syntax where i < > 3 order by 1;
select * from test_cmp_syntax where i <>  3 order by 1;
select * from test_cmp_syntax where i </*comment*/ > 3 order by 1;
select * from test_cmp_syntax where i <--comment
                                        > 3 order by 1;
select * from test_cmp_syntax where i<  >3 order by 1;

select * from test_cmp_syntax where i ! = 3 order by 1;
select * from test_cmp_syntax where i !=  3 order by 1;
select * from test_cmp_syntax where i !/*comment*/ = 3 order by 1;
select * from test_cmp_syntax where i !--comment
                                        = 3 order by 1;
select * from test_cmp_syntax where i!  =3 order by 1;
