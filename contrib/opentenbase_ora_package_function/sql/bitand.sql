\c opentenbase_ora_package_function_regression_ora
-- test const
select bitand(123,67);
select bitand(67,bitand(123,67));
select bitand('123',67);
select bitand(123,'67');
select bitand('123','67');
select bitand('67',bitand('123',67));

-- test var
create table bitand_test(id int,c1 int,c2 bigint,c3 varchar(30),c4 text);
insert into bitand_test values(1,15,31,'63','127');
insert into bitand_test values(2,31,63,'127','255');
insert into bitand_test values(3,63,127,'255','511');
select id,bitand(c1,c2),bitand(c2,c3),bitand(c3,c4),bitand(c1,c3),bitand(c1,c4),bitand(c2,c4) from bitand_test order by id;

-- test null
insert into bitand_test values(4,63,null,'',null);
select id,bitand(c1,c2),bitand(c2,c3),bitand(c3,c4),bitand(c1,c3),bitand(c1,c4),bitand(c2,c4) from bitand_test where id=4 order by 1;
select bitand('',c2),bitand(null,c3),bitand(c3,null),bitand(null,null),bitand(c1,''),bitand(c2,c4) from bitand_test order by 6;

-- test error
select bitand('1a',3);
select bitand(3,'234a');
insert into bitand_test values(5,63,null,'q','abc');
select id,bitand(c1,c2) from bitand_test where id=5;
select id,bitand(c2,c3) from bitand_test where id=5;
select id,bitand(c3,c4) from bitand_test where id=5;

-- clean up
drop table bitand_test;
