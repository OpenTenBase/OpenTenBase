\c regression_ora
select * from dual where 1 = any(1,2,3);
select * from dual where 3 > any(1,2,-1);
select * from dual where 1 < any(2,3,4);
select * from dual where 1 <> any(2,3,4);


select * from dual where 1 = all(1,2,3);
select * from dual where 3 > all(1,2,-1);
select * from dual where 1 < all(2,3,4);
select * from dual where 1 <> all(2,3,4);

select * from dual where 1 = all(array[1,2,3]);
select * from dual where 3 > all(array[1,2,-1]);
select * from dual where 1 < all(array[2,3,4]);
select * from dual where 1 <> all(array[2,3,4]);


select * from dual where 1 = all(array[1,2,3]);
select * from dual where 3 > all(array[1,2,-1], array[3,4,5]);
select * from dual where 1 < all(array[2,3,4]);
select * from dual where 1 <> all(array[[2,3,4],[2,3,4]]);

select * from dual where 1 <> any(null, 1);
select * from dual where 1 > any(null, null);
select * from dual where 1 < all(null, null);
select * from dual where null = any(null); 

create table test_anyall(i int, j varchar(20));
insert into test_anyall values(1,'1'),(2,'2'),(3,'3');
select * from test_anyall where i = any('5', j) order by 1;
select * from test_anyall where i = all('2', j) order by 1;
select * from test_anyall where i = any(select j from test_anyall) order by 1;


