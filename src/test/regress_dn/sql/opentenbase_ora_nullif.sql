\c regression_ora

-- tapd例子
select 'a'||NULLIF('', '1')||'b' from dual;
select 'a'||NULLIF('null', 'null')||'b' from dual;
select 'a'||NULLIF('null', '')||'b' from dual;
select 'a'||NULLIF('', '')||'b' from dual;
select 'a'||NULLIF('2', '')||'b' from dual;
select 'a'||NULLIF('2', '2')||'b' from dual;

-- 直接使用函数
-- TestPoint : 数字比较
select nullif(1,0) from dual;
select nullif(1,3) from dual;
select nullif(1,1) from dual;
select nullif(110,110) from dual;
select nullif(100.000,100.0) from dual;
select nullif(321,123) from dual;
select nullif(-8, 8) from dual;
select nullif(-8,-8) from dual;
select nullif(8-1,7) from dual;
select nullif(7,6+1) from dual;
select nullif(2*8, 4*4) from dual;

-- TestPoint : 字符串
select nullif('','') from dual;
select nullif('a','a') from dual;
select nullif('b','b') from dual;
select nullif('a','b') from dual;
select nullif('haha','') from dual;
select nullif('','a') from dual;
select nullif('一一','11') from dual;
select nullif('hello','world') from dual;
select nullif('yes','y es') from dual;
select nullif('你好','你好') from dual;
select nullif(' 明天','明天') from dual;
select nullif('鷄','鷄') from dual;
select nullif('鶸鷄','鶸鷄') from dual;
-- TestPoint : 超长字符串
select nullif('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa') from dual;
select nullif('if God had gifted me with some beauty and much wealth, I should have made it as hard for you to leave me, as it is now for me to leave you. I am not talking to you now through the medium of custom, conventionalities, nor even of mortal flesh: it is my spirit that addresses your spirit; just as if both had passed through the grave, and we stood at God’s feet, equal — as we are! ’','if God had gifted me with some beauty and much wealth, I should have made it as hard for you to leave me, as it is now for me to leave you. I am not talking to you now through the medium of custom, conventionalities, nor even of mortal flesh: it is my spirit that addresses your spirit; just as if both had passed through the grave, and we stood at God’s feet, equal — as we are! ’') from dual;

-- TestPoint : 混用
select nullif('2021-09-15', to_date('2021-09-15')) from dual;
select nullif('2021-09-15'::date, to_date('2021-09-15')) from dual;
select nullif(null, null) from dual;
select nullif(null, trim(' ')) from dual;
select nullif(null, 1) from dual;
select nullif(null, 'a') from dual;
select nullif(null, '') from dual;
select nullif(trim(' '), null) from dual;
select nullif(trim(' '), trim(' ')) from dual;
select nullif(trim(' '), 1) from dual;
select nullif(trim(' '), 'a') from dual;
select nullif(trim(' '), '') from dual;
select nullif(1, null) from dual;
select nullif(1, trim(' ')) from dual;
select nullif(1, 1) from dual;
select nullif(1, '1') from dual;
select nullif(1, '1'::integer) from dual;
select nullif(1, cast('1' as integer)) from dual;
select nullif(1, '') from dual;
select nullif('1', null) from dual;
select nullif('1', trim(' ')) from dual;
select nullif('1', 1) from dual;
select nullif('1', '1') from dual;
select nullif('1', '') from dual;
select nullif('', '') from dual;
select nullif('', '1') from dual;
select nullif('', 'a') from dual;
select nullif('', null) from dual;
select nullif('', trim(' ')) from dual;

-- TestPoint : 函数嵌套使用
select nullif('a'||trim('            ')||'a', 'aa') from dual;
select nullif('a'||trim('            ')||'a', 'aa') as res from dual;
select nullif('', nvl2(7*16+13, ascii(null), 123)) from dual;
select nvl(nullif('', nvl2(7*16+13, ascii(null), 123)), -1) from dual;
select nullif(nvl(trim('  '),'-1'), 1-2) from dual;   --err
select nullif(ltrim(rtrim('xyxxDWEYExyyx','xy'),'xy'), 'D'||'W'||'E'||'Y'||'E') from dual;
select nullif('=='||trim(trailing ' ' from trim(leading ' ' from '  '||length(lpad('tech', 7))||' '))||'==', '==7==') from dual;
SELECT nullif(trim(SUBSTR(lpad('abcde',INSTR('32.8,63.5',',', 1, 1), '2'), 2)) , 'bcde') FROM DUAL;
select nullif(substring('Ruby on Rails', 1, 4), substr('Ruby on Rails', 1, 4)) from dual;
select nullif(instr('TechOnTheNet', 'On','0001E1', '00001'),0) from dual;
SELECT nullif(INSTR('32.8,63.5',',', 1, 1), 2+2+1)  FROM dual;

-- TestPoint : 查询中使用
create table test_nullif1(id int, c1 varchar2(50), c2 varchar2(50), c3 int, c4 number(20,10), c5 float);
insert into test_nullif1(id,c1,c2,c3,c4,c5) values(1,'abc', '明天', 1234, 1234.1234, 123.123);
insert into test_nullif1(id,c1,c2,c3,c4,c5) values(2,'bcd', '你好', 2345, 2345.2345, 234.234);
insert into test_nullif1(id,c1,c2,c3,c4,c5) values(3,'cde', '天空', 3456, 3456.3456, 345.345);
insert into test_nullif1(id,c1,c2,c3,c4,c5) values(4,'def', '一碧', 4567, 4567.4567, 456.456);
insert into test_nullif1(id,c1,c2,c3,c4,c5) values(5,'fgh', '灿烂', 5678, 5678.5678, 567.567);
insert into test_nullif1(id,c1,c2,c3,c4,c5) values(6,'ghi', '的阳光', 6789, 6789.6789, 678.678);
insert into test_nullif1(id,c1,c2,c3,c4,c5) values(7,'hij', '密密的', 7890, 7890.7890, 789.789);
insert into test_nullif1(id,c1,c2,c3,c4,c5) values(8,'ijk', '松针的', 8901, 8901.8901, 890.890);
insert into test_nullif1(id,c1,c2,c3,c4,c5) values(9,'jkl', '缝隙间', 9012, 9012.9012, 901.901);
insert into test_nullif1(id,c1,c2,c3,c4,c5) values(10,'klm', '射下来', 0123, 0123.0123, 012.012);
insert into test_nullif1(id,c1,c2,c3,c4,c5) values(11,'lmn', '形成', 12345, 12345.12345, 987.987);
insert into test_nullif1(id,c1,c2,c3,c4,c5) values(12,'mno', '一束束', 23456, 23456.23456, 876.876);
insert into test_nullif1(id,c1,c2,c3,c4,c5) values(13,'nop', '粗粗细细', 34567, 34567.34567, 765.765);
insert into test_nullif1(id,c1,c2,c3,c4,c5) values(14,'opq', '这一次相遇', 45678, 45678.45678, 654.654);
insert into test_nullif1(id,c1,c2,c3,c4,c5) values(15,'pqr', '美得彻骨', 56789, 56789.56789, 543.543);
insert into test_nullif1(id,c1,c2,c3,c4,c5) values(16,'qrs', '美得震颤', 67890, 67890.67890, 432.432);
insert into test_nullif1(id,c1,c2,c3,c4,c5) values(17,'rst', '美得孤绝', 78901, 78901.78901, 321.321);


create table test_nullif2(id int, c1 varchar2(50), c2 varchar2(50), c3 int, c4 number(20,10), c5 float);
insert into test_nullif2(id,c1,c2,c3,c4,c5) values(1,'abc', '明天', 1234, 1234.1234, 123.123);
insert into test_nullif2(id,c1,c2,c3,c4,c5) values(2,'bcd', '你好', 2345, 2345.2345, 234.234);
insert into test_nullif2(id,c1,c2,c3,c4,c5) values(3,'cde', '天空', 3456, 3456.3456, 345.345);
insert into test_nullif2(id,c1,c2,c3,c4,c5) values(4,'def', '一碧', 4567, 4567.4567, 456.456);
insert into test_nullif2(id,c1,c2,c3,c4,c5) values(5,'fgh', '灿烂', 5678, 5678.5678, 567.567);
insert into test_nullif2(id,c1,c2,c3,c4,c5) values(6,'ghi', '的阳光', 6789, 6789.6789, 678.678);
insert into test_nullif2(id,c1,c2,c3,c4,c5) values(7,'hij', '密密的', 7890, 7890.7890, 789.789);
insert into test_nullif2(id,c1,c2,c3,c4,c5) values(8,'ijk', '松针的', 8901, 8901.8901, 890.890);
insert into test_nullif2(id,c1,c2,c3,c4,c5) values(9,'jkl', '缝隙间', 9012, 9012.9012, 901.901);
insert into test_nullif2(id,c1,c2,c3,c4,c5) values(10,'klm', '射下来', 0123, 0123.0123, 012.012);
insert into test_nullif2(id,c1,c2,c3,c4,c5) values(11,'lmn', '形成1', 123451, 12345.1234, 987.9871);
insert into test_nullif2(id,c1,c2,c3,c4,c5) values(12,'mno', '一束束1', 234561, 23456.2345, 876.8761);
insert into test_nullif2(id,c1,c2,c3,c4,c5) values(13,'nop', '粗粗细细1', 345671, 34567.3456, 765.7651);
insert into test_nullif2(id,c1,c2,c3,c4,c5) values(14,'opq', '这一次相遇1', 456781, 45678.4567, 654.6541);
insert into test_nullif2(id,c1,c2,c3,c4,c5) values(15,'pqr', '美得彻骨1', 567891, 56789.5678, 543.5431);
insert into test_nullif2(id,c1,c2,c3,c4,c5) values(16,'qrs', '美得震颤1', 678901, 67890.6789, 432.4321);
insert into test_nullif2(id,c1,c2,c3,c4,c5) values(17,'rst', '美得孤绝1', 789011, 78901.7890, 321.3211);

-------------------------------------------------------------------------------------------------------------------------------------------------------------
select nullif(t1.c1, t2.c1) from test_nullif1 t1, test_nullif2 t2 where t1.id = t2.id and nullif(t1.c1, t2.c1) is not null order by t1.c1, t2.c1;
select nullif(t1.c2, t2.c2) from test_nullif1 t1, test_nullif2 t2 where t1.id = t2.id and nullif(t1.c2, t2.c2) is not null order by t1.c1, t2.c1;
select nullif(t1.c3, t2.c3) from test_nullif1 t1, test_nullif2 t2 where t1.id = t2.id and nullif(t1.c3, t2.c3) is not null order by t1.c1, t2.c1;
select nullif(t1.c4, t2.c4) from test_nullif1 t1, test_nullif2 t2 where t1.id = t2.id and nullif(t1.c4, t2.c4) is not null order by t1.c1, t2.c1;
select nullif(t1.c5, t2.c5) from test_nullif1 t1, test_nullif2 t2 where t1.id = t2.id and nullif(t1.c5, t2.c5) is not null order by t1.c1, t2.c1;

select nullif(t2.c1, t1.c1) from test_nullif1 t1, test_nullif2 t2 where t1.id = t2.id and nullif(t1.c1, t2.c1) is not null order by t1.c1, t2.c1;
select nullif(t2.c2, t1.c2) from test_nullif1 t1, test_nullif2 t2 where t1.id = t2.id and nullif(t1.c2, t2.c2) is not null order by t1.c1, t2.c1;
select nullif(t2.c3, t1.c3) from test_nullif1 t1, test_nullif2 t2 where t1.id = t2.id and nullif(t1.c3, t2.c3) is not null order by t1.c1, t2.c1;
select nullif(t2.c4, t1.c4) from test_nullif1 t1, test_nullif2 t2 where t1.id = t2.id and nullif(t1.c4, t2.c4) is not null order by t1.c1, t2.c1;
select nullif(t2.c5, t1.c5) from test_nullif1 t1, test_nullif2 t2 where t1.id = t2.id and nullif(t1.c5, t2.c5) is not null order by t1.c1, t2.c1;

select t1.c1, t2.c1 from test_nullif1 t1, test_nullif2 t2 where nullif(t1.c1, t2.c1) is null order by t1.c1, t2.c1;
select t1.c2, t2.c2 from test_nullif1 t1, test_nullif2 t2 where nullif(t1.c2, t2.c2) is null order by t1.c1, t2.c1;
select t1.c3, t2.c3 from test_nullif1 t1, test_nullif2 t2 where nullif(t1.c3, t2.c3) is null order by t1.c1, t2.c1;
select t1.c4, t2.c4 from test_nullif1 t1, test_nullif2 t2 where nullif(t1.c4, t2.c4) is null order by t1.c1, t2.c1;
select t1.c5, t2.c5 from test_nullif1 t1, test_nullif2 t2 where nullif(t1.c5, t2.c5) is null order by t1.c1, t2.c1;

select * from test_nullif1 where nullif(c4, 7890) is not null order by id;
select * from test_nullif1 where nullif(c5, 7890) is not null order by id;
select * from test_nullif1 where nullif(c3, 7890) is not null order by id;
-- TestPoint : 报错
select * from test_nullif1 where nullif(c2, 7890) is not null order by id;
select * from test_nullif1 where nullif(c1, 7890) is not null order by id;

-- TestPoint : update
update test_nullif1 set c1='abcc' where nullif(c1,'abc') is null;
update test_nullif1 set c1='bcdd' where nullif(c1,'bcd') is null and nullif(c2,'灿烂') is not null;
update test_nullif1 set c1='cdee' where nullif(c1,'cde') is null and nullif(c2,'天空') is not null;

-- TestPoint : cte
With CTE AS(Select ID, c1 FROM test_nullif1 t1 where nullif(t1.c1, 'abc') is not null)
Select * From CTE order by id;

With CTE AS(Select ID, c2 FROM test_nullif1 t1 where nullif(t1.c2, '形成') is not null)
Select * From CTE order by id;

With CTE AS(Select ID, c3 FROM test_nullif1 t1 where nullif(t1.c3, 7890) is not null)
Select * From CTE order by id;

With CTE AS(Select ID, c4 FROM test_nullif1 t1 where nullif(t1.c4, 7890.789) is not null)
Select * From CTE order by id;

-- TestPoint : function
create or replace function pro1(p1 int, check1 varchar) returns varchar as
$$
declare
res varchar2(10);
begin
select nullif(c1, check1) into res from test_nullif1 t1 where t1.id=p1;
raise notice '%','res: '||res;
if res is null then
    return 'the characters are right';
else
    return 'pls check the column';
end if;
end;
$$language default_plsql;


select pro1(3,'cde');
select pro1(13,'opq');
select pro1(5,'bcd');
select pro1(11,'哈哈');
select pro1(15,'pqr');
-- TODO: this case failed, it should return null and no any message output.
select pro1(-1, 'do nothing');

-- TestPoint : delete
delete from test_nullif1 where nullif(c3, 7890) is null;
delete from test_nullif1 where nullif(c3, 7890) is not null;

-- 878855291
select nullif(now(),null) is null;

drop function pro1;


drop table test_nullif1;
drop table test_nullif2;


-- add more test
create or replace function f1f(a int) returns varchar2 as
$$
begin
return null;
end;
$$ language  default_plsql;

create or replace function f2f(a int) returns int as
$$
begin
return a;
end;
$$ language  default_plsql;

create or replace function f3f(a int) returns char as
$$
begin
return to_char(a);
end;
$$ language  default_plsql;

create or replace function f4f(a int) returns varchar2 as
$$
begin
return cast(a as varchar2);
end;
$$ language  default_plsql;

create table niftest(a int, b varchar2(8));
insert into niftest values(1, '1');
insert into niftest values(2, null);
insert into niftest values(3, '');
insert into niftest values(4, null);

-- expect OK
select nullif(to_char(a), to_char(b)) from niftest;
select nullif(1, 1) from dual;
select nullif(to_number('1'), to_number('3')) from dual;

select nullif(to_number('1'), 3.2) from dual;


select nullif(1, null) from dual;
select nullif('12', null) from dual;
select nullif('12', '') from dual;
select nullif(to_date('2022-3-2'), null) from dual;
select nullif('1', to_char(1)) from dual;

do
$$
declare
a int := 3;
b int := 5;
c number;
begin
select nullif(1, a) into c from dual;
select nullif(1, b) into c from dual;
end;
$$;

select nullif(1, f2f(1)) from dual;

select nullif('ab', 'a' || 'b') from dual;
select nullif('ab', 'a' || '  b') from dual;
select nullif('ab', 'a' || '' || 'b') from dual;

select nullif('12', 1 || '2') from dual;
select nullif('1', '2') from dual;
select nullif(1, 2.3) from dual;

select nullif('', '1') from dual;
select nullif('', '23') from dual;
select nullif(' ', '23') from dual;
select nullif('', '') from dual;
select nullif('', null) from dual;


-- expect report error
select nullif(1, '1') from dual;

select nullif('1', to_number('1')) from dual;
select nullif(1, cast(1 as varchar2(30))) from dual;
select nullif(1, cast(1 as char(1))) from dual;
select nullif('1', case when 1 = 1 then to_number('2') else to_number('1') end) from dual;
select nullif(1, '') from dual;
select nullif(null, '1') from dual;
select nullif(f1f(1), 1) from dual;
select nullif(1, to_char(1)) from dual;
select nullif(1, f3f(1)) from dual;
select nullif(1, f4f(1)) from dual;
select nullif(1, f1f(1)) from dual;
select nullif(1, case when 1 = 1 then to_char(2) else to_char(1) end) from dual;
select nullif(null, '') from dual;
select nullif('', 1) from dual;
select nullif(a, b) from niftest;

-- begin: 117428000
select  nullif( cast('a' as CHAR(20)),cast('a' as CHAR(20)) ) from dual;
select  nullif( cast('a' as CHAR(20)),cast('' as CHAR(20)) ) from dual;
select  nullif( cast('' as CHAR(20)),cast('a' as CHAR(20)) ) from dual;
select  nullif( cast('' as CHAR(20)),cast('' as CHAR(20)) ) from dual;
select  nullif( null,cast('' as CHAR(20)) ) from dual;
select  nullif( '',cast('' as CHAR(20)) ) from dual;
select  nullif( ''::CHAR(20),cast('' as CHAR(20)) ) from dual;
select  nullif( ''::date,cast('' as CHAR(20)) ) from dual;
select  nullif( cast('' as date),cast('' as CHAR(20)) ) from dual;
select  nullif( cast('' as varchar2),cast('' as CHAR(20)) ) from dual;
-- end: 117428000

-- begin:126794443
select nullif(s.col1,22) c1, 2 c2 from dual left outer join (select 1 col1,2 col2 from dual where 1=2) s on 1=1 ;			-- failed
select nullif(s.col1,22) c1, 2 c2 from dual left outer join (select abs(123) col1,2 col2 from dual where 1=2) s on 1=1 ;	-- failed
select nullif(s.col1,22) c1, 2 c2 from dual left outer join (select '' col1,2 col2 from dual) s on 1=1 ;					-- failed
-- end: 126794443

drop table niftest;
drop function f1f;
drop function f2f;
drop function f3f;
drop function f4f;

