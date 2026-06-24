\c regression_ora

SET client_min_messages = warning;
SET client_encoding = utf8;

-- numeric op all(numeric)
select 'test numeric < all' from dual where 1 < all(2,3,4);
select 'test numeric <= all' from dual where 1 <= all(2,3,4);
select 'test numeric <> all' from dual where 1 <> all(2,3,4);
select 'test numeric > all' from dual where 5 > all(2,3,4);
select 'test numeric = all' from dual where 1 = all(1,1,1);
select 'test numeric >= all' from dual where 4 >= all(2,3,4);

-- numeric op all(str)
select 'test numeric < all' from dual where 1 < all('2','3','4');
select 'test numeric <= all' from dual where 1 <= all('2','3','4');
select 'test numeric <> all' from dual where 1 <> all('2','3','4');
select 'test numeric > all' from dual where 5 > all('2','3','4');
select 'test numeric = all' from dual where 1 = all('1','1','1');
select 'test numeric >= all' from dual where 4 >= all('2','3','4');

-- str op all(str)
select 'test str < all' from dual where '1' < all('2','3','4');
select 'test str <= all' from dual where '1' <= all('2','3','4');
select 'test str <> all' from dual where '1' <> all('2','3','4');
select 'test str > all' from dual where '5' > all('2','3','4');
select 'test str = all' from dual where '1' = all('1','1','1');
select 'test str >= all' from dual where '4' >= all('2','3','4');

-- str op all(numeric)
select 'test str < all' from dual where '1' < all(2,3,4);
select 'test str <= all' from dual where '1' <= all(2,3,4);
select 'test str <> all' from dual where '1' <> all(2,3,4);
select 'test str > all' from dual where '5' > all(2,3,4);
select 'test str = all' from dual where '1' = all(1,1,1);
select 'test str >= all' from dual where '4' >= all(2,3,4);

-- numeric op any(numeric)
select 'test numeric < any' from dual where 1 < any(2,3,4);
select 'test numeric <= any' from dual where 1 <= any(2,3,4);
select 'test numeric <> any' from dual where 1 <> any(2,3,4);
select 'test numeric > any' from dual where 5 > any(2,3,4);
select 'test numeric = any' from dual where 1 = any(1,1,1);
select 'test numeric >= any' from dual where 4 >= any(2,3,4);

-- numeric op any(str)
select 'test numeric < any' from dual where 1 < any('2','3','4');
select 'test numeric <= any' from dual where 1 <= any('2','3','4');
select 'test numeric <> any' from dual where 1 <> any('2','3','4');
select 'test numeric > any' from dual where 5 > any('2','3','4');
select 'test numeric = any' from dual where 1 = any('1','1','1');
select 'test numeric >= any' from dual where 4 >= any('2','3','4');

-- str op any(str)
select 'test str < any' from dual where '1' < any('2','3','4');
select 'test str <= any' from dual where '1' <= any('2','3','4');
select 'test str <> any' from dual where '1' <> any('2','3','4');
select 'test str > any' from dual where '5' > any('2','3','4');
select 'test str = any' from dual where '1' = any('1','1','1');
select 'test str >= any' from dual where '4' >= any('2','3','4');

-- str op any(numeric)
select 'test str < any' from dual where '1' < any(2,3,4);
select 'test str <= any' from dual where '1' <= any(2,3,4);
select 'test str <> any' from dual where '1' <> any(2,3,4);
select 'test str > any' from dual where '5' > any(2,3,4);
select 'test str = any' from dual where '1' = any(1,1,1);
select 'test str >= any' from dual where '4' >= any(2,3,4);

-- numeric op some(numeric)
select 'test numeric < some' from dual where 1 < some(2,3,4);
select 'test numeric <= some' from dual where 1 <= some(2,3,4);
select 'test numeric <> some' from dual where 1 <> some(2,3,4);
select 'test numeric > some' from dual where 5 > some(2,3,4);
select 'test numeric = some' from dual where 1 = some(1,1,1);
select 'test numeric >= some' from dual where 4 >= some(2,3,4);

-- numeric op some(str)
select 'test numeric < some' from dual where 1 < some('2','3','4');
select 'test numeric <= some' from dual where 1 <= some('2','3','4');
select 'test numeric <> some' from dual where 1 <> some('2','3','4');
select 'test numeric > some' from dual where 5 > some('2','3','4');
select 'test numeric = some' from dual where 1 = some('1','1','1');
select 'test numeric >= some' from dual where 4 >= some('2','3','4');

-- str op some(str)
select 'test str < some' from dual where '1' < some('2','3','4');
select 'test str <= some' from dual where '1' <= some('2','3','4');
select 'test str <> some' from dual where '1' <> some('2','3','4');
select 'test str > some' from dual where '5' > some('2','3','4');
select 'test str = some' from dual where '1' = some('1','1','1');
select 'test str >= some' from dual where '4' >= some('2','3','4');

-- str op some(numeric)
select 'test str < some' from dual where '1' < some(2,3,4);
select 'test str <= some' from dual where '1' <= some(2,3,4);
select 'test str <> some' from dual where '1' <> some(2,3,4);
select 'test str > some' from dual where '5' > some(2,3,4);
select 'test str = some' from dual where '1' = some(1,1,1);
select 'test str >= some' from dual where '4' >= some(2,3,4);

-- all include subquery 
select 1 from dual where 1 <= all('2','3',(select '1' from dual));
select 1 from dual where 1 <= all('2.1','3.2',(select '1.1' from dual));

-- all include subquery error
select 1 from dual where 1 <= all('a','3.2',(select '1.1' from dual));
select 1 from dual where 1 <= all('1','3.2',(select 'a' from dual));

-- all include func
select 1 from dual where 1 <= all('1','3.2', to_char('123'));
select 1 from dual where 1 <= all('1','3.2', textlen('123'));
select 1 from dual where 1 < all(ARRAY['5','6'], ARRAY['2','6'], ARRAY['3', '4']);
select 1 from dual where '1' < all(ARRAY[5,6], ARRAY[2,6], ARRAY[3, 4]);
select 1 from dual where 1 < all(ARRAY[ARRAY['3', '5'],ARRAY['6','7']], ARRAY[ARRAY['2', '4'],ARRAY['8','9']]);
select 1 from dual where 1 < all(ARRAY[ARRAY[3, 5],ARRAY[6,7]], ARRAY[ARRAY[2, 4],ARRAY[8,9]]);
select 1 from dual where 1 < all(array[array[array[array['2', 3]]]], array[array[array[array['4', 5]]]]);
select 1 from dual where 1 < all(array[array[array[array['2', '3']]]], array[array[array[array['4', '3']]]]);
select 1 from dual where 1 < all(array[array[array[array[array['2', '3'], array['4', '5']]]]], array[array[array[array[array['4', '5'], array['6', '7']]]]]);
select 1 from dual where 1 < all(array[array[array[array[array['2', 3], array['4', 5]]]]], array[array[array[array[array['4', 5], array['6', 7]]]]]);

-- colref op all
drop table if exists t_op_some_any_all;
create table t_op_some_any_all(id int,f1 numeric(4,2));
select * from t_op_some_any_all where f1<>all('1','2','3');
select * from t_op_some_any_all where id<>all('1','2','3');
select * from t_op_some_any_all where id<>all('1','2',3);  
select * from t_op_some_any_all where id<>all(1,2,3); 
drop table if exists t_op_some_any_all;

-- DONE --
