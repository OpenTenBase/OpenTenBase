\c regression_ora
select * from (select string_to_array('a b cd ef', ' '));
select * from table((select string_to_array('a b cd ef', ' ')));
select * from table(select string_to_array('a b cd ef', ' '));

-- The subquery must return a collection type
select * from table(select '{1}'::int[]);
select * from table(select '{1,2}'::int[]);
select * from table(select 1); -- error

-- The SELECT list of the subquery must contain exactly one item
select * from table(select string_to_array('a b cd ef', ' '));
select * from table(select 1, string_to_array('a b cd ef', ' ')); -- error

-- The subquery must return only a single collection
select * from table((select string_to_array('a b cd ef', ' ') from (select * from generate_series(1, 1))));
select * from table((select string_to_array('a b cd ef', ' ') from (select * from generate_series(1, 2)))); -- error

-- TAPD:119760864
SELECT sum(to_number(t.column_value))
FROM
TABLE(XMLSEQUENCE(EXTRACT(xmltype('<root><parent_1 gid="1" id="1"><idx>1</idx></parent_1></root>'), '//*[@gid=1]/idx/text()'))) t;

SELECT sum(to_number(extract(t.column_value,'//text()')))
FROM
TABLE(XMLSEQUENCE(EXTRACT(xmltype('<root><parent_1 gid="1" id="1"><idx>1</idx></parent_1></root>'), '//*[@gid=1]/idx/text()'))) t;

SELECT sum(to_number(extract(t.column_value,'/idx/text()')))
FROM
TABLE(XMLSEQUENCE(EXTRACT(xmltype('<root><parent_1 gid="1" id="1"><idx>1</idx></parent_1></root>'), '//*[@gid=1]/idx'))) t;
