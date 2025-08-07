/*
 * test for to_number with 'x' format
 */

\c regression

select to_number('ff', 'xx');

\c regression_ora

select to_number('ff', 'xx');
/* 15 character */
select to_number('fffffffffffffff', 'xxxxxxxxxxxxxxxxxxx');
select to_number('123456789012345', 'xxxxxxxxxxxxxxxxxxx');
/* 16 character */
select to_number('ffffffffffffffff', 'xxxxxxxxxxxxxxxxxxx');
select to_number('f123456789012345', 'xxxxxxxxxxxxxxxxxxxx');
/* with scale */
select to_number('124.36', 'xxxxxxx');
select to_number('124.66', 'xxxxxxx');
/* with space */
select to_number('fff ', 'xxxxxx');
select to_number('f f', 'xxxxxx');

/* max values support , opentenbase_ora is 63 digits */
select to_number('fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff', 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx');
select to_number('ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff', 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx');
-- TODO: The result is different with opentenbase_ora.
-- opentenbase_ora: 7237005577332262213973186563042994240829000000000000000000000000000000000000
select to_number('fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff', 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx');
/* minus */
select to_number('-f123456d789012345', 'xxxxxxxxxxxxxxxxxxxx');
/* with scale */
select to_number('-f123456d789012345.314', 'xxxxxxxxxxxxxxxxxxxxxxxxx');
/* with illegal character */
select to_number('uf123456789012345', 'xxxxxxxxxxxxxxxxxxxx');
select to_number('f1234567u89012345', 'xxxxxxxxxxxxxxxxxxxx');

/* fmt is less than str */
select to_number('ff', 'x');
select to_number('ffffffffffffffff', 'xxxxxxxxx');

/* invalid fromat mode */
select to_number('ff', ' xx');
select to_number('ff', 'x x');


\c regression
select to_number('ff', 'xx')
