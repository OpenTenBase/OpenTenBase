/*
 * test cases for binary_integer types.
 * 1. syntaxer support
 *  1.1 binary_integer
 * 2. assignment - binary_integer
 *  2.1 simple_integer/pls_integer/int2/4/8 var as source - over/under flow
 *  2.2 simple_integer/pls_integer/int2/4/8 var as target - over/under flow
 *  2.3 simple_integer/pls_integer/int2/4/8 const as target - over/under flow
 *  2.4 float4/8 var as source - over/under flow
 *  2.5 numeric/float4/8 var as target - over/under flow
 *  2.6 numeric/float4/8 const as target - over/under flow
 * 3. operator - binary_integer
 *  3.1 binary_integer/simple_integer/pls_integer/int2/4/8 + - * / neg(-) > < =
 *   3.1.1 overflow/underflow
 *  3.2 numeric/float4/8 + - * / > < =
 *   3.2.1 overflow/underflow
 *  3.3 lvalue is float8
 *  3.4 lvalue is binary_integer
 *  3.5 div by 0 and power
 * 4. dbms_output - put binary/simple/pls_integer
 */
\c opentenbase_ora_package_function_regression_ora
-- 1. syntaxer support
--  1.1 binary_integer
create or replace function simpint_test() returns text as $$
declare
	bint binary_integer;
begin
	bint = 123;
	RETURN 'OK' || bint;
end;
$$ language default_plsql;
select simpint_test();

-- 2. assignment - binary_integer
--  2.1 simple_integer/pls_integer/int2/4/8 var as source - over/under flow
create or replace function simpint_test() returns text as $$
declare
	bint binary_integer;
    pls pls_integer;
    si simple_integer default 0;
	i2 int2;
	i4 int4;
	i8 bigint;
begin
    pls = 12;
    si = 12;
	i2 = 2;
	i4 = 4;
	i8 = 8;

    bint = pls;
	raise notice 'bint %',bint;

	bint = si;
	raise notice 'bint %',bint;

	bint = i2;
	raise notice 'bint %',bint;

	bint = i4;
	raise notice 'bint %',bint;

	bint = i8;
	raise notice 'bint %',bint;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

-- over/under flow
-- boundary
create or replace function simpint_test() returns text as $$
declare
	bint binary_integer;
	i8u bigint;
	i8o bigint;
begin
	i8u = -2147483648;
	i8o = 2147483647;

	bint = i8u;
	raise notice 'bint %',bint;

	bint = i8o;
	raise notice 'bint %',bint;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

create or replace function simpint_test() returns text as $$
declare
	bint binary_integer;
	i8u bigint;
	i8o bigint;
begin
	i8u = -2147483649;
	i8o = 2147483648;

	bint = i8u;
	raise notice 'bint %',bint;

	bint = i8o;
	raise notice 'bint %',bint;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--  2.2 simple_integer/pls_integer/int2/4/8 var as target - over/under flow
create or replace function simpint_test() returns text as $$
declare
	bint binary_integer;
    pls pls_integer;
    si simple_integer default 0;
	i2 int2;
	i4 int4;
	i8 bigint;
begin
	bint = 12;
    pls = bint + 1;
    si = bint + 1;
	i2 = bint;
	i4 = bint + 1;
	i8 = bint + 2;

    raise notice 'bint %',pls;
	raise notice 'bint %',si;
	raise notice 'bint %',i2;
	raise notice 'bint %',i4;
	raise notice 'bint %',i8;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

create or replace function simpint_test() returns text as $$
declare
	bint binary_integer;
    pls pls_integer;
    si simple_integer default 0;
	i2 int2;
	i4 int4;
	i8 bigint;
begin
	bint = 2147483647;
    pls = bint + 1;
    si = bint + 1;
	i2 = bint;
	i4 = bint + 1;
	i8 = bint + 2;

    raise notice 'bint %',pls;
	raise notice 'bint %',si;
	raise notice 'bint %',i2;
	raise notice 'bint %',i4;
	raise notice 'bint %',i8;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--  2.3 simple_integer/pls_integer/int2/4/8 const as target - over/under flow
create or replace function simpint_test() returns text as $$
declare
	bint binary_integer;
begin
    bint = 2::pls_integer;
	raise notice 'bint %',bint;

	bint = 4::simple_integer;
	raise notice 'bint %',bint;

	bint = 2::int2;
	raise notice 'bint %',bint;

	bint = 4::int4;
	raise notice 'bint %',bint;

	bint = 8::int8;
	raise notice 'bint %',bint;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

-- overflow
create or replace function simpint_test() returns text as $$
declare
	bint binary_integer;
begin
	bint = -2147483649;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

create or replace function simpint_test() returns text as $$
declare
	bint binary_integer;
begin
	bint = 2147483648;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--  2.4 float4/8 var as source - over/under flow
create or replace function simpint_test() returns text as $$
declare
	bint binary_integer;
	f4 float4;
	f8 float8;
	n numeric;
begin
	f4 = 12.0;
	f8 = 14.0;
	n = 15;

	bint = f4;
	raise notice 'bint %',bint;

	bint = f8;
	raise notice 'bint %',bint;

	bint = n;
	raise notice 'bint %',bint;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

-- over/underflow
create or replace function simpint_test() returns text as $$
declare
	bint binary_integer;
	f8 float8;
begin
	f8 = -2147483649;

	bint = f8;
	raise notice 'bint %',bint;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--  2.5 numeric/float4/8 var as target - over/under flow
create or replace function simpint_test() returns text as $$
declare
	bint binary_integer;
	f4 float4;
	f8 float8;
	n numeric;
begin
	bint = 12;
	f4 = bint + 1.1;
	f8 = bint + 2.2;
	n = bint + 33.3;

	raise notice 'bint %',f4;
	raise notice 'bint %',f8;
	raise notice 'bint %',n;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--  2.6 numeric/float4/8 const as target - over/under flow
create or replace function simpint_test() returns text as $$
declare
	bint binary_integer;
begin
	bint = 12.0;
	raise notice 'bint %',bint;

	bint = 14.0;
	raise notice 'bint %',bint;

	bint = 15;
	raise notice 'bint %',bint;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

-- 3. operator - binary_integer
--  3.1 binary_integer/simple_integer/pls_integer/int2/4/8 + - * / neg(-) > < =
-- var
create or replace function simpint_test() returns text as $$
declare
	bint binary_integer;
    pls pls_integer;
    si simple_integer default 0;
	i2 int2;
	i4 int4;
	i8 bigint;
begin
	bint = 12;
    pls = 12;
    si = 12;
	i2 = 2;
	i4 = 4;
	i8 = 8;

    bint = bint + pls;
	bint = bint - pls;
	bint = bint * pls;
	bint = bint / pls;
	raise notice 'bint %',bint;

    bint = bint + si;
	bint = bint - si;
	bint = bint * si;
	bint = bint / si;
	raise notice 'bint %',bint;

	bint = bint + i2;
	bint = bint - i2;
	bint = bint * i2;
	bint = bint / i2;
	raise notice 'bint %',bint;

	bint = bint + i4;
	bint = bint - i4;
	bint = bint * i4;
	bint = bint / i4;
	raise notice 'bint %',bint;

	bint = bint + i8;
	bint = bint - i8;
	bint = bint * i8;
	bint = bint / i8;
	raise notice 'bint %',bint;

	bint = -bint * i8;
	raise notice 'bint %',bint;

	if bint > 1 or bint > pls or bint > si or bint >i2 then
		raise notice '%', bint >= i8;
	else
		raise notice '%', bint <= i4;
	end if;

	if bint > 1.1 then
		raise notice 'OK';
	else
		raise notice 'OK';
	end if;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

-- const
create or replace function simpint_test() returns text as $$
declare
	bint binary_integer;
begin
	bint = 12;

	bint = bint + 2::int2;
	raise notice 'bint %',bint;
	bint = bint + 4::int4;
	raise notice 'bint %',bint;
	bint = bint * 8::int8;
	raise notice 'bint %',bint;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

-- binary_integer + - * / neg(-) > < =
create or replace function simpint_test() returns text as $$
declare
	bi1 binary_integer default 12;
	bi2 binary_integer default 4;
	bi3 binary_integer default 0;
	res bool default false;
begin
	bi3 = bi1 + bi2;
	raise notice 'bi %',bi3;
	
	bi3 = bi1 - bi2;
	raise notice 'bi %',bi3;
	
	bi3 = bi1 * bi2;
	raise notice 'bi %',bi3;
	
	bi3 = bi1 / bi2;
	raise notice 'bi %',bi3;
	
	bi3 = - bi1;
	raise notice 'bi %',bi3;
	
	bi3 = + bi1;
	raise notice 'bi %',bi3;
	
	res = bi1 > bi2;
	raise notice 'res %',res;
	
	res = bi1 < bi2;
	raise notice 'res %',res;
	
	res = bi1 >= bi2;
	raise notice 'res %',res;
	
	res = bi1 <= bi2;
	raise notice 'res %',res;
	
	res = (bi1 = bi2);
	raise notice 'res %',res;
	
	res = (bi1 <> bi2);
	raise notice 'res %',res;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--   3.1.1 overflow/underflow
declare
   bi binary_integer;
begin
   bi := 2147483647;
   bi := bi + 1::binary_integer;
end;
/

declare
   bi binary_integer;
begin
   bi := -2147483647;
   bi := bi - 2::binary_integer;
end;
/

declare
   bi binary_integer;
begin
   bi := 214748364;
   bi := bi * 11::binary_integer;
end;
/

declare
   bi binary_integer;
begin
   bi := -2147483648;
   bi := -bi;
end;
/

declare
   a simple_integer default 0;
   b binary_integer;
begin
	a := 2147483647;
	b := 2;
	raise notice '%',a+b;
end;
/

declare
   a simple_integer default 0;
   b binary_integer;
begin
	a := -2147483647;
	b := 2;
	raise notice '%',a-b;
end;
/

declare
   a simple_integer default 0;
   b binary_integer;
begin
	a := 2147483647;
	b := 2;
	raise notice '%',a*b;
end;
/

declare
   a simple_integer default 0;
   b binary_integer;
begin
	a := 2147483647;
	b := 2;
	raise notice '%',a/b;
end;
/
--  3.2 numeric/float4/8 + - * / > < =
-- var
create or replace function simpint_test() returns text as $$
declare
	bint binary_integer;
	f4 float4;
	f8 float8;
begin
	bint = 12;
	f4 = 4.4;
	f8 = 8.8;

	bint = bint + f4;
	raise notice 'bint %',bint;
	bint = bint + f8;
	raise notice 'bint %',bint;

	f4 = bint + f4;
	raise notice 'bint %',f4;
	f8 = bint + f8;
	raise notice 'bint %',f8;
	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--   3.2.1 overflow/underflow
-- tested
--  3.3 lvalue is float8
-- tested
--  3.4 lvalue is binary_integer
-- tested
--  3.5 div by 0 and power
create or replace function simpint_test() returns text as $$
declare
	bint binary_integer;
	f4 float4;
	f8 float8;
begin
	bint = 1;
	bint = bint / 0;
	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

-- 4. dbms_output - put binary/simple/pls_integer
create or replace function simpint_test() returns text as $$
declare
	bi binary_integer;
	si simple_integer default 1;
	pls pls_integer;
begin
	bi = 1;
	si = 1;
	pls = 1;
	CALL dbms_output.serveroutput('t');
	CALL dbms_output.put(bi);
	CALL dbms_output.put(si);
	CALL dbms_output.put(pls);

	CALL dbms_output.put_line(bi);
	CALL dbms_output.put_line(si);
	CALL dbms_output.put_line(pls);
	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

-- free testing
create or replace function simpint_test() returns text as $$
declare
	bint binary_integer; -- must be a default
	f4 float4;
	f8 float8;
begin
	bint = 2147483647;
	bint = 2147483647 ^ 2;
	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

create table bi_t(s1 int, s2 binary_integer);
explain (costs off) select * from bi_t where s2 > 1.0;
explain (costs off) select * from bi_t where s2 > 1;
drop table bi_t;

-- test floor(pls_integer) function
declare
x pls_integer;
    y pls_integer;
    z int := 3;
begin
    x = 1234;
    y = floor(x/3);
    z = floor(z);
    raise notice '%', y;
    raise notice '%', z;
end;
/
-- make sure the floor(pls_integer) not cause not unique error
declare
sql text;
    source regtype;
begin
for source in select CASTSOURCE from pg_cast where CASTTARGET = 'PLS_INTEGER'::regtype loop
        sql = 'select floor(4::' || source::text || ')';
raise notice '%', sql;
        perform sql;
end loop;
end;
/
