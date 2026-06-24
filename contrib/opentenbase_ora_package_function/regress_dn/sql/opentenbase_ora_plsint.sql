\c opentenbase_ora_package_function_regression_ora
/*
 * test cases for pls_integer and simple_integer types.
 * 1. syntaxer support
 *  1.1 pls_integer
 *  1.2 simple_integer
 * 2. assignment - pls_integer
 *  2.1 int2/4/8 var as source - over/under flow
 *  2.2 int2/4/8 var as target - over/under flow
 *  2.3 int2/4/8 const as target - over/under flow
 *  2.4 float4/8 var as source - over/under flow
 *  2.5 numeric/float4/8 var as target - over/under flow
 *  2.6 numeric/float4/8 const as target - over/under flow
 * 3. operator - pls_integer
 *  3.1 pls_integer/simple_integer/int2/4/8 + - * / neg(-) > < =
 *   3.1.1 overflow/underflow
 *  3.2 numeric/float4/8 + - * / > < =
 *   3.2.1 overflow/underflow
 *  3.3 lvalue is float8
 *  3.4 lvalue is pls_integer
 *  3.5 div by 0 and power
 * 4. assignment - simple_integer
 *  4.1 int2/4/8 var as source - over/under flow
 *  4.2 int2/4/8 var as target - over/under flow
 *  4.3 int2/4/8 const as target - over/under flow
 *  4.4 float4/8 var as source - over/under flow
 *  4.5 numeric/float4/8 var as target - over/under flow
 *  4.6 numeric/float4/8 const as target - over/under flow
 * 5. operator - simple_integer
 *  5.1 simple_integer/pls_integer/int2/4/8 + - * / neg(-) > < =
 *   5.1.1 overflow/underflow
 *  5.2 numeric/float4/8 + - * / > < =
 *   5.2.1 overflow/underflow
 *  5.3 lvalue is float8
 *  5.4 lvalue is simple_integer
 *  5.5 div by 0 and power
 */
-- 1. syntaxer support
--  1.1 pls_integer
create or replace function simpint_test() returns text as $$
declare
	pls pls_integer;
begin
	pls = 123;
	RETURN 'OK' || pls;
end;
$$ language default_plsql;
select simpint_test();

--  1.2 simple_integer
create or replace function simpint_test() returns text as $$
declare
	si simple_integer default 1;
begin
	si = 123;
	RETURN 'OK' || si;
end;
$$ language default_plsql;
select simpint_test();

-- 2. assignment - pls_integer
--  2.1 int2/4/8 var as source - over/under flow
create or replace function simpint_test() returns text as $$
declare
	pls pls_integer;
	i2 int2;
	i4 int4;
	i8 bigint;
begin
	i2 = 2;
	i4 = 4;
	i8 = 8;

	pls = i2;
	raise notice 'pls %',pls;

	pls = i4;
	raise notice 'pls %',pls;

	pls = i8;
	raise notice 'pls %',pls;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

-- over/under flow
-- boundary
create or replace function simpint_test() returns text as $$
declare
	pls pls_integer;
	i8u bigint;
	i8o bigint;
begin
	i8u = -2147483648;
	i8o = 2147483647;

	pls = i8u;
	raise notice 'pls %',pls;

	pls = i8o;
	raise notice 'pls %',pls;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

create or replace function simpint_test() returns text as $$
declare
	pls pls_integer;
	i8u bigint;
	i8o bigint;
begin
	i8u = -2147483649;
	i8o = 2147483648;

	pls = i8u;
	raise notice 'pls %',pls;

	pls = i8o;
	raise notice 'pls %',pls;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--  2.2 int2/4/8 var as target - over/under flow
create or replace function simpint_test() returns text as $$
declare
	pls pls_integer;
	i2 int2;
	i4 int4;
	i8 bigint;
begin
	pls = 12;
	i2 = pls;
	i4 = pls + 1;
	i8 = pls + 2;

	raise notice 'pls %',i2;
	raise notice 'pls %',i4;
	raise notice 'pls %',i8;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

create or replace function simpint_test() returns text as $$
declare
	pls pls_integer;
	i2 int2;
	i4 int4;
	i8 bigint;
begin
	pls = 2147483647;
	i2 = pls;
	i4 = pls + 1;
	i8 = pls + 2;

	raise notice 'pls %',i2;
	raise notice 'pls %',i4;
	raise notice 'pls %',i8;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--  2.3 int2/4/8 const as target - over/under flow
create or replace function simpint_test() returns text as $$
declare
	pls pls_integer;
begin
	pls = 2::int2;
	raise notice 'pls %',pls;

	pls = 4::int4;
	raise notice 'pls %',pls;

	pls = 8::int8;
	raise notice 'pls %',pls;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

-- overflow
create or replace function simpint_test() returns text as $$
declare
	pls pls_integer;
begin
	pls = -2147483649;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

create or replace function simpint_test() returns text as $$
declare
	pls pls_integer;
begin
	pls = 2147483648;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--  2.4 float4/8 var as source - over/under flow
create or replace function simpint_test() returns text as $$
declare
	pls pls_integer;
	f4 float4;
	f8 float8;
	n numeric;
begin
	f4 = 12.0;
	f8 = 14.0;
	n = 15;

	pls = f4;
	raise notice 'pls %',pls;

	pls = f8;
	raise notice 'pls %',pls;

	pls = n;
	raise notice 'pls %',pls;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

-- over/underflow
create or replace function simpint_test() returns text as $$
declare
	pls pls_integer;
	f8 float8;
begin
	f8 = -2147483649;

	pls = f8;
	raise notice 'pls %',pls;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--  2.5 numeric/float4/8 var as target - over/under flow
create or replace function simpint_test() returns text as $$
declare
	pls pls_integer;
	f4 float4;
	f8 float8;
	n numeric;
begin
	pls = 12;
	f4 = pls + 1.1;
	f8 = pls + 2.2;
	n = pls + 33.3;

	raise notice 'pls %',f4;
	raise notice 'pls %',f8;
	raise notice 'pls %',n;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--  2.6 numeric/float4/8 const as target - over/under flow
create or replace function simpint_test() returns text as $$
declare
	pls pls_integer;
begin
	pls = 12.0;
	raise notice 'pls %',pls;

	pls = 14.0;
	raise notice 'pls %',pls;

	pls = 15;
	raise notice 'pls %',pls;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

-- 3. operator - pls_integer
--  3.1 simple_integer/int2/4/8 + - * / neg(-) > < =
-- var
create or replace function simpint_test() returns text as $$
declare
	pls pls_integer;
	si simple_integer;
	i2 int2;
	i4 int4;
	i8 bigint;
begin
	pls = 12;
	si = 12;
	i2 = 2;
	i4 = 4;
	i8 = 8;

	pls = si + i2;
	raise notice 'pls %',pls;
	pls = si + i4;
	raise notice 'pls %',pls;
	pls = si * i8;
	raise notice 'pls %',pls;

	pls = pls + i2;
	raise notice 'pls %',pls;
	pls = pls + i4;
	raise notice 'pls %',pls;
	pls = pls * i8;
	raise notice 'pls %',pls;

	pls = -pls * i8;
	raise notice 'pls %',pls;

	if pls > 1 then
		raise notice 'pls > 1';
	else
		raise notice 'pls <= 1';
	end if;

	if pls > 1.1 then
		raise notice 'pls > 1';
	else
		raise notice 'pls <= 1';
	end if;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

-- const
create or replace function simpint_test() returns text as $$
declare
	pls pls_integer;
begin
	pls = 12;

	pls = pls + 2::int2;
	raise notice 'pls %',pls;
	pls = pls + 4::int4;
	raise notice 'pls %',pls;
	pls = pls * 8::int8;
	raise notice 'pls %',pls;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

-- pls_integer + - * / neg(-) > < =
create or replace function simpint_test() returns text as $$
declare
	pi1 pls_integer default 12;
	pi2 pls_integer default 4;
	pi3 pls_integer default 0;
	res bool default false;
begin
	pi3 = pi1 + pi2;
	raise notice 'pi %',pi3;
	
	pi3 = pi1 - pi2;
	raise notice 'pi %',pi3;
	
	pi3 = pi1 * pi2;
	raise notice 'pi %',pi3;
	
	pi3 = pi1 / pi2;
	raise notice 'pi %',pi3;
	
	pi3 = - pi1;
	raise notice 'pi %',pi3;
	
	pi3 = + pi1;
	raise notice 'pi %',pi3;
	
	res = pi1 > pi2;
	raise notice 'res %',res;
	
	res = pi1 < pi2;
	raise notice 'res %',res;
	
	res = pi1 >= pi2;
	raise notice 'res %',res;
	
	res = pi1 <= pi2;
	raise notice 'res %',res;
	
	res = (pi1 = pi2);
	raise notice 'res %',res;
	
	res = (pi1 <> pi2);
	raise notice 'res %',res;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--   3.1.1 overflow/underflow
declare
   pls pls_integer;
begin
   pls := 2147483647;
   pls := pls + 1::pls_integer;
end;
/

declare
   pls pls_integer;
begin
   pls := -2147483647;
   pls := pls - 2::pls_integer;
end;
/

declare
   pls pls_integer;
begin
   pls := 214748364;
   pls := pls * 11::pls_integer;
end;
/

declare
   pls pls_integer;
begin
   pls := -2147483648;
   pls := -pls;
end;
/

--  3.2 numeric/float4/8 + - * / > < =
-- var
create or replace function simpint_test() returns text as $$
declare
	pls pls_integer;
	f4 float4;
	f8 float8;
begin
	pls = 12;
	f4 = 4.4;
	f8 = 8.8;

	pls = pls + f4;
	raise notice 'pls %',pls;
	pls = pls + f8;
	raise notice 'pls %',pls;

	f4 = pls + f4;
	raise notice 'pls %',f4;
	f8 = pls + f8;
	raise notice 'pls %',f8;
	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--   3.2.1 overflow/underflow
-- tested
--  3.3 lvalue is float8
-- tested
--  3.4 lvalue is pls_integer
-- tested
--  3.5 div by 0 and power
create or replace function simpint_test() returns text as $$
declare
	pls pls_integer;
	f4 float4;
	f8 float8;
begin
	pls = 1;
	pls = pls / 0;
	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

-- 4. assignment - simple_integer
--  4.1 int2/4/8 var as source - over/under flow
create or replace function simpint_test() returns text as $$
declare
	si simple_integer default 0;
	i2 int2;
	i4 int4;
	i8 bigint;
begin
	i2 = 2;
	i4 = 4;
	i8 = 8;

	si = i2;
	raise notice 'si %',si;

	si = i4;
	raise notice 'si %',si;

	si = i8;
	raise notice 'si %',si;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

-- over/underflow
create or replace function simpint_test() returns text as $$
declare
	si simple_integer default 0;
	i8u bigint;
	i8o bigint;
begin
	i8u = -2147483648;
	i8o = 2147483647;

	si = i8u;
	raise notice 'si %',si;

	si = i8o;
	raise notice 'si %',si;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

create or replace function simpint_test() returns text as $$
declare
	si simple_integer default 0;
	i8u bigint;
	i8o bigint;
begin
	i8u = -21474836480;
	i8o = 21474836470;

	si = i8u;
	raise notice 'si %',si;

	si = i8o;
	raise notice 'si %',si;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--  4.2 int2/4/8 var as target - over/under flow
create or replace function simpint_test() returns text as $$
declare
	si simple_integer default 0;
	i2 int2;
	i4 int4;
	i8 bigint;
begin
	si = 10;
	i2 = si + 1;
	raise notice 'si %',i2;
	i4 = si + 2;
	raise notice 'si %',i4;
	i8 = si + 3;
	raise notice 'si %',i8;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--  4.3 int2/4/8 const as target - over/under flow
create or replace function simpint_test() returns text as $$
declare
	si simple_integer default 0;
begin
	si = 10::int2;
	raise notice 'si %',si;
	si = 14::int4;
	raise notice 'si %',si;
	si = 14::int8;
	raise notice 'si %',si;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--  4.4 float4/8 var as source - over/under flow
create or replace function simpint_test() returns text as $$
declare
	si simple_integer default 0;
	f4 float8;
	f8 float8;
	n numeric;
begin
	f4 = 4.4;
	f8 = 8.8;
	n = 10.10;

	-- as source 
	si = f4;
	raise notice 'si %',si;
	si = f8;
	raise notice 'si %',si;
	si = n;
	raise notice 'si %',si;

	-- as target
	f4 = si + 10;
	f8 = si + 10;
	n = si + 10;
	raise notice 'si %',f4;
	raise notice 'si %',f8;
	raise notice 'si %',n;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--  4.5 numeric/float4/8 var as target - over/under flow
-- tested above
-- over/under flow tested in bigint;
--  4.6 numeric/float4/8 const as target - over/under flow
create or replace function simpint_test() returns text as $$
declare
	si simple_integer default 0;
begin
	-- as source 
	si = 4.4::float4;
	raise notice 'si %',si;
	si = 8.8::float8;
	raise notice 'si %',si;
	si = 9.0::numeric;
	raise notice 'si %',si;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

-- importance
-- 5. operator - simple_integer
-- 5.1 simple_integer/pls_integer/int2/4/8 + - * / neg(-) > < =
create or replace function simpint_test() returns text as $$
declare
	si simple_integer default 0;
	pls pls_integer;
	i2 int2;
	i4 int4;
	i8 bigint;
begin
	pls = 12;
	i2 = 2;
	i4 = 4;
	i8 = 8;

	si = 2 * 4 * 8;

	si = si + pls;
	si = si - pls;
	si = si * pls;
	si = si / pls;

	si = si + i2;
	si = si - i2;
	si = si * i2;
	si = si / i2;

	si = si + i4;
	si = si - i4;
	si = si * i4;
	si = si / i4;

	si = si + i8;
	si = si - i8;
	si = si * i8;
	si = si / i8;

	raise notice 'si % %',si, -si;

	if si <> pls or si = pls or si > i2 or si < i4 or si > i8  or si = i8+i4 or si >= i2+i8 then
		raise notice 'cond11';
	else
		raise notice 'cond2';
	end if;
	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

-- simple_integer + - * / neg(-) > < =
create or replace function simpint_test() returns text as $$
declare
	si1 simple_integer default 12;
	si2 simple_integer default 4;
	si3 simple_integer default 0;
	res bool default false;
begin
	si3 = si1 + si2;
	raise notice 'si %',si3;
	
	si3 = si1 - si2;
	raise notice 'si %',si3;
	
	si3 = si1 * si2;
	raise notice 'si %',si3;
	
	si3 = si1 / si2;
	raise notice 'si %',si3;
	
	si3 = - si1;
	raise notice 'si %',si3;
	
	si3 = + si1;
	raise notice 'si %',si3;
	
	res = si1 > si2;
	raise notice 'res %',res;
	
	res = si1 < si2;
	raise notice 'res %',res;
	
	res = si1 >= si2;
	raise notice 'res %',res;
	
	res = si1 <= si2;
	raise notice 'res %',res;
	
	res = (si1 = si2);
	raise notice 'res %',res;
	
	res = (si1 <> si2);
	raise notice 'res %',res;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--   5.1.1 overflow/underflow
create or replace function simpint_test() returns text as $$
declare
	si simple_integer default 0;
	i2 int2;
	i4 int4;
	i8 bigint;
	ex text;
begin
	-- overflow
	si = 2147483647;
	i2 = 1;
	si = si + i2;
	raise notice 'si %',si;

	si = 2147483647;
	i4 = 1;
	si = si + i4;
	raise notice 'si %',si;

	begin
		si = 2147483647;
		i8 = 1;
		si = si + i8;
		raise notice 'si %',si;
	exception when others then
		get stacked diagnostics ex=MESSAGE_TEXT;
		raise notice 'ex: %',ex;
	end;

	-- underflow
	si = -2147483648;
	i2 = 1;
	si = si - i2;
	raise notice 'si %',si;

	si = -2147483648;
	i4 = 1;
	si = si - i4;
	raise notice 'si %',si;

	si = -2147483648;
	si = -si;
	raise notice 'neg -2147483648 si %',si;
	begin
		si = -2147483648;
		i8 = 1;
		si = si - i8;
		raise notice 'si %',si;
	exception when others then
		get stacked diagnostics ex=MESSAGE_TEXT;
		raise notice 'ex: %',ex;
	end;
	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

create or replace function simpint_test() returns text as $$
declare
	si1 simple_integer default 2147483647;
	si2 simple_integer default 0;
	si3 simple_integer default 0;
begin
	-- overflow
	si2 = 6;
	si3 = si1 + si2;
	raise notice 'si %',si3;
	
	-- underflow
	si3 = -si1-si2;
	raise notice 'si %',si3;
	
	-- overflow
	si3 = si1 * si2;
	raise notice 'si %',si3;

	-- underflow
	si3 = -si1 * si2;
	raise notice 'si %',si3;
	
	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

declare
   a simple_integer default 0;
   b pls_integer;
begin
	a := 2147483647;
	b := 2;
	raise notice '%',a+b;
end;
/

declare
   a simple_integer default 0;
   b pls_integer;
begin
	a := -2147483647;
	b := 2;
	raise notice '%',a-b;
end;
/

declare
   a simple_integer default 0;
   b pls_integer;
begin
	a := 2147483647;
	b := 2;
	raise notice '%',a*b;
end;
/

declare
   a simple_integer default 0;
   b pls_integer;
begin
	a := 2147483647;
	b := 2;
	raise notice '%',a/b;
end;
/

--  5.2 numeric/float4/8 + - * / > < =
create or replace function simpint_test() returns text as $$
declare
	si simple_integer default 0;
	i4 float4;
	i8 float8;
begin
	i4 = 4.2;
	i8 = 8.1;

	si = 2 * 4 * 8;

	si = si + i4;
	si = si - i4;
	si = si * i4;
	si = si / i4;

	si = si + i8;
	si = si - i8;
	si = si * i8;
	si = si / i8;

	raise notice 'si % %',si, -si;

	if si < i4 or si > i8  or si = i8+i4 then
		raise notice 'cond11';
	else
		raise notice 'cond2';
	end if;
	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--   5.2.1 overflow/underflow
create or replace function simpint_test() returns text as $$
declare
	si simple_integer default 0;
	i4 float4;
	i8 float8;
	ex text;
begin
	-- overflow
	begin
		si = 2147483647;
		si = si + 1.1;
		raise notice 'si %',si;
	exception when others then
		get stacked diagnostics ex=MESSAGE_TEXT;
		raise notice 'ex: %',ex;
	end;

	begin
		si = 2147483647;
		i4 = 1.1;
		si = si + i4;
		raise notice 'si %',si;
	exception when others then
		get stacked diagnostics ex=MESSAGE_TEXT;
		raise notice 'ex: %',ex;
	end;

	begin
		si = 2147483647;
		i8 = 1;
		si = si + i8;
		raise notice 'si %',si;
	exception when others then
		get stacked diagnostics ex=MESSAGE_TEXT;
		raise notice 'ex: %',ex;
	end;

	-- underflow
	begin
		si = -2147483648;
		si = si - 1.1;
		raise notice 'si %',si;
	exception when others then
		get stacked diagnostics ex=MESSAGE_TEXT;
		raise notice 'ex: %',ex;
	end;

	begin
		si = -2147483648;
		i4 = 1.1;
		si = si - i4;
		raise notice 'si %',si;
	exception when others then
		get stacked diagnostics ex=MESSAGE_TEXT;
		raise notice 'ex: %',ex;
	end;

	begin
		si = -2147483648;
		i8 = 1;
		si = si - i8;
		raise notice 'si %',si;
	exception when others then
		get stacked diagnostics ex=MESSAGE_TEXT;
		raise notice 'ex: %',ex;
	end;
	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--  5.3 lvalue is float8 -- no overflow
create or replace function simpint_test() returns text as $$
declare
	si float8;
	i4 float4;
	i8 float8;
	ex text;
begin
	-- overflow
	begin
		si = 2147483647;
		si = si + 1.1;
		raise notice 'si %',si;
	exception when others then
		get stacked diagnostics ex=MESSAGE_TEXT;
		raise notice 'ex: %',ex;
	end;

	begin
		si = 2147483647;
		i4 = 1.1;
		si = si + i4;
		raise notice 'si %',si;
	exception when others then
		get stacked diagnostics ex=MESSAGE_TEXT;
		raise notice 'ex: %',ex;
	end;

	begin
		si = 2147483647;
		i8 = 1;
		si = si + i8;
		raise notice 'si %',si;
	exception when others then
		get stacked diagnostics ex=MESSAGE_TEXT;
		raise notice 'ex: %',ex;
	end;

	-- underflow
	begin
		si = -2147483648;
		si = si - 1.1;
		raise notice 'si %',si;
	exception when others then
		get stacked diagnostics ex=MESSAGE_TEXT;
		raise notice 'ex: %',ex;
	end;

	begin
		si = -2147483648;
		i4 = 1.1;
		si = si - i4;
		raise notice 'si %',si;
	exception when others then
		get stacked diagnostics ex=MESSAGE_TEXT;
		raise notice 'ex: %',ex;
	end;

	begin
		si = -2147483648;
		i8 = 1;
		si = si - i8;
		raise notice 'si %',si;
	exception when others then
		get stacked diagnostics ex=MESSAGE_TEXT;
		raise notice 'ex: %',ex;
	end;
	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

create or replace function simpint_test() returns text as $$
declare
	si simple_integer default 0;
	f4 float4;
	f8 float8;
begin
	si = 2147483647;
	si = si + 1;
	raise notice 'si %',si;
	si = si + 1;
	raise notice 'si %',si;
	si = si + 1;
	raise notice 'si %',si;

	si = 2147483647;
	si = si + 3;
	raise notice 'si %',si;

	si = -2147483648;
	si = si - 1;
	raise notice 'si %',si;
	si = si - 1;
	raise notice 'si %',si;
	si = si - 1;
	raise notice 'si %',si;

	si = -2147483648;
	si = si - 3;
	raise notice 'si %',si;

	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

--  5.4 lvalue is simple_integer
-- tested above
--  5.5 div by 0 and power
create or replace function simpint_test() returns text as $$
declare
	pls simple_integer default 0;
	f4 float4;
	f8 float8;
begin
	pls = 1;
	pls = pls / 0;
	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

create or replace function simpint_test() returns text as $$
declare
	pls simple_integer default 0;
	f4 float4;
	f8 float8;
begin
	pls = 2147483647;
	pls = 2147483647 ^ 2;
	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

-- free testing
create or replace function simpint_test() returns text as $$
declare
	pls simple_integer; -- must be a default
	f4 float4;
	f8 float8;
begin
	pls = 2147483647;
	pls = 2147483647 ^ 2;
	RETURN 'OK';
end;
$$ language default_plsql;
select simpint_test();

create table si_t(s1 int, s2 simple_integer);
explain (costs off) select * from si_t where s2 > 1.0;
explain (costs off) select * from si_t where s2 > 1;
drop table si_t;

-- test drop procedure which param is plsint type

drop package pkg1;
create or replace package pkg1 is
  TYPE sum_multiples IS TABLE OF PLS_INTEGER INDEX BY PLS_INTEGER;
  n  PLS_INTEGER := 5;   -- number of multiples to sum for display
  sn PLS_INTEGER := 10;  -- number of multiples to sum
  m  PLS_INTEGER := 3;   -- multiple
end pkg1;
/

create or replace procedure get_sum_multiples (
    multiple IN PLS_INTEGER,
    num      IN PLS_INTEGER,
    v1       out pkg1.sum_multiples
  )
IS
    s pkg1.sum_multiples;
BEGIN
    FOR i IN 1..num LOOP
      s(i) := multiple * ((i * (i + 1)) / 2);  -- sum of multiples
    END LOOP;
     v1:=s;
END get_sum_multiples;
/

declare
  v pkg1.sum_multiples;
  i PLS_INTEGER;
begin
  get_sum_multiples (pkg1.m, pkg1.sn,v);
  raise notice 'Sum of the first % multiples of % is %',pkg1.n,pkg1.m, v(pkg1.n);
  i := v.LAST;
  WHILE i IS NOT NULL LOOP
    raise notice 'Loop[%] is %',i, v(i);
    i := v.PRIOR(i);
  END LOOP;

 raise notice '%', pkg1.n;
end;
/

drop procedure if exists get_sum_multiples(PLS_INTEGER,PLS_INTEGER,pkg1.sum_multiples);
drop package pkg1;
