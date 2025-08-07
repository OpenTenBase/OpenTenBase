\c regression_ora

CREATE TABLE t_econtext_32(c1 int, c2 int);
INSERT INTO t_econtext_32 VALUES (1, 10);

-- 1
CREATE or REPLACE PROCEDURE p_econtext32_01()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_02();
EXCEPTION WHEN OTHERS THEN
	raise notice '%', SQLERRM;
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_02()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_03();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_03()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_04();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_04()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_05();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_05()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_06();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_06()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_07();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_07()
AS $$
DECLARE
	i int;
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	i := 1 / 0;
END; $$;

CALL p_econtext32_01;

-- 2
CREATE or REPLACE PROCEDURE p_econtext32_01()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_02();
EXCEPTION WHEN OTHERS THEN
	raise notice '%', SQLERRM;
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_02()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_03();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_03()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_04();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_04()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_05();
EXCEPTION WHEN OTHERS THEN
	raise notice '%', SQLERRM;
	update t_econtext_32 set c2 = 100 where c1 = 1;
	RAISE division_by_zero;
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_05()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_06();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_06()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_07();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_07()
AS $$
DECLARE
	i int;
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	i := 1 / 0;
END; $$;

CALL p_econtext32_01;

-- 3
CREATE or REPLACE PROCEDURE p_econtext32_01()
AS $$
DECLARE
	i int;
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_02();
EXCEPTION WHEN OTHERS THEN
	rollback;
	i := 1;
	raise notice '%', SQLERRM;
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_02()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_03();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_03()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_04();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_04()
AS $$
DECLARE
	i int;
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_05();
EXCEPTION WHEN OTHERS THEN
	raise notice '%', SQLERRM;
	i := 3;
	commit;
	i := 2;
	update t_econtext_32 set c2 = 100 where c1 = 1;
	rollback;
	i := 1;
	RAISE division_by_zero;
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_05()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_06();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_06()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_07();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_07()
AS $$
DECLARE
	i int;
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	commit;
	i := 1 / 0;
END; $$;

CALL p_econtext32_01;

-- 4
CREATE or REPLACE PROCEDURE p_econtext32_01()
AS $$
DECLARE
	i int;
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_02();
EXCEPTION WHEN OTHERS THEN
	rollback;
	i := 1;
	raise notice '%', SQLERRM;
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_02()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_03();
	raise notice '02 live';
	RAISE division_by_zero;
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_03()
AS $$
DECLARE
	i int;
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_04();
	raise notice '03 live';
	rollback;
	i := 1 / 0;
EXCEPTION WHEN OTHERS THEN
	i := 1;
	rollback;
	i := 1;
	commit;
	i := 1;
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_04()
AS $$
DECLARE
	i int;
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_05();
EXCEPTION WHEN OTHERS THEN
	raise notice '%', SQLERRM;
	i := 3;
	commit;
	i := 2;
	update t_econtext_32 set c2 = 100 where c1 = 1;
	rollback;
	i := 1;
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_05()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_06();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_06()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_07();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_07()
AS $$
DECLARE
	i int;
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	commit;
	i := 1 / 0;
END; $$;

CALL p_econtext32_01;

-- 5
CREATE or REPLACE PROCEDURE p_econtext32_01()
AS $$
DECLARE
	i int;
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_02();
EXCEPTION WHEN OTHERS THEN
	rollback;
	i := 1;
	raise notice '%', SQLERRM;
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_02()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_03();
	raise notice '02 live';
	RAISE division_by_zero;
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_03()
AS $$
DECLARE
	i int;
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_04();
	raise notice '03 live';
	rollback;
	i := 1 / 0;
EXCEPTION WHEN OTHERS THEN
	i := 1;
	rollback;
	i := 1;
	commit;
	i := 1;
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_04()
AS $$
DECLARE
	i int;
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_05();
EXCEPTION WHEN OTHERS THEN
	rollback;
	raise notice '%', SQLERRM;
	i := 3;
	commit;
	i := 2;
	update t_econtext_32 set c2 = 100 where c1 = 1;
	rollback;
	i := 1;
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_05()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_06();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_06()
AS $$
DECLARE
	i int;
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_07();
	raise notice '06 live';
	rollback;
	update t_econtext_32 set c2 = 100 where c1 = 1;
	rollback;
	commit;
	i := 1 / 0;
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_07()
AS $$
DECLARE
	i int;
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	commit;
	i := 1 / 0;
EXCEPTION WHEN OTHERS THEN
	i := 1;
END; $$;

CALL p_econtext32_01;

-- 6
CREATE or REPLACE PROCEDURE p_econtext32_01()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_02();
EXCEPTION WHEN OTHERS THEN
	raise notice '%', SQLERRM;
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_02()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_03();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_03()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_04();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_04()
AS $$
DECLARE
	rec RECORD;
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_05();
EXCEPTION WHEN OTHERS THEN
	raise notice '%', SQLERRM;
	update t_econtext_32 set c2 = 100 where c1 = 1;
	-- RAISE division_by_zero;
	FOR rec IN SELECT t.i as a FROM generate_series(1,10) t(i)
	LOOP
		IF rec.a = 10 THEN
			RAISE feature_not_supported;
		END IF;
	END LOOP;
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_05()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_06();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_06()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_07();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_07()
AS $$
DECLARE
	i int;
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	i := 1 / 0;
END; $$;

CALL p_econtext32_01;

-- 7
CREATE or REPLACE PROCEDURE p_econtext32_01()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_02();
EXCEPTION WHEN OTHERS THEN
	raise notice '%', SQLERRM;
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_02()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_03();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_03()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_04();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_04()
AS $$
DECLARE
	rec RECORD;
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_05();
EXCEPTION WHEN OTHERS THEN
	raise notice '%', SQLERRM;
	update t_econtext_32 set c2 = 100 where c1 = 1;
	-- RAISE division_by_zero;
	FOR rec IN SELECT t.i as a FROM generate_series(1,10) t(i)
	LOOP
		IF rec.a % 2 = 1 THEN
			commit;
		ELSE
			-- rollback;
		END IF;
		IF rec.a = 10 THEN
			RAISE feature_not_supported;
		END IF;
	END LOOP;
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_05()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_06();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_06()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_07();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_07()
AS $$
DECLARE
	i int;
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	i := 1 / 0;
END; $$;

CALL p_econtext32_01;

-- 8
CREATE or REPLACE PROCEDURE p_econtext32_01()
AS $$
DECLARE
	a varchar;
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_02();
EXCEPTION WHEN OTHERS THEN
	rollback;
	a := 'sss';
	rollback;
	raise notice '%', SQLERRM;
	rollback;
	a := 'sss';
	rollback;
	a := 'sss';
	commit;
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_02()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_03();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_03()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_04();
END; $$;

CREATE or REPLACE PROCEDURE p_econtext32_04()
AS $$
DECLARE
	rec RECORD;
	a varchar;
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_05();
	update t_econtext_32 set c2 = 100 where c1 = 1;
	-- RAISE division_by_zero;
	FOR rec IN SELECT t.i as a FROM generate_series(1,10) t(i)
	LOOP
		IF rec.a % 2 = 1 THEN
			commit;
		ELSE
			-- will coredump https://tapd.woa.com/20385652/prong/stories/view/1020385652116707362
			-- rollback;
		END IF;
		IF rec.a = 10 THEN
			RAISE feature_not_supported;
		END IF;
	END LOOP;
EXCEPTION WHEN OTHERS THEN
	a := 'sss';
	rollback;
	a := 'sss';
	raise notice '%', SQLERRM;
	commit;
	a := 'sss';
	a := 1 / 0;
END; $$;

CREATE or REPLACE PROCEDURE p_econtext32_05()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_06();
EXCEPTION WHEN OTHERS THEN
	rollback;
	raise notice '%', SQLERRM;
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_06()
AS $$
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	call p_econtext32_07();
END; $$;
CREATE or REPLACE PROCEDURE p_econtext32_07()
AS $$
DECLARE
	i int;
BEGIN
	update t_econtext_32 set c2 = 100 where c1 = 1;
	i := 1 / 0;
END; $$;

CALL p_econtext32_01;
