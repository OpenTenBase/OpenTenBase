-- https://tapd.woa.com/20385652/bugtrace/bugs/view/1020385652119748173
DROP SCHEMA if exists chqin CASCADE;
DROP TABLE  if exists rqg_table5;
CREATE SCHEMA chqin;
CREATE TABLE rqg_table5 (c0 int,c1 int,c2 text,c3 text,c4 date,c5 date,c6 timestamp,c7 timestamp,c8 numeric,c9 numeric)   distribute by shard(c4);
alter table rqg_table5 alter column c0 drop not null;
CREATE INDEX idx_rqg_table5_c1 ON rqg_table5(c1);
CREATE INDEX idx_rqg_table5_c3 ON rqg_table5(c3);
CREATE INDEX idx_rqg_table5_c5 ON rqg_table5(c5);
CREATE INDEX idx_rqg_table5_c7 ON rqg_table5(c7);
CREATE INDEX idx_rqg_table5_c9 ON rqg_table5(c9);
INSERT INTO rqg_table5 VALUES  (7, 7, 'foo', 'foo', '1984-05-09 10:38:06', '2027-05-23', '1973-06-09 12:47:30.027560', NULL, -1.23456789123457e-09, 741081088) ,  (NULL, NULL, 'bar', 'kxch', '2034-07-02 09:04:19', '1998-10-11', '2023-08-10 23:42:17', '2026-08-15', 1.23456789123457e-09, -1.23456789123457e-09) ,  (7, 5, NULL, NULL, '2003-11-19 08:35:38', '2014-11-23 01:03:00', '2021-06-09 01:11:19.045011', '1989-01-08 14:42:11', -1.23456789123457e+39, NULL) ,  (5, 2, NULL, 'bar', '1997-11-06 21:08:46', '1971-10-05', '2012-07-15 09:39:01', NULL, 1.23456789123457e-09, 0.123456789123457) ,  (5, NULL, 'foo', 'xchabagu', '1999-01-10', '1993-11-13', '2026-11-07 10:13:13.019609', '2005-02-06', 1.23456789123457e-09, -1.23456789123457e+30) ,  (5, 3, NULL, 'chabagujufwtssmoylnnacqjrtqjknjeyggcbfejwpkwxllkkdxdbqyumzfgrdsoenpwqatgfksszrsrjsuomlbaataldafqyhjrmdthqhnhaadcthmufcjyxrqdnhsebcadp', '1993-07-25', '1971-11-15', NULL, '2035-10-10 08:05:13.053608', -1.23456789123457e+44, 0.597320556640625) ,  (4, 7, 'foo', 'habagujuf', '2011-06-09 17:03:12', '2007-11-16', NULL, '2025-07-26 14:17:48.042531', 1.23456789123457e-09, -6.9267272918486e+124) ,  (NULL, 4, NULL, NULL, '1986-05-04', '1979-01-20 00:33:56', '1990-08-23 04:40:55.040681', '1986-04-08 07:07:56.065405', -9.75570678686509e+124, -1.23456789123457e+25) ,  (0, 7, 'bar', NULL, '1986-01-19 09:45:34', '1994-10-22 14:25:37', '1983-05-11 17:15:57.002762', '2022-12-26 05:19:35.060918', 1.23456789123457e+43, 1.23456789123457e+30) ,  (NULL, 1, 'bar', 'bar', '2009-09-15 16:07:54', '1974-01-24', '1974-11-12 14:45:04.025025', NULL, 1.23456789123457e+39, 3.21979225859319e+18) ;
select c0 from rqg_table5 t1 where 1 = 2 union select t1.c0 from rqg_table5 t1 where t1.c0 = 2 AND exists (select 1 from rqg_table5 t2 where t2.c3 = t1.c3) group by t1.c0 except select sum(c0) c0 from rqg_table5 t1 where 1 = 2 ;
DROP TABLE  if exists rqg_table5;
DROP SCHEMA if exists chqin CASCADE;

--https://tapd.woa.com/OpenTenBase_C/bugtrace/bugs/view?bug_id=1020385652121956411&jump_count=1
DROP TABLE IF EXISTS a;
CREATE TABLE a(id int, s1 int not null);
SET TRANSFORM_INSERT_TO_COPY TO ON;
ALTER TABLE a ALTER COLUMN s1 ADD GENERATED ALWAYS AS IDENTITY;
INSERT INTO a VALUES(1),(2);
SET TRANSFORM_INSERT_TO_COPY TO OFF;
DROP TABLE IF EXISTS a;
