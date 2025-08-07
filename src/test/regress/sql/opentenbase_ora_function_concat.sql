\c regression_ora
CREATE EXTENSION IF NOT EXISTS opentenbase_ora_package_function;
create table tbl_test_listagg(cf1 int, cf2 int,  cf3 varchar(32), cf4 varchar(32), cf5 int, cf6 numeric(8,2)) distribute by replication;;
insert into tbl_test_listagg(cf1, cf2,  cf3, cf4, cf5, cf6) values(1,500, 'China','Guangzhou', 23000, 1981.12);
insert into tbl_test_listagg(cf1, cf2,  cf3, cf4, cf5, cf6) values(2,1500, 'China', 'Shanghai', 29000, 3094.54);
insert into tbl_test_listagg(cf1, cf2,  cf3, cf4, cf5, cf6) values(3,1500, 'China', 'Beijing', 25000, 2984.83);
insert into tbl_test_listagg(cf1, cf2,  cf3, cf4, cf5, cf6) values(4,1000, 'China', 'Shenzhen', 24000, 2172.18);
insert into tbl_test_listagg(cf1, cf2,  cf3, cf4, cf5, cf6) values(5,1000,'USA','New York', 35000, 10893.39);
insert into tbl_test_listagg(cf1, cf2,  cf3, cf4, cf5, cf6) values(6,500, 'USA', 'Bostom', 15000, 1349.78);
insert into tbl_test_listagg(cf1, cf2,  cf3, cf4, cf5, cf6) values(7,500, 'Japan','Tokyo', 40000, 12978.37);
insert into tbl_test_listagg(cf1, cf2,  cf3, cf4, cf5, cf6) values(8,800, 'China', 'Hongkong', 23500, 3921.56);
insert into tbl_test_listagg(cf1, cf2,  cf3, cf4, cf5, cf6) values(9,800, 'China', 'Hangzhou', 15500, 1001.09);
insert into tbl_test_listagg(cf1, cf2,  cf3, cf4, cf5, cf6) values(10,100, 'USA', 'Los Angele', 15500, 5009.43);

select listagg(cf4) WITHIN GROUP (ORDER BY cf4) from tbl_test_listagg;

select listagg(cf4,',') WITHIN GROUP (ORDER BY cf4) from tbl_test_listagg;

select listagg(cf4,',') WITHIN GROUP (ORDER BY cf1) from tbl_test_listagg;

select cf3, listagg (cf4,',') WITHIN GROUP (ORDER BY cf4) from tbl_test_listagg group by cf3 ;

select cf3, listagg(cf4,',') WITHIN GROUP (ORDER BY cf1 desc) cf4, listagg(cf5::text,'+') WITHIN GROUP (ORDER BY cf1) cf5 from tbl_test_listagg group by cf3 ;

select listagg(cf2) WITHIN GROUP (ORDER BY cf4) from tbl_test_listagg;

select listagg(cf2,',') WITHIN GROUP (ORDER BY cf4) from tbl_test_listagg;

select listagg(cf2,',') WITHIN GROUP (ORDER BY cf1) from tbl_test_listagg;

select cf3, listagg (cf2,',') WITHIN GROUP (ORDER BY cf4) from tbl_test_listagg group by cf3 ;

select cf3, listagg(cf4,',') WITHIN GROUP (ORDER BY cf1 desc) cf4, listagg(cf5,'+') WITHIN GROUP (ORDER BY cf1) cf5 from tbl_test_listagg group by cf3 ;

select listagg(cf6) WITHIN GROUP (ORDER BY cf4) from tbl_test_listagg;

select listagg(cf6,',') WITHIN GROUP (ORDER BY cf4) from tbl_test_listagg;

select listagg(cf6,',') WITHIN GROUP (ORDER BY cf1) from tbl_test_listagg;

select cf3, listagg (cf6,',') WITHIN GROUP (ORDER BY cf4) from tbl_test_listagg group by cf3 ;

select cf3, listagg(cf4,',') WITHIN GROUP (ORDER BY cf1 desc) cf4, listagg(cf5,'+') WITHIN GROUP (ORDER BY cf1) cf5, listagg(cf6,'+') WITHIN GROUP (ORDER BY cf1) cf6 from tbl_test_listagg group by cf3 ;

select wmsys.wm_concat(cf1), cf2 from tbl_test_listagg group by 2 order by 2,1;
select wmsys.wm_concat(cf3), cf2 from tbl_test_listagg group by 2 order by 2,1;
select wmsys.wm_concat(cf6), cf2 from tbl_test_listagg group by 2 order by 2,1;
select wmsys.wm_concat(cf1) over (partition by cf1), cf2 from tbl_test_listagg group by cf1, cf2 order by 1,2;

drop table if exists tbl_test_listagg;
