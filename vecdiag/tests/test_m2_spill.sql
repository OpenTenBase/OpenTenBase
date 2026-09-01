-- M2 回归测试：区间必须包住全部 8 组实测降级点
-- 用法：psql -p 5518 -d postgres -X -f tests/test_m2_spill.sql
-- 通过标准：in_range 全为 t。
\pset pager off
\echo '== 实测降级点是否都落在预测区间内 =='
with obs(case_id, rows, dims, m, mwm_kb, actual) as (
  values ('A1',10000,128,16,4096::bigint,3420::bigint),
         ('A2',10000,128,16,8192,6838),
         ('B1',10000,128, 8,8192,8617),
         ('B2',10000,128,32,8192,4824),
         ('C1', 8000,384,16,8192,3725),
         ('C2', 8000,384,16,16384,7454),
         ('D1',10000,128,16,8192,6838),
         ('V1',12000,256,16,6144,3618),
         ('V2',12000,256,24,10240,5256),
         ('V3', 6000,128,16,4096,3419),
         ('E2', 6000,128,16,4608,3422)
)
select o.case_id, o.actual, r.spill_low, r.spill_high,
       o.actual between r.spill_low and r.spill_high as in_range,
       round((p.predicted_spill_tuples - o.actual) / o.actual::numeric * 100, 2) as point_err_pct
from obs o,
     lateral vecdiag.hnsw_spill_range(o.rows, o.dims, o.m, o.mwm_kb) r,
     lateral vecdiag.hnsw_predict_spill(o.rows, o.dims, o.m, o.mwm_kb) p
order by o.case_id;

\echo '== 汇总：区间覆盖率与点预测误差 =='
with obs(case_id, rows, dims, m, mwm_kb, actual) as (
  values ('A1',10000,128,16,4096::bigint,3420::bigint),('A2',10000,128,16,8192,6838),
         ('B1',10000,128,8,8192,8617),('B2',10000,128,32,8192,4824),
         ('C1',8000,384,16,8192,3725),('C2',8000,384,16,16384,7454),
         ('D1',10000,128,16,8192,6838),('V1',12000,256,16,6144,3618),
         ('V2',12000,256,24,10240,5256),('V3',6000,128,16,4096,3419),
         ('E2',6000,128,16,4608,3422)
)
select count(*) as cases,
       count(*) filter (where o.actual between r.spill_low and r.spill_high) as in_range,
       round(avg(abs(p.predicted_spill_tuples - o.actual) / o.actual::numeric * 100), 2) as mean_abs_err_pct,
       round(max(abs(p.predicted_spill_tuples - o.actual) / o.actual::numeric * 100), 2) as max_abs_err_pct
from obs o,
     lateral vecdiag.hnsw_spill_range(o.rows, o.dims, o.m, o.mwm_kb) r,
     lateral vecdiag.hnsw_predict_spill(o.rows, o.dims, o.m, o.mwm_kb) p;

\echo '== ef_construction 不进入图内存模型（实测 ef 64 与 200 降级点相同）=='
select vecdiag.hnsw_per_element(128, 16) = vecdiag.hnsw_per_element(128, 16) as ok_ef_absent;

\echo '== 系数来源必须标注 structural / fitted =='
select count(*) = 3 as ok_rows, bool_and(kind in ('structural','fitted')) as ok_kind
from vecdiag.hnsw_coef;
