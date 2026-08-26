--
-- PostgreSQL database dump
--

\restrict CTwzwJ0kadvZaNXaaz57pIsCAoTJKZ677hxIvEk9e4cJLCCVyo430BYnHT6bIRP

-- Dumped from database version 18.6
-- Dumped by pg_dump version 18.6

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: vecdiag; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA vecdiag;


ALTER SCHEMA vecdiag OWNER TO postgres;

--
-- Name: SCHEMA vecdiag; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA vecdiag IS 'IVFFlat/HNSW 构建期内存与耗时诊断。外部 schema，不修改 pgvector 与 PostgreSQL 本体。';


--
-- Name: checkpoint_kind; Type: TYPE; Schema: vecdiag; Owner: postgres
--

CREATE TYPE vecdiag.checkpoint_kind AS ENUM (
    'C1',
    'C2',
    'C3',
    'none'
);


ALTER TYPE vecdiag.checkpoint_kind OWNER TO postgres;

--
-- Name: abi(text); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.abi(p_key text) RETURNS bigint
    LANGUAGE sql STABLE STRICT
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
  select value from vecdiag.abi_const where key = p_key;
$$;


ALTER FUNCTION vecdiag.abi(p_key text) OWNER TO postgres;

--
-- Name: FUNCTION abi(p_key text); Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON FUNCTION vecdiag.abi(p_key text) IS 'ABI 常数取值。缺键返回 NULL，调用方必须处理。';


--
-- Name: allocset_capacity_floor(bigint); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.allocset_capacity_floor(p_bytes bigint) RETURNS bigint
    LANGUAGE sql IMMUTABLE STRICT
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
  -- 块从 8 kB 起倍增到 8 MB 封顶；封顶后按 8 MB 一块继续加。
  -- 倍增段累计和 = 8K·(2^(i+1)−1)，i=0..10（2^10·8K = 8 MB）。
  with s as (
    select 8192::bigint * (power(2, i + 1)::bigint - 1) as cum
    from generate_series(0, 10) as g(i)
  ),
  doubling as (
    select coalesce(max(cum), 0) as cum_le, (select max(cum) from s) as cum_max
    from s where cum <= p_bytes
  )
  select case
           when cum_le < cum_max then cum_le            -- 还在倍增段
           else cum_max + ((p_bytes - cum_max) / (8 * 1024 * 1024)) * (8 * 1024 * 1024)
         end
  from doubling;
$$;


ALTER FUNCTION vecdiag.allocset_capacity_floor(p_bytes bigint) OWNER TO postgres;

--
-- Name: FUNCTION allocset_capacity_floor(p_bytes bigint); Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON FUNCTION vecdiag.allocset_capacity_floor(p_bytes bigint) IS 'AllocSet 块倍增（8kB→8MB 封顶，之后每块 8MB）造成的有效容量台阶下界。';


--
-- Name: current_mwm_kb(); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.current_mwm_kb() RETURNS bigint
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
  select vecdiag.parse_mem_kb(current_setting('maintenance_work_mem'));
$$;


ALTER FUNCTION vecdiag.current_mwm_kb() OWNER TO postgres;

--
-- Name: diagnose(); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.diagnose() RETURNS TABLE(severity text, object text, problem text, cause text, fix text, verify text)
    LANGUAGE plpgsql STABLE
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
declare
    r          record;
    v_mwm_kb   bigint := vecdiag.current_mwm_kb();
    v_measured int;
begin
    -- ---------------- 前提检查：本机是否做过 ABI 实测 ----------------
    select count(*) into v_measured from vecdiag.abi_const where source = 'measured';
    if v_measured = 0 then
        return query select
          'warn',
          'vecdiag.abi_const',
          '本机没有任何实测得到的 ABI 常数，下面所有内存预测都只是按源码值推算',
          'ABI 常数（MAXALIGN(itemsize)、MaxHeapTuplesPerPage）与机器、编译器、BLCKSZ 绑定；'
          || '表里当前全部是 source=''source-code'' 的值',
          '跑 tools/abi_probe.sh 实测一遍，把结果写回 vecdiag.abi_const 并把 source 改成 measured',
          '实测后 dims=128 的 MAXALIGN(itemsize) 应为 520；再用 tools/checkpoint_verify.sh 确认'
          || '预测值与报错原文逐字相等';
    end if;

    -- ---------------- 逐个向量列体检 ----------------
    for r in
        select c.oid, c.relname, n.nspname, a.attname, a.atttypmod as dims,
               c.relpages, c.reltuples::bigint as reltuples, c.relpersistence
        from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        join pg_attribute a on a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped
        join pg_type t on t.oid = a.atttypid
        where c.relkind = 'r' and t.typname = 'vector'
          and n.nspname not in ('pg_catalog', 'information_schema', 'pg_toast')
        order by c.relpages desc, c.relname
    loop
        -- 统计信息过期：预测的主导项直接依赖行数与页数
        if r.relpages = 0 or r.reltuples <= 0 then
            return query select
              'warn',
              format('%s.%s(%s)', r.nspname, r.relname, r.attname),
              '统计信息缺失或过期，无法给出可信的构建内存预测',
              format('pg_class 里 relpages=%s、reltuples=%s；numSamples 的上限是 '
                     'relpages × MaxHeapTuplesPerPage，页数不准会让预测偏一个数量级',
                     r.relpages, r.reltuples),
              format('先执行 ANALYZE %I.%I;', r.nspname, r.relname),
              'ANALYZE 后重跑 vecdiag.diagnose()，本条应当消失';
            continue;
        end if;

        -- IVFFlat 可行性：按 pgvector 文档的经验取 lists = rows/1000（下限 1）
        declare
            v_lists int := greatest((r.reltuples / 1000)::int, 1);
            p       record;
        begin
            select * into p from vecdiag.ivfflat_predict(
                     r.reltuples, r.dims, v_lists, r.relpages, false, v_mwm_kb);
            if p.first_hit <> 'none' then
                return query select
                  'error',
                  format('%s.%s(%s)', r.nspname, r.relname, r.attname),
                  format('按 lists=%s 建 IVFFlat 会在 %s 检查点失败，报错数字会是 %s MB',
                         v_lists, p.first_hit, p.predicted_mb),
                  format('该检查点的累积需求 %s 字节 超过 maintenance_work_mem=%s kB。'
                         'C1 只含 centers、C2 再加 samples、C3 再加 k-means 九项，'
                         '第一个越界的检查点决定报错数字（ivfbuild.c:394/459、ivfkmeans.c:290）',
                         case p.first_hit when 'C1' then p.c1_bytes
                                          when 'C2' then p.c2_bytes
                                          else p.c3_bytes end, p.mwm_kb),
                  format('执行 SET maintenance_work_mem = ''%skB''; 再建索引；'
                         '或把 lists 降到 %s 以下重建', (p.c3_bytes / 1024) + 1,
                         greatest(v_lists / 2, 1)),
                  format('SET maintenance_work_mem=''%skB''; 再建索引应当不再报错；'
                         '或先用 select * from vecdiag.ivfflat_predict(%s,%s,%s,%s) 复算',
                         (p.c3_bytes / 1024) + 1, r.reltuples, r.dims, v_lists, r.relpages);
            end if;
        end;

        -- HNSW 落盘降级风险
        declare
            h record;
        begin
            select * into h from vecdiag.hnsw_predict_spill(r.reltuples, r.dims, 16, v_mwm_kb);
            if h.will_spill then
                return query select
                  'warn',
                  format('%s.%s(%s)', r.nspname, r.relname, r.attname),
                  format('按 m=16 建 HNSW 预计在第 %s 行左右发生落盘降级（图放不下内存）',
                         h.predicted_spill_tuples),
                  format('预计图内存 %s MB 超过 maintenance_work_mem=%s kB。'
                         '上游此时只打一个 NOTICE 然后转磁盘继续建（hnswbuild.c:530-549），'
                         '**构建不会失败，只会显著变慢**，所以很容易被忽略',
                         h.estimated_graph_mb, h.mwm_kb),
                  format('执行 SET maintenance_work_mem = ''%sMB''; 再建索引；'
                         '内存确实给不到这么多时改用更小的 m（如 m=8，图内存约降 %s%%）',
                         h.recommended_mwm_mb,
                         round((1 - vecdiag.hnsw_per_element(r.dims, 8)
                                  / vecdiag.hnsw_per_element(r.dims, 16)) * 100)),
                  format('SET maintenance_work_mem=''%sMB''; 重建后 NOTICE 应当消失；'
                         '降级点区间可用 select * from vecdiag.hnsw_spill_range(%s,%s,16,%s) 复算。'
                         '注意 confidence=%s',
                         h.recommended_mwm_mb, r.reltuples, r.dims, v_mwm_kb, h.confidence);
            end if;
        end;

        -- 维度过大导致 TOAST：estimate_relpages 不可用（真实 relpages 仍然可用）
        if vecdiag.toast_risk(r.dims) then
            return query select
              'info',
              format('%s.%s(%s)', r.nspname, r.relname, r.attname),
              format('维度 %s 下 vector 可能被 TOAST，页数估算函数不可用于这张表', r.dims),
              'vector 是变长类型，超过约 2000 字节（dims 约 498 以上）会被压缩或移到 TOAST 表，'
              || '堆内元组变小、relpages 与按 itemsize 的估算脱钩',
              '预测时一律传真实 relpages（本函数已经这么做），不要调用 vecdiag.estimate_relpages()',
              '对照 pg_class.relpages 与 vecdiag.estimate_relpages() 的差值；实测 100 行×960 维'
              || '两者会让预测从 17 MB 变成 58 MB';
        end if;
    end loop;
end;
$$;


ALTER FUNCTION vecdiag.diagnose() OWNER TO postgres;

--
-- Name: FUNCTION diagnose(); Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON FUNCTION vecdiag.diagnose() IS '零参数体检入口。每条输出都带问题/原因/调整方法/验证方式四要素；拿不到前提时明说拿不到。';


--
-- Name: estimate_relpages(bigint, integer); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.estimate_relpages(p_rows bigint, p_dims integer) RETURNS bigint
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
  with c as (
    select current_setting('block_size')::bigint                as blcksz,
           24::bigint                                           as page_header,
           4::bigint                                            as item_id,
           -- t_hoff = MAXALIGN(SizeofHeapTupleHeader=23) = 24
           -- 之后是 int(4) → 4 字节对齐补到 28 → vector 变长 520 → 合计 MAXALIGN
           vecdiag.maxalign(24 + 4 + 4 + vecdiag.vector_itemsize(p_dims)) as tuple_len
  )
  select greatest(
           ceil(p_rows::numeric
                / greatest(((blcksz - page_header) / (tuple_len + item_id)), 1))::bigint,
           case when p_rows > 0 then 1 else 0 end)
  from c;
$$;


ALTER FUNCTION vecdiag.estimate_relpages(p_rows bigint, p_dims integer) OWNER TO postgres;

--
-- Name: FUNCTION estimate_relpages(p_rows bigint, p_dims integer); Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON FUNCTION vecdiag.estimate_relpages(p_rows bigint, p_dims integer) IS '仅在拿不到 pg_class.relpages 时使用；调用方必须把 pages_estimated 标为 true。⚠️ dims 大到让 vector 超过 TOAST 阈值（约 2000 字节，即 dims ≳ 498）时，值会被压缩或移到 TOAST 表，主元组变小、relpages 远小于本函数的估算，numSamples 上限随之算错。实测 100 行 × 960 维那组，用估算值会把预测从 17 MB 抬到 58 MB。高维场景必须先 ANALYZE 用真实 relpages。';


--
-- Name: hnsw_per_element(integer, integer); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.hnsw_per_element(p_dims integer, p_m integer DEFAULT 16) RETURNS numeric
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
  select (select value from vecdiag.hnsw_coef where key = 'base_bytes')
       + (select value from vecdiag.hnsw_coef where key = 'slot_coef') * p_m
       + (select value from vecdiag.hnsw_coef where key = 'bytes_per_dim') * p_dims;
$$;


ALTER FUNCTION vecdiag.hnsw_per_element(p_dims integer, p_m integer) OWNER TO postgres;

--
-- Name: FUNCTION hnsw_per_element(p_dims integer, p_m integer); Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON FUNCTION vecdiag.hnsw_per_element(p_dims integer, p_m integer) IS '每元素图内存（字节）。ef_construction 不进入该式——实测 ef 64→200 降级点完全相同（6838 行）。';


--
-- Name: hnsw_predict_spill(bigint, integer, integer, bigint, numeric); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.hnsw_predict_spill(p_rows bigint, p_dims integer, p_m integer DEFAULT 16, p_mwm_kb bigint DEFAULT NULL::bigint, p_margin numeric DEFAULT 1.15) RETURNS TABLE(will_spill boolean, predicted_spill_tuples bigint, estimated_graph_mb numeric, recommended_mwm_mb integer, per_element_bytes numeric, mwm_kb bigint, confidence text, evidence_source text)
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
  with p as (
    select vecdiag.hnsw_per_element(p_dims, p_m)                as pe,
           coalesce(p_mwm_kb, vecdiag.current_mwm_kb())          as mwm
  ),
  q as (
    select p.*,
           pe * greatest(p_rows, 0)                              as graph_bytes,
           floor(mwm * 1024 / pe)::bigint                        as spill_at
    from p
  )
  select graph_bytes >= mwm * 1024,
         case when graph_bytes >= mwm * 1024 then spill_at else null end,
         round(graph_bytes / 1048576.0, 1),
         ceil(graph_bytes * p_margin / 1048576.0)::int,
         pe, mwm,
         -- 标定覆盖 dims∈[128,384]、m∈[8,32]；越界必须标 extrapolated
         case when p_dims between 128 and 384 and p_m between 8 and 32
              then 'calibrated' else 'extrapolated' end,
         'results/m2-20260826/hnsw_spill.csv（8 组实测，A/C 组自洽性 0.03%/0.05%）'
  from q;
$$;


ALTER FUNCTION vecdiag.hnsw_predict_spill(p_rows bigint, p_dims integer, p_m integer, p_mwm_kb bigint, p_margin numeric) OWNER TO postgres;

--
-- Name: FUNCTION hnsw_predict_spill(p_rows bigint, p_dims integer, p_m integer, p_mwm_kb bigint, p_margin numeric); Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON FUNCTION vecdiag.hnsw_predict_spill(p_rows bigint, p_dims integer, p_m integer, p_mwm_kb bigint, p_margin numeric) IS '事前预警。confidence=extrapolated 时说明 (dims,m) 超出标定范围，结论只能当量级参考。';


--
-- Name: hnsw_spill_range(bigint, integer, integer, bigint); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.hnsw_spill_range(p_rows bigint, p_dims integer, p_m integer DEFAULT 16, p_mwm_kb bigint DEFAULT NULL::bigint) RETURNS TABLE(spill_low bigint, spill_high bigint, range_pct numeric, note text)
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
  with p as (
    select vecdiag.hnsw_per_element(p_dims, p_m)              as pe,
           coalesce(p_mwm_kb, vecdiag.current_mwm_kb()) * 1024 as bytes
  ),
  q as (
    -- 区间两端再各放 0.5%：per_element 本身是拟合值，标定样本间的离散约 0.3%。
    select floor(vecdiag.allocset_capacity_floor(bytes) / pe * 0.995)::bigint as lo,
           ceil(bytes / pe * 1.005)::bigint                                   as hi
    from p
  )
  select lo, hi,
         round((hi - lo) / nullif(hi, 0)::numeric * 100, 1),
         '下端来自 AllocSet 块台阶，上端为朴素线性外推，两端各放 0.5% 覆盖 per_element 的拟合离散。'
         '实测降级点应落在区间内；区间随 maintenance_work_mem 变大而收窄。'
  from q;
$$;


ALTER FUNCTION vecdiag.hnsw_spill_range(p_rows bigint, p_dims integer, p_m integer, p_mwm_kb bigint) OWNER TO postgres;

--
-- Name: FUNCTION hnsw_spill_range(p_rows bigint, p_dims integer, p_m integer, p_mwm_kb bigint); Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON FUNCTION vecdiag.hnsw_spill_range(p_rows bigint, p_dims integer, p_m integer, p_mwm_kb bigint) IS '降级点区间。maintenance_work_mem 只有几 MB 时区间较宽，这是块粒度决定的，不是模型系数不准。';


--
-- Name: intra_phase_pct(text, bigint, bigint, bigint, bigint); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.intra_phase_pct(p_phase text, p_blocks_total bigint, p_blocks_done bigint, p_tuples_total bigint, p_tuples_done bigint) RETURNS numeric
    LANGUAGE sql IMMUTABLE
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
  select case
           when coalesce(p_tuples_total, 0) > 0
             then least(p_tuples_done::numeric / p_tuples_total, 1)
           when coalesce(p_blocks_total, 0) > 0
             then least(p_blocks_done::numeric / p_blocks_total, 1)
           else null                       -- 拿不到计数 → 交给调用方做时间插值
         end;
$$;


ALTER FUNCTION vecdiag.intra_phase_pct(p_phase text, p_blocks_total bigint, p_blocks_done bigint, p_tuples_total bigint, p_tuples_done bigint) OWNER TO postgres;

--
-- Name: ivfflat_legacy080_num_samples(integer); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.ivfflat_legacy080_num_samples(p_lists integer) RETURNS bigint
    LANGUAGE sql IMMUTABLE
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
  select greatest(p_lists::bigint * 50, 10000::bigint);
$$;


ALTER FUNCTION vecdiag.ivfflat_legacy080_num_samples(p_lists integer) OWNER TO postgres;

--
-- Name: FUNCTION ivfflat_legacy080_num_samples(p_lists integer); Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON FUNCTION vecdiag.ivfflat_legacy080_num_samples(p_lists integer) IS '0.8.0 口径：无 maxTuples 上限。小表上因此显著高估（对照实验用，不作预测）。';


--
-- Name: ivfflat_memory_breakdown(bigint, integer, integer, bigint, boolean, bigint); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.ivfflat_memory_breakdown(p_rows bigint, p_dims integer, p_lists integer, p_relpages bigint DEFAULT NULL::bigint, p_empty_build boolean DEFAULT false, p_itemsize bigint DEFAULT NULL::bigint) RETURNS TABLE(component text, bytes bigint, checkpoint vecdiag.checkpoint_kind, source_ref text, note text)
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
  with p as (
    select coalesce(p_itemsize, vecdiag.vector_itemsize(p_dims))          as isize,
           coalesce(p_relpages, vecdiag.estimate_relpages(p_rows, p_dims)) as pages,
           p_lists::bigint                                                as l,
           p_dims::bigint                                                 as d
  ),
  s as (
    select p.*,
           vecdiag.ivfflat_num_samples(p_lists, p.pages, p_empty_build)      as numsamples
    from p
  ),
  q as (
    select s.*,
           least(s.numsamples, greatest(p_rows, 0))                       as sampled
    from s
  )
  select 'centers (VectorArrayInit)',
         vecdiag.vector_array_size(l, isize), 'C1'::vecdiag.checkpoint_kind,
         'ivfbuild.c:394', 'memoryUsed 起点：VECTOR_ARRAY_SIZE(lists, itemsize)' from q
  union all
  select 'samples (VectorArrayInit)',
         vecdiag.vector_array_size(numsamples, isize), 'C2',
         'ivfbuild.c:459', 'numSamples 用上限值，不是实采条数' from q
  union all
  select 'newCenters', vecdiag.vector_array_size(l, isize), 'C3',
         'ivfkmeans.c:266', 'VECTOR_ARRAY_SIZE(numCenters, centers->itemsize)' from q
  union all
  select 'agg', 4 * l * d, 'C3', 'ivfkmeans.c:267', 'sizeof(float)*numCenters*dimensions' from q
  union all
  select 'centerCounts', 4 * l, 'C3', 'ivfkmeans.c:268', 'sizeof(int)*numCenters' from q
  union all
  select 'closestCenters', 4 * sampled, 'C3', 'ivfkmeans.c:269',
         'sizeof(int)*numSamples，此处 numSamples = samples->length（实采条数）' from q
  union all
  select 'lowerBound', 4 * sampled * l, 'C3', 'ivfkmeans.c:270',
         '主导项：sizeof(float)*numSamples*numCenters' from q
  union all
  select 'upperBound', 4 * sampled, 'C3', 'ivfkmeans.c:271', 'sizeof(float)*numSamples' from q
  union all
  select 's', 4 * l, 'C3', 'ivfkmeans.c:272', 'sizeof(float)*numCenters' from q
  union all
  select 'halfcdist', 4 * l * l, 'C3', 'ivfkmeans.c:273', 'sizeof(float)*numCenters*numCenters' from q
  union all
  select 'newcdist', 4 * l, 'C3', 'ivfkmeans.c:274', 'sizeof(float)*numCenters' from q;
$$;


ALTER FUNCTION vecdiag.ivfflat_memory_breakdown(p_rows bigint, p_dims integer, p_lists integer, p_relpages bigint, p_empty_build boolean, p_itemsize bigint) OWNER TO postgres;

--
-- Name: FUNCTION ivfflat_memory_breakdown(p_rows bigint, p_dims integer, p_lists integer, p_relpages bigint, p_empty_build boolean, p_itemsize bigint); Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON FUNCTION vecdiag.ivfflat_memory_breakdown(p_rows bigint, p_dims integer, p_lists integer, p_relpages bigint, p_empty_build boolean, p_itemsize bigint) IS '11 行分项：centers(C1) + samples(C2) + kmeans 9 项(C3)。每行标源码行号，无魔数。';


--
-- Name: ivfflat_mwm_plan(bigint, integer, integer, bigint, boolean); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.ivfflat_mwm_plan(p_rows bigint, p_dims integer, p_lists integer, p_relpages bigint DEFAULT NULL::bigint, p_empty_build boolean DEFAULT false) RETURNS TABLE(target vecdiag.checkpoint_kind, mwm_kb bigint, expect_mb integer, window_kb bigint, comment_text text)
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
  with p as (
    select c1_bytes / 1024 as k1, c2_bytes / 1024 as k2, c3_bytes / 1024 as k3,
           c1_bytes, c2_bytes, c3_bytes
    from vecdiag.ivfflat_predict(p_rows, p_dims, p_lists, p_relpages, p_empty_build, null, null)
  )
  select 'C1'::vecdiag.checkpoint_kind, greatest(k1 - 1, 64),
         (c1_bytes / 1048576 + 1)::int, null::bigint,
         '低于 floor(C1/1024) 即在第一个检查点越界，报错只反映 centers' from p
  union all
  select 'C2', greatest(k1, 64), (c2_bytes / 1048576 + 1)::int, k2 - k1,
         '落在 [C1, C2) 之间：C1 放行、C2 越界' from p
  union all
  select 'C3', greatest((k2 + k3) / 2, 64), (c3_bytes / 1048576 + 1)::int, k3 - k2,
         '落在 [C2, C3) 之间才真正验证 kmeans 9 项；window_kb 就是这个窗口的宽度' from p
  union all
  select 'none', k3 + 4096, null::int, null::bigint,
         '高于 floor(C3/1024) 时构建应当成功，用于 K15 保守方向的正例' from p;
$$;


ALTER FUNCTION vecdiag.ivfflat_mwm_plan(p_rows bigint, p_dims integer, p_lists integer, p_relpages bigint, p_empty_build boolean) OWNER TO postgres;

--
-- Name: FUNCTION ivfflat_mwm_plan(p_rows bigint, p_dims integer, p_lists integer, p_relpages bigint, p_empty_build boolean); Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON FUNCTION vecdiag.ivfflat_mwm_plan(p_rows bigint, p_dims integer, p_lists integer, p_relpages bigint, p_empty_build boolean) IS '生成验证矩阵用的 maintenance_work_mem 取值。设计 T1.5 的饱和组时必须先调它，不要凭感觉设内存。';


--
-- Name: ivfflat_num_samples(integer, bigint, boolean); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.ivfflat_num_samples(p_lists integer, p_relpages bigint, p_empty_build boolean DEFAULT false) RETURNS bigint
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
  select case
           when p_empty_build then 1::bigint
           else greatest(
                  least(
                    greatest(p_lists::bigint * 50, 10000::bigint),
                    coalesce(p_relpages, 0) * vecdiag.abi('max_heap_tuples_per_page')
                  ), 1::bigint)
         end;
$$;


ALTER FUNCTION vecdiag.ivfflat_num_samples(p_lists integer, p_relpages bigint, p_empty_build boolean) OWNER TO postgres;

--
-- Name: FUNCTION ivfflat_num_samples(p_lists integer, p_relpages bigint, p_empty_build boolean); Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON FUNCTION vecdiag.ivfflat_num_samples(p_lists integer, p_relpages bigint, p_empty_build boolean) IS '上限用 relpages*MaxHeapTuplesPerPage（ivfbuild.c:446），不是表行数。p_empty_build 对应 heap==NULL 的 ambuildempty 路径，与表是否 unlogged 无关。';


--
-- Name: ivfflat_predict(bigint, integer, integer, bigint, boolean, bigint, bigint); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.ivfflat_predict(p_rows bigint, p_dims integer, p_lists integer, p_relpages bigint DEFAULT NULL::bigint, p_empty_build boolean DEFAULT false, p_mwm_kb bigint DEFAULT NULL::bigint, p_itemsize bigint DEFAULT NULL::bigint) RETURNS TABLE(first_hit vecdiag.checkpoint_kind, predicted_mb integer, mwm_kb bigint, c1_bytes bigint, c2_bytes bigint, c3_bytes bigint, num_samples bigint, sampled bigint, relpages_used bigint, pages_estimated boolean, c3_applicable boolean)
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
  with b as (
    select checkpoint, sum(bytes)::bigint as bytes
    from vecdiag.ivfflat_memory_breakdown(p_rows, p_dims, p_lists, p_relpages,
                                          p_empty_build, p_itemsize)
    group by checkpoint
  ),
  c as (
    select coalesce((select bytes from b where checkpoint = 'C1'), 0)::bigint as c1,
           coalesce((select bytes from b where checkpoint = 'C2'), 0)::bigint as c2_delta,
           coalesce((select bytes from b where checkpoint = 'C3'), 0)::bigint as c3_delta
  ),
  a as (
    select c1, (c1 + c2_delta)::bigint as c2, (c1 + c2_delta + c3_delta)::bigint as c3 from c
  ),
  m as (
    select a.*,
           coalesce(p_mwm_kb, vecdiag.current_mwm_kb())                    as mwm,
           coalesce(p_relpages, vecdiag.estimate_relpages(p_rows, p_dims)) as pages,
           p_relpages is null                                             as est
    from a
  ),
  n as (
    select m.*,
           vecdiag.ivfflat_num_samples(p_lists, m.pages, p_empty_build)   as ns
    from m
  ),
  q as (
    -- samples->length == 0 时 IvfflatKmeans 走 RandomCenters，**根本不调 ElkanKmeans**，
    -- 因此 C3 这个检查点不存在（ivfkmeans.c:561-565）。空表就是这种情况。
    select n.*,
           least(n.ns, greatest(p_rows, 0))                               as sampled_len
    from n
  ),
  h as (
    -- 触发条件必须是**整数除法**：floor(bytes/1024) > mwm_kb（ivfutils.c:124）。
    -- 注意 sum(bigint) 返回 numeric，若不显式转回 bigint，除法会变成精确小数，
    -- 判定就成了"向上取整"，边界处会整体偏 1 kB —— 这个坑已被验证矩阵抓到过一次。
    select q.*,
           case when q.c1 / 1024 > q.mwm then 'C1'
                when q.c2 / 1024 > q.mwm then 'C2'
                when q.sampled_len > 0 and q.c3 / 1024 > q.mwm then 'C3'
                else 'none' end::vecdiag.checkpoint_kind as fh
    from q
  )
  select fh,
         case fh
           when 'C1' then (c1 / 1048576 + 1)::int      -- ivfutils.c:128 的 +1 向上取整
           when 'C2' then (c2 / 1048576 + 1)::int
           when 'C3' then (c3 / 1048576 + 1)::int
           else null
         end,
         mwm, c1, c2, c3, ns, sampled_len, pages, est, sampled_len > 0
  from h;
$$;


ALTER FUNCTION vecdiag.ivfflat_predict(p_rows bigint, p_dims integer, p_lists integer, p_relpages bigint, p_empty_build boolean, p_mwm_kb bigint, p_itemsize bigint) OWNER TO postgres;

--
-- Name: FUNCTION ivfflat_predict(p_rows bigint, p_dims integer, p_lists integer, p_relpages bigint, p_empty_build boolean, p_mwm_kb bigint, p_itemsize bigint); Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON FUNCTION vecdiag.ivfflat_predict(p_rows bigint, p_dims integer, p_lists integer, p_relpages bigint, p_empty_build boolean, p_mwm_kb bigint, p_itemsize bigint) IS '必须看 first_hit：低 maintenance_work_mem 或大 lists 时先越界的往往是 C1/C2，只报单一总量的实现会系统性对不上报错原文。';


--
-- Name: ivfflat_predict_legacy080(bigint, integer, integer, bigint); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.ivfflat_predict_legacy080(p_rows bigint, p_dims integer, p_lists integer, p_mwm_kb bigint DEFAULT NULL::bigint) RETURNS TABLE(legacy_bytes bigint, legacy_mb integer, legacy_fires boolean, legacy_samples bigint, sampled bigint)
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
  with p as (
    select vecdiag.vector_itemsize(p_dims)                as isize,
           p_lists::bigint                                as l,
           p_dims::bigint                                 as d,
           vecdiag.ivfflat_legacy080_num_samples(p_lists)  as ns
  ),
  q as (
    select p.*, least(p.ns, greatest(p_rows, 0)) as sampled from p
  ),
  t as (
    select q.*,
           vecdiag.vector_array_size(ns, isize)                 -- samplesSize（用容量）
         + vecdiag.vector_array_size(l, isize)                  -- centersSize
         + vecdiag.vector_array_size(l, isize)                  -- newCentersSize
         + 4 * l * d                                            -- aggSize
         + 4 * l                                                -- centerCountsSize
         + 4 * sampled                                          -- closestCentersSize
         + 4 * sampled * l                                      -- lowerBoundSize
         + 4 * sampled                                          -- upperBoundSize
         + 4 * l                                                -- sSize
         + 4 * l * l                                            -- halfcdistSize
         + 4 * l as total                                       -- newcdistSize
    from q
  )
  select total,
         (total / 1048576 + 1)::int,
         -- 0.8.0 按字节比较：totalSize > mwm_kb * 1024
         total > coalesce(p_mwm_kb, vecdiag.current_mwm_kb()) * 1024,
         ns, sampled
  from t;
$$;


ALTER FUNCTION vecdiag.ivfflat_predict_legacy080(p_rows bigint, p_dims integer, p_lists integer, p_mwm_kb bigint) OWNER TO postgres;

--
-- Name: FUNCTION ivfflat_predict_legacy080(p_rows bigint, p_dims integer, p_lists integer, p_mwm_kb bigint); Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON FUNCTION vecdiag.ivfflat_predict_legacy080(p_rows bigint, p_dims integer, p_lists integer, p_mwm_kb bigint) IS '旧模型对照。它只有一个总量、没有 first_hit，因此在低内存/大 lists 场景下无法复现报错文本。';


--
-- Name: ivfflat_predict_table(regclass, integer, name, bigint, bigint); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.ivfflat_predict_table(p_table regclass, p_lists integer, p_column name DEFAULT NULL::name, p_mwm_kb bigint DEFAULT NULL::bigint, p_rows_exact bigint DEFAULT NULL::bigint) RETURNS TABLE(first_hit vecdiag.checkpoint_kind, predicted_mb integer, mwm_kb bigint, c1_bytes bigint, c2_bytes bigint, c3_bytes bigint, num_samples bigint, sampled bigint, relpages_used bigint, pages_estimated boolean, c3_applicable boolean)
    LANGUAGE plpgsql STABLE
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
declare
    v_rows     bigint;
    v_pages    bigint;
    v_dims     int;
    v_col      name;
begin
    select c.reltuples::bigint, c.relpages::bigint
      into v_rows, v_pages
      from pg_class c where c.oid = p_table;

    if v_pages is null then
        raise exception 'vecdiag: 找不到关系 %', p_table;
    end if;

    -- reltuples 是 ANALYZE 的**估计值**。主导项 lowerBound = 4*sampled*lists 直接依赖它，
    -- 估计误差会原样放大成预测误差（实测见过 1 MB 级偏差）。验证矩阵这类知道真实行数的
    -- 场景请显式传 p_rows_exact，不要让统计误差混进"模型误差"。
    if p_rows_exact is not null then
        v_rows := p_rows_exact;
    end if;

    -- 定位向量列：显式指定优先，否则取第一个 vector 列
    select a.attname, a.atttypmod
      into v_col, v_dims
      from pg_attribute a
      join pg_type t on t.oid = a.atttypid
     where a.attrelid = p_table and a.attnum > 0 and not a.attisdropped
       and t.typname = 'vector'
       and (p_column is null or a.attname = p_column)
     order by a.attnum
     limit 1;

    if v_dims is null then
        raise exception 'vecdiag: 表 % 上找不到 vector 列（p_column=%）', p_table, p_column;
    end if;
    if v_dims <= 0 then
        raise exception 'vecdiag: 列 %.% 未声明维度，无法预测（vector 而非 vector(N)）', p_table, v_col;
    end if;
    if v_rows < 0 then
        raise exception 'vecdiag: % 的 reltuples 为 %，请先 ANALYZE', p_table, v_rows;
    end if;

    return query
      -- 正常的 CREATE INDEX 一定有 heap，因此 p_empty_build 恒为 false。
      -- 表是不是 unlogged 与此无关（见 ivfflat_num_samples 的注释）。
      select * from vecdiag.ivfflat_predict(v_rows, v_dims, p_lists, v_pages,
                                            false, p_mwm_kb, null);
end;
$$;


ALTER FUNCTION vecdiag.ivfflat_predict_table(p_table regclass, p_lists integer, p_column name, p_mwm_kb bigint, p_rows_exact bigint) OWNER TO postgres;

--
-- Name: FUNCTION ivfflat_predict_table(p_table regclass, p_lists integer, p_column name, p_mwm_kb bigint, p_rows_exact bigint); Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON FUNCTION vecdiag.ivfflat_predict_table(p_table regclass, p_lists integer, p_column name, p_mwm_kb bigint, p_rows_exact bigint) IS 'relpages 依赖统计信息，用之前先 ANALYZE。reltuples 是估计值，知道真实行数时请传 p_rows_exact。';


--
-- Name: maxalign(bigint); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.maxalign(p_bytes bigint) RETURNS bigint
    LANGUAGE sql IMMUTABLE STRICT
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
  select ((p_bytes + 8 - 1) / 8) * 8;
$$;


ALTER FUNCTION vecdiag.maxalign(p_bytes bigint) OWNER TO postgres;

--
-- Name: FUNCTION maxalign(p_bytes bigint); Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON FUNCTION vecdiag.maxalign(p_bytes bigint) IS '对齐宽度硬编为 8（x86-64 的 MAXIMUM_ALIGNOF）。换架构时同时改这里与 abi_const.maximum_alignof。';


--
-- Name: parse_mem_kb(text); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.parse_mem_kb(p_setting text) RETURNS bigint
    LANGUAGE plpgsql IMMUTABLE STRICT
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $_$
declare
    v_txt  text := lower(btrim(p_setting));
    v_num  numeric;
    v_unit text;
begin
    if v_txt ~ '^[0-9]+$' then
        return v_txt::bigint;                     -- 裸数字 = kB
    end if;

    v_num  := (regexp_match(v_txt, '^([0-9]+(?:\.[0-9]+)?)'))[1]::numeric;
    v_unit := btrim(regexp_replace(v_txt, '^[0-9]+(?:\.[0-9]+)?', ''));

    return case v_unit
             when 'b'  then (v_num / 1024)::bigint
             when 'kb' then v_num::bigint
             when 'mb' then (v_num * 1024)::bigint
             when 'gb' then (v_num * 1024 * 1024)::bigint
             when 'tb' then (v_num * 1024 * 1024 * 1024)::bigint
             else null
           end;
end;
$_$;


ALTER FUNCTION vecdiag.parse_mem_kb(p_setting text) OWNER TO postgres;

--
-- Name: FUNCTION parse_mem_kb(p_setting text); Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON FUNCTION vecdiag.parse_mem_kb(p_setting text) IS '把内存类 GUC 文本解析为 kB。裸数字按 kB 处理（PostgreSQL 语义），无法识别的单位返回 NULL 而不是猜。';


--
-- Name: progress_curve(text, text, text, text); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.progress_curve(p_run_id text, p_am text, p_size_class text DEFAULT 'pooled'::text, p_dataset text DEFAULT 'synthetic'::text) RETURNS TABLE(elapsed_ms bigint, phase text, intra_pct numeric, intra_source text, raw_pct numeric, mono_pct numeric, eta_ms bigint)
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
  with s as (
    select ps.elapsed_ms, ps.phase,
           vecdiag.intra_phase_pct(ps.phase, ps.blocks_total, ps.blocks_done,
                                   ps.tuples_total, ps.tuples_done) as intra
    from vecdiag.progress_sample ps
    where ps.run_id = p_run_id
    order by ps.elapsed_ms
  ),
  w as (
    select phase, weight,
           sum(weight) over (order by weight desc, phase
                             rows between unbounded preceding and 1 preceding) as prior_w
    from vecdiag.stage_weight
    where am = p_am and size_class = p_size_class and dataset = p_dataset
  ),
  j as (
    select s.elapsed_ms, s.phase, s.intra,
           coalesce(w.weight, 0) as wt, coalesce(w.prior_w, 0) as prior_w
    from s left join w on w.phase = s.phase
  ),
  r as (
    select elapsed_ms, phase, intra,
           case when intra is null then 'time-interpolated' else 'view-counter' end as src,
           least(100, greatest(0,
             (prior_w + wt * coalesce(intra, 0.5)) * 100)) as raw
    from j
  ),
  m as (
    select r.*, max(raw) over (order by elapsed_ms
                               rows between unbounded preceding and current row) as mono
    from r
  )
  select elapsed_ms, phase, round(intra, 4), src, round(raw, 2), round(mono, 2),
         -- 线性外推：按当前单调进度推总时长，减去已用
         case when mono > 0 and mono < 100
              then (elapsed_ms * (100 - mono) / mono)::bigint
              else 0 end
  from m;
$$;


ALTER FUNCTION vecdiag.progress_curve(p_run_id text, p_am text, p_size_class text, p_dataset text) OWNER TO postgres;

--
-- Name: FUNCTION progress_curve(p_run_id text, p_am text, p_size_class text, p_dataset text); Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON FUNCTION vecdiag.progress_curve(p_run_id text, p_am text, p_size_class text, p_dataset text) IS 'intra_source=time-interpolated 的行说明该阶段的阶段内进度是插值，不是视图计数——报告里必须写清哪些阶段属于这一类。';


--
-- Name: recommend_stage_weights(text, bigint, text); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.recommend_stage_weights(p_am text, p_rows bigint, p_dataset text DEFAULT 'sift1m'::text) RETURNS TABLE(applicable boolean, size_class text, dataset text, phases integer, max_dispersion numeric, note text)
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
  with pick as (
    select case
             when p_rows <= 200000  then 'S'
             when p_rows <= 650000  then 'M'
             else 'L'
           end as cls
  ),
  cand as (
    select w.size_class, w.dataset, count(*)::int as phases, max(w.dispersion) as disp
    from vecdiag.stage_weight_usable w, pick
    where w.am = p_am and w.dataset = p_dataset and w.size_class = pick.cls
    group by w.size_class, w.dataset
  ),
  -- 首选档不可用时，退到同数据集下任何可用档，并在 note 里说清换了档
  fallback as (
    select w.size_class, w.dataset, count(*)::int as phases, max(w.dispersion) as disp
    from vecdiag.stage_weight_usable w
    where w.am = p_am and w.dataset = p_dataset
    group by w.size_class, w.dataset
    order by max(w.dispersion)
    limit 1
  )
  select true, c.size_class, c.dataset, c.phases, c.disp,
         format('按行数 %s 命中 %s 档；极差 %s 达标', p_rows, c.size_class, c.disp)
  from cand c
  union all
  select true, f.size_class, f.dataset, f.phases, f.disp,
         format('行数 %s 对应的档位没有达标权重，退用极差最小的 %s 档；'
                'ETA 只能当量级参考，结论里要注明换档', p_rows, f.size_class)
  from fallback f where not exists (select 1 from cand)
  union all
  select false, null, p_dataset, 0, null,
         format('数据集 %s 下 %s 没有任何达标权重。请先跑 tools/measure_build_time.sh '
                '与 tools/load_stage_weights.sh 标定，不要拿别的数据集的权重顶替', p_dataset, p_am)
  where not exists (select 1 from cand) and not exists (select 1 from fallback);
$$;


ALTER FUNCTION vecdiag.recommend_stage_weights(p_am text, p_rows bigint, p_dataset text) OWNER TO postgres;

--
-- Name: FUNCTION recommend_stage_weights(p_am text, p_rows bigint, p_dataset text); Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON FUNCTION vecdiag.recommend_stage_weights(p_am text, p_rows bigint, p_dataset text) IS '给人和 AI 用的入口：不用先读文档就知道该取哪组权重。选不出来时明确返回 applicable=false，并给出该跑哪个脚本去标定，而不是随便返回一组凑数。';


--
-- Name: stage_weight_dispersion_limit(); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.stage_weight_dispersion_limit() RETURNS numeric
    LANGUAGE sql IMMUTABLE
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$ select 0.25::numeric $$;


ALTER FUNCTION vecdiag.stage_weight_dispersion_limit() OWNER TO postgres;

--
-- Name: FUNCTION stage_weight_dispersion_limit(); Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON FUNCTION vecdiag.stage_weight_dispersion_limit() IS '权重可用性的极差上限。实测可用档 0.08/0.16、不可用档 0.51/0.72，阈值取中间空隙 0.25。';


--
-- Name: toast_risk(integer); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.toast_risk(p_dims integer) RETURNS boolean
    LANGUAGE sql IMMUTABLE STRICT
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
  -- TOAST_TUPLE_THRESHOLD = MAXALIGN(BLCKSZ/4) 之下才留在主元组，
  -- 超过就可能被压缩或外置，堆内元组大小与 itemsize 脱钩。
  select vecdiag.vector_itemsize(p_dims) > (current_setting('block_size')::bigint / 4);
$$;


ALTER FUNCTION vecdiag.toast_risk(p_dims integer) OWNER TO postgres;

--
-- Name: FUNCTION toast_risk(p_dims integer); Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON FUNCTION vecdiag.toast_risk(p_dims integer) IS 'true 表示该维度下 vector 可能被 TOAST，estimate_relpages 的结果不可用于预测，必须取真实 relpages。';


--
-- Name: vector_array_size(bigint, bigint); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.vector_array_size(p_length bigint, p_itemsize bigint) RETURNS bigint
    LANGUAGE sql STABLE STRICT
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
  select vecdiag.abi('sizeof_VectorArrayData')
       + p_length * vecdiag.maxalign(p_itemsize);
$$;


ALTER FUNCTION vecdiag.vector_array_size(p_length bigint, p_itemsize bigint) OWNER TO postgres;

--
-- Name: vector_itemsize(integer); Type: FUNCTION; Schema: vecdiag; Owner: postgres
--

CREATE FUNCTION vecdiag.vector_itemsize(p_dims integer) RETURNS bigint
    LANGUAGE sql IMMUTABLE STRICT
    SET search_path TO 'pg_catalog', 'pg_temp'
    AS $$
  select 8::bigint + 4::bigint * p_dims;
$$;


ALTER FUNCTION vecdiag.vector_itemsize(p_dims integer) OWNER TO postgres;

--
-- Name: FUNCTION vector_itemsize(p_dims integer); Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON FUNCTION vecdiag.vector_itemsize(p_dims integer) IS '只覆盖 vector 类型。halfvec/bit/sparsevec 的 itemsize 不同，未验证前不得套用（RQ-107 局限）。';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: abi_const; Type: TABLE; Schema: vecdiag; Owner: postgres
--

CREATE TABLE vecdiag.abi_const (
    key text NOT NULL,
    value bigint NOT NULL,
    source text NOT NULL,
    source_ref text NOT NULL,
    note text,
    measured_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT abi_const_source_check CHECK ((source = ANY (ARRAY['measured'::text, 'source-code'::text])))
);


ALTER TABLE vecdiag.abi_const OWNER TO postgres;

--
-- Name: TABLE abi_const; Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON TABLE vecdiag.abi_const IS 'ABI 常数。换机器、换编译器、换 BLCKSZ 都必须重测后覆盖，不得沿用他机数值。';


--
-- Name: hnsw_calib; Type: TABLE; Schema: vecdiag; Owner: postgres
--

CREATE TABLE vecdiag.hnsw_calib (
    id integer NOT NULL,
    dims integer NOT NULL,
    m integer NOT NULL,
    ef_construction integer NOT NULL,
    mwm_kb bigint NOT NULL,
    spill_tuples bigint,
    per_element numeric,
    run_id text,
    measured_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE vecdiag.hnsw_calib OWNER TO postgres;

--
-- Name: TABLE hnsw_calib; Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON TABLE vecdiag.hnsw_calib IS '降级点标定样本。per_element = mwm_kb*1024/spill_tuples。换机器或换 pgvector 版本必须重标定。';


--
-- Name: hnsw_calib_id_seq; Type: SEQUENCE; Schema: vecdiag; Owner: postgres
--

CREATE SEQUENCE vecdiag.hnsw_calib_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE vecdiag.hnsw_calib_id_seq OWNER TO postgres;

--
-- Name: hnsw_calib_id_seq; Type: SEQUENCE OWNED BY; Schema: vecdiag; Owner: postgres
--

ALTER SEQUENCE vecdiag.hnsw_calib_id_seq OWNED BY vecdiag.hnsw_calib.id;


--
-- Name: hnsw_coef; Type: TABLE; Schema: vecdiag; Owner: postgres
--

CREATE TABLE vecdiag.hnsw_coef (
    key text NOT NULL,
    value numeric NOT NULL,
    kind text NOT NULL,
    source_ref text NOT NULL,
    note text,
    CONSTRAINT hnsw_coef_kind_check CHECK ((kind = ANY (ARRAY['structural'::text, 'fitted'::text])))
);


ALTER TABLE vecdiag.hnsw_coef OWNER TO postgres;

--
-- Name: progress_sample; Type: TABLE; Schema: vecdiag; Owner: postgres
--

CREATE TABLE vecdiag.progress_sample (
    run_id text NOT NULL,
    elapsed_ms bigint NOT NULL,
    pid integer,
    phase text,
    blocks_total bigint,
    blocks_done bigint,
    tuples_total bigint,
    tuples_done bigint,
    relid oid,
    index_relid oid
);


ALTER TABLE vecdiag.progress_sample OWNER TO postgres;

--
-- Name: TABLE progress_sample; Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON TABLE vecdiag.progress_sample IS '进度视图的原始抽样序列。所有 M3 结论都必须能从这张表重算，不接受口述。';


--
-- Name: stage_weight; Type: TABLE; Schema: vecdiag; Owner: postgres
--

CREATE TABLE vecdiag.stage_weight (
    am text NOT NULL,
    phase text NOT NULL,
    weight numeric NOT NULL,
    n_samples integer NOT NULL,
    dispersion numeric,
    source text DEFAULT 'measured'::text NOT NULL,
    run_id text,
    measured_at timestamp with time zone DEFAULT now() NOT NULL,
    size_class text DEFAULT 'pooled'::text NOT NULL,
    dataset text DEFAULT 'synthetic'::text NOT NULL,
    CONSTRAINT stage_weight_weight_check CHECK ((weight >= (0)::numeric))
);


ALTER TABLE vecdiag.stage_weight OWNER TO postgres;

--
-- Name: TABLE stage_weight; Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON TABLE vecdiag.stage_weight IS '阶段权重 = 该阶段耗时占总构建耗时的比例，来自实测。dispersion 是重复之间的极差，必须一起报告：离散大说明该阶段耗时不稳定，用它做 ETA 要标注不确定性。';


--
-- Name: stage_weight_audit; Type: VIEW; Schema: vecdiag; Owner: postgres
--

CREATE VIEW vecdiag.stage_weight_audit AS
 SELECT am,
    phase,
    size_class,
    dataset,
    weight,
    n_samples,
    dispersion,
        CASE
            WHEN (size_class = 'pooled'::text) THEN '不可用：pooled 把多个规模档混在一起求平均，无物理意义'::text
            WHEN (max(COALESCE(dispersion, (1)::numeric)) OVER (PARTITION BY am, size_class, dataset) > vecdiag.stage_weight_dispersion_limit()) THEN format('不可用：本组最大极差 %s 超过上限 %s（该组内某阶段耗时占比不稳定，通常是构建太快、被检查点或 autovacuum 整体拖慢）'::text, max(COALESCE(dispersion, (1)::numeric)) OVER (PARTITION BY am, size_class, dataset), vecdiag.stage_weight_dispersion_limit())
            ELSE '可用'::text
        END AS usability,
    run_id,
    measured_at
   FROM vecdiag.stage_weight;


ALTER VIEW vecdiag.stage_weight_audit OWNER TO postgres;

--
-- Name: VIEW stage_weight_audit; Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON VIEW vecdiag.stage_weight_audit IS '给审查者看的全量视图：每组权重都带"可用/不可用 + 原因"。证据不删，结论不混。';


--
-- Name: stage_weight_usable; Type: VIEW; Schema: vecdiag; Owner: postgres
--

CREATE VIEW vecdiag.stage_weight_usable AS
 SELECT w.am,
    w.phase,
    w.size_class,
    w.dataset,
    w.weight,
    w.n_samples,
    w.dispersion,
    w.source,
    w.run_id,
    w.measured_at
   FROM (vecdiag.stage_weight w
     JOIN ( SELECT stage_weight.am,
            stage_weight.size_class,
            stage_weight.dataset
           FROM vecdiag.stage_weight
          WHERE (stage_weight.size_class <> 'pooled'::text)
          GROUP BY stage_weight.am, stage_weight.size_class, stage_weight.dataset
         HAVING (max(COALESCE(stage_weight.dispersion, (1)::numeric)) <= vecdiag.stage_weight_dispersion_limit())) ok ON (((ok.am = w.am) AND (ok.size_class = w.size_class) AND (ok.dataset = w.dataset))));


ALTER VIEW vecdiag.stage_weight_usable OWNER TO postgres;

--
-- Name: VIEW stage_weight_usable; Type: COMMENT; Schema: vecdiag; Owner: postgres
--

COMMENT ON VIEW vecdiag.stage_weight_usable IS '**消费方默认用这个视图。** 只含极差达标且按规模分档的权重；pooled 一律排除。被排除的组仍在 vecdiag.stage_weight 里，附极差可查，用于说明"为什么必须分档"。';


--
-- Name: hnsw_calib id; Type: DEFAULT; Schema: vecdiag; Owner: postgres
--

ALTER TABLE ONLY vecdiag.hnsw_calib ALTER COLUMN id SET DEFAULT nextval('vecdiag.hnsw_calib_id_seq'::regclass);


--
-- Data for Name: abi_const; Type: TABLE DATA; Schema: vecdiag; Owner: postgres
--

COPY vecdiag.abi_const (key, value, source, source_ref, note, measured_at) FROM stdin;
sizeof_VectorArrayData	24	source-code	pgvector v0.8.6 src/ivfflat.h:120-126	int length + int maxlen + int dim + (4 字节补齐) + Size itemsize = 24。SQL 层只能实测出上界（kB 粒度下 <252），占总量约 0.0001%，不影响有效数字。	2026-08-26 12:31:42.557519+08
maximum_alignof	8	source-code	PostgreSQL MAXIMUM_ALIGNOF on x86-64	MAXALIGN 的对齐宽度。换架构需重确认。	2026-08-26 12:31:42.557519+08
ivfflat_max_lists	32768	source-code	pgvector v0.8.6 src/ivfflat.h:57	IVFFLAT_MAX_LISTS。	2026-08-26 12:31:42.557519+08
maxalign_itemsize_dims128	520	measured	tools/abi_probe.sh run abi-final-20260826	C1 隔离法 + 对 maintenance_work_mem 二分到 1 kB 精度，四组 lists 差分	2026-08-26 23:19:27.810459+08
max_heap_tuples_per_page	291	measured	tools/abi_probe.sh run abi-final-20260826（实测 520，与源码推算一致）	(BLCKSZ - SizeOfPageHeaderData) / (MAXALIGN(SizeofHeapTupleHeader) + sizeof(ItemIdData))，8kB 块下为 291。可由 C2 检查点反解交叉验证。	2026-08-26 23:19:27.8162+08
\.


--
-- Data for Name: hnsw_calib; Type: TABLE DATA; Schema: vecdiag; Owner: postgres
--

COPY vecdiag.hnsw_calib (id, dims, m, ef_construction, mwm_kb, spill_tuples, per_element, run_id, measured_at) FROM stdin;
\.


--
-- Data for Name: hnsw_coef; Type: TABLE DATA; Schema: vecdiag; Owner: postgres
--

COPY vecdiag.hnsw_coef (key, value, kind, source_ref, note) FROM stdin;
bytes_per_dim	4	structural	pgvector v0.8.6 vector 的 x[] 为 float	实测 dims 128→384 每维增量 4.005 字节，与 sizeof(float) 相符
slot_coef	31.89	fitted	hnswutils.c:218-227（邻居数组分配点）	dims=128 上 m∈{8,16,32} 三点线性拟合；机制清楚但每槽字节数未做结构推导
base_bytes	206.4	fitted	hnswutils.c:245-267（HnswInitElement）	HnswElementData 与指针数组的常数部分，拟合值
\.


--
-- Data for Name: progress_sample; Type: TABLE DATA; Schema: vecdiag; Owner: postgres
--

COPY vecdiag.progress_sample (run_id, elapsed_ms, pid, phase, blocks_total, blocks_done, tuples_total, tuples_done, relid, index_relid) FROM stdin;
m3r-pooled/ab_on_1	1	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	62	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	124	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	185	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	247	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	308	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	370	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	432	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	493	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	556	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	617	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	679	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	741	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	802	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	864	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	925	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	986	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	1047	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	1108	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	1169	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	1230	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	1291	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	1352	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	1414	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	1475	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	1536	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	1597	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	1658	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	1719	8936	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_1	1780	8936	building index: assigning tuples	21440	238	0	0	68707	0
m3r-pooled/ab_on_1	1848	8936	building index: assigning tuples	21440	963	0	0	68707	0
m3r-pooled/ab_on_1	1912	8936	building index: assigning tuples	21440	1558	0	0	68707	0
m3r-pooled/ab_on_1	1978	8936	building index: assigning tuples	21440	2224	0	0	68707	0
m3r-pooled/ab_on_1	2047	8936	building index: assigning tuples	21440	2902	0	0	68707	0
m3r-pooled/ab_on_1	2114	8936	building index: assigning tuples	21440	3571	0	0	68707	0
m3r-pooled/ab_on_1	2177	8936	building index: assigning tuples	21440	4190	0	0	68707	0
m3r-pooled/ab_on_1	2239	8936	building index: assigning tuples	21440	4834	0	0	68707	0
m3r-pooled/ab_on_1	2300	8936	building index: assigning tuples	21440	5457	0	0	68707	0
m3r-pooled/ab_on_1	2361	8936	building index: assigning tuples	21440	6082	0	0	68707	0
m3r-pooled/ab_on_1	2422	8936	building index: assigning tuples	21440	6705	0	0	68707	0
m3r-pooled/ab_on_1	2485	8936	building index: assigning tuples	21440	7348	0	0	68707	0
m3r-pooled/ab_on_1	2547	8936	building index: assigning tuples	21440	7972	0	0	68707	0
m3r-pooled/ab_on_1	2617	8936	building index: assigning tuples	21440	8689	0	0	68707	0
m3r-pooled/ab_on_1	2678	8936	building index: assigning tuples	21440	9316	0	0	68707	0
m3r-pooled/ab_on_1	2740	8936	building index: assigning tuples	21440	9947	0	0	68707	0
m3r-pooled/ab_on_1	2805	8936	building index: assigning tuples	21440	10588	0	0	68707	0
m3r-pooled/ab_on_1	2866	8936	building index: assigning tuples	21440	11196	0	0	68707	0
m3r-pooled/ab_on_1	2927	8936	building index: assigning tuples	21440	11857	0	0	68707	0
m3r-pooled/ab_on_1	2989	8936	building index: assigning tuples	21440	12480	0	0	68707	0
m3r-pooled/ab_on_1	3050	8936	building index: assigning tuples	21440	13104	0	0	68707	0
m3r-pooled/ab_on_1	3111	8936	building index: assigning tuples	21440	13734	0	0	68707	0
m3r-pooled/ab_on_1	3182	8936	building index: assigning tuples	21440	14414	0	0	68707	0
m3r-pooled/ab_on_1	3242	8936	building index: assigning tuples	21440	15038	0	0	68707	0
m3r-pooled/ab_on_1	3303	8936	building index: assigning tuples	21440	15697	0	0	68707	0
m3r-pooled/ab_on_1	3364	8936	building index: assigning tuples	21440	16322	0	0	68707	0
m3r-pooled/ab_on_1	3432	8936	building index: assigning tuples	21440	17000	0	0	68707	0
m3r-pooled/ab_on_1	3494	8936	building index: assigning tuples	21440	17613	0	0	68707	0
m3r-pooled/ab_on_1	3558	8936	building index: assigning tuples	21440	18275	0	0	68707	0
m3r-pooled/ab_on_1	3618	8936	building index: assigning tuples	21440	18900	0	0	68707	0
m3r-pooled/ab_on_1	3679	8936	building index: assigning tuples	21440	19525	0	0	68707	0
m3r-pooled/ab_on_1	3740	8936	building index: assigning tuples	21440	20141	0	0	68707	0
m3r-pooled/ab_on_1	3802	8936	building index: assigning tuples	21440	20764	0	0	68707	0
m3r-pooled/ab_on_1	3862	8936	building index: assigning tuples	21440	21401	0	0	68707	0
m3r-pooled/ab_on_1	3925	8936	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-pooled/ab_on_1	3988	8936	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-pooled/ab_on_1	4049	8936	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-pooled/ab_on_1	4111	8936	building index: loading tuples	21440	21440	300000	38863	68707	0
m3r-pooled/ab_on_1	4172	8936	building index: loading tuples	21440	21440	300000	43838	68707	0
m3r-pooled/ab_on_1	4234	8936	building index: loading tuples	21440	21440	300000	43853	68707	0
m3r-pooled/ab_on_1	4295	8936	building index: loading tuples	21440	21440	300000	43853	68707	0
m3r-pooled/ab_on_1	4356	8936	building index: loading tuples	21440	21440	300000	43853	68707	0
m3r-pooled/ab_on_1	4419	8936	building index: loading tuples	21440	21440	300000	66521	68707	0
m3r-pooled/ab_on_1	4481	8936	building index: loading tuples	21440	21440	300000	105690	68707	0
m3r-pooled/ab_on_1	4543	8936	building index: loading tuples	21440	21440	300000	128544	68707	0
m3r-pooled/ab_on_1	4603	8936	building index: loading tuples	21440	21440	300000	136612	68707	0
m3r-pooled/ab_on_1	4664	8936	building index: loading tuples	21440	21440	300000	136612	68707	0
m3r-pooled/ab_on_1	4725	8936	building index: loading tuples	21440	21440	300000	136612	68707	0
m3r-pooled/ab_on_1	4785	8936	building index: loading tuples	21440	21440	300000	136612	68707	0
m3r-pooled/ab_on_1	4847	8936	building index: loading tuples	21440	21440	300000	136627	68707	0
m3r-pooled/ab_on_1	4908	8936	building index: loading tuples	21440	21440	300000	136627	68707	0
m3r-pooled/ab_on_1	4968	8936	building index: loading tuples	21440	21440	300000	136627	68707	0
m3r-pooled/ab_on_1	5030	8936	building index: loading tuples	21440	21440	300000	136627	68707	0
m3r-pooled/ab_on_1	5091	8936	building index: loading tuples	21440	21440	300000	148786	68707	0
m3r-pooled/ab_on_1	5152	8936	building index: loading tuples	21440	21440	300000	167543	68707	0
m3r-pooled/ab_on_1	5214	8936	building index: loading tuples	21440	21440	300000	167543	68707	0
m3r-pooled/ab_on_1	5275	8936	building index: loading tuples	21440	21440	300000	173290	68707	0
m3r-pooled/ab_on_1	5337	8936	building index: loading tuples	21440	21440	300000	173625	68707	0
m3r-pooled/ab_on_1	5399	8936	building index: loading tuples	21440	21440	300000	173625	68707	0
m3r-pooled/ab_on_1	5460	8936	building index: loading tuples	21440	21440	300000	173625	68707	0
m3r-pooled/ab_on_1	5521	8936	building index: loading tuples	21440	21440	300000	173625	68707	0
m3r-pooled/ab_on_1	5582	8936	building index: loading tuples	21440	21440	300000	173625	68707	0
m3r-pooled/ab_on_1	5643	8936	building index: loading tuples	21440	21440	300000	174215	68707	0
m3r-pooled/ab_on_1	5706	8936	building index: loading tuples	21440	21440	300000	217537	68707	0
m3r-pooled/ab_on_1	5767	8936	building index: loading tuples	21440	21440	300000	229396	68707	0
m3r-pooled/ab_on_1	5829	8936	building index: loading tuples	21440	21440	300000	229396	68707	0
m3r-pooled/ab_on_1	5891	8936	building index: loading tuples	21440	21440	300000	229396	68707	0
m3r-pooled/ab_on_1	5953	8936	building index: loading tuples	21440	21440	300000	229396	68707	0
m3r-pooled/ab_on_1	6013	8936	building index: loading tuples	21440	21440	300000	229411	68707	0
m3r-pooled/ab_on_1	6074	8936	building index: loading tuples	21440	21440	300000	229411	68707	0
m3r-pooled/ab_on_1	6134	8936	building index: loading tuples	21440	21440	300000	229411	68707	0
m3r-pooled/ab_on_1	6195	8936	building index: loading tuples	21440	21440	300000	230063	68707	0
m3r-pooled/ab_on_1	6258	8936	building index: loading tuples	21440	21440	300000	270837	68707	0
m3r-pooled/ab_on_1	6320	8936	building index: loading tuples	21440	21440	300000	293436	68707	0
m3r-pooled/ab_on_1	6380	8936	building index: loading tuples	21440	21440	300000	300000	68707	0
m3r-pooled/ab_on_4	1798	10445	building index: assigning tuples	21440	2239	0	0	68707	0
m3r-pooled/ab_on_4	1859	10445	building index: assigning tuples	21440	2862	0	0	68707	0
m3r-pooled/ab_on_4	1920	10445	building index: assigning tuples	21440	3485	0	0	68707	0
m3r-pooled/ab_on_4	1981	10445	building index: assigning tuples	21440	4109	0	0	68707	0
m3r-pooled/ab_on_4	2042	10445	building index: assigning tuples	21440	4733	0	0	68707	0
m3r-pooled/ab_on_4	2103	10445	building index: assigning tuples	21440	5358	0	0	68707	0
m3r-pooled/ab_on_4	2164	10445	building index: assigning tuples	21440	6017	0	0	68707	0
m3r-pooled/ab_on_4	2225	10445	building index: assigning tuples	21440	6640	0	0	68707	0
m3r-pooled/ab_on_4	2287	10445	building index: assigning tuples	21440	7231	0	0	68707	0
m3r-pooled/ab_on_4	2349	10445	building index: assigning tuples	21440	7854	0	0	68707	0
m3r-pooled/ab_on_4	2411	10445	building index: assigning tuples	21440	8476	0	0	68707	0
m3r-pooled/ab_on_4	2472	10445	building index: assigning tuples	21440	9097	0	0	68707	0
m3r-pooled/ab_on_2	1	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	63	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	125	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	187	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	249	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	311	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	373	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	434	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	497	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	558	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	620	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	682	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	743	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	805	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	867	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	929	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	991	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	1053	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	1115	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	1177	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	1238	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	1301	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	1363	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	1425	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	1487	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	1549	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	1611	9523	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_2	1672	9523	building index: assigning tuples	21440	247	0	0	68707	0
m3r-pooled/ab_on_2	1733	9523	building index: assigning tuples	21440	857	0	0	68707	0
m3r-pooled/ab_on_2	1794	9523	building index: assigning tuples	21440	1468	0	0	68707	0
m3r-pooled/ab_on_2	1855	9523	building index: assigning tuples	21440	2092	0	0	68707	0
m3r-pooled/ab_on_2	1916	9523	building index: assigning tuples	21440	2716	0	0	68707	0
m3r-pooled/ab_on_2	1976	9523	building index: assigning tuples	21440	3341	0	0	68707	0
m3r-pooled/ab_on_2	2036	9523	building index: assigning tuples	21440	3962	0	0	68707	0
m3r-pooled/ab_on_2	2097	9523	building index: assigning tuples	21440	4602	0	0	68707	0
m3r-pooled/ab_on_2	2158	9523	building index: assigning tuples	21440	5214	0	0	68707	0
m3r-pooled/ab_on_2	2218	9523	building index: assigning tuples	21440	5839	0	0	68707	0
m3r-pooled/ab_on_2	2279	9523	building index: assigning tuples	21440	6496	0	0	68707	0
m3r-pooled/ab_on_2	2340	9523	building index: assigning tuples	21440	7087	0	0	68707	0
m3r-pooled/ab_on_2	2400	9523	building index: assigning tuples	21440	7744	0	0	68707	0
m3r-pooled/ab_on_2	2461	9523	building index: assigning tuples	21440	8335	0	0	68707	0
m3r-pooled/ab_on_2	2521	9523	building index: assigning tuples	21440	8956	0	0	68707	0
m3r-pooled/ab_on_2	2582	9523	building index: assigning tuples	21440	9580	0	0	68707	0
m3r-pooled/ab_on_2	2642	9523	building index: assigning tuples	21440	10203	0	0	68707	0
m3r-pooled/ab_on_2	2703	9523	building index: assigning tuples	21440	10827	0	0	68707	0
m3r-pooled/ab_on_2	2823	9523	building index: assigning tuples	21440	12308	0	0	68707	0
m3r-pooled/ab_on_2	2932	9523	building index: assigning tuples	21440	13313	0	0	68707	0
m3r-pooled/ab_on_2	3025	9523	building index: assigning tuples	21440	14034	0	0	68707	0
m3r-pooled/ab_on_2	3087	9523	building index: assigning tuples	21440	14646	0	0	68707	0
m3r-pooled/ab_on_2	3150	9523	building index: assigning tuples	21440	15265	0	0	68707	0
m3r-pooled/ab_on_2	3211	9523	building index: assigning tuples	21440	15854	0	0	68707	0
m3r-pooled/ab_on_2	3271	9523	building index: assigning tuples	21440	16473	0	0	68707	0
m3r-pooled/ab_on_2	3332	9523	building index: assigning tuples	21440	17098	0	0	68707	0
m3r-pooled/ab_on_2	3392	9523	building index: assigning tuples	21440	17714	0	0	68707	0
m3r-pooled/ab_on_2	3453	9523	building index: assigning tuples	21440	18337	0	0	68707	0
m3r-pooled/ab_on_2	3513	9523	building index: assigning tuples	21440	18962	0	0	68707	0
m3r-pooled/ab_on_2	3573	9523	building index: assigning tuples	21440	19585	0	0	68707	0
m3r-pooled/ab_on_2	3633	9523	building index: assigning tuples	21440	20175	0	0	68707	0
m3r-pooled/ab_on_2	3694	9523	building index: assigning tuples	21440	20825	0	0	68707	0
m3r-pooled/ab_on_2	3756	9523	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-pooled/ab_on_2	3817	9523	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-pooled/ab_on_2	3880	9523	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-pooled/ab_on_2	3942	9523	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-pooled/ab_on_2	4004	9523	building index: loading tuples	21440	21440	300000	39417	68707	0
m3r-pooled/ab_on_2	4065	9523	building index: loading tuples	21440	21440	300000	63381	68707	0
m3r-pooled/ab_on_2	4128	9523	building index: loading tuples	21440	21440	300000	92366	68707	0
m3r-pooled/ab_on_2	4189	9523	building index: loading tuples	21440	21440	300000	111350	68707	0
m3r-pooled/ab_on_2	4251	9523	building index: loading tuples	21440	21440	300000	152754	68707	0
m3r-pooled/ab_on_2	4313	9523	building index: loading tuples	21440	21440	300000	154232	68707	0
m3r-pooled/ab_on_2	4375	9523	building index: loading tuples	21440	21440	300000	154232	68707	0
m3r-pooled/ab_on_2	4435	9523	building index: loading tuples	21440	21440	300000	154247	68707	0
m3r-pooled/ab_on_2	4496	9523	building index: loading tuples	21440	21440	300000	154247	68707	0
m3r-pooled/ab_on_2	4557	9523	building index: loading tuples	21440	21440	300000	154247	68707	0
m3r-pooled/ab_on_2	4617	9523	building index: loading tuples	21440	21440	300000	154247	68707	0
m3r-pooled/ab_on_2	4677	9523	building index: loading tuples	21440	21440	300000	154247	68707	0
m3r-pooled/ab_on_2	4738	9523	building index: loading tuples	21440	21440	300000	178270	68707	0
m3r-pooled/ab_on_2	4800	9523	building index: loading tuples	21440	21440	300000	216086	68707	0
m3r-pooled/ab_on_2	4862	9523	building index: loading tuples	21440	21440	300000	243669	68707	0
m3r-pooled/ab_on_2	4924	9523	building index: loading tuples	21440	21440	300000	256520	68707	0
m3r-pooled/ab_on_2	4986	9523	building index: loading tuples	21440	21440	300000	296807	68707	0
m3r-pooled/ab_on_4	2534	10445	building index: assigning tuples	21440	9717	0	0	68707	0
m3r-pooled/ab_on_4	2595	10445	building index: assigning tuples	21440	10322	0	0	68707	0
m3r-pooled/ab_on_4	2657	10445	building index: assigning tuples	21440	10945	0	0	68707	0
m3r-pooled/ab_on_4	2718	10445	building index: assigning tuples	21440	11552	0	0	68707	0
m3r-pooled/ab_on_4	2779	10445	building index: assigning tuples	21440	12176	0	0	68707	0
m3r-pooled/ab_on_4	2840	10445	building index: assigning tuples	21440	12801	0	0	68707	0
m3r-pooled/ab_on_4	2901	10445	building index: assigning tuples	21440	13426	0	0	68707	0
m3r-pooled/ab_on_4	2962	10445	building index: assigning tuples	21440	14015	0	0	68707	0
m3r-pooled/ab_on_4	3023	10445	building index: assigning tuples	21440	14638	0	0	68707	0
m3r-pooled/ab_on_4	3085	10445	building index: assigning tuples	21440	15299	0	0	68707	0
m3r-pooled/ab_on_4	3145	10445	building index: assigning tuples	21440	15925	0	0	68707	0
m3r-pooled/ab_on_4	3206	10445	building index: assigning tuples	21440	16534	0	0	68707	0
m3r-pooled/ab_on_4	3266	10445	building index: assigning tuples	21440	17153	0	0	68707	0
m3r-pooled/ab_on_4	3328	10445	building index: assigning tuples	21440	17777	0	0	68707	0
m3r-pooled/ab_on_4	3389	10445	building index: assigning tuples	21440	18402	0	0	68707	0
m3r-pooled/ab_on_4	3450	10445	building index: assigning tuples	21440	19025	0	0	68707	0
m3r-pooled/ab_on_4	3511	10445	building index: assigning tuples	21440	19649	0	0	68707	0
m3r-pooled/ab_on_4	3572	10445	building index: assigning tuples	21440	20273	0	0	68707	0
m3r-pooled/ab_on_4	3633	10445	building index: assigning tuples	21440	20881	0	0	68707	0
m3r-pooled/ab_on_4	3694	10445	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-pooled/ab_on_4	3755	10445	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-pooled/ab_on_4	3818	10445	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-pooled/ab_on_4	3880	10445	building index: loading tuples	21440	21440	300000	3516	68707	0
m3r-pooled/ab_on_4	3943	10445	building index: loading tuples	21440	21440	300000	36413	68707	0
m3r-pooled/ab_on_4	4004	10445	building index: loading tuples	21440	21440	300000	63255	68707	0
m3r-pooled/ab_on_4	4065	10445	building index: loading tuples	21440	21440	300000	97070	68707	0
m3r-pooled/ab_on_4	4125	10445	building index: loading tuples	21440	21440	300000	128004	68707	0
m3r-pooled/ab_on_4	4187	10445	building index: loading tuples	21440	21440	300000	153833	68707	0
m3r-pooled/ab_on_4	4248	10445	building index: loading tuples	21440	21440	300000	155944	68707	0
m3r-pooled/ab_on_4	4308	10445	building index: loading tuples	21440	21440	300000	156431	68707	0
m3r-pooled/ab_on_4	4369	10445	building index: loading tuples	21440	21440	300000	189850	68707	0
m3r-pooled/ab_on_4	4431	10445	building index: loading tuples	21440	21440	300000	189850	68707	0
m3r-pooled/ab_on_4	4491	10445	building index: loading tuples	21440	21440	300000	189850	68707	0
m3r-pooled/ab_on_4	4551	10445	building index: loading tuples	21440	21440	300000	189850	68707	0
m3r-pooled/ab_on_4	4612	10445	building index: loading tuples	21440	21440	300000	189850	68707	0
m3r-pooled/ab_on_4	4671	10445	building index: loading tuples	21440	21440	300000	189865	68707	0
m3r-pooled/ab_on_3	1	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	63	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	124	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	187	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	249	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	311	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	373	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	435	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	498	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	559	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	621	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	682	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	743	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	804	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	866	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	927	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	988	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	1050	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	1111	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	1172	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	1234	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	1295	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	1356	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	1418	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	1480	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	1541	9972	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_3	1603	9972	building index: assigning tuples	21440	158	0	0	68707	0
m3r-pooled/ab_on_3	1665	9972	building index: assigning tuples	21440	820	0	0	68707	0
m3r-pooled/ab_on_3	1727	9972	building index: assigning tuples	21440	1447	0	0	68707	0
m3r-pooled/ab_on_3	1790	9972	building index: assigning tuples	21440	2077	0	0	68707	0
m3r-pooled/ab_on_3	1852	9972	building index: assigning tuples	21440	2702	0	0	68707	0
m3r-pooled/ab_on_3	1913	9972	building index: assigning tuples	21440	3327	0	0	68707	0
m3r-pooled/ab_on_3	1976	9972	building index: assigning tuples	21440	3994	0	0	68707	0
m3r-pooled/ab_on_3	2037	9972	building index: assigning tuples	21440	4620	0	0	68707	0
m3r-pooled/ab_on_3	2098	9972	building index: assigning tuples	21440	5246	0	0	68707	0
m3r-pooled/ab_on_3	2159	9972	building index: assigning tuples	21440	5905	0	0	68707	0
m3r-pooled/ab_on_3	2219	9972	building index: assigning tuples	21440	6532	0	0	68707	0
m3r-pooled/ab_on_3	2281	9972	building index: assigning tuples	21440	7159	0	0	68707	0
m3r-pooled/ab_on_3	2342	9972	building index: assigning tuples	21440	7787	0	0	68707	0
m3r-pooled/ab_on_3	2403	9972	building index: assigning tuples	21440	8413	0	0	68707	0
m3r-pooled/ab_on_3	2464	9972	building index: assigning tuples	21440	9039	0	0	68707	0
m3r-pooled/ab_on_3	2525	9972	building index: assigning tuples	21440	9665	0	0	68707	0
m3r-pooled/ab_on_3	2586	9972	building index: assigning tuples	21440	10290	0	0	68707	0
m3r-pooled/ab_on_3	2648	9972	building index: assigning tuples	21440	10915	0	0	68707	0
m3r-pooled/ab_on_3	2708	9972	building index: assigning tuples	21440	11542	0	0	68707	0
m3r-pooled/ab_on_3	2769	9972	building index: assigning tuples	21440	12170	0	0	68707	0
m3r-pooled/ab_on_3	2830	9972	building index: assigning tuples	21440	12796	0	0	68707	0
m3r-pooled/ab_on_3	2891	9972	building index: assigning tuples	21440	13456	0	0	68707	0
m3r-pooled/ab_on_3	2952	9972	building index: assigning tuples	21440	14080	0	0	68707	0
m3r-pooled/ab_on_3	3012	9972	building index: assigning tuples	21440	14706	0	0	68707	0
m3r-pooled/ab_on_3	3073	9972	building index: assigning tuples	21440	15328	0	0	68707	0
m3r-pooled/ab_on_3	3134	9972	building index: assigning tuples	21440	15937	0	0	68707	0
m3r-pooled/ab_on_3	3194	9972	building index: assigning tuples	21440	16562	0	0	68707	0
m3r-pooled/ab_on_3	3255	9972	building index: assigning tuples	21440	17186	0	0	68707	0
m3r-pooled/ab_on_3	3316	9972	building index: assigning tuples	21440	17811	0	0	68707	0
m3r-pooled/ab_on_3	3376	9972	building index: assigning tuples	21440	18437	0	0	68707	0
m3r-pooled/ab_on_3	3437	9972	building index: assigning tuples	21440	19063	0	0	68707	0
m3r-pooled/ab_on_3	3498	9972	building index: assigning tuples	21440	19674	0	0	68707	0
m3r-pooled/ab_on_3	3558	9972	building index: assigning tuples	21440	20302	0	0	68707	0
m3r-pooled/ab_on_3	3619	9972	building index: assigning tuples	21440	20935	0	0	68707	0
m3r-pooled/ab_on_3	3681	9972	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-pooled/ab_on_3	3742	9972	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-pooled/ab_on_3	3803	9972	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-pooled/ab_on_3	3865	9972	building index: loading tuples	21440	21440	300000	6831	68707	0
m3r-pooled/ab_on_3	3927	9972	building index: loading tuples	21440	21440	300000	41942	68707	0
m3r-pooled/ab_on_3	3989	9972	building index: loading tuples	21440	21440	300000	79331	68707	0
m3r-pooled/ab_on_3	4050	9972	building index: loading tuples	21440	21440	300000	109267	68707	0
m3r-pooled/ab_on_3	4112	9972	building index: loading tuples	21440	21440	300000	110261	68707	0
m3r-pooled/ab_on_3	4173	9972	building index: loading tuples	21440	21440	300000	110261	68707	0
m3r-pooled/ab_on_3	4234	9972	building index: loading tuples	21440	21440	300000	110261	68707	0
m3r-pooled/ab_on_3	4294	9972	building index: loading tuples	21440	21440	300000	110276	68707	0
m3r-pooled/ab_on_3	4356	9972	building index: loading tuples	21440	21440	300000	137329	68707	0
m3r-pooled/ab_on_3	4418	9972	building index: loading tuples	21440	21440	300000	172111	68707	0
m3r-pooled/ab_on_3	4479	9972	building index: loading tuples	21440	21440	300000	172111	68707	0
m3r-pooled/ab_on_3	4539	9972	building index: loading tuples	21440	21440	300000	172111	68707	0
m3r-pooled/ab_on_3	4599	9972	building index: loading tuples	21440	21440	300000	172111	68707	0
m3r-pooled/ab_on_3	4660	9972	building index: loading tuples	21440	21440	300000	172111	68707	0
m3r-pooled/ab_on_3	4720	9972	building index: loading tuples	21440	21440	300000	172111	68707	0
m3r-pooled/ab_on_3	4781	9972	building index: loading tuples	21440	21440	300000	172126	68707	0
m3r-pooled/ab_on_3	4842	9972	building index: loading tuples	21440	21440	300000	172126	68707	0
m3r-pooled/ab_on_3	4902	9972	building index: loading tuples	21440	21440	300000	172126	68707	0
m3r-pooled/ab_on_3	4964	9972	building index: loading tuples	21440	21440	300000	200693	68707	0
m3r-pooled/ab_on_3	5025	9972	building index: loading tuples	21440	21440	300000	231987	68707	0
m3r-pooled/ab_on_3	5087	9972	building index: loading tuples	21440	21440	300000	259341	68707	0
m3r-pooled/ab_on_3	5149	9972	building index: loading tuples	21440	21440	300000	267209	68707	0
m3r-pooled/ab_on_3	5211	9972	building index: loading tuples	21440	21440	300000	299381	68707	0
m3r-pooled/ab_on_4	4733	10445	building index: loading tuples	21440	21440	300000	219509	68707	0
m3r-pooled/ab_on_4	4794	10445	building index: loading tuples	21440	21440	300000	251709	68707	0
m3r-pooled/ab_on_4	4855	10445	building index: loading tuples	21440	21440	300000	282632	68707	0
m3r-pooled/ab_on_4	4916	10445	building index: loading tuples	21440	21440	300000	298233	68707	0
m3r-pooled/hnsw_M_1	1	4718	building index: loading tuples	0	0	0	0	68701	0
m3r-pooled/hnsw_M_1	63	4718	building index: loading tuples	7168	18	0	291	68701	0
m3r-pooled/hnsw_M_1	124	4718	building index: loading tuples	7168	74	0	1056	68701	0
m3r-pooled/hnsw_M_1	192	4718	building index: loading tuples	7168	129	0	1816	68701	0
m3r-pooled/hnsw_M_1	252	4718	building index: loading tuples	7168	174	0	2465	68701	0
m3r-pooled/hnsw_M_1	311	4718	building index: loading tuples	7168	214	0	3089	68701	0
m3r-pooled/hnsw_M_1	374	4718	building index: loading tuples	7168	259	0	3727	68701	0
m3r-pooled/hnsw_M_1	435	4718	building index: loading tuples	7168	302	0	4316	68701	0
m3r-pooled/hnsw_M_1	494	4718	building index: loading tuples	7168	349	0	4924	68701	0
m3r-pooled/hnsw_M_1	557	4718	building index: loading tuples	7168	394	0	5572	68701	0
m3r-pooled/hnsw_M_1	616	4718	building index: loading tuples	7168	439	0	6196	68701	0
m3r-pooled/hnsw_M_1	675	4718	building index: loading tuples	7168	483	0	6827	68701	0
m3r-pooled/hnsw_M_1	745	4718	building index: loading tuples	7168	537	0	7540	68701	0
m3r-pooled/hnsw_M_1	804	4718	building index: loading tuples	7168	581	0	8145	68701	0
m3r-pooled/hnsw_M_1	863	4718	building index: loading tuples	7168	624	0	8717	68701	0
m3r-pooled/hnsw_M_1	921	4718	building index: loading tuples	7168	664	0	9259	68701	0
m3r-pooled/hnsw_M_1	979	4718	building index: loading tuples	7168	694	0	9795	68701	0
m3r-pooled/hnsw_M_1	1038	4718	building index: loading tuples	7168	737	0	10331	68701	0
m3r-pooled/hnsw_M_1	1096	4718	building index: loading tuples	7168	780	0	10849	68701	0
m3r-pooled/hnsw_M_1	1155	4718	building index: loading tuples	7168	806	0	11367	68701	0
m3r-pooled/hnsw_M_1	1213	4718	building index: loading tuples	7168	849	0	11865	68701	0
m3r-pooled/hnsw_M_1	1271	4718	building index: loading tuples	7168	875	0	12362	68701	0
m3r-pooled/hnsw_M_1	1330	4718	building index: loading tuples	7168	917	0	12854	68701	0
m3r-pooled/hnsw_M_1	1388	4718	building index: loading tuples	7168	951	0	13327	68701	0
m3r-pooled/hnsw_M_1	1447	4718	building index: loading tuples	7168	988	0	13786	68701	0
m3r-pooled/hnsw_M_1	1505	4718	building index: loading tuples	7168	1014	0	14245	68701	0
m3r-pooled/ab_on_4	1	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	64	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	126	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	189	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	251	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	314	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	376	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	439	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	501	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	563	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	626	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	688	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	751	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	813	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	875	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	937	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	998	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	1060	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	1121	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	1182	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	1243	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	1305	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	1366	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	1428	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	1491	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	1552	10445	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ab_on_4	1614	10445	building index: assigning tuples	21440	397	0	0	68707	0
m3r-pooled/ab_on_4	1676	10445	building index: assigning tuples	21440	1005	0	0	68707	0
m3r-pooled/ab_on_4	1737	10445	building index: assigning tuples	21440	1648	0	0	68707	0
m3-20260826/ab_on_1	1	6020	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_1	62	6020	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_1	121	6020	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_1	180	6020	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_1	240	6020	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_1	300	6020	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_1	359	6020	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_1	417	6020	building index: assigning tuples	4286	58	0	0	68396	0
m3-20260826/ab_on_1	478	6020	building index: assigning tuples	4286	824	0	0	68396	0
m3-20260826/ab_on_1	538	6020	building index: assigning tuples	4286	1583	0	0	68396	0
m3-20260826/ab_on_1	598	6020	building index: assigning tuples	4286	2357	0	0	68396	0
m3-20260826/ab_on_1	658	6020	building index: assigning tuples	4286	3121	0	0	68396	0
m3-20260826/ab_on_1	718	6020	building index: assigning tuples	4286	3992	0	0	68396	0
m3-20260826/ab_on_1	780	6020	building index: loading tuples	4286	4286	60000	14115	68396	0
m3-20260826/ab_on_1	842	6020	building index: loading tuples	4286	4286	60000	57735	68396	0
m3-20260826/ab_on_2	1	6127	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_2	60	6127	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_2	122	6127	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_2	185	6127	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_2	248	6127	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_2	309	6127	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_2	369	6127	building index: assigning tuples	4286	718	0	0	68396	0
m3-20260826/ab_on_2	430	6127	building index: assigning tuples	4286	1478	0	0	68396	0
m3-20260826/ab_on_2	490	6127	building index: assigning tuples	4286	2238	0	0	68396	0
m3-20260826/ab_on_2	552	6127	building index: assigning tuples	4286	3024	0	0	68396	0
m3-20260826/ab_on_2	611	6127	building index: assigning tuples	4286	3785	0	0	68396	0
m3-20260826/ab_on_2	672	6127	building index: assigning tuples	4286	4286	0	0	68396	0
m3-20260826/ab_on_2	733	6127	building index: loading tuples	4286	4286	60000	49999	68396	0
m3-20260826/ab_on_3	1	6241	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_3	61	6241	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_3	123	6241	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_3	184	6241	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_3	245	6241	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_3	308	6241	building index: assigning tuples	4286	5	0	0	68396	0
m3-20260826/ab_on_3	368	6241	building index: assigning tuples	4286	686	0	0	68396	0
m3-20260826/ab_on_3	430	6241	building index: assigning tuples	4286	1473	0	0	68396	0
m3-20260826/ab_on_3	493	6241	building index: assigning tuples	4286	2251	0	0	68396	0
m3-20260826/ab_on_3	553	6241	building index: assigning tuples	4286	3020	0	0	68396	0
m3-20260826/ab_on_3	612	6241	building index: assigning tuples	4286	3781	0	0	68396	0
m3-20260826/ab_on_3	672	6241	building index: assigning tuples	4286	4286	0	0	68396	0
m3-20260826/ab_on_3	733	6241	building index: loading tuples	4286	4286	60000	44670	68396	0
m3-20260826/ab_on_4	1	6340	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_4	61	6340	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_4	123	6340	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_4	185	6340	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_4	247	6340	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ab_on_4	307	6340	building index: assigning tuples	4286	22	0	0	68396	0
m3-20260826/ab_on_4	369	6340	building index: assigning tuples	4286	808	0	0	68396	0
m3-20260826/ab_on_4	432	6340	building index: assigning tuples	4286	1597	0	0	68396	0
m3-20260826/ab_on_4	494	6340	building index: assigning tuples	4286	2372	0	0	68396	0
m3-20260826/ab_on_4	554	6340	building index: assigning tuples	4286	3139	0	0	68396	0
m3-20260826/ab_on_4	613	6340	building index: assigning tuples	4286	3916	0	0	68396	0
m3-20260826/ab_on_4	674	6340	building index: loading tuples	4286	4286	60000	5353	68396	0
m3-20260826/ab_on_4	735	6340	building index: loading tuples	4286	4286	60000	48630	68396	0
m3-20260826/hnsw_M_1	1	4993	building index: loading tuples	0	0	0	0	68396	0
m3-20260826/hnsw_M_1	62	4993	building index: loading tuples	4286	52	0	683	68396	0
m3-20260826/hnsw_M_1	123	4993	building index: loading tuples	4286	128	0	1813	68396	0
m3-20260826/hnsw_M_1	185	4993	building index: loading tuples	4286	200	0	2771	68396	0
m3-20260826/hnsw_M_1	245	4993	building index: loading tuples	4286	267	0	3716	68396	0
m3-20260826/hnsw_M_1	307	4993	building index: loading tuples	4286	334	0	4691	68396	0
m3-20260826/hnsw_M_1	367	4993	building index: loading tuples	4286	408	0	5719	68396	0
m3-20260826/hnsw_M_1	427	4993	building index: loading tuples	4286	477	0	6706	68396	0
m3-20260826/hnsw_M_1	486	4993	building index: loading tuples	4286	550	0	7683	68396	0
m3-20260826/hnsw_M_1	545	4993	building index: loading tuples	4286	617	0	8652	68396	0
m3-20260826/hnsw_M_1	605	4993	building index: loading tuples	4286	688	0	9621	68396	0
m3-20260826/hnsw_M_1	664	4993	building index: loading tuples	4286	756	0	10604	68396	0
m3-20260826/hnsw_M_1	725	4993	building index: loading tuples	4286	823	0	11572	68396	0
m3-20260826/hnsw_M_1	785	4993	building index: loading tuples	4286	898	0	12599	68396	0
m3-20260826/hnsw_M_1	846	4993	building index: loading tuples	4286	966	0	13524	68396	0
m3-20260826/hnsw_M_1	907	4993	building index: loading tuples	4286	1037	0	14561	68396	0
m3-20260826/hnsw_M_1	968	4993	building index: loading tuples	4286	1109	0	15535	68396	0
m3-20260826/hnsw_M_1	1030	4993	building index: loading tuples	4286	1183	0	16561	68396	0
m3-20260826/hnsw_M_1	1091	4993	building index: loading tuples	4286	1253	0	17570	68396	0
m3-20260826/hnsw_M_1	1153	4993	building index: loading tuples	4286	1330	0	18593	68396	0
m3-20260826/hnsw_M_1	1215	4993	building index: loading tuples	4286	1391	0	19494	68396	0
m3-20260826/hnsw_M_1	1275	4993	building index: loading tuples	4286	1465	0	20514	68396	0
m3-20260826/hnsw_M_1	1337	4993	building index: loading tuples	4286	1549	0	21656	68396	0
m3-20260826/hnsw_M_1	1398	4993	building index: loading tuples	4286	1617	0	22642	68396	0
m3-20260826/hnsw_M_1	1458	4993	building index: loading tuples	4286	1693	0	23668	68396	0
m3-20260826/hnsw_M_1	1519	4993	building index: loading tuples	4286	1764	0	24698	68396	0
m3-20260826/hnsw_M_1	1579	4993	building index: loading tuples	4286	1835	0	25722	68396	0
m3-20260826/hnsw_M_1	1639	4993	building index: loading tuples	4286	1920	0	26882	68396	0
m3-20260826/hnsw_M_1	1700	4993	building index: loading tuples	4286	1989	0	27883	68396	0
m3-20260826/hnsw_M_1	1759	4993	building index: loading tuples	4286	2064	0	28901	68396	0
m3-20260826/hnsw_M_1	1818	4993	building index: loading tuples	4286	2137	0	29913	68396	0
m3-20260826/hnsw_M_1	1878	4993	building index: loading tuples	4286	2210	0	30917	68396	0
m3-20260826/hnsw_M_1	1937	4993	building index: loading tuples	4286	2282	0	31927	68396	0
m3-20260826/hnsw_M_1	1997	4993	building index: loading tuples	4286	2364	0	33080	68396	0
m3-20260826/hnsw_M_1	2057	4993	building index: loading tuples	4286	2438	0	34103	68396	0
m3-20260826/hnsw_M_1	2117	4993	building index: loading tuples	4286	2525	0	35374	68396	0
m3-20260826/hnsw_M_1	2179	4993	building index: loading tuples	4286	2611	0	36557	68396	0
m3-20260826/hnsw_M_1	2239	4993	building index: loading tuples	4286	2675	0	37473	68396	0
m3-20260826/hnsw_M_1	2301	4993	building index: loading tuples	4286	2742	0	38393	68396	0
m3-20260826/hnsw_M_1	2363	4993	building index: loading tuples	4286	2814	0	39403	68396	0
m3-20260826/hnsw_M_1	2425	4993	building index: loading tuples	4286	2883	0	40427	68396	0
m3-20260826/hnsw_M_1	2486	4993	building index: loading tuples	4286	2960	0	41424	68396	0
m3-20260826/hnsw_M_1	2548	4993	building index: loading tuples	4286	3020	0	42279	68396	0
m3-20260826/hnsw_M_1	2609	4993	building index: loading tuples	4286	3091	0	43277	68396	0
m3-20260826/hnsw_M_1	2668	4993	building index: loading tuples	4286	3161	0	44276	68396	0
m3-20260826/hnsw_M_1	2729	4993	building index: loading tuples	4286	3230	0	45225	68396	0
m3-20260826/hnsw_M_1	2791	4993	building index: loading tuples	4286	3301	0	46235	68396	0
m3-20260826/hnsw_M_1	2852	4993	building index: loading tuples	4286	3376	0	47257	68396	0
m3-20260826/hnsw_M_1	2914	4993	building index: loading tuples	4286	3447	0	48274	68396	0
m3-20260826/hnsw_M_1	2975	4993	building index: loading tuples	4286	3530	0	49413	68396	0
m3-20260826/hnsw_M_1	3036	4993	building index: loading tuples	4286	3600	0	50411	68396	0
m3-20260826/hnsw_M_1	3096	4993	building index: loading tuples	4286	3671	0	51410	68396	0
m3-20260826/hnsw_M_1	3155	4993	building index: loading tuples	4286	3745	0	52413	68396	0
m3-20260826/hnsw_M_1	3215	4993	building index: loading tuples	4286	3828	0	53591	68396	0
m3-20260826/hnsw_M_1	3276	4993	building index: loading tuples	4286	3920	0	54829	68396	0
m3-20260826/hnsw_M_1	3337	4993	building index: loading tuples	4286	4006	0	56099	68396	0
m3-20260826/hnsw_M_1	3397	4993	building index: loading tuples	4286	4080	0	57124	68396	0
m3-20260826/hnsw_M_1	3458	4993	building index: loading tuples	4286	4156	0	58178	68396	0
m3-20260826/hnsw_M_1	3518	4993	building index: loading tuples	4286	4227	0	59173	68396	0
m3-20260826/hnsw_M_1	3578	4993	building index: loading tuples	4286	4286	0	59981	68396	0
m3-20260826/hnsw_M_1	3638	4993	building index: loading tuples	4286	4286	0	59981	68396	0
m3-20260826/hnsw_M_1	3698	4993	building index: loading tuples	4286	4286	0	59981	68396	0
m3-20260826/hnsw_M_1	3759	4993	building index: loading tuples	4286	4286	0	59981	68396	0
m3-20260826/hnsw_M_1	3819	4993	building index: loading tuples	4286	4286	0	59981	68396	0
m3-20260826/hnsw_M_2	1	5333	building index: loading tuples	0	0	0	0	68396	0
m3-20260826/hnsw_M_2	61	5333	building index: loading tuples	4286	54	0	742	68396	0
m3-20260826/hnsw_M_2	120	5333	building index: loading tuples	4286	131	0	1856	68396	0
m3-20260826/hnsw_M_2	179	5333	building index: loading tuples	4286	209	0	2949	68396	0
m3-20260826/hnsw_M_2	240	5333	building index: loading tuples	4286	281	0	4002	68396	0
m3-20260826/hnsw_M_2	299	5333	building index: loading tuples	4286	360	0	5030	68396	0
m3-20260826/hnsw_M_2	358	5333	building index: loading tuples	4286	440	0	6132	68396	0
m3-20260826/hnsw_M_2	420	5333	building index: loading tuples	4286	528	0	7409	68396	0
m3-20260826/hnsw_M_2	482	5333	building index: loading tuples	4286	598	0	8386	68396	0
m3-20260826/hnsw_M_2	542	5333	building index: loading tuples	4286	676	0	9473	68396	0
m3-20260826/hnsw_M_2	604	5333	building index: loading tuples	4286	752	0	10504	68396	0
m3-20260826/hnsw_M_2	663	5333	building index: loading tuples	4286	822	0	11510	68396	0
m3-20260826/hnsw_M_2	722	5333	building index: loading tuples	4286	894	0	12517	68396	0
m3-20260826/hnsw_M_2	782	5333	building index: loading tuples	4286	963	0	13500	68396	0
m3-20260826/hnsw_M_2	841	5333	building index: loading tuples	4286	1038	0	14518	68396	0
m3-20260826/hnsw_M_2	900	5333	building index: loading tuples	4286	1108	0	15507	68396	0
m3-20260826/hnsw_M_2	960	5333	building index: loading tuples	4286	1180	0	16544	68396	0
m3-20260826/hnsw_M_2	1020	5333	building index: loading tuples	4286	1250	0	17523	68396	0
m3-20260826/hnsw_M_2	1080	5333	building index: loading tuples	4286	1319	0	18502	68396	0
m3-20260826/hnsw_M_2	1139	5333	building index: loading tuples	4286	1394	0	19486	68396	0
m3-20260826/hnsw_M_2	1198	5333	building index: loading tuples	4286	1461	0	20497	68396	0
m3-20260826/hnsw_M_2	1258	5333	building index: loading tuples	4286	1533	0	21483	68396	0
m3-20260826/hnsw_M_2	1317	5333	building index: loading tuples	4286	1599	0	22468	68396	0
m3-20260826/hnsw_M_2	1377	5333	building index: loading tuples	4286	1696	0	23747	68396	0
m3-20260826/hnsw_M_2	1439	5333	building index: loading tuples	4286	1781	0	24965	68396	0
m3-20260826/hnsw_M_2	1499	5333	building index: loading tuples	4286	1860	0	25997	68396	0
m3-20260826/hnsw_M_2	1560	5333	building index: loading tuples	4286	1940	0	27159	68396	0
m3-20260826/hnsw_M_2	1621	5333	building index: loading tuples	4286	2029	0	28433	68396	0
m3-20260826/hnsw_M_2	1681	5333	building index: loading tuples	4286	2099	0	29416	68396	0
m3-20260826/hnsw_M_2	1741	5333	building index: loading tuples	4286	2167	0	30400	68396	0
m3-20260826/hnsw_M_2	1801	5333	building index: loading tuples	4286	2242	0	31387	68396	0
m3-20260826/hnsw_M_2	1861	5333	building index: loading tuples	4286	2314	0	32374	68396	0
m3-20260826/hnsw_M_2	1921	5333	building index: loading tuples	4286	2383	0	33402	68396	0
m3-20260826/hnsw_M_2	1981	5333	building index: loading tuples	4286	2456	0	34395	68396	0
m3-20260826/hnsw_M_2	2040	5333	building index: loading tuples	4286	2531	0	35424	68396	0
m3-20260826/hnsw_M_2	2101	5333	building index: loading tuples	4286	2604	0	36450	68396	0
m3-20260826/hnsw_M_2	2161	5333	building index: loading tuples	4286	2676	0	37435	68396	0
m3-20260826/hnsw_M_2	2221	5333	building index: loading tuples	4286	2743	0	38420	68396	0
m3-20260826/hnsw_M_2	2280	5333	building index: loading tuples	4286	2822	0	39483	68396	0
m3-20260826/hnsw_M_2	2342	5333	building index: loading tuples	4286	2881	0	40376	68396	0
m3-20260826/hnsw_M_2	2401	5333	building index: loading tuples	4286	2951	0	41351	68396	0
m3-20260826/hnsw_M_2	2461	5333	building index: loading tuples	4286	3044	0	42610	68396	0
m3-20260826/hnsw_M_2	2522	5333	building index: loading tuples	4286	3115	0	43611	68396	0
m3-20260826/hnsw_M_2	2582	5333	building index: loading tuples	4286	3185	0	44614	68396	0
m3-20260826/hnsw_M_2	2642	5333	building index: loading tuples	4286	3251	0	45585	68396	0
m3-20260826/hnsw_M_2	2702	5333	building index: loading tuples	4286	3327	0	46580	68396	0
m3-20260826/hnsw_M_2	2761	5333	building index: loading tuples	4286	3395	0	47568	68396	0
m3-20260826/hnsw_M_2	2820	5333	building index: loading tuples	4286	3467	0	48544	68396	0
m3-20260826/hnsw_M_2	2880	5333	building index: loading tuples	4286	3541	0	49599	68396	0
m3-20260826/hnsw_M_2	2941	5333	building index: loading tuples	4286	3617	0	50691	68396	0
m3-20260826/hnsw_M_2	3001	5333	building index: loading tuples	4286	3691	0	51683	68396	0
m3-20260826/hnsw_M_2	3060	5333	building index: loading tuples	4286	3761	0	52689	68396	0
m3-20260826/hnsw_M_2	3120	5333	building index: loading tuples	4286	3847	0	53917	68396	0
m3-20260826/hnsw_M_2	3180	5333	building index: loading tuples	4286	3939	0	55155	68396	0
m3-20260826/hnsw_M_2	3241	5333	building index: loading tuples	4286	4027	0	56379	68396	0
m3-20260826/hnsw_M_2	3300	5333	building index: loading tuples	4286	4095	0	57356	68396	0
m3-20260826/hnsw_M_2	3360	5333	building index: loading tuples	4286	4170	0	58361	68396	0
m3-20260826/hnsw_M_2	3419	5333	building index: loading tuples	4286	4239	0	59358	68396	0
m3-20260826/hnsw_M_2	3479	5333	building index: loading tuples	4286	4286	0	59983	68396	0
m3-20260826/hnsw_M_2	3540	5333	building index: loading tuples	4286	4286	0	59983	68396	0
m3-20260826/hnsw_M_2	3600	5333	building index: loading tuples	4286	4286	0	59983	68396	0
m3-20260826/hnsw_M_2	3661	5333	building index: loading tuples	4286	4286	0	59983	68396	0
m3-20260826/hnsw_M_3	1	5686	building index: loading tuples	0	0	0	0	68396	0
m3-20260826/hnsw_M_3	63	5686	building index: loading tuples	4286	50	0	694	68396	0
m3-20260826/hnsw_M_3	122	5686	building index: loading tuples	4286	125	0	1792	68396	0
m3-20260826/hnsw_M_3	181	5686	building index: loading tuples	4286	203	0	2888	68396	0
m3-20260826/hnsw_M_3	240	5686	building index: loading tuples	4286	282	0	3962	68396	0
m3-20260826/hnsw_M_3	299	5686	building index: loading tuples	4286	358	0	5023	68396	0
m3-20260826/hnsw_M_3	358	5686	building index: loading tuples	4286	437	0	6114	68396	0
m3-20260826/hnsw_M_3	419	5686	building index: loading tuples	4286	514	0	7176	68396	0
m3-20260826/hnsw_M_3	480	5686	building index: loading tuples	4286	591	0	8271	68396	0
m3-20260826/hnsw_M_3	541	5686	building index: loading tuples	4286	665	0	9319	68396	0
m3-20260826/hnsw_M_3	603	5686	building index: loading tuples	4286	744	0	10403	68396	0
m3-20260826/hnsw_M_3	664	5686	building index: loading tuples	4286	812	0	11364	68396	0
m3-20260826/hnsw_M_3	724	5686	building index: loading tuples	4286	888	0	12440	68396	0
m3-20260826/hnsw_M_3	786	5686	building index: loading tuples	4286	966	0	13500	68396	0
m3-20260826/hnsw_M_3	847	5686	building index: loading tuples	4286	1038	0	14518	68396	0
m3-20260826/hnsw_M_3	906	5686	building index: loading tuples	4286	1108	0	15528	68396	0
m3-20260826/hnsw_M_3	966	5686	building index: loading tuples	4286	1183	0	16586	68396	0
m3-20260826/hnsw_M_3	1025	5686	building index: loading tuples	4286	1258	0	17643	68396	0
m3-20260826/hnsw_M_3	1084	5686	building index: loading tuples	4286	1336	0	18682	68396	0
m3-20260826/hnsw_M_3	1143	5686	building index: loading tuples	4286	1426	0	19935	68396	0
m3-20260826/hnsw_M_3	1204	5686	building index: loading tuples	4286	1487	0	20874	68396	0
m3-20260826/hnsw_M_3	1264	5686	building index: loading tuples	4286	1559	0	21837	68396	0
m3-20260826/hnsw_M_3	1323	5686	building index: loading tuples	4286	1632	0	22797	68396	0
m3-20260826/hnsw_M_3	1386	5686	building index: loading tuples	4286	1701	0	23805	68396	0
m3-20260826/hnsw_M_3	1447	5686	building index: loading tuples	4286	1781	0	24979	68396	0
m3-20260826/hnsw_M_3	1509	5686	building index: loading tuples	4286	1861	0	26027	68396	0
m3-20260826/hnsw_M_3	1568	5686	building index: loading tuples	4286	1931	0	27064	68396	0
m3-20260826/hnsw_M_3	1628	5686	building index: loading tuples	4286	2001	0	28067	68396	0
m3-20260826/hnsw_M_3	1689	5686	building index: loading tuples	4286	2097	0	29390	68396	0
m3-20260826/hnsw_M_3	1750	5686	building index: loading tuples	4286	2178	0	30451	68396	0
m3-20260826/hnsw_M_3	1810	5686	building index: loading tuples	4286	2254	0	31521	68396	0
m3-20260826/hnsw_M_3	1869	5686	building index: loading tuples	4286	2318	0	32483	68396	0
m3-20260826/hnsw_M_3	1929	5686	building index: loading tuples	4286	2401	0	33669	68396	0
m3-20260826/hnsw_M_3	1988	5686	building index: loading tuples	4286	2484	0	34745	68396	0
m3-20260826/hnsw_M_3	2048	5686	building index: loading tuples	4286	2555	0	35791	68396	0
m3-20260826/hnsw_M_3	2107	5686	building index: loading tuples	4286	2633	0	36890	68396	0
m3-20260826/hnsw_M_3	2168	5686	building index: loading tuples	4286	2712	0	37959	68396	0
m3-20260826/hnsw_M_3	2227	5686	building index: loading tuples	4286	2789	0	39044	68396	0
m3-20260826/hnsw_M_3	2288	5686	building index: loading tuples	4286	2866	0	40109	68396	0
m3-20260826/hnsw_M_3	2347	5686	building index: loading tuples	4286	2942	0	41166	68396	0
m3-20260826/hnsw_M_3	2407	5686	building index: loading tuples	4286	3016	0	42226	68396	0
m3-20260826/hnsw_M_3	2467	5686	building index: loading tuples	4286	3112	0	43525	68396	0
m3-20260826/hnsw_M_3	2529	5686	building index: loading tuples	4286	3184	0	44554	68396	0
m3-20260826/hnsw_M_3	2589	5686	building index: loading tuples	4286	3255	0	45590	68396	0
m3-20260826/hnsw_M_3	2648	5686	building index: loading tuples	4286	3329	0	46603	68396	0
m3-20260826/hnsw_M_3	2708	5686	building index: loading tuples	4286	3406	0	47669	68396	0
m3-20260826/hnsw_M_3	2770	5686	building index: loading tuples	4286	3477	0	48713	68396	0
m3-20260826/hnsw_M_3	2831	5686	building index: loading tuples	4286	3556	0	49742	68396	0
m3-20260826/hnsw_M_3	2893	5686	building index: loading tuples	4286	3630	0	50820	68396	0
m3-20260826/hnsw_M_3	2954	5686	building index: loading tuples	4286	3702	0	51860	68396	0
m3-20260826/hnsw_M_3	3014	5686	building index: loading tuples	4286	3791	0	53089	68396	0
m3-20260826/hnsw_M_3	3076	5686	building index: loading tuples	4286	3870	0	54168	68396	0
m3-20260826/hnsw_M_3	3137	5686	building index: loading tuples	4286	3946	0	55227	68396	0
m3-20260826/hnsw_M_3	3197	5686	building index: loading tuples	4286	4022	0	56289	68396	0
m3-20260826/hnsw_M_3	3256	5686	building index: loading tuples	4286	4109	0	57548	68396	0
m3-20260826/hnsw_M_3	3316	5686	building index: loading tuples	4286	4188	0	58622	68396	0
m3-20260826/hnsw_M_3	3377	5686	building index: loading tuples	4286	4262	0	59670	68396	0
m3-20260826/hnsw_M_3	3436	5686	building index: loading tuples	4286	4286	0	59992	68396	0
m3-20260826/hnsw_M_3	3497	5686	building index: loading tuples	4286	4286	0	59992	68396	0
m3-20260826/hnsw_M_3	3558	5686	building index: loading tuples	4286	4286	0	59992	68396	0
m3-20260826/hnsw_M_3	3619	5686	building index: loading tuples	4286	4286	0	59992	68396	0
m3-20260826/ivf_L_1	1	4529	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_1	62	4529	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_1	123	4529	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_1	184	4529	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_1	246	4529	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_1	307	4529	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_1	369	4529	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_1	428	4529	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_1	487	4529	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_1	547	4529	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_1	606	4529	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_1	665	4529	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_1	725	4529	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_1	784	4529	building index: assigning tuples	8572	579	0	0	68401	0
m3-20260826/ivf_L_1	845	4529	building index: assigning tuples	8572	1492	0	0	68401	0
m3-20260826/ivf_L_1	909	4529	building index: assigning tuples	8572	2393	0	0	68401	0
m3-20260826/ivf_L_1	971	4529	building index: assigning tuples	8572	3323	0	0	68401	0
m3-20260826/ivf_L_1	1034	4529	building index: assigning tuples	8572	4242	0	0	68401	0
m3-20260826/ivf_L_1	1095	4529	building index: assigning tuples	8572	5147	0	0	68401	0
m3-20260826/ivf_L_1	1157	4529	building index: assigning tuples	8572	6003	0	0	68401	0
m3-20260826/ivf_L_1	1216	4529	building index: assigning tuples	8572	6897	0	0	68401	0
m3-20260826/ivf_L_1	1276	4529	building index: assigning tuples	8572	7797	0	0	68401	0
m3-20260826/ivf_L_1	1335	4529	building index: assigning tuples	8572	8572	0	0	68401	0
m3-20260826/ivf_L_1	1396	4529	building index: loading tuples	8572	8572	60000	10498	68401	0
m3-20260826/ivf_L_1	1456	4529	building index: loading tuples	8572	8572	60000	28896	68401	0
m3-20260826/ivf_L_1	1516	4529	building index: loading tuples	8572	8572	60000	40189	68401	0
m3-20260826/ivf_L_1	1576	4529	building index: loading tuples	8572	8572	60000	60000	68401	0
m3-20260826/ivf_L_2	1	4686	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_2	61	4686	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_2	123	4686	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_2	184	4686	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_2	245	4686	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_2	306	4686	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_2	367	4686	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_2	427	4686	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_2	488	4686	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_2	549	4686	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_2	610	4686	building index: assigning tuples	8572	1	0	0	68401	0
m3-20260826/ivf_L_2	671	4686	building index: assigning tuples	8572	895	0	0	68401	0
m3-20260826/ivf_L_2	741	4686	building index: assigning tuples	8572	1905	0	0	68401	0
m3-20260826/ivf_L_2	803	4686	building index: assigning tuples	8572	2803	0	0	68401	0
m3-20260826/ivf_L_2	866	4686	building index: assigning tuples	8572	3697	0	0	68401	0
m3-20260826/ivf_L_2	928	4686	building index: assigning tuples	8572	4594	0	0	68401	0
m3-20260826/ivf_L_2	990	4686	building index: assigning tuples	8572	5495	0	0	68401	0
m3-20260826/ivf_L_2	1051	4686	building index: assigning tuples	8572	6384	0	0	68401	0
m3-20260826/ivf_L_2	1113	4686	building index: assigning tuples	8572	7249	0	0	68401	0
m3-20260826/ivf_L_2	1176	4686	building index: assigning tuples	8572	8162	0	0	68401	0
m3-20260826/ivf_L_2	1238	4686	building index: assigning tuples	8572	8572	0	0	68401	0
m3-20260826/ivf_L_2	1298	4686	building index: loading tuples	8572	8572	60000	19901	68401	0
m3-20260826/ivf_L_2	1360	4686	building index: loading tuples	8572	8572	60000	35865	68401	0
m3-20260826/ivf_L_2	1420	4686	building index: loading tuples	8572	8572	60000	49966	68401	0
m3-20260826/ivf_L_2	1481	4686	building index: loading tuples	8572	8572	60000	60000	68401	0
m3-20260826/ivf_L_3	0	4831	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_3	60	4831	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_3	121	4831	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_3	182	4831	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_3	242	4831	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_3	303	4831	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_3	364	4831	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_3	425	4831	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_3	485	4831	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_3	546	4831	building index: performing k-means	0	0	0	0	68401	0
m3-20260826/ivf_L_3	607	4831	building index: assigning tuples	8572	9	0	0	68401	0
m3-20260826/ivf_L_3	668	4831	building index: assigning tuples	8572	695	0	0	68401	0
m3-20260826/ivf_L_3	728	4831	building index: assigning tuples	8572	1523	0	0	68401	0
m3-20260826/ivf_L_3	790	4831	building index: assigning tuples	8572	2415	0	0	68401	0
m3-20260826/ivf_L_3	852	4831	building index: assigning tuples	8572	3295	0	0	68401	0
m3-20260826/ivf_L_3	914	4831	building index: assigning tuples	8572	4177	0	0	68401	0
m3-20260826/ivf_L_3	976	4831	building index: assigning tuples	8572	5070	0	0	68401	0
m3-20260826/ivf_L_3	1038	4831	building index: assigning tuples	8572	5900	0	0	68401	0
m3-20260826/ivf_L_3	1100	4831	building index: assigning tuples	8572	6716	0	0	68401	0
m3-20260826/ivf_L_3	1162	4831	building index: assigning tuples	8572	7558	0	0	68401	0
m3-20260826/ivf_L_3	1224	4831	building index: assigning tuples	8572	8374	0	0	68401	0
m3-20260826/ivf_L_3	1285	4831	building index: assigning tuples	8572	8572	0	0	68401	0
m3-20260826/ivf_L_3	1347	4831	building index: loading tuples	8572	8572	60000	21553	68401	0
m3-20260826/ivf_L_3	1410	4831	building index: loading tuples	8572	8572	60000	38233	68401	0
m3-20260826/ivf_L_3	1472	4831	building index: loading tuples	8572	8572	60000	54145	68401	0
m3-20260826/ivf_M_1	1	4209	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ivf_M_1	63	4209	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ivf_M_1	124	4209	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ivf_M_1	184	4209	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ivf_M_1	245	4209	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ivf_M_1	306	4209	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ivf_M_1	367	4209	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ivf_M_1	428	4209	building index: assigning tuples	4286	102	0	0	68396	0
m3-20260826/ivf_M_1	489	4209	building index: assigning tuples	4286	800	0	0	68396	0
m3-20260826/ivf_M_1	550	4209	building index: assigning tuples	4286	1489	0	0	68396	0
m3-20260826/ivf_M_1	611	4209	building index: assigning tuples	4286	2185	0	0	68396	0
m3-20260826/ivf_M_1	672	4209	building index: assigning tuples	4286	2891	0	0	68396	0
m3-20260826/ivf_M_1	732	4209	building index: assigning tuples	4286	3593	0	0	68396	0
m3-20260826/ivf_M_1	793	4209	building index: assigning tuples	4286	4282	0	0	68396	0
m3-20260826/ivf_M_1	854	4209	building index: loading tuples	4286	4286	60000	19620	68396	0
m3-20260826/ivf_M_1	916	4209	building index: loading tuples	4286	4286	60000	47505	68396	0
m3-20260826/ivf_M_2	1	4313	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ivf_M_2	63	4313	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ivf_M_2	126	4313	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ivf_M_2	188	4313	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ivf_M_2	251	4313	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ivf_M_2	313	4313	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ivf_M_2	375	4313	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ivf_M_2	437	4313	building index: assigning tuples	4286	244	0	0	68396	0
m3-20260826/ivf_M_2	500	4313	building index: assigning tuples	4286	879	0	0	68396	0
m3-20260826/ivf_M_2	562	4313	building index: assigning tuples	4286	1499	0	0	68396	0
m3-20260826/ivf_M_2	622	4313	building index: assigning tuples	4286	2117	0	0	68396	0
m3-20260826/ivf_M_2	682	4313	building index: assigning tuples	4286	2731	0	0	68396	0
m3-20260826/ivf_M_2	742	4313	building index: assigning tuples	4286	3352	0	0	68396	0
m3-20260826/ivf_M_2	801	4313	building index: assigning tuples	4286	3949	0	0	68396	0
m3-20260826/ivf_M_2	861	4313	building index: assigning tuples	4286	4286	0	0	68396	0
m3-20260826/ivf_M_2	923	4313	building index: loading tuples	4286	4286	60000	39870	68396	0
m3-20260826/ivf_M_2	983	4313	building index: loading tuples	4286	4286	60000	60000	68396	0
m3-20260826/ivf_M_3	1	4416	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ivf_M_3	63	4416	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ivf_M_3	124	4416	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ivf_M_3	186	4416	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ivf_M_3	247	4416	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ivf_M_3	308	4416	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ivf_M_3	369	4416	building index: performing k-means	0	0	0	0	68396	0
m3-20260826/ivf_M_3	431	4416	building index: assigning tuples	4286	239	0	0	68396	0
m3-20260826/ivf_M_3	493	4416	building index: assigning tuples	4286	970	0	0	68396	0
m3-20260826/ivf_M_3	556	4416	building index: assigning tuples	4286	1680	0	0	68396	0
m3-20260826/ivf_M_3	618	4416	building index: assigning tuples	4286	2373	0	0	68396	0
m3-20260826/ivf_M_3	679	4416	building index: assigning tuples	4286	3074	0	0	68396	0
m3-20260826/ivf_M_3	740	4416	building index: assigning tuples	4286	3770	0	0	68396	0
m3-20260826/ivf_M_3	801	4416	building index: assigning tuples	4286	4286	0	0	68396	0
m3-20260826/ivf_M_3	864	4416	building index: loading tuples	4286	4286	60000	32808	68396	0
m3-20260826/ivf_M_3	926	4416	building index: loading tuples	4286	4286	60000	50475	68396	0
m3-20260826/ivf_S_1	1	4084	building index: performing k-means	0	0	0	0	68391	0
m3-20260826/ivf_S_1	62	4084	building index: performing k-means	0	0	0	0	68391	0
m3-20260826/ivf_S_1	122	4084	building index: assigning tuples	1429	452	0	0	68391	0
m3-20260826/ivf_S_1	182	4084	building index: loading tuples	1429	1429	20000	2460	68391	0
m3-20260826/ivf_S_2	1	4126	building index: performing k-means	0	0	0	0	68391	0
m3-20260826/ivf_S_2	63	4126	building index: performing k-means	0	0	0	0	68391	0
m3-20260826/ivf_S_2	125	4126	building index: assigning tuples	1429	1164	0	0	68391	0
m3-20260826/ivf_S_3	0	4163	building index: performing k-means	0	0	0	0	68391	0
m3-20260826/ivf_S_3	62	4163	building index: performing k-means	0	0	0	0	68391	0
m3-20260826/ivf_S_3	123	4163	building index: assigning tuples	1429	1184	0	0	68391	0
m3r-pooled/hnsw_M_1	1564	4718	building index: loading tuples	7168	1047	0	14713	68701	0
m3r-pooled/hnsw_M_1	1623	4718	building index: loading tuples	7168	1088	0	15174	68701	0
m3r-pooled/hnsw_M_1	1684	4718	building index: loading tuples	7168	1114	0	15622	68701	0
m3r-pooled/hnsw_M_1	1748	4718	building index: loading tuples	7168	1143	0	16087	68701	0
m3r-pooled/hnsw_M_1	1810	4718	building index: loading tuples	7168	1181	0	16539	68701	0
m3r-pooled/hnsw_M_1	1871	4718	building index: loading tuples	7168	1210	0	16996	68701	0
m3r-pooled/hnsw_M_1	1933	4718	building index: loading tuples	7168	1252	0	17438	68701	0
m3r-pooled/hnsw_M_1	1995	4718	building index: loading tuples	7168	1277	0	17878	68701	0
m3r-pooled/hnsw_M_1	2057	4718	building index: loading tuples	7168	1305	0	18324	68701	0
m3r-pooled/hnsw_M_1	2127	4718	building index: loading tuples	7168	1339	0	18805	68701	0
m3r-pooled/hnsw_M_1	2188	4718	building index: loading tuples	7168	1367	0	19226	68701	0
m3r-pooled/hnsw_M_1	2248	4718	building index: loading tuples	7168	1404	0	19656	68701	0
m3r-pooled/hnsw_M_1	2310	4718	building index: loading tuples	7168	1436	0	20076	68701	0
m3r-pooled/hnsw_M_1	2372	4718	building index: loading tuples	7168	1464	0	20455	68701	0
m3r-pooled/hnsw_M_1	2433	4718	building index: loading tuples	7168	1488	0	20840	68701	0
m3r-pooled/hnsw_M_1	2499	4718	building index: loading tuples	7168	1528	0	21302	68701	0
m3r-pooled/hnsw_M_1	2561	4718	building index: loading tuples	7168	1547	0	21713	68701	0
m3r-pooled/hnsw_M_1	2622	4718	building index: loading tuples	7168	1582	0	22155	68701	0
m3r-pooled/hnsw_M_1	2685	4718	building index: loading tuples	7168	1613	0	22574	68701	0
m3r-pooled/hnsw_M_1	2746	4718	building index: loading tuples	7168	1639	0	22972	68701	0
m3r-pooled/hnsw_M_1	2806	4718	building index: loading tuples	7168	1670	0	23356	68701	0
m3r-pooled/hnsw_M_1	2868	4718	building index: loading tuples	7168	1695	0	23807	68701	0
m3r-pooled/hnsw_M_1	2927	4718	building index: loading tuples	7168	1733	0	24210	68701	0
m3r-pooled/hnsw_M_1	2986	4718	building index: loading tuples	7168	1758	0	24618	68701	0
m3r-pooled/hnsw_M_1	3045	4718	building index: loading tuples	7168	1783	0	25013	68701	0
m3r-pooled/hnsw_M_1	3105	4718	building index: loading tuples	7168	1807	0	25395	68701	0
m3r-pooled/hnsw_M_1	3164	4718	building index: loading tuples	7168	1844	0	25755	68701	0
m3r-pooled/hnsw_M_1	3224	4718	building index: loading tuples	7168	1872	0	26121	68701	0
m3r-pooled/hnsw_M_1	3284	4718	building index: loading tuples	7168	1892	0	26504	68701	0
m3r-pooled/hnsw_M_1	3346	4718	building index: loading tuples	7168	1925	0	26941	68701	0
m3r-pooled/hnsw_M_1	3408	4718	building index: loading tuples	7168	1957	0	27354	68701	0
m3r-pooled/hnsw_M_1	3474	4718	building index: loading tuples	7168	1981	0	27769	68701	0
m3r-pooled/hnsw_M_1	3537	4718	building index: loading tuples	7168	2014	0	28196	68701	0
m3r-pooled/hnsw_M_1	3601	4718	building index: loading tuples	7168	2041	0	28605	68701	0
m3r-pooled/hnsw_M_1	3664	4718	building index: loading tuples	7168	2069	0	29010	68701	0
m3r-pooled/hnsw_M_1	3727	4718	building index: loading tuples	7168	2102	0	29420	68701	0
m3r-pooled/hnsw_M_1	3790	4718	building index: loading tuples	7168	2130	0	29837	68701	0
m3r-pooled/hnsw_M_1	3853	4718	building index: loading tuples	7168	2168	0	30339	68701	0
m3r-pooled/hnsw_M_1	3918	4718	building index: loading tuples	7168	2198	0	30797	68701	0
m3r-pooled/hnsw_M_1	3981	4718	building index: loading tuples	7168	2226	0	31205	68701	0
m3r-pooled/hnsw_M_1	4044	4718	building index: loading tuples	7168	2254	0	31600	68701	0
m3r-pooled/hnsw_M_1	4107	4718	building index: loading tuples	7168	2285	0	31957	68701	0
m3r-pooled/hnsw_M_1	4170	4718	building index: loading tuples	7168	2312	0	32317	68701	0
m3r-pooled/hnsw_M_1	4234	4718	building index: loading tuples	7168	2336	0	32682	68701	0
m3r-pooled/hnsw_M_1	4297	4718	building index: loading tuples	7168	2351	0	33048	68701	0
m3r-pooled/hnsw_M_1	4360	4718	building index: loading tuples	7168	2382	0	33396	68701	0
m3r-pooled/hnsw_M_1	4423	4718	building index: loading tuples	7168	2409	0	33747	68701	0
m3r-pooled/hnsw_M_1	4486	4718	building index: loading tuples	7168	2436	0	34107	68701	0
m3r-pooled/hnsw_M_1	4549	4718	building index: loading tuples	7168	2455	0	34456	68701	0
m3r-pooled/hnsw_M_1	4613	4718	building index: loading tuples	7168	2482	0	34797	68701	0
m3r-pooled/hnsw_M_1	4676	4718	building index: loading tuples	7168	2509	0	35144	68701	0
m3r-pooled/hnsw_M_1	4736	4718	building index: loading tuples	7168	2540	0	35490	68701	0
m3r-pooled/hnsw_M_1	4796	4718	building index: loading tuples	7168	2554	0	35834	68701	0
m3r-pooled/hnsw_M_1	4856	4718	building index: loading tuples	7168	2586	0	36206	68701	0
m3r-pooled/hnsw_M_1	4917	4718	building index: loading tuples	7168	2609	0	36583	68701	0
m3r-pooled/hnsw_M_1	4977	4718	building index: loading tuples	7168	2636	0	36940	68701	0
m3r-pooled/hnsw_M_1	5038	4718	building index: loading tuples	7168	2655	0	37281	68701	0
m3r-pooled/hnsw_M_1	5098	4718	building index: loading tuples	7168	2682	0	37640	68701	0
m3r-pooled/hnsw_M_1	5158	4718	building index: loading tuples	7168	2728	0	38174	68701	0
m3r-pooled/hnsw_M_1	5221	4718	building index: loading tuples	7168	2763	0	38736	68701	0
m3r-pooled/hnsw_M_1	5284	4718	building index: loading tuples	7168	2802	0	39279	68701	0
m3r-pooled/hnsw_M_1	5344	4718	building index: loading tuples	7168	2841	0	39812	68701	0
m3r-pooled/hnsw_M_1	5409	4718	building index: loading tuples	7168	2873	0	40236	68701	0
m3r-pooled/hnsw_M_1	5468	4718	building index: loading tuples	7168	2900	0	40602	68701	0
m3r-pooled/hnsw_M_1	5530	4718	building index: loading tuples	7168	2932	0	41024	68701	0
m3r-pooled/hnsw_M_1	5595	4718	building index: loading tuples	7168	2964	0	41447	68701	0
m3r-pooled/hnsw_M_1	5655	4718	building index: loading tuples	7168	2992	0	41838	68701	0
m3r-pooled/hnsw_M_1	5716	4718	building index: loading tuples	7168	3011	0	42251	68701	0
m3r-pooled/hnsw_M_1	5782	4718	building index: loading tuples	7168	3039	0	42624	68701	0
m3r-pooled/hnsw_M_1	5842	4718	building index: loading tuples	7168	3072	0	42996	68701	0
m3r-pooled/hnsw_M_1	5902	4718	building index: loading tuples	7168	3098	0	43382	68701	0
m3r-pooled/hnsw_M_1	5962	4718	building index: loading tuples	7168	3119	0	43751	68701	0
m3r-pooled/hnsw_M_1	6022	4718	building index: loading tuples	7168	3153	0	44154	68701	0
m3r-pooled/hnsw_M_1	6083	4718	building index: loading tuples	7168	3179	0	44516	68701	0
m3r-pooled/hnsw_M_1	6143	4718	building index: loading tuples	7168	3208	0	44867	68701	0
m3r-pooled/hnsw_M_1	6203	4718	building index: loading tuples	7168	3230	0	45231	68701	0
m3r-pooled/hnsw_M_1	6263	4718	building index: loading tuples	7168	3260	0	45594	68701	0
m3r-pooled/hnsw_M_1	6323	4718	building index: loading tuples	7168	3285	0	45963	68701	0
m3r-pooled/hnsw_M_1	6385	4718	building index: loading tuples	7168	3311	0	46346	68701	0
m3r-pooled/hnsw_M_1	6449	4718	building index: loading tuples	7168	3341	0	46724	68701	0
m3r-pooled/hnsw_M_1	6514	4718	building index: loading tuples	7168	3363	0	47105	68701	0
m3r-pooled/hnsw_M_1	6574	4718	building index: loading tuples	7168	3396	0	47469	68701	0
m3r-pooled/hnsw_M_1	6634	4718	building index: loading tuples	7168	3411	0	47787	68701	0
m3r-pooled/hnsw_M_1	6695	4718	building index: loading tuples	7168	3440	0	48124	68701	0
m3r-pooled/hnsw_M_1	6755	4718	building index: loading tuples	7168	3462	0	48463	68701	0
m3r-pooled/hnsw_M_1	6816	4718	building index: loading tuples	7168	3483	0	48809	68701	0
m3r-pooled/hnsw_M_1	6876	4718	building index: loading tuples	7168	3512	0	49163	68701	0
m3r-pooled/hnsw_M_1	6936	4718	building index: loading tuples	7168	3537	0	49498	68701	0
m3r-pooled/hnsw_M_1	6996	4718	building index: loading tuples	7168	3559	0	49856	68701	0
m3r-pooled/hnsw_M_1	7056	4718	building index: loading tuples	7168	3589	0	50239	68701	0
m3r-pooled/hnsw_M_1	7116	4718	building index: loading tuples	7168	3614	0	50617	68701	0
m3r-pooled/hnsw_M_1	7176	4718	building index: loading tuples	7168	3645	0	51050	68701	0
m3r-pooled/hnsw_M_1	7236	4718	building index: loading tuples	7168	3671	0	51448	68701	0
m3r-pooled/hnsw_M_1	7297	4718	building index: loading tuples	7168	3702	0	51859	68701	0
m3r-pooled/hnsw_M_1	7357	4718	building index: loading tuples	7168	3732	0	52257	68701	0
m3r-pooled/hnsw_M_1	7418	4718	building index: loading tuples	7168	3759	0	52683	68701	0
m3r-pooled/hnsw_M_1	7478	4718	building index: loading tuples	7168	3790	0	53083	68701	0
m3r-pooled/hnsw_M_1	7538	4718	building index: loading tuples	7168	3824	0	53495	68701	0
m3r-pooled/hnsw_M_1	7599	4718	building index: loading tuples	7168	3856	0	53939	68701	0
m3r-pooled/hnsw_M_1	7659	4718	building index: loading tuples	7168	3878	0	54352	68701	0
m3r-pooled/hnsw_M_1	7719	4718	building index: loading tuples	7168	3914	0	54784	68701	0
m3r-pooled/hnsw_M_1	7779	4718	building index: loading tuples	7168	3945	0	55202	68701	0
m3r-pooled/hnsw_M_1	7839	4718	building index: loading tuples	7168	3967	0	55604	68701	0
m3r-pooled/hnsw_M_1	7899	4718	building index: loading tuples	7168	4001	0	56009	68701	0
m3r-pooled/hnsw_M_1	7959	4718	building index: loading tuples	7168	4032	0	56433	68701	0
m3r-pooled/hnsw_M_1	8019	4718	building index: loading tuples	7168	4059	0	56850	68701	0
m3r-pooled/hnsw_M_1	8079	4718	building index: loading tuples	7168	4089	0	57247	68701	0
m3r-pooled/hnsw_M_1	8140	4718	building index: loading tuples	7168	4121	0	57669	68701	0
m3r-pooled/hnsw_M_1	8200	4718	building index: loading tuples	7168	4143	0	58052	68701	0
m3r-pooled/hnsw_M_1	8260	4718	building index: loading tuples	7168	4177	0	58437	68701	0
m3r-pooled/hnsw_M_1	8320	4718	building index: loading tuples	7168	4198	0	58798	68701	0
m3r-pooled/hnsw_M_1	8381	4718	building index: loading tuples	7168	4223	0	59143	68701	0
m3r-pooled/hnsw_M_1	8441	4718	building index: loading tuples	7168	4252	0	59471	68701	0
m3r-pooled/hnsw_M_1	8502	4718	building index: loading tuples	7168	4273	0	59799	68701	0
m3r-pooled/hnsw_M_1	8562	4718	building index: loading tuples	7168	4297	0	60115	68701	0
m3r-pooled/hnsw_M_1	8622	4718	building index: loading tuples	7168	4317	0	60438	68701	0
m3r-pooled/hnsw_M_1	8682	4718	building index: loading tuples	7168	4342	0	60746	68701	0
m3r-pooled/hnsw_M_1	8743	4718	building index: loading tuples	7168	4362	0	61066	68701	0
m3r-pooled/hnsw_M_1	8803	4718	building index: loading tuples	7168	4386	0	61377	68701	0
m3r-pooled/hnsw_M_1	8864	4718	building index: loading tuples	7168	4403	0	61696	68701	0
m3r-pooled/hnsw_M_1	8925	4718	building index: loading tuples	7168	4423	0	62005	68701	0
m3r-pooled/hnsw_M_1	8986	4718	building index: loading tuples	7168	4447	0	62318	68701	0
m3r-pooled/hnsw_M_1	9048	4718	building index: loading tuples	7168	4475	0	62618	68701	0
m3r-pooled/hnsw_M_1	9111	4718	building index: loading tuples	7168	4495	0	62938	68701	0
m3r-pooled/hnsw_M_1	9178	4718	building index: loading tuples	7168	4515	0	63271	68701	0
m3r-pooled/hnsw_M_1	9244	4718	building index: loading tuples	7168	4548	0	63589	68701	0
m3r-pooled/hnsw_M_1	9306	4718	building index: loading tuples	7168	4568	0	63886	68701	0
m3r-pooled/hnsw_M_1	9366	4718	building index: loading tuples	7168	4583	0	64180	68701	0
m3r-pooled/hnsw_M_1	9426	4718	building index: loading tuples	7168	4603	0	64478	68701	0
m3r-pooled/hnsw_M_1	9487	4718	building index: loading tuples	7168	4627	0	64779	68701	0
m3r-pooled/hnsw_M_1	9547	4718	building index: loading tuples	7168	4647	0	65072	68701	0
m3r-pooled/hnsw_M_1	9607	4718	building index: loading tuples	7168	4667	0	65362	68701	0
m3r-pooled/hnsw_M_1	9667	4718	building index: loading tuples	7168	4691	0	65662	68701	0
m3r-pooled/hnsw_M_1	9728	4718	building index: loading tuples	7168	4710	0	65955	68701	0
m3r-pooled/hnsw_M_1	9788	4718	building index: loading tuples	7168	4734	0	66251	68701	0
m3r-pooled/hnsw_M_1	9849	4718	building index: loading tuples	7168	4754	0	66537	68701	0
m3r-pooled/hnsw_M_1	9909	4718	building index: loading tuples	7168	4777	0	66824	68701	0
m3r-pooled/hnsw_M_1	9970	4718	building index: loading tuples	7168	4796	0	67115	68701	0
m3r-pooled/hnsw_M_1	10030	4718	building index: loading tuples	7168	4820	0	67400	68701	0
m3r-pooled/hnsw_M_1	10091	4718	building index: loading tuples	7168	4831	0	67686	68701	0
m3r-pooled/hnsw_M_1	10151	4718	building index: loading tuples	7168	4855	0	67989	68701	0
m3r-pooled/hnsw_M_1	10211	4718	building index: loading tuples	7168	4875	0	68288	68701	0
m3r-pooled/hnsw_M_1	10272	4718	building index: loading tuples	7168	4905	0	68672	68701	0
m3r-pooled/hnsw_M_1	10332	4718	building index: loading tuples	7168	4929	0	69000	68701	0
m3r-pooled/hnsw_M_1	10392	4718	building index: loading tuples	7168	4950	0	69317	68701	0
m3r-pooled/hnsw_M_1	10452	4718	building index: loading tuples	7168	4974	0	69633	68701	0
m3r-pooled/hnsw_M_1	10515	4718	building index: loading tuples	7168	4998	0	69943	68701	0
m3r-pooled/hnsw_M_1	10575	4718	building index: loading tuples	7168	5018	0	70247	68701	0
m3r-pooled/hnsw_M_1	10636	4718	building index: loading tuples	7168	5041	0	70548	68701	0
m3r-pooled/hnsw_M_1	10696	4718	building index: loading tuples	7168	5061	0	70846	68701	0
m3r-pooled/hnsw_M_1	10756	4718	building index: loading tuples	7168	5081	0	71139	68701	0
m3r-pooled/hnsw_M_1	10817	4718	building index: loading tuples	7168	5105	0	71434	68701	0
m3r-pooled/hnsw_M_1	10877	4718	building index: loading tuples	7168	5125	0	71736	68701	0
m3r-pooled/hnsw_M_1	10937	4718	building index: loading tuples	7168	5149	0	72029	68701	0
m3r-pooled/hnsw_M_1	10998	4718	building index: loading tuples	7168	5168	0	72315	68701	0
m3r-pooled/hnsw_M_1	11058	4718	building index: loading tuples	7168	5192	0	72608	68701	0
m3r-pooled/hnsw_M_1	11119	4718	building index: loading tuples	7168	5203	0	72891	68701	0
m3r-pooled/hnsw_M_1	11180	4718	building index: loading tuples	7168	5226	0	73173	68701	0
m3r-pooled/hnsw_M_1	11240	4718	building index: loading tuples	7168	5246	0	73462	68701	0
m3r-pooled/hnsw_M_1	11301	4718	building index: loading tuples	7168	5270	0	73750	68701	0
m3r-pooled/hnsw_M_1	11362	4718	building index: loading tuples	7168	5290	0	74048	68701	0
m3r-pooled/hnsw_M_1	11423	4718	building index: loading tuples	7168	5309	0	74332	68701	0
m3r-pooled/hnsw_M_1	11483	4718	building index: loading tuples	7168	5333	0	74624	68701	0
m3r-pooled/hnsw_M_1	11544	4718	building index: loading tuples	7168	5352	0	74903	68701	0
m3r-pooled/hnsw_M_1	11604	4718	building index: loading tuples	7168	5376	0	75194	68701	0
m3r-pooled/hnsw_M_1	11664	4718	building index: loading tuples	7168	5396	0	75478	68701	0
m3r-pooled/hnsw_M_1	11725	4718	building index: loading tuples	7168	5411	0	75761	68701	0
m3r-pooled/hnsw_M_1	11786	4718	building index: loading tuples	7168	5430	0	76036	68701	0
m3r-pooled/hnsw_M_1	11846	4718	building index: loading tuples	7168	5453	0	76316	68701	0
m3r-pooled/hnsw_M_1	11907	4718	building index: loading tuples	7168	5473	0	76611	68701	0
m3r-pooled/hnsw_M_1	11967	4718	building index: loading tuples	7168	5497	0	76914	68701	0
m3r-pooled/hnsw_M_1	12027	4718	building index: loading tuples	7168	5516	0	77203	68701	0
m3r-pooled/hnsw_M_1	12087	4718	building index: loading tuples	7168	5540	0	77478	68701	0
m3r-pooled/hnsw_M_1	12148	4718	building index: loading tuples	7168	5551	0	77771	68701	0
m3r-pooled/hnsw_M_1	12208	4718	building index: loading tuples	7168	5575	0	78055	68701	0
m3r-pooled/hnsw_M_1	12268	4718	building index: loading tuples	7168	5594	0	78338	68701	0
m3r-pooled/hnsw_M_1	12328	4718	building index: loading tuples	7168	5618	0	78623	68701	0
m3r-pooled/hnsw_M_1	12389	4718	building index: loading tuples	7168	5637	0	78909	68701	0
m3r-pooled/hnsw_M_1	12449	4718	building index: loading tuples	7168	5657	0	79194	68701	0
m3r-pooled/hnsw_M_1	12509	4718	building index: loading tuples	7168	5682	0	79509	68701	0
m3r-pooled/hnsw_M_1	12569	4718	building index: loading tuples	7168	5699	0	79855	68701	0
m3r-pooled/hnsw_M_1	12629	4718	building index: loading tuples	7168	5723	0	80171	68701	0
m3r-pooled/hnsw_M_1	12690	4718	building index: loading tuples	7168	5747	0	80484	68701	0
m3r-pooled/hnsw_M_1	12750	4718	building index: loading tuples	7168	5771	0	80778	68701	0
m3r-pooled/hnsw_M_1	12811	4718	building index: loading tuples	7168	5790	0	81070	68701	0
m3r-pooled/hnsw_M_1	12871	4718	building index: loading tuples	7168	5814	0	81375	68701	0
m3r-pooled/hnsw_M_1	12931	4718	building index: loading tuples	7168	5835	0	81706	68701	0
m3r-pooled/hnsw_M_1	12991	4718	building index: loading tuples	7168	5859	0	82018	68701	0
m3r-pooled/hnsw_M_1	13051	4718	building index: loading tuples	7168	5879	0	82337	68701	0
m3r-pooled/hnsw_M_1	13113	4718	building index: loading tuples	7168	5904	0	82667	68701	0
m3r-pooled/hnsw_M_1	13173	4718	building index: loading tuples	7168	5929	0	82988	68701	0
m3r-pooled/hnsw_M_1	13234	4718	building index: loading tuples	7168	5953	0	83311	68701	0
m3r-pooled/hnsw_M_1	13296	4718	building index: loading tuples	7168	5973	0	83626	68701	0
m3r-pooled/hnsw_M_1	13359	4718	building index: loading tuples	7168	5993	0	83954	68701	0
m3r-pooled/hnsw_M_1	13421	4718	building index: loading tuples	7168	6018	0	84276	68701	0
m3r-pooled/hnsw_M_1	13481	4718	building index: loading tuples	7168	6038	0	84595	68701	0
m3r-pooled/hnsw_M_1	13542	4718	building index: loading tuples	7168	6063	0	84909	68701	0
m3r-pooled/hnsw_M_1	13602	4718	building index: loading tuples	7168	6083	0	85216	68701	0
m3r-pooled/hnsw_M_1	13662	4718	building index: loading tuples	7168	6107	0	85539	68701	0
m3r-pooled/hnsw_M_1	13722	4718	building index: loading tuples	7168	6127	0	85855	68701	0
m3r-pooled/hnsw_M_1	13783	4718	building index: loading tuples	7168	6151	0	86160	68701	0
m3r-pooled/hnsw_M_1	13843	4718	building index: loading tuples	7168	6171	0	86469	68701	0
m3r-pooled/hnsw_M_1	13903	4718	building index: loading tuples	7168	6195	0	86775	68701	0
m3r-pooled/hnsw_M_1	13963	4718	building index: loading tuples	7168	6215	0	87087	68701	0
m3r-pooled/hnsw_M_1	14024	4718	building index: loading tuples	7168	6248	0	87397	68701	0
m3r-pooled/hnsw_M_1	14084	4718	building index: loading tuples	7168	6268	0	87700	68701	0
m3r-pooled/hnsw_M_1	14146	4718	building index: loading tuples	7168	6283	0	87999	68701	0
m3r-pooled/hnsw_M_1	14209	4718	building index: loading tuples	7168	6312	0	88323	68701	0
m3r-pooled/hnsw_M_1	14272	4718	building index: loading tuples	7168	6327	0	88627	68701	0
m3r-pooled/hnsw_M_1	14334	4718	building index: loading tuples	7168	6347	0	88931	68701	0
m3r-pooled/hnsw_M_1	14396	4718	building index: loading tuples	7168	6371	0	89238	68701	0
m3r-pooled/hnsw_M_1	14459	4718	building index: loading tuples	7168	6395	0	89540	68701	0
m3r-pooled/hnsw_M_1	14521	4718	building index: loading tuples	7168	6414	0	89855	68701	0
m3r-pooled/hnsw_M_1	14584	4718	building index: loading tuples	7168	6442	0	90149	68701	0
m3r-pooled/hnsw_M_1	14646	4718	building index: loading tuples	7168	6461	0	90448	68701	0
m3r-pooled/hnsw_M_1	14708	4718	building index: loading tuples	7168	6485	0	90746	68701	0
m3r-pooled/hnsw_M_1	14771	4718	building index: loading tuples	7168	6505	0	91044	68701	0
m3r-pooled/hnsw_M_1	14833	4718	building index: loading tuples	7168	6528	0	91348	68701	0
m3r-pooled/hnsw_M_1	14896	4718	building index: loading tuples	7168	6552	0	91653	68701	0
m3r-pooled/hnsw_M_1	14958	4718	building index: loading tuples	7168	6563	0	91955	68701	0
m3r-pooled/hnsw_M_1	15021	4718	building index: loading tuples	7168	6587	0	92248	68701	0
m3r-pooled/hnsw_M_1	15083	4718	building index: loading tuples	7168	6607	0	92537	68701	0
m3r-pooled/hnsw_M_1	15145	4718	building index: loading tuples	7168	6630	0	92830	68701	0
m3r-pooled/hnsw_M_1	15207	4718	building index: loading tuples	7168	6653	0	93126	68701	0
m3r-pooled/hnsw_M_1	15269	4718	building index: loading tuples	7168	6673	0	93438	68701	0
m3r-pooled/hnsw_M_1	15332	4718	building index: loading tuples	7168	6697	0	93767	68701	0
m3r-pooled/hnsw_M_1	15394	4718	building index: loading tuples	7168	6721	0	94068	68701	0
m3r-pooled/hnsw_M_1	15454	4718	building index: loading tuples	7168	6741	0	94367	68701	0
m3r-pooled/hnsw_M_1	15514	4718	building index: loading tuples	7168	6763	0	94711	68701	0
m3r-pooled/hnsw_M_1	15575	4718	building index: loading tuples	7168	6787	0	95014	68701	0
m3r-pooled/hnsw_M_1	15635	4718	building index: loading tuples	7168	6807	0	95319	68701	0
m3r-pooled/hnsw_M_1	15695	4718	building index: loading tuples	7168	6830	0	95612	68701	0
m3r-pooled/hnsw_M_1	15756	4718	building index: loading tuples	7168	6850	0	95905	68701	0
m3r-pooled/hnsw_M_1	15816	4718	building index: loading tuples	7168	6870	0	96190	68701	0
m3r-pooled/hnsw_M_1	15876	4718	building index: loading tuples	7168	6894	0	96477	68701	0
m3r-pooled/hnsw_M_1	15936	4718	building index: loading tuples	7168	6913	0	96765	68701	0
m3r-pooled/hnsw_M_1	15997	4718	building index: loading tuples	7168	6933	0	97053	68701	0
m3r-pooled/hnsw_M_1	16057	4718	building index: loading tuples	7168	6954	0	97337	68701	0
m3r-pooled/hnsw_M_1	16117	4718	building index: loading tuples	7168	6976	0	97615	68701	0
m3r-pooled/hnsw_M_1	16177	4718	building index: loading tuples	7168	6993	0	97895	68701	0
m3r-pooled/hnsw_M_1	16238	4718	building index: loading tuples	7168	7013	0	98210	68701	0
m3r-pooled/hnsw_M_1	16298	4718	building index: loading tuples	7168	7035	0	98520	68701	0
m3r-pooled/hnsw_M_1	16358	4718	building index: loading tuples	7168	7060	0	98828	68701	0
m3r-pooled/hnsw_M_1	16419	4718	building index: loading tuples	7168	7081	0	99136	68701	0
m3r-pooled/hnsw_M_1	16479	4718	building index: loading tuples	7168	7103	0	99459	68701	0
m3r-pooled/hnsw_M_1	16539	4718	building index: loading tuples	7168	7128	0	99790	68701	0
m3r-pooled/hnsw_M_1	16599	4718	building index: loading tuples	7168	7168	0	99995	68701	0
m3r-pooled/hnsw_M_1	16660	4718	building index: loading tuples	7168	7168	0	99995	68701	0
m3r-pooled/hnsw_M_1	16722	4718	building index: loading tuples	7168	7168	0	99995	68701	0
m3r-pooled/hnsw_M_1	16783	4718	building index: loading tuples	7168	7168	0	99995	68701	0
m3r-pooled/hnsw_M_1	16843	4718	building index: loading tuples	7168	7168	0	99995	68701	0
m3r-pooled/hnsw_M_1	16905	4718	building index: loading tuples	7168	7168	0	99995	68701	0
m3r-pooled/hnsw_M_1	16966	4718	building index: loading tuples	7168	7168	0	99995	68701	0
m3r-pooled/hnsw_M_2	1	6154	building index: loading tuples	0	0	0	0	68701	0
m3r-pooled/hnsw_M_2	62	6154	building index: loading tuples	7168	20	0	243	68701	0
m3r-pooled/hnsw_M_2	121	6154	building index: loading tuples	7168	67	0	1007	68701	0
m3r-pooled/hnsw_M_2	186	6154	building index: loading tuples	7168	123	0	1752	68701	0
m3r-pooled/hnsw_M_2	248	6154	building index: loading tuples	7168	175	0	2495	68701	0
m3r-pooled/hnsw_M_2	310	6154	building index: loading tuples	7168	234	0	3289	68701	0
m3r-pooled/hnsw_M_2	373	6154	building index: loading tuples	7168	292	0	4050	68701	0
m3r-pooled/hnsw_M_2	451	6154	building index: loading tuples	7168	350	0	4857	68701	0
m3r-pooled/hnsw_M_2	512	6154	building index: loading tuples	7168	391	0	5508	68701	0
m3r-pooled/hnsw_M_2	572	6154	building index: loading tuples	7168	440	0	6158	68701	0
m3r-pooled/hnsw_M_2	632	6154	building index: loading tuples	7168	489	0	6795	68701	0
m3r-pooled/hnsw_M_2	692	6154	building index: loading tuples	7168	533	0	7426	68701	0
m3r-pooled/hnsw_M_2	752	6154	building index: loading tuples	7168	577	0	8049	68701	0
m3r-pooled/hnsw_M_2	812	6154	building index: loading tuples	7168	618	0	8655	68701	0
m3r-pooled/hnsw_M_2	872	6154	building index: loading tuples	7168	661	0	9249	68701	0
m3r-pooled/hnsw_M_2	932	6154	building index: loading tuples	7168	704	0	9822	68701	0
m3r-pooled/hnsw_M_2	992	6154	building index: loading tuples	7168	739	0	10383	68701	0
m3r-pooled/hnsw_M_2	1052	6154	building index: loading tuples	7168	781	0	10929	68701	0
m3r-pooled/hnsw_M_2	1112	6154	building index: loading tuples	7168	824	0	11483	68701	0
m3r-pooled/hnsw_M_2	1174	6154	building index: loading tuples	7168	858	0	12030	68701	0
m3r-pooled/hnsw_M_2	1239	6154	building index: loading tuples	7168	900	0	12574	68701	0
m3r-pooled/hnsw_M_2	1305	6154	building index: loading tuples	7168	933	0	13090	68701	0
m3r-pooled/hnsw_M_2	1369	6154	building index: loading tuples	7168	971	0	13604	68701	0
m3r-pooled/hnsw_M_2	1435	6154	building index: loading tuples	7168	1003	0	14102	68701	0
m3r-pooled/hnsw_M_2	1499	6154	building index: loading tuples	7168	1035	0	14584	68701	0
m3r-pooled/hnsw_M_2	1562	6154	building index: loading tuples	7168	1075	0	15055	68701	0
m3r-pooled/hnsw_M_2	1624	6154	building index: loading tuples	7168	1116	0	15543	68701	0
m3r-pooled/hnsw_M_2	1689	6154	building index: loading tuples	7168	1144	0	16015	68701	0
m3r-pooled/hnsw_M_2	1753	6154	building index: loading tuples	7168	1177	0	16498	68701	0
m3r-pooled/hnsw_M_2	1817	6154	building index: loading tuples	7168	1214	0	16994	68701	0
m3r-pooled/hnsw_M_2	1882	6154	building index: loading tuples	7168	1246	0	17444	68701	0
m3r-pooled/hnsw_M_2	1945	6154	building index: loading tuples	7168	1274	0	17886	68701	0
m3r-pooled/hnsw_M_2	2007	6154	building index: loading tuples	7168	1306	0	18330	68701	0
m3r-pooled/hnsw_M_2	2069	6154	building index: loading tuples	7168	1341	0	18762	68701	0
m3r-pooled/hnsw_M_2	2131	6154	building index: loading tuples	7168	1373	0	19232	68701	0
m3r-pooled/hnsw_M_2	2223	6154	building index: loading tuples	7168	1420	0	19837	68701	0
m3r-pooled/hnsw_M_2	2286	6154	building index: loading tuples	7168	1449	0	20266	68701	0
m3r-pooled/hnsw_M_2	2347	6154	building index: loading tuples	7168	1476	0	20669	68701	0
m3r-pooled/hnsw_M_2	2409	6154	building index: loading tuples	7168	1509	0	21111	68701	0
m3r-pooled/hnsw_M_2	2470	6154	building index: loading tuples	7168	1537	0	21529	68701	0
m3r-pooled/hnsw_M_2	2532	6154	building index: loading tuples	7168	1565	0	21918	68701	0
m3r-pooled/hnsw_M_2	2593	6154	building index: loading tuples	7168	1594	0	22357	68701	0
m3r-pooled/hnsw_M_2	2655	6154	building index: loading tuples	7168	1626	0	22767	68701	0
m3r-pooled/hnsw_M_2	2717	6154	building index: loading tuples	7168	1653	0	23174	68701	0
m3r-pooled/hnsw_M_2	2786	6154	building index: loading tuples	7168	1683	0	23684	68701	0
m3r-pooled/hnsw_M_2	2848	6154	building index: loading tuples	7168	1728	0	24177	68701	0
m3r-pooled/hnsw_M_2	2913	6154	building index: loading tuples	7168	1758	0	24632	68701	0
m3r-pooled/hnsw_M_2	2977	6154	building index: loading tuples	7168	1787	0	25085	68701	0
m3r-pooled/hnsw_M_2	3041	6154	building index: loading tuples	7168	1821	0	25529	68701	0
m3r-pooled/hnsw_M_2	3106	6154	building index: loading tuples	7168	1854	0	25970	68701	0
m3r-pooled/hnsw_M_2	3172	6154	building index: loading tuples	7168	1882	0	26399	68701	0
m3r-pooled/hnsw_M_2	3237	6154	building index: loading tuples	7168	1910	0	26817	68701	0
m3r-pooled/hnsw_M_2	3300	6154	building index: loading tuples	7168	1942	0	27200	68701	0
m3r-pooled/hnsw_M_2	3367	6154	building index: loading tuples	7168	1966	0	27618	68701	0
m3r-pooled/hnsw_M_2	3429	6154	building index: loading tuples	7168	1998	0	28025	68701	0
m3r-pooled/hnsw_M_2	3494	6154	building index: loading tuples	7168	2026	0	28415	68701	0
m3r-pooled/hnsw_M_2	3557	6154	building index: loading tuples	7168	2054	0	28823	68701	0
m3r-pooled/hnsw_M_2	3619	6154	building index: loading tuples	7168	2087	0	29246	68701	0
m3r-pooled/hnsw_M_2	3684	6154	building index: loading tuples	7168	2111	0	29689	68701	0
m3r-pooled/hnsw_M_2	3749	6154	building index: loading tuples	7168	2152	0	30135	68701	0
m3r-pooled/hnsw_M_2	3811	6154	building index: loading tuples	7168	2196	0	30691	68701	0
m3r-pooled/hnsw_M_2	3874	6154	building index: loading tuples	7168	2232	0	31186	68701	0
m3r-pooled/hnsw_M_2	3939	6154	building index: loading tuples	7168	2268	0	31682	68701	0
m3r-pooled/hnsw_M_2	4004	6154	building index: loading tuples	7168	2291	0	32134	68701	0
m3r-pooled/hnsw_M_2	4071	6154	building index: loading tuples	7168	2325	0	32558	68701	0
m3r-pooled/hnsw_M_2	4139	6154	building index: loading tuples	7168	2360	0	33024	68701	0
m3r-pooled/hnsw_M_2	4206	6154	building index: loading tuples	7168	2386	0	33383	68701	0
m3r-pooled/hnsw_M_2	4268	6154	building index: loading tuples	7168	2407	0	33733	68701	0
m3r-pooled/hnsw_M_2	4329	6154	building index: loading tuples	7168	2436	0	34077	68701	0
m3r-pooled/hnsw_M_2	4391	6154	building index: loading tuples	7168	2461	0	34426	68701	0
m3r-pooled/hnsw_M_2	4452	6154	building index: loading tuples	7168	2482	0	34775	68701	0
m3r-pooled/hnsw_M_2	4516	6154	building index: loading tuples	7168	2507	0	35127	68701	0
m3r-pooled/hnsw_M_2	4577	6154	building index: loading tuples	7168	2536	0	35480	68701	0
m3r-pooled/hnsw_M_2	4639	6154	building index: loading tuples	7168	2562	0	35837	68701	0
m3r-pooled/hnsw_M_2	4701	6154	building index: loading tuples	7168	2592	0	36211	68701	0
m3r-pooled/hnsw_M_2	4762	6154	building index: loading tuples	7168	2614	0	36595	68701	0
m3r-pooled/hnsw_M_2	4824	6154	building index: loading tuples	7168	2644	0	36979	68701	0
m3r-pooled/hnsw_M_2	4887	6154	building index: loading tuples	7168	2670	0	37390	68701	0
m3r-pooled/hnsw_M_2	4952	6154	building index: loading tuples	7168	2706	0	37927	68701	0
m3r-pooled/hnsw_M_2	5014	6154	building index: loading tuples	7168	2749	0	38487	68701	0
m3r-pooled/hnsw_M_2	5076	6154	building index: loading tuples	7168	2792	0	39057	68701	0
m3r-pooled/hnsw_M_2	5138	6154	building index: loading tuples	7168	2830	0	39643	68701	0
m3r-pooled/hnsw_M_2	5202	6154	building index: loading tuples	7168	2862	0	40067	68701	0
m3r-pooled/hnsw_M_2	5264	6154	building index: loading tuples	7168	2887	0	40452	68701	0
m3r-pooled/hnsw_M_2	5326	6154	building index: loading tuples	7168	2918	0	40855	68701	0
m3r-pooled/hnsw_M_2	5388	6154	building index: loading tuples	7168	2954	0	41316	68701	0
m3r-pooled/hnsw_M_2	5451	6154	building index: loading tuples	7168	2985	0	41737	68701	0
m3r-pooled/hnsw_M_2	5511	6154	building index: loading tuples	7168	3011	0	42136	68701	0
m3r-pooled/hnsw_M_2	5573	6154	building index: loading tuples	7168	3041	0	42533	68701	0
m3r-pooled/hnsw_M_2	5636	6154	building index: loading tuples	7168	3072	0	42940	68701	0
m3r-pooled/hnsw_M_2	5699	6154	building index: loading tuples	7168	3095	0	43343	68701	0
m3r-pooled/hnsw_M_2	5762	6154	building index: loading tuples	7168	3126	0	43748	68701	0
m3r-pooled/hnsw_M_2	5824	6154	building index: loading tuples	7168	3156	0	44164	68701	0
m3r-pooled/hnsw_M_2	5886	6154	building index: loading tuples	7168	3182	0	44545	68701	0
m3r-pooled/hnsw_M_2	5948	6154	building index: loading tuples	7168	3213	0	44939	68701	0
m3r-pooled/hnsw_M_2	6012	6154	building index: loading tuples	7168	3235	0	45337	68701	0
m3r-pooled/hnsw_M_2	6075	6154	building index: loading tuples	7168	3269	0	45743	68701	0
m3r-pooled/hnsw_M_2	6136	6154	building index: loading tuples	7168	3291	0	46115	68701	0
m3r-pooled/hnsw_M_2	6196	6154	building index: loading tuples	7168	3321	0	46488	68701	0
m3r-pooled/hnsw_M_2	6257	6154	building index: loading tuples	7168	3347	0	46873	68701	0
m3r-pooled/hnsw_M_2	6319	6154	building index: loading tuples	7168	3378	0	47285	68701	0
m3r-pooled/hnsw_M_2	6381	6154	building index: loading tuples	7168	3408	0	47645	68701	0
m3r-pooled/hnsw_M_2	6443	6154	building index: loading tuples	7168	3430	0	48009	68701	0
m3r-pooled/hnsw_M_2	6506	6154	building index: loading tuples	7168	3460	0	48393	68701	0
m3r-pooled/hnsw_M_2	6569	6154	building index: loading tuples	7168	3486	0	48766	68701	0
m3r-pooled/hnsw_M_2	6630	6154	building index: loading tuples	7168	3516	0	49170	68701	0
m3r-pooled/hnsw_M_2	6695	6154	building index: loading tuples	7168	3543	0	49578	68701	0
m3r-pooled/hnsw_M_2	6758	6154	building index: loading tuples	7168	3573	0	49973	68701	0
m3r-pooled/hnsw_M_2	6820	6154	building index: loading tuples	7168	3595	0	50373	68701	0
m3r-pooled/hnsw_M_2	6882	6154	building index: loading tuples	7168	3626	0	50806	68701	0
m3r-pooled/hnsw_M_2	6943	6154	building index: loading tuples	7168	3658	0	51246	68701	0
m3r-pooled/hnsw_M_2	7004	6154	building index: loading tuples	7168	3689	0	51669	68701	0
m3r-pooled/hnsw_M_2	7067	6154	building index: loading tuples	7168	3724	0	52093	68701	0
m3r-pooled/hnsw_M_2	7129	6154	building index: loading tuples	7168	3756	0	52545	68701	0
m3r-pooled/hnsw_M_2	7191	6154	building index: loading tuples	7168	3788	0	52990	68701	0
m3r-pooled/hnsw_M_2	7255	6154	building index: loading tuples	7168	3815	0	53441	68701	0
m3r-pooled/hnsw_M_2	7317	6154	building index: loading tuples	7168	3856	0	53914	68701	0
m3r-pooled/hnsw_M_2	7383	6154	building index: loading tuples	7168	3879	0	54362	68701	0
m3r-pooled/hnsw_M_2	7444	6154	building index: loading tuples	7168	3911	0	54816	68701	0
m3r-pooled/hnsw_M_2	7505	6154	building index: loading tuples	7168	3947	0	55256	68701	0
m3r-pooled/hnsw_M_2	7569	6154	building index: loading tuples	7168	3979	0	55694	68701	0
m3r-pooled/hnsw_M_2	7633	6154	building index: loading tuples	7168	4010	0	56122	68701	0
m3r-pooled/hnsw_M_2	7697	6154	building index: loading tuples	7168	4042	0	56593	68701	0
m3r-pooled/hnsw_M_2	7758	6154	building index: loading tuples	7168	4073	0	57004	68701	0
m3r-pooled/hnsw_M_2	7819	6154	building index: loading tuples	7168	4105	0	57447	68701	0
m3r-pooled/hnsw_M_2	7879	6154	building index: loading tuples	7168	4131	0	57876	68701	0
m3r-pooled/hnsw_M_2	7940	6154	building index: loading tuples	7168	4162	0	58295	68701	0
m3r-pooled/hnsw_M_2	8001	6154	building index: loading tuples	7168	4196	0	58662	68701	0
m3r-pooled/hnsw_M_2	8061	6154	building index: loading tuples	7168	4217	0	59032	68701	0
m3r-pooled/hnsw_M_2	8121	6154	building index: loading tuples	7168	4242	0	59373	68701	0
m3r-pooled/hnsw_M_2	8181	6154	building index: loading tuples	7168	4263	0	59726	68701	0
m3r-pooled/hnsw_M_2	8245	6154	building index: loading tuples	7168	4296	0	60101	68701	0
m3r-pooled/hnsw_M_2	8308	6154	building index: loading tuples	7168	4321	0	60485	68701	0
m3r-pooled/hnsw_M_2	8371	6154	building index: loading tuples	7168	4346	0	60866	68701	0
m3r-pooled/hnsw_M_2	8434	6154	building index: loading tuples	7168	4380	0	61252	68701	0
m3r-pooled/hnsw_M_2	8497	6154	building index: loading tuples	7168	4400	0	61573	68701	0
m3r-pooled/hnsw_M_2	8557	6154	building index: loading tuples	7168	4424	0	61882	68701	0
m3r-pooled/hnsw_M_2	8617	6154	building index: loading tuples	7168	4444	0	62194	68701	0
m3r-pooled/hnsw_M_2	8677	6154	building index: loading tuples	7168	4468	0	62509	68701	0
m3r-pooled/hnsw_M_2	8737	6154	building index: loading tuples	7168	4488	0	62811	68701	0
m3r-pooled/hnsw_M_2	8797	6154	building index: loading tuples	7168	4509	0	63123	68701	0
m3r-pooled/hnsw_M_2	8858	6154	building index: loading tuples	7168	4533	0	63438	68701	0
m3r-pooled/hnsw_M_2	8918	6154	building index: loading tuples	7168	4553	0	63743	68701	0
m3r-pooled/hnsw_M_2	8978	6154	building index: loading tuples	7168	4567	0	64044	68701	0
m3r-pooled/hnsw_M_2	9037	6154	building index: loading tuples	7168	4596	0	64341	68701	0
m3r-pooled/hnsw_M_2	9097	6154	building index: loading tuples	7168	4610	0	64640	68701	0
m3r-pooled/hnsw_M_2	9156	6154	building index: loading tuples	7168	4640	0	64941	68701	0
m3r-pooled/hnsw_M_2	9217	6154	building index: loading tuples	7168	4657	0	65239	68701	0
m3r-pooled/hnsw_M_2	9278	6154	building index: loading tuples	7168	4675	0	65540	68701	0
m3r-pooled/hnsw_M_2	9338	6154	building index: loading tuples	7168	4705	0	65847	68701	0
m3r-pooled/hnsw_M_2	9397	6154	building index: loading tuples	7168	4719	0	66177	68701	0
m3r-pooled/hnsw_M_2	9457	6154	building index: loading tuples	7168	4749	0	66482	68701	0
m3r-pooled/hnsw_M_2	9516	6154	building index: loading tuples	7168	4763	0	66778	68701	0
m3r-pooled/hnsw_M_2	9576	6154	building index: loading tuples	7168	4793	0	67086	68701	0
m3r-pooled/hnsw_M_2	9636	6154	building index: loading tuples	7168	4806	0	67384	68701	0
m3r-pooled/hnsw_M_2	9696	6154	building index: loading tuples	7168	4836	0	67688	68701	0
m3r-pooled/hnsw_M_2	9756	6154	building index: loading tuples	7168	4850	0	67998	68701	0
m3r-pooled/hnsw_M_2	9815	6154	building index: loading tuples	7168	4880	0	68317	68701	0
m3r-pooled/hnsw_M_2	9875	6154	building index: loading tuples	7168	4912	0	68716	68701	0
m3r-pooled/hnsw_M_2	9935	6154	building index: loading tuples	7168	4926	0	69060	68701	0
m3r-pooled/hnsw_M_2	9996	6154	building index: loading tuples	7168	4957	0	69387	68701	0
m3r-pooled/hnsw_M_2	10056	6154	building index: loading tuples	7168	4971	0	69703	68701	0
m3r-pooled/hnsw_M_2	10115	6154	building index: loading tuples	7168	5001	0	70021	68701	0
m3r-pooled/hnsw_M_2	10175	6154	building index: loading tuples	7168	5015	0	70327	68701	0
m3r-pooled/hnsw_M_2	10235	6154	building index: loading tuples	7168	5044	0	70637	68701	0
m3r-pooled/hnsw_M_2	10298	6154	building index: loading tuples	7168	5066	0	70952	68701	0
m3r-pooled/hnsw_M_2	10358	6154	building index: loading tuples	7168	5093	0	71298	68701	0
m3r-pooled/hnsw_M_2	10429	6154	building index: loading tuples	7168	5111	0	71595	68701	0
m3r-pooled/hnsw_M_2	10490	6154	building index: loading tuples	7168	5137	0	71898	68701	0
m3r-pooled/hnsw_M_2	10550	6154	building index: loading tuples	7168	5154	0	72192	68701	0
m3r-pooled/hnsw_M_2	10611	6154	building index: loading tuples	7168	5180	0	72484	68701	0
m3r-pooled/hnsw_M_2	10671	6154	building index: loading tuples	7168	5197	0	72777	68701	0
m3r-pooled/hnsw_M_2	10731	6154	building index: loading tuples	7168	5211	0	73073	68701	0
m3r-pooled/hnsw_M_2	10791	6154	building index: loading tuples	7168	5241	0	73370	68701	0
m3r-pooled/hnsw_M_2	10851	6154	building index: loading tuples	7168	5255	0	73695	68701	0
m3r-pooled/hnsw_M_2	10911	6154	building index: loading tuples	7168	5285	0	74002	68701	0
m3r-pooled/hnsw_M_2	10970	6154	building index: loading tuples	7168	5299	0	74302	68701	0
m3r-pooled/hnsw_M_2	11031	6154	building index: loading tuples	7168	5329	0	74608	68701	0
m3r-pooled/hnsw_M_2	11091	6154	building index: loading tuples	7168	5343	0	74907	68701	0
m3r-pooled/hnsw_M_2	11151	6154	building index: loading tuples	7168	5373	0	75218	68701	0
m3r-pooled/hnsw_M_2	11211	6154	building index: loading tuples	7168	5387	0	75509	68701	0
m3r-pooled/hnsw_M_2	11271	6154	building index: loading tuples	7168	5416	0	75808	68701	0
m3r-pooled/hnsw_M_2	11330	6154	building index: loading tuples	7168	5430	0	76110	68701	0
m3r-pooled/hnsw_M_2	11390	6154	building index: loading tuples	7168	5461	0	76440	68701	0
m3r-pooled/hnsw_M_2	11455	6154	building index: loading tuples	7168	5475	0	76763	68701	0
m3r-pooled/hnsw_M_2	11517	6154	building index: loading tuples	7168	5504	0	77055	68701	0
m3r-pooled/hnsw_M_2	11580	6154	building index: loading tuples	7168	5518	0	77354	68701	0
m3r-pooled/hnsw_M_2	11643	6154	building index: loading tuples	7168	5548	0	77655	68701	0
m3r-pooled/hnsw_M_2	11703	6154	building index: loading tuples	7168	5566	0	77947	68701	0
m3r-pooled/hnsw_M_2	11764	6154	building index: loading tuples	7168	5583	0	78238	68701	0
m3r-pooled/hnsw_M_2	11827	6154	building index: loading tuples	7168	5608	0	78532	68701	0
m3r-pooled/hnsw_M_2	11889	6154	building index: loading tuples	7168	5626	0	78826	68701	0
m3r-pooled/hnsw_M_2	11951	6154	building index: loading tuples	7168	5652	0	79120	68701	0
m3r-pooled/hnsw_M_2	12012	6154	building index: loading tuples	7168	5670	0	79439	68701	0
m3r-pooled/hnsw_M_2	12073	6154	building index: loading tuples	7168	5698	0	79805	68701	0
m3r-pooled/hnsw_M_2	12134	6154	building index: loading tuples	7168	5728	0	80135	68701	0
m3r-pooled/hnsw_M_2	12197	6154	building index: loading tuples	7168	5743	0	80478	68701	0
m3r-pooled/hnsw_M_2	12261	6154	building index: loading tuples	7168	5770	0	80798	68701	0
m3r-pooled/hnsw_M_2	12325	6154	building index: loading tuples	7168	5796	0	81107	68701	0
m3r-pooled/hnsw_M_2	12389	6154	building index: loading tuples	7168	5809	0	81377	68701	0
m3r-pooled/hnsw_M_2	12449	6154	building index: loading tuples	7168	5827	0	81663	68701	0
m3r-pooled/hnsw_M_2	12508	6154	building index: loading tuples	7168	5854	0	81960	68701	0
m3r-pooled/hnsw_M_2	12568	6154	building index: loading tuples	7168	5876	0	82279	68701	0
m3r-pooled/hnsw_M_2	12631	6154	building index: loading tuples	7168	5895	0	82597	68701	0
m3r-pooled/hnsw_M_2	12702	6154	building index: loading tuples	7168	5922	0	82973	68701	0
m3r-pooled/hnsw_M_2	12763	6154	building index: loading tuples	7168	5949	0	83298	68701	0
m3r-pooled/hnsw_M_2	12826	6154	building index: loading tuples	7168	5967	0	83604	68701	0
m3r-pooled/hnsw_M_2	12898	6154	building index: loading tuples	7168	5995	0	83980	68701	0
m3r-pooled/hnsw_M_2	12958	6154	building index: loading tuples	7168	6017	0	84297	68701	0
m3r-pooled/hnsw_M_2	13020	6154	building index: loading tuples	7168	6048	0	84623	68701	0
m3r-pooled/hnsw_M_2	13081	6154	building index: loading tuples	7168	6062	0	84938	68701	0
m3r-pooled/hnsw_M_2	13144	6154	building index: loading tuples	7168	6092	0	85262	68701	0
m3r-pooled/hnsw_M_2	13205	6154	building index: loading tuples	7168	6110	0	85597	68701	0
m3r-pooled/hnsw_M_2	13266	6154	building index: loading tuples	7168	6137	0	85936	68701	0
m3r-pooled/hnsw_M_2	13328	6154	building index: loading tuples	7168	6164	0	86235	68701	0
m3r-pooled/hnsw_M_2	13389	6154	building index: loading tuples	7168	6178	0	86540	68701	0
m3r-pooled/hnsw_M_2	13449	6154	building index: loading tuples	7168	6204	0	86839	68701	0
m3r-pooled/hnsw_M_2	13508	6154	building index: loading tuples	7168	6218	0	87141	68701	0
m3r-pooled/hnsw_M_2	13571	6154	building index: loading tuples	7168	6249	0	87468	68701	0
m3r-pooled/hnsw_M_2	13632	6154	building index: loading tuples	7168	6263	0	87774	68701	0
m3r-pooled/hnsw_M_2	13696	6154	building index: loading tuples	7168	6291	0	88155	68701	0
m3r-pooled/hnsw_M_2	13766	6154	building index: loading tuples	7168	6317	0	88468	68701	0
m3r-pooled/hnsw_M_2	13827	6154	building index: loading tuples	7168	6348	0	88803	68701	0
m3r-pooled/hnsw_M_2	13888	6154	building index: loading tuples	7168	6362	0	89132	68701	0
m3r-pooled/hnsw_M_2	13949	6154	building index: loading tuples	7168	6388	0	89438	68701	0
m3r-pooled/hnsw_M_2	14010	6154	building index: loading tuples	7168	6406	0	89743	68701	0
m3r-pooled/hnsw_M_2	14081	6154	building index: loading tuples	7168	6440	0	90136	68701	0
m3r-pooled/hnsw_M_2	14145	6154	building index: loading tuples	7168	6464	0	90493	68701	0
m3r-pooled/hnsw_M_2	14207	6154	building index: loading tuples	7168	6489	0	90862	68701	0
m3r-pooled/hnsw_M_2	14270	6154	building index: loading tuples	7168	6514	0	91233	68701	0
m3r-pooled/hnsw_M_2	14334	6154	building index: loading tuples	7168	6542	0	91589	68701	0
m3r-pooled/hnsw_M_2	14397	6154	building index: loading tuples	7168	6568	0	91903	68701	0
m3r-pooled/hnsw_M_2	14460	6154	building index: loading tuples	7168	6581	0	92206	68701	0
m3r-pooled/hnsw_M_2	14521	6154	building index: loading tuples	7168	6603	0	92495	68701	0
m3r-pooled/hnsw_M_2	14582	6154	building index: loading tuples	7168	6628	0	92787	68701	0
m3r-pooled/hnsw_M_2	14644	6154	building index: loading tuples	7168	6646	0	93089	68701	0
m3r-pooled/hnsw_M_2	14707	6154	building index: loading tuples	7168	6672	0	93387	68701	0
m3r-pooled/hnsw_M_2	14770	6154	building index: loading tuples	7168	6689	0	93681	68701	0
m3r-pooled/hnsw_M_2	14833	6154	building index: loading tuples	7168	6707	0	93978	68701	0
m3r-pooled/hnsw_M_2	14896	6154	building index: loading tuples	7168	6737	0	94277	68701	0
m3r-pooled/hnsw_M_2	14957	6154	building index: loading tuples	7168	6760	0	94631	68701	0
m3r-pooled/hnsw_M_2	15022	6154	building index: loading tuples	7168	6775	0	94972	68701	0
m3r-pooled/hnsw_M_2	15086	6154	building index: loading tuples	7168	6806	0	95302	68701	0
m3r-pooled/hnsw_M_2	15149	6154	building index: loading tuples	7168	6819	0	95609	68701	0
m3r-pooled/hnsw_M_2	15214	6154	building index: loading tuples	7168	6849	0	95918	68701	0
m3r-pooled/hnsw_M_2	15277	6154	building index: loading tuples	7168	6863	0	96217	68701	0
m3r-pooled/hnsw_M_2	15342	6154	building index: loading tuples	7168	6893	0	96523	68701	0
m3r-pooled/hnsw_M_2	15405	6154	building index: loading tuples	7168	6910	0	96823	68701	0
m3r-pooled/hnsw_M_2	15467	6154	building index: loading tuples	7168	6938	0	97119	68701	0
m3r-pooled/hnsw_M_2	15531	6154	building index: loading tuples	7168	6951	0	97413	68701	0
m3r-pooled/hnsw_M_2	15594	6154	building index: loading tuples	7168	6977	0	97703	68701	0
m3r-pooled/hnsw_M_2	15656	6154	building index: loading tuples	7168	6998	0	97997	68701	0
m3r-pooled/hnsw_M_2	15719	6154	building index: loading tuples	7168	7020	0	98302	68701	0
m3r-pooled/hnsw_M_2	15782	6154	building index: loading tuples	7168	7042	0	98603	68701	0
m3r-pooled/hnsw_M_2	15846	6154	building index: loading tuples	7168	7066	0	98941	68701	0
m3r-pooled/hnsw_M_2	15910	6154	building index: loading tuples	7168	7090	0	99263	68701	0
m3r-pooled/hnsw_M_2	15973	6154	building index: loading tuples	7168	7112	0	99592	68701	0
m3r-pooled/hnsw_M_2	16036	6154	building index: loading tuples	7168	7135	0	99891	68701	0
m3r-pooled/hnsw_M_2	16099	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	16159	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	16220	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	16281	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	16342	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	16404	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	16464	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	16525	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	16586	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	16647	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	16709	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	16771	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	16833	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	16894	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	16955	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	17016	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	17078	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	17140	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	17202	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	17264	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	17324	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	17386	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	17448	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_2	17510	6154	building index: loading tuples	7168	7168	0	99997	68701	0
m3r-pooled/hnsw_M_3	1	7630	building index: loading tuples	0	0	0	0	68701	0
m3r-pooled/hnsw_M_3	61	7630	building index: loading tuples	7168	23	0	331	68701	0
m3r-pooled/hnsw_M_3	122	7630	building index: loading tuples	7168	90	0	1274	68701	0
m3r-pooled/hnsw_M_3	184	7630	building index: loading tuples	7168	145	0	2023	68701	0
m3r-pooled/hnsw_M_3	245	7630	building index: loading tuples	7168	200	0	2761	68701	0
m3r-pooled/hnsw_M_3	307	7630	building index: loading tuples	7168	246	0	3511	68701	0
m3r-pooled/hnsw_M_3	369	7630	building index: loading tuples	7168	297	0	4148	68701	0
m3r-pooled/hnsw_M_3	439	7630	building index: loading tuples	7168	357	0	4979	68701	0
m3r-pooled/hnsw_M_3	502	7630	building index: loading tuples	7168	416	0	5744	68701	0
m3r-pooled/hnsw_M_3	564	7630	building index: loading tuples	7168	462	0	6519	68701	0
m3r-pooled/hnsw_M_3	627	7630	building index: loading tuples	7168	521	0	7288	68701	0
m3r-pooled/hnsw_M_3	690	7630	building index: loading tuples	7168	574	0	8014	68701	0
m3r-pooled/hnsw_M_3	754	7630	building index: loading tuples	7168	623	0	8730	68701	0
m3r-pooled/hnsw_M_3	817	7630	building index: loading tuples	7168	671	0	9412	68701	0
m3r-pooled/hnsw_M_3	880	7630	building index: loading tuples	7168	719	0	10054	68701	0
m3r-pooled/hnsw_M_3	945	7630	building index: loading tuples	7168	766	0	10712	68701	0
m3r-pooled/hnsw_M_3	1008	7630	building index: loading tuples	7168	810	0	11362	68701	0
m3r-pooled/hnsw_M_3	1071	7630	building index: loading tuples	7168	854	0	11997	68701	0
m3r-pooled/hnsw_M_3	1136	7630	building index: loading tuples	7168	901	0	12611	68701	0
m3r-pooled/hnsw_M_3	1198	7630	building index: loading tuples	7168	938	0	13146	68701	0
m3r-pooled/hnsw_M_3	1258	7630	building index: loading tuples	7168	974	0	13662	68701	0
m3r-pooled/hnsw_M_3	1318	7630	building index: loading tuples	7168	1011	0	14201	68701	0
m3r-pooled/hnsw_M_3	1379	7630	building index: loading tuples	7168	1057	0	14774	68701	0
m3r-pooled/hnsw_M_3	1443	7630	building index: loading tuples	7168	1097	0	15319	68701	0
m3r-pooled/hnsw_M_3	1504	7630	building index: loading tuples	7168	1132	0	15834	68701	0
m3r-pooled/hnsw_M_3	1565	7630	building index: loading tuples	7168	1163	0	16348	68701	0
m3r-pooled/hnsw_M_3	1626	7630	building index: loading tuples	7168	1199	0	16862	68701	0
m3r-pooled/hnsw_M_3	1691	7630	building index: loading tuples	7168	1249	0	17457	68701	0
m3r-pooled/hnsw_M_3	1761	7630	building index: loading tuples	7168	1285	0	17976	68701	0
m3r-pooled/hnsw_M_3	1827	7630	building index: loading tuples	7168	1324	0	18486	68701	0
m3r-pooled/hnsw_M_3	1891	7630	building index: loading tuples	7168	1351	0	18985	68701	0
m3r-pooled/hnsw_M_3	1953	7630	building index: loading tuples	7168	1390	0	19465	68701	0
m3r-pooled/hnsw_M_3	2013	7630	building index: loading tuples	7168	1426	0	19969	68701	0
m3r-pooled/hnsw_M_3	2073	7630	building index: loading tuples	7168	1461	0	20455	68701	0
m3r-pooled/hnsw_M_3	2134	7630	building index: loading tuples	7168	1498	0	20977	68701	0
m3r-pooled/hnsw_M_3	2194	7630	building index: loading tuples	7168	1534	0	21475	68701	0
m3r-pooled/hnsw_M_3	2254	7630	building index: loading tuples	7168	1569	0	21952	68701	0
m3r-pooled/hnsw_M_3	2314	7630	building index: loading tuples	7168	1605	0	22467	68701	0
m3r-pooled/hnsw_M_3	2374	7630	building index: loading tuples	7168	1641	0	22944	68701	0
m3r-pooled/hnsw_M_3	2433	7630	building index: loading tuples	7168	1676	0	23405	68701	0
m3r-pooled/hnsw_M_3	2493	7630	building index: loading tuples	7168	1714	0	23990	68701	0
m3r-pooled/hnsw_M_3	2553	7630	building index: loading tuples	7168	1751	0	24530	68701	0
m3r-pooled/hnsw_M_3	2613	7630	building index: loading tuples	7168	1787	0	25033	68701	0
m3r-pooled/hnsw_M_3	2675	7630	building index: loading tuples	7168	1823	0	25505	68701	0
m3r-pooled/hnsw_M_3	2738	7630	building index: loading tuples	7168	1854	0	25968	68701	0
m3r-pooled/hnsw_M_3	2799	7630	building index: loading tuples	7168	1890	0	26445	68701	0
m3r-pooled/hnsw_M_3	2861	7630	building index: loading tuples	7168	1925	0	26932	68701	0
m3r-pooled/hnsw_M_3	2921	7630	building index: loading tuples	7168	1961	0	27416	68701	0
m3r-pooled/hnsw_M_3	2982	7630	building index: loading tuples	7168	1996	0	27908	68701	0
m3r-pooled/hnsw_M_3	3043	7630	building index: loading tuples	7168	2032	0	28396	68701	0
m3r-pooled/hnsw_M_3	3103	7630	building index: loading tuples	7168	2059	0	28879	68701	0
m3r-pooled/hnsw_M_3	3163	7630	building index: loading tuples	7168	2095	0	29366	68701	0
m3r-pooled/hnsw_M_3	3223	7630	building index: loading tuples	7168	2130	0	29841	68701	0
m3r-pooled/hnsw_M_3	3284	7630	building index: loading tuples	7168	2176	0	30422	68701	0
m3r-pooled/hnsw_M_3	3343	7630	building index: loading tuples	7168	2212	0	30938	68701	0
m3r-pooled/hnsw_M_3	3403	7630	building index: loading tuples	7168	2239	0	31402	68701	0
m3r-pooled/hnsw_M_3	3463	7630	building index: loading tuples	7168	2274	0	31846	68701	0
m3r-pooled/hnsw_M_3	3523	7630	building index: loading tuples	7168	2308	0	32283	68701	0
m3r-pooled/hnsw_M_3	3586	7630	building index: loading tuples	7168	2335	0	32728	68701	0
m3r-pooled/hnsw_M_3	3646	7630	building index: loading tuples	7168	2368	0	33144	68701	0
m3r-pooled/hnsw_M_3	3706	7630	building index: loading tuples	7168	2395	0	33578	68701	0
m3r-pooled/hnsw_M_3	3768	7630	building index: loading tuples	7168	2429	0	34001	68701	0
m3r-pooled/hnsw_M_3	3828	7630	building index: loading tuples	7168	2455	0	34414	68701	0
m3r-pooled/hnsw_M_3	3888	7630	building index: loading tuples	7168	2489	0	34836	68701	0
m3r-pooled/hnsw_M_3	3950	7630	building index: loading tuples	7168	2515	0	35264	68701	0
m3r-pooled/hnsw_M_3	4010	7630	building index: loading tuples	7168	2550	0	35691	68701	0
m3r-pooled/hnsw_M_3	4071	7630	building index: loading tuples	7168	2585	0	36134	68701	0
m3r-pooled/hnsw_M_3	4132	7630	building index: loading tuples	7168	2616	0	36610	68701	0
m3r-pooled/hnsw_M_3	4192	7630	building index: loading tuples	7168	2642	0	37052	68701	0
m3r-pooled/hnsw_M_3	4253	7630	building index: loading tuples	7168	2680	0	37472	68701	0
m3r-pooled/hnsw_M_3	4313	7630	building index: loading tuples	7168	2718	0	38061	68701	0
m3r-pooled/hnsw_M_3	4374	7630	building index: loading tuples	7168	2767	0	38739	68701	0
m3r-pooled/hnsw_M_3	4435	7630	building index: loading tuples	7168	2820	0	39457	68701	0
m3r-pooled/hnsw_M_3	4495	7630	building index: loading tuples	7168	2857	0	40007	68701	0
m3r-pooled/hnsw_M_3	4555	7630	building index: loading tuples	7168	2892	0	40467	68701	0
m3r-pooled/hnsw_M_3	4616	7630	building index: loading tuples	7168	2919	0	40944	68701	0
m3r-pooled/hnsw_M_3	4676	7630	building index: loading tuples	7168	2959	0	41445	68701	0
m3r-pooled/hnsw_M_3	4736	7630	building index: loading tuples	7168	2994	0	41909	68701	0
m3r-pooled/hnsw_M_3	4796	7630	building index: loading tuples	7168	3029	0	42378	68701	0
m3r-pooled/hnsw_M_3	4857	7630	building index: loading tuples	7168	3064	0	42853	68701	0
m3r-pooled/hnsw_M_3	4918	7630	building index: loading tuples	7168	3091	0	43317	68701	0
m3r-pooled/hnsw_M_3	4978	7630	building index: loading tuples	7168	3126	0	43790	68701	0
m3r-pooled/hnsw_M_3	5038	7630	building index: loading tuples	7168	3161	0	44264	68701	0
m3r-pooled/hnsw_M_3	5098	7630	building index: loading tuples	7168	3187	0	44696	68701	0
m3r-pooled/hnsw_M_3	5158	7630	building index: loading tuples	7168	3221	0	45134	68701	0
m3r-pooled/hnsw_M_3	5220	7630	building index: loading tuples	7168	3258	0	45676	68701	0
m3r-pooled/hnsw_M_3	5293	7630	building index: loading tuples	7168	3301	0	46130	68701	0
m3r-pooled/hnsw_M_3	5353	7630	building index: loading tuples	7168	3327	0	46574	68701	0
m3r-pooled/hnsw_M_3	5414	7630	building index: loading tuples	7168	3362	0	47046	68701	0
m3r-pooled/hnsw_M_3	5474	7630	building index: loading tuples	7168	3397	0	47502	68701	0
m3r-pooled/hnsw_M_3	5535	7630	building index: loading tuples	7168	3423	0	47918	68701	0
m3r-pooled/hnsw_M_3	5595	7630	building index: loading tuples	7168	3449	0	48355	68701	0
m3r-pooled/hnsw_M_3	5656	7630	building index: loading tuples	7168	3489	0	48817	68701	0
m3r-pooled/hnsw_M_3	5721	7630	building index: loading tuples	7168	3521	0	49300	68701	0
m3r-pooled/hnsw_M_3	5785	7630	building index: loading tuples	7168	3547	0	49749	68701	0
m3r-pooled/hnsw_M_3	5846	7630	building index: loading tuples	7168	3582	0	50225	68701	0
m3r-pooled/hnsw_M_3	5906	7630	building index: loading tuples	7168	3621	0	50693	68701	0
m3r-pooled/hnsw_M_3	5966	7630	building index: loading tuples	7168	3658	0	51219	68701	0
m3r-pooled/hnsw_M_3	6027	7630	building index: loading tuples	7168	3694	0	51712	68701	0
m3r-pooled/hnsw_M_3	6087	7630	building index: loading tuples	7168	3725	0	52184	68701	0
m3r-pooled/hnsw_M_3	6147	7630	building index: loading tuples	7168	3765	0	52705	68701	0
m3r-pooled/hnsw_M_3	6207	7630	building index: loading tuples	7168	3801	0	53188	68701	0
m3r-pooled/hnsw_M_3	6268	7630	building index: loading tuples	7168	3841	0	53714	68701	0
m3r-pooled/hnsw_M_3	6328	7630	building index: loading tuples	7168	3876	0	54213	68701	0
m3r-pooled/hnsw_M_3	6388	7630	building index: loading tuples	7168	3912	0	54714	68701	0
m3r-pooled/hnsw_M_3	6448	7630	building index: loading tuples	7168	3948	0	55225	68701	0
m3r-pooled/hnsw_M_3	6508	7630	building index: loading tuples	7168	3984	0	55713	68701	0
m3r-pooled/hnsw_M_3	6568	7630	building index: loading tuples	7168	4020	0	56187	68701	0
m3r-pooled/hnsw_M_3	6628	7630	building index: loading tuples	7168	4056	0	56699	68701	0
m3r-pooled/hnsw_M_3	6689	7630	building index: loading tuples	7168	4088	0	57203	68701	0
m3r-pooled/hnsw_M_3	6773	7630	building index: loading tuples	7168	4127	0	57795	68701	0
m3r-pooled/hnsw_M_3	6841	7630	building index: loading tuples	7168	4162	0	58261	68701	0
m3r-pooled/hnsw_M_3	6901	7630	building index: loading tuples	7168	4187	0	58672	68701	0
m3r-pooled/hnsw_M_3	6963	7630	building index: loading tuples	7168	4222	0	59098	68701	0
m3r-pooled/hnsw_M_3	7025	7630	building index: loading tuples	7168	4247	0	59498	68701	0
m3r-pooled/hnsw_M_3	7085	7630	building index: loading tuples	7168	4280	0	59904	68701	0
m3r-pooled/hnsw_M_3	7146	7630	building index: loading tuples	7168	4306	0	60302	68701	0
m3r-pooled/hnsw_M_3	7206	7630	building index: loading tuples	7168	4331	0	60687	68701	0
m3r-pooled/hnsw_M_3	7265	7630	building index: loading tuples	7168	4364	0	61073	68701	0
m3r-pooled/hnsw_M_3	7325	7630	building index: loading tuples	7168	4389	0	61462	68701	0
m3r-pooled/hnsw_M_3	7384	7630	building index: loading tuples	7168	4414	0	61840	68701	0
m3r-pooled/hnsw_M_3	7444	7630	building index: loading tuples	7168	4448	0	62224	68701	0
m3r-pooled/hnsw_M_3	7504	7630	building index: loading tuples	7168	4472	0	62597	68701	0
m3r-pooled/hnsw_M_3	7565	7630	building index: loading tuples	7168	4498	0	62988	68701	0
m3r-pooled/hnsw_M_3	7627	7630	building index: loading tuples	7168	4523	0	63368	68701	0
m3r-pooled/hnsw_M_3	7686	7630	building index: loading tuples	7168	4556	0	63745	68701	0
m3r-pooled/hnsw_M_3	7746	7630	building index: loading tuples	7168	4580	0	64110	68701	0
m3r-pooled/hnsw_M_3	7806	7630	building index: loading tuples	7168	4605	0	64477	68701	0
m3r-pooled/hnsw_M_3	7866	7630	building index: loading tuples	7168	4630	0	64844	68701	0
m3r-pooled/hnsw_M_3	7926	7630	building index: loading tuples	7168	4654	0	65203	68701	0
m3r-pooled/hnsw_M_3	7988	7630	building index: loading tuples	7168	4683	0	65562	68701	0
m3r-pooled/hnsw_M_3	8049	7630	building index: loading tuples	7168	4707	0	65927	68701	0
m3r-pooled/hnsw_M_3	8110	7630	building index: loading tuples	7168	4740	0	66293	68701	0
m3r-pooled/hnsw_M_3	8170	7630	building index: loading tuples	7168	4765	0	66651	68701	0
m3r-pooled/hnsw_M_3	8230	7630	building index: loading tuples	7168	4789	0	67026	68701	0
m3r-pooled/hnsw_M_3	8293	7630	building index: loading tuples	7168	4814	0	67407	68701	0
m3r-pooled/hnsw_M_3	8357	7630	building index: loading tuples	7168	4848	0	67804	68701	0
m3r-pooled/hnsw_M_3	8420	7630	building index: loading tuples	7168	4873	0	68184	68701	0
m3r-pooled/hnsw_M_3	8482	7630	building index: loading tuples	7168	4899	0	68635	68701	0
m3r-pooled/hnsw_M_3	8546	7630	building index: loading tuples	7168	4933	0	69068	68701	0
m3r-pooled/hnsw_M_3	8610	7630	building index: loading tuples	7168	4959	0	69470	68701	0
m3r-pooled/hnsw_M_3	8673	7630	building index: loading tuples	7168	4993	0	69866	68701	0
m3r-pooled/hnsw_M_3	8735	7630	building index: loading tuples	7168	5018	0	70244	68701	0
m3r-pooled/hnsw_M_3	8796	7630	building index: loading tuples	7168	5043	0	70618	68701	0
m3r-pooled/hnsw_M_3	8856	7630	building index: loading tuples	7168	5067	0	70988	68701	0
m3r-pooled/hnsw_M_3	8920	7630	building index: loading tuples	7168	5100	0	71366	68701	0
m3r-pooled/hnsw_M_3	8980	7630	building index: loading tuples	7168	5125	0	71727	68701	0
m3r-pooled/hnsw_M_3	9040	7630	building index: loading tuples	7168	5149	0	72090	68701	0
m3r-pooled/hnsw_M_3	9100	7630	building index: loading tuples	7168	5174	0	72450	68701	0
m3r-pooled/hnsw_M_3	9160	7630	building index: loading tuples	7168	5208	0	72852	68701	0
m3r-pooled/hnsw_M_3	9228	7630	building index: loading tuples	7168	5232	0	73207	68701	0
m3r-pooled/hnsw_M_3	9288	7630	building index: loading tuples	7168	5256	0	73558	68701	0
m3r-pooled/hnsw_M_3	9347	7630	building index: loading tuples	7168	5281	0	73918	68701	0
m3r-pooled/hnsw_M_3	9407	7630	building index: loading tuples	7168	5305	0	74276	68701	0
m3r-pooled/hnsw_M_3	9466	7630	building index: loading tuples	7168	5329	0	74628	68701	0
m3r-pooled/hnsw_M_3	9525	7630	building index: loading tuples	7168	5354	0	74983	68701	0
m3r-pooled/hnsw_M_3	9585	7630	building index: loading tuples	7168	5378	0	75342	68701	0
m3r-pooled/hnsw_M_3	9645	7630	building index: loading tuples	7168	5402	0	75691	68701	0
m3r-pooled/hnsw_M_3	9705	7630	building index: loading tuples	7168	5427	0	76045	68701	0
m3r-pooled/hnsw_M_3	9764	7630	building index: loading tuples	7168	5451	0	76407	68701	0
m3r-pooled/hnsw_M_3	9824	7630	building index: loading tuples	7168	5484	0	76763	68701	0
m3r-pooled/hnsw_M_3	9883	7630	building index: loading tuples	7168	5508	0	77106	68701	0
m3r-pooled/hnsw_M_3	9943	7630	building index: loading tuples	7168	5532	0	77466	68701	0
m3r-pooled/hnsw_M_3	10002	7630	building index: loading tuples	7168	5557	0	77831	68701	0
m3r-pooled/hnsw_M_3	10062	7630	building index: loading tuples	7168	5581	0	78190	68701	0
m3r-pooled/hnsw_M_3	10122	7630	building index: loading tuples	7168	5606	0	78548	68701	0
m3r-pooled/hnsw_M_3	10182	7630	building index: loading tuples	7168	5630	0	78901	68701	0
m3r-pooled/hnsw_M_3	10242	7630	building index: loading tuples	7168	5655	0	79264	68701	0
m3r-pooled/hnsw_M_3	10301	7630	building index: loading tuples	7168	5696	0	79672	68701	0
m3r-pooled/hnsw_M_3	10362	7630	building index: loading tuples	7168	5721	0	80065	68701	0
m3r-pooled/hnsw_M_3	10422	7630	building index: loading tuples	7168	5745	0	80426	68701	0
m3r-pooled/hnsw_M_3	10481	7630	building index: loading tuples	7168	5770	0	80797	68701	0
m3r-pooled/hnsw_M_3	10540	7630	building index: loading tuples	7168	5795	0	81156	68701	0
m3r-pooled/hnsw_M_3	10600	7630	building index: loading tuples	7168	5819	0	81511	68701	0
m3r-pooled/hnsw_M_3	10660	7630	building index: loading tuples	7168	5852	0	81881	68701	0
m3r-pooled/hnsw_M_3	10720	7630	building index: loading tuples	7168	5877	0	82245	68701	0
m3r-pooled/hnsw_M_3	10780	7630	building index: loading tuples	7168	5901	0	82586	68701	0
m3r-pooled/hnsw_M_3	10839	7630	building index: loading tuples	7168	5926	0	82973	68701	0
m3r-pooled/hnsw_M_3	10897	7630	building index: loading tuples	7168	5951	0	83354	68701	0
m3r-pooled/hnsw_M_3	10956	7630	building index: loading tuples	7168	5984	0	83733	68701	0
m3r-pooled/hnsw_M_3	11016	7630	building index: loading tuples	7168	6009	0	84119	68701	0
m3r-pooled/hnsw_M_3	11076	7630	building index: loading tuples	7168	6034	0	84510	68701	0
m3r-pooled/hnsw_M_3	11136	7630	building index: loading tuples	7168	6059	0	84892	68701	0
m3r-pooled/hnsw_M_3	11195	7630	building index: loading tuples	7168	6092	0	85268	68701	0
m3r-pooled/hnsw_M_3	11255	7630	building index: loading tuples	7168	6117	0	85653	68701	0
m3r-pooled/hnsw_M_3	11315	7630	building index: loading tuples	7168	6142	0	86032	68701	0
m3r-pooled/hnsw_M_3	11374	7630	building index: loading tuples	7168	6167	0	86403	68701	0
m3r-pooled/hnsw_M_3	11433	7630	building index: loading tuples	7168	6195	0	86766	68701	0
m3r-pooled/hnsw_M_3	11493	7630	building index: loading tuples	7168	6228	0	87142	68701	0
m3r-pooled/hnsw_M_3	11552	7630	building index: loading tuples	7168	6253	0	87521	68701	0
m3r-pooled/hnsw_M_3	11612	7630	building index: loading tuples	7168	6281	0	87886	68701	0
m3r-pooled/hnsw_M_3	11672	7630	building index: loading tuples	7168	6299	0	88258	68701	0
m3r-pooled/hnsw_M_3	11731	7630	building index: loading tuples	7168	6323	0	88624	68701	0
m3r-pooled/hnsw_M_3	11790	7630	building index: loading tuples	7168	6360	0	88994	68701	0
m3r-pooled/hnsw_M_3	11850	7630	building index: loading tuples	7168	6380	0	89357	68701	0
m3r-pooled/hnsw_M_3	11908	7630	building index: loading tuples	7168	6409	0	89715	68701	0
m3r-pooled/hnsw_M_3	11967	7630	building index: loading tuples	7168	6430	0	90075	68701	0
m3r-pooled/hnsw_M_3	12026	7630	building index: loading tuples	7168	6458	0	90425	68701	0
m3r-pooled/hnsw_M_3	12085	7630	building index: loading tuples	7168	6478	0	90770	68701	0
m3r-pooled/hnsw_M_3	12144	7630	building index: loading tuples	7168	6503	0	91123	68701	0
m3r-pooled/hnsw_M_3	12203	7630	building index: loading tuples	7168	6527	0	91483	68701	0
m3r-pooled/hnsw_M_3	12262	7630	building index: loading tuples	7168	6560	0	91821	68701	0
m3r-pooled/hnsw_M_3	12321	7630	building index: loading tuples	7168	6575	0	92116	68701	0
m3r-pooled/hnsw_M_3	12380	7630	building index: loading tuples	7168	6599	0	92407	68701	0
m3r-pooled/hnsw_M_3	12441	7630	building index: loading tuples	7168	6623	0	92710	68701	0
m3r-pooled/hnsw_M_3	12500	7630	building index: loading tuples	7168	6639	0	93011	68701	0
m3r-pooled/hnsw_M_3	12560	7630	building index: loading tuples	7168	6662	0	93310	68701	0
m3r-pooled/hnsw_M_3	12620	7630	building index: loading tuples	7168	6682	0	93607	68701	0
m3r-pooled/hnsw_M_3	12679	7630	building index: loading tuples	7168	6706	0	93912	68701	0
m3r-pooled/hnsw_M_3	12739	7630	building index: loading tuples	7168	6729	0	94203	68701	0
m3r-pooled/hnsw_M_3	12798	7630	building index: loading tuples	7168	6751	0	94550	68701	0
m3r-pooled/hnsw_M_3	12858	7630	building index: loading tuples	7168	6771	0	94868	68701	0
m3r-pooled/hnsw_M_3	12918	7630	building index: loading tuples	7168	6795	0	95171	68701	0
m3r-pooled/hnsw_M_3	12978	7630	building index: loading tuples	7168	6815	0	95472	68701	0
m3r-pooled/hnsw_M_3	13038	7630	building index: loading tuples	7168	6839	0	95769	68701	0
m3r-pooled/hnsw_M_3	13097	7630	building index: loading tuples	7168	6858	0	96058	68701	0
m3r-pooled/hnsw_M_3	13156	7630	building index: loading tuples	7168	6886	0	96350	68701	0
m3r-pooled/hnsw_M_3	13215	7630	building index: loading tuples	7168	6901	0	96630	68701	0
m3r-pooled/hnsw_M_3	13274	7630	building index: loading tuples	7168	6921	0	96921	68701	0
m3r-pooled/hnsw_M_3	13333	7630	building index: loading tuples	7168	6946	0	97211	68701	0
m3r-pooled/hnsw_M_3	13392	7630	building index: loading tuples	7168	6963	0	97493	68701	0
m3r-pooled/hnsw_M_3	13451	7630	building index: loading tuples	7168	6983	0	97775	68701	0
m3r-pooled/hnsw_M_3	13510	7630	building index: loading tuples	7168	7004	0	98063	68701	0
m3r-pooled/hnsw_M_3	13568	7630	building index: loading tuples	7168	7026	0	98354	68701	0
m3r-pooled/hnsw_M_3	13628	7630	building index: loading tuples	7168	7045	0	98644	68701	0
m3r-pooled/hnsw_M_3	13687	7630	building index: loading tuples	7168	7067	0	98960	68701	0
m3r-pooled/hnsw_M_3	13746	7630	building index: loading tuples	7168	7089	0	99262	68701	0
m3r-pooled/hnsw_M_3	13805	7630	building index: loading tuples	7168	7112	0	99584	68701	0
m3r-pooled/hnsw_M_3	13865	7630	building index: loading tuples	7168	7134	0	99886	68701	0
m3r-pooled/hnsw_M_3	13924	7630	building index: loading tuples	7168	7168	0	100000	68701	0
m3r-pooled/hnsw_M_3	13985	7630	building index: loading tuples	7168	7168	0	100000	68701	0
m3r-pooled/hnsw_M_3	14047	7630	building index: loading tuples	7168	7168	0	100000	68701	0
m3r-pooled/hnsw_M_3	14107	7630	building index: loading tuples	7168	7168	0	100000	68701	0
m3r-pooled/hnsw_M_3	14169	7630	building index: loading tuples	7168	7168	0	100000	68701	0
m3r-pooled/hnsw_M_3	14230	7630	building index: loading tuples	7168	7168	0	100000	68701	0
m3r-pooled/hnsw_M_3	14291	7630	building index: loading tuples	7168	7168	0	100000	68701	0
m3r-pooled/hnsw_M_3	14351	7630	building index: loading tuples	7168	7168	0	100000	68701	0
m3r-pooled/hnsw_M_3	14411	7630	building index: loading tuples	7168	7168	0	100000	68701	0
m3r-pooled/hnsw_M_3	14470	7630	building index: loading tuples	7168	7168	0	100000	68701	0
m3r-pooled/hnsw_M_3	14529	7630	building index: loading tuples	7168	7168	0	100000	68701	0
m3r-pooled/hnsw_M_3	14588	7630	building index: loading tuples	7168	7168	0	100000	68701	0
m3r-pooled/hnsw_M_3	14648	7630	building index: loading tuples	7168	7168	0	100000	68701	0
m3r-pooled/hnsw_M_3	14707	7630	building index: loading tuples	7168	7168	0	100000	68701	0
m3r-pooled/hnsw_M_3	14767	7630	building index: loading tuples	7168	7168	0	100000	68701	0
m3r-pooled/ivf_L_1	1	32743	initializing	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	85	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	147	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	208	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	270	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	331	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	393	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	454	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	515	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	577	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	639	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	700	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	761	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	822	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	884	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	946	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	1007	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	1069	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	1130	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	1191	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	1253	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	1314	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	1376	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	1437	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	1498	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	1560	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	1620	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	1682	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	1742	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	1802	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	1862	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	1923	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	1983	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	2044	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	2104	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	2164	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	2224	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	2283	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	2344	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	2403	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	2463	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	2522	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	2582	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	2642	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	2702	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	2764	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	2824	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	2884	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	2944	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	3003	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	3063	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	3124	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	3185	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	3246	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	3307	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	3366	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	3427	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	3489	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	3548	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	3609	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	3669	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	3729	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	3789	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	3850	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	3912	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	3972	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	4033	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	4093	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	4153	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	4213	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	4273	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	4335	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	4395	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	4455	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	4516	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	4576	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	4636	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	4696	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	4757	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	4820	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	4880	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	4941	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	5001	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	5061	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	5121	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	5182	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	5243	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	5304	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	5364	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	5424	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	5484	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	5544	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	5604	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	5664	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	5724	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	5783	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	5843	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	5903	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	5963	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	6023	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	6083	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	6143	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	6203	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	6264	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	6324	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	6384	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	6444	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	6503	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	6562	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	6621	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	6680	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	6739	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	6798	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	6857	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	6916	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	6976	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	7035	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	7095	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	7156	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	7216	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	7276	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	7335	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	7394	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	7454	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	7513	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	7573	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	7632	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	7691	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	7750	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	7810	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	7870	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	7929	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	7988	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	8047	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	8106	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	8166	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	8226	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	8287	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	8346	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	8406	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	8466	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	8526	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	8587	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	8647	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	8707	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	8766	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	8827	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	8888	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	8947	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	9007	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	9067	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	9127	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	9186	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	9245	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	9304	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_1	9363	32743	building index: assigning tuples	71488	25	0	0	68713	0
m3r-pooled/ivf_L_1	9430	32743	building index: assigning tuples	71488	1414	0	0	68713	0
m3r-pooled/ivf_L_1	9504	32743	building index: assigning tuples	71488	2862	0	0	68713	0
m3r-pooled/ivf_L_1	9590	32743	building index: assigning tuples	71488	4544	0	0	68713	0
m3r-pooled/ivf_L_1	9658	32743	building index: assigning tuples	71488	5732	0	0	68713	0
m3r-pooled/ivf_L_1	9728	32743	building index: assigning tuples	71488	7063	0	0	68713	0
m3r-pooled/ivf_L_1	9802	32743	building index: assigning tuples	71488	8686	0	0	68713	0
m3r-pooled/ivf_L_1	9881	32743	building index: assigning tuples	71488	10069	0	0	68713	0
m3r-pooled/ivf_L_1	9945	32743	building index: assigning tuples	71488	11290	0	0	68713	0
m3r-pooled/ivf_L_1	10010	32743	building index: assigning tuples	71488	12418	0	0	68713	0
m3r-pooled/ivf_L_1	10070	32743	building index: assigning tuples	71488	12479	0	0	68713	0
m3r-pooled/ivf_L_1	10131	32743	building index: assigning tuples	71488	13311	0	0	68713	0
m3r-pooled/ivf_L_1	10191	32743	building index: assigning tuples	71488	14316	0	0	68713	0
m3r-pooled/ivf_L_1	10267	32743	building index: assigning tuples	71488	15776	0	0	68713	0
m3r-pooled/ivf_L_1	10336	32743	building index: assigning tuples	71488	17279	0	0	68713	0
m3r-pooled/ivf_L_1	10411	32743	building index: assigning tuples	71488	18558	0	0	68713	0
m3r-pooled/ivf_L_1	10483	32743	building index: assigning tuples	71488	19966	0	0	68713	0
m3r-pooled/ivf_L_1	10552	32743	building index: assigning tuples	71488	21565	0	0	68713	0
m3r-pooled/ivf_L_1	10639	32743	building index: assigning tuples	71488	23083	0	0	68713	0
m3r-pooled/ivf_L_1	10705	32743	building index: assigning tuples	71488	24595	0	0	68713	0
m3r-pooled/ivf_L_1	10785	32743	building index: assigning tuples	71488	26276	0	0	68713	0
m3r-pooled/ivf_L_1	10865	32743	building index: assigning tuples	71488	27501	0	0	68713	0
m3r-pooled/ivf_L_1	10931	32743	building index: assigning tuples	71488	28911	0	0	68713	0
m3r-pooled/ivf_L_1	11009	32743	building index: assigning tuples	71488	30610	0	0	68713	0
m3r-pooled/ivf_L_1	11086	32743	building index: assigning tuples	71488	31985	0	0	68713	0
m3r-pooled/ivf_L_1	11165	32743	building index: assigning tuples	71488	33433	0	0	68713	0
m3r-pooled/ivf_L_1	11228	32743	building index: assigning tuples	71488	33623	0	0	68713	0
m3r-pooled/ivf_L_1	11299	32743	building index: assigning tuples	71488	34283	0	0	68713	0
m3r-pooled/ivf_L_1	11370	32743	building index: assigning tuples	71488	35170	0	0	68713	0
m3r-pooled/ivf_L_1	11430	32743	building index: assigning tuples	71488	36293	0	0	68713	0
m3r-pooled/ivf_L_1	11491	32743	building index: assigning tuples	71488	36293	0	0	68713	0
m3r-pooled/ivf_L_1	11550	32743	building index: assigning tuples	71488	36589	0	0	68713	0
m3r-pooled/ivf_L_1	11615	32743	building index: assigning tuples	71488	37801	0	0	68713	0
m3r-pooled/ivf_L_1	11684	32743	building index: assigning tuples	71488	39236	0	0	68713	0
m3r-pooled/ivf_L_1	11749	32743	building index: assigning tuples	71488	40501	0	0	68713	0
m3r-pooled/ivf_L_1	11826	32743	building index: assigning tuples	71488	42112	0	0	68713	0
m3r-pooled/ivf_L_1	11895	32743	building index: assigning tuples	71488	43458	0	0	68713	0
m3r-pooled/ivf_L_1	11967	32743	building index: assigning tuples	71488	45192	0	0	68713	0
m3r-pooled/ivf_L_1	12060	32743	building index: assigning tuples	71488	46748	0	0	68713	0
m3r-pooled/ivf_L_1	12128	32743	building index: assigning tuples	71488	48024	0	0	68713	0
m3r-pooled/ivf_L_1	12202	32743	building index: assigning tuples	71488	49334	0	0	68713	0
m3r-pooled/ivf_L_1	12268	32743	building index: assigning tuples	71488	50843	0	0	68713	0
m3r-pooled/ivf_L_1	12338	32743	building index: assigning tuples	71488	52601	0	0	68713	0
m3r-pooled/ivf_L_1	12422	32743	building index: assigning tuples	71488	53883	0	0	68713	0
m3r-pooled/ivf_L_1	12492	32743	building index: assigning tuples	71488	55362	0	0	68713	0
m3r-pooled/ivf_L_1	12562	32743	building index: assigning tuples	71488	56863	0	0	68713	0
m3r-pooled/ivf_L_1	12630	32743	building index: assigning tuples	71488	58281	0	0	68713	0
m3r-pooled/ivf_L_1	12704	32743	building index: assigning tuples	71488	59634	0	0	68713	0
m3r-pooled/ivf_L_1	12794	32743	building index: assigning tuples	71488	61846	0	0	68713	0
m3r-pooled/ivf_L_1	12892	32743	building index: assigning tuples	71488	63524	0	0	68713	0
m3r-pooled/ivf_L_1	12963	32743	building index: assigning tuples	71488	65294	0	0	68713	0
m3r-pooled/ivf_L_1	13044	32743	building index: assigning tuples	71488	65627	0	0	68713	0
m3r-pooled/ivf_L_1	13105	32743	building index: assigning tuples	71488	65646	0	0	68713	0
m3r-pooled/ivf_L_1	13167	32743	building index: assigning tuples	71488	67196	0	0	68713	0
m3r-pooled/ivf_L_1	13246	32743	building index: assigning tuples	71488	69067	0	0	68713	0
m3r-pooled/ivf_L_1	13343	32743	building index: assigning tuples	71488	70750	0	0	68713	0
m3r-pooled/ivf_L_1	13419	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_1	13483	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_1	13544	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_1	13604	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_1	13663	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_1	13727	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_1	13787	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_1	13849	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_1	13909	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_1	13969	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_1	14029	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_1	14089	32743	building index: loading tuples	71488	71488	1000000	37299	68713	0
m3r-pooled/ivf_L_1	14149	32743	building index: loading tuples	71488	71488	1000000	37299	68713	0
m3r-pooled/ivf_L_1	14208	32743	building index: loading tuples	71488	71488	1000000	37299	68713	0
m3r-pooled/ivf_L_1	14267	32743	building index: loading tuples	71488	71488	1000000	37299	68713	0
m3r-pooled/ivf_L_1	14326	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-pooled/ivf_L_1	14385	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-pooled/ivf_L_1	14444	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-pooled/ivf_L_1	14503	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-pooled/ivf_L_1	14562	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-pooled/ivf_L_1	14622	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-pooled/ivf_L_1	14681	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-pooled/ivf_L_1	14741	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-pooled/ivf_L_1	14800	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-pooled/ivf_L_1	14858	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-pooled/ivf_L_1	14917	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-pooled/ivf_L_1	14977	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-pooled/ivf_L_1	15037	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-pooled/ivf_L_1	15098	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-pooled/ivf_L_1	15159	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-pooled/ivf_L_1	15222	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-pooled/ivf_L_1	15284	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-pooled/ivf_L_1	15346	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-pooled/ivf_L_1	15408	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-pooled/ivf_L_1	15470	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-pooled/ivf_L_1	15532	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-pooled/ivf_L_1	15594	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-pooled/ivf_L_1	15656	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-pooled/ivf_L_1	15718	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-pooled/ivf_L_1	15781	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-pooled/ivf_L_1	15842	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-pooled/ivf_L_1	15906	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-pooled/ivf_L_1	15968	32743	building index: loading tuples	71488	71488	1000000	68274	68713	0
m3r-pooled/ivf_L_1	16030	32743	building index: loading tuples	71488	71488	1000000	99164	68713	0
m3r-pooled/ivf_L_1	16092	32743	building index: loading tuples	71488	71488	1000000	130106	68713	0
m3r-pooled/ivf_L_1	16155	32743	building index: loading tuples	71488	71488	1000000	153581	68713	0
m3r-pooled/ivf_L_1	16216	32743	building index: loading tuples	71488	71488	1000000	161052	68713	0
m3r-pooled/ivf_L_1	16278	32743	building index: loading tuples	71488	71488	1000000	197600	68713	0
m3r-pooled/ivf_L_1	16341	32743	building index: loading tuples	71488	71488	1000000	222878	68713	0
m3r-pooled/ivf_L_1	16405	32743	building index: loading tuples	71488	71488	1000000	244454	68713	0
m3r-pooled/ivf_L_1	16467	32743	building index: loading tuples	71488	71488	1000000	268723	68713	0
m3r-pooled/ivf_L_1	16529	32743	building index: loading tuples	71488	71488	1000000	312396	68713	0
m3r-pooled/ivf_L_1	16593	32743	building index: loading tuples	71488	71488	1000000	342350	68713	0
m3r-pooled/ivf_L_1	16662	32743	building index: loading tuples	71488	71488	1000000	362728	68713	0
m3r-pooled/ivf_L_1	16725	32743	building index: loading tuples	71488	71488	1000000	377190	68713	0
m3r-pooled/ivf_L_1	16788	32743	building index: loading tuples	71488	71488	1000000	408426	68713	0
m3r-pooled/ivf_L_1	16859	32743	building index: loading tuples	71488	71488	1000000	439353	68713	0
m3r-pooled/ivf_L_1	16920	32743	building index: loading tuples	71488	71488	1000000	456921	68713	0
m3r-pooled/ivf_L_1	16982	32743	building index: loading tuples	71488	71488	1000000	486139	68713	0
m3r-pooled/ivf_L_1	17044	32743	building index: loading tuples	71488	71488	1000000	497491	68713	0
m3r-pooled/ivf_L_1	17114	32743	building index: loading tuples	71488	71488	1000000	501208	68713	0
m3r-pooled/ivf_L_1	17177	32743	building index: loading tuples	71488	71488	1000000	501208	68713	0
m3r-pooled/ivf_L_1	17239	32743	building index: loading tuples	71488	71488	1000000	501598	68713	0
m3r-pooled/ivf_L_1	17304	32743	building index: loading tuples	71488	71488	1000000	546318	68713	0
m3r-pooled/ivf_L_1	17366	32743	building index: loading tuples	71488	71488	1000000	569304	68713	0
m3r-pooled/ivf_L_1	17429	32743	building index: loading tuples	71488	71488	1000000	593975	68713	0
m3r-pooled/ivf_L_1	17492	32743	building index: loading tuples	71488	71488	1000000	608470	68713	0
m3r-pooled/ivf_L_1	17555	32743	building index: loading tuples	71488	71488	1000000	650004	68713	0
m3r-pooled/ivf_L_1	17669	32743	building index: loading tuples	71488	71488	1000000	655842	68713	0
m3r-pooled/ivf_L_1	17779	32743	building index: loading tuples	71488	71488	1000000	667434	68713	0
m3r-pooled/ivf_L_1	17842	32743	building index: loading tuples	71488	71488	1000000	707402	68713	0
m3r-pooled/ivf_L_1	17905	32743	building index: loading tuples	71488	71488	1000000	717680	68713	0
m3r-pooled/ivf_L_1	17970	32743	building index: loading tuples	71488	71488	1000000	717680	68713	0
m3r-pooled/ivf_L_1	18032	32743	building index: loading tuples	71488	71488	1000000	717680	68713	0
m3r-pooled/ivf_L_1	18095	32743	building index: loading tuples	71488	71488	1000000	717695	68713	0
m3r-pooled/ivf_L_1	18157	32743	building index: loading tuples	71488	71488	1000000	748604	68713	0
m3r-pooled/ivf_L_1	18219	32743	building index: loading tuples	71488	71488	1000000	776129	68713	0
m3r-pooled/ivf_L_1	18282	32743	building index: loading tuples	71488	71488	1000000	779519	68713	0
m3r-pooled/ivf_L_1	18354	32743	building index: loading tuples	71488	71488	1000000	779519	68713	0
m3r-pooled/ivf_L_1	18417	32743	building index: loading tuples	71488	71488	1000000	779522	68713	0
m3r-pooled/ivf_L_1	18479	32743	building index: loading tuples	71488	71488	1000000	781979	68713	0
m3r-pooled/ivf_L_1	18541	32743	building index: loading tuples	71488	71488	1000000	820287	68713	0
m3r-pooled/ivf_L_1	18603	32743	building index: loading tuples	71488	71488	1000000	841387	68713	0
m3r-pooled/ivf_L_1	18665	32743	building index: loading tuples	71488	71488	1000000	871045	68713	0
m3r-pooled/ivf_L_1	18728	32743	building index: loading tuples	71488	71488	1000000	872306	68713	0
m3r-pooled/ivf_L_1	18813	32743	building index: loading tuples	71488	71488	1000000	872306	68713	0
m3r-pooled/ivf_L_1	18876	32743	building index: loading tuples	71488	71488	1000000	872321	68713	0
m3r-pooled/ivf_L_1	18938	32743	building index: loading tuples	71488	71488	1000000	887219	68713	0
m3r-pooled/ivf_L_1	19001	32743	building index: loading tuples	71488	71488	1000000	919623	68713	0
m3r-pooled/ivf_L_1	19064	32743	building index: loading tuples	71488	71488	1000000	934142	68713	0
m3r-pooled/ivf_L_1	19126	32743	building index: loading tuples	71488	71488	1000000	934142	68713	0
m3r-pooled/ivf_L_1	19187	32743	building index: loading tuples	71488	71488	1000000	934142	68713	0
m3r-pooled/ivf_L_1	19249	32743	building index: loading tuples	71488	71488	1000000	934157	68713	0
m3r-pooled/ivf_L_1	19310	32743	building index: loading tuples	71488	71488	1000000	942993	68713	0
m3r-pooled/ivf_L_1	19372	32743	building index: loading tuples	71488	71488	1000000	987869	68713	0
m3r-pooled/ivf_L_1	19443	32743	building index: loading tuples	71488	71488	1000000	999999	68713	0
m3r-pooled/ivf_L_1	19509	32743	building index: loading tuples	71488	71488	1000000	1000000	68713	0
m3r-pooled/ivf_L_2	1	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	63	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	125	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	186	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	248	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	309	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	370	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	430	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	491	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	551	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	611	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	672	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	732	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	792	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	852	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	912	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	972	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	1033	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	1094	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	1155	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	1216	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	1277	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	1339	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	1401	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	1463	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	1523	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	1585	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	1646	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	1707	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	1767	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	1829	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	1891	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	1952	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	2013	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	2075	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	2137	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	2199	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	2259	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	2321	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	2381	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	2442	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	2502	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	2562	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	2623	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	2683	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	2744	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	2804	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	2864	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	2924	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	2984	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	3043	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	3103	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	3164	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	3224	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	3284	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	3344	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	3404	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	3464	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	3524	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	3584	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	3644	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	3705	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	3766	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	3827	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	3888	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	3948	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	4008	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	4069	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	4128	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	4188	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	4248	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	4308	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	4368	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	4429	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	4491	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	4552	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	4613	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	4675	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	4736	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	4797	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	4859	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	4920	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	4982	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_2	5045	1929	building index: assigning tuples	71488	95	0	0	68713	0
m3r-pooled/ivf_L_2	5110	1929	building index: assigning tuples	71488	821	0	0	68713	0
m3r-pooled/ivf_L_2	5172	1929	building index: assigning tuples	71488	1544	0	0	68713	0
m3r-pooled/ivf_L_2	5243	1929	building index: assigning tuples	71488	2193	0	0	68713	0
m3r-pooled/ivf_L_2	5306	1929	building index: assigning tuples	71488	2839	0	0	68713	0
m3r-pooled/ivf_L_2	5368	1929	building index: assigning tuples	71488	3525	0	0	68713	0
m3r-pooled/ivf_L_2	5430	1929	building index: assigning tuples	71488	4556	0	0	68713	0
m3r-pooled/ivf_L_2	5506	1929	building index: assigning tuples	71488	5856	0	0	68713	0
m3r-pooled/ivf_L_2	5595	1929	building index: assigning tuples	71488	7371	0	0	68713	0
m3r-pooled/ivf_L_2	5672	1929	building index: assigning tuples	71488	8506	0	0	68713	0
m3r-pooled/ivf_L_2	5753	1929	building index: assigning tuples	71488	9831	0	0	68713	0
m3r-pooled/ivf_L_2	5825	1929	building index: assigning tuples	71488	11037	0	0	68713	0
m3r-pooled/ivf_L_2	5921	1929	building index: assigning tuples	71488	12453	0	0	68713	0
m3r-pooled/ivf_L_2	5997	1929	building index: assigning tuples	71488	13684	0	0	68713	0
m3r-pooled/ivf_L_2	6082	1929	building index: assigning tuples	71488	15488	0	0	68713	0
m3r-pooled/ivf_L_2	6190	1929	building index: assigning tuples	71488	16988	0	0	68713	0
m3r-pooled/ivf_L_2	6276	1929	building index: assigning tuples	71488	18460	0	0	68713	0
m3r-pooled/ivf_L_2	6367	1929	building index: assigning tuples	71488	19834	0	0	68713	0
m3r-pooled/ivf_L_2	6451	1929	building index: assigning tuples	71488	21254	0	0	68713	0
m3r-pooled/ivf_L_2	6528	1929	building index: assigning tuples	71488	22472	0	0	68713	0
m3r-pooled/ivf_L_2	6608	1929	building index: assigning tuples	71488	23787	0	0	68713	0
m3r-pooled/ivf_L_2	6678	1929	building index: assigning tuples	71488	24897	0	0	68713	0
m3r-pooled/ivf_L_2	6754	1929	building index: assigning tuples	71488	26022	0	0	68713	0
m3r-pooled/ivf_L_2	6846	1929	building index: assigning tuples	71488	27402	0	0	68713	0
m3r-pooled/ivf_L_2	6926	1929	building index: assigning tuples	71488	28466	0	0	68713	0
m3r-pooled/ivf_L_2	6997	1929	building index: assigning tuples	71488	29306	0	0	68713	0
m3r-pooled/ivf_L_2	7065	1929	building index: assigning tuples	71488	30412	0	0	68713	0
m3r-pooled/ivf_L_2	7148	1929	building index: assigning tuples	71488	31652	0	0	68713	0
m3r-pooled/ivf_L_2	7230	1929	building index: assigning tuples	71488	32946	0	0	68713	0
m3r-pooled/ivf_L_2	7307	1929	building index: assigning tuples	71488	34273	0	0	68713	0
m3r-pooled/ivf_L_2	7381	1929	building index: assigning tuples	71488	35479	0	0	68713	0
m3r-pooled/ivf_L_2	7466	1929	building index: assigning tuples	71488	35479	0	0	68713	0
m3r-pooled/ivf_L_2	7539	1929	building index: assigning tuples	71488	37015	0	0	68713	0
m3r-pooled/ivf_L_2	7633	1929	building index: assigning tuples	71488	38398	0	0	68713	0
m3r-pooled/ivf_L_2	7711	1929	building index: assigning tuples	71488	39749	0	0	68713	0
m3r-pooled/ivf_L_2	7788	1929	building index: assigning tuples	71488	40905	0	0	68713	0
m3r-pooled/ivf_L_2	7858	1929	building index: assigning tuples	71488	41903	0	0	68713	0
m3r-pooled/ivf_L_2	7925	1929	building index: assigning tuples	71488	43015	0	0	68713	0
m3r-pooled/ivf_L_2	7996	1929	building index: assigning tuples	71488	43920	0	0	68713	0
m3r-pooled/ivf_L_2	8076	1929	building index: assigning tuples	71488	45086	0	0	68713	0
m3r-pooled/ivf_L_2	8166	1929	building index: assigning tuples	71488	46633	0	0	68713	0
m3r-pooled/ivf_L_2	8251	1929	building index: assigning tuples	71488	48093	0	0	68713	0
m3r-pooled/ivf_L_2	8336	1929	building index: assigning tuples	71488	49340	0	0	68713	0
m3r-pooled/ivf_L_2	8420	1929	building index: assigning tuples	71488	50614	0	0	68713	0
m3r-pooled/ivf_L_2	8485	1929	building index: assigning tuples	71488	51608	0	0	68713	0
m3r-pooled/ivf_L_2	8566	1929	building index: assigning tuples	71488	52792	0	0	68713	0
m3r-pooled/ivf_L_2	8646	1929	building index: assigning tuples	71488	54032	0	0	68713	0
m3r-pooled/ivf_L_2	8718	1929	building index: assigning tuples	71488	55135	0	0	68713	0
m3r-pooled/ivf_L_2	8789	1929	building index: assigning tuples	71488	56469	0	0	68713	0
m3r-pooled/ivf_L_2	8868	1929	building index: assigning tuples	71488	57749	0	0	68713	0
m3r-pooled/ivf_L_2	8963	1929	building index: assigning tuples	71488	59274	0	0	68713	0
m3r-pooled/ivf_L_2	9057	1929	building index: assigning tuples	71488	60401	0	0	68713	0
m3r-pooled/ivf_L_2	9139	1929	building index: assigning tuples	71488	61743	0	0	68713	0
m3r-pooled/ivf_L_2	9241	1929	building index: assigning tuples	71488	63368	0	0	68713	0
m3r-pooled/ivf_L_2	9320	1929	building index: assigning tuples	71488	64355	0	0	68713	0
m3r-pooled/ivf_L_2	9397	1929	building index: assigning tuples	71488	65819	0	0	68713	0
m3r-pooled/ivf_L_2	9499	1929	building index: assigning tuples	71488	67253	0	0	68713	0
m3r-pooled/ivf_L_2	9580	1929	building index: assigning tuples	71488	68386	0	0	68713	0
m3r-pooled/ivf_L_2	9690	1929	building index: assigning tuples	71488	68411	0	0	68713	0
m3r-pooled/ivf_L_2	9768	1929	building index: assigning tuples	71488	70079	0	0	68713	0
m3r-pooled/ivf_L_2	9830	1929	building index: assigning tuples	71488	70462	0	0	68713	0
m3r-pooled/ivf_L_2	9892	1929	building index: assigning tuples	71488	71203	0	0	68713	0
m3r-pooled/ivf_L_2	9953	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_2	10018	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_2	10084	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_2	10146	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_2	10209	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_2	10272	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_2	10334	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_2	10396	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_2	10458	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_2	10519	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_2	10580	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_2	10642	1929	building index: loading tuples	71488	71488	1000000	38082	68713	0
m3r-pooled/ivf_L_2	10703	1929	building index: loading tuples	71488	71488	1000000	57519	68713	0
m3r-pooled/ivf_L_2	10764	1929	building index: loading tuples	71488	71488	1000000	57519	68713	0
m3r-pooled/ivf_L_2	10825	1929	building index: loading tuples	71488	71488	1000000	57519	68713	0
m3r-pooled/ivf_L_2	10886	1929	building index: loading tuples	71488	71488	1000000	57519	68713	0
m3r-pooled/ivf_L_2	10946	1929	building index: loading tuples	71488	71488	1000000	57519	68713	0
m3r-pooled/ivf_L_2	11006	1929	building index: loading tuples	71488	71488	1000000	57534	68713	0
m3r-pooled/ivf_L_2	11067	1929	building index: loading tuples	71488	71488	1000000	57534	68713	0
m3r-pooled/ivf_L_2	11130	1929	building index: loading tuples	71488	71488	1000000	57534	68713	0
m3r-pooled/ivf_L_2	11192	1929	building index: loading tuples	71488	71488	1000000	57534	68713	0
m3r-pooled/ivf_L_2	11253	1929	building index: loading tuples	71488	71488	1000000	57534	68713	0
m3r-pooled/ivf_L_2	11315	1929	building index: loading tuples	71488	71488	1000000	79172	68713	0
m3r-pooled/ivf_L_2	11377	1929	building index: loading tuples	71488	71488	1000000	88443	68713	0
m3r-pooled/ivf_L_2	11440	1929	building index: loading tuples	71488	71488	1000000	88443	68713	0
m3r-pooled/ivf_L_2	11503	1929	building index: loading tuples	71488	71488	1000000	88443	68713	0
m3r-pooled/ivf_L_2	11565	1929	building index: loading tuples	71488	71488	1000000	88464	68713	0
m3r-pooled/ivf_L_2	11626	1929	building index: loading tuples	71488	71488	1000000	88464	68713	0
m3r-pooled/ivf_L_2	11688	1929	building index: loading tuples	71488	71488	1000000	88464	68713	0
m3r-pooled/ivf_L_2	11750	1929	building index: loading tuples	71488	71488	1000000	101114	68713	0
m3r-pooled/ivf_L_2	11812	1929	building index: loading tuples	71488	71488	1000000	135353	68713	0
m3r-pooled/ivf_L_2	11874	1929	building index: loading tuples	71488	71488	1000000	160242	68713	0
m3r-pooled/ivf_L_2	11936	1929	building index: loading tuples	71488	71488	1000000	181242	68713	0
m3r-pooled/ivf_L_2	11998	1929	building index: loading tuples	71488	71488	1000000	181242	68713	0
m3r-pooled/ivf_L_2	12059	1929	building index: loading tuples	71488	71488	1000000	181242	68713	0
m3r-pooled/ivf_L_2	12120	1929	building index: loading tuples	71488	71488	1000000	181242	68713	0
m3r-pooled/ivf_L_2	12181	1929	building index: loading tuples	71488	71488	1000000	181242	68713	0
m3r-pooled/ivf_L_2	12243	1929	building index: loading tuples	71488	71488	1000000	181257	68713	0
m3r-pooled/ivf_L_2	12304	1929	building index: loading tuples	71488	71488	1000000	181257	68713	0
m3r-pooled/ivf_L_2	12365	1929	building index: loading tuples	71488	71488	1000000	181257	68713	0
m3r-pooled/ivf_L_2	12426	1929	building index: loading tuples	71488	71488	1000000	181257	68713	0
m3r-pooled/ivf_L_2	12487	1929	building index: loading tuples	71488	71488	1000000	181257	68713	0
m3r-pooled/ivf_L_2	12548	1929	building index: loading tuples	71488	71488	1000000	210243	68713	0
m3r-pooled/ivf_L_2	12609	1929	building index: loading tuples	71488	71488	1000000	223640	68713	0
m3r-pooled/ivf_L_2	12669	1929	building index: loading tuples	71488	71488	1000000	266923	68713	0
m3r-pooled/ivf_L_2	12730	1929	building index: loading tuples	71488	71488	1000000	274012	68713	0
m3r-pooled/ivf_L_2	12793	1929	building index: loading tuples	71488	71488	1000000	318798	68713	0
m3r-pooled/ivf_L_2	12854	1929	building index: loading tuples	71488	71488	1000000	347905	68713	0
m3r-pooled/ivf_L_2	12915	1929	building index: loading tuples	71488	71488	1000000	373857	68713	0
m3r-pooled/ivf_L_2	12977	1929	building index: loading tuples	71488	71488	1000000	398625	68713	0
m3r-pooled/ivf_L_2	13038	1929	building index: loading tuples	71488	71488	1000000	428625	68713	0
m3r-pooled/ivf_L_2	13100	1929	building index: loading tuples	71488	71488	1000000	459553	68713	0
m3r-pooled/ivf_L_2	13162	1929	building index: loading tuples	71488	71488	1000000	471368	68713	0
m3r-pooled/ivf_L_2	13223	1929	building index: loading tuples	71488	71488	1000000	495461	68713	0
m3r-pooled/ivf_L_2	13285	1929	building index: loading tuples	71488	71488	1000000	513768	68713	0
m3r-pooled/ivf_L_2	13347	1929	building index: loading tuples	71488	71488	1000000	521402	68713	0
m3r-pooled/ivf_L_2	13428	1929	building index: loading tuples	71488	71488	1000000	521402	68713	0
m3r-pooled/ivf_L_2	13489	1929	building index: loading tuples	71488	71488	1000000	521402	68713	0
m3r-pooled/ivf_L_2	13551	1929	building index: loading tuples	71488	71488	1000000	521402	68713	0
m3r-pooled/ivf_L_2	13612	1929	building index: loading tuples	71488	71488	1000000	521417	68713	0
m3r-pooled/ivf_L_2	13673	1929	building index: loading tuples	71488	71488	1000000	536274	68713	0
m3r-pooled/ivf_L_2	13737	1929	building index: loading tuples	71488	71488	1000000	570279	68713	0
m3r-pooled/ivf_L_2	13799	1929	building index: loading tuples	71488	71488	1000000	591477	68713	0
m3r-pooled/ivf_L_2	13860	1929	building index: loading tuples	71488	71488	1000000	614183	68713	0
m3r-pooled/ivf_L_2	13923	1929	building index: loading tuples	71488	71488	1000000	632527	68713	0
m3r-pooled/ivf_L_2	13987	1929	building index: loading tuples	71488	71488	1000000	670003	68713	0
m3r-pooled/ivf_L_2	14048	1929	building index: loading tuples	71488	71488	1000000	693084	68713	0
m3r-pooled/ivf_L_2	14109	1929	building index: loading tuples	71488	71488	1000000	706953	68713	0
m3r-pooled/ivf_L_2	14171	1929	building index: loading tuples	71488	71488	1000000	706953	68713	0
m3r-pooled/ivf_L_2	14234	1929	building index: loading tuples	71488	71488	1000000	706953	68713	0
m3r-pooled/ivf_L_2	14296	1929	building index: loading tuples	71488	71488	1000000	706953	68713	0
m3r-pooled/ivf_L_2	14357	1929	building index: loading tuples	71488	71488	1000000	706953	68713	0
m3r-pooled/ivf_L_2	14418	1929	building index: loading tuples	71488	71488	1000000	706968	68713	0
m3r-pooled/ivf_L_2	14479	1929	building index: loading tuples	71488	71488	1000000	737876	68713	0
m3r-pooled/ivf_L_2	14541	1929	building index: loading tuples	71488	71488	1000000	768816	68713	0
m3r-pooled/ivf_L_2	14607	1929	building index: loading tuples	71488	71488	1000000	799735	68713	0
m3r-pooled/ivf_L_2	14670	1929	building index: loading tuples	71488	71488	1000000	820574	68713	0
m3r-pooled/ivf_L_2	14732	1929	building index: loading tuples	71488	71488	1000000	860481	68713	0
m3r-pooled/ivf_L_2	14795	1929	building index: loading tuples	71488	71488	1000000	884210	68713	0
m3r-pooled/ivf_L_2	14855	1929	building index: loading tuples	71488	71488	1000000	908303	68713	0
m3r-pooled/ivf_L_2	14917	1929	building index: loading tuples	71488	71488	1000000	914604	68713	0
m3r-pooled/ivf_L_2	14979	1929	building index: loading tuples	71488	71488	1000000	914604	68713	0
m3r-pooled/ivf_L_2	15040	1929	building index: loading tuples	71488	71488	1000000	914604	68713	0
m3r-pooled/ivf_L_2	15102	1929	building index: loading tuples	71488	71488	1000000	924264	68713	0
m3r-pooled/ivf_L_2	15163	1929	building index: loading tuples	71488	71488	1000000	954351	68713	0
m3r-pooled/ivf_L_2	15225	1929	building index: loading tuples	71488	71488	1000000	954351	68713	0
m3r-pooled/ivf_L_2	15287	1929	building index: loading tuples	71488	71488	1000000	954351	68713	0
m3r-pooled/ivf_L_2	15348	1929	building index: loading tuples	71488	71488	1000000	954351	68713	0
m3r-pooled/ivf_L_2	15408	1929	building index: loading tuples	71488	71488	1000000	954351	68713	0
m3r-pooled/ivf_L_2	15469	1929	building index: loading tuples	71488	71488	1000000	954351	68713	0
m3r-pooled/ivf_L_2	15532	1929	building index: loading tuples	71488	71488	1000000	954351	68713	0
m3r-pooled/ivf_L_2	15593	1929	building index: loading tuples	71488	71488	1000000	954351	68713	0
m3r-pooled/ivf_L_2	15655	1929	building index: loading tuples	71488	71488	1000000	954366	68713	0
m3r-pooled/ivf_L_2	15718	1929	building index: loading tuples	71488	71488	1000000	954366	68713	0
m3r-pooled/ivf_L_2	15780	1929	building index: loading tuples	71488	71488	1000000	954366	68713	0
m3r-pooled/ivf_L_2	15842	1929	building index: loading tuples	71488	71488	1000000	980243	68713	0
m3r-pooled/ivf_L_2	15905	1929	building index: loading tuples	71488	71488	1000000	1000000	68713	0
m3r-pooled/ivf_L_2	15966	1929	building index: loading tuples	71488	71488	1000000	1000000	68713	0
m3r-pooled/ivf_L_3	0	3189	initializing	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	61	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	121	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	182	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	243	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	305	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	366	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	427	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	488	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	548	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	610	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	671	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	732	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	793	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	855	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	916	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	977	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	1037	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	1098	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	1159	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	1221	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	1282	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	1343	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	1403	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	1465	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	1525	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	1586	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	1647	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	1707	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	1768	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	1829	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	1889	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	1950	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	2011	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	2071	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	2132	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	2194	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	2255	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	2315	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	2376	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	2437	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	2497	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	2558	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	2619	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	2679	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	2740	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	2800	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	2861	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	2921	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	2982	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	3043	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	3103	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	3164	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	3224	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	3284	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	3344	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	3405	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	3465	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	3525	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	3586	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	3648	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	3709	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	3770	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	3831	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	3892	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	3952	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	4013	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	4074	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	4134	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	4195	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	4255	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	4316	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	4376	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	4436	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	4497	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	4558	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	4618	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	4678	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	4739	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	4799	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	4859	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	4920	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	4980	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	5041	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	5101	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	5161	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	5222	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	5283	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	5345	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	5406	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	5467	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	5528	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	5588	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	5649	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	5709	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	5770	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	5830	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	5891	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	5952	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	6012	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	6073	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	6133	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	6194	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	6254	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	6314	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	6374	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	6435	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	6495	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	6555	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	6615	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	6676	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	6736	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	6796	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	6857	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	6917	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	6978	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	7039	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	7100	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	7161	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	7222	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	7282	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	7342	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	7403	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	7464	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	7525	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	7587	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	7648	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	7710	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	7772	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	7834	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	7895	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	7957	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	8019	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-pooled/ivf_L_3	8083	3189	building index: assigning tuples	71488	229	0	0	68713	0
m3r-pooled/ivf_L_3	8153	3189	building index: assigning tuples	71488	1204	0	0	68713	0
m3r-pooled/ivf_L_3	8217	3189	building index: assigning tuples	71488	2096	0	0	68713	0
m3r-pooled/ivf_L_3	8300	3189	building index: assigning tuples	71488	3735	0	0	68713	0
m3r-pooled/ivf_L_3	8376	3189	building index: assigning tuples	71488	4774	0	0	68713	0
m3r-pooled/ivf_L_3	8454	3189	building index: assigning tuples	71488	5954	0	0	68713	0
m3r-pooled/ivf_L_3	8539	3189	building index: assigning tuples	71488	7367	0	0	68713	0
m3r-pooled/ivf_L_3	8616	3189	building index: assigning tuples	71488	8598	0	0	68713	0
m3r-pooled/ivf_L_3	8698	3189	building index: assigning tuples	71488	9985	0	0	68713	0
m3r-pooled/ivf_L_3	8779	3189	building index: assigning tuples	71488	11114	0	0	68713	0
m3r-pooled/ivf_L_3	8849	3189	building index: assigning tuples	71488	12481	0	0	68713	0
m3r-pooled/ivf_L_3	8937	3189	building index: assigning tuples	71488	13911	0	0	68713	0
m3r-pooled/ivf_L_3	9016	3189	building index: assigning tuples	71488	14994	0	0	68713	0
m3r-pooled/ivf_L_3	9081	3189	building index: assigning tuples	71488	16119	0	0	68713	0
m3r-pooled/ivf_L_3	9158	3189	building index: assigning tuples	71488	17361	0	0	68713	0
m3r-pooled/ivf_L_3	9230	3189	building index: assigning tuples	71488	18477	0	0	68713	0
m3r-pooled/ivf_L_3	9301	3189	building index: assigning tuples	71488	19754	0	0	68713	0
m3r-pooled/ivf_L_3	9380	3189	building index: assigning tuples	71488	20782	0	0	68713	0
m3r-pooled/ivf_L_3	9459	3189	building index: assigning tuples	71488	22362	0	0	68713	0
m3r-pooled/ivf_L_3	9541	3189	building index: assigning tuples	71488	23514	0	0	68713	0
m3r-pooled/ivf_L_3	9620	3189	building index: assigning tuples	71488	24860	0	0	68713	0
m3r-pooled/ivf_L_3	9701	3189	building index: assigning tuples	71488	26176	0	0	68713	0
m3r-pooled/ivf_L_3	9791	3189	building index: assigning tuples	71488	27929	0	0	68713	0
m3r-pooled/ivf_L_3	9877	3189	building index: assigning tuples	71488	28375	0	0	68713	0
m3r-pooled/ivf_L_3	9951	3189	building index: assigning tuples	71488	29597	0	0	68713	0
m3r-pooled/ivf_L_3	10019	3189	building index: assigning tuples	71488	30709	0	0	68713	0
m3r-pooled/ivf_L_3	10085	3189	building index: assigning tuples	71488	32151	0	0	68713	0
m3r-pooled/ivf_L_3	10166	3189	building index: assigning tuples	71488	33335	0	0	68713	0
m3r-pooled/ivf_L_3	10253	3189	building index: assigning tuples	71488	34717	0	0	68713	0
m3r-pooled/ivf_L_3	10331	3189	building index: assigning tuples	71488	35923	0	0	68713	0
m3r-pooled/ivf_L_3	10402	3189	building index: assigning tuples	71488	36875	0	0	68713	0
m3r-pooled/ivf_L_3	10465	3189	building index: assigning tuples	71488	37301	0	0	68713	0
m3r-pooled/ivf_L_3	10530	3189	building index: assigning tuples	71488	38065	0	0	68713	0
m3r-pooled/ivf_L_3	10609	3189	building index: assigning tuples	71488	39343	0	0	68713	0
m3r-pooled/ivf_L_3	10676	3189	building index: assigning tuples	71488	40600	0	0	68713	0
m3r-pooled/ivf_L_3	10769	3189	building index: assigning tuples	71488	41892	0	0	68713	0
m3r-pooled/ivf_L_3	10834	3189	building index: assigning tuples	71488	43205	0	0	68713	0
m3r-pooled/ivf_L_3	10916	3189	building index: assigning tuples	71488	44393	0	0	68713	0
m3r-pooled/ivf_L_3	10993	3189	building index: assigning tuples	71488	45615	0	0	68713	0
m3r-pooled/ivf_L_3	11067	3189	building index: assigning tuples	71488	46926	0	0	68713	0
m3r-pooled/ivf_L_3	11136	3189	building index: assigning tuples	71488	48079	0	0	68713	0
m3r-pooled/ivf_L_3	11214	3189	building index: assigning tuples	71488	49437	0	0	68713	0
m3r-pooled/ivf_L_3	11287	3189	building index: assigning tuples	71488	50563	0	0	68713	0
m3r-pooled/ivf_L_3	11389	3189	building index: assigning tuples	71488	52106	0	0	68713	0
m3r-pooled/ivf_L_3	11470	3189	building index: assigning tuples	71488	53628	0	0	68713	0
m3r-pooled/ivf_L_3	11551	3189	building index: assigning tuples	71488	54043	0	0	68713	0
m3r-pooled/ivf_L_3	11615	3189	building index: assigning tuples	71488	54240	0	0	68713	0
m3r-pooled/ivf_L_3	11687	3189	building index: assigning tuples	71488	55963	0	0	68713	0
m3r-pooled/ivf_L_3	11764	3189	building index: assigning tuples	71488	56767	0	0	68713	0
m3r-pooled/ivf_L_3	11827	3189	building index: assigning tuples	71488	57365	0	0	68713	0
m3r-pooled/ivf_L_3	11903	3189	building index: assigning tuples	71488	57791	0	0	68713	0
m3r-pooled/ivf_L_3	11964	3189	building index: assigning tuples	71488	57791	0	0	68713	0
m3r-pooled/ivf_L_3	12025	3189	building index: assigning tuples	71488	57791	0	0	68713	0
m3r-pooled/ivf_L_3	12085	3189	building index: assigning tuples	71488	58528	0	0	68713	0
m3r-pooled/ivf_L_3	12152	3189	building index: assigning tuples	71488	59827	0	0	68713	0
m3r-pooled/ivf_L_3	12230	3189	building index: assigning tuples	71488	61066	0	0	68713	0
m3r-pooled/ivf_L_3	12305	3189	building index: assigning tuples	71488	62589	0	0	68713	0
m3r-pooled/ivf_L_3	12390	3189	building index: assigning tuples	71488	63791	0	0	68713	0
m3r-pooled/ivf_L_3	12465	3189	building index: assigning tuples	71488	64945	0	0	68713	0
m3r-pooled/ivf_L_3	12557	3189	building index: assigning tuples	71488	66644	0	0	68713	0
m3r-pooled/ivf_L_3	12641	3189	building index: assigning tuples	71488	68017	0	0	68713	0
m3r-pooled/ivf_L_3	12730	3189	building index: assigning tuples	71488	69207	0	0	68713	0
m3r-pooled/ivf_L_3	12805	3189	building index: assigning tuples	71488	70613	0	0	68713	0
m3r-pooled/ivf_L_3	12871	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_3	12940	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_3	13008	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_3	13072	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_3	13133	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_3	13196	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_3	13259	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_3	13320	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_3	13382	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_3	13445	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_3	13505	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_3	13567	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_3	13629	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_3	13690	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_3	13752	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_3	13814	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-pooled/ivf_L_3	13875	3189	building index: loading tuples	71488	71488	1000000	9375	68713	0
m3r-pooled/ivf_L_3	13938	3189	building index: loading tuples	71488	71488	1000000	30941	68713	0
m3r-pooled/ivf_L_3	14000	3189	building index: loading tuples	71488	71488	1000000	30941	68713	0
m3r-pooled/ivf_L_3	14061	3189	building index: loading tuples	71488	71488	1000000	30941	68713	0
m3r-pooled/ivf_L_3	14122	3189	building index: loading tuples	71488	71488	1000000	46785	68713	0
m3r-pooled/ivf_L_3	14184	3189	building index: loading tuples	71488	71488	1000000	46785	68713	0
m3r-pooled/ivf_L_3	14244	3189	building index: loading tuples	71488	71488	1000000	46785	68713	0
m3r-pooled/ivf_L_3	14305	3189	building index: loading tuples	71488	71488	1000000	46785	68713	0
m3r-pooled/ivf_L_3	14365	3189	building index: loading tuples	71488	71488	1000000	46785	68713	0
m3r-pooled/ivf_L_3	14427	3189	building index: loading tuples	71488	71488	1000000	46800	68713	0
m3r-pooled/ivf_L_3	14489	3189	building index: loading tuples	71488	71488	1000000	46800	68713	0
m3r-pooled/ivf_L_3	14550	3189	building index: loading tuples	71488	71488	1000000	46800	68713	0
m3r-pooled/ivf_L_3	14611	3189	building index: loading tuples	71488	71488	1000000	46800	68713	0
m3r-pooled/ivf_L_3	14672	3189	building index: loading tuples	71488	71488	1000000	46800	68713	0
m3r-pooled/ivf_L_3	14734	3189	building index: loading tuples	71488	71488	1000000	71053	68713	0
m3r-pooled/ivf_L_3	14796	3189	building index: loading tuples	71488	71488	1000000	102405	68713	0
m3r-pooled/ivf_L_3	14858	3189	building index: loading tuples	71488	71488	1000000	108653	68713	0
m3r-pooled/ivf_L_3	14918	3189	building index: loading tuples	71488	71488	1000000	108653	68713	0
m3r-pooled/ivf_L_3	14979	3189	building index: loading tuples	71488	71488	1000000	108653	68713	0
m3r-pooled/ivf_L_3	15038	3189	building index: loading tuples	71488	71488	1000000	108653	68713	0
m3r-pooled/ivf_L_3	15098	3189	building index: loading tuples	71488	71488	1000000	108668	68713	0
m3r-pooled/ivf_L_3	15160	3189	building index: loading tuples	71488	71488	1000000	108668	68713	0
m3r-pooled/ivf_L_3	15220	3189	building index: loading tuples	71488	71488	1000000	108668	68713	0
m3r-pooled/ivf_L_3	15280	3189	building index: loading tuples	71488	71488	1000000	108668	68713	0
m3r-pooled/ivf_L_3	15340	3189	building index: loading tuples	71488	71488	1000000	108668	68713	0
m3r-pooled/ivf_L_3	15400	3189	building index: loading tuples	71488	71488	1000000	108668	68713	0
m3r-pooled/ivf_L_3	15461	3189	building index: loading tuples	71488	71488	1000000	139584	68713	0
m3r-pooled/ivf_L_3	15521	3189	building index: loading tuples	71488	71488	1000000	139584	68713	0
m3r-pooled/ivf_L_3	15581	3189	building index: loading tuples	71488	71488	1000000	139584	68713	0
m3r-pooled/ivf_L_3	15641	3189	building index: loading tuples	71488	71488	1000000	139584	68713	0
m3r-pooled/ivf_L_3	15701	3189	building index: loading tuples	71488	71488	1000000	139599	68713	0
m3r-pooled/ivf_L_3	15762	3189	building index: loading tuples	71488	71488	1000000	139599	68713	0
m3r-pooled/ivf_L_3	15822	3189	building index: loading tuples	71488	71488	1000000	170503	68713	0
m3r-pooled/ivf_L_3	15883	3189	building index: loading tuples	71488	71488	1000000	200254	68713	0
m3r-pooled/ivf_L_3	15945	3189	building index: loading tuples	71488	71488	1000000	223838	68713	0
m3r-pooled/ivf_L_3	16006	3189	building index: loading tuples	71488	71488	1000000	232774	68713	0
m3r-pooled/ivf_L_3	16067	3189	building index: loading tuples	71488	71488	1000000	258161	68713	0
m3r-pooled/ivf_L_3	16129	3189	building index: loading tuples	71488	71488	1000000	294210	68713	0
m3r-pooled/ivf_L_3	16190	3189	building index: loading tuples	71488	71488	1000000	325123	68713	0
m3r-pooled/ivf_L_3	16251	3189	building index: loading tuples	71488	71488	1000000	335696	68713	0
m3r-pooled/ivf_L_3	16312	3189	building index: loading tuples	71488	71488	1000000	379453	68713	0
m3r-pooled/ivf_L_3	16373	3189	building index: loading tuples	71488	71488	1000000	401957	68713	0
m3r-pooled/ivf_L_3	16435	3189	building index: loading tuples	71488	71488	1000000	417905	68713	0
m3r-pooled/ivf_L_3	16496	3189	building index: loading tuples	71488	71488	1000000	423426	68713	0
m3r-pooled/ivf_L_3	16558	3189	building index: loading tuples	71488	71488	1000000	467479	68713	0
m3r-pooled/ivf_L_3	16619	3189	building index: loading tuples	71488	71488	1000000	490915	68713	0
m3r-pooled/ivf_L_3	16680	3189	building index: loading tuples	71488	71488	1000000	518075	68713	0
m3r-pooled/ivf_L_3	16742	3189	building index: loading tuples	71488	71488	1000000	543910	68713	0
m3r-pooled/ivf_L_3	16803	3189	building index: loading tuples	71488	71488	1000000	554088	68713	0
m3r-pooled/ivf_L_3	16865	3189	building index: loading tuples	71488	71488	1000000	572529	68713	0
m3r-pooled/ivf_L_3	16926	3189	building index: loading tuples	71488	71488	1000000	572529	68713	0
m3r-pooled/ivf_L_3	16986	3189	building index: loading tuples	71488	71488	1000000	572529	68713	0
m3r-pooled/ivf_L_3	17046	3189	building index: loading tuples	71488	71488	1000000	572529	68713	0
m3r-pooled/ivf_L_3	17106	3189	building index: loading tuples	71488	71488	1000000	572544	68713	0
m3r-pooled/ivf_L_3	17166	3189	building index: loading tuples	71488	71488	1000000	572544	68713	0
m3r-pooled/ivf_L_3	17227	3189	building index: loading tuples	71488	71488	1000000	610583	68713	0
m3r-pooled/ivf_L_3	17289	3189	building index: loading tuples	71488	71488	1000000	634375	68713	0
m3r-pooled/ivf_L_3	17350	3189	building index: loading tuples	71488	71488	1000000	665305	68713	0
m3r-pooled/ivf_L_3	17424	3189	building index: loading tuples	71488	71488	1000000	727166	68713	0
m3r-pooled/ivf_L_3	17566	3189	building index: loading tuples	71488	71488	1000000	746556	68713	0
m3r-pooled/ivf_L_3	17628	3189	building index: loading tuples	71488	71488	1000000	758097	68713	0
m3r-pooled/ivf_L_3	17721	3189	building index: loading tuples	71488	71488	1000000	758112	68713	0
m3r-pooled/ivf_L_3	17782	3189	building index: loading tuples	71488	71488	1000000	758112	68713	0
m3r-pooled/ivf_L_3	17842	3189	building index: loading tuples	71488	71488	1000000	758112	68713	0
m3r-pooled/ivf_L_3	17901	3189	building index: loading tuples	71488	71488	1000000	758112	68713	0
m3r-pooled/ivf_L_3	17962	3189	building index: loading tuples	71488	71488	1000000	776992	68713	0
m3r-pooled/ivf_L_3	18024	3189	building index: loading tuples	71488	71488	1000000	811856	68713	0
m3r-pooled/ivf_L_3	18086	3189	building index: loading tuples	71488	71488	1000000	832706	68713	0
m3r-pooled/ivf_L_3	18147	3189	building index: loading tuples	71488	71488	1000000	850871	68713	0
m3r-pooled/ivf_L_3	18225	3189	building index: loading tuples	71488	71488	1000000	877468	68713	0
m3r-pooled/ivf_L_3	18286	3189	building index: loading tuples	71488	71488	1000000	912711	68713	0
m3r-pooled/ivf_L_3	18358	3189	building index: loading tuples	71488	71488	1000000	912711	68713	0
m3r-pooled/ivf_L_3	18418	3189	building index: loading tuples	71488	71488	1000000	912711	68713	0
m3r-pooled/ivf_L_3	18479	3189	building index: loading tuples	71488	71488	1000000	912711	68713	0
m3r-pooled/ivf_L_3	18539	3189	building index: loading tuples	71488	71488	1000000	912726	68713	0
m3r-pooled/ivf_L_3	18600	3189	building index: loading tuples	71488	71488	1000000	912726	68713	0
m3r-pooled/ivf_L_3	18661	3189	building index: loading tuples	71488	71488	1000000	930493	68713	0
m3r-pooled/ivf_L_3	18722	3189	building index: loading tuples	71488	71488	1000000	965676	68713	0
m3r-pooled/ivf_L_3	18786	3189	building index: loading tuples	71488	71488	1000000	982978	68713	0
m3r-pooled/ivf_L_3	18848	3189	building index: loading tuples	71488	71488	1000000	999999	68713	0
m3r-pooled/ivf_L_3	18909	3189	building index: loading tuples	71488	71488	1000000	1000000	68713	0
m3r-pooled/ivf_M_1	1	31977	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_1	61	31977	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_1	122	31977	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_1	182	31977	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_1	243	31977	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_1	303	31977	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_1	363	31977	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_1	424	31977	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_1	484	31977	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_1	544	31977	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_1	612	31977	building index: assigning tuples	21440	1055	0	0	68707	0
m3r-pooled/ivf_M_1	707	31977	building index: assigning tuples	21440	3273	0	0	68707	0
m3r-pooled/ivf_M_1	782	31977	building index: assigning tuples	21440	5213	0	0	68707	0
m3r-pooled/ivf_M_1	845	31977	building index: assigning tuples	21440	6850	0	0	68707	0
m3r-pooled/ivf_M_1	908	31977	building index: assigning tuples	21440	8723	0	0	68707	0
m3r-pooled/ivf_M_1	1005	31977	building index: assigning tuples	21440	11135	0	0	68707	0
m3r-pooled/ivf_M_1	1085	31977	building index: assigning tuples	21440	13186	0	0	68707	0
m3r-pooled/ivf_M_1	1157	31977	building index: assigning tuples	21440	14990	0	0	68707	0
m3r-pooled/ivf_M_1	1242	31977	building index: assigning tuples	21440	17123	0	0	68707	0
m3r-pooled/ivf_M_1	1318	31977	building index: assigning tuples	21440	18945	0	0	68707	0
m3r-pooled/ivf_M_1	1382	31977	building index: assigning tuples	21440	20543	0	0	68707	0
m3r-pooled/ivf_M_1	1444	31977	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-pooled/ivf_M_1	1507	31977	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-pooled/ivf_M_1	1567	31977	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-pooled/ivf_M_1	1627	31977	building index: loading tuples	21440	21440	300000	41649	68707	0
m3r-pooled/ivf_M_1	1687	31977	building index: loading tuples	21440	21440	300000	69606	68707	0
m3r-pooled/ivf_M_1	1748	31977	building index: loading tuples	21440	21440	300000	97338	68707	0
m3r-pooled/ivf_M_1	1808	31977	building index: loading tuples	21440	21440	300000	131472	68707	0
m3r-pooled/ivf_M_1	1868	31977	building index: loading tuples	21440	21440	300000	131472	68707	0
m3r-pooled/ivf_M_1	1930	31977	building index: loading tuples	21440	21440	300000	142657	68707	0
m3r-pooled/ivf_M_1	1989	31977	building index: loading tuples	21440	21440	300000	158145	68707	0
m3r-pooled/ivf_M_1	2049	31977	building index: loading tuples	21440	21440	300000	158145	68707	0
m3r-pooled/ivf_M_1	2108	31977	building index: loading tuples	21440	21440	300000	158145	68707	0
m3r-pooled/ivf_M_1	2169	31977	building index: loading tuples	21440	21440	300000	158145	68707	0
m3r-pooled/ivf_M_1	2229	31977	building index: loading tuples	21440	21440	300000	182469	68707	0
m3r-pooled/ivf_M_1	2290	31977	building index: loading tuples	21440	21440	300000	210224	68707	0
m3r-pooled/ivf_M_1	2350	31977	building index: loading tuples	21440	21440	300000	239684	68707	0
m3r-pooled/ivf_M_1	2414	31977	building index: loading tuples	21440	21440	300000	261464	68707	0
m3r-pooled/ivf_M_1	2475	31977	building index: loading tuples	21440	21440	300000	297835	68707	0
m3r-pooled/ivf_M_2	1	32197	initializing	0	0	0	0	68707	0
m3r-pooled/ivf_M_2	60	32197	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_2	120	32197	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_2	179	32197	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_2	239	32197	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_2	299	32197	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_2	359	32197	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_2	419	32197	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_2	478	32197	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_2	538	32197	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_2	603	32197	building index: assigning tuples	21440	1456	0	0	68707	0
m3r-pooled/ivf_M_2	690	32197	building index: assigning tuples	21440	3171	0	0	68707	0
m3r-pooled/ivf_M_2	762	32197	building index: assigning tuples	21440	5187	0	0	68707	0
m3r-pooled/ivf_M_2	838	32197	building index: assigning tuples	21440	7085	0	0	68707	0
m3r-pooled/ivf_M_2	900	32197	building index: assigning tuples	21440	9132	0	0	68707	0
m3r-pooled/ivf_M_2	989	32197	building index: assigning tuples	21440	11142	0	0	68707	0
m3r-pooled/ivf_M_2	1069	32197	building index: assigning tuples	21440	13108	0	0	68707	0
m3r-pooled/ivf_M_2	1140	32197	building index: assigning tuples	21440	14801	0	0	68707	0
m3r-pooled/ivf_M_2	1200	32197	building index: assigning tuples	21440	16584	0	0	68707	0
m3r-pooled/ivf_M_2	1259	32197	building index: assigning tuples	21440	18165	0	0	68707	0
m3r-pooled/ivf_M_2	1317	32197	building index: assigning tuples	21440	19692	0	0	68707	0
m3r-pooled/ivf_M_2	1376	32197	building index: assigning tuples	21440	21265	0	0	68707	0
m3r-pooled/ivf_M_2	1436	32197	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-pooled/ivf_M_2	1497	32197	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-pooled/ivf_M_2	1557	32197	building index: loading tuples	21440	21440	300000	19521	68707	0
m3r-pooled/ivf_M_2	1617	32197	building index: loading tuples	21440	21440	300000	49455	68707	0
m3r-pooled/ivf_M_2	1676	32197	building index: loading tuples	21440	21440	300000	79671	68707	0
m3r-pooled/ivf_M_2	1737	32197	building index: loading tuples	21440	21440	300000	109684	68707	0
m3r-pooled/ivf_M_2	1797	32197	building index: loading tuples	21440	21440	300000	140615	68707	0
m3r-pooled/ivf_M_2	1857	32197	building index: loading tuples	21440	21440	300000	140615	68707	0
m3r-pooled/ivf_M_2	1916	32197	building index: loading tuples	21440	21440	300000	140615	68707	0
m3r-pooled/ivf_M_2	1975	32197	building index: loading tuples	21440	21440	300000	140615	68707	0
m3r-pooled/ivf_M_2	2034	32197	building index: loading tuples	21440	21440	300000	140615	68707	0
m3r-pooled/ivf_M_2	2094	32197	building index: loading tuples	21440	21440	300000	140615	68707	0
m3r-pooled/ivf_M_2	2154	32197	building index: loading tuples	21440	21440	300000	140615	68707	0
m3r-pooled/ivf_M_2	2214	32197	building index: loading tuples	21440	21440	300000	140615	68707	0
m3r-pooled/ivf_M_2	2274	32197	building index: loading tuples	21440	21440	300000	140630	68707	0
m3r-pooled/ivf_M_2	2334	32197	building index: loading tuples	21440	21440	300000	140630	68707	0
m3r-pooled/ivf_M_2	2394	32197	building index: loading tuples	21440	21440	300000	161030	68707	0
m3r-pooled/ivf_M_2	2459	32197	building index: loading tuples	21440	21440	300000	202467	68707	0
m3r-pooled/ivf_M_2	2521	32197	building index: loading tuples	21440	21440	300000	233393	68707	0
m3r-pooled/ivf_M_2	2581	32197	building index: loading tuples	21440	21440	300000	257008	68707	0
m3r-pooled/ivf_M_2	2642	32197	building index: loading tuples	21440	21440	300000	265841	68707	0
m3r-pooled/ivf_M_2	2704	32197	building index: loading tuples	21440	21440	300000	295257	68707	0
m3r-pooled/ivf_M_2	2764	32197	building index: loading tuples	21440	21440	300000	299999	68707	0
m3r-pooled/ivf_M_3	1	32446	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_3	60	32446	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_3	121	32446	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_3	182	32446	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_3	242	32446	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_3	303	32446	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_3	363	32446	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_3	424	32446	building index: performing k-means	0	0	0	0	68707	0
m3r-pooled/ivf_M_3	484	32446	building index: assigning tuples	21440	359	0	0	68707	0
m3r-pooled/ivf_M_3	545	32446	building index: assigning tuples	21440	1783	0	0	68707	0
m3r-pooled/ivf_M_3	614	32446	building index: assigning tuples	21440	3103	0	0	68707	0
m3r-pooled/ivf_M_3	683	32446	building index: assigning tuples	21440	4709	0	0	68707	0
m3r-pooled/ivf_M_3	754	32446	building index: assigning tuples	21440	6527	0	0	68707	0
m3r-pooled/ivf_M_3	826	32446	building index: assigning tuples	21440	8331	0	0	68707	0
m3r-pooled/ivf_M_3	893	32446	building index: assigning tuples	21440	10070	0	0	68707	0
m3r-pooled/ivf_M_3	955	32446	building index: assigning tuples	21440	11612	0	0	68707	0
m3r-pooled/ivf_M_3	1016	32446	building index: assigning tuples	21440	13191	0	0	68707	0
m3r-pooled/ivf_M_3	1078	32446	building index: assigning tuples	21440	14801	0	0	68707	0
m3r-pooled/ivf_M_3	1140	32446	building index: assigning tuples	21440	16389	0	0	68707	0
m3r-pooled/ivf_M_3	1202	32446	building index: assigning tuples	21440	17981	0	0	68707	0
m3r-pooled/ivf_M_3	1264	32446	building index: assigning tuples	21440	19617	0	0	68707	0
m3r-pooled/ivf_M_3	1326	32446	building index: assigning tuples	21440	21204	0	0	68707	0
m3r-pooled/ivf_M_3	1389	32446	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-pooled/ivf_M_3	1452	32446	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-pooled/ivf_M_3	1512	32446	building index: loading tuples	21440	21440	300000	17194	68707	0
m3r-pooled/ivf_M_3	1571	32446	building index: loading tuples	21440	21440	300000	56344	68707	0
m3r-pooled/ivf_M_3	1631	32446	building index: loading tuples	21440	21440	300000	81050	68707	0
m3r-pooled/ivf_M_3	1691	32446	building index: loading tuples	21440	21440	300000	87897	68707	0
m3r-pooled/ivf_M_3	1751	32446	building index: loading tuples	21440	21440	300000	87912	68707	0
m3r-pooled/ivf_M_3	1812	32446	building index: loading tuples	21440	21440	300000	87927	68707	0
m3r-pooled/ivf_M_3	1873	32446	building index: loading tuples	21440	21440	300000	87927	68707	0
m3r-pooled/ivf_M_3	1932	32446	building index: loading tuples	21440	21440	300000	125550	68707	0
m3r-pooled/ivf_M_3	1993	32446	building index: loading tuples	21440	21440	300000	153357	68707	0
m3r-pooled/ivf_M_3	2053	32446	building index: loading tuples	21440	21440	300000	170470	68707	0
m3r-pooled/ivf_M_3	2112	32446	building index: loading tuples	21440	21440	300000	204423	68707	0
m3r-pooled/ivf_M_3	2172	32446	building index: loading tuples	21440	21440	300000	242504	68707	0
m3r-pooled/ivf_M_3	2232	32446	building index: loading tuples	21440	21440	300000	242504	68707	0
m3r-pooled/ivf_M_3	2291	32446	building index: loading tuples	21440	21440	300000	242504	68707	0
m3r-pooled/ivf_M_3	2351	32446	building index: loading tuples	21440	21440	300000	242504	68707	0
m3r-pooled/ivf_M_3	2410	32446	building index: loading tuples	21440	21440	300000	242504	68707	0
m3r-pooled/ivf_M_3	2469	32446	building index: loading tuples	21440	21440	300000	242504	68707	0
m3r-pooled/ivf_M_3	2527	32446	building index: loading tuples	21440	21440	300000	242504	68707	0
m3r-pooled/ivf_M_3	2586	32446	building index: loading tuples	21440	21440	300000	242519	68707	0
m3r-pooled/ivf_M_3	2645	32446	building index: loading tuples	21440	21440	300000	282761	68707	0
m3r-pooled/ivf_M_3	2705	32446	building index: loading tuples	21440	21440	300000	300000	68707	0
m3r-pooled/ivf_S_1	1	30575	building index: performing k-means	0	0	0	0	68701	0
m3r-pooled/ivf_S_1	93	30575	building index: performing k-means	0	0	0	0	68701	0
m3r-pooled/ivf_S_1	154	30575	building index: performing k-means	0	0	0	0	68701	0
m3r-pooled/ivf_S_1	214	30575	building index: performing k-means	0	0	0	0	68701	0
m3r-pooled/ivf_S_1	276	30575	building index: performing k-means	0	0	0	0	68701	0
m3r-pooled/ivf_S_1	339	30575	building index: performing k-means	0	0	0	0	68701	0
m3r-pooled/ivf_S_1	401	30575	building index: performing k-means	0	0	0	0	68701	0
m3r-pooled/ivf_S_1	463	30575	building index: assigning tuples	7168	1351	0	0	68701	0
m3r-pooled/ivf_S_1	527	30575	building index: assigning tuples	7168	3555	0	0	68701	0
m3r-pooled/ivf_S_1	589	30575	building index: assigning tuples	7168	5752	0	0	68701	0
m3r-pooled/ivf_S_1	651	30575	building index: assigning tuples	7168	7168	0	0	68701	0
m3r-pooled/ivf_S_1	711	30575	building index: loading tuples	7168	7168	100000	31795	68701	0
m3r-pooled/ivf_S_1	772	30575	building index: loading tuples	7168	7168	100000	51719	68701	0
m3r-pooled/ivf_S_1	831	30575	building index: loading tuples	7168	7168	100000	51719	68701	0
m3r-pooled/ivf_S_1	889	30575	building index: loading tuples	7168	7168	100000	51719	68701	0
m3r-pooled/ivf_S_1	948	30575	building index: loading tuples	7168	7168	100000	63730	68701	0
m3r-pooled/ivf_S_1	1007	30575	building index: loading tuples	7168	7168	100000	63730	68701	0
m3r-pooled/ivf_S_1	1065	30575	building index: loading tuples	7168	7168	100000	63730	68701	0
m3r-pooled/ivf_S_1	1125	30575	building index: loading tuples	7168	7168	100000	94716	68701	0
m3r-pooled/ivf_S_2	0	30795	building index: performing k-means	0	0	0	0	68701	0
m3r-pooled/ivf_S_2	60	30795	building index: performing k-means	0	0	0	0	68701	0
m3r-pooled/ivf_S_2	119	30795	building index: performing k-means	0	0	0	0	68701	0
m3r-pooled/ivf_S_2	179	30795	building index: performing k-means	0	0	0	0	68701	0
m3r-pooled/ivf_S_2	239	30795	building index: performing k-means	0	0	0	0	68701	0
m3r-pooled/ivf_S_2	299	30795	building index: performing k-means	0	0	0	0	68701	0
m3r-pooled/ivf_S_2	360	30795	building index: assigning tuples	7168	1535	0	0	68701	0
m3r-pooled/ivf_S_2	421	30795	building index: assigning tuples	7168	3775	0	0	68701	0
m3r-pooled/ivf_S_2	483	30795	building index: assigning tuples	7168	6035	0	0	68701	0
m3r-pooled/ivf_S_2	544	30795	building index: assigning tuples	7168	7168	0	0	68701	0
m3r-pooled/ivf_S_2	604	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	664	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	722	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	781	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	840	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	898	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	956	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	1015	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	1073	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	1132	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	1190	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	1249	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	1308	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	1366	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	1425	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	1484	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	1542	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	1601	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	1660	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	1719	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	1778	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	1838	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	1898	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	1958	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	2020	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	2079	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	2139	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	2198	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	2256	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	2314	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	2373	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	2431	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	2490	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	2548	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	2606	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	2665	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	2723	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	2782	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	2840	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	2898	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	2956	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	3014	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	3073	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	3131	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	3189	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	3248	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	3306	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	3364	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	3424	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	3483	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	3543	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	3602	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	3662	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	3721	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	3780	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	3840	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	3899	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	3958	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	4019	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	4079	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	4138	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	4197	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	4257	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	4317	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	4376	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	4436	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	4495	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	4555	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	4614	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	4674	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	4733	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	4793	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	4852	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	4911	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	4970	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	5028	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	5088	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	5148	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	5207	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	5267	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	5326	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-pooled/ivf_S_2	5386	30795	building index: loading tuples	7168	7168	100000	42181	68701	0
m3r-pooled/ivf_S_2	5445	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-pooled/ivf_S_2	5504	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-pooled/ivf_S_2	5563	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-pooled/ivf_S_2	5622	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-pooled/ivf_S_2	5681	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-pooled/ivf_S_2	5740	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-pooled/ivf_S_2	5799	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-pooled/ivf_S_2	5858	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-pooled/ivf_S_2	5919	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-pooled/ivf_S_2	5978	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-pooled/ivf_S_2	6037	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-pooled/ivf_S_2	6096	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-pooled/ivf_S_2	6155	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-pooled/ivf_S_2	6215	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-pooled/ivf_S_2	6274	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-pooled/ivf_S_2	6333	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-pooled/ivf_S_2	6392	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-pooled/ivf_S_2	6451	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-pooled/ivf_S_2	6511	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-pooled/ivf_S_2	6572	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-pooled/ivf_S_2	6631	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-pooled/ivf_S_2	6691	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-pooled/ivf_S_2	6751	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-pooled/ivf_S_2	6811	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-pooled/ivf_S_2	6871	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-pooled/ivf_S_2	6931	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-pooled/ivf_S_2	6991	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-pooled/ivf_S_2	7051	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-pooled/ivf_S_2	7111	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-pooled/ivf_S_2	7170	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-pooled/ivf_S_2	7231	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-pooled/ivf_S_2	7291	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-pooled/ivf_S_2	7351	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-pooled/ivf_S_2	7411	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-pooled/ivf_S_2	7471	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-pooled/ivf_S_2	7532	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-pooled/ivf_S_2	7592	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-pooled/ivf_S_2	7653	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-pooled/ivf_S_2	7716	30795	building index: loading tuples	7168	7168	100000	75313	68701	0
m3r-pooled/ivf_S_2	7779	30795	building index: loading tuples	7168	7168	100000	75313	68701	0
m3r-pooled/ivf_S_2	7842	30795	building index: loading tuples	7168	7168	100000	75313	68701	0
m3r-pooled/ivf_S_2	7905	30795	building index: loading tuples	7168	7168	100000	75313	68701	0
m3r-pooled/ivf_S_2	7966	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-pooled/ivf_S_2	8025	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-pooled/ivf_S_2	8085	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-pooled/ivf_S_2	8145	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-pooled/ivf_S_2	8205	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-pooled/ivf_S_2	8267	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-pooled/ivf_S_2	8328	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-pooled/ivf_S_2	8388	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-pooled/ivf_S_2	8447	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-pooled/ivf_S_2	8506	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-pooled/ivf_S_2	8565	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-pooled/ivf_S_2	8624	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-pooled/ivf_S_2	8683	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-pooled/ivf_S_2	8742	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-pooled/ivf_S_2	8802	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-pooled/ivf_S_2	8863	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-pooled/ivf_S_2	8922	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-pooled/ivf_S_2	8982	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-pooled/ivf_S_2	9042	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-pooled/ivf_S_2	9102	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-pooled/ivf_S_2	9163	30795	building index: loading tuples	7168	7168	100000	99444	68701	0
m3r-pooled/ivf_S_3	1	31710	building index: performing k-means	0	0	0	0	68701	0
m3r-pooled/ivf_S_3	60	31710	building index: performing k-means	0	0	0	0	68701	0
m3r-pooled/ivf_S_3	120	31710	building index: performing k-means	0	0	0	0	68701	0
m3r-pooled/ivf_S_3	181	31710	building index: performing k-means	0	0	0	0	68701	0
m3r-pooled/ivf_S_3	241	31710	building index: performing k-means	0	0	0	0	68701	0
m3r-pooled/ivf_S_3	303	31710	building index: performing k-means	0	0	0	0	68701	0
m3r-pooled/ivf_S_3	364	31710	building index: assigning tuples	7168	132	0	0	68701	0
m3r-pooled/ivf_S_3	427	31710	building index: assigning tuples	7168	2274	0	0	68701	0
m3r-pooled/ivf_S_3	489	31710	building index: assigning tuples	7168	4497	0	0	68701	0
m3r-pooled/ivf_S_3	550	31710	building index: assigning tuples	7168	6699	0	0	68701	0
m3r-pooled/ivf_S_3	612	31710	building index: assigning tuples	7168	7168	0	0	68701	0
m3r-pooled/ivf_S_3	672	31710	building index: loading tuples	7168	7168	100000	30827	68701	0
m3r-pooled/ivf_S_3	732	31710	building index: loading tuples	7168	7168	100000	30827	68701	0
m3r-pooled/ivf_S_3	791	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-pooled/ivf_S_3	850	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-pooled/ivf_S_3	910	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-pooled/ivf_S_3	971	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-pooled/ivf_S_3	1030	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-pooled/ivf_S_3	1090	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-pooled/ivf_S_3	1150	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-pooled/ivf_S_3	1210	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-pooled/ivf_S_3	1270	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-pooled/ivf_S_3	1331	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-pooled/ivf_S_3	1391	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-pooled/ivf_S_3	1451	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-pooled/ivf_S_3	1510	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-pooled/ivf_S_3	1570	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-pooled/ivf_S_3	1629	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-pooled/ivf_S_3	1689	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-pooled/ivf_S_3	1748	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-pooled/ivf_S_3	1808	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-pooled/ivf_S_3	1868	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-pooled/ivf_S_3	1928	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-pooled/ivf_S_3	1988	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-pooled/ivf_S_3	2048	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-pooled/ivf_S_3	2107	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-pooled/ivf_S_3	2168	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-pooled/ivf_S_3	2227	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-pooled/ivf_S_3	2287	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-pooled/ivf_S_3	2348	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-pooled/ivf_S_3	2409	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-pooled/ivf_S_3	2469	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-pooled/ivf_S_3	2530	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-pooled/ivf_S_3	2591	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-pooled/ivf_S_3	2651	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-pooled/ivf_S_3	2711	31710	building index: loading tuples	7168	7168	100000	87015	68701	0
m3r-S/ivf_S_1	1	30575	building index: performing k-means	0	0	0	0	68701	0
m3r-S/ivf_S_1	93	30575	building index: performing k-means	0	0	0	0	68701	0
m3r-S/ivf_S_1	154	30575	building index: performing k-means	0	0	0	0	68701	0
m3r-S/ivf_S_1	214	30575	building index: performing k-means	0	0	0	0	68701	0
m3r-S/ivf_S_1	276	30575	building index: performing k-means	0	0	0	0	68701	0
m3r-S/ivf_S_1	339	30575	building index: performing k-means	0	0	0	0	68701	0
m3r-S/ivf_S_1	401	30575	building index: performing k-means	0	0	0	0	68701	0
m3r-S/ivf_S_1	463	30575	building index: assigning tuples	7168	1351	0	0	68701	0
m3r-S/ivf_S_1	527	30575	building index: assigning tuples	7168	3555	0	0	68701	0
m3r-S/ivf_S_1	589	30575	building index: assigning tuples	7168	5752	0	0	68701	0
m3r-S/ivf_S_1	651	30575	building index: assigning tuples	7168	7168	0	0	68701	0
m3r-S/ivf_S_1	711	30575	building index: loading tuples	7168	7168	100000	31795	68701	0
m3r-S/ivf_S_1	772	30575	building index: loading tuples	7168	7168	100000	51719	68701	0
m3r-S/ivf_S_1	831	30575	building index: loading tuples	7168	7168	100000	51719	68701	0
m3r-S/ivf_S_1	889	30575	building index: loading tuples	7168	7168	100000	51719	68701	0
m3r-S/ivf_S_1	948	30575	building index: loading tuples	7168	7168	100000	63730	68701	0
m3r-S/ivf_S_1	1007	30575	building index: loading tuples	7168	7168	100000	63730	68701	0
m3r-S/ivf_S_1	1065	30575	building index: loading tuples	7168	7168	100000	63730	68701	0
m3r-S/ivf_S_1	1125	30575	building index: loading tuples	7168	7168	100000	94716	68701	0
m3r-S/ivf_S_2	0	30795	building index: performing k-means	0	0	0	0	68701	0
m3r-S/ivf_S_2	60	30795	building index: performing k-means	0	0	0	0	68701	0
m3r-S/ivf_S_2	119	30795	building index: performing k-means	0	0	0	0	68701	0
m3r-S/ivf_S_2	179	30795	building index: performing k-means	0	0	0	0	68701	0
m3r-S/ivf_S_2	239	30795	building index: performing k-means	0	0	0	0	68701	0
m3r-S/ivf_S_2	299	30795	building index: performing k-means	0	0	0	0	68701	0
m3r-S/ivf_S_2	360	30795	building index: assigning tuples	7168	1535	0	0	68701	0
m3r-S/ivf_S_2	421	30795	building index: assigning tuples	7168	3775	0	0	68701	0
m3r-S/ivf_S_2	483	30795	building index: assigning tuples	7168	6035	0	0	68701	0
m3r-S/ivf_S_2	544	30795	building index: assigning tuples	7168	7168	0	0	68701	0
m3r-S/ivf_S_2	604	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	664	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	722	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	781	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	840	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	898	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	956	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	1015	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	1073	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	1132	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	1190	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	1249	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	1308	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	1366	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	1425	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	1484	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	1542	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	1601	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	1660	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	1719	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	1778	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	1838	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	1898	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	1958	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	2020	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	2079	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	2139	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	2198	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	2256	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	2314	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	2373	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	2431	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	2490	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	2548	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	2606	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	2665	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	2723	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	2782	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	2840	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	2898	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	2956	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	3014	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	3073	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	3131	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	3189	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	3248	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	3306	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	3364	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	3424	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	3483	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	3543	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	3602	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	3662	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	3721	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	3780	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	3840	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	3899	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	3958	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	4019	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	4079	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	4138	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	4197	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	4257	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	4317	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	4376	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	4436	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	4495	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	4555	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	4614	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	4674	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	4733	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	4793	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	4852	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	4911	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	4970	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	5028	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	5088	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	5148	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	5207	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	5267	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	5326	30795	building index: loading tuples	7168	7168	100000	30812	68701	0
m3r-S/ivf_S_2	5386	30795	building index: loading tuples	7168	7168	100000	42181	68701	0
m3r-S/ivf_S_2	5445	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-S/ivf_S_2	5504	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-S/ivf_S_2	5563	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-S/ivf_S_2	5622	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-S/ivf_S_2	5681	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-S/ivf_S_2	5740	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-S/ivf_S_2	5799	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-S/ivf_S_2	5858	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-S/ivf_S_2	5919	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-S/ivf_S_2	5978	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-S/ivf_S_2	6037	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-S/ivf_S_2	6096	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-S/ivf_S_2	6155	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-S/ivf_S_2	6215	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-S/ivf_S_2	6274	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-S/ivf_S_2	6333	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-S/ivf_S_2	6392	30795	building index: loading tuples	7168	7168	100000	44388	68701	0
m3r-S/ivf_S_2	6451	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-S/ivf_S_2	6511	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-S/ivf_S_2	6572	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-S/ivf_S_2	6631	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-S/ivf_S_2	6691	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-S/ivf_S_2	6751	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-S/ivf_S_2	6811	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-S/ivf_S_2	6871	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-S/ivf_S_2	6931	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-S/ivf_S_2	6991	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-S/ivf_S_2	7051	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-S/ivf_S_2	7111	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-S/ivf_S_2	7170	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-S/ivf_S_2	7231	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-S/ivf_S_2	7291	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-S/ivf_S_2	7351	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-S/ivf_S_2	7411	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-S/ivf_S_2	7471	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-S/ivf_S_2	7532	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-S/ivf_S_2	7592	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-S/ivf_S_2	7653	30795	building index: loading tuples	7168	7168	100000	44403	68701	0
m3r-S/ivf_S_2	7716	30795	building index: loading tuples	7168	7168	100000	75313	68701	0
m3r-S/ivf_S_2	7779	30795	building index: loading tuples	7168	7168	100000	75313	68701	0
m3r-S/ivf_S_2	7842	30795	building index: loading tuples	7168	7168	100000	75313	68701	0
m3r-S/ivf_S_2	7905	30795	building index: loading tuples	7168	7168	100000	75313	68701	0
m3r-S/ivf_S_2	7966	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-S/ivf_S_2	8025	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-S/ivf_S_2	8085	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-S/ivf_S_2	8145	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-S/ivf_S_2	8205	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-S/ivf_S_2	8267	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-S/ivf_S_2	8328	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-S/ivf_S_2	8388	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-S/ivf_S_2	8447	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-S/ivf_S_2	8506	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-S/ivf_S_2	8565	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-S/ivf_S_2	8624	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-S/ivf_S_2	8683	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-S/ivf_S_2	8742	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-S/ivf_S_2	8802	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-S/ivf_S_2	8863	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-S/ivf_S_2	8922	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-S/ivf_S_2	8982	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-S/ivf_S_2	9042	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-S/ivf_S_2	9102	30795	building index: loading tuples	7168	7168	100000	75328	68701	0
m3r-S/ivf_S_2	9163	30795	building index: loading tuples	7168	7168	100000	99444	68701	0
m3r-S/ivf_S_3	1	31710	building index: performing k-means	0	0	0	0	68701	0
m3r-S/ivf_S_3	60	31710	building index: performing k-means	0	0	0	0	68701	0
m3r-S/ivf_S_3	120	31710	building index: performing k-means	0	0	0	0	68701	0
m3r-S/ivf_S_3	181	31710	building index: performing k-means	0	0	0	0	68701	0
m3r-S/ivf_S_3	241	31710	building index: performing k-means	0	0	0	0	68701	0
m3r-S/ivf_S_3	303	31710	building index: performing k-means	0	0	0	0	68701	0
m3r-S/ivf_S_3	364	31710	building index: assigning tuples	7168	132	0	0	68701	0
m3r-S/ivf_S_3	427	31710	building index: assigning tuples	7168	2274	0	0	68701	0
m3r-S/ivf_S_3	489	31710	building index: assigning tuples	7168	4497	0	0	68701	0
m3r-S/ivf_S_3	550	31710	building index: assigning tuples	7168	6699	0	0	68701	0
m3r-S/ivf_S_3	612	31710	building index: assigning tuples	7168	7168	0	0	68701	0
m3r-S/ivf_S_3	672	31710	building index: loading tuples	7168	7168	100000	30827	68701	0
m3r-S/ivf_S_3	732	31710	building index: loading tuples	7168	7168	100000	30827	68701	0
m3r-S/ivf_S_3	791	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-S/ivf_S_3	850	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-S/ivf_S_3	910	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-S/ivf_S_3	971	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-S/ivf_S_3	1030	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-S/ivf_S_3	1090	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-S/ivf_S_3	1150	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-S/ivf_S_3	1210	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-S/ivf_S_3	1270	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-S/ivf_S_3	1331	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-S/ivf_S_3	1391	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-S/ivf_S_3	1451	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-S/ivf_S_3	1510	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-S/ivf_S_3	1570	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-S/ivf_S_3	1629	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-S/ivf_S_3	1689	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-S/ivf_S_3	1748	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-S/ivf_S_3	1808	31710	building index: loading tuples	7168	7168	100000	37059	68701	0
m3r-S/ivf_S_3	1868	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-S/ivf_S_3	1928	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-S/ivf_S_3	1988	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-S/ivf_S_3	2048	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-S/ivf_S_3	2107	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-S/ivf_S_3	2168	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-S/ivf_S_3	2227	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-S/ivf_S_3	2287	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-S/ivf_S_3	2348	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-S/ivf_S_3	2409	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-S/ivf_S_3	2469	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-S/ivf_S_3	2530	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-S/ivf_S_3	2591	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-S/ivf_S_3	2651	31710	building index: loading tuples	7168	7168	100000	37074	68701	0
m3r-S/ivf_S_3	2711	31710	building index: loading tuples	7168	7168	100000	87015	68701	0
m3r-M/ivf_M_1	1	31977	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_1	61	31977	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_1	122	31977	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_1	182	31977	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_1	243	31977	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_1	303	31977	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_1	363	31977	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_1	424	31977	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_1	484	31977	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_1	544	31977	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_1	612	31977	building index: assigning tuples	21440	1055	0	0	68707	0
m3r-M/ivf_M_1	707	31977	building index: assigning tuples	21440	3273	0	0	68707	0
m3r-M/ivf_M_1	782	31977	building index: assigning tuples	21440	5213	0	0	68707	0
m3r-M/ivf_M_1	845	31977	building index: assigning tuples	21440	6850	0	0	68707	0
m3r-M/ivf_M_1	908	31977	building index: assigning tuples	21440	8723	0	0	68707	0
m3r-M/ivf_M_1	1005	31977	building index: assigning tuples	21440	11135	0	0	68707	0
m3r-M/ivf_M_1	1085	31977	building index: assigning tuples	21440	13186	0	0	68707	0
m3r-M/ivf_M_1	1157	31977	building index: assigning tuples	21440	14990	0	0	68707	0
m3r-M/ivf_M_1	1242	31977	building index: assigning tuples	21440	17123	0	0	68707	0
m3r-M/ivf_M_1	1318	31977	building index: assigning tuples	21440	18945	0	0	68707	0
m3r-M/ivf_M_1	1382	31977	building index: assigning tuples	21440	20543	0	0	68707	0
m3r-M/ivf_M_1	1444	31977	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-M/ivf_M_1	1507	31977	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-M/ivf_M_1	1567	31977	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-M/ivf_M_1	1627	31977	building index: loading tuples	21440	21440	300000	41649	68707	0
m3r-M/ivf_M_1	1687	31977	building index: loading tuples	21440	21440	300000	69606	68707	0
m3r-M/ivf_M_1	1748	31977	building index: loading tuples	21440	21440	300000	97338	68707	0
m3r-M/ivf_M_1	1808	31977	building index: loading tuples	21440	21440	300000	131472	68707	0
m3r-M/ivf_M_1	1868	31977	building index: loading tuples	21440	21440	300000	131472	68707	0
m3r-M/ivf_M_1	1930	31977	building index: loading tuples	21440	21440	300000	142657	68707	0
m3r-M/ivf_M_1	1989	31977	building index: loading tuples	21440	21440	300000	158145	68707	0
m3r-M/ivf_M_1	2049	31977	building index: loading tuples	21440	21440	300000	158145	68707	0
m3r-M/ivf_M_1	2108	31977	building index: loading tuples	21440	21440	300000	158145	68707	0
m3r-M/ivf_M_1	2169	31977	building index: loading tuples	21440	21440	300000	158145	68707	0
m3r-M/ivf_M_1	2229	31977	building index: loading tuples	21440	21440	300000	182469	68707	0
m3r-M/ivf_M_1	2290	31977	building index: loading tuples	21440	21440	300000	210224	68707	0
m3r-M/ivf_M_1	2350	31977	building index: loading tuples	21440	21440	300000	239684	68707	0
m3r-M/ivf_M_1	2414	31977	building index: loading tuples	21440	21440	300000	261464	68707	0
m3r-M/ivf_M_1	2475	31977	building index: loading tuples	21440	21440	300000	297835	68707	0
m3r-M/ivf_M_2	1	32197	initializing	0	0	0	0	68707	0
m3r-M/ivf_M_2	60	32197	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_2	120	32197	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_2	179	32197	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_2	239	32197	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_2	299	32197	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_2	359	32197	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_2	419	32197	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_2	478	32197	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_2	538	32197	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_2	603	32197	building index: assigning tuples	21440	1456	0	0	68707	0
m3r-M/ivf_M_2	690	32197	building index: assigning tuples	21440	3171	0	0	68707	0
m3r-M/ivf_M_2	762	32197	building index: assigning tuples	21440	5187	0	0	68707	0
m3r-M/ivf_M_2	838	32197	building index: assigning tuples	21440	7085	0	0	68707	0
m3r-M/ivf_M_2	900	32197	building index: assigning tuples	21440	9132	0	0	68707	0
m3r-M/ivf_M_2	989	32197	building index: assigning tuples	21440	11142	0	0	68707	0
m3r-M/ivf_M_2	1069	32197	building index: assigning tuples	21440	13108	0	0	68707	0
m3r-M/ivf_M_2	1140	32197	building index: assigning tuples	21440	14801	0	0	68707	0
m3r-M/ivf_M_2	1200	32197	building index: assigning tuples	21440	16584	0	0	68707	0
m3r-M/ivf_M_2	1259	32197	building index: assigning tuples	21440	18165	0	0	68707	0
m3r-M/ivf_M_2	1317	32197	building index: assigning tuples	21440	19692	0	0	68707	0
m3r-M/ivf_M_2	1376	32197	building index: assigning tuples	21440	21265	0	0	68707	0
m3r-M/ivf_M_2	1436	32197	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-M/ivf_M_2	1497	32197	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-M/ivf_M_2	1557	32197	building index: loading tuples	21440	21440	300000	19521	68707	0
m3r-M/ivf_M_2	1617	32197	building index: loading tuples	21440	21440	300000	49455	68707	0
m3r-M/ivf_M_2	1676	32197	building index: loading tuples	21440	21440	300000	79671	68707	0
m3r-M/ivf_M_2	1737	32197	building index: loading tuples	21440	21440	300000	109684	68707	0
m3r-M/ivf_M_2	1797	32197	building index: loading tuples	21440	21440	300000	140615	68707	0
m3r-M/ivf_M_2	1857	32197	building index: loading tuples	21440	21440	300000	140615	68707	0
m3r-M/ivf_M_2	1916	32197	building index: loading tuples	21440	21440	300000	140615	68707	0
m3r-M/ivf_M_2	1975	32197	building index: loading tuples	21440	21440	300000	140615	68707	0
m3r-M/ivf_M_2	2034	32197	building index: loading tuples	21440	21440	300000	140615	68707	0
m3r-M/ivf_M_2	2094	32197	building index: loading tuples	21440	21440	300000	140615	68707	0
m3r-M/ivf_M_2	2154	32197	building index: loading tuples	21440	21440	300000	140615	68707	0
m3r-M/ivf_M_2	2214	32197	building index: loading tuples	21440	21440	300000	140615	68707	0
m3r-M/ivf_M_2	2274	32197	building index: loading tuples	21440	21440	300000	140630	68707	0
m3r-M/ivf_M_2	2334	32197	building index: loading tuples	21440	21440	300000	140630	68707	0
m3r-M/ivf_M_2	2394	32197	building index: loading tuples	21440	21440	300000	161030	68707	0
m3r-M/ivf_M_2	2459	32197	building index: loading tuples	21440	21440	300000	202467	68707	0
m3r-M/ivf_M_2	2521	32197	building index: loading tuples	21440	21440	300000	233393	68707	0
m3r-M/ivf_M_2	2581	32197	building index: loading tuples	21440	21440	300000	257008	68707	0
m3r-M/ivf_M_2	2642	32197	building index: loading tuples	21440	21440	300000	265841	68707	0
m3r-M/ivf_M_2	2704	32197	building index: loading tuples	21440	21440	300000	295257	68707	0
m3r-M/ivf_M_2	2764	32197	building index: loading tuples	21440	21440	300000	299999	68707	0
m3r-M/ivf_M_3	1	32446	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_3	60	32446	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_3	121	32446	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_3	182	32446	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_3	242	32446	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_3	303	32446	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_3	363	32446	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_3	424	32446	building index: performing k-means	0	0	0	0	68707	0
m3r-M/ivf_M_3	484	32446	building index: assigning tuples	21440	359	0	0	68707	0
m3r-M/ivf_M_3	545	32446	building index: assigning tuples	21440	1783	0	0	68707	0
m3r-M/ivf_M_3	614	32446	building index: assigning tuples	21440	3103	0	0	68707	0
m3r-M/ivf_M_3	683	32446	building index: assigning tuples	21440	4709	0	0	68707	0
m3r-M/ivf_M_3	754	32446	building index: assigning tuples	21440	6527	0	0	68707	0
m3r-M/ivf_M_3	826	32446	building index: assigning tuples	21440	8331	0	0	68707	0
m3r-M/ivf_M_3	893	32446	building index: assigning tuples	21440	10070	0	0	68707	0
m3r-M/ivf_M_3	955	32446	building index: assigning tuples	21440	11612	0	0	68707	0
m3r-M/ivf_M_3	1016	32446	building index: assigning tuples	21440	13191	0	0	68707	0
m3r-M/ivf_M_3	1078	32446	building index: assigning tuples	21440	14801	0	0	68707	0
m3r-M/ivf_M_3	1140	32446	building index: assigning tuples	21440	16389	0	0	68707	0
m3r-M/ivf_M_3	1202	32446	building index: assigning tuples	21440	17981	0	0	68707	0
m3r-M/ivf_M_3	1264	32446	building index: assigning tuples	21440	19617	0	0	68707	0
m3r-M/ivf_M_3	1326	32446	building index: assigning tuples	21440	21204	0	0	68707	0
m3r-M/ivf_M_3	1389	32446	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-M/ivf_M_3	1452	32446	building index: assigning tuples	21440	21440	0	0	68707	0
m3r-M/ivf_M_3	1512	32446	building index: loading tuples	21440	21440	300000	17194	68707	0
m3r-M/ivf_M_3	1571	32446	building index: loading tuples	21440	21440	300000	56344	68707	0
m3r-M/ivf_M_3	1631	32446	building index: loading tuples	21440	21440	300000	81050	68707	0
m3r-M/ivf_M_3	1691	32446	building index: loading tuples	21440	21440	300000	87897	68707	0
m3r-M/ivf_M_3	1751	32446	building index: loading tuples	21440	21440	300000	87912	68707	0
m3r-M/ivf_M_3	1812	32446	building index: loading tuples	21440	21440	300000	87927	68707	0
m3r-M/ivf_M_3	1873	32446	building index: loading tuples	21440	21440	300000	87927	68707	0
m3r-M/ivf_M_3	1932	32446	building index: loading tuples	21440	21440	300000	125550	68707	0
m3r-M/ivf_M_3	1993	32446	building index: loading tuples	21440	21440	300000	153357	68707	0
m3r-M/ivf_M_3	2053	32446	building index: loading tuples	21440	21440	300000	170470	68707	0
m3r-M/ivf_M_3	2112	32446	building index: loading tuples	21440	21440	300000	204423	68707	0
m3r-M/ivf_M_3	2172	32446	building index: loading tuples	21440	21440	300000	242504	68707	0
m3r-M/ivf_M_3	2232	32446	building index: loading tuples	21440	21440	300000	242504	68707	0
m3r-M/ivf_M_3	2291	32446	building index: loading tuples	21440	21440	300000	242504	68707	0
m3r-M/ivf_M_3	2351	32446	building index: loading tuples	21440	21440	300000	242504	68707	0
m3r-M/ivf_M_3	2410	32446	building index: loading tuples	21440	21440	300000	242504	68707	0
m3r-M/ivf_M_3	2469	32446	building index: loading tuples	21440	21440	300000	242504	68707	0
m3r-M/ivf_M_3	2527	32446	building index: loading tuples	21440	21440	300000	242504	68707	0
m3r-M/ivf_M_3	2586	32446	building index: loading tuples	21440	21440	300000	242519	68707	0
m3r-M/ivf_M_3	2645	32446	building index: loading tuples	21440	21440	300000	282761	68707	0
m3r-M/ivf_M_3	2705	32446	building index: loading tuples	21440	21440	300000	300000	68707	0
m3r-L/ivf_L_1	1	32743	initializing	0	0	0	0	68713	0
m3r-L/ivf_L_1	85	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	147	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	208	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	270	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	331	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	393	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	454	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	515	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	577	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	639	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	700	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	761	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	822	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	884	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	946	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	1007	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	1069	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	1130	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	1191	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	1253	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	1314	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	1376	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	1437	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	1498	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	1560	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	1620	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	1682	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	1742	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	1802	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	1862	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	1923	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	1983	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	2044	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	2104	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	2164	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	2224	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	2283	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	2344	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	2403	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	2463	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	2522	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	2582	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	2642	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	2702	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	2764	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	2824	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	2884	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	2944	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	3003	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	3063	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	3124	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	3185	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	3246	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	3307	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	3366	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	3427	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	3489	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	3548	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	3609	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	3669	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	3729	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	3789	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	3850	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	3912	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	3972	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	4033	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	4093	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	4153	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	4213	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	4273	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	4335	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	4395	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	4455	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	4516	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	4576	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	4636	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	4696	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	4757	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	4820	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	4880	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	4941	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	5001	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	5061	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	5121	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	5182	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	5243	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	5304	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	5364	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	5424	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	5484	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	5544	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	5604	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	5664	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	5724	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	5783	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	5843	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	5903	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	5963	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	6023	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	6083	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	6143	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	6203	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	6264	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	6324	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	6384	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	6444	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	6503	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	6562	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	6621	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	6680	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	6739	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	6798	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	6857	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	6916	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	6976	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	7035	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	7095	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	7156	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	7216	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	7276	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	7335	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	7394	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	7454	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	7513	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	7573	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	7632	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	7691	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	7750	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	7810	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	7870	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	7929	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	7988	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	8047	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	8106	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	8166	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	8226	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	8287	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	8346	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	8406	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	8466	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	8526	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	8587	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	8647	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	8707	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	8766	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	8827	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	8888	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	8947	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	9007	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	9067	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	9127	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	9186	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	9245	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	9304	32743	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_1	9363	32743	building index: assigning tuples	71488	25	0	0	68713	0
m3r-L/ivf_L_1	9430	32743	building index: assigning tuples	71488	1414	0	0	68713	0
m3r-L/ivf_L_1	9504	32743	building index: assigning tuples	71488	2862	0	0	68713	0
m3r-L/ivf_L_1	9590	32743	building index: assigning tuples	71488	4544	0	0	68713	0
m3r-L/ivf_L_1	9658	32743	building index: assigning tuples	71488	5732	0	0	68713	0
m3r-L/ivf_L_1	9728	32743	building index: assigning tuples	71488	7063	0	0	68713	0
m3r-L/ivf_L_1	9802	32743	building index: assigning tuples	71488	8686	0	0	68713	0
m3r-L/ivf_L_1	9881	32743	building index: assigning tuples	71488	10069	0	0	68713	0
m3r-L/ivf_L_1	9945	32743	building index: assigning tuples	71488	11290	0	0	68713	0
m3r-L/ivf_L_1	10010	32743	building index: assigning tuples	71488	12418	0	0	68713	0
m3r-L/ivf_L_1	10070	32743	building index: assigning tuples	71488	12479	0	0	68713	0
m3r-L/ivf_L_1	10131	32743	building index: assigning tuples	71488	13311	0	0	68713	0
m3r-L/ivf_L_1	10191	32743	building index: assigning tuples	71488	14316	0	0	68713	0
m3r-L/ivf_L_1	10267	32743	building index: assigning tuples	71488	15776	0	0	68713	0
m3r-L/ivf_L_1	10336	32743	building index: assigning tuples	71488	17279	0	0	68713	0
m3r-L/ivf_L_1	10411	32743	building index: assigning tuples	71488	18558	0	0	68713	0
m3r-L/ivf_L_1	10483	32743	building index: assigning tuples	71488	19966	0	0	68713	0
m3r-L/ivf_L_1	10552	32743	building index: assigning tuples	71488	21565	0	0	68713	0
m3r-L/ivf_L_1	10639	32743	building index: assigning tuples	71488	23083	0	0	68713	0
m3r-L/ivf_L_1	10705	32743	building index: assigning tuples	71488	24595	0	0	68713	0
m3r-L/ivf_L_1	10785	32743	building index: assigning tuples	71488	26276	0	0	68713	0
m3r-L/ivf_L_1	10865	32743	building index: assigning tuples	71488	27501	0	0	68713	0
m3r-L/ivf_L_1	10931	32743	building index: assigning tuples	71488	28911	0	0	68713	0
m3r-L/ivf_L_1	11009	32743	building index: assigning tuples	71488	30610	0	0	68713	0
m3r-L/ivf_L_1	11086	32743	building index: assigning tuples	71488	31985	0	0	68713	0
m3r-L/ivf_L_1	11165	32743	building index: assigning tuples	71488	33433	0	0	68713	0
m3r-L/ivf_L_1	11228	32743	building index: assigning tuples	71488	33623	0	0	68713	0
m3r-L/ivf_L_1	11299	32743	building index: assigning tuples	71488	34283	0	0	68713	0
m3r-L/ivf_L_1	11370	32743	building index: assigning tuples	71488	35170	0	0	68713	0
m3r-L/ivf_L_1	11430	32743	building index: assigning tuples	71488	36293	0	0	68713	0
m3r-L/ivf_L_1	11491	32743	building index: assigning tuples	71488	36293	0	0	68713	0
m3r-L/ivf_L_1	11550	32743	building index: assigning tuples	71488	36589	0	0	68713	0
m3r-L/ivf_L_1	11615	32743	building index: assigning tuples	71488	37801	0	0	68713	0
m3r-L/ivf_L_1	11684	32743	building index: assigning tuples	71488	39236	0	0	68713	0
m3r-L/ivf_L_1	11749	32743	building index: assigning tuples	71488	40501	0	0	68713	0
m3r-L/ivf_L_1	11826	32743	building index: assigning tuples	71488	42112	0	0	68713	0
m3r-L/ivf_L_1	11895	32743	building index: assigning tuples	71488	43458	0	0	68713	0
m3r-L/ivf_L_1	11967	32743	building index: assigning tuples	71488	45192	0	0	68713	0
m3r-L/ivf_L_1	12060	32743	building index: assigning tuples	71488	46748	0	0	68713	0
m3r-L/ivf_L_1	12128	32743	building index: assigning tuples	71488	48024	0	0	68713	0
m3r-L/ivf_L_1	12202	32743	building index: assigning tuples	71488	49334	0	0	68713	0
m3r-L/ivf_L_1	12268	32743	building index: assigning tuples	71488	50843	0	0	68713	0
m3r-L/ivf_L_1	12338	32743	building index: assigning tuples	71488	52601	0	0	68713	0
m3r-L/ivf_L_1	12422	32743	building index: assigning tuples	71488	53883	0	0	68713	0
m3r-L/ivf_L_1	12492	32743	building index: assigning tuples	71488	55362	0	0	68713	0
m3r-L/ivf_L_1	12562	32743	building index: assigning tuples	71488	56863	0	0	68713	0
m3r-L/ivf_L_1	12630	32743	building index: assigning tuples	71488	58281	0	0	68713	0
m3r-L/ivf_L_1	12704	32743	building index: assigning tuples	71488	59634	0	0	68713	0
m3r-L/ivf_L_1	12794	32743	building index: assigning tuples	71488	61846	0	0	68713	0
m3r-L/ivf_L_1	12892	32743	building index: assigning tuples	71488	63524	0	0	68713	0
m3r-L/ivf_L_1	12963	32743	building index: assigning tuples	71488	65294	0	0	68713	0
m3r-L/ivf_L_1	13044	32743	building index: assigning tuples	71488	65627	0	0	68713	0
m3r-L/ivf_L_1	13105	32743	building index: assigning tuples	71488	65646	0	0	68713	0
m3r-L/ivf_L_1	13167	32743	building index: assigning tuples	71488	67196	0	0	68713	0
m3r-L/ivf_L_1	13246	32743	building index: assigning tuples	71488	69067	0	0	68713	0
m3r-L/ivf_L_1	13343	32743	building index: assigning tuples	71488	70750	0	0	68713	0
m3r-L/ivf_L_1	13419	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_1	13483	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_1	13544	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_1	13604	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_1	13663	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_1	13727	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_1	13787	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_1	13849	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_1	13909	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_1	13969	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_1	14029	32743	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_1	14089	32743	building index: loading tuples	71488	71488	1000000	37299	68713	0
m3r-L/ivf_L_1	14149	32743	building index: loading tuples	71488	71488	1000000	37299	68713	0
m3r-L/ivf_L_1	14208	32743	building index: loading tuples	71488	71488	1000000	37299	68713	0
m3r-L/ivf_L_1	14267	32743	building index: loading tuples	71488	71488	1000000	37299	68713	0
m3r-L/ivf_L_1	14326	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-L/ivf_L_1	14385	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-L/ivf_L_1	14444	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-L/ivf_L_1	14503	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-L/ivf_L_1	14562	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-L/ivf_L_1	14622	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-L/ivf_L_1	14681	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-L/ivf_L_1	14741	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-L/ivf_L_1	14800	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-L/ivf_L_1	14858	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-L/ivf_L_1	14917	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-L/ivf_L_1	14977	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-L/ivf_L_1	15037	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-L/ivf_L_1	15098	32743	building index: loading tuples	71488	71488	1000000	37314	68713	0
m3r-L/ivf_L_1	15159	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-L/ivf_L_1	15222	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-L/ivf_L_1	15284	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-L/ivf_L_1	15346	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-L/ivf_L_1	15408	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-L/ivf_L_1	15470	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-L/ivf_L_1	15532	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-L/ivf_L_1	15594	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-L/ivf_L_1	15656	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-L/ivf_L_1	15718	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-L/ivf_L_1	15781	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-L/ivf_L_1	15842	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-L/ivf_L_1	15906	32743	building index: loading tuples	71488	71488	1000000	68259	68713	0
m3r-L/ivf_L_1	15968	32743	building index: loading tuples	71488	71488	1000000	68274	68713	0
m3r-L/ivf_L_1	16030	32743	building index: loading tuples	71488	71488	1000000	99164	68713	0
m3r-L/ivf_L_1	16092	32743	building index: loading tuples	71488	71488	1000000	130106	68713	0
m3r-L/ivf_L_1	16155	32743	building index: loading tuples	71488	71488	1000000	153581	68713	0
m3r-L/ivf_L_1	16216	32743	building index: loading tuples	71488	71488	1000000	161052	68713	0
m3r-L/ivf_L_1	16278	32743	building index: loading tuples	71488	71488	1000000	197600	68713	0
m3r-L/ivf_L_1	16341	32743	building index: loading tuples	71488	71488	1000000	222878	68713	0
m3r-L/ivf_L_1	16405	32743	building index: loading tuples	71488	71488	1000000	244454	68713	0
m3r-L/ivf_L_1	16467	32743	building index: loading tuples	71488	71488	1000000	268723	68713	0
m3r-L/ivf_L_1	16529	32743	building index: loading tuples	71488	71488	1000000	312396	68713	0
m3r-L/ivf_L_1	16593	32743	building index: loading tuples	71488	71488	1000000	342350	68713	0
m3r-L/ivf_L_1	16662	32743	building index: loading tuples	71488	71488	1000000	362728	68713	0
m3r-L/ivf_L_1	16725	32743	building index: loading tuples	71488	71488	1000000	377190	68713	0
m3r-L/ivf_L_1	16788	32743	building index: loading tuples	71488	71488	1000000	408426	68713	0
m3r-L/ivf_L_1	16859	32743	building index: loading tuples	71488	71488	1000000	439353	68713	0
m3r-L/ivf_L_1	16920	32743	building index: loading tuples	71488	71488	1000000	456921	68713	0
m3r-L/ivf_L_1	16982	32743	building index: loading tuples	71488	71488	1000000	486139	68713	0
m3r-L/ivf_L_1	17044	32743	building index: loading tuples	71488	71488	1000000	497491	68713	0
m3r-L/ivf_L_1	17114	32743	building index: loading tuples	71488	71488	1000000	501208	68713	0
m3r-L/ivf_L_1	17177	32743	building index: loading tuples	71488	71488	1000000	501208	68713	0
m3r-L/ivf_L_1	17239	32743	building index: loading tuples	71488	71488	1000000	501598	68713	0
m3r-L/ivf_L_1	17304	32743	building index: loading tuples	71488	71488	1000000	546318	68713	0
m3r-L/ivf_L_1	17366	32743	building index: loading tuples	71488	71488	1000000	569304	68713	0
m3r-L/ivf_L_1	17429	32743	building index: loading tuples	71488	71488	1000000	593975	68713	0
m3r-L/ivf_L_1	17492	32743	building index: loading tuples	71488	71488	1000000	608470	68713	0
m3r-L/ivf_L_1	17555	32743	building index: loading tuples	71488	71488	1000000	650004	68713	0
m3r-L/ivf_L_1	17669	32743	building index: loading tuples	71488	71488	1000000	655842	68713	0
m3r-L/ivf_L_1	17779	32743	building index: loading tuples	71488	71488	1000000	667434	68713	0
m3r-L/ivf_L_1	17842	32743	building index: loading tuples	71488	71488	1000000	707402	68713	0
m3r-L/ivf_L_1	17905	32743	building index: loading tuples	71488	71488	1000000	717680	68713	0
m3r-L/ivf_L_1	17970	32743	building index: loading tuples	71488	71488	1000000	717680	68713	0
m3r-L/ivf_L_1	18032	32743	building index: loading tuples	71488	71488	1000000	717680	68713	0
m3r-L/ivf_L_1	18095	32743	building index: loading tuples	71488	71488	1000000	717695	68713	0
m3r-L/ivf_L_1	18157	32743	building index: loading tuples	71488	71488	1000000	748604	68713	0
m3r-L/ivf_L_1	18219	32743	building index: loading tuples	71488	71488	1000000	776129	68713	0
m3r-L/ivf_L_1	18282	32743	building index: loading tuples	71488	71488	1000000	779519	68713	0
m3r-L/ivf_L_1	18354	32743	building index: loading tuples	71488	71488	1000000	779519	68713	0
m3r-L/ivf_L_1	18417	32743	building index: loading tuples	71488	71488	1000000	779522	68713	0
m3r-L/ivf_L_1	18479	32743	building index: loading tuples	71488	71488	1000000	781979	68713	0
m3r-L/ivf_L_1	18541	32743	building index: loading tuples	71488	71488	1000000	820287	68713	0
m3r-L/ivf_L_1	18603	32743	building index: loading tuples	71488	71488	1000000	841387	68713	0
m3r-L/ivf_L_1	18665	32743	building index: loading tuples	71488	71488	1000000	871045	68713	0
m3r-L/ivf_L_1	18728	32743	building index: loading tuples	71488	71488	1000000	872306	68713	0
m3r-L/ivf_L_1	18813	32743	building index: loading tuples	71488	71488	1000000	872306	68713	0
m3r-L/ivf_L_1	18876	32743	building index: loading tuples	71488	71488	1000000	872321	68713	0
m3r-L/ivf_L_1	18938	32743	building index: loading tuples	71488	71488	1000000	887219	68713	0
m3r-L/ivf_L_1	19001	32743	building index: loading tuples	71488	71488	1000000	919623	68713	0
m3r-L/ivf_L_1	19064	32743	building index: loading tuples	71488	71488	1000000	934142	68713	0
m3r-L/ivf_L_1	19126	32743	building index: loading tuples	71488	71488	1000000	934142	68713	0
m3r-L/ivf_L_1	19187	32743	building index: loading tuples	71488	71488	1000000	934142	68713	0
m3r-L/ivf_L_1	19249	32743	building index: loading tuples	71488	71488	1000000	934157	68713	0
m3r-L/ivf_L_1	19310	32743	building index: loading tuples	71488	71488	1000000	942993	68713	0
m3r-L/ivf_L_1	19372	32743	building index: loading tuples	71488	71488	1000000	987869	68713	0
m3r-L/ivf_L_1	19443	32743	building index: loading tuples	71488	71488	1000000	999999	68713	0
m3r-L/ivf_L_1	19509	32743	building index: loading tuples	71488	71488	1000000	1000000	68713	0
m3r-L/ivf_L_2	1	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	63	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	125	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	186	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	248	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	309	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	370	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	430	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	491	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	551	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	611	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	672	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	732	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	792	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	852	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	912	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	972	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	1033	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	1094	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	1155	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	1216	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	1277	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	1339	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	1401	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	1463	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	1523	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	1585	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	1646	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	1707	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	1767	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	1829	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	1891	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	1952	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	2013	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	2075	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	2137	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	2199	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	2259	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	2321	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	2381	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	2442	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	2502	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	2562	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	2623	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	2683	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	2744	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	2804	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	2864	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	2924	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	2984	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	3043	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	3103	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	3164	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	3224	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	3284	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	3344	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	3404	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	3464	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	3524	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	3584	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	3644	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	3705	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	3766	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	3827	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	3888	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	3948	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	4008	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	4069	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	4128	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	4188	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	4248	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	4308	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	4368	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	4429	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	4491	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	4552	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	4613	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	4675	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	4736	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	4797	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	4859	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	4920	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	4982	1929	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_2	5045	1929	building index: assigning tuples	71488	95	0	0	68713	0
m3r-L/ivf_L_2	5110	1929	building index: assigning tuples	71488	821	0	0	68713	0
m3r-L/ivf_L_2	5172	1929	building index: assigning tuples	71488	1544	0	0	68713	0
m3r-L/ivf_L_2	5243	1929	building index: assigning tuples	71488	2193	0	0	68713	0
m3r-L/ivf_L_2	5306	1929	building index: assigning tuples	71488	2839	0	0	68713	0
m3r-L/ivf_L_2	5368	1929	building index: assigning tuples	71488	3525	0	0	68713	0
m3r-L/ivf_L_2	5430	1929	building index: assigning tuples	71488	4556	0	0	68713	0
m3r-L/ivf_L_2	5506	1929	building index: assigning tuples	71488	5856	0	0	68713	0
m3r-L/ivf_L_2	5595	1929	building index: assigning tuples	71488	7371	0	0	68713	0
m3r-L/ivf_L_2	5672	1929	building index: assigning tuples	71488	8506	0	0	68713	0
m3r-L/ivf_L_2	5753	1929	building index: assigning tuples	71488	9831	0	0	68713	0
m3r-L/ivf_L_2	5825	1929	building index: assigning tuples	71488	11037	0	0	68713	0
m3r-L/ivf_L_2	5921	1929	building index: assigning tuples	71488	12453	0	0	68713	0
m3r-L/ivf_L_2	5997	1929	building index: assigning tuples	71488	13684	0	0	68713	0
m3r-L/ivf_L_2	6082	1929	building index: assigning tuples	71488	15488	0	0	68713	0
m3r-L/ivf_L_2	6190	1929	building index: assigning tuples	71488	16988	0	0	68713	0
m3r-L/ivf_L_2	6276	1929	building index: assigning tuples	71488	18460	0	0	68713	0
m3r-L/ivf_L_2	6367	1929	building index: assigning tuples	71488	19834	0	0	68713	0
m3r-L/ivf_L_2	6451	1929	building index: assigning tuples	71488	21254	0	0	68713	0
m3r-L/ivf_L_2	6528	1929	building index: assigning tuples	71488	22472	0	0	68713	0
m3r-L/ivf_L_2	6608	1929	building index: assigning tuples	71488	23787	0	0	68713	0
m3r-L/ivf_L_2	6678	1929	building index: assigning tuples	71488	24897	0	0	68713	0
m3r-L/ivf_L_2	6754	1929	building index: assigning tuples	71488	26022	0	0	68713	0
m3r-L/ivf_L_2	6846	1929	building index: assigning tuples	71488	27402	0	0	68713	0
m3r-L/ivf_L_2	6926	1929	building index: assigning tuples	71488	28466	0	0	68713	0
m3r-L/ivf_L_2	6997	1929	building index: assigning tuples	71488	29306	0	0	68713	0
m3r-L/ivf_L_2	7065	1929	building index: assigning tuples	71488	30412	0	0	68713	0
m3r-L/ivf_L_2	7148	1929	building index: assigning tuples	71488	31652	0	0	68713	0
m3r-L/ivf_L_2	7230	1929	building index: assigning tuples	71488	32946	0	0	68713	0
m3r-L/ivf_L_2	7307	1929	building index: assigning tuples	71488	34273	0	0	68713	0
m3r-L/ivf_L_2	7381	1929	building index: assigning tuples	71488	35479	0	0	68713	0
m3r-L/ivf_L_2	7466	1929	building index: assigning tuples	71488	35479	0	0	68713	0
m3r-L/ivf_L_2	7539	1929	building index: assigning tuples	71488	37015	0	0	68713	0
m3r-L/ivf_L_2	7633	1929	building index: assigning tuples	71488	38398	0	0	68713	0
m3r-L/ivf_L_2	7711	1929	building index: assigning tuples	71488	39749	0	0	68713	0
m3r-L/ivf_L_2	7788	1929	building index: assigning tuples	71488	40905	0	0	68713	0
m3r-L/ivf_L_2	7858	1929	building index: assigning tuples	71488	41903	0	0	68713	0
m3r-L/ivf_L_2	7925	1929	building index: assigning tuples	71488	43015	0	0	68713	0
m3r-L/ivf_L_2	7996	1929	building index: assigning tuples	71488	43920	0	0	68713	0
m3r-L/ivf_L_2	8076	1929	building index: assigning tuples	71488	45086	0	0	68713	0
m3r-L/ivf_L_2	8166	1929	building index: assigning tuples	71488	46633	0	0	68713	0
m3r-L/ivf_L_2	8251	1929	building index: assigning tuples	71488	48093	0	0	68713	0
m3r-L/ivf_L_2	8336	1929	building index: assigning tuples	71488	49340	0	0	68713	0
m3r-L/ivf_L_2	8420	1929	building index: assigning tuples	71488	50614	0	0	68713	0
m3r-L/ivf_L_2	8485	1929	building index: assigning tuples	71488	51608	0	0	68713	0
m3r-L/ivf_L_2	8566	1929	building index: assigning tuples	71488	52792	0	0	68713	0
m3r-L/ivf_L_2	8646	1929	building index: assigning tuples	71488	54032	0	0	68713	0
m3r-L/ivf_L_2	8718	1929	building index: assigning tuples	71488	55135	0	0	68713	0
m3r-L/ivf_L_2	8789	1929	building index: assigning tuples	71488	56469	0	0	68713	0
m3r-L/ivf_L_2	8868	1929	building index: assigning tuples	71488	57749	0	0	68713	0
m3r-L/ivf_L_2	8963	1929	building index: assigning tuples	71488	59274	0	0	68713	0
m3r-L/ivf_L_2	9057	1929	building index: assigning tuples	71488	60401	0	0	68713	0
m3r-L/ivf_L_2	9139	1929	building index: assigning tuples	71488	61743	0	0	68713	0
m3r-L/ivf_L_2	9241	1929	building index: assigning tuples	71488	63368	0	0	68713	0
m3r-L/ivf_L_2	9320	1929	building index: assigning tuples	71488	64355	0	0	68713	0
m3r-L/ivf_L_2	9397	1929	building index: assigning tuples	71488	65819	0	0	68713	0
m3r-L/ivf_L_2	9499	1929	building index: assigning tuples	71488	67253	0	0	68713	0
m3r-L/ivf_L_2	9580	1929	building index: assigning tuples	71488	68386	0	0	68713	0
m3r-L/ivf_L_2	9690	1929	building index: assigning tuples	71488	68411	0	0	68713	0
m3r-L/ivf_L_2	9768	1929	building index: assigning tuples	71488	70079	0	0	68713	0
m3r-L/ivf_L_2	9830	1929	building index: assigning tuples	71488	70462	0	0	68713	0
m3r-L/ivf_L_2	9892	1929	building index: assigning tuples	71488	71203	0	0	68713	0
m3r-L/ivf_L_2	9953	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_2	10018	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_2	10084	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_2	10146	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_2	10209	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_2	10272	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_2	10334	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_2	10396	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_2	10458	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_2	10519	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_2	10580	1929	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_2	10642	1929	building index: loading tuples	71488	71488	1000000	38082	68713	0
m3r-L/ivf_L_2	10703	1929	building index: loading tuples	71488	71488	1000000	57519	68713	0
m3r-L/ivf_L_2	10764	1929	building index: loading tuples	71488	71488	1000000	57519	68713	0
m3r-L/ivf_L_2	10825	1929	building index: loading tuples	71488	71488	1000000	57519	68713	0
m3r-L/ivf_L_2	10886	1929	building index: loading tuples	71488	71488	1000000	57519	68713	0
m3r-L/ivf_L_2	10946	1929	building index: loading tuples	71488	71488	1000000	57519	68713	0
m3r-L/ivf_L_2	11006	1929	building index: loading tuples	71488	71488	1000000	57534	68713	0
m3r-L/ivf_L_2	11067	1929	building index: loading tuples	71488	71488	1000000	57534	68713	0
m3r-L/ivf_L_2	11130	1929	building index: loading tuples	71488	71488	1000000	57534	68713	0
m3r-L/ivf_L_2	11192	1929	building index: loading tuples	71488	71488	1000000	57534	68713	0
m3r-L/ivf_L_2	11253	1929	building index: loading tuples	71488	71488	1000000	57534	68713	0
m3r-L/ivf_L_2	11315	1929	building index: loading tuples	71488	71488	1000000	79172	68713	0
m3r-L/ivf_L_2	11377	1929	building index: loading tuples	71488	71488	1000000	88443	68713	0
m3r-L/ivf_L_2	11440	1929	building index: loading tuples	71488	71488	1000000	88443	68713	0
m3r-L/ivf_L_2	11503	1929	building index: loading tuples	71488	71488	1000000	88443	68713	0
m3r-L/ivf_L_2	11565	1929	building index: loading tuples	71488	71488	1000000	88464	68713	0
m3r-L/ivf_L_2	11626	1929	building index: loading tuples	71488	71488	1000000	88464	68713	0
m3r-L/ivf_L_2	11688	1929	building index: loading tuples	71488	71488	1000000	88464	68713	0
m3r-L/ivf_L_2	11750	1929	building index: loading tuples	71488	71488	1000000	101114	68713	0
m3r-L/ivf_L_2	11812	1929	building index: loading tuples	71488	71488	1000000	135353	68713	0
m3r-L/ivf_L_2	11874	1929	building index: loading tuples	71488	71488	1000000	160242	68713	0
m3r-L/ivf_L_2	11936	1929	building index: loading tuples	71488	71488	1000000	181242	68713	0
m3r-L/ivf_L_2	11998	1929	building index: loading tuples	71488	71488	1000000	181242	68713	0
m3r-L/ivf_L_2	12059	1929	building index: loading tuples	71488	71488	1000000	181242	68713	0
m3r-L/ivf_L_2	12120	1929	building index: loading tuples	71488	71488	1000000	181242	68713	0
m3r-L/ivf_L_2	12181	1929	building index: loading tuples	71488	71488	1000000	181242	68713	0
m3r-L/ivf_L_2	12243	1929	building index: loading tuples	71488	71488	1000000	181257	68713	0
m3r-L/ivf_L_2	12304	1929	building index: loading tuples	71488	71488	1000000	181257	68713	0
m3r-L/ivf_L_2	12365	1929	building index: loading tuples	71488	71488	1000000	181257	68713	0
m3r-L/ivf_L_2	12426	1929	building index: loading tuples	71488	71488	1000000	181257	68713	0
m3r-L/ivf_L_2	12487	1929	building index: loading tuples	71488	71488	1000000	181257	68713	0
m3r-L/ivf_L_2	12548	1929	building index: loading tuples	71488	71488	1000000	210243	68713	0
m3r-L/ivf_L_2	12609	1929	building index: loading tuples	71488	71488	1000000	223640	68713	0
m3r-L/ivf_L_2	12669	1929	building index: loading tuples	71488	71488	1000000	266923	68713	0
m3r-L/ivf_L_2	12730	1929	building index: loading tuples	71488	71488	1000000	274012	68713	0
m3r-L/ivf_L_2	12793	1929	building index: loading tuples	71488	71488	1000000	318798	68713	0
m3r-L/ivf_L_2	12854	1929	building index: loading tuples	71488	71488	1000000	347905	68713	0
m3r-L/ivf_L_2	12915	1929	building index: loading tuples	71488	71488	1000000	373857	68713	0
m3r-L/ivf_L_2	12977	1929	building index: loading tuples	71488	71488	1000000	398625	68713	0
m3r-L/ivf_L_2	13038	1929	building index: loading tuples	71488	71488	1000000	428625	68713	0
m3r-L/ivf_L_2	13100	1929	building index: loading tuples	71488	71488	1000000	459553	68713	0
m3r-L/ivf_L_2	13162	1929	building index: loading tuples	71488	71488	1000000	471368	68713	0
m3r-L/ivf_L_2	13223	1929	building index: loading tuples	71488	71488	1000000	495461	68713	0
m3r-L/ivf_L_2	13285	1929	building index: loading tuples	71488	71488	1000000	513768	68713	0
m3r-L/ivf_L_2	13347	1929	building index: loading tuples	71488	71488	1000000	521402	68713	0
m3r-L/ivf_L_2	13428	1929	building index: loading tuples	71488	71488	1000000	521402	68713	0
m3r-L/ivf_L_2	13489	1929	building index: loading tuples	71488	71488	1000000	521402	68713	0
m3r-L/ivf_L_2	13551	1929	building index: loading tuples	71488	71488	1000000	521402	68713	0
m3r-L/ivf_L_2	13612	1929	building index: loading tuples	71488	71488	1000000	521417	68713	0
m3r-L/ivf_L_2	13673	1929	building index: loading tuples	71488	71488	1000000	536274	68713	0
m3r-L/ivf_L_2	13737	1929	building index: loading tuples	71488	71488	1000000	570279	68713	0
m3r-L/ivf_L_2	13799	1929	building index: loading tuples	71488	71488	1000000	591477	68713	0
m3r-L/ivf_L_2	13860	1929	building index: loading tuples	71488	71488	1000000	614183	68713	0
m3r-L/ivf_L_2	13923	1929	building index: loading tuples	71488	71488	1000000	632527	68713	0
m3r-L/ivf_L_2	13987	1929	building index: loading tuples	71488	71488	1000000	670003	68713	0
m3r-L/ivf_L_2	14048	1929	building index: loading tuples	71488	71488	1000000	693084	68713	0
m3r-L/ivf_L_2	14109	1929	building index: loading tuples	71488	71488	1000000	706953	68713	0
m3r-L/ivf_L_2	14171	1929	building index: loading tuples	71488	71488	1000000	706953	68713	0
m3r-L/ivf_L_2	14234	1929	building index: loading tuples	71488	71488	1000000	706953	68713	0
m3r-L/ivf_L_2	14296	1929	building index: loading tuples	71488	71488	1000000	706953	68713	0
m3r-L/ivf_L_2	14357	1929	building index: loading tuples	71488	71488	1000000	706953	68713	0
m3r-L/ivf_L_2	14418	1929	building index: loading tuples	71488	71488	1000000	706968	68713	0
m3r-L/ivf_L_2	14479	1929	building index: loading tuples	71488	71488	1000000	737876	68713	0
m3r-L/ivf_L_2	14541	1929	building index: loading tuples	71488	71488	1000000	768816	68713	0
m3r-L/ivf_L_2	14607	1929	building index: loading tuples	71488	71488	1000000	799735	68713	0
m3r-L/ivf_L_2	14670	1929	building index: loading tuples	71488	71488	1000000	820574	68713	0
m3r-L/ivf_L_2	14732	1929	building index: loading tuples	71488	71488	1000000	860481	68713	0
m3r-L/ivf_L_2	14795	1929	building index: loading tuples	71488	71488	1000000	884210	68713	0
m3r-L/ivf_L_2	14855	1929	building index: loading tuples	71488	71488	1000000	908303	68713	0
m3r-L/ivf_L_2	14917	1929	building index: loading tuples	71488	71488	1000000	914604	68713	0
m3r-L/ivf_L_2	14979	1929	building index: loading tuples	71488	71488	1000000	914604	68713	0
m3r-L/ivf_L_2	15040	1929	building index: loading tuples	71488	71488	1000000	914604	68713	0
m3r-L/ivf_L_2	15102	1929	building index: loading tuples	71488	71488	1000000	924264	68713	0
m3r-L/ivf_L_2	15163	1929	building index: loading tuples	71488	71488	1000000	954351	68713	0
m3r-L/ivf_L_2	15225	1929	building index: loading tuples	71488	71488	1000000	954351	68713	0
m3r-L/ivf_L_2	15287	1929	building index: loading tuples	71488	71488	1000000	954351	68713	0
m3r-L/ivf_L_2	15348	1929	building index: loading tuples	71488	71488	1000000	954351	68713	0
m3r-L/ivf_L_2	15408	1929	building index: loading tuples	71488	71488	1000000	954351	68713	0
m3r-L/ivf_L_2	15469	1929	building index: loading tuples	71488	71488	1000000	954351	68713	0
m3r-L/ivf_L_2	15532	1929	building index: loading tuples	71488	71488	1000000	954351	68713	0
m3r-L/ivf_L_2	15593	1929	building index: loading tuples	71488	71488	1000000	954351	68713	0
m3r-L/ivf_L_2	15655	1929	building index: loading tuples	71488	71488	1000000	954366	68713	0
m3r-L/ivf_L_2	15718	1929	building index: loading tuples	71488	71488	1000000	954366	68713	0
m3r-L/ivf_L_2	15780	1929	building index: loading tuples	71488	71488	1000000	954366	68713	0
m3r-L/ivf_L_2	15842	1929	building index: loading tuples	71488	71488	1000000	980243	68713	0
m3r-L/ivf_L_2	15905	1929	building index: loading tuples	71488	71488	1000000	1000000	68713	0
m3r-L/ivf_L_2	15966	1929	building index: loading tuples	71488	71488	1000000	1000000	68713	0
m3r-L/ivf_L_3	0	3189	initializing	0	0	0	0	68713	0
m3r-L/ivf_L_3	61	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	121	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	182	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	243	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	305	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	366	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	427	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	488	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	548	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	610	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	671	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	732	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	793	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	855	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	916	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	977	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	1037	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	1098	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	1159	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	1221	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	1282	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	1343	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	1403	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	1465	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	1525	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	1586	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	1647	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	1707	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	1768	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	1829	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	1889	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	1950	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	2011	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	2071	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	2132	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	2194	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	2255	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	2315	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	2376	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	2437	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	2497	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	2558	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	2619	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	2679	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	2740	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	2800	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	2861	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	2921	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	2982	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	3043	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	3103	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	3164	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	3224	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	3284	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	3344	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	3405	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	3465	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	3525	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	3586	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	3648	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	3709	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	3770	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	3831	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	3892	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	3952	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	4013	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	4074	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	4134	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	4195	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	4255	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	4316	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	4376	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	4436	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	4497	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	4558	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	4618	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	4678	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	4739	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	4799	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	4859	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	4920	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	4980	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	5041	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	5101	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	5161	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	5222	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	5283	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	5345	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	5406	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	5467	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	5528	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	5588	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	5649	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	5709	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	5770	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	5830	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	5891	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	5952	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	6012	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	6073	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	6133	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	6194	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	6254	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	6314	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	6374	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	6435	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	6495	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	6555	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	6615	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	6676	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	6736	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	6796	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	6857	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	6917	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	6978	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	7039	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	7100	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	7161	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	7222	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	7282	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	7342	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	7403	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	7464	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	7525	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	7587	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	7648	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	7710	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	7772	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	7834	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	7895	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	7957	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	8019	3189	building index: performing k-means	0	0	0	0	68713	0
m3r-L/ivf_L_3	8083	3189	building index: assigning tuples	71488	229	0	0	68713	0
m3r-L/ivf_L_3	8153	3189	building index: assigning tuples	71488	1204	0	0	68713	0
m3r-L/ivf_L_3	8217	3189	building index: assigning tuples	71488	2096	0	0	68713	0
m3r-L/ivf_L_3	8300	3189	building index: assigning tuples	71488	3735	0	0	68713	0
m3r-L/ivf_L_3	8376	3189	building index: assigning tuples	71488	4774	0	0	68713	0
m3r-L/ivf_L_3	8454	3189	building index: assigning tuples	71488	5954	0	0	68713	0
m3r-L/ivf_L_3	8539	3189	building index: assigning tuples	71488	7367	0	0	68713	0
m3r-L/ivf_L_3	8616	3189	building index: assigning tuples	71488	8598	0	0	68713	0
m3r-L/ivf_L_3	8698	3189	building index: assigning tuples	71488	9985	0	0	68713	0
m3r-L/ivf_L_3	8779	3189	building index: assigning tuples	71488	11114	0	0	68713	0
m3r-L/ivf_L_3	8849	3189	building index: assigning tuples	71488	12481	0	0	68713	0
m3r-L/ivf_L_3	8937	3189	building index: assigning tuples	71488	13911	0	0	68713	0
m3r-L/ivf_L_3	9016	3189	building index: assigning tuples	71488	14994	0	0	68713	0
m3r-L/ivf_L_3	9081	3189	building index: assigning tuples	71488	16119	0	0	68713	0
m3r-L/ivf_L_3	9158	3189	building index: assigning tuples	71488	17361	0	0	68713	0
m3r-L/ivf_L_3	9230	3189	building index: assigning tuples	71488	18477	0	0	68713	0
m3r-L/ivf_L_3	9301	3189	building index: assigning tuples	71488	19754	0	0	68713	0
m3r-L/ivf_L_3	9380	3189	building index: assigning tuples	71488	20782	0	0	68713	0
m3r-L/ivf_L_3	9459	3189	building index: assigning tuples	71488	22362	0	0	68713	0
m3r-L/ivf_L_3	9541	3189	building index: assigning tuples	71488	23514	0	0	68713	0
m3r-L/ivf_L_3	9620	3189	building index: assigning tuples	71488	24860	0	0	68713	0
m3r-L/ivf_L_3	9701	3189	building index: assigning tuples	71488	26176	0	0	68713	0
m3r-L/ivf_L_3	9791	3189	building index: assigning tuples	71488	27929	0	0	68713	0
m3r-L/ivf_L_3	9877	3189	building index: assigning tuples	71488	28375	0	0	68713	0
m3r-L/ivf_L_3	9951	3189	building index: assigning tuples	71488	29597	0	0	68713	0
m3r-L/ivf_L_3	10019	3189	building index: assigning tuples	71488	30709	0	0	68713	0
m3r-L/ivf_L_3	10085	3189	building index: assigning tuples	71488	32151	0	0	68713	0
m3r-L/ivf_L_3	10166	3189	building index: assigning tuples	71488	33335	0	0	68713	0
m3r-L/ivf_L_3	10253	3189	building index: assigning tuples	71488	34717	0	0	68713	0
m3r-L/ivf_L_3	10331	3189	building index: assigning tuples	71488	35923	0	0	68713	0
m3r-L/ivf_L_3	10402	3189	building index: assigning tuples	71488	36875	0	0	68713	0
m3r-L/ivf_L_3	10465	3189	building index: assigning tuples	71488	37301	0	0	68713	0
m3r-L/ivf_L_3	10530	3189	building index: assigning tuples	71488	38065	0	0	68713	0
m3r-L/ivf_L_3	10609	3189	building index: assigning tuples	71488	39343	0	0	68713	0
m3r-L/ivf_L_3	10676	3189	building index: assigning tuples	71488	40600	0	0	68713	0
m3r-L/ivf_L_3	10769	3189	building index: assigning tuples	71488	41892	0	0	68713	0
m3r-L/ivf_L_3	10834	3189	building index: assigning tuples	71488	43205	0	0	68713	0
m3r-L/ivf_L_3	10916	3189	building index: assigning tuples	71488	44393	0	0	68713	0
m3r-L/ivf_L_3	10993	3189	building index: assigning tuples	71488	45615	0	0	68713	0
m3r-L/ivf_L_3	11067	3189	building index: assigning tuples	71488	46926	0	0	68713	0
m3r-L/ivf_L_3	11136	3189	building index: assigning tuples	71488	48079	0	0	68713	0
m3r-L/ivf_L_3	11214	3189	building index: assigning tuples	71488	49437	0	0	68713	0
m3r-L/ivf_L_3	11287	3189	building index: assigning tuples	71488	50563	0	0	68713	0
m3r-L/ivf_L_3	11389	3189	building index: assigning tuples	71488	52106	0	0	68713	0
m3r-L/ivf_L_3	11470	3189	building index: assigning tuples	71488	53628	0	0	68713	0
m3r-L/ivf_L_3	11551	3189	building index: assigning tuples	71488	54043	0	0	68713	0
m3r-L/ivf_L_3	11615	3189	building index: assigning tuples	71488	54240	0	0	68713	0
m3r-L/ivf_L_3	11687	3189	building index: assigning tuples	71488	55963	0	0	68713	0
m3r-L/ivf_L_3	11764	3189	building index: assigning tuples	71488	56767	0	0	68713	0
m3r-L/ivf_L_3	11827	3189	building index: assigning tuples	71488	57365	0	0	68713	0
m3r-L/ivf_L_3	11903	3189	building index: assigning tuples	71488	57791	0	0	68713	0
m3r-L/ivf_L_3	11964	3189	building index: assigning tuples	71488	57791	0	0	68713	0
m3r-L/ivf_L_3	12025	3189	building index: assigning tuples	71488	57791	0	0	68713	0
m3r-L/ivf_L_3	12085	3189	building index: assigning tuples	71488	58528	0	0	68713	0
m3r-L/ivf_L_3	12152	3189	building index: assigning tuples	71488	59827	0	0	68713	0
m3r-L/ivf_L_3	12230	3189	building index: assigning tuples	71488	61066	0	0	68713	0
m3r-L/ivf_L_3	12305	3189	building index: assigning tuples	71488	62589	0	0	68713	0
m3r-L/ivf_L_3	12390	3189	building index: assigning tuples	71488	63791	0	0	68713	0
m3r-L/ivf_L_3	12465	3189	building index: assigning tuples	71488	64945	0	0	68713	0
m3r-L/ivf_L_3	12557	3189	building index: assigning tuples	71488	66644	0	0	68713	0
m3r-L/ivf_L_3	12641	3189	building index: assigning tuples	71488	68017	0	0	68713	0
m3r-L/ivf_L_3	12730	3189	building index: assigning tuples	71488	69207	0	0	68713	0
m3r-L/ivf_L_3	12805	3189	building index: assigning tuples	71488	70613	0	0	68713	0
m3r-L/ivf_L_3	12871	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_3	12940	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_3	13008	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_3	13072	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_3	13133	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_3	13196	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_3	13259	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_3	13320	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_3	13382	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_3	13445	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_3	13505	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_3	13567	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_3	13629	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_3	13690	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_3	13752	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_3	13814	3189	building index: assigning tuples	71488	71488	0	0	68713	0
m3r-L/ivf_L_3	13875	3189	building index: loading tuples	71488	71488	1000000	9375	68713	0
m3r-L/ivf_L_3	13938	3189	building index: loading tuples	71488	71488	1000000	30941	68713	0
m3r-L/ivf_L_3	14000	3189	building index: loading tuples	71488	71488	1000000	30941	68713	0
m3r-L/ivf_L_3	14061	3189	building index: loading tuples	71488	71488	1000000	30941	68713	0
m3r-L/ivf_L_3	14122	3189	building index: loading tuples	71488	71488	1000000	46785	68713	0
m3r-L/ivf_L_3	14184	3189	building index: loading tuples	71488	71488	1000000	46785	68713	0
m3r-L/ivf_L_3	14244	3189	building index: loading tuples	71488	71488	1000000	46785	68713	0
m3r-L/ivf_L_3	14305	3189	building index: loading tuples	71488	71488	1000000	46785	68713	0
m3r-L/ivf_L_3	14365	3189	building index: loading tuples	71488	71488	1000000	46785	68713	0
m3r-L/ivf_L_3	14427	3189	building index: loading tuples	71488	71488	1000000	46800	68713	0
m3r-L/ivf_L_3	14489	3189	building index: loading tuples	71488	71488	1000000	46800	68713	0
m3r-L/ivf_L_3	14550	3189	building index: loading tuples	71488	71488	1000000	46800	68713	0
m3r-L/ivf_L_3	14611	3189	building index: loading tuples	71488	71488	1000000	46800	68713	0
m3r-L/ivf_L_3	14672	3189	building index: loading tuples	71488	71488	1000000	46800	68713	0
m3r-L/ivf_L_3	14734	3189	building index: loading tuples	71488	71488	1000000	71053	68713	0
m3r-L/ivf_L_3	14796	3189	building index: loading tuples	71488	71488	1000000	102405	68713	0
m3r-L/ivf_L_3	14858	3189	building index: loading tuples	71488	71488	1000000	108653	68713	0
m3r-L/ivf_L_3	14918	3189	building index: loading tuples	71488	71488	1000000	108653	68713	0
m3r-L/ivf_L_3	14979	3189	building index: loading tuples	71488	71488	1000000	108653	68713	0
m3r-L/ivf_L_3	15038	3189	building index: loading tuples	71488	71488	1000000	108653	68713	0
m3r-L/ivf_L_3	15098	3189	building index: loading tuples	71488	71488	1000000	108668	68713	0
m3r-L/ivf_L_3	15160	3189	building index: loading tuples	71488	71488	1000000	108668	68713	0
m3r-L/ivf_L_3	15220	3189	building index: loading tuples	71488	71488	1000000	108668	68713	0
m3r-L/ivf_L_3	15280	3189	building index: loading tuples	71488	71488	1000000	108668	68713	0
m3r-L/ivf_L_3	15340	3189	building index: loading tuples	71488	71488	1000000	108668	68713	0
m3r-L/ivf_L_3	15400	3189	building index: loading tuples	71488	71488	1000000	108668	68713	0
m3r-L/ivf_L_3	15461	3189	building index: loading tuples	71488	71488	1000000	139584	68713	0
m3r-L/ivf_L_3	15521	3189	building index: loading tuples	71488	71488	1000000	139584	68713	0
m3r-L/ivf_L_3	15581	3189	building index: loading tuples	71488	71488	1000000	139584	68713	0
m3r-L/ivf_L_3	15641	3189	building index: loading tuples	71488	71488	1000000	139584	68713	0
m3r-L/ivf_L_3	15701	3189	building index: loading tuples	71488	71488	1000000	139599	68713	0
m3r-L/ivf_L_3	15762	3189	building index: loading tuples	71488	71488	1000000	139599	68713	0
m3r-L/ivf_L_3	15822	3189	building index: loading tuples	71488	71488	1000000	170503	68713	0
m3r-L/ivf_L_3	15883	3189	building index: loading tuples	71488	71488	1000000	200254	68713	0
m3r-L/ivf_L_3	15945	3189	building index: loading tuples	71488	71488	1000000	223838	68713	0
m3r-L/ivf_L_3	16006	3189	building index: loading tuples	71488	71488	1000000	232774	68713	0
m3r-L/ivf_L_3	16067	3189	building index: loading tuples	71488	71488	1000000	258161	68713	0
m3r-L/ivf_L_3	16129	3189	building index: loading tuples	71488	71488	1000000	294210	68713	0
m3r-L/ivf_L_3	16190	3189	building index: loading tuples	71488	71488	1000000	325123	68713	0
m3r-L/ivf_L_3	16251	3189	building index: loading tuples	71488	71488	1000000	335696	68713	0
m3r-L/ivf_L_3	16312	3189	building index: loading tuples	71488	71488	1000000	379453	68713	0
m3r-L/ivf_L_3	16373	3189	building index: loading tuples	71488	71488	1000000	401957	68713	0
m3r-L/ivf_L_3	16435	3189	building index: loading tuples	71488	71488	1000000	417905	68713	0
m3r-L/ivf_L_3	16496	3189	building index: loading tuples	71488	71488	1000000	423426	68713	0
m3r-L/ivf_L_3	16558	3189	building index: loading tuples	71488	71488	1000000	467479	68713	0
m3r-L/ivf_L_3	16619	3189	building index: loading tuples	71488	71488	1000000	490915	68713	0
m3r-L/ivf_L_3	16680	3189	building index: loading tuples	71488	71488	1000000	518075	68713	0
m3r-L/ivf_L_3	16742	3189	building index: loading tuples	71488	71488	1000000	543910	68713	0
m3r-L/ivf_L_3	16803	3189	building index: loading tuples	71488	71488	1000000	554088	68713	0
m3r-L/ivf_L_3	16865	3189	building index: loading tuples	71488	71488	1000000	572529	68713	0
m3r-L/ivf_L_3	16926	3189	building index: loading tuples	71488	71488	1000000	572529	68713	0
m3r-L/ivf_L_3	16986	3189	building index: loading tuples	71488	71488	1000000	572529	68713	0
m3r-L/ivf_L_3	17046	3189	building index: loading tuples	71488	71488	1000000	572529	68713	0
m3r-L/ivf_L_3	17106	3189	building index: loading tuples	71488	71488	1000000	572544	68713	0
m3r-L/ivf_L_3	17166	3189	building index: loading tuples	71488	71488	1000000	572544	68713	0
m3r-L/ivf_L_3	17227	3189	building index: loading tuples	71488	71488	1000000	610583	68713	0
m3r-L/ivf_L_3	17289	3189	building index: loading tuples	71488	71488	1000000	634375	68713	0
m3r-L/ivf_L_3	17350	3189	building index: loading tuples	71488	71488	1000000	665305	68713	0
m3r-L/ivf_L_3	17424	3189	building index: loading tuples	71488	71488	1000000	727166	68713	0
m3r-L/ivf_L_3	17566	3189	building index: loading tuples	71488	71488	1000000	746556	68713	0
m3r-L/ivf_L_3	17628	3189	building index: loading tuples	71488	71488	1000000	758097	68713	0
m3r-L/ivf_L_3	17721	3189	building index: loading tuples	71488	71488	1000000	758112	68713	0
m3r-L/ivf_L_3	17782	3189	building index: loading tuples	71488	71488	1000000	758112	68713	0
m3r-L/ivf_L_3	17842	3189	building index: loading tuples	71488	71488	1000000	758112	68713	0
m3r-L/ivf_L_3	17901	3189	building index: loading tuples	71488	71488	1000000	758112	68713	0
m3r-L/ivf_L_3	17962	3189	building index: loading tuples	71488	71488	1000000	776992	68713	0
m3r-L/ivf_L_3	18024	3189	building index: loading tuples	71488	71488	1000000	811856	68713	0
m3r-L/ivf_L_3	18086	3189	building index: loading tuples	71488	71488	1000000	832706	68713	0
m3r-L/ivf_L_3	18147	3189	building index: loading tuples	71488	71488	1000000	850871	68713	0
m3r-L/ivf_L_3	18225	3189	building index: loading tuples	71488	71488	1000000	877468	68713	0
m3r-L/ivf_L_3	18286	3189	building index: loading tuples	71488	71488	1000000	912711	68713	0
m3r-L/ivf_L_3	18358	3189	building index: loading tuples	71488	71488	1000000	912711	68713	0
m3r-L/ivf_L_3	18418	3189	building index: loading tuples	71488	71488	1000000	912711	68713	0
m3r-L/ivf_L_3	18479	3189	building index: loading tuples	71488	71488	1000000	912711	68713	0
m3r-L/ivf_L_3	18539	3189	building index: loading tuples	71488	71488	1000000	912726	68713	0
m3r-L/ivf_L_3	18600	3189	building index: loading tuples	71488	71488	1000000	912726	68713	0
m3r-L/ivf_L_3	18661	3189	building index: loading tuples	71488	71488	1000000	930493	68713	0
m3r-L/ivf_L_3	18722	3189	building index: loading tuples	71488	71488	1000000	965676	68713	0
m3r-L/ivf_L_3	18786	3189	building index: loading tuples	71488	71488	1000000	982978	68713	0
m3r-L/ivf_L_3	18848	3189	building index: loading tuples	71488	71488	1000000	999999	68713	0
m3r-L/ivf_L_3	18909	3189	building index: loading tuples	71488	71488	1000000	1000000	68713	0
\.


--
-- Data for Name: stage_weight; Type: TABLE DATA; Schema: vecdiag; Owner: postgres
--

COPY vecdiag.stage_weight (am, phase, weight, n_samples, dispersion, source, run_id, measured_at, size_class, dataset) FROM stdin;
hnsw	building index: loading tuples	1.0000	3	0.0000	measured	m3-20260826	2026-08-26 16:34:14.580946+08	pooled	synthetic
ivfflat	building index: assigning tuples	0.4080	13	0.2885	measured	m3-20260826	2026-08-26 16:34:14.580946+08	pooled	synthetic
ivfflat	building index: loading tuples	0.1324	11	0.1731	measured	m3-20260826	2026-08-26 16:34:14.580946+08	pooled	synthetic
ivfflat	building index: performing k-means	0.4596	13	0.2821	measured	m3-20260826	2026-08-26 16:34:14.580946+08	pooled	synthetic
hnsw	building index: loading tuples	1.0000	3	0.0000	measured	m3r-pooled	2026-08-26 22:47:45.54253+08	pooled	sift1m
ivfflat	building index: assigning tuples	0.2941	13	0.4310	measured	m3r-pooled	2026-08-26 22:47:45.54253+08	pooled	sift1m
ivfflat	building index: loading tuples	0.4136	13	0.7230	measured	m3r-pooled	2026-08-26 22:47:45.54253+08	pooled	sift1m
ivfflat	building index: performing k-means	0.2828	13	0.4613	measured	m3r-pooled	2026-08-26 22:47:45.54253+08	pooled	sift1m
ivfflat	initializing	0.0096	3	0.0190	measured	m3r-pooled	2026-08-26 22:47:45.54253+08	pooled	sift1m
ivfflat	building index: assigning tuples	0.1150	3	0.1847	measured	m3r-S	2026-08-26 22:47:45.712846+08	S	sift1m
ivfflat	building index: loading tuples	0.7058	3	0.5144	measured	m3r-S	2026-08-26 22:47:45.712846+08	S	sift1m
ivfflat	building index: performing k-means	0.1792	3	0.3297	measured	m3r-S	2026-08-26 22:47:45.712846+08	S	sift1m
ivfflat	building index: assigning tuples	0.3369	3	0.0479	measured	m3r-M	2026-08-26 22:47:45.852903+08	M	sift1m
ivfflat	building index: loading tuples	0.4329	3	0.0821	measured	m3r-M	2026-08-26 22:47:45.852903+08	M	sift1m
ivfflat	building index: performing k-means	0.2083	3	0.0786	measured	m3r-M	2026-08-26 22:47:45.852903+08	M	sift1m
ivfflat	initializing	0.0219	1	0.0000	measured	m3r-M	2026-08-26 22:47:45.852903+08	M	sift1m
ivfflat	building index: assigning tuples	0.2622	3	0.0829	measured	m3r-L	2026-08-26 22:47:45.998706+08	L	sift1m
ivfflat	building index: loading tuples	0.3042	3	0.0816	measured	m3r-L	2026-08-26 22:47:45.998706+08	L	sift1m
ivfflat	building index: performing k-means	0.4302	3	0.1584	measured	m3r-L	2026-08-26 22:47:45.998706+08	L	sift1m
ivfflat	initializing	0.0033	2	0.0002	measured	m3r-L	2026-08-26 22:47:45.998706+08	L	sift1m
\.


--
-- Name: hnsw_calib_id_seq; Type: SEQUENCE SET; Schema: vecdiag; Owner: postgres
--

SELECT pg_catalog.setval('vecdiag.hnsw_calib_id_seq', 1, false);


--
-- Name: abi_const abi_const_pkey; Type: CONSTRAINT; Schema: vecdiag; Owner: postgres
--

ALTER TABLE ONLY vecdiag.abi_const
    ADD CONSTRAINT abi_const_pkey PRIMARY KEY (key);


--
-- Name: hnsw_calib hnsw_calib_pkey; Type: CONSTRAINT; Schema: vecdiag; Owner: postgres
--

ALTER TABLE ONLY vecdiag.hnsw_calib
    ADD CONSTRAINT hnsw_calib_pkey PRIMARY KEY (id);


--
-- Name: hnsw_coef hnsw_coef_pkey; Type: CONSTRAINT; Schema: vecdiag; Owner: postgres
--

ALTER TABLE ONLY vecdiag.hnsw_coef
    ADD CONSTRAINT hnsw_coef_pkey PRIMARY KEY (key);


--
-- Name: stage_weight stage_weight_pkey; Type: CONSTRAINT; Schema: vecdiag; Owner: postgres
--

ALTER TABLE ONLY vecdiag.stage_weight
    ADD CONSTRAINT stage_weight_pkey PRIMARY KEY (am, phase, size_class, dataset);


--
-- Name: progress_sample_run_idx; Type: INDEX; Schema: vecdiag; Owner: postgres
--

CREATE INDEX progress_sample_run_idx ON vecdiag.progress_sample USING btree (run_id, elapsed_ms);


--
-- PostgreSQL database dump complete
--

\unrestrict CTwzwJ0kadvZaNXaaz57pIsCAoTJKZ677hxIvEk9e4cJLCCVyo430BYnHT6bIRP

