-- vecdiag 50 · 零参数体检（M4）
--
-- 设计约束（门禁 K16/T4.2）：
--   * 入口函数 **零参数**（pronargs = 0），拿到库就能跑，不需要先读文档；
--   * 每一条输出都必须给齐**四要素**：问题 / 原因 / 调整方法 / 验证方式，缺一不算合格；
--   * 静态目录推断出来的风险，措辞上必须与 EXPLAIN 结论区分开，不许冒充执行计划事实；
--   * 拿不到前提（统计信息过期、本机没标定 ABI、没有达标的阶段权重）时**明说拿不到**，
--     不用默认值蒙一个结论。

\set ON_ERROR_STOP on

create or replace function vecdiag.diagnose()
returns table (
    severity   text,     -- error / warn / info
    object     text,
    problem    text,
    cause      text,
    fix        text,
    verify     text
)
language plpgsql stable
set search_path = pg_catalog, pg_temp
as $$
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

comment on function vecdiag.diagnose() is
  '零参数体检入口。每条输出都带问题/原因/调整方法/验证方式四要素；拿不到前提时明说拿不到。';
