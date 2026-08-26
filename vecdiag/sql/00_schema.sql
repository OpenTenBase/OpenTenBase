-- vecdiag 00 · schema、ABI 常数表与基础工具函数
--
-- 本文件不依赖 pgvector，可以独立安装。
-- 所有函数都固定 search_path，避免被临时对象劫持。
--
-- 适用基线：PostgreSQL 18.6（REL_18_STABLE）+ pgvector 0.8.6
-- 源码引用格式统一为 <文件>:<行号>，指向 pgvector v0.8.6 tag。

\set ON_ERROR_STOP on

create schema if not exists vecdiag;

comment on schema vecdiag is
  'IVFFlat/HNSW 构建期内存与耗时诊断。外部 schema，不修改 pgvector 与 PostgreSQL 本体。';

-- ---------------------------------------------------------------------------
-- ABI 常数
--
-- 这几个值与机器、编译器、块大小绑定，**换机器必须重测**（见 tools/abi_probe.sh）。
-- source 列必须诚实标注来源：
--   measured    实测得到（可复算）
--   source-code 读源码得到（无法在 SQL 层实测，或实测精度不足）
-- ---------------------------------------------------------------------------
create table if not exists vecdiag.abi_const (
    key         text primary key,
    value       bigint      not null,
    source      text        not null check (source in ('measured', 'source-code')),
    source_ref  text        not null,
    note        text,
    measured_at timestamptz not null default now()
);

comment on table vecdiag.abi_const is
  'ABI 常数。换机器、换编译器、换 BLCKSZ 都必须重测后覆盖，不得沿用他机数值。';

insert into vecdiag.abi_const (key, value, source, source_ref, note) values
  ('sizeof_VectorArrayData', 24, 'source-code', 'pgvector v0.8.6 src/ivfflat.h:120-126',
   'int length + int maxlen + int dim + (4 字节补齐) + Size itemsize = 24。SQL 层只能实测出上界（kB 粒度下 <252），占总量约 0.0001%，不影响有效数字。'),
  ('maximum_alignof', 8, 'source-code', 'PostgreSQL MAXIMUM_ALIGNOF on x86-64',
   'MAXALIGN 的对齐宽度。换架构需重确认。'),
  ('max_heap_tuples_per_page', 291, 'source-code',
   'PostgreSQL 18 src/include/access/htup_details.h:629-631',
   '(BLCKSZ - SizeOfPageHeaderData) / (MAXALIGN(SizeofHeapTupleHeader) + sizeof(ItemIdData))，8kB 块下为 291。可由 C2 检查点反解交叉验证。'),
  ('ivfflat_max_lists', 32768, 'source-code', 'pgvector v0.8.6 src/ivfflat.h:57',
   'IVFFLAT_MAX_LISTS。')
on conflict (key) do nothing;

-- ---------------------------------------------------------------------------
-- 基础工具函数
-- ---------------------------------------------------------------------------

create or replace function vecdiag.abi(p_key text)
returns bigint
language sql stable strict
set search_path = pg_catalog, pg_temp
as $$
  select value from vecdiag.abi_const where key = p_key;
$$;

comment on function vecdiag.abi(text) is 'ABI 常数取值。缺键返回 NULL，调用方必须处理。';

-- MAXALIGN(n)：向上取整到 maximum_alignof 的倍数
create or replace function vecdiag.maxalign(p_bytes bigint)
returns bigint
language sql immutable strict
set search_path = pg_catalog, pg_temp
as $$
  select ((p_bytes + 8 - 1) / 8) * 8;
$$;

comment on function vecdiag.maxalign(bigint) is
  '对齐宽度硬编为 8（x86-64 的 MAXIMUM_ALIGNOF）。换架构时同时改这里与 abi_const.maximum_alignof。';

-- vector 类型单个元素在 VectorArray 中的字节数
-- VECTOR_SIZE(dim) = offsetof(Vector, x) + sizeof(float)*dim
--                  = (vl_len_ 4 + dim 2 + unused 2) + 4*dim = 8 + 4*dim
create or replace function vecdiag.vector_itemsize(p_dims int)
returns bigint
language sql immutable strict
set search_path = pg_catalog, pg_temp
as $$
  select 8::bigint + 4::bigint * p_dims;
$$;

comment on function vecdiag.vector_itemsize(int) is
  '只覆盖 vector 类型。halfvec/bit/sparsevec 的 itemsize 不同，未验证前不得套用（RQ-107 局限）。';

-- VECTOR_ARRAY_SIZE(length, size)  ← pgvector v0.8.6 src/ivfflat.h:318
create or replace function vecdiag.vector_array_size(p_length bigint, p_itemsize bigint)
returns bigint
language sql stable strict
set search_path = pg_catalog, pg_temp
as $$
  select vecdiag.abi('sizeof_VectorArrayData')
       + p_length * vecdiag.maxalign(p_itemsize);
$$;

-- ---------------------------------------------------------------------------
-- 内存参数解析
--
-- current_setting('maintenance_work_mem') 可能返回 '256MB'，也可能返回裸数字。
-- PostgreSQL 内存类 GUC 的裸数字单位是 **kB**——旧版实现曾把它当 MB，
-- 造成 1024 倍高估并把危险配置误判为 ok，这里必须按 kB 解析。
-- ---------------------------------------------------------------------------
create or replace function vecdiag.parse_mem_kb(p_setting text)
returns bigint
language plpgsql immutable strict
set search_path = pg_catalog, pg_temp
as $$
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
$$;

comment on function vecdiag.parse_mem_kb(text) is
  '把内存类 GUC 文本解析为 kB。裸数字按 kB 处理（PostgreSQL 语义），无法识别的单位返回 NULL 而不是猜。';

-- 当前会话的 maintenance_work_mem（kB）
create or replace function vecdiag.current_mwm_kb()
returns bigint
language sql stable
set search_path = pg_catalog, pg_temp
as $$
  select vecdiag.parse_mem_kb(current_setting('maintenance_work_mem'));
$$;


