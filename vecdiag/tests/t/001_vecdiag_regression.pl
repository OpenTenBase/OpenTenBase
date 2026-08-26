#!/usr/bin/perl
# T4.5 · 回归/TAP 用例
#
# 为什么是 Perl + Test::More 而不是 PostgreSQL::Test::Cluster：
#   后者会为每个测试文件新起一个临时集群，跑得慢，而且本项目的断言全部是
#   "在一个装好 vecdiag 的库上，模型的判定对不对"，不需要独立集群。
#   这里用 core 模块 Test::More 输出标准 TAP，`prove` 能直接跑：
#       prove -v tests/t/001_vecdiag_regression.pl
#   环境变量：PGHOME / PGPORT / PGDB（默认与其他脚本一致）
#
# 覆盖（对齐 T4.5 验收项）：
#   1) 预测=报错：模型说会超限的配置，真建索引确实报错，且报错里的 MB 数与模型一致
#   2) 零参数可运行：vecdiag.diagnose() 无参可跑，且每行四要素齐全
#   3) 低内存预警：HNSW 在小内存下预测降级，NOTICE 里的行号落在预测区间内
#   4) HNSW 降级识别：降级点预测与实测偏差在 1% 以内
#   5) 今天修掉的两个缺陷各留一条回归：阶段顺序来自源码常量；HNSW 不上报 tuples_total
use strict;
use warnings;
use Test::More;

my $PGHOME = $ENV{PGHOME} // '/data/pg18/install';
my $PGPORT = $ENV{PGPORT} // '5518';
my $PGDB   = $ENV{PGDB}   // 'postgres';
my $PSQL   = "$PGHOME/bin/psql -p $PGPORT -d $PGDB -X -q -At";

sub q1 {                                  # 取单值
    my ($sql) = @_;
    my $out = `$PSQL -c "$sql" 2>&1`;
    chomp $out;
    return $out;
}
sub run_sql {                             # 取 (stdout, stderr) —— 需要看报错原文
    my ($sql) = @_;
    my $err = "/tmp/vecdiag_tap_$$.err";
    my $out = `$PSQL -c "$sql" 2>$err`;
    my $e = do { local (@ARGV, $/) = ($err); <> } // '';
    unlink $err;
    chomp $out;
    return ($out, $e);
}

# --- 0 前置：库里必须已经装好 vecdiag ---------------------------------------
is(q1("select count(*) from pg_namespace where nspname='vecdiag'"), '1',
   'vecdiag schema 已安装（否则先跑 tools/install.sh）')
  or BAIL_OUT('vecdiag 未安装，后面的断言没有意义');

# --- 1 预测=报错 -------------------------------------------------------------
# 取一个模型判定"会超限"的配置，真建一次，要求：真的报错，且报错里的 MB 与模型一致。
{
    my $rows = 100000; my $dims = 128; my $lists = 1000; my $mwm = 1024;   # 1 MB
    my $fh = q1("select first_hit::text from vecdiag.ivfflat_predict(
                   p_rows := $rows, p_dims := $dims, p_lists := $lists, p_mwm_kb := $mwm)");
    isnt($fh, 'none', "模型判定 lists=$lists / mwm=1MB 会超限（first_hit=$fh）");

    my $mb = q1("select predicted_mb from vecdiag.ivfflat_predict(
                   p_rows := $rows, p_dims := $dims, p_lists := $lists, p_mwm_kb := $mwm)");
    run_sql("drop table if exists tap_t; create table tap_t as
             select id, v from sift_base order by id limit $rows; analyze tap_t");
    my (undef, $err) = run_sql("set maintenance_work_mem='${mwm}kB';
             create index tap_ix on tap_t using ivfflat (v vector_l2_ops) with (lists=$lists)");
    like($err, qr/memory required is \d+ MB/, '真建索引确实报了内存不足');
    my ($actual_mb) = $err =~ /memory required is (\d+) MB/;
    is($actual_mb, $mb, "报错里的 MB 与模型预测一致（模型 $mb / 实际 ".($actual_mb // 'n/a').'）');
}

# --- 2 零参数体检 ------------------------------------------------------------
is(q1("select pronargs from pg_proc p join pg_namespace n on n.oid=p.pronamespace
        where n.nspname='vecdiag' and p.proname='diagnose'"), '0',
   'vecdiag.diagnose() 是零参数（迁移场景下可以直接粘贴执行）');

{
    my $bad = q1("select count(*) from vecdiag.diagnose()
                   where problem is null or cause is null or fix is null or verify is null
                      or problem='' or cause='' or fix='' or verify=''");
    is($bad, '0', 'diagnose() 每行四要素齐全（问题/原因/调整方法/验证方式）');
}

# --- 3 低内存预警 + 4 降级识别 ----------------------------------------------
{
    my $rows = 50000; my $dims = 128; my $m = 16; my $mwm = 20480;   # 20 MB
    my $spill = q1("select will_spill::text from vecdiag.hnsw_predict_spill($rows, $dims, $m, $mwm)");
    is($spill, 'true', 'HNSW 在 20MB 下预测会降级落盘');

    my $pred = q1("select predicted_spill_tuples from vecdiag.hnsw_predict_spill($rows, $dims, $m, $mwm)");
    run_sql("drop table if exists tap_h; create table tap_h as
             select id, v from sift_base order by id limit $rows; analyze tap_h");
    my (undef, $err) = run_sql("set maintenance_work_mem='${mwm}kB';
             create index tap_hix on tap_h using hnsw (v vector_l2_ops) with (m=$m)");
    like($err, qr/no longer fits into maintenance_work_mem after (\d+) tuples/,
         '实际构建打出了降级 NOTICE');
    my ($actual) = $err =~ /after (\d+) tuples/;
    if (defined $actual && $pred =~ /^\d+$/ && $pred > 0) {
        my $rel = abs($actual - $pred) / $pred * 100;
        cmp_ok($rel, '<', 1.0,
               sprintf('降级点预测偏差 < 1%%（预测 %s / 实测 %s / 偏差 %.2f%%）', $pred, $actual, $rel));
        # 区间口径也要守：实测必须落在 hnsw_spill_range 给的区间里
        my $in = q1("select ($actual between spill_low and spill_high)::text
                       from vecdiag.hnsw_spill_range($rows, $dims, $m, $mwm)");
        is($in, 'true', '实测降级点落在预测区间内');
    } else {
        fail('拿不到实测降级行号，无法比对');
        fail('拿不到实测降级行号，无法比对区间');
    }
}

# --- 5 今天修掉的两个缺陷，各留一条回归 -------------------------------------
# 缺陷一：阶段前序权重曾按"权重大小"排序，导致第一个采样点的进度就是 99.83%。
#         顺序必须来自源码常量 PROGRESS_CREATEIDX_SUBPHASE。
is(q1("select ord from vecdiag.phase_order where am='ivfflat' and phase='initializing'"), '1',
   '阶段顺序表里 initializing 排第 1（顺序来自源码常量，不是按权重大小排）');
is(q1("select string_agg(phase, '>' order by ord) from vecdiag.phase_order where am='ivfflat'"),
   'initializing>building index: performing k-means>building index: assigning tuples>building index: loading tuples',
   'IVFFlat 四个阶段的顺序与 ivfflat.h:61-64 一致');
{
    # 曲线不能一开始就接近 100%：抽一条已归档的实测序列，要求前 10% 时间内进度 < 50%
    my $n = q1("select count(*) from vecdiag.progress_sample where run_id='m3r-L/ivf_L_1'");
    if ($n && $n > 0) {
        my $early = q1("with c as (select * from vecdiag.progress_curve('m3r-L/ivf_L_1','ivfflat','L','sift1m')),
                             b as (select max(elapsed_ms) t from c)
                        select coalesce(max(mono_pct),0) from c, b where c.elapsed_ms <= b.t * 0.10");
        cmp_ok($early + 0, '<', 50,
               "构建前 10% 时间内的进度 < 50%（实测 $early%；回归『进度一开始就 99.83%』那个缺陷）");
    } else {
        SKIP: { skip('库里没有 m3r-L/ivf_L_1 采样序列，跳过曲线断言', 1); }
    }
}
# 缺陷二：HNSW 构建不上报 tuples_total（实测恒为 0），按它过滤会一行都取不到。
{
    my $n = q1("select count(*) from vecdiag.progress_sample
                 where run_id like '%hnsw_spill' and coalesce(tuples_total,0) > 0");
    is($n, '0', 'HNSW 采样序列里 tuples_total 恒为 0（源码事实，ETA 修正函数必须靠外部传行数）');
}

# --- 5b T4.3 边界：所需内存必须精确到 kB，且与"报错里的 MB"是两个不同的数 ------
{
    my $rows = 50000; my $dims = 128; my $lists = 300;
    my $need = q1("select min_mwm_kb from vecdiag.ivfflat_min_mwm_kb($rows, $dims, $lists)");
    like($need, qr/^\d+$/, "拿到所需内存下界 $need kB");
    my $err_mb = q1("select predicted_mb from vecdiag.ivfflat_predict(
                       p_rows := $rows, p_dims := $dims, p_lists := $lists, p_mwm_kb := 1024)");
    cmp_ok($err_mb + 0, '<', $need / 1024,
           "报错消息里的 MB（$err_mb）小于真正所需（约 ".int($need/1024)." MB）——"
           . "照报错设内存会再失败一次，这是 T4.3 抓到的坑");

    run_sql("drop table if exists tap_b; create table tap_b as
             select id, v from sift_base order by id limit $rows; analyze tap_b");
    my (undef, $e1) = run_sql("set maintenance_work_mem='${need}kB';
             create index tap_bix on tap_b using ivfflat (v vector_l2_ops) with (lists=$lists)");
    is(q1("select count(*) from pg_class where relname='tap_bix'"), '1',
       "所需内存 $need kB 处确实建成（模型不偏乐观）");
    run_sql("drop index if exists tap_bix");

    my $lo = $need - 1;
    my (undef, $e2) = run_sql("set maintenance_work_mem='${lo}kB';
             create index tap_bix on tap_b using ivfflat (v vector_l2_ops) with (lists=$lists)");
    like($e2, qr/memory required is/, "所需内存 −1 kB（$lo kB）处确实报错（边界精确到 kB）");
    run_sql("drop index if exists tap_bix; drop table if exists tap_b");
}

# --- 6 安全加固回归（T4.6）：所有 vecdiag 函数必须固定 search_path ----------
is(q1("select count(*) from pg_proc p join pg_namespace n on n.oid=p.pronamespace
        where n.nspname='vecdiag' and p.prokind='f'
          and not exists (select 1 from unnest(coalesce(p.proconfig,'{}')) c
                          where c like 'search_path=%')"), '0',
   '所有 vecdiag 函数都固定了 search_path');

run_sql("drop index if exists tap_ix; drop index if exists tap_hix;
         drop table if exists tap_t; drop table if exists tap_h");
done_testing();
