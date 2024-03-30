/*-------------------------------------------------------------------------
 *
 * tsm_system_time.c
 *      support routines for SYSTEM_TIME tablesample method
 *
 * The desire here is to produce a random sample with as many rows as possible
 * in no more than the specified amount of time.  We use a block-sampling
 * approach.  To ensure that the whole relation will be visited if necessary,
 * we start at a randomly chosen block and then advance with a stride that
 * is randomly chosen but is relatively prime to the relation's nblocks.
 *
 * Because of the time dependence, this method is necessarily unrepeatable.
 * However, we do what we can to reduce surprising behavior by selecting
 * the sampling pattern just once per query, much as in tsm_system_rows.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *      contrib/tsm_system_time/tsm_system_time.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h" // 包含PostgreSQL的核心头文件

// 如果是MSVC编译器，则包含额外的头文件
#ifdef _MSC_VER
#include <float.h> // 包含isnan函数的声明
#endif
#include <math.h> // 包含数学函数的声明

// 包含其他必要的PostgreSQL头文件
#include "access/relscan.h" // 关系扫描
#include "access/tsmapi.h" // 表采样方法API
#include "catalog/pg_type.h" // 类型信息
#include "miscadmin.h" // 杂项管理
#include "optimizer/clauses.h" // 查询优化器中的子句处理
#include "optimizer/cost.h" // 查询优化器中的成本估计
#include "utils/sampling.h" // 采样工具
#include "utils/spccache.h" // 表空间缓存

// 模块魔术值，用于插件系统
PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(tsm_system_time_handler);

/*
 * 系统时间采样器的私有状态结构体。
 */
/* Private state */
typedef struct
{
    uint32        seed;            /* random seed */// 随机种子
    double        millis;            /* time limit for sampling */
    instr_time    start_time;        /* scan start time */
    OffsetNumber lt;            /* last tuple returned from current block */
    BlockNumber doneblocks;        /* number of already-scanned blocks */
    BlockNumber lb;                /* last block visited */
    /* these three values are not changed during a rescan: */
    BlockNumber nblocks;        /* number of blocks in relation */
    BlockNumber firstblock;        /* first block to sample from */
    BlockNumber step;            /* step size, or 0 if not set yet */
} SystemTimeSamplerData;
// 静态函数声明
static void system_time_samplescangetsamplesize(PlannerInfo *root,
                                    RelOptInfo *baserel,
                                    List *paramexprs,
                                    BlockNumber *pages,
                                    double *tuples);
static void system_time_initsamplescan(SampleScanState *node,
                           int eflags);
static void system_time_beginsamplescan(SampleScanState *node,
                            Datum *params,
                            int nparams,
                            uint32 seed);
static BlockNumber system_time_nextsampleblock(SampleScanState *node);
static OffsetNumber system_time_nextsampletuple(SampleScanState *node,
                            BlockNumber blockno,
                            OffsetNumber maxoffset);
static uint32 random_relative_prime(uint32 n, SamplerRandomState randstate);


/*
 * 创建一个TsmRoutine描述符，用于SYSTEM_TIME方法。
 */
// tsm_system_time_handler是一个函数，它在PostgreSQL的表采样器插件系统中注册一个基于系统时间的采样方法。
Datum
tsm_system_time_handler(PG_FUNCTION_ARGS)
{
    // 创建一个新的TsmRoutine结构体实例，用于描述采样方法的特性和行为。
    TsmRoutine *tsm = makeNode(TsmRoutine);

    // 设置采样方法接受的参数类型。这里只接受一个参数，类型为浮点数（FLOAT8OID）。
    tsm->parameterTypes = list_make1_oid(FLOAT8OID);

    // 这些标志指示采样方法是否可以在查询之间或扫描之间重复使用。
    // 对于基于时间的采样方法，我们设置为false，因为每次调用都需要重新计算。
    tsm->repeatable_across_queries = false;
    tsm->repeatable_across_scans = false;

    // 指定采样方法的各个步骤对应的函数。
    // SampleScanGetSampleSize：估计采样大小的函数。
    tsm->SampleScanGetSampleSize = system_time_samplescangetsamplesize;
    // InitSampleScan：初始化采样扫描的函数。
    tsm->InitSampleScan = system_time_initsamplescan;
    // BeginSampleScan：开始采样扫描的函数。
    tsm->BeginSampleScan = system_time_beginsamplescan;
    // NextSampleBlock：选择下一个采样块的函数。
    tsm->NextSampleBlock = system_time_nextsampleblock;
    // NextSampleTuple：选择下一个采样元组的函数。
    tsm->NextSampleTuple = system_time_nextsampletuple;
    // EndSampleScan：结束采样扫描的函数。这里设置为NULL，因为基于时间的采样不需要特别的结束处理。
    tsm->EndSampleScan = NULL;

    // 返回创建的TsmRoutine结构体，这样PostgreSQL就可以使用这个采样方法了。
    PG_RETURN_POINTER(tsm);
}
/*
 * 系统时间采样方法的采样大小估计函数。
 */
static void
system_time_samplescangetsamplesize(PlannerInfo *root, // 指向查询计划器信息的结构体
                                    RelOptInfo *baserel, // 指向基础关系（表）的优化信息
                                    List *paramexprs, // 采样方法参数的表达式列表
                                    BlockNumber *pages, // 输出参数，用于存储估计的页数
                                    double *tuples) // 输出参数，用于存储估计的元组数
{
    Node       *limitnode; // 用于存储时间限制表达式的节点
    double        millis; // 用于存储时间限制的毫秒数
    double        spc_random_page_cost; // 表空间的随机读取成本（每页）
    double        npages; // 估计的页数
    double        ntuples; // 估计的元组数

    // 尝试从参数中提取时间限制的估计值
    limitnode = (Node *) linitial(paramexprs);
    limitnode = estimate_expression_value(root, limitnode);

    // 如果提取的值是一个非空的常数值，则将其转换为双精度浮点数
    if (IsA(limitnode, Const) &&
        !((Const *) limitnode)->constisnull)
    {
        millis = DatumGetFloat8(((Const *) limitnode)->constvalue);
        // 如果时间限制是负数或NaN，则使用默认值1000毫秒
        if (millis < 0 || isnan(millis))
        {
            millis = 1000;
        }
    }
    else
    {
        // 如果没有获取到非空的常数值，则同样使用默认时间限制1000毫秒
        millis = 1000;
    }

    // 获取计划器对表空间的随机读取成本的估计
    get_tablespace_page_costs(baserel->reltablespace,
                              &spc_random_page_cost,
                              NULL);
}

    /*
     * 通过假设成本数字表示的是毫秒来估计我们可以读取的页数。这完全是、毫无疑问地
     * 错误的，但我们必须做点什么来产生一个估计，而且没有更好的答案。
     */
    // 根据毫秒数和每页的成本估计我们可以读取的页数
if (spc_random_page_cost > 0)
    npages = millis / spc_random_page_cost; // 如果每页的成本大于0，页数等于毫秒数除以每页成本
else
    npages = millis; // 如果每页成本不大于0，页数等于毫秒数（这个估计不太准确）

// 将页数限制在一个合理的值
npages = clamp_row_est(Min((double) baserel->pages, npages)); // 取baserel->pages和npages的最小值，并应用clamp_row_est函数进行限制

// 如果基础关系中有元组和页数的信息
if (baserel->tuples > 0 && baserel->pages > 0)
{
    // 根据元组密度估计返回的元组数
    double        density = baserel->tuples / (double) baserel->pages;

    ntuples = npages * density; // 估计的元组数等于页数乘以元组密度
}
else
{
    // 由于缺乏数据，假设每页有一个元组
    ntuples = npages;
}

// 将估计的元组数限制在关系大小的范围内
ntuples = clamp_row_est(Min(baserel->tuples, ntuples)); // 取baserel->tuples和ntuples的最小值，并应用clamp_row_est函数进行限制

// 将估计的页数和元组数赋值给输出参数
*pages = npages;
*tuples = ntuples;
}

/*
 * 在执行器设置期间初始化采样扫描。
 */
static void
system_time_initsamplescan(SampleScanState *node, int eflags)
{
    node->tsm_state = palloc0(sizeof(SystemTimeSamplerData)); // 为采样器状态分配内存并初始化
    // 注意上述操作使tsm_state->step保持为0
}

/*
 * 检查参数并准备进行采样扫描。
 */
static void
system_time_beginsamplescan(SampleScanState *node,
                            Datum *params,
                            int nparams,
                            uint32 seed)
{
    SystemTimeSamplerData *sampler = (SystemTimeSamplerData *) node->tsm_state; // 采样器状态
    double        millis = DatumGetFloat8(params[0]); // 从参数中获取毫秒数

// 如果毫秒数是负数或NaN，报告错误
    if (millis < 0 || isnan(millis))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TABLESAMPLE_ARGUMENT),
                 errmsg("sample collection time must not be negative")));

    sampler->seed = seed; // 设置随机种子
    sampler->millis = millis; // 设置采样时间限制
    sampler->lt = InvalidOffsetNumber; // 设置最后一个元组偏移量为无效
    sampler->doneblocks = 0; // 设置已扫描块数为0
    // start_time, lb将在第一次NextSampleBlock调用时初始化
    // 我们有意不在这里改变nblocks/firstblock/step的值
}

/*
 * 选择下一个要采样的块。
 *
 * 使用线性探测算法来选择下一个块。
 */
static BlockNumber
system_time_nextsampleblock(SampleScanState *node) // node是当前采样扫描的状态结构体
{
    SystemTimeSamplerData *sampler = (SystemTimeSamplerData *) node->tsm_state; // 从状态结构体中获取采样器数据
    HeapScanDesc scan = node->ss.ss_currentScanDesc; // 获取当前扫描描述符
    instr_time    cur_time; // 当前时间

    // 这是扫描过程中的第一次调用吗？
    if (sampler->doneblocks == 0)
    {
        // 这是查询中的第一次扫描吗？
        if (sampler->step == 0)
        {
            // 现在我们已经有了扫描描述符，可以进行初始化
            SamplerRandomState randstate; // 随机状态

            // 如果关系（表）为空，则无需扫描
            if (scan->rs_nblocks == 0)
                return InvalidBlockNumber; // 返回无效块号

            // 我们只在设置步骤时需要随机数生成器
            sampler_random_init_state(sampler->seed, randstate); // 初始化随机状态

            // 每个查询只计算一次块数、起始块和步长
            sampler->nblocks = scan->rs_nblocks; // 关系中的块数

            // 选择关系内的一个随机起始块
            sampler->firstblock = sampler_random_fract(randstate) * sampler->nblocks;

            // 为线性探测找到与块数相对质的步长
            sampler->step = random_relative_prime(sampler->nblocks, randstate);
        }

        // 重置最后一个块和开始时间
        sampler->lb = sampler->firstblock; // 设置最后一个块为起始块
        INSTR_TIME_SET_CURRENT(sampler->start_time); // 设置当前时间为开始时间
    }

    /* If we've read all blocks in relation, we're done */
    if (++sampler->doneblocks > sampler->nblocks)
        return InvalidBlockNumber;

    /* If we've used up all the allotted time, we're done */
    INSTR_TIME_SET_CURRENT(cur_time);
    INSTR_TIME_SUBTRACT(cur_time, sampler->start_time);
    if (INSTR_TIME_GET_MILLISEC(cur_time) >= sampler->millis)
        return InvalidBlockNumber;

   /*
     * 在一个查询中的扫描之间，scan->rs_nblocks减少的可能性大概是不存在的；
     * 但为了安全起见，循环直到我们选择一个小于scan->rs_nblocks的块号。我们不关心自第一次扫描以来scan->rs_nblocks是否增加了。
     */
    do
    {
        /* Advance lb, using uint64 arithmetic to forestall overflow */
        sampler->lb = ((uint64) sampler->lb + sampler->step) % sampler->nblocks;
    } while (sampler->lb >= scan->rs_nblocks);

    return sampler->lb;
}

/*
 * 选择当前块中的下一个采样元组。
 *
 * 在块采样中，我们的目标是采样每个选中块中的所有元组。
 *
 * 当我们到达块的末尾时，返回InvalidOffsetNumber，这会指示SampleScan继续到下一个块。
 */

static OffsetNumber
system_time_nextsampletuple(SampleScanState *node, // 当前采样扫描的状态结构体
                            BlockNumber blockno, // 当前块的块号
                            OffsetNumber maxoffset) // 当前块中的最大元组偏移量
{
    SystemTimeSamplerData *sampler = (SystemTimeSamplerData *) node->tsm_state; // 从状态结构体中获取采样器数据
    OffsetNumber tupoffset = sampler->lt; // 获取最后一个处理的元组偏移量

    // 推进到页上的下一个可能的元组偏移量
    if (tupoffset == InvalidOffsetNumber)
        tupoffset = FirstOffsetNumber; // 如果最后一个偏移量无效，则从第一个偏移量开始
    else
        tupoffset++; // 否则，递增到下一个偏移量

    // 是否完成当前块的采样？
    if (tupoffset > maxoffset)
        tupoffset = InvalidOffsetNumber; // 如果当前偏移量超出了块的范围，则设置为无效偏移量

    sampler->lt = tupoffset; // 更新最后一个处理的元组偏移量

    return tupoffset; // 返回当前元组偏移量
}

/*
 * 计算两个uint32类型数的最大公约数。
 */
static uint32
gcd(uint32 a, uint32 b)
{
    uint32        c;

    // 使用欧几里得算法计算最大公约数
    while (a != 0)
    {
        c = a;
        a = b % a; // 计算b除以a的余数
        b = c; // 然后将a的值赋给b，继续循环
    }

    return b; // 返回最大公约数
}
/*
 * 选择一个小于`n`并且与`n`互质的随机值，如果可能的话（否则返回1）。
 */
static uint32
random_relative_prime(uint32 n, SamplerRandomState randstate)
{
    uint32        r; // 用于存储随机生成的数

    // 安全检查，避免无限循环或对于小的`n`返回零结果。
    if (n <= 1)
        return 1; // 如果`n`小于或等于1，直接返回1，因为1与任何正整数都互质。

    // 循环生成随机数，直到找到一个与`n`互质的数。
    // 理论上，只需要2或3次迭代，因为两个数互质的概率约为61%。
    // 但是为了安全起见，在循环中包含CHECK_FOR_INTERRUPTS，以处理可能的中断。
    do
    {
        CHECK_FOR_INTERRUPTS(); // 检查是否有中断信号
        r = (uint32) (sampler_random_fract(randstate) * n); // 生成一个0到`n`之间的随机浮点数，并转换为无符号整数
    } while (r == 0 || gcd(r, n) > 1); // 如果`r`为0或与`n`不互质，则继续循环

    return r; // 返回找到的与`n`互质的随机数
}
