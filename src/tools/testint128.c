/*-------------------------------------------------------------------------
 *
 * testint128.c
 *      Testbed for roll-our-own 128-bit integer arithmetic.
 *
 * This is a standalone test program that compares the behavior of an
 * implementation in int128.h to an (assumed correct) int128 native type.
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *      src/tools/testint128.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

/*
 * By default, we test the non-native implementation in int128.h; but
 * by predefining USE_NATIVE_INT128 to 1, you can test the native
 * implementation, just to be sure.
 */
#ifndef USE_NATIVE_INT128
#define USE_NATIVE_INT128 0
#endif

#include "common/int128.h"

/*
 * We assume the parts of this union are laid out compatibly.
 */
typedef union
{
    int128        i128;
    INT128        I128;
    union
    {
#ifdef WORDS_BIGENDIAN
        int64        hi;
        uint64        lo;
#else
        uint64        lo;
        int64        hi;
#endif
    }            hl;
}            test128;


/*
 * Control version of comparator.
 */
static inline int
my_int128_compare(int128 x, int128 y)
{
    if (x < y)
        return -1;
    if (x > y)
        return 1;
    return 0;
}

/*
 * 获取一个随机的 uint64 值。
 * 我们不假设 random() 函数能生成超过 16 位的好的随机数。
 */
static uint64
get_random_uint64(void)
{
    uint64 x;

    x = (uint64) (random() & 0xFFFF) << 48;  // 取 16 位随机数左移 48 位
    x |= (uint64) (random() & 0xFFFF) << 32; // 取 16 位随机数左移 32 位，并合并到 x
    x |= (uint64) (random() & 0xFFFF) << 16; // 取 16 位随机数左移 16 位，并合并到 x
    x |= (uint64) (random() & 0xFFFF);       // 取 16 位随机数，并合并到 x
    return x;
}

/*
 * 主程序。
 *
 * 生成大量随机数并对每个实现进行测试。
 * 结果应该是可重现的，因为我们没有调用 srandom() 函数。
 *
 * 如果你不喜欢默认的 10 亿迭代次数，你可以给出一个循环次数。
 */
int
main(int argc, char **argv)
{
    long count;

    if (argc >= 2)
        count = strtol(argv[1], NULL, 0); // 将命令行参数转换为长整型作为迭代次数
    else
        count = 1000000000; // 默认迭代 10 亿次

    while (count-- > 0)
    {
        int64 x = get_random_uint64(); // 生成随机数 x
        int64 y = get_random_uint64(); // 生成随机数 y
        int64 z = get_random_uint64(); // 生成随机数 z
        test128 t1; // 定义测试结构体 t1
        test128 t2; // 定义测试结构体 t2

        /* 检查无符号加法 */
        t1.hl.hi = x; // 设置 t1 的高 64 位为 x
        t1.hl.lo = y; // 设置 t1 的低 64 位为 y
        t2 = t1; // 复制 t1 到 t2
        t1.i128 += (int128) (uint64) z; // 执行无符号加法
        int128_add_uint64(&t2.I128, (uint64) z); // 调用函数执行无符号加法

        if (t1.hl.hi != t2.hl.hi || t1.hl.lo != t2.hl.lo)
        {
            printf("%016lX%016lX + unsigned %lX\n", x, y, z);
            printf("native = %016lX%016lX\n", t1.hl.hi, t1.hl.lo);
            printf("result = %016lX%016lX\n", t2.hl.hi, t2.hl.lo);
            return 1;
        }

        /* 检查有符号加法 */
        t1.hl.hi = x;
        t1.hl.lo = y;
        t2 = t1;
        t1.i128 += (int128) z;
        int128_add_int64(&t2.I128, z);

        if (t1.hl.hi != t2.hl.hi || t1.hl.lo != t2.hl.lo)
        {
            printf("%016lX%016lX + signed %lX\n", x, y, z);
            printf("native = %016lX%016lX\n", t1.hl.hi, t1.hl.lo);
            printf("result = %016lX%016lX\n", t2.hl.hi, t2.hl.lo);
            return 1;
        }

        /* 检查乘法 */
        t1.i128 = (int128) x * (int128) y;

        t2.hl.hi = t2.hl.lo = 0;
        int128_add_int64_mul_int64(&t2.I128, x, y);

        if (t1.hl.hi != t2.hl.hi || t1.hl.lo != t2.hl.lo)
        {
            printf("%lX * %lX\n", x, y);
            printf("native = %016lX%016lX\n", t1.hl.hi, t1.hl.lo);
            printf("result = %016lX%016lX\n", t2.hl.hi, t2.hl.lo);
            return 1;
        }

        /* 检查比较 */
        t1.hl.hi = x;
        t1.hl.lo = y;
        t2.hl.hi = z;
        t2.hl.lo = get_random_uint64();

        if (my_int128_compare(t1.i128, t2.i128) !=
            int128_compare(t1.I128, t2.I128))
        {
            printf("comparison failure: %d vs %d\n",
                   my_int128_compare(t1.i128, t2.i128),
                   int128_compare(t1.I128, t2.I128));
            printf("arg1 = %016lX%016lX\n", t1.hl.hi, t1.hl.lo);
            printf("arg2 = %016lX%016lX\n", t2.hl.hi, t2.hl.lo);
            return 1;
        }

        /* 检查具有相同高位的情况；上述检查几乎不会触发它 */
        t2.hl.hi = x;

        if (my_int128_compare(t1.i128, t2.i128) !=
            int128_compare(t1.I128, t2.I128))
        {
            printf("comparison failure: %d vs %d\n",
                   my_int128_compare(t1.i128, t2.i128),
                   int128_compare(t1.I128, t2.I128));
            printf("arg1 = %016lX%016lX\n", t1.hl.hi, t1.hl.lo);
            printf("arg2 = %016lX%016lX\n", t2.hl.hi, t2.hl.lo);
            return 1;
        }
    }

    return 0;
}
