/* contrib/seg/seg--unpackaged--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION seg FROM unpackaged" to load this file. \quit

-- 添加自定义类型 seg，表示浮点数区间
ALTER EXTENSION seg ADD type seg;

-- 添加函数 seg_in，用于将输入的字符串转换为 seg 类型
ALTER EXTENSION seg ADD function seg_in(cstring);

-- 添加函数 seg_out，用于将 seg 类型转换为输出字符串
ALTER EXTENSION seg ADD function seg_out(seg);

-- 定义函数 seg_over_left，计算两个区间的左重叠部分
ALTER EXTENSION seg ADD function seg_over_left(seg,seg);

-- 定义函数 seg_over_right，计算两个区间的右重叠部分
ALTER EXTENSION seg ADD function seg_over_right(seg,seg);

-- 定义函数 seg_left，获取第一个区间与第二个区间的交集
ALTER EXTENSION seg ADD function seg_left(seg,seg);

-- 定义函数 seg_right，获取第二个区间与第一个区间的交集
ALTER EXTENSION seg ADD function seg_right(seg,seg);

-- 定义比较运算符
ALTER EXTENSION seg ADD function seg_lt(seg,seg);
ALTER EXTENSION seg ADD function seg_le(seg,seg);
ALTER EXTENSION seg ADD function seg_gt(seg,seg);
ALTER EXTENSION seg ADD function seg_ge(seg,seg);

-- 定义区间包含关系的函数
ALTER EXTENSION seg ADD function seg_contains(seg,seg);
ALTER EXTENSION seg ADD function seg_contained(seg,seg);

-- 定义区间重叠关系的函数
ALTER EXTENSION seg ADD function seg_overlap(seg,seg);

-- 定义区间相等与不相等的函数
ALTER EXTENSION seg ADD function seg_same(seg,seg);
ALTER EXTENSION seg ADD function seg_different(seg,seg);

-- 定义区间比较函数
ALTER EXTENSION seg ADD function seg_cmp(seg,seg);

-- 定义区间并集与交集的函数
ALTER EXTENSION seg ADD function seg_union(seg,seg);
ALTER EXTENSION seg ADD function seg_inter(seg,seg);

-- 定义获取区间大小、中心、上界、下界的函数
ALTER EXTENSION seg ADD function seg_size(seg);
ALTER EXTENSION seg ADD function seg_center(seg);
ALTER EXTENSION seg ADD function seg_upper(seg);
ALTER EXTENSION seg ADD function seg_lower(seg);

-- 定义运算符
ALTER EXTENSION seg ADD operator >(seg,seg);
ALTER EXTENSION seg ADD operator >=(seg,seg);
ALTER EXTENSION seg ADD operator <(seg,seg);
ALTER EXTENSION seg ADD operator <=(seg,seg);
ALTER EXTENSION seg ADD operator >>(seg,seg);
ALTER EXTENSION seg ADD operator <<(seg,seg);
ALTER EXTENSION seg ADD operator &<(seg,seg);
ALTER EXTENSION seg ADD operator &&(seg,seg);
ALTER EXTENSION seg ADD operator &>(seg,seg);
ALTER EXTENSION seg ADD operator <>(seg,seg);
ALTER EXTENSION seg ADD operator =(seg,seg);
ALTER EXTENSION seg ADD operator <@(seg,seg);
ALTER EXTENSION seg ADD operator @>(seg,seg);
ALTER EXTENSION seg ADD operator ~(seg,seg);
ALTER EXTENSION seg ADD operator @(seg,seg);

-- GiST 索引支持方法
ALTER EXTENSION seg ADD function gseg_consistent(internal,seg,integer,oid,internal);
ALTER EXTENSION seg ADD function gseg_compress(internal);
ALTER EXTENSION seg ADD function gseg_decompress(internal);
ALTER EXTENSION seg ADD function gseg_penalty(internal,internal,internal);
ALTER EXTENSION seg ADD function gseg_picksplit(internal,internal);
ALTER EXTENSION seg ADD function gseg_union(internal,internal);
ALTER EXTENSION seg ADD function gseg_same(seg,seg,internal);

-- 为 B-tree 索引定义操作符族和操作符类
ALTER EXTENSION seg ADD operator family seg_ops using btree;
ALTER EXTENSION seg ADD operator class seg_ops using btree;

-- 为 GiST 索引定义操作符族和操作符类
ALTER EXTENSION seg ADD operator family gist_seg_ops using gist;
ALTER EXTENSION seg ADD operator class gist_seg_ops using gist;