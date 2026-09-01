#!/usr/bin/env python3
# SIFT1M 替代获取路径：ann-benchmarks HDF5 → corpus-texmex fvecs/ivecs。
#
# 为什么需要它：原站 ftp.irisa.fr 在部分网络下只有 ~20 KB/s（161 MB 要两个多小时），
# ann-benchmarks.com 的 sift-128-euclidean.hdf5（294 MB）实测约 800 KB/s。
# 转换产物经过验证（results/rocky-20260827/README.md）：
#   sift_base.fvecs  sha256 21f66e29… —— 与原站官方文件**逐字节一致**
#   sift_query.fvecs sha256 f7fc9be1… —— 逐字节一致
#   sift_groundtruth.ivecs 与官方**不同**（ann-benchmarks 自算邻居顺序）；
#     本仓库工具不使用该文件（召回的 ground truth 一律库内顺序扫描重算）
# 注意 fvecs 头部是 int32 维度（不是 float32）——写错过一次，文件大小对但内容全错。
# 依赖：h5py（pip install h5py）。用法：python3 sift_from_annbenchmarks.py <in.hdf5> <out_dir>
# SIFT1M HDF5 → fvecs/ivecs 转换（ann-benchmarks sift-128-euclidean.hdf5 → corpus-texmex 格式）
# 用途：原站 ftp.irisa.fr 在 Mac/VM 上都只有 ~12-20 KB/s，ann-benchmarks.com 有 ~800 KB/s。
# 转换后各文件与 corpus-texmex 官方格式一致（[int32 dim][dim×float32] / [int32 k][k×int32]），
# 值来自同一份数据管线，可去与 archive 里的 sift_sha256.txt 比对。
# 用法：python3 sift_hdf5_to_fvecs.py <in.hdf5> <out_dir>
import h5py
import numpy as np
import os
import sys

src = sys.argv[1]
out = os.path.abspath(sys.argv[2] if len(sys.argv) > 2 else f"{os.path.dirname(src)}/sift_mirror")

def write_fvecs(path, arr):
    # fvecs 头部是 **int32 维度**（不是 float32！）。写错类型位模式完全不同，
    # 文件大小虽对、内容全错——2026-08-27 实踩，sha 对不上归档值才发现的。
    n, d = arr.shape
    f32 = arr.astype("<f4")
    with open(path, "wb") as fh:
        for i in range(n):
            fh.write(np.int32(d).tobytes())
            fh.write(f32[i].tobytes())
    return os.path.getsize(path)

def write_ivecs(path, arr):
    n, k = arr.shape
    with open(path, "wb") as fh:
        blk = np.empty(k + 1, dtype="<i4")
        for i in range(n):
            blk[0] = np.int32(k)
            blk[1:] = arr[i].astype("<i4")
            fh.write(blk.tobytes())
    return os.path.getsize(path)

with h5py.File(src, "r") as f:
    print("keys:", list(f.keys()))
    for key in f.keys():
        ds = f[key]
        print(f"  {key}: shape={ds.shape} dtype={ds.dtype}")

os.makedirs(out, exist_ok=True)
with h5py.File(src, "r") as f:
    # ann-benchmarks 命名：train=底库(1M)、test=查询(10k)、neighbors=groundtruth。
    # 与原站 corpus-texmex 的对应：train→sift_base、test→sift_query、neighbors→sift_groundtruth；
    # sift_learn(100k 训练集) 不在 HDF5 里，本仓库工具也不用它。
    pairs = [
        ("train", "sift_base.fvecs",        write_fvecs),
        ("test",  "sift_query.fvecs",       write_fvecs),
        ("neighbors", "sift_groundtruth.ivecs", write_ivecs),
    ]
    for key, name, fn in pairs:
        if key in f:
            sz = fn(os.path.join(out, name), f[key][:])
            print(f"{name}: {sz} 字节")

print("done ->", out)