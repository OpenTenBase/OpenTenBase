#!/usr/bin/env python3
"""Generate a deterministic, small SIFT-format dataset for end-to-end tests."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np


def write_vecs(path: Path, values: np.ndarray) -> None:
    values = np.asarray(values)
    width = values.shape[1]
    header = np.full((len(values), 1), width, dtype="<i4")
    payload = values.astype("<f4", copy=False).view("<i4")
    np.concatenate((header, payload), axis=1).tofile(path)


def write_ivecs(path: Path, values: np.ndarray) -> None:
    values = np.asarray(values, dtype="<i4")
    header = np.full((len(values), 1), values.shape[1], dtype="<i4")
    np.concatenate((header, values), axis=1).tofile(path)


def generate(output: Path, rows: int, queries: int, dimension: int, k: int,
             seed: int) -> Path:
    if rows < k or queries < 1 or dimension < 1:
        raise ValueError("rows must be >= k; queries and dimension must be positive")
    root = output / "sift1m"
    root.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)
    base = rng.normal(size=(rows, dimension)).astype("<f4")
    selected = rng.choice(rows, size=queries, replace=False)
    query = base[selected] + rng.normal(scale=0.01, size=(queries, dimension)).astype("<f4")
    distances = np.sum((query[:, None, :] - base[None, :, :]) ** 2, axis=2)
    nearest = np.argsort(distances, axis=1)[:, :k].astype("<i4")
    write_vecs(root / "sift_base.fvecs", base)
    write_vecs(root / "sift_query.fvecs", query)
    write_ivecs(root / "sift_groundtruth.ivecs", nearest)
    return root


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("data"))
    parser.add_argument("--rows", type=int, default=2000)
    parser.add_argument("--queries", type=int, default=20)
    parser.add_argument("--dimension", type=int, default=128)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--seed", type=int, default=20260911)
    args = parser.parse_args()
    print(generate(args.output, args.rows, args.queries, args.dimension, args.k, args.seed))


if __name__ == "__main__":
    main()
