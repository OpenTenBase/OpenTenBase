"""SIFT1M TexMex file adapter."""

from pathlib import Path

import numpy as np


def read_vecs(path: Path, dtype: str) -> np.ndarray:
    raw = np.fromfile(path, dtype="<i4")
    if raw.size == 0:
        raise ValueError(f"empty vector file: {path}")
    width = int(raw[0]) + 1
    if width <= 1 or raw.size % width:
        raise ValueError(f"invalid vector file: {path}")
    rows = raw.reshape(-1, width)
    if np.any(rows[:, 0] != width - 1):
        raise ValueError(f"mixed dimensions in vector file: {path}")
    values = rows[:, 1:]
    return values.view(dtype) if dtype == "<f4" else values


def read_fvecs(path: Path) -> np.ndarray:
    return read_vecs(path, "<f4")


def read_ivecs(path: Path) -> np.ndarray:
    return read_vecs(path, "<i4")


def load(data_root: Path, config: dict, query_count: int, k: int,
         validate_expected_size: bool = True):
    from . import VectorDataset
    root = data_root / "sift1m"
    dataset = VectorDataset(
        name="sift1m", metric="l2", dimension=config["dimension"], root=root,
        base=read_fvecs(root / "sift_base.fvecs"),
        queries=read_fvecs(root / "sift_query.fvecs")[:query_count],
        ground_truth=read_ivecs(root / "sift_groundtruth.ivecs")[:query_count, :k],
    )
    return dataset.validate(query_count, k, validate_expected_size)
