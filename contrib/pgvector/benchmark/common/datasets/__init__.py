"""Simple, fixed adapters for the three benchmark datasets."""

from dataclasses import dataclass
from pathlib import Path

import numpy as np

from common.config import METRIC_CONFIG, metric_config


@dataclass
class VectorDataset:
    name: str
    metric: str
    dimension: int
    root: Path
    base: np.ndarray
    queries: np.ndarray
    ground_truth: np.ndarray

    @property
    def size(self) -> int:
        return len(self.base)

    def validate(self, query_count: int, k: int,
                 validate_expected_size: bool = True) -> "VectorDataset":
        expected_size = METRIC_CONFIG[self.metric]["expected_size"]
        if self.base.ndim != 2 or self.base.shape[1] != self.dimension:
            raise ValueError(f"{self.name}: invalid base shape {self.base.shape}")
        if self.queries.shape != (query_count, self.dimension):
            raise ValueError(f"{self.name}: invalid query shape {self.queries.shape}")
        if self.ground_truth.shape != (query_count, k):
            raise ValueError(f"{self.name}: invalid ground-truth shape {self.ground_truth.shape}")
        if validate_expected_size and self.size != expected_size:
            raise ValueError(f"{self.name}: expected {expected_size} base vectors, got {self.size}")
        if np.any(self.ground_truth < 0) or np.any(self.ground_truth >= self.size):
            raise ValueError(f"{self.name}: ground truth contains out-of-range IDs")
        return self


def load_dataset(name: str, data_root, query_count: int = 1000,
                 k: int = 10, validate_expected_size: bool = True) -> VectorDataset:
    if name == "sift1m":
        from .sift1m import load
    elif name == "glove100":
        from .glove100 import load
    elif name == "music100":
        from .music100 import load
    else:
        raise ValueError(f"unsupported dataset {name!r}; choose sift1m, glove100, or music100")
    metric = next(metric for metric, config in METRIC_CONFIG.items() if config["dataset"] == name)
    return load(
        Path(data_root), metric_config(metric), query_count, k,
        validate_expected_size,
    )


def load_metric(metric: str, data_root, query_count: int = 1000,
                k: int = 10, validate_expected_size: bool = True) -> VectorDataset:
    return load_dataset(
        metric_config(metric)["dataset"], data_root, query_count, k,
        validate_expected_size,
    )


__all__ = ["VectorDataset", "load_dataset", "load_metric"]
