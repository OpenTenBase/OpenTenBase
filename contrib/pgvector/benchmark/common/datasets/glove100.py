"""GloVe-100-angular HDF5 adapter."""

from pathlib import Path

import h5py
import numpy as np


def load(data_root: Path, config: dict, query_count: int, k: int,
         validate_expected_size: bool = True):
    from . import VectorDataset
    root = data_root / "glove-100-angular"
    path = root / "glove-100-angular.hdf5"
    if not path.exists():
        raise FileNotFoundError(f"missing {path}; run ./download.sh --dataset cosine")
    with h5py.File(path, "r") as source:
        for key in ("train", "test", "neighbors"):
            if key not in source:
                raise ValueError(f"{path} does not contain {key!r}")
        dataset = VectorDataset(
            name="glove100", metric="cosine", dimension=config["dimension"], root=root,
            base=np.asarray(source["train"], dtype=np.float32),
            queries=np.asarray(source["test"][:query_count], dtype=np.float32),
            ground_truth=np.asarray(source["neighbors"][:query_count, :k], dtype=np.int32),
        )
    return dataset.validate(query_count, k, validate_expected_size)
